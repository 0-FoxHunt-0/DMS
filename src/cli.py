import time
import threading
from typing import List
from queue import Queue, Empty
from pathlib import Path
from typing import List, Tuple, Optional

import typer
from rich import print as rprint
import logging
from datetime import datetime
from rich.table import Table

from .discord_client import DiscordClient
from .scanner import ScanResult, scan_media
from .config import load_env, set_env_var


load_env()

# Tee-style print: mirror console messages to log file if logging is configured
_orig_rprint = rprint

def _tee_print(*args, level: str = "info", **kwargs):
    message = " ".join(str(a) for a in args)
    if logging.getLogger().handlers:
        log_fn = getattr(logging, level, logging.info)
        log_fn(message)
    return _orig_rprint(*args, **kwargs)

# Override rprint to tee into logs once logging is configured
rprint = _tee_print  # type: ignore

def _setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Clear existing handlers to avoid duplicates
    for h in list(root.handlers):
        root.removeHandler(h)
    fh = logging.FileHandler(str(log_file), encoding="utf-8")
    fmt = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh.setFormatter(fmt)
    root.addHandler(fh)


def _cleanup_old_logs(log_dir: Path, keep: int = 5) -> None:
    """Remove old run_*.log files, keeping the most recent `keep` files.

    This is a lightweight cleanup to prevent unbounded growth of the logs directory.
    """
    try:
        if not log_dir.exists():
            return
        log_files = list(log_dir.glob("run_*.log"))
        # Sort newest first by modification time
        log_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for old in log_files[keep:]:
            try:
                old.unlink()
            except Exception:
                # Best-effort cleanup; ignore files that cannot be deleted
                pass
    except Exception:
        # Never let cleanup errors impact the main flow
        pass

app = typer.Typer(add_completion=False, help="Send media to Discord, pairing MP4+GIF and handling segments.")


def _print_plan(result: ScanResult) -> None:
    table = Table(title="Planned Uploads", show_lines=False)
    table.add_column("Type", style="cyan")
    table.add_column("Items", style="white")

    for pair in result.pairs:
        table.add_row("pair", f"{pair.mp4_path.name} + {pair.gif_path.name}")
    for single in result.singles:
        table.add_row("single", single.path.name)
    rprint(table)


@app.callback(invoke_without_command=True)
def _root(gui: bool = typer.Option(False, "--gui", help="Launch the GUI and exit")) -> None:
    """Root options for the CLI.

    Providing --gui will open the graphical interface and exit.
    """
    if gui:
        try:
            from .gui import launch_gui
            launch_gui()
        except Exception as e:
            rprint(f"[red]Failed to launch GUI: {e}[/red]")
            raise typer.Exit(code=1)
        raise typer.Exit(code=0)
    # If invoked with no command and no --gui, show help
    # Typer supplies context, but we keep it simple here and just print help via rprint
    # so that `python main.py` without args shows the commands.
    raise typer.Exit(code=2)


@app.command()
def gui() -> None:
    """Launch the graphical user interface."""
    try:
        from .gui import launch_gui
        launch_gui()
    except Exception as e:
        rprint(f"[red]Failed to launch GUI: {e}[/red]")
        raise typer.Exit(code=1)


@app.command()
def send(
    input_dir: Path = typer.Argument(..., exists=True, file_okay=False, dir_okay=True, readable=True),
    channel_url: str = typer.Argument(..., help="Discord channel URL like https://discord.com/channels/<guild>/<channel>"),
    token: Optional[str] = typer.Option(
        None,
        help="Discord token; can be set via DISCORD_TOKEN or stored in .env",
        envvar="DISCORD_TOKEN",
    ),
    log_file: Optional[Path] = typer.Option(None, help="Write logs to this file (tee console output). If omitted, a timestamped file in logs is used."),
    token_type: str = typer.Option("auto", help="Token type: 'bot', 'user', or 'auto' (detect)"),
    post_title: Optional[str] = typer.Option(None, help="Title to use when creating a Forum/Media post thread"),
    post_tag: Optional[str] = typer.Option(None, help="Forum tag to apply by name (if forum requires tags)"),
    relay_from: Optional[str] = typer.Option(None, help="Optional source channel URL to relay media from (downloads then re-uploads)"),
    relay_download_dir: Path = typer.Option(Path(".adms_cache"), help="Directory to store downloaded media during relay"),
    ignore_dedupe: bool = typer.Option(False, help="Ignore channel history and send all files"),
    dry_run: bool = typer.Option(False, help="Print actions without uploading"),
    history_limit: int = typer.Option(1000, help="Max messages to scan for dedupe"),
    request_timeout: float = typer.Option(30.0, help="HTTP request timeout (seconds) for history"),
    upload_timeout: float = typer.Option(120.0, help="HTTP request timeout (seconds) for uploads"),
    delay_seconds: float = typer.Option(1.0, help="Delay between messages (seconds)"),
    max_file_mb: float = typer.Option(10.0, help="Skip files larger than this size (MB), unless overridden"),
    skip_oversize: bool = typer.Option(True, help="Skip files exceeding max_file_mb instead of attempting upload"),
    concurrency: int = typer.Option(1, help="Number of concurrent uploads (messages)"),
) -> None:
    if log_file is None:
        default_dir = Path("logs")
        log_file = default_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    _setup_logging(log_file)
    rprint(f"[green]Logging to {log_file}[/green]")
    # Best-effort: keep only the last 5 run logs
    _cleanup_old_logs(log_file.parent, keep=5)
    if not token:
        rprint("[yellow]No token found. You'll be prompted and can save it for reuse.[/yellow]")
        token = typer.prompt("Enter Discord token", hide_input=True)
        if not token:
            rprint("[red]No token provided.[/red]")
            raise typer.Exit(code=2)
        if typer.confirm("Save token to .env for future runs?", default=True):
            set_env_var("DISCORD_TOKEN", token)
            rprint("[green]Token saved to .env[/green]")

    client = DiscordClient(token=token, token_type=token_type)
    guild_id, channel_id, thread_id = client.parse_ids_from_url(channel_url)
    if channel_id is None:
        rprint("[red]Invalid channel URL. Expected https://discord.com/channels/<guild>/<channel>[/red]")
        raise typer.Exit(code=2)

    # Inspect channel type
    ch = client.get_channel(channel_id)
    ch_type = ch.get("type") if ch else None
    # Discord channel types of interest: 0=text, 2=voice, 10=News, 11=Store (legacy), 12=Stage, 15=Forum, 16=Media
    if ch is None:
        rprint("[yellow]Warning: Failed to fetch channel info. Proceeding assuming a text/thread channel.[/yellow]")
    elif ch_type == 2:
        rprint("[red]Destination is a voice channel. Aborting.[/red]")
        raise typer.Exit(code=2)
    is_forum_like = ch_type in (15, 16) if ch is not None else False
    # If a thread URL was provided, prefer sending into that thread
    target_channel_id = channel_id
    if is_forum_like and thread_id is None:
        # Need to create thread (post)
        title = post_title or typer.prompt("Enter post title for Forum/Media channel")
        applied_tag_ids = None
        if ch and post_tag:
            tag_l = post_tag.strip().lower()
            for t in ch.get("available_tags", []):
                if str(t.get("name", "")).lower() == tag_l:
                    applied_tag_ids = [t.get("id")]
                    break
            if not applied_tag_ids:
                rprint(f"[yellow]Warning: Tag '{post_tag}' not found in channel; creating without tags.[/yellow]")
        new_thread_id = client.start_forum_post(channel_id, title, content=title, applied_tag_ids=applied_tag_ids)
        if not new_thread_id:
            rprint("[red]Failed to create post thread. See [DEBUG] logs above for details.[/red]")
            raise typer.Exit(code=2)
        target_channel_id = new_thread_id
    elif thread_id is not None:
        target_channel_id = thread_id

    # Relay mode: download from source and send to destination
    if relay_from:
        _g, _c, _t = client.parse_ids_from_url(relay_from)
        source_id = _t or _c
        if source_id is None:
            rprint("[red]Invalid --relay-from URL. Expected https://discord.com/channels/<guild>/<channel>[/red]")
            raise typer.Exit(code=2)
        rprint(f"[bold]Relaying media from[/bold] {relay_from} [bold]to[/bold] {channel_url}")
        sent, skipped = client.relay_media(
            source_channel_id=source_id,
            dest_channel_id=target_channel_id,
            download_dir=relay_download_dir,
            max_messages=history_limit,
            request_timeout=request_timeout,
            upload_timeout=upload_timeout,
            delay_seconds=delay_seconds,
            max_file_mb=max_file_mb,
            skip_oversize=skip_oversize,
        )
        rprint(f"[green]Relay complete.[/green] Sent {sent}, skipped {skipped}.")
        return

    rprint(f"[bold]Scanning:[/bold] {input_dir}")
    scan = scan_media(input_dir)
    _print_plan(scan)

    if not ignore_dedupe:
        rprint("[bold]Fetching existing filenames from channel for dedupe...[/bold]")
        # IMPORTANT: dedupe must use the actual destination (thread if created/provided)
        existing = client.fetch_existing_filenames(target_channel_id, max_messages=history_limit, request_timeout=request_timeout)
        before_pairs = len(scan.pairs)
        before_singles = len(scan.singles)

        scan = scan.filter_against_filenames(existing)
        removed_pairs = before_pairs - len(scan.pairs)
        removed_singles = before_singles - len(scan.singles)
        rprint(f"Dedupe removed {removed_pairs} pairs and {removed_singles} singles")

    if dry_run:
        rprint("[yellow]Dry run: no uploads performed[/yellow]\n")
        return

    rprint("[bold]Uploading...[/bold]")
    # Build tasks with size checks
    sent_count = 0
    skipped_oversize = 0
    bytes_limit = int(max_file_mb * 1024 * 1024)
    tasks: List[List[Path]] = []

    for pair in scan.pairs:
        mp4_ok = pair.mp4_path.stat().st_size <= bytes_limit
        gif_ok = pair.gif_path.stat().st_size <= bytes_limit
        files_to_send: List[Path] = []
        if mp4_ok or not skip_oversize:
            files_to_send.append(pair.mp4_path)
        else:
            skipped_oversize += 1
        if gif_ok or not skip_oversize:
            files_to_send.append(pair.gif_path)
        else:
            skipped_oversize += 1
        if files_to_send:
            tasks.append(files_to_send)

    for single in scan.singles:
        size_ok = single.path.stat().st_size <= bytes_limit
        if not size_ok and skip_oversize:
            skipped_oversize += 1
            continue
        tasks.append([single.path])

    q: Queue = Queue()
    for tsk in tasks:
        q.put(tsk)

    lock = threading.Lock()

    def _worker(idx: int) -> None:
        nonlocal sent_count
        while True:
            try:
                files = q.get_nowait()
            except Empty:
                break
            try:
                logging.info(f"Uploading: {', '.join(p.name for p in files)}")
                client.send_message_with_files(
                    channel_id=target_channel_id,
                    files=files,
                    content=None,
                    timeout=upload_timeout,
                )
                with lock:
                    sent_count += len(files)
            except Exception as e:
                rprint(f"[red]Failed to upload {', '.join(p.name for p in files)}: {e}[/red]")
            finally:
                q.task_done()
                time.sleep(max(0.0, delay_seconds))

    max_workers = max(1, int(concurrency))
    threads: List[threading.Thread] = []
    for i in range(max_workers):
        t = threading.Thread(target=_worker, args=(i,), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    if skipped_oversize:
        rprint(f"[yellow]Skipped {skipped_oversize} file(s) due to size over {max_file_mb:.2f} MB[/yellow]")
    rprint(f"[green]Done. Sent {sent_count} file(s).[/green]")


