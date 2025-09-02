import time
from pathlib import Path
from typing import List, Tuple, Optional

import typer
from rich import print as rprint
from rich.table import Table

from .discord_client import DiscordClient
from .scanner import ScanResult, scan_media
from .config import load_env, set_env_var


load_env()

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


@app.command()
def send(
    input_dir: Path = typer.Argument(..., exists=True, file_okay=False, dir_okay=True, readable=True),
    channel_url: str = typer.Argument(..., help="Discord channel URL like https://discord.com/channels/<guild>/<channel>"),
    token: Optional[str] = typer.Option(
        None,
        help="Discord token; can be set via DISCORD_TOKEN or stored in .env",
        envvar="DISCORD_TOKEN",
    ),
    token_type: str = typer.Option("bot", help="Token type: 'bot' or 'user'"),
    ignore_dedupe: bool = typer.Option(False, help="Ignore channel history and send all files"),
    dry_run: bool = typer.Option(False, help="Print actions without uploading"),
    history_limit: int = typer.Option(1000, help="Max messages to scan for dedupe"),
    request_timeout: float = typer.Option(30.0, help="HTTP request timeout (seconds) for history"),
    upload_timeout: float = typer.Option(120.0, help="HTTP request timeout (seconds) for uploads"),
    delay_seconds: float = typer.Option(1.0, help="Delay between messages (seconds)"),
    max_file_mb: float = typer.Option(10.0, help="Skip files larger than this size (MB), unless overridden"),
    skip_oversize: bool = typer.Option(True, help="Skip files exceeding max_file_mb instead of attempting upload"),
) -> None:
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
    channel_id = client.parse_channel_id_from_url(channel_url)
    if channel_id is None:
        rprint("[red]Invalid channel URL. Expected https://discord.com/channels/<guild>/<channel>[/red]")
        raise typer.Exit(code=2)

    rprint(f"[bold]Scanning:[/bold] {input_dir}")
    scan = scan_media(input_dir)
    _print_plan(scan)

    if not ignore_dedupe:
        rprint("[bold]Fetching existing filenames from channel for dedupe...[/bold]")
        existing = client.fetch_existing_filenames(channel_id, max_messages=history_limit, request_timeout=request_timeout)
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
    # Send pairs first, then singles
    sent_count = 0
    skipped_oversize = 0
    bytes_limit = int(max_file_mb * 1024 * 1024)
    for pair in scan.pairs:
        # size checks
        mp4_ok = pair.mp4_path.stat().st_size <= bytes_limit
        gif_ok = pair.gif_path.stat().st_size <= bytes_limit
        files_to_send = []
        if mp4_ok or not skip_oversize:
            files_to_send.append(pair.mp4_path)
        else:
            skipped_oversize += 1
        if gif_ok or not skip_oversize:
            files_to_send.append(pair.gif_path)
        else:
            skipped_oversize += 1
        if not files_to_send:
            continue
        client.send_message_with_files(
            channel_id=channel_id,
            files=files_to_send,
            content=pair.root_key,
            timeout=upload_timeout,
        )
        sent_count += len(files_to_send)
        time.sleep(max(0.0, delay_seconds))

    for single in scan.singles:
        size_ok = single.path.stat().st_size <= bytes_limit
        if not size_ok and skip_oversize:
            skipped_oversize += 1
            continue
        client.send_message_with_files(
            channel_id=channel_id,
            files=[single.path],
            content=single.root_key,
            timeout=upload_timeout,
        )
        sent_count += 1
        time.sleep(max(0.0, delay_seconds))

    if skipped_oversize:
        rprint(f"[yellow]Skipped {skipped_oversize} file(s) due to size over {max_file_mb:.2f} MB[/yellow]")
    rprint(f"[green]Done. Sent {sent_count} file(s).[/green]")


