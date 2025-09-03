from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional, Tuple, List
import threading
from queue import Queue, Empty

from .discord_client import DiscordClient
from .scanner import scan_media


def send_media_job(
    input_dir: Path,
    channel_url: str,
    *,
    token: str,
    token_type: str = "auto",
    post_title: Optional[str] = None,
    post_tag: Optional[str] = None,
    relay_from: Optional[str] = None,
    relay_download_dir: Path = Path(".adms_cache"),
    ignore_dedupe: bool = False,
    dry_run: bool = False,
    history_limit: int = 1000,
    request_timeout: float = 30.0,
    upload_timeout: float = 120.0,
    delay_seconds: float = 1.0,
    max_file_mb: float = 10.0,
    skip_oversize: bool = True,
    cancel_event: Optional[object] = None,
    on_log: Optional[callable] = None,
    on_thread_created: Optional[callable] = None,
    concurrency: int = 1,
) -> str:
    """Headless job used by GUI to perform a single send operation.

    Returns a short human-readable result string.
    """
    def _log(msg: str) -> None:
        try:
            if on_log is not None:
                on_log(msg)
            else:
                logging.info(msg)
        except Exception:
            # Never allow UI logging failures to break the job
            logging.info(msg)

    client = DiscordClient(token=token, token_type=token_type)
    guild_id, channel_id, thread_id = client.parse_ids_from_url(channel_url)
    if channel_id is None:
        raise ValueError("Invalid channel URL. Expected https://discord.com/channels/<guild>/<channel>")

    # If destination is forum/media channel and no thread id is provided, create a thread
    _log(f"Scanning '{input_dir}' and preparing destination...")
    ch = client.get_channel(channel_id, request_timeout=request_timeout)
    ch_type = ch.get("type") if ch else None
    is_forum_like = ch_type in (15, 16) if ch is not None else False
    target_channel_id = channel_id
    if is_forum_like and thread_id is None:
        title = post_title or Path(input_dir).name
        applied_tag_ids = None
        if ch and post_tag:
            tag_l = post_tag.strip().lower()
            for t in ch.get("available_tags", []):
                if str(t.get("name", "")).lower() == tag_l:
                    applied_tag_ids = [t.get("id")]
                    break
        _log(f"Creating new post in forum/media channel: title='{title}' tag='{post_tag or ''}'")
        new_thread_id = client.start_forum_post(channel_id, title, content=title, applied_tag_ids=applied_tag_ids)
        if not new_thread_id:
            raise RuntimeError("Failed to create post thread")
        target_channel_id = new_thread_id
        # Inform caller about the created thread so UI/clients can update URLs
        try:
            if on_thread_created is not None:
                # Prefer the compact /channels/<guild>/<channel>/<thread> form
                new_thread_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{new_thread_id}"
                on_thread_created(new_thread_url)
        except Exception:
            # Never let callback issues break the flow
            pass
    elif thread_id is not None:
        target_channel_id = thread_id

    if relay_from:
        _g, _c, _t = client.parse_ids_from_url(relay_from)
        source_id = _t or _c
        if source_id is None:
            raise ValueError("Invalid relay_from URL")
        _log(f"Relaying media from {relay_from} -> target thread/channel...")
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
        _log(f"Relay complete. Sent {sent}, skipped {skipped}.")
        return f"Relayed: sent={sent}, skipped={skipped}"

    _log("Scanning input directory for media...")
    scan = scan_media(input_dir)
    if not ignore_dedupe:
        _log("Fetching recent filenames for dedupe...")
        # IMPORTANT: dedupe must use the actual destination (thread if created/provided)
        existing = client.fetch_existing_filenames(target_channel_id, max_messages=history_limit, request_timeout=request_timeout)
        scan = scan.filter_against_filenames(existing)
    _log(f"Found {len(scan.pairs)} pair(s) and {len(scan.singles)} single(s) after dedupe.")

    sent_count = 0
    skipped_oversize = 0
    bytes_limit = int(max_file_mb * 1024 * 1024)
    def _should_cancel() -> bool:
        try:
            return bool(cancel_event and getattr(cancel_event, "is_set", lambda: False)())
        except Exception:
            return False

    # Build tasks upfront, applying size checks
    Task = List[Path]
    tasks: List[Task] = []

    for pair in scan.pairs:
        if _should_cancel():
            return f"Cancelled after sending {sent_count} file(s)"
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
        if _should_cancel():
            return f"Cancelled after sending {sent_count} file(s)"
        size_ok = single.path.stat().st_size <= bytes_limit
        if not size_ok and skip_oversize:
            skipped_oversize += 1
            continue
        tasks.append([single.path])

    # If dry-run, just report planned actions
    if dry_run:
        _log(f"Dry run: {len(tasks)} message(s) would be sent. Skipped {skipped_oversize} oversize file(s).")
        return f"Dry run. Planned {len(tasks)} message(s). Skipped {skipped_oversize} oversize file(s)."

    # Bounded concurrent uploader using worker threads and a task queue
    q: Queue = Queue()
    for t in tasks:
        q.put(t)

    lock = threading.Lock()

    def _worker(worker_index: int) -> None:
        nonlocal sent_count
        while True:
            if _should_cancel():
                break
            try:
                files: List[Path] = q.get_nowait()
            except Empty:
                break
            try:
                _log(f"Uploading: {', '.join(p.name for p in files)}")
                client.send_message_with_files(
                    channel_id=target_channel_id,
                    files=files,
                    content=None,
                    timeout=upload_timeout,
                )
                with lock:
                    sent_count += len(files)
            except Exception as e:
                _log(f"Failed to upload {', '.join(p.name for p in files)}: {e}")
            finally:
                q.task_done()
                # Per-message delay to avoid hammering the API
                time.sleep(max(0.0, delay_seconds))

    max_workers = max(1, int(concurrency))
    threads: List[threading.Thread] = []
    for i in range(max_workers):
        t = threading.Thread(target=_worker, args=(i,), daemon=True)
        threads.append(t)
        t.start()
    # Wait for all tasks to finish or cancellation
    for t in threads:
        t.join()

    if skipped_oversize:
        _log(f"Finished. Sent {sent_count}, skipped {skipped_oversize} oversize file(s).")
        return f"Done. Sent {sent_count}, skipped {skipped_oversize} oversize file(s)."
    _log(f"Finished. Sent {sent_count} file(s).")
    return f"Done. Sent {sent_count} file(s)."


