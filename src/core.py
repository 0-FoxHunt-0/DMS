from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional, Tuple

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
        existing = client.fetch_existing_filenames(channel_id, max_messages=history_limit, request_timeout=request_timeout)
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

    for pair in scan.pairs:
        if _should_cancel():
            return f"Cancelled after sending {sent_count} file(s)"
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
        try:
            _log(f"Uploading pair: {', '.join(p.name for p in files_to_send)}")
            client.send_message_with_files(
                channel_id=target_channel_id,
                files=files_to_send,
                content=None,
                timeout=upload_timeout,
            )
            sent_count += len(files_to_send)
        except Exception as e:
            _log(f"Failed to upload pair '{pair.root_key}': {e}")
        time.sleep(max(0.0, delay_seconds))

    for single in scan.singles:
        if _should_cancel():
            return f"Cancelled after sending {sent_count} file(s)"
        size_ok = single.path.stat().st_size <= bytes_limit
        if not size_ok and skip_oversize:
            skipped_oversize += 1
            continue
        try:
            _log(f"Uploading single: {single.path.name}")
            client.send_message_with_files(
                channel_id=target_channel_id,
                files=[single.path],
                content=None,
                timeout=upload_timeout,
            )
            sent_count += 1
        except Exception as e:
            _log(f"Failed to upload '{single.path.name}': {e}")
        time.sleep(max(0.0, delay_seconds))

    if skipped_oversize:
        _log(f"Finished. Sent {sent_count}, skipped {skipped_oversize} oversize file(s).")
        return f"Done. Sent {sent_count}, skipped {skipped_oversize} oversize file(s)."
    _log(f"Finished. Sent {sent_count} file(s).")
    return f"Done. Sent {sent_count} file(s)."


