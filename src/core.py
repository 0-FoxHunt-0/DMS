from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Optional, Tuple, List, Set
import json
import threading
from queue import Queue, Empty

from .discord_client import DiscordClient
from .discord_client import DiscordAuthError
from .scanner import scan_media, _variants
from .scanner import VIDEO_EXTS, GIF_EXTS, IMAGE_EXTS, ScanResult
from .scanner import detect_remote_duplicates
from .logging_utils import start_thread_log, sanitize_for_filename


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
    segment_separators: bool = True,
    separator_text: str = "----------------------------------------",
    # Accepted but not used directly here; consumed by GUI flow prior to invoking this function
    prepend_enabled: bool = False,
    prepend_text: str = "",
    media_types: Optional[List[str]] = None,
    ignore_segmentation: bool = False,
    only_root_level: bool = False,
    logger: Optional[logging.Logger] = None,
    run_dir: Optional[Path] = None,
    confirm_dupe_removal: Optional[callable] = None,
) -> str:
    """Headless job used by GUI to perform a single send operation.

    Returns a short human-readable result string.
    """
    current_logger: Optional[logging.Logger] = logger
    dedupe_logger: Optional[logging.Logger] = None

    def _log(msg: str) -> None:
        # Always echo to UI if provided
        try:
            if on_log is not None:
                on_log(msg)
        except Exception:
            pass
        # And persist to file logs
        try:
            if current_logger is not None:
                current_logger.info(msg)
            else:
                logging.info(msg)
        except Exception:
            logging.info(msg)

    client = DiscordClient(token=token, token_type=token_type)
    guild_id, channel_id, thread_id = client.parse_ids_from_url(channel_url)
    if channel_id is None:
        raise ValueError("Invalid channel URL. Expected https://discord.com/channels/<guild>/<channel>")

    # Initialize dedicated dedupe logger under run directory
    try:
        if run_dir is not None:
            dedupe_logger = start_thread_log(run_dir, "dedupe")
            try:
                dedupe_logger.info("[dedupe] logger initialized")
            except Exception:
                pass
    except Exception:
        dedupe_logger = None

    # If destination is forum/media channel and no thread id is provided, create a thread
    _log(f"Scanning '{input_dir}' and preparing destination...")
    try:
        _log(f"[core] fetching channel info for {channel_id}")
        ch = client.get_channel(channel_id, request_timeout=request_timeout)
    except DiscordAuthError as e:
        _log(f"Authentication error: {e}")
        return "Aborted: authentication failed (401/403)"
    ch_type = ch.get("type") if ch else None
    is_forum_like = ch_type in (15, 16) if ch is not None else False
    target_channel_id = channel_id
    if is_forum_like and thread_id is None:
        if dry_run:
            _log("Dry run: skipping thread creation; will dedupe against parent channel if enabled.")
            # Keep target_channel_id as parent channel for dry-run
        else:
            title = post_title or Path(input_dir).name
            applied_tag_ids = None
            if ch and post_tag:
                tag_l = post_tag.strip().lower()
                for t in ch.get("available_tags", []):
                    if str(t.get("name", "")).lower() == tag_l:
                        applied_tag_ids = [t.get("id")]
                        break
            _log(f"[core] thread lookup: trying existing title='{title}' tag='{post_tag or ''}'")
            try:
                existing_id = client.find_existing_thread_by_name(channel_id, title, request_timeout=request_timeout, guild_id=guild_id)
            except Exception as e:
                existing_id = None
                _log(f"[core] thread lookup error: {e}")
            if existing_id:
                target_channel_id = existing_id
                thread_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{existing_id}"
                _log(f"[core] using existing thread: id={existing_id} title='{title}' url={thread_url}")
                # Attach per-thread logger when possible
                try:
                    if run_dir is not None:
                        key = f"thread-{sanitize_for_filename(title)}-{str(existing_id)[:8]}"
                        tl = start_thread_log(run_dir, key)
                        # switch current logger for remainder of job
                        current_logger = tl
                except Exception:
                    pass
            else:
                _log(f"[core] creating new thread: title='{title}' tag='{post_tag or ''}'")
                new_thread_id = client.start_forum_post(channel_id, title, content=title, applied_tag_ids=applied_tag_ids)
                if not new_thread_id:
                    raise RuntimeError("Failed to create post thread")
                target_channel_id = new_thread_id
                thread_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{new_thread_id}"
                _log(f"[core] created thread: id={new_thread_id} title='{title}' url={thread_url}")
                # Attach per-thread logger for remainder of job
                try:
                    if run_dir is not None:
                        key = f"thread-{sanitize_for_filename(title)}-{str(new_thread_id)[:8]}"
                        tl = start_thread_log(run_dir, key)
                        current_logger = tl
                except Exception:
                    pass
                # Inform caller about the created thread so UI/clients can update URLs
                try:
                    if on_thread_created is not None:
                        # Prefer the compact /channels/<guild>/<channel>/<thread> form
                        new_thread_url = thread_url
                        on_thread_created(new_thread_url)
                except Exception:
                    # Never let callback issues break the flow
                    pass
    elif thread_id is not None:
        target_channel_id = thread_id
        # Known thread supplied; attach per-thread logger if run_dir present
        try:
            if run_dir is not None:
                key = f"thread-{str(thread_id)[:8]}"
                tl = start_thread_log(run_dir, key)
                current_logger = tl
                _log(f"[core] using provided thread id={thread_id} url=https://discord.com/channels/{guild_id}/{channel_id}/{thread_id}")
        except Exception:
            pass

    if relay_from:
        _g, _c, _t = client.parse_ids_from_url(relay_from)
        source_id = _t or _c
        if source_id is None:
            raise ValueError("Invalid relay_from URL")
        _log(f"Relaying media from {relay_from} -> target thread/channel...")
        try:
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
        except DiscordAuthError as e:
            _log(f"Authentication error during relay: {e}")
            return "Aborted: authentication failed (401/403)"
        _log(f"Relay complete. Sent {sent}, skipped {skipped}.")
        return f"Relayed: sent={sent}, skipped={skipped}"

    _log("Scanning input directory for media...")
    scan = scan_media(input_dir)
    # Track duplicates detected for end-of-run summary
    duplicates_detected: List[str] = []
    # Track remote dupe report for JSON/prompt
    remote_dupe_report = None

    if not ignore_dedupe:
        _log("Fetching recent filenames for dedupe...")
        try:
            # IMPORTANT: dedupe must use the actual destination (thread if created/provided)
            remote_existing = client.fetch_existing_filenames(
                target_channel_id, max_messages=history_limit, request_timeout=request_timeout
            )
        except DiscordAuthError as e:
            _log(f"Authentication error during dedupe: {e}")
            return "Aborted: authentication failed (401/403)"
        except Exception as e:
            remote_existing = set()
            _log(f"Warning: dedupe fetch failed, proceeding without dedupe: {e}")

        # Use remote history set for dedupe
        existing_set = set(remote_existing)

        # New: report the dedupe catalog size and local cache contribution
        try:
            _log(f"Dedupe catalog size: {len(existing_set)} filename(s) (remote)")
            if dedupe_logger is not None:
                dedupe_logger.info(f"[dedupe] remote={len(remote_existing)}")
        except Exception:
            pass

        # Get deduplication diagnostics from scanner
        # Use scan_before (unfiltered) to capture ALL duplicates for upload_dupes JSON
        try:
            diagnostics = scan_before.get_dedupe_diagnostics(existing_set)
            planned_names = diagnostics["planned_names"]
            planned_variants = diagnostics["planned_variants"]
            existing_variants = diagnostics["existing_variants"]
            hits = diagnostics["hits"]
            duplicates_detected = diagnostics["duplicates"]
            
            _log(f"Dedupe pre-filter: {hits} of {len(planned_names)} filename(s) match local+remote")
            if hits == 0 and dedupe_logger is not None:
                try:
                    # Log a small sample of decoded remote names for debugging
                    sample_remote = sorted(list(existing_variants))[:10]
                    dedupe_logger.info(f"[dedupe] sample remote names: {', '.join(sample_remote)}")
                    sample_planned = sorted(list(planned_variants))[:10]
                    dedupe_logger.info(f"[dedupe] sample planned names: {', '.join(sample_planned)}")
                except Exception:
                    pass
            if dedupe_logger is not None:
                try:
                    dedupe_logger.info(f"[dedupe] planned={len(planned_names)} hits={len(duplicates_detected)}")
                except Exception:
                    pass
        except Exception:
            pass

        # Apply filter
        scan_before = scan
        # Expand existing_set to include all variants before filtering
        existing_expanded: Set[str] = set()
        for n in existing_set:
            for v in _variants(n):
                existing_expanded.add(v)
        scan = scan.filter_against_filenames(existing_expanded)
        try:
            before_count = len(scan_before.pairs) * 2 + len(scan_before.singles)
            after_count = len(scan.pairs) * 2 + len(scan.singles)
            removed = max(0, before_count - after_count)
            _log(f"Dedupe post-filter: removed {removed} attachment(s); remaining {after_count}")
            if dedupe_logger is not None:
                try:
                    dedupe_logger.info(f"[dedupe] post-filter removed={removed} remaining={after_count}")
                except Exception:
                    pass
        except Exception:
            pass
    # Detect remote duplicates in the destination thread/channel
    # Pass unfiltered scan (scan_before) so detection sees all planned files, not just those remaining after upload dedupe
    try:
        remote_dupe_report = detect_remote_duplicates(
            client, target_channel_id, scan_before if not ignore_dedupe else scan, max_messages=history_limit, request_timeout=request_timeout
        )
    except Exception as e:
        remote_dupe_report = None
        try:
            _log(f"Warning: remote dupe detection failed: {e}")
        except Exception:
            pass

    # If remote dupes exist, write dupes.json entry and prompt for removal
    def _write_dupes_json(upload_dupes_list: List[str]) -> None:
        try:
            base_dir = run_dir or Path("logs")
            base_dir.mkdir(parents=True, exist_ok=True)
            fp = base_dir / "dupes.json"
            existing: dict = {}
            if fp.exists():
                try:
                    with fp.open("r", encoding="utf-8") as f:
                        existing = json.load(f) or {}
                except Exception:
                    existing = {}
            remote_list = existing.get("remote_dupes") or []
            upload_list = existing.get("upload_dupes") or []
            # Prepare remote entry (if any)
            if remote_dupe_report is not None and getattr(remote_dupe_report, "groups", None):
                rem_entry = {
                    "thread_id": remote_dupe_report.thread_id,
                    "thread_name": remote_dupe_report.thread_name,
                    "groups": [
                        {
                            "filename": g.filename,
                            "keep": (g.messages[0].id if g.messages else None),
                            "to_delete": [m.id for m in (g.messages[1:] if len(g.messages) > 1 else [])],
                            "messages": [
                                {
                                    "id": m.id,
                                    "timestamp": m.timestamp,
                                    "filenames": m.filenames,
                                    "embed_urls": m.embed_urls,
                                }
                                for m in g.messages
                            ],
                        }
                        for g in remote_dupe_report.groups
                    ],
                }
                remote_list.append(rem_entry)
            # Prepare upload entry (if any)
            if upload_dupes_list:
                upl_entry = {
                    "thread_id": remote_dupe_report.thread_id if remote_dupe_report else target_channel_id,
                    "thread_name": (remote_dupe_report.thread_name if remote_dupe_report else None),
                    "duplicates": sorted(list(set(upload_dupes_list))),
                }
                upload_list.append(upl_entry)
            data = {"remote_dupes": remote_list, "upload_dupes": upload_list}
            with fp.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # If we found any remote dupes, notify and optionally delete
    if remote_dupe_report is not None and remote_dupe_report.groups:
        # Write JSON record including current upload_dupes (if computed already)
        _write_dupes_json(duplicates_detected)
        thread_display = remote_dupe_report.thread_name or str(target_channel_id)
        _log(f"Duped files have been detected in ({thread_display})\nPlease refer to the logs for the list")
        do_delete = False
        try:
            if callable(confirm_dupe_removal):
                do_delete = bool(confirm_dupe_removal(thread_display))
        except Exception:
            do_delete = False
        if do_delete:
            # Delete all but oldest per group
            deleted = 0
            for g in remote_dupe_report.groups:
                if not g.messages or len(g.messages) < 2:
                    continue
                for m in g.messages[1:]:
                    try:
                        if client.delete_message(target_channel_id, m.id, request_timeout=request_timeout):
                            deleted += 1
                    except DiscordAuthError as e:
                        _log(f"Authentication error during dupe deletion: {e}")
                        return "Aborted: authentication failed (401/403)"
                    except Exception as e:
                        _log(f"Warning: failed to delete message {m.id}: {e}")
            _log(f"Deleted {deleted} duplicate message(s) on remote (kept oldest per file).")
            
            # Collect only the verified duplicate filenames (normalized, from remote)
            # These are the files we should NOT re-upload
            remote_dupe_keys: Set[str] = set()
            for g in remote_dupe_report.groups:
                remote_dupe_keys.add(g.filename.lower())  # normalized remote filename
            
            # After deletion, refresh dedupe sets and re-filter from original scan
            try:
                if not ignore_dedupe:
                    remote_existing2 = client.fetch_existing_filenames(
                        target_channel_id, max_messages=history_limit, request_timeout=request_timeout
                    )
                    existing_expanded2: Set[str] = set()
                    for n in remote_existing2:
                        for v in _variants(n):
                            existing_expanded2.add(v)
                    # Re-run from the original pre-filter scan (if available)
                    try:
                        scan = scan_before.filter_against_filenames(existing_expanded2)  # type: ignore[name-defined]
                    except Exception:
                        # If scan_before not defined (ignore_dedupe True earlier), keep current scan
                        pass
                    
                    # Exclude only verified duplicates using variant matching
                    # This prevents re-uploading files that were just deleted as duplicates
                    # Note: Messages can contain multiple files (clusters); only exclude the actual dupes
                    def _is_dupe(local_filename: str) -> bool:
                        # Check if any variant of this local name matches a remote dupe
                        for variant in _variants(local_filename):
                            if variant in remote_dupe_keys:
                                return True
                        return False
                    
                    scan = ScanResult(
                        pairs=[p for p in scan.pairs if not _is_dupe(p.mp4_path.name) and not _is_dupe(p.gif_path.name)],
                        singles=[s for s in scan.singles if not _is_dupe(s.path.name)]
                    )
            except Exception as e:
                _log(f"Warning: failed to refresh dedupe after deletion: {e}")

    _log(f"Found {len(scan.pairs)} pair(s) and {len(scan.singles)} single(s) after dedupe.")
    _log(f"[core] uploads target channel/thread id={target_channel_id}")

    # Optional filter: restrict to root-level files only (exclude subfolders)
    if only_root_level:
        def _is_root(root_key: str) -> bool:
            try:
                return (root_key.split("/", 1)[0] == ".")
            except Exception:
                return False
        scan = ScanResult(
            pairs=[p for p in scan.pairs if _is_root(p.root_key)],
            singles=[s for s in scan.singles if _is_root(s.root_key)],
        )

    # Determine selected media categories
    selected = set((mt or '').strip().lower() for mt in (media_types or []))
    if not selected or 'all' in selected:
        selected = {'videos', 'gifs', 'images'}

    def _is_ext_selected(ext: str) -> bool:
        if ext in VIDEO_EXTS:
            return 'videos' in selected
        if ext in GIF_EXTS:
            return 'gifs' in selected
        if ext in IMAGE_EXTS:
            return 'images' in selected
        return False

    sent_count = 0
    skipped_oversize = 0
    bytes_limit = int(max_file_mb * 1024 * 1024)
    def _should_cancel() -> bool:
        try:
            return bool(cancel_event and getattr(cancel_event, "is_set", lambda: False)())
        except Exception:
            return False

    # Build items with size checks and keep track of root_keys
    from typing import Tuple as _Tuple
    TaskItem = _Tuple[str, List[Path]]  # (root_key, files)
    items: List[TaskItem] = []

    for pair in scan.pairs:
        if _should_cancel():
            return f"Cancelled after sending {sent_count} file(s)"
        mp4_ok = pair.mp4_path.stat().st_size <= bytes_limit
        gif_ok = pair.gif_path.stat().st_size <= bytes_limit
        files_to_send: List[Path] = []
        if _is_ext_selected(pair.mp4_path.suffix.lower()):
            if mp4_ok or not skip_oversize:
                files_to_send.append(pair.mp4_path)
            else:
                skipped_oversize += 1
        else:
            # Not selected -> skip
            pass
        if _is_ext_selected(pair.gif_path.suffix.lower()):
            if gif_ok or not skip_oversize:
                files_to_send.append(pair.gif_path)
            else:
                skipped_oversize += 1
        else:
            # Not selected -> skip
            pass
        if files_to_send:
            items.append((pair.root_key, files_to_send))

    for single in scan.singles:
        if _should_cancel():
            return f"Cancelled after sending {sent_count} file(s)"
        # Filter by selected categories
        if not _is_ext_selected(single.path.suffix.lower()):
            continue
        size_ok = single.path.stat().st_size <= bytes_limit
        if not size_ok and skip_oversize:
            skipped_oversize += 1
            continue
        items.append((single.root_key, [single.path]))

    # If dry-run, just report planned actions
    if dry_run:
        _log(f"Dry run: {len(items)} message(s) would be sent. Skipped {skipped_oversize} oversize file(s).")
        return f"Dry run. Planned {len(items)} message(s). Skipped {skipped_oversize} oversize file(s)."

    # Determine segmented groups
    from collections import Counter, defaultdict
    counts = Counter(rk for rk, _files in items)
    segmented_keys = set() if ignore_segmentation else {rk for rk, c in counts.items() if c > 1}
    try:
        if segmented_keys:
            _log(f"[core] segmented groups detected: {', '.join(sorted(segmented_keys))}")
    except Exception:
        pass

    if segment_separators and segmented_keys:
        # Aggregate segments per root key and send up to 10 attachments per message
        MAX_ATTACHMENTS = 10
        started_group: set[str] = set()
        pending_for_key: dict[str, List[Path]] = defaultdict(list)
        remaining_segments: dict[str, int] = {rk: counts[rk] for rk in segmented_keys}

        def _flush(rk_local: str) -> None:
            nonlocal sent_count
            pending = pending_for_key.get(rk_local) or []
            if not pending:
                return
            try:
                _log(f"Uploading: {', '.join(p.name for p in pending)}")
                client.send_message_with_files(channel_id=target_channel_id, files=pending, content=None, timeout=upload_timeout)
                sent_count += len(pending)
            except DiscordAuthError as e:
                _log(f"Authentication error while uploading: {e}")
                raise
            except Exception as e:
                _log(f"Failed to upload {', '.join(p.name for p in pending)}: {e}")
            finally:
                pending_for_key[rk_local] = []
                time.sleep(max(0.0, delay_seconds))

        for rk, files in items:
            if _should_cancel():
                break
            if rk in segmented_keys:
                # Leading separator once per group
                if rk not in started_group:
                    try:
                        last = client.get_last_message_content(target_channel_id, request_timeout=request_timeout)
                    except DiscordAuthError as e:
                        _log(f"Authentication error while checking last message: {e}")
                        return "Aborted: authentication failed (401/403)"
                    except Exception as e:
                        last = None
                        _log(f"Warning: failed to check last message: {e}")
                    if last != separator_text:
                        try:
                            client.send_text_message(target_channel_id, separator_text, timeout=request_timeout)
                        except DiscordAuthError as e:
                            _log(f"Authentication error while sending separator: {e}")
                            return "Aborted: authentication failed (401/403)"
                        except Exception as e:
                            _log(f"Warning: failed to send separator for {rk}: {e}")
                        time.sleep(max(0.0, delay_seconds))
                    started_group.add(rk)

                # Add this segment's files; flush if it would exceed attachment limit
                current = pending_for_key[rk]
                if current and (len(current) + len(files) > MAX_ATTACHMENTS):
                    try:
                        _flush(rk)
                    except DiscordAuthError:
                        return "Aborted: authentication failed (401/403)"
                pending_for_key[rk].extend(files)
                remaining_segments[rk] = max(0, remaining_segments.get(rk, 0) - 1)
                # If we reached limit or this was the last segment -> flush
                if len(pending_for_key[rk]) >= MAX_ATTACHMENTS or remaining_segments[rk] == 0:
                    try:
                        _flush(rk)
                    except DiscordAuthError:
                        return "Aborted: authentication failed (401/403)"
                    # If this was the last segment for this group, send trailing separator
                    if remaining_segments[rk] == 0:
                        try:
                            last = client.get_last_message_content(target_channel_id, request_timeout=request_timeout)
                        except DiscordAuthError as e:
                            _log(f"Authentication error while checking last message: {e}")
                            return "Aborted: authentication failed (401/403)"
                        except Exception as e:
                            last = None
                            _log(f"Warning: failed to check last message: {e}")
                        if last != separator_text:
                            try:
                                client.send_text_message(target_channel_id, separator_text, timeout=request_timeout)
                            except DiscordAuthError as e:
                                _log(f"Authentication error while sending separator: {e}")
                                return "Aborted: authentication failed (401/403)"
                            except Exception as e:
                                _log(f"Warning: failed to send separator for {rk}: {e}")
                            time.sleep(max(0.0, delay_seconds))
            else:
                # Non-segmented: send as-is (pairs together)
                try:
                    _log(f"Uploading: {', '.join(p.name for p in files)}")
                    client.send_message_with_files(channel_id=target_channel_id, files=files, content=None, timeout=upload_timeout)
                    sent_count += len(files)
                except DiscordAuthError as e:
                    _log(f"Authentication error while uploading: {e}")
                    return "Aborted: authentication failed (401/403)"
                except Exception as e:
                    _log(f"Failed to upload {', '.join(p.name for p in files)}: {e}")
                finally:
                    time.sleep(max(0.0, delay_seconds))
    else:
        # Bounded concurrent uploader using worker threads and a task queue
        q: Queue = Queue()
        for _rk, files in items:
            q.put(files)

        lock = threading.Lock()

        # Shared signal to stop workers on fatal auth errors
        stop_all = threading.Event()

        def _worker(worker_index: int) -> None:
            nonlocal sent_count
            while True:
                if _should_cancel() or stop_all.is_set():
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
                except DiscordAuthError as e:
                    _log(f"Authentication error while uploading: {e}")
                    # Signal all workers to stop and drain the queue
                    stop_all.set()
                    # Re-raise to be handled by outer join
                    raise
                except Exception as e:
                    _log(f"Failed to upload {', '.join(p.name for p in files)}: {e}")
                finally:
                    q.task_done()
                    # Per-message delay to avoid hammering the API
                    time.sleep(max(0.0, delay_seconds))

        max_workers = max(1, int(concurrency))
        threads: List[threading.Thread] = []
        worker_errors: List[BaseException] = []
        for i in range(max_workers):
            def _wrap(idx: int):
                def _run():
                    try:
                        _worker(idx)
                    except Exception as ex:
                        worker_errors.append(ex)
                return _run
            t = threading.Thread(target=_wrap(i), daemon=True)
            threads.append(t)
            t.start()
        # Wait for all tasks to finish or cancellation
        for t in threads:
            t.join()
        if worker_errors:
            return "Aborted: authentication failed (401/403)"

    # Emit end-of-run dedupe summary counts only (details recorded in dupes.json)
    try:
        if dedupe_logger is not None and not ignore_dedupe:
            dedupe_logger.info("[dedupe] summary start")
            dedupe_logger.info(f"[dedupe] duplicates detected: count={len(duplicates_detected)}")
            dedupe_logger.info("[dedupe] summary end")
    except Exception:
        pass

    # Persist upload duplicates into dupes.json even when no remote dupes were found
    try:
        if not ignore_dedupe and duplicates_detected:
            # Reuse helper defined above
            _write_dupes_json(duplicates_detected)  # type: ignore[misc]
    except Exception:
        pass

    if skipped_oversize:
        _log(f"Finished. Sent {sent_count}, skipped {skipped_oversize} oversize file(s).")
        return f"Done. Sent {sent_count}, skipped {skipped_oversize} oversize file(s)."
    _log(f"Finished. Sent {sent_count} file(s).")
    return f"Done. Sent {sent_count} file(s)."


