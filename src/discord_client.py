import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple

import requests


DISCORD_API = "https://discord.com/api/v10"


def _auth_header(token: str, token_type: str) -> str:
    if token_type.lower() == "bot":
        return f"Bot {token}"
    return token


_CDN_FILENAME_RE = re.compile(r"/([A-Za-z0-9_\-\.]+\.(?:mp4|gif))(?:\?|$)", re.IGNORECASE)


@dataclass
class DiscordClient:
    token: str
    token_type: str = "auto"
    user_agent: str = "AutoDisMediaSend (https://github.com/0-FoxHunt-0/disdrop, 1.0)"
    _resolved_token_type: Optional[str] = field(default=None, init=False, repr=False)

    def _headers(self) -> dict:
        self._ensure_token_type()
        resolved = self._resolved_token_type or self.token_type
        return {
            "Authorization": _auth_header(self.token, resolved),
            "User-Agent": self.user_agent,
        }

    def _ensure_token_type(self) -> None:
        if self._resolved_token_type is not None:
            return
        if self.token_type != "auto":
            self._resolved_token_type = self.token_type
            return
        # Try resolving by calling /users/@me
        test_url = f"{DISCORD_API}/users/@me"
        try:
            # Try bot style first
            r = requests.get(test_url, headers={"Authorization": f"Bot {self.token}", "User-Agent": self.user_agent}, timeout=10)
            if r.status_code == 200:
                self._resolved_token_type = "bot"
                return
        except requests.RequestException:
            pass
        try:
            r = requests.get(test_url, headers={"Authorization": self.token, "User-Agent": self.user_agent}, timeout=10)
            if r.status_code == 200:
                self._resolved_token_type = "user"
                return
        except requests.RequestException:
            pass
        # Fallback to bot if undetermined
        self._resolved_token_type = "bot"

    @staticmethod
    def parse_channel_id_from_url(channel_url: str) -> Optional[str]:
        # https://discord.com/channels/<guild>/<channel>
        m = re.search(r"discord\.com/channels/\d+/(\d+)", channel_url)
        if not m:
            return None
        return m.group(1)

    @staticmethod
    def parse_ids_from_url(channel_url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Return (guild_id, channel_id, thread_id) parsed from a Discord URL, if present.

        Supports both forms:
        - /channels/<guild>/<channel>/<thread>
        - /channels/<guild>/<channel>/threads/<thread>
        """
        m = re.search(r"discord\.com/channels/(\d+)/(\d+)(?:/(?:threads/)?(\d+))?", channel_url)
        if not m:
            return None, None, None
        guild_id = m.group(1)
        channel_id = m.group(2)
        thread_id = m.group(3) if m.lastindex and m.lastindex >= 3 else None
        return guild_id, channel_id, thread_id

    def get_channel(self, channel_id: str, request_timeout: float = 30.0) -> Optional[dict]:
        url = f"{DISCORD_API}/channels/{channel_id}"
        resp = self._request_with_retries("GET", url, timeout=request_timeout)
        if resp is None or not (200 <= resp.status_code < 300):
            return None
        try:
            return resp.json()
        except Exception:
            return None

    def start_forum_post(
        self,
        parent_channel_id: str,
        title: str,
        content: Optional[str] = None,
        request_timeout: float = 30.0,
        applied_tag_ids: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Create a new forum/media post thread under a parent channel. Returns thread ID or None."""
        url = f"{DISCORD_API}/channels/{parent_channel_id}/threads"
        # Use title when content is empty to avoid empty-message rejections on some forums
        payload: dict = {"name": title, "message": {"content": (content or title)}}
        if applied_tag_ids:
            payload["applied_tags"] = applied_tag_ids
        resp = self._request_with_retries("POST", url, json=payload, timeout=request_timeout)
        if resp is None:
            print("[DEBUG] Thread creation: no response from Discord API")
            return None
        if not (200 <= resp.status_code < 300):
            try:
                details = resp.json()
            except Exception:
                details = resp.text
            import logging
            logging.error(f"Thread creation failed: {resp.status_code} {details}")
            return None
        try:
            data = resp.json()
            return data.get("id")
        except Exception as e:
            import logging
            logging.error(f"Thread creation parse error: {e}")
            return None

    def fetch_existing_filenames(self, channel_id: str, max_messages: int = 1000, request_timeout: float = 30.0) -> Set[str]:
        existing: Set[str] = set()
        url = f"{DISCORD_API}/channels/{channel_id}/messages"
        params = {"limit": 100}
        last_id: Optional[str] = None
        fetched = 0

        while fetched < max_messages:
            if last_id:
                params["before"] = last_id
            resp = self._request_with_retries("GET", url, params=params, timeout=request_timeout)
            if resp is None:
                break
            messages = resp.json()
            if not messages:
                break
            for msg in messages:
                # attachments
                for att in msg.get("attachments", []):
                    fn = att.get("filename") or self._extract_filename_from_url(att.get("url", ""))
                    if fn:
                        existing.add(fn)
                # embeds (image/video URLs)
                for emb in msg.get("embeds", []):
                    url_fields = [emb.get("url"), emb.get("thumbnail", {}).get("url"), emb.get("video", {}).get("url"), emb.get("image", {}).get("url")]
                    for u in url_fields:
                        if not u:
                            continue
                        fn = self._extract_filename_from_url(u)
                        if fn:
                            existing.add(fn)
            fetched += len(messages)
            last_id = messages[-1]["id"]

        return existing

    @staticmethod
    def _extract_filename_from_url(url: str) -> Optional[str]:
        m = _CDN_FILENAME_RE.search(url)
        if m:
            return m.group(1)
        return None

    def send_message_with_files(self, channel_id: str, files: List[Path], content: Optional[str] = None, timeout: float = 120.0) -> None:
        url = f"{DISCORD_API}/channels/{channel_id}/messages"
        multipart_files = []
        file_handles = []
        try:
            for idx, p in enumerate(files):
                fh = p.open("rb")
                file_handles.append(fh)
                # Include filename in multipart so Discord receives the correct name
                multipart_files.append((f"files[{idx}]", (p.name, fh)))
            data = {}
            if content:
                data["content"] = content
            resp = self._request_with_retries("POST", url, data=data, files=multipart_files, timeout=timeout)
            if resp is None or resp.status_code not in (200, 201):
                status = getattr(resp, "status_code", "unknown")
                text = getattr(resp, "text", "")
                raise RuntimeError(f"Discord upload failed: {status} {text}")
        finally:
            for fh in file_handles:
                try:
                    fh.close()
                except Exception:
                    pass

    def _request_with_retries(self, method: str, url: str, max_retries: int = 5, timeout: float = 30.0, **kwargs):
        backoff = 1.0
        for attempt in range(max_retries):
            try:
                resp = requests.request(method, url, headers=self._headers(), timeout=timeout, **kwargs)
                if resp.status_code == 429:
                    retry_after = 1.0
                    try:
                        data = resp.json()
                        retry_after = float(data.get("retry_after", retry_after))
                    except Exception:
                        pass
                    import time as _t
                    _t.sleep(retry_after)
                    continue
                if 200 <= resp.status_code < 300:
                    return resp
                if resp.status_code in (500, 502, 503, 504):
                    import time as _t
                    _t.sleep(backoff)
                    backoff = min(backoff * 2, 10.0)
                    continue
                return resp
            except requests.RequestException:
                import time as _t
                _t.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
                continue
        return None



# --- Media relay helpers ---
@dataclass(frozen=True)
class MediaItem:
    filename: str
    url: str


def _is_media_filename(filename: str) -> bool:
    return bool(_CDN_FILENAME_RE.search(filename))


def _unique_path(dest_dir: Path, filename: str) -> Path:
    base = Path(filename).name
    candidate = dest_dir / base
    if not candidate.exists():
        return candidate
    stem = Path(base).stem
    suffix = Path(base).suffix
    index = 1
    while True:
        candidate = dest_dir / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


class DiscordClient(DiscordClient):
    def collect_media_items(
        self,
        channel_id: str,
        max_messages: int = 1000,
        include_attachments: bool = True,
        include_embeds: bool = True,
        request_timeout: float = 30.0,
    ) -> List[MediaItem]:
        items: List[MediaItem] = []
        seen: Set[Tuple[str, str]] = set()
        url = f"{DISCORD_API}/channels/{channel_id}/messages"
        params = {"limit": 100}
        last_id: Optional[str] = None
        fetched = 0

        while fetched < max_messages:
            if last_id:
                params["before"] = last_id
            resp = self._request_with_retries("GET", url, params=params, timeout=request_timeout)
            if resp is None:
                break
            messages = resp.json()
            if not messages:
                break
            for msg in messages:
                if include_attachments:
                    for att in msg.get("attachments", []):
                        fn = att.get("filename")
                        u = att.get("url")
                        if fn and u and _is_media_filename(fn):
                            key = (fn, u)
                            if key not in seen:
                                seen.add(key)
                                items.append(MediaItem(filename=fn, url=u))
                if include_embeds:
                    for emb in msg.get("embeds", []):
                        url_fields = [
                            emb.get("url"),
                            emb.get("thumbnail", {}).get("url"),
                            emb.get("video", {}).get("url"),
                            emb.get("image", {}).get("url"),
                        ]
                        for u in url_fields:
                            if not u:
                                continue
                            fn = self._extract_filename_from_url(u)
                            if fn and _is_media_filename(fn):
                                key = (fn, u)
                                if key not in seen:
                                    seen.add(key)
                                    items.append(MediaItem(filename=fn, url=u))
            fetched += len(messages)
            last_id = messages[-1]["id"]

        return items

    def _download_to_file(self, url: str, dest_path: Path, timeout: float = 120.0, bytes_limit: Optional[int] = None) -> bool:
        try:
            with requests.get(url, headers=self._headers(), timeout=timeout, stream=True) as r:
                if r.status_code != 200:
                    return False
                content_len = r.headers.get("Content-Length")
                if bytes_limit is not None and content_len is not None:
                    try:
                        if int(content_len) > bytes_limit:
                            return False
                    except Exception:
                        pass
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with dest_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 256):
                        if not chunk:
                            continue
                        f.write(chunk)
                return True
        except requests.RequestException:
            return False

    def relay_media(
        self,
        source_channel_id: str,
        dest_channel_id: str,
        download_dir: Path,
        max_messages: int = 1000,
        include_attachments: bool = True,
        include_embeds: bool = True,
        request_timeout: float = 30.0,
        upload_timeout: float = 120.0,
        delay_seconds: float = 1.0,
        max_file_mb: float = 10.0,
        skip_oversize: bool = True,
    ) -> Tuple[int, int]:
        items = self.collect_media_items(
            channel_id=source_channel_id,
            max_messages=max_messages,
            include_attachments=include_attachments,
            include_embeds=include_embeds,
            request_timeout=request_timeout,
        )
        items.reverse()  # send in chronological order
        sent = 0
        skipped = 0
        bytes_limit = int(max_file_mb * 1024 * 1024)
        for item in items:
            dest_path = _unique_path(download_dir, item.filename)
            ok = self._download_to_file(item.url, dest_path, timeout=request_timeout, bytes_limit=bytes_limit if skip_oversize else None)
            if not ok:
                skipped += 1
                continue
            if skip_oversize and dest_path.stat().st_size > bytes_limit:
                skipped += 1
                continue
            try:
                self.send_message_with_files(dest_channel_id, [dest_path], timeout=upload_timeout)
                sent += 1
                time.sleep(max(0.0, delay_seconds))
            except Exception:
                skipped += 1
                continue
        return sent, skipped

