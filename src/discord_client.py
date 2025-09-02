import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Set

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
    token_type: str = "bot"
    user_agent: str = "AutoDisMediaSend (https://github.com/0-FoxHunt-0/disdrop, 1.0)"

    def _headers(self) -> dict:
        return {
            "Authorization": _auth_header(self.token, self.token_type),
            "User-Agent": self.user_agent,
        }

    @staticmethod
    def parse_channel_id_from_url(channel_url: str) -> Optional[str]:
        # https://discord.com/channels/<guild>/<channel>
        m = re.search(r"discord\.com/channels/\d+/(\d+)", channel_url)
        if not m:
            return None
        return m.group(1)

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


