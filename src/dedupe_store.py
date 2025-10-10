from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Iterable, Set

from .config import CONFIG_DIR


class DedupeStore:
    """Thread-safe persistent store of seen filenames per channel/thread id.

    Stored at <config_dir>/dedupe.json with structure:
      { "<channel_or_thread_id>": ["filename1", "filename2", ...], ... }
    Filenames are stored in lowercase for case-insensitive matching.
    """

    def __init__(self, file_path: Path | None = None) -> None:
        self._path: Path = file_path or (CONFIG_DIR / "dedupe.json")
        self._lock = threading.Lock()
        self._data: dict[str, Set[str]] = {}
        self._load()

    def _load(self) -> None:
        try:
            if not self._path.exists():
                self._data = {}
                return
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            data: dict[str, Set[str]] = {}
            for key, names in (raw or {}).items():
                try:
                    key_s = str(key)
                    s = {str(n).lower() for n in (names or [])}
                    if s:
                        data[key_s] = s
                except Exception:
                    continue
            self._data = data
        except Exception:
            # Corrupt or unreadable file; reset in-memory view
            self._data = {}

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        try:
            serializable = {k: sorted(list(v)) for k, v in self._data.items() if v}
            self._path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            # Best-effort persistence; do not propagate
            pass

    def get_names_for(self, channel_or_thread_id: str) -> Set[str]:
        key = str(channel_or_thread_id)
        with self._lock:
            return set(self._data.get(key, set()))

    def add_names_for(self, channel_or_thread_id: str, names: Iterable[str]) -> None:
        key = str(channel_or_thread_id)
        names_l = {str(n).lower() for n in names if n}
        if not names_l:
            return
        with self._lock:
            current = self._data.get(key)
            if current is None:
                self._data[key] = set(names_l)
            else:
                current.update(names_l)
            self._save()


