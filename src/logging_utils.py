from __future__ import annotations

import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional


_LOG_FMT = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _ensure_dir(p: Path) -> None:
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def sanitize_settings(settings: Dict[str, object]) -> Dict[str, object]:
    """Return a copy with sensitive values masked and Paths normalized to str.

    Any key containing token/password/secret/key is masked.
    """
    redacted: Dict[str, object] = {}
    for k, v in (settings or {}).items():
        key_l = str(k).lower()
        if any(s in key_l for s in ("token", "password", "secret", "apikey", "api_key", "key")):
            redacted[k] = "***"
            continue
        if isinstance(v, Path):
            redacted[k] = str(v)
        else:
            redacted[k] = v
    return redacted


def format_kv(d: Dict[str, object]) -> str:
    """Format a dict as compact k=v pairs suitable for single-line logs."""
    parts = []
    for k, v in d.items():
        if v is None or v == "":
            continue
        s = str(v)
        if re.search(r"\s", s):
            s = f'"{s}"'
        parts.append(f"{k}={s}")
    return " ".join(parts)


def sanitize_for_filename(name: str, max_len: int = 80) -> str:
    """Sanitize an arbitrary string for safe file names."""
    if name is None:
        return ""
    # Replace non-safe chars with underscore
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", str(name))
    # Collapse consecutive underscores
    safe = re.sub(r"_+", "_", safe).strip("._")
    if not safe:
        safe = "unnamed"
    if len(safe) > max_len:
        safe = safe[:max_len]
    return safe


def _configure_root_logger(log_path: Path) -> None:
    _ensure_dir(log_path.parent)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Clear existing handlers to avoid duplicates
    for h in list(root.handlers):
        try:
            root.removeHandler(h)
        except Exception:
            pass
    fh = logging.FileHandler(str(log_path), encoding="utf-8")
    fh.setFormatter(_LOG_FMT)
    root.addHandler(fh)


def init_run_logging(
    *,
    base_dir: Path = Path("logs"),
    preferred_run_id: Optional[str] = None,
    log_file: Optional[Path] = None,
) -> Tuple[Path, str, Path]:
    """Initialize per-run logging.

    - If log_file is provided, use it for the root log and place per-thread files under
      a sibling directory named after the file stem: <log_file.parent>/<log_file.stem>/
    - Otherwise, create <base_dir>/run_<timestamp>/run.log

    Returns: (run_dir, run_id, root_log_path)
    """
    if log_file is not None:
        log_file = Path(log_file)
        run_id = log_file.stem or (preferred_run_id or datetime.now().strftime("%Y%m%d_%H%M%S"))
        run_dir = (log_file.parent / run_id)
        _ensure_dir(run_dir)
        _configure_root_logger(log_file)
        return run_dir, run_id, log_file

    run_id = preferred_run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = base_dir / f"run_{run_id}"
    _ensure_dir(run_dir)
    root_log = run_dir / "run.log"
    _configure_root_logger(root_log)
    return run_dir, run_id, root_log


def prune_old_runs(base_dir: Path, keep: int = 5) -> None:
    """Remove oldest run_* directories under base_dir beyond the `keep` most recent."""
    try:
        if not base_dir.exists():
            return
        run_dirs = [p for p in base_dir.iterdir() if p.is_dir() and p.name.startswith("run_")]
        run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for old in run_dirs[keep:]:
            try:
                shutil.rmtree(old, ignore_errors=True)
            except Exception:
                pass
    except Exception:
        pass


def _has_handler_for(logger: logging.Logger, file_path: Path) -> bool:
    target = os.path.abspath(str(file_path))
    for h in logger.handlers:
        try:
            if isinstance(h, logging.FileHandler):
                if os.path.abspath(getattr(h, "baseFilename", "")) == target:
                    return True
        except Exception:
            continue
    return False


def start_thread_log(run_dir: Path, key: str) -> logging.Logger:
    """Create or return a child logger that writes to run_dir/<key>.log and propagates to root.

    The logger name is adms.thread.<key> to avoid handler collisions.
    """
    key_safe = sanitize_for_filename(key)
    thread_log_path = run_dir / f"{key_safe}.log"
    logger_name = f"adms.thread.{key_safe}"
    lg = logging.getLogger(logger_name)
    lg.setLevel(logging.INFO)
    lg.propagate = True
    if not _has_handler_for(lg, thread_log_path):
        try:
            fh = logging.FileHandler(str(thread_log_path), encoding="utf-8")
            fh.setFormatter(_LOG_FMT)
            lg.addHandler(fh)
        except Exception:
            # If we cannot add the handler, still return a usable logger that propagates
            pass
    return lg


