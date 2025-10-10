from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
import shutil
import tempfile

from dotenv import load_dotenv, set_key, unset_key
from platformdirs import user_config_dir, user_log_dir


"""Centralized config paths and helpers with package-local preferred storage.

Primary location: <package_dir>/config
Fallback: User AppData config dir when package dir is not writable.
Auto-migration copies existing AppData files into the package dir the first time.
"""

# Application metadata and user-scoped paths (persist across working directories)
APP_NAME = "AutoDisMediaSend"

# Package root (directory that contains this module)
PACKAGE_ROOT = Path(__file__).resolve().parent
PACKAGE_CONFIG_DIR = PACKAGE_ROOT / "config"

# User-scoped config/log dirs
USER_CONFIG_DIR = Path(user_config_dir(APP_NAME, appauthor=False))
LOG_DIR = Path(user_log_dir(APP_NAME, appauthor=False))


def _dir_is_writable(p: Path) -> bool:
    try:
        p.mkdir(parents=True, exist_ok=True)
        # Attempt to write a temp file
        with tempfile.NamedTemporaryFile(dir=str(p), delete=True) as _:
            pass
        return True
    except Exception:
        return False


def get_config_dir() -> Path:
    """Return the preferred config directory.

    Use package-local config dir if writable; otherwise fallback to user config dir.
    """
    if _dir_is_writable(PACKAGE_CONFIG_DIR):
        return PACKAGE_CONFIG_DIR
    return USER_CONFIG_DIR


# Resolved config directory and key files
CONFIG_DIR = get_config_dir()
GUI_SETTINGS_PATH = CONFIG_DIR / "gui_settings.json"
DOTENV_PATH = CONFIG_DIR / ".env"


def ensure_config_location() -> None:
    """Ensure CONFIG_DIR exists and migrate files from AppData if needed.

    If the chosen CONFIG_DIR is the package dir, and AppData has older files that
    are not present in the package dir yet, copy them over and best-effort delete
    the originals.
    """
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    # Only migrate when using the package dir
    try:
        if CONFIG_DIR == PACKAGE_CONFIG_DIR:
            src_settings = USER_CONFIG_DIR / "gui_settings.json"
            dst_settings = GUI_SETTINGS_PATH
            if src_settings.exists() and not dst_settings.exists():
                try:
                    dst_settings.parent.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass
                try:
                    shutil.copy2(str(src_settings), str(dst_settings))
                    try:
                        src_settings.unlink()
                    except Exception:
                        pass
                except Exception:
                    pass
            src_env = USER_CONFIG_DIR / ".env"
            dst_env = DOTENV_PATH
            if src_env.exists() and not dst_env.exists():
                try:
                    dst_env.parent.mkdir(parents=True, exist_ok=True)
                except Exception:
                    pass
                try:
                    shutil.copy2(str(src_env), str(dst_env))
                    try:
                        src_env.unlink()
                    except Exception:
                        pass
                except Exception:
                    pass
    except Exception:
        pass


def load_env() -> None:
    """Load environment variables from the project's .env file if present.

    We do not override existing environment variables to respect the caller's environment.
    """
    # Ensure config dir exists and migrate if needed
    ensure_config_location()
    # override=False ensures OS env vars take precedence over .env values
    load_dotenv(DOTENV_PATH, override=False)


def set_env_var(key: str, value: str) -> None:
    """Persist a key/value to the project's .env file (creates it if missing)."""
    # Ensure config dir and .env exist before writing
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    if not DOTENV_PATH.exists():
        try:
            DOTENV_PATH.touch()
        except Exception:
            pass
    # Write or update the key in-place
    set_key(str(DOTENV_PATH), key, value)


def unset_env_var(key: str) -> None:
    """Remove a key from the project's .env file if present."""
    if DOTENV_PATH.exists():
        unset_key(str(DOTENV_PATH), key)


