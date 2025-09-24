from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv, set_key, unset_key
from platformdirs import user_config_dir, user_log_dir


# Application metadata and user-scoped paths (persist across working directories)
APP_NAME = "AutoDisMediaSend"
CONFIG_DIR = Path(user_config_dir(APP_NAME, appauthor=False))
LOG_DIR = Path(user_log_dir(APP_NAME, appauthor=False))
DOTENV_PATH = CONFIG_DIR / ".env"


def load_env() -> None:
    """Load environment variables from the project's .env file if present.

    We do not override existing environment variables to respect the caller's environment.
    """
    # override=False ensures OS env vars take precedence over .env values
    # Ensure config dir exists before attempting to read .env
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
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


