from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv, set_key, unset_key


# Resolve project root (two levels up from this file: src/config.py -> project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOTENV_PATH = PROJECT_ROOT / ".env"


def load_env() -> None:
    """Load environment variables from the project's .env file if present.

    We do not override existing environment variables to respect the caller's environment.
    """
    # override=False ensures OS env vars take precedence over .env values
    load_dotenv(DOTENV_PATH, override=False)


def set_env_var(key: str, value: str) -> None:
    """Persist a key/value to the project's .env file (creates it if missing)."""
    # Ensure .env exists before writing
    if not DOTENV_PATH.exists():
        DOTENV_PATH.touch()
    # Write or update the key in-place
    set_key(str(DOTENV_PATH), key, value)


def unset_env_var(key: str) -> None:
    """Remove a key from the project's .env file if present."""
    if DOTENV_PATH.exists():
        unset_key(str(DOTENV_PATH), key)


