"""RetailIQ central configuration.

All settings come from environment variables (optionally loaded from a
`.env` file). No credentials are hardcoded anywhere in the codebase.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # safe no-op when .env is absent

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# ---------------------------------------------------------------- paths ---
RAW_DATA_DIR = Path(_env("RAW_DATA_DIR", str(PROJECT_ROOT / "data" / "raw")))
CURATED_DATA_DIR = Path(_env("CURATED_DATA_DIR", str(PROJECT_ROOT / "data" / "curated")))
REJECTED_DATA_DIR = Path(_env("REJECTED_DATA_DIR", str(PROJECT_ROOT / "data" / "rejected")))
DQ_LOG_DIR = Path(_env("DQ_LOG_DIR", str(PROJECT_ROOT / "data" / "curated" / "_dq_log")))

for _d in (RAW_DATA_DIR, CURATED_DATA_DIR, REJECTED_DATA_DIR, DQ_LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------- Redshift --------
REDSHIFT_HOST = _env("REDSHIFT_HOST", "")
REDSHIFT_PORT = int(_env("REDSHIFT_PORT", "5439"))
REDSHIFT_DATABASE = _env("REDSHIFT_DATABASE", "retailiq")
REDSHIFT_USER = _env("REDSHIFT_USER", "")
REDSHIFT_PASSWORD = _env("REDSHIFT_PASSWORD", "")


def redshift_configured() -> bool:
    """True only when every Redshift setting has a real (non-placeholder) value."""
    return all(
        [
            REDSHIFT_HOST and "your-cluster" not in REDSHIFT_HOST,
            REDSHIFT_USER,
            REDSHIFT_PASSWORD and "your-password" not in REDSHIFT_PASSWORD,
        ]
    )
