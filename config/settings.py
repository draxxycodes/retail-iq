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


# --------------------------------------------------------- MySQL ----------
MYSQL_HOST = _env("MYSQL_HOST", "localhost")
MYSQL_PORT = int(_env("MYSQL_PORT", "3306"))
MYSQL_DATABASE = _env("MYSQL_DATABASE", "retail_db")
MYSQL_USER = _env("MYSQL_USER", "")
MYSQL_PASSWORD = _env("MYSQL_PASSWORD", "")
MYSQL_JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"


def mysql_configured() -> bool:
    """True when MySQL JDBC settings have real values."""
    return bool(MYSQL_USER and MYSQL_PASSWORD)


# --------------------------------------------------------- Oracle ---------
ORACLE_HOST = _env("ORACLE_HOST", "localhost")
ORACLE_PORT = int(_env("ORACLE_PORT", "1521"))
ORACLE_SERVICE = _env("ORACLE_SERVICE", "XEPDB1")
ORACLE_USER = _env("ORACLE_USER", "")
ORACLE_PASSWORD = _env("ORACLE_PASSWORD", "")
ORACLE_JDBC_URL = f"jdbc:oracle:thin:@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"


def oracle_configured() -> bool:
    """True when Oracle JDBC settings have real values."""
    return bool(ORACLE_USER and ORACLE_PASSWORD)


# ------------------------------------------------------------- S3 --------
S3_BUCKET = _env("S3_BUCKET", "")
S3_PREFIX = _env("S3_PREFIX", "retailiq/curated")
REDSHIFT_IAM_ROLE = _env("REDSHIFT_IAM_ROLE", "")


def s3_configured() -> bool:
    """True when S3 staging bucket and Redshift IAM role are set."""
    return bool(S3_BUCKET and REDSHIFT_IAM_ROLE and "arn:aws" in REDSHIFT_IAM_ROLE)
