"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _load_dotenv_if_available() -> None:
    """Load .env for local dev if python-dotenv is installed."""
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        return
    load_dotenv()


@dataclass(frozen=True)
class Settings:
    eia_api_key: str
    months_to_pull: int = 5
    gcp_project_id: str | None = None
    bq_dataset: str | None = None
    bq_table: str | None = None
    gcp_credentials_file: str = ".gcp_user_creds.json"


def get_settings() -> Settings:
    _load_dotenv_if_available()

    api_key = os.getenv("DEPARTMENT_OF_ENERGY_API_KEY", "").strip()
    if not api_key:
        raise ValueError("Missing required env var: DEPARTMENT_OF_ENERGY_API_KEY")

    months_to_pull = int(os.getenv("MONTHS_TO_PULL", "5"))

    return Settings(
        eia_api_key=api_key,
        months_to_pull=months_to_pull,
        gcp_project_id=os.getenv("GCP_PROJECT_ID"),
        bq_dataset=os.getenv("BQ_DATASET"),
        bq_table=os.getenv("BQ_TABLE"),
        gcp_credentials_file=os.getenv("GCP_CREDENTIALS_FILE", ".gcp_user_creds.json"),
    )
