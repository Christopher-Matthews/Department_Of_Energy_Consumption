"""Run EIA retail-sales ingestion and load into BigQuery."""

from __future__ import annotations

import json

from bigquery_client import BigQueryClient
from config import get_settings
from eia_client import EIAClient


REQUIRED_FIELDS = {"stateId", "stateDescription", "period", "sales", "salesUnits"}
BACKFILL_MONTHS_IF_NEW_TABLE = 15
INCREMENTAL_MONTHS_IF_TABLE_EXISTS = 4


def _validate_rows(rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError("No rows returned from EIA API")

    missing = REQUIRED_FIELDS - set(rows[0].keys())
    if missing:
        raise ValueError(f"Missing required fields in row payload: {sorted(missing)}")


def main() -> None:
    settings = get_settings()

    bq_client = BigQueryClient(
        project_id=settings.gcp_project_id or "",
        dataset=settings.bq_dataset or "",
        table=settings.bq_table or "",
        credentials_file=settings.gcp_credentials_file,
    )
    created_table = bq_client.ensure_table_and_schema()

    months_to_pull = (
        BACKFILL_MONTHS_IF_NEW_TABLE
        if created_table
        else INCREMENTAL_MONTHS_IF_TABLE_EXISTS
    )

    eia_client = EIAClient(api_key=settings.eia_api_key)
    rows = eia_client.get_monthly_retail_sales(months=months_to_pull)

    _validate_rows(rows)

    print(f"Fetched {len(rows)} rows for last {months_to_pull} complete months.")
    print("Top 5 rows:")
    print(json.dumps(rows[:5], indent=2))
    total_rows_in_table = bq_client.load_rows(rows)

    if created_table:
        print(f"Created table {bq_client.table_id} with expected schema.")
    print(f"Upserted {len(rows)} rows into {bq_client.table_id}.")
    print(f"Table now contains {total_rows_in_table} rows.")


if __name__ == "__main__":
    main()
