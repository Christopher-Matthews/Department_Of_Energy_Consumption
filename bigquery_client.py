"""Minimal BigQuery loader for EIA ingestion rows."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

EXPECTED_SCHEMA = [
    bigquery.SchemaField("stateId", "STRING"),
    bigquery.SchemaField("stateDescription", "STRING"),
    bigquery.SchemaField("period", "STRING"),
    bigquery.SchemaField("sales", "NUMERIC"),
    bigquery.SchemaField("salesUnits", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]


class BigQueryClient:
    def __init__(
        self,
        project_id: str,
        dataset: str,
        table: str,
        credentials_file: str = ".gcp_user_creds.json",
    ) -> None:
        if not project_id:
            raise ValueError("Missing GCP project id")
        if not dataset:
            raise ValueError("Missing BigQuery dataset")
        if not table:
            raise ValueError("Missing BigQuery table")
        if not credentials_file:
            raise ValueError("Missing GCP credentials file")

        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.credentials_file = credentials_file

        cred_path = Path(credentials_file)
        if not cred_path.is_absolute():
            cred_path = Path.cwd() / cred_path
        if not cred_path.exists():
            raise FileNotFoundError(f"GCP credentials file not found: {cred_path}")

        self.client = bigquery.Client.from_service_account_json(
            str(cred_path),
            project=project_id,
        )

    @property
    def table_id(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}"

    @property
    def staging_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.table}__staging"

    def ensure_table_and_schema(self) -> bool:
        """
        Ensure target table exists with expected schema.

        Returns True if table was created in this call, otherwise False.
        """
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset)
        self.client.create_dataset(dataset_ref, exists_ok=True)

        try:
            table = self.client.get_table(self.table_id)
        except NotFound:
            table = bigquery.Table(self.table_id, schema=EXPECTED_SCHEMA)
            self.client.create_table(table)
            return True

        actual = [(field.name, field.field_type.upper()) for field in table.schema]
        expected = [(field.name, field.field_type.upper()) for field in EXPECTED_SCHEMA]
        if actual != expected:
            raise ValueError(
                f"Schema mismatch for {self.table_id}. "
                f"Expected {expected}, got {actual}."
            )
        return False

    def _prepare_rows_for_load(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        loaded_at = datetime.now(timezone.utc).isoformat()
        prepared_rows: list[dict[str, Any]] = []
        for row in rows:
            prepared_rows.append(
                {
                    "stateId": row.get("stateId"),
                    "stateDescription": row.get("stateDescription"),
                    "period": row.get("period"),
                    "sales": row.get("sales"),
                    "salesUnits": row.get("salesUnits"),
                    "loaded_at": loaded_at,
                }
            )
        return prepared_rows

    def _merge_staging_into_target(self) -> None:
        merge_sql = f"""
        MERGE `{self.table_id}` T
        USING `{self.staging_table_id}` S
        ON T.stateId = S.stateId AND T.period = S.period
        WHEN MATCHED THEN
          UPDATE SET
            stateDescription = S.stateDescription,
            sales = S.sales,
            salesUnits = S.salesUnits,
            loaded_at = S.loaded_at
        WHEN NOT MATCHED THEN
          INSERT (stateId, stateDescription, period, sales, salesUnits, loaded_at)
          VALUES (S.stateId, S.stateDescription, S.period, S.sales, S.salesUnits, S.loaded_at)
        """
        self.client.query(merge_sql).result()

    def load_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        prepared_rows = self._prepare_rows_for_load(rows)

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=EXPECTED_SCHEMA,
        )

        load_job = self.client.load_table_from_json(
            prepared_rows,
            self.staging_table_id,
            job_config=job_config,
        )
        load_job.result()

        self._merge_staging_into_target()

        table = self.client.get_table(self.table_id)
        return table.num_rows
