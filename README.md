# Department Of Energy Consumption

Simple Python ingestion pipeline for pulling data from the [EIA API](https://api.eia.gov) and loading it into Google BigQuery.

## Current Scope

- Fetch monthly retail electricity sales from EIA API
- Load into BigQuery with upsert (dedup) logic
- Keep implementation intentionally minimal for MVP

## Project Structure

- `ingest.py` - main entrypoint for running ingestion
- `config.py` - environment/config values
- `eia_client.py` - EIA API requests
- `bigquery_client.py` - BigQuery table management + merge/upsert logic
- `tests/test_ingest.py` - initial test scaffold

## Setup

1. Create and activate a Python virtual environment (`python3.11` recommended)
2. Install dependencies: `pip install -r requirements.txt`
3. Add `.gcp_user_creds.json` to repo root (gitignored)
4. Configure `.env`:
   - `DEPARTMENT_OF_ENERGY_API_KEY`
   - `GCP_PROJECT_ID`
   - `BQ_DATASET`
   - `BQ_TABLE`
   - `GCP_CREDENTIALS_FILE=.gcp_user_creds.json`

## Data Model

BigQuery target table schema:

- `stateId` STRING
- `stateDescription` STRING
- `period` STRING
- `sales` NUMERIC
- `salesUnits` STRING
- `loaded_at` TIMESTAMP

## Run

Run ingestion: `python ingest.py`

Load behavior:

- If table does not exist: create table and load last 15 complete months
- If table exists: load last 4 complete months

## Notes

- `.env` and `.gcp_user_creds.json` are gitignored.
- Ingestion uses BigQuery `MERGE` keyed on `stateId + period` to avoid duplicate rows across reruns.
