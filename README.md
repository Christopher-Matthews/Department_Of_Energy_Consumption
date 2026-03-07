# Department Of Energy Consumption

Simple Python ingestion pipeline for pulling data from the [EIA API](https://api.eia.gov) and loading it into Google BigQuery.

## Current Scope

- Fetch data from EIA API using a project API key
- Load raw/normalized records into BigQuery
- Keep implementation intentionally minimal for MVP

## Project Structure

- `ingest.py` - main entrypoint for running ingestion
- `config.py` - environment/config values
- `eia_client.py` - EIA API requests
- `bigquery_client.py` - BigQuery load/write logic
- `tests/test_ingest.py` - initial test scaffold

## Setup

1. Create and activate a Python virtual environment
2. Install dependencies:
   `pip install -r requirements.txt`
3. Configure environment variables in `.env`:
   - `DEPARTMENT_OF_ENERGY_API_KEY`

## Run

Run ingestion:

`python ingest.py`

## Notes

- `.env` is gitignored and should not be committed.
- This README will be expanded as endpoint/table details are finalized.
