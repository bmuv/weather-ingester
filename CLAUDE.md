# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A data engineering pipeline that ingests daily weather data from the [Open-Meteo API](https://open-meteo.com/en/docs/historical-weather-api) for 10 cities worldwide, archives raw JSON to Google Cloud Storage, and loads structured records into a local PostgreSQL database.

## Commands

### Python (managed with `uv`, Python 3.14)

```bash
uv sync                        # Install dependencies
uv run python main.py          # Run ingester locally
uv run jupyter notebook        # Start Jupyter for exploration (dev group)
```

### Docker

```bash
# Requires .env file with: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, GCS_BUCKET_NAME, GCP_KEY_PATH
docker compose up -d pgdatabase pgadmin   # Start only DB + pgAdmin
docker compose up --build ingester        # Build and run the ingester
docker compose down                       # Stop all services
```

pgAdmin is accessible at `http://localhost:8085` (admin@admin.com / root).

### Terraform (GCS bucket provisioning)

```bash
cd terraform
terraform init
terraform plan
terraform apply    # Provisions GCS bucket in GCP project de-zoomcamp-489713, region us-central1
terraform destroy
```

GCP credentials are expected at `keys/my-creds.json` (gitignored via `*.json`).

## Architecture

The pipeline has three logical stages run in sequence by `main.py`:

1. **Extract** (`ingester/extract.py`) — Calls Open-Meteo's forecast API for each city in `CITIES`, with tenacity retry logic (3 attempts, exponential backoff) on transient network errors. `extract_all()` collects failures without aborting the run. Each result is augmented with `_city_name` for downstream use.

2. **Archive** (not yet implemented) — Raw JSON responses are intended to be uploaded to the GCS bucket using the key pattern `raw/{city_name}/{run_date}.json` (key helper: `config.gcs_raw_key()`).

3. **Load** (not yet implemented) — Structured data is upserted into PostgreSQL using `ON CONFLICT (city_id, date)` to make runs idempotent. Run metadata is tracked in `ingestion_runs`.

### Configuration (`ingester/config.py`)

All runtime config is centralized here: API URL, `DAILY_VARIABLES` list, `CITIES` list with coordinates, GCS bucket name, DB connection params, and `get_date_range()` (defaults to lookback 30 days, yesterday as end date). The cities list in `config.py` **must stay in sync** with the seed data in `sql/init.sql` — city `name` is the join key.

### Database Schema (`sql/init.sql`)

Three tables auto-created on first Postgres container start:
- `cities` — reference data, seeded with the 10 cities
- `weather_daily` — fact table with `UNIQUE (city_id, date)` for idempotent upserts
- `ingestion_runs` — audit log per pipeline execution

### Environment Variables

| Variable | Used by | Notes |
|---|---|---|
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | ingester, docker-compose | DB connection |
| `GCS_BUCKET_NAME` | ingester | GCS archive target |
| `GOOGLE_APPLICATION_CREDENTIALS` | ingester container | Path to service account key inside container |
| `GCP_KEY_PATH` | docker-compose | Host path to service account JSON, mounted read-only |
| `LOOKBACK_DAYS` | ingester | Days of history to fetch (default: 30) |
