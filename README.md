# weather-ingester

A production-style batch data pipeline that ingests daily weather data for multiple cities from the Open-Meteo API, archives raw responses to Google Cloud Storage, validates and normalizes records, and loads them into a PostgreSQL database. Infrastructure is fully provisioned with Terraform. The entire stack runs locally with a single `docker compose up`.

---

## Why This Project Exists

Most data engineering tutorials show you how to load a CSV into a database. This project shows something closer to what production pipelines actually require: a repeatable, observable, failure-tolerant ingestion process where raw data is preserved before transformation, bad records are caught at the border, and re-running the pipeline never produces duplicates.

The data source is the [Open-Meteo API](https://open-meteo.com) -- a fully free, open-source weather API with no key, no rate limits, and no registration required. Daily weather summaries (temperature, precipitation, wind, humidity) are pulled for a configurable list of cities. The batch cadence matches the data cadence: Open-Meteo publishes daily aggregates once per day, so a daily batch job is the architecturally correct choice here, not a stream.

---

## Architecture


<img width="1228" height="345" alt="image" src="https://github.com/user-attachments/assets/f2c45e94-80e1-4fd9-823d-f8738d8590f0" />




All four stages are orchestrated by `main.py`. Infrastructure (GCS bucket, IAM bindings) is managed by Terraform. PostgreSQL and the ingester run as Docker containers wired together with Docker Compose.

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.13 |
| Containerization | Docker + Docker Compose |
| Database | PostgreSQL 15 |
| Object Storage | Google Cloud Storage |
| Infrastructure | Terraform |
| HTTP + Retry | requests + tenacity |
| DB Driver | psycopg2 |
| GCS Client | google-cloud-storage |

---

## Schema

Three tables. Normalized, not flat.

```sql
-- Reference table for city metadata
CREATE TABLE cities (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    latitude    NUMERIC(8, 5) NOT NULL,
    longitude   NUMERIC(8, 5) NOT NULL,
    timezone    TEXT NOT NULL
);

-- One row per city per day
-- Unique constraint on (city_id, date) enforces idempotency
CREATE TABLE weather_daily (
    id                  SERIAL PRIMARY KEY,
    city_id             INTEGER REFERENCES cities(id),
    date                DATE NOT NULL,
    temp_max_c          NUMERIC(5, 2),
    temp_min_c          NUMERIC(5, 2),
    precipitation_mm    NUMERIC(6, 2),
    windspeed_max_kmh   NUMERIC(6, 2),
    humidity_avg_pct    NUMERIC(5, 2),
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (city_id, date)
);

-- One row per pipeline run for observability
CREATE TABLE ingestion_runs (
    id                  SERIAL PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    cities_processed    INTEGER DEFAULT 0,
    records_inserted    INTEGER DEFAULT 0,
    records_skipped     INTEGER DEFAULT 0,
    records_rejected    INTEGER DEFAULT 0,
    status              TEXT CHECK (status IN ('running', 'success', 'failed'))
);
```

---

## Project Structure

```
weather-ingester/
├── terraform/
│   ├── main.tf          # GCS bucket + IAM binding
│   ├── variables.tf
│   └── outputs.tf
├── ingester/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── config.py        # city list + API variables config
│   ├── extract.py       # Open-Meteo API calls with retry
│   ├── archive.py       # raw JSON → GCS
│   ├── validate.py      # field-level data quality checks
│   ├── load.py          # upsert into PostgreSQL
│   └── main.py          # orchestrates all four stages
├── sql/
│   ├── init.sql         # schema + constraints
│   └── queries.sql      # analytical queries
├── docker-compose.yml
├── .env.example
├── Makefile
└── README.md
```

---

## Prerequisites

- Docker + Docker Compose
- Terraform >= 1.5
- A GCP project with billing enabled
- A GCP service account key JSON with Storage Admin permissions

---

## Setup

**1. Clone and configure environment**

```bash
git clone https://github.com/your-username/weather-ingester.git
cd weather-ingester
cp .env.example .env
# Fill in POSTGRES_*, GCP_PROJECT_ID, GCS_BUCKET_NAME, GOOGLE_APPLICATION_CREDENTIALS
```

**2. Provision GCP infrastructure**

```bash
make infra
# Equivalent to: cd terraform && terraform init && terraform apply
```

**3. Start the stack**

```bash
make up
# Equivalent to: docker compose up --build -d
```

**4. Run the pipeline**

```bash
make run
# Equivalent to: docker compose exec ingester python main.py
```

**5. Verify**

```bash
docker compose exec db psql -U postgres -d weather -c \
  "SELECT c.name, COUNT(*) as days_loaded FROM weather_daily w
   JOIN cities c ON w.city_id = c.id
   GROUP BY c.name ORDER BY days_loaded DESC;"
```

---

## Makefile Targets

| Target | Action |
|---|---|
| `make infra` | Run `terraform apply` to provision GCP resources |
| `make up` | Build and start all Docker containers |
| `make run` | Execute the ingestion pipeline |
| `make psql` | Open a psql shell into the running database |
| `make teardown` | Stop containers and destroy Terraform-managed infra |
| `make logs` | Tail ingester container logs |

---

## Idempotency

Re-running the pipeline is safe. The `UNIQUE (city_id, date)` constraint on `weather_daily` combined with `INSERT ... ON CONFLICT DO NOTHING` means duplicate records are silently skipped. The `ingestion_runs` table records how many rows were inserted vs. skipped per run, so you can verify this behavior.

```sql
-- After a second run, skipped should equal total records, inserted should be 0
SELECT records_inserted, records_skipped FROM ingestion_runs ORDER BY started_at DESC LIMIT 2;
```

---

## Data Quality

`validate.py` runs four checks on every record before it reaches the database:

- `temp_max_c` and `temp_min_c` are not null
- `temp_max_c` >= `temp_min_c`
- `precipitation_mm` >= 0
- All expected fields are present in the API response

Records that fail any check are logged with the failure reason and counted in `ingestion_runs.records_rejected`. They are not loaded.

---

## Raw Data Archival

Before any transformation or load, raw JSON responses from Open-Meteo are written to GCS:

```
gs://{bucket}/raw/{city_name}/{date}.json
```

This serves two purposes. First, if the schema changes downstream, historical raw data can be replayed without re-hitting the API. Second, it creates an audit trail that separates what was received from what was loaded.

---

## Analytical Queries

See `sql/queries.sql` for ready-to-run examples:

- Hottest cities by average max temperature over the last 30 days
- Cities with the highest total precipitation this month
- Days with windspeed above a defined threshold by city
- Pipeline run history with insert/skip/reject counts
- Cities with missing data for any date in a range

---

## Extending This Project

This pipeline was intentionally designed as a foundation for the full DE Zoomcamp stack:

- **Module 2 (Kestra)**: Replace `main.py` with a Kestra flow. The four-stage structure maps directly to Kestra tasks. Add scheduling and failure alerting.
- **Module 3 (BigQuery)**: Point the load stage at BigQuery instead of Postgres. The GCS archive layer is already in place -- BigQuery can load directly from it.
- **Module 4 (dbt)**: Add a dbt project on top of `weather_daily` to build aggregated models (monthly summaries, city rankings, anomaly flags).
- **Module 5 (Bruin)**: Swap the ingestion layer for a Bruin pipeline with built-in quality assertions.

---

## Design Decisions

**Why batch, not streaming?**
Open-Meteo publishes daily weather aggregates once per day. There is no sub-daily data to stream. A scheduled daily batch job matches the data cadence exactly. Streaming infrastructure here would add cost and operational complexity with no benefit.

**Why archive raw JSON to GCS before loading?**
Separating extraction from loading means the raw source record is preserved regardless of what happens downstream. If a bug in `validate.py` incorrectly rejects records, or if the Postgres schema changes, the raw data can be replayed without re-fetching from the API.

**Why `ON CONFLICT DO NOTHING` instead of upsert?**
Weather history does not change retroactively. Once a daily record is loaded, it should not be overwritten by a re-run. `DO NOTHING` is the correct semantic here. An `ON CONFLICT DO UPDATE` would be appropriate if the source data could be revised after the fact (e.g., forecast data being corrected).

---
