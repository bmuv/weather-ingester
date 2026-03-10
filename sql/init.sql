-- =============================================================
-- weather-ingester schema
-- Run automatically by Postgres on first container start
-- via /docker-entrypoint-initdb.d/
-- =============================================================

CREATE TABLE IF NOT EXISTS cities (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    latitude    NUMERIC(8, 5) NOT NULL,
    longitude   NUMERIC(8, 5) NOT NULL,
    timezone    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_daily (
    id                  SERIAL PRIMARY KEY,
    city_id             INTEGER NOT NULL REFERENCES cities(id),
    date                DATE NOT NULL,
    temp_max_c          NUMERIC(5, 2),
    temp_min_c          NUMERIC(5, 2),
    precipitation_mm    NUMERIC(6, 2),
    windspeed_max_kmh   NUMERIC(6, 2),
    humidity_avg_pct    NUMERIC(5, 2),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (city_id, date)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id                  SERIAL PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    cities_processed    INTEGER NOT NULL DEFAULT 0,
    records_inserted    INTEGER NOT NULL DEFAULT 0,
    records_skipped     INTEGER NOT NULL DEFAULT 0,
    records_rejected    INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed'))
);

-- Seed city reference data
INSERT INTO cities (name, latitude, longitude, timezone) VALUES
    ('New York',       40.71280, -74.00600, 'America/New_York'),
    ('Los Angeles',    34.05220, -118.24370, 'America/Los_Angeles'),
    ('Chicago',        41.85003, -87.65005, 'America/Chicago'),
    ('Houston',        29.76328, -95.36327, 'America/Chicago'),
    ('Atlanta',        33.74900, -84.38798, 'America/New_York'),
    ('London',         51.50853, -0.12574,  'Europe/London'),
    ('Paris',          48.85341,  2.34880,  'Europe/Paris'),
    ('Tokyo',          35.68950, 139.69171, 'Asia/Tokyo'),
    ('Sydney',        -33.86785, 151.20732, 'Australia/Sydney'),
    ('Nairobi',        -1.28333,  36.81667, 'Africa/Nairobi')
ON CONFLICT (name) DO NOTHING;