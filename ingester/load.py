import logging
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from .config import DB_CONFIG, VARIABLE_MAP

log = logging.getLogger(__name__)


@contextmanager
def get_conn():
    """
    Context manager for Postgres connections.
    Commits on clean exit, rolls back on exception.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _get_city_id_map(conn) -> dict[str, int]:
    """
    Return a mapping of city name → city.id from the database.
    Used to resolve the foreign key without a per-row lookup.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT name, id FROM cities;")
        return {row[0]: row[1] for row in cur.fetchall()}


def _start_run(conn) -> int:
    """Insert a new ingestion_runs row with status='running'. Returns the run id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_runs (started_at, status)
            VALUES (NOW(), 'running')
            RETURNING id;
            """,
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    log.info(f"Started ingestion run id={run_id}")
    return run_id


def _finish_run(conn, run_id: int, inserted: int, skipped: int, rejected: int, status: str):
    """Update the ingestion_runs row with final counts and status."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion_runs
            SET completed_at      = NOW(),
                cities_processed  = (SELECT COUNT(DISTINCT city_id) FROM weather_daily),
                records_inserted  = %s,
                records_skipped   = %s,
                records_rejected  = %s,
                status            = %s
            WHERE id = %s;
            """,
            (inserted, skipped, rejected, status, run_id),
        )
    conn.commit()
    log.info(
        f"Run id={run_id} {status}: "
        f"inserted={inserted} skipped={skipped} rejected={rejected}"
    )


def load(valid_rows: list[dict], rejected_count: int) -> tuple[int, int]:
    """
    Upsert validated rows into weather_daily.

    Uses INSERT ... ON CONFLICT DO NOTHING on the (city_id, date) unique
    constraint. Rows that already exist are silently skipped -- safe to
    re-run without producing duplicates.

    Returns (inserted, skipped) counts.
    """
    if not valid_rows:
        log.warning("No valid rows to load.")
        return 0, 0

    with get_conn() as conn:
        run_id = _start_run(conn)

        try:
            city_id_map = _get_city_id_map(conn)

            # Build insert tuples
            # Row order: city_id, date, temp_max, temp_min, precip, wind, humidity
            db_col_order = list(VARIABLE_MAP.values())  # matches schema column order
            records = []

            for row in valid_rows:
                city_name = row["_city_name"]
                city_id = city_id_map.get(city_name)
                if city_id is None:
                    log.warning(f"City '{city_name}' not found in cities table -- skipping")
                    continue

                record = (
                    city_id,
                    row["date"],
                    *[row.get(api_var) for api_var in VARIABLE_MAP.keys()],
                )
                records.append(record)

            # Bulk upsert using execute_values for efficiency
            inserted = 0
            skipped = 0

            with conn.cursor() as cur:
                for record in records:
                    cur.execute(
                        f"""
                        INSERT INTO weather_daily
                            (city_id, date, {", ".join(db_col_order)})
                        VALUES
                            (%s, %s, {", ".join(["%s"] * len(db_col_order))})
                        ON CONFLICT (city_id, date) DO NOTHING;
                        """,
                        record,
                    )
                    if cur.rowcount == 1:
                        inserted += 1
                    else:
                        skipped += 1

            conn.commit()
            _finish_run(conn, run_id, inserted, skipped, rejected_count, "success")
            return inserted, skipped

        except Exception as e:
            log.error(f"Load failed: {e}")
            _finish_run(conn, run_id, 0, 0, rejected_count, "failed")
            raise