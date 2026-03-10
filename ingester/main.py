import logging
import sys
from .config import CITIES, get_date_range
from .extract import extract_all
from .archive import archive_all
from .validate import validate_all
from .load import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main")


def run():
    start_date, end_date = get_date_range()
    log.info(f"Pipeline starting | cities={len(CITIES)} | {start_date} to {end_date}")

    # ── Stage 1: Extract ──────────────────────────────────────────────────────
    raw_responses, extract_failures = extract_all(CITIES, start_date, end_date)
    if not raw_responses:
        log.error("Extraction returned no data. Aborting.")
        sys.exit(1)

    # ── Stage 2: Archive ──────────────────────────────────────────────────────
    archived, archive_failures = archive_all(raw_responses)
    if archive_failures:
        # Archive failures are non-fatal -- log and continue
        # Data is still valid; loss is only in the raw backup layer
        log.warning(f"Archive failures ({len(archive_failures)}): {archive_failures}")

    # ── Stage 3: Validate ─────────────────────────────────────────────────────
    valid_rows, rejected = validate_all(archived)
    if not valid_rows:
        log.error("Validation produced no valid rows. Aborting.")
        sys.exit(1)

    # ── Stage 4: Load ─────────────────────────────────────────────────────────
    inserted, skipped = load(valid_rows, rejected_count=len(rejected))

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info(
        f"Pipeline complete | "
        f"extracted={len(raw_responses)} "
        f"archived={len(archived)} "
        f"valid={len(valid_rows)} "
        f"rejected={len(rejected)} "
        f"inserted={inserted} "
        f"skipped={skipped} "
        f"extract_failures={len(extract_failures)} "
        f"archive_failures={len(archive_failures)}"
    )


if __name__ == "__main__":
    run()