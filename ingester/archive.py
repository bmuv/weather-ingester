import json
import logging
from datetime import date
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from .config import GCS_BUCKET_NAME, gcs_raw_key

log = logging.getLogger(__name__)


def _get_client() -> storage.Client:
    """
    Returns a GCS client. Credentials are picked up automatically from
    GOOGLE_APPLICATION_CREDENTIALS env var, which Docker Compose sets
    by mounting the service account key JSON.
    """
    return storage.Client()


def archive_response(raw_response: dict, run_date: str) -> str | None:
    """
    Write a single raw API response to GCS.

    Object key pattern: raw/{city_name}/{run_date}.json
    Overwrites if the object already exists (safe -- content is deterministic
    for a given city + date range).

    Returns the GCS URI on success, None on failure.
    """
    city_name = raw_response.get("_city_name", "unknown")
    key = gcs_raw_key(city_name, run_date)

    try:
        client = _get_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(key)
        blob.upload_from_string(
            data=json.dumps(raw_response, indent=2),
            content_type="application/json",
        )
        uri = f"gs://{GCS_BUCKET_NAME}/{key}"
        log.info(f"Archived {city_name} → {uri}")
        return uri

    except GoogleAPIError as e:
        log.error(f"GCS upload failed for {city_name}: {e}")
        return None


def archive_all(raw_responses: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Archive all raw responses to GCS. Uses today's date as the run date
    in the object key so you can trace which pipeline run produced each file.

    Failures are collected rather than raised so one bad upload doesn't
    abort the rest.

    Returns:
        archived  - list of raw_response dicts that were successfully archived
        failures  - list of error message strings for failed archives
    """
    run_date = date.today().isoformat()
    archived = []
    failures = []

    for response in raw_responses:
        uri = archive_response(response, run_date)
        if uri:
            archived.append(response)
        else:
            city_name = response.get("_city_name", "unknown")
            failures.append(f"Archive failed for {city_name}")

    log.info(f"Archive complete: {len(archived)} succeeded, {len(failures)} failed")
    return archived, failures