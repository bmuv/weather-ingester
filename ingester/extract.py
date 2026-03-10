import logging
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from .config import OPEN_METEO_URL, DAILY_VARIABLES

log = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Raised when a city's data cannot be fetched after all retries."""
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def _fetch(params: dict) -> dict:
    """
    Single HTTP GET to Open-Meteo. Retries on transient network errors only.
    HTTP 4xx/5xx raise immediately -- they are not retryable without param changes.
    """
    response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def extract_city(city: dict, start_date: str, end_date: str) -> dict:
    """
    Fetch daily weather data for a single city over the given date range.

    Returns the raw API response dict, augmented with the city name
    for downstream archival and loading.

    Raises ExtractionError if the request fails after retries.
    """
    params = {
        "latitude":   city["latitude"],
        "longitude":  city["longitude"],
        "start_date": start_date,
        "end_date":   end_date,
        "timezone":   city["timezone"],
    }
    # Pass daily as a list -- requests serializes this as repeated params:
    # daily=temperature_2m_max&daily=temperature_2m_min&...
    # Open-Meteo rejects a single comma-joined string for this parameter
    params["daily"] = DAILY_VARIABLES

    log.info(f"Fetching {city['name']} | {start_date} to {end_date}")

    try:
        data = _fetch(params)
    except requests.HTTPError as e:
        raise ExtractionError(
            f"HTTP error for {city['name']}: {e.response.status_code} {e.response.text}"
        ) from e
    except (requests.Timeout, requests.ConnectionError) as e:
        raise ExtractionError(
            f"Network error for {city['name']} after retries: {e}"
        ) from e

    # Augment with city name so archive and load stages don't need to re-derive it
    data["_city_name"] = city["name"]
    return data


def extract_all(cities: list[dict], start_date: str, end_date: str) -> tuple[list[dict], list[str]]:
    """
    Fetch weather data for all cities. Failures are collected and returned
    rather than raising immediately, so one bad city doesn't abort the run.

    Returns:
        results  - list of successful raw API response dicts
        failures - list of error message strings for failed cities
    """
    results = []
    failures = []

    for city in cities:
        try:
            data = extract_city(city, start_date, end_date)
            results.append(data)
        except ExtractionError as e:
            log.error(str(e))
            failures.append(str(e))

    log.info(f"Extraction complete: {len(results)} succeeded, {len(failures)} failed")
    return results, failures