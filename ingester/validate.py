import logging
from .config import VARIABLE_MAP

log = logging.getLogger(__name__)


def validate_response(raw_response: dict) -> tuple[list[dict], list[str]]:
    """
    Validate a single city's raw API response.

    Unpacks the columnar API response into one dict per day, then
    applies field-level checks to each record. Returns valid records
    and a list of rejection reasons for bad ones.

    Open-Meteo returns data in columnar format:
        {
            "daily": {
                "time":                ["2024-03-01", "2024-03-02", ...],
                "temperature_2m_max":  [12.3, 14.1, ...],
                "precipitation_sum":   [0.0, 2.4, ...],
                ...
            }
        }

    We pivot this into row format before validating:
        [
            {"date": "2024-03-01", "temperature_2m_max": 12.3, ...},
            {"date": "2024-03-02", "temperature_2m_max": 14.1, ...},
        ]
    """
    city_name = raw_response.get("_city_name", "unknown")
    daily = raw_response.get("daily", {})
    dates = daily.get("time", [])

    if not dates:
        return [], [f"{city_name}: no daily data in response"]

    # Check all expected variables are present
    missing = [v for v in VARIABLE_MAP if v not in daily]
    if missing:
        log.warning(f"{city_name}: missing variables {missing} -- will load as NULL")

    # Pivot columnar → row format
    rows = []
    for i, date in enumerate(dates):
        row = {"date": date, "_city_name": city_name}
        for api_var in VARIABLE_MAP:
            values = daily.get(api_var, [])
            row[api_var] = values[i] if i < len(values) else None
        rows.append(row)

    valid = []
    rejected = []

    for row in rows:
        reasons = _check(row, city_name)
        if reasons:
            rejected.extend(reasons)
            log.warning(f"Rejected {city_name} {row['date']}: {reasons}")
        else:
            valid.append(row)

    return valid, rejected


def _check(row: dict, city_name: str) -> list[str]:
    """
    Apply all quality checks to a single row.
    Returns a list of failure reasons. Empty list means the row is valid.
    """
    reasons = []
    date = row["date"]

    temp_max = row.get("temperature_2m_max")
    temp_min = row.get("temperature_2m_min")
    precip   = row.get("precipitation_sum")

    # Both temp fields must be present
    if temp_max is None:
        reasons.append(f"{city_name} {date}: temperature_2m_max is null")
    if temp_min is None:
        reasons.append(f"{city_name} {date}: temperature_2m_min is null")

    # Max must be >= min
    if temp_max is not None and temp_min is not None:
        if temp_max < temp_min:
            reasons.append(
                f"{city_name} {date}: temp_max ({temp_max}) < temp_min ({temp_min})"
            )

    # Precipitation cannot be negative
    if precip is not None and precip < 0:
        reasons.append(f"{city_name} {date}: precipitation_sum is negative ({precip})")

    return reasons


def validate_all(raw_responses: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Validate all city responses. Collects valid records across all cities
    into a flat list ready for bulk loading.

    Returns:
        valid    - flat list of validated row dicts across all cities
        rejected - flat list of rejection reason strings
    """
    all_valid = []
    all_rejected = []

    for response in raw_responses:
        valid, rejected = validate_response(response)
        all_valid.extend(valid)
        all_rejected.extend(rejected)

    log.info(
        f"Validation complete: {len(all_valid)} valid, {len(all_rejected)} rejected"
    )
    return all_valid, all_rejected