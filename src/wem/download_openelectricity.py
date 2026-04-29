"""Download post-Reform WEM facility data from the Open Electricity API.

Post-Reform (Oct 2023+) WEM switched to 5-minute dispatch intervals.
AEMO's public CSV archive was never updated past Sep 2023. Open Electricity
(formerly OpenNEM) provides the same data via a REST API.

Data returned:
  - energy (MWh) per unit per month/day
  - market_value ($) per unit per month/day
  - Captured price derived as market_value / energy

API docs: https://docs.openelectricity.org.au
Plan limits (Community/free): 1-year history, 500 credits/day, 2 req/s burst.
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import date

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

API_BASE = "https://api.openelectricity.org.au/v4"
# Max facilities per request — keeps URL length safe and avoids timeouts
FACILITY_BATCH_SIZE = 30


def _get_api_key() -> str:
    """Retrieve Open Electricity API key from macOS Keychain."""
    result = subprocess.run(
        ["security", "find-generic-password", "-s", "openelectricity-api-key", "-w"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Open Electricity API key not found in Keychain. "
            "Store it with: security add-generic-password -a openelectricity "
            '-s openelectricity-api-key -w "YOUR_KEY"'
        )
    return result.stdout.strip()


def _get_wem_facility_codes(api_key: str) -> list[str]:
    """Fetch the list of WEM facility codes from the API."""
    resp = requests.get(
        f"{API_BASE}/facilities/",
        params={"network_code": "WEM"},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success", True):
        raise RuntimeError(f"API error listing facilities: {data.get('error')}")
    return [
        f["code"]
        for f in data.get("data", [])
        if f.get("network_id") == "WEM" and f.get("code")
    ]


def _api_get(api_key: str, endpoint: str, params: list[tuple[str, str]] | dict) -> dict:
    """Make a rate-limited GET request to the Open Electricity API."""
    resp = requests.get(
        f"{API_BASE}{endpoint}",
        params=params,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success", True):
        error = data.get("error", "unknown")
        raise RuntimeError(f"API error: {error}")
    return data


def _fetch_facility_data(
    api_key: str,
    facility_codes: list[str],
    metrics: list[str],
    interval: str,
    date_start: str,
    date_end: str,
) -> list[dict]:
    """Fetch facility data in batches and merge results.

    Returns list of result dicts, each with 'name', 'columns', 'data' keys.
    """
    all_results: dict[str, list[dict]] = {m: [] for m in metrics}

    for i in range(0, len(facility_codes), FACILITY_BATCH_SIZE):
        batch = facility_codes[i : i + FACILITY_BATCH_SIZE]
        params: list[tuple[str, str]] = [
            ("interval", interval),
            ("date_start", date_start),
            ("date_end", date_end),
        ]
        for m in metrics:
            params.append(("metrics", m))
        for code in batch:
            params.append(("facility_code", code))

        logger.debug(
            f"Fetching {interval} {','.join(metrics)} for {len(batch)} facilities "
            f"({date_start} to {date_end})"
        )

        data = _api_get(api_key, "/data/facilities/WEM", params)

        for block in data.get("data", []):
            metric = block["metric"]
            for result in block.get("results", []):
                if result.get("data"):
                    all_results[metric].append(result)

        # Respect 2/s burst limit
        if i + FACILITY_BATCH_SIZE < len(facility_codes):
            time.sleep(0.6)

    return all_results


def fetch_post_reform_monthly(
    year: int,
    month: int,
    generators: pd.DataFrame,
) -> pd.DataFrame:
    """Fetch one month of post-Reform WEM data and return a monthly aggregate DataFrame.

    Returns DataFrame matching the pre-Reform pipeline schema:
        duid, month, generation_mwh, revenue_aud, capacity_factor,
        captured_price, avg_rrp, price_capture_ratio
    """
    api_key = _get_api_key()
    facility_codes = _get_wem_facility_codes(api_key)

    month_label = f"{year}-{month:02d}"
    # API date range: start of month to start of next month
    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1}-01-01"
    else:
        end = f"{year}-{month + 1:02d}-01"

    logger.info(f"Fetching post-Reform WEM monthly data for {month_label}")
    results = _fetch_facility_data(
        api_key, facility_codes,
        metrics=["energy", "market_value"],
        interval="1M",
        date_start=start,
        date_end=end,
    )

    # Build unit_code → MWh and unit_code → revenue mappings
    energy_by_unit: dict[str, float] = {}
    for r in results.get("energy", []):
        unit = r["columns"].get("unit_code", "")
        for ts, val in r["data"]:
            if val is not None:
                energy_by_unit[unit] = energy_by_unit.get(unit, 0) + val

    revenue_by_unit: dict[str, float] = {}
    for r in results.get("market_value", []):
        unit = r["columns"].get("unit_code", "")
        for ts, val in r["data"]:
            if val is not None:
                revenue_by_unit[unit] = revenue_by_unit.get(unit, 0) + val

    # Also fetch system-level price for avg_rrp context
    avg_rrp = _fetch_system_avg_price(api_key, start, end)

    # Build capacity lookup from our generator metadata
    from calendar import monthrange
    hours_in_month = monthrange(year, month)[1] * 24
    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()

    rows = []
    all_units = set(energy_by_unit) | set(revenue_by_unit)
    for unit in all_units:
        mwh = energy_by_unit.get(unit, 0)
        if mwh <= 0:
            continue  # Skip consumption-only or zero-generation units

        revenue = revenue_by_unit.get(unit, 0)
        capacity = duid_capacity.get(unit)

        cap_factor = mwh / (capacity * hours_in_month) if capacity and capacity > 0 else None
        if cap_factor is not None and cap_factor > 1.1:
            logger.warning(
                f"{unit} {month_label}: monthly CF {cap_factor:.4f} — possible registration mismatch"
            )

        captured = revenue / mwh if mwh > 0 else None
        pcr = captured / avg_rrp if captured is not None and avg_rrp and avg_rrp != 0 else None

        rows.append({
            "duid": unit,
            "month": month_label,
            "generation_mwh": round(mwh, 1),
            "revenue_aud": round(revenue, 0),
            "capacity_factor": round(cap_factor, 4) if cap_factor is not None else None,
            "captured_price": round(captured, 2) if captured is not None else None,
            "avg_rrp": round(avg_rrp, 2) if avg_rrp is not None else None,
            "price_capture_ratio": round(pcr, 4) if pcr is not None else None,
        })

    result = pd.DataFrame(rows)
    logger.info(f"Post-Reform WEM {month_label}: {len(result)} units with generation")
    return result


def fetch_post_reform_daily(
    year: int,
    month: int,
    generators: pd.DataFrame,
) -> pd.DataFrame:
    """Fetch daily generation for one month of post-Reform WEM data.

    Returns DataFrame matching the daily aggregate schema:
        duid, date, daily_generation_mwh, daily_capacity_factor
    """
    api_key = _get_api_key()
    facility_codes = _get_wem_facility_codes(api_key)

    month_label = f"{year}-{month:02d}"
    start = f"{year}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1}-01-01"
    else:
        end = f"{year}-{month + 1:02d}-01"

    logger.info(f"Fetching post-Reform WEM daily data for {month_label}")
    results = _fetch_facility_data(
        api_key, facility_codes,
        metrics=["energy"],
        interval="1d",
        date_start=start,
        date_end=end,
    )

    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()

    rows = []
    for r in results.get("energy", []):
        unit = r["columns"].get("unit_code", "")
        capacity = duid_capacity.get(unit)
        for ts, val in r["data"]:
            if val is None or val <= 0:
                continue
            date_str = ts[:10]  # "2024-01-15T00:00:00+08:00" → "2024-01-15"
            cf = val / (capacity * 24) if capacity and capacity > 0 else None
            if cf is not None and cf > 1.1:
                logger.warning(f"{unit} {date_str}: daily CF {cf:.4f} — possible registration mismatch")
            rows.append({
                "duid": unit,
                "date": date_str,
                "daily_generation_mwh": round(val, 1),
                "daily_capacity_factor": round(cf, 4) if cf is not None else None,
            })

    result = pd.DataFrame(rows)
    logger.info(f"Post-Reform WEM daily {month_label}: {len(result)} unit-days")
    return result


def _fetch_system_avg_price(api_key: str, date_start: str, date_end: str) -> float | None:
    """Fetch WEM system average price for the period.

    Uses the network endpoint with the price metric. Falls back to None
    if price data is unavailable (Community plan may not support it).
    """
    try:
        data = _api_get(api_key, "/data/network/WEM", {
            "metrics": "market_value",
            "interval": "1M",
            "date_start": date_start,
            "date_end": date_end,
            "secondary_grouping": "fueltech",
        })
        # Sum all market_value and energy to get volume-weighted avg price
        # Fallback: just return None and let the caller handle it
        total_mv = 0
        for block in data.get("data", []):
            if block["metric"] == "market_value":
                for r in block.get("results", []):
                    for ts, val in r["data"]:
                        if val:
                            total_mv += val
        # We'd need energy to compute avg price, but market_value alone isn't enough
        # Just return None for now — the pre-Reform pipeline also used balancing price
        return None
    except Exception as e:
        logger.debug(f"Could not fetch system price: {e}")
        return None
