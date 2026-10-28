"""Download DISPATCH_UNIT_SCADA and DISPATCHLOAD via NEMOSIS.

Downloads one month at a time to keep memory manageable. Returns raw
5-minute interval data for aggregation.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import tempfile
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
from nemosis import dynamic_data_compiler

from . import config

logger = logging.getLogger(__name__)


def fetch_scada_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download DISPATCH_UNIT_SCADA for a single month via NEMOSIS.

    Returns DataFrame with columns: SETTLEMENTDATE, DUID, SCADAVALUE
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCH_UNIT_SCADA for {year}-{month:02d}...")
    scada = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name="DISPATCH_UNIT_SCADA",
        raw_data_location=nemosis_cache,
        select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
        fformat="parquet",
        rebuild=rebuild,
    )

    if scada is None or scada.empty:
        logger.warning(f"No SCADA data for {year}-{month:02d}")
        return pd.DataFrame()

    scada["SETTLEMENTDATE"] = pd.to_datetime(scada["SETTLEMENTDATE"])
    scada["SCADAVALUE"] = pd.to_numeric(scada["SCADAVALUE"], errors="coerce")
    scada = scada.dropna(subset=["SCADAVALUE"])

    logger.info(f"SCADA {year}-{month:02d}: {len(scada):,} rows, "
                f"{scada['DUID'].nunique()} DUIDs")
    return scada


def fetch_dispatchload_month(
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
) -> pd.DataFrame:
    """Download DISPATCHLOAD (AVAILABILITY, UIGF) for a single month via NEMOSIS.

    Returns DataFrame with columns: SETTLEMENTDATE, DUID, AVAILABILITY, UIGF.

    UIGF (unconstrained intermittent generation forecast) is requested for a
    future measured-curtailed-energy methodology (S3-04), not for the current
    shortfall proxy. Historical schema boundary: UIGF is populated only for
    intermittent (semi-scheduled) units — other unit types report no value —
    and older MMS archive vintages may not carry the column at all, so the
    output tolerates its absence (column is NaN-filled rather than fatal).
    """
    nemosis_cache = str(Path(cache_dir) / "nemosis_cache")
    Path(nemosis_cache).mkdir(parents=True, exist_ok=True)

    start_time = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end_time = f"{year + 1}/01/01 00:00:00"
    else:
        end_time = f"{year}/{month + 1:02d}/01 00:00:00"

    logger.info(f"Fetching DISPATCHLOAD for {year}-{month:02d}...")
    uigf_requested = True
    try:
        dispatch = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=nemosis_cache,
            select_columns=["SETTLEMENTDATE", "DUID", "AVAILABILITY", "INTERVENTION", "UIGF"],
            fformat="parquet",
            rebuild=rebuild,
        )
    except Exception as exc:
        # Historical schema boundary: UIGF availability depends on the raw
        # archive vintage AND the installed nemosis version (its table spec
        # may not know the column). Retry without it rather than failing the
        # monthly lane — UIGF is a pass-through for the future S3-04
        # methodology, not required by the current proxy metric.
        logger.warning(
            f"DISPATCHLOAD fetch with UIGF failed for {year}-{month:02d} "
            f"({type(exc).__name__}: {exc}); retrying without UIGF"
        )
        uigf_requested = False
        dispatch = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=nemosis_cache,
            select_columns=["SETTLEMENTDATE", "DUID", "AVAILABILITY", "INTERVENTION"],
            fformat="parquet",
            rebuild=rebuild,
        )

    if dispatch is None or dispatch.empty:
        logger.warning(f"No DISPATCHLOAD data for {year}-{month:02d}")
        return pd.DataFrame()

    dispatch["INTERVENTION"] = pd.to_numeric(dispatch["INTERVENTION"], errors="coerce")
    dispatch = dispatch[dispatch["INTERVENTION"] == 0].copy()
    dispatch.drop(columns=["INTERVENTION"], inplace=True)

    dispatch["SETTLEMENTDATE"] = pd.to_datetime(dispatch["SETTLEMENTDATE"])
    dispatch["AVAILABILITY"] = pd.to_numeric(dispatch["AVAILABILITY"], errors="coerce")
    dispatch = dispatch.dropna(subset=["AVAILABILITY"])

    # UIGF: informational pass-through (future methodology input). Absent on
    # historical archive vintages -> NaN column; only intermittent units
    # report values.
    if "UIGF" in dispatch.columns:
        dispatch["UIGF"] = pd.to_numeric(dispatch["UIGF"], errors="coerce")
        uigf_duids = dispatch.loc[dispatch["UIGF"].notna(), "DUID"].nunique()
    else:
        dispatch["UIGF"] = float("nan")
        uigf_duids = 0

    logger.info(f"DISPATCHLOAD {year}-{month:02d}: {len(dispatch):,} rows, "
                f"{dispatch['DUID'].nunique()} DUIDs, {uigf_duids} with UIGF"
                + ("" if uigf_requested else " (UIGF unavailable — schema boundary)"))
    return dispatch



# NEMWEB keeps each market day's Next_Day_Dispatch file in Current for a few
# weeks, then inside a monthly bundle in ARCHIVE. nemosis's
# NEXT_DAY_DISPATCHLOAD reads only Current, so older days are fetched here.
_NEXT_DAY_ARCHIVE_URL = (
    "https://nemweb.com.au/Reports/ARCHIVE/Next_Day_Dispatch/"
    "PUBLIC_NEXT_DAY_DISPATCH_{year:04d}{month:02d}01.zip"
)
_NEXT_DAY_CURRENT_URL = "https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/"
_UNIT_SOLUTION_COLUMNS = ["SETTLEMENTDATE", "DUID", "INTERVENTION", "AVAILABILITY"]


def heal_dispatchload_gaps(
    dispatch: pd.DataFrame,
    year: int,
    month: int,
    cache_dir: str,
    rebuild: bool = False,
    now: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Fill whole 5-minute intervals the monthly DISPATCHLOAD archive lacks.

    AEMO's MMSDM archive can stop short of the month end: the September 2025
    DISPATCHLOAD file ends at 2025/09/30 04:00 (4,417,392 rows, then END OF
    REPORT), so the last 240 intervals had no AVAILABILITY and every
    wind/solar unit's curtailment for that month covered 29 days of 30. The
    same intervals are in NEMWEB's Next_Day_Dispatch file for that market day
    (the same DISPATCH UNIT_SOLUTION rows; no UIGF).

    Only intervals missing for EVERY unit are filled, and only for a month
    that has ended at least two days ago (Next_Day_Dispatch publishes the
    morning after the market day) and already has some archive rows. An
    empty frame is publication lag, not a gap, and is returned unchanged.
    """
    if dispatch is None or dispatch.empty:
        return dispatch

    month_start = pd.Timestamp(year=year, month=month, day=1)
    month_end = month_start + pd.offsets.MonthBegin(1)
    now = pd.Timestamp.now() if now is None else now
    if month_end > now - pd.Timedelta(days=2):
        return dispatch

    expected = pd.date_range(month_start + pd.Timedelta(minutes=5), month_end, freq="5min")
    missing = expected.difference(pd.DatetimeIndex(dispatch["SETTLEMENTDATE"].unique()))
    if missing.empty:
        return dispatch

    month_label = f"{year}-{month:02d}"
    logger.warning(
        f"DISPATCHLOAD {month_label}: archive lacks {len(missing)} of {len(expected)} "
        f"intervals ({missing.min()} to {missing.max()}); filling from Next_Day_Dispatch"
    )

    # A market day runs 04:05 to 04:00 the next morning (interval-end stamps).
    market_days = sorted({(t - pd.Timedelta(hours=4, minutes=5)).date() for t in missing})
    try:
        extra = fetch_next_day_unit_solution(market_days, cache_dir, rebuild=rebuild)
    except Exception as exc:
        logger.warning(
            f"Next_Day_Dispatch fill failed for {month_label} "
            f"({type(exc).__name__}: {exc}); {len(missing)} intervals stay without AVAILABILITY"
        )
        return dispatch

    extra = extra.copy()
    extra["INTERVENTION"] = pd.to_numeric(extra["INTERVENTION"], errors="coerce")
    extra = extra[extra["INTERVENTION"] == 0].drop(columns=["INTERVENTION"])
    extra["SETTLEMENTDATE"] = pd.to_datetime(extra["SETTLEMENTDATE"])
    extra["AVAILABILITY"] = pd.to_numeric(extra["AVAILABILITY"], errors="coerce")
    extra = extra.dropna(subset=["AVAILABILITY"])
    extra = extra[extra["SETTLEMENTDATE"].isin(missing)].copy()
    extra["UIGF"] = float("nan")

    filled = extra["SETTLEMENTDATE"].nunique()
    logger.warning(
        f"DISPATCHLOAD {month_label}: filled {filled} of {len(missing)} missing intervals "
        f"({len(extra):,} rows) from Next_Day_Dispatch"
        + ("" if filled == len(missing) else
           f"; {len(missing) - filled} intervals stay without AVAILABILITY")
    )
    if extra.empty:
        return dispatch
    return pd.concat([dispatch, extra[dispatch.columns]], ignore_index=True)


def fetch_next_day_unit_solution(days, cache_dir: str, rebuild: bool = False) -> pd.DataFrame:
    """DISPATCH UNIT_SOLUTION rows (SETTLEMENTDATE, DUID, INTERVENTION, AVAILABILITY)
    for each market day, from NEMWEB Next_Day_Dispatch, cached per day.

    Raises FileNotFoundError when a day is neither in the monthly ARCHIVE
    bundle nor on the Current page.
    """
    frames = []
    todo = []
    for day in days:
        cache_path = Path(cache_dir) / f"next_day_unit_solution_{day:%Y%m%d}.feather"
        if cache_path.exists() and not rebuild:
            frames.append(pd.read_feather(cache_path))
        else:
            todo.append(day)

    for (year, month), month_days in _group_by_month(todo).items():
        found = _next_day_files_from_archive(year, month, month_days, cache_dir)
        for day in month_days:
            day_zip = found.get(day) or _next_day_file_from_current(day)
            if day_zip is None:
                raise FileNotFoundError(f"Next_Day_Dispatch {day:%Y-%m-%d} not on NEMWEB")
            df = _parse_unit_solution(day_zip)
            if df.empty:
                raise ValueError(f"Next_Day_Dispatch {day:%Y-%m-%d} has no UNIT_SOLUTION rows")
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            df.to_feather(Path(cache_dir) / f"next_day_unit_solution_{day:%Y%m%d}.feather")
            logger.info(f"Next_Day_Dispatch {day:%Y-%m-%d}: {len(df):,} UNIT_SOLUTION rows")
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=_UNIT_SOLUTION_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def _group_by_month(days) -> dict:
    groups: dict = {}
    for day in days:
        groups.setdefault((day.year, day.month), []).append(day)
    return groups


def _get(url: str, *, stream: bool = False) -> requests.Response | None:
    """GET with the repo's retry policy; None on 404."""
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = requests.get(
                url, timeout=config.REQUEST_TIMEOUT, stream=stream,
                headers={"User-Agent": config.USER_AGENT},
            )
            if resp.status_code == 404:
                resp.close()
                return None
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == config.MAX_RETRIES - 1:
                raise
            wait = config.RETRY_BACKOFF * (attempt + 1)
            logger.warning(f"GET {url} failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
            time.sleep(wait)
    return None


def _next_day_files_from_archive(year: int, month: int, days, cache_dir: str) -> dict:
    """Return {day: daily zip bytes} for the days found in that month's ARCHIVE bundle.

    The ~215 MB bundle is staged under cache_dir, not /tmp (a small RAM disk on the NAS).
    """
    resp = _get(_NEXT_DAY_ARCHIVE_URL.format(year=year, month=month), stream=True)
    if resp is None:
        return {}
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir=cache_dir) as tmp:
        tmp_path = tmp.name
        with resp:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                tmp.write(chunk)
    try:
        with zipfile.ZipFile(tmp_path) as bundle:
            names = bundle.namelist()
            found = {}
            for day in days:
                stem = f"PUBLIC_NEXT_DAY_DISPATCH_{day:%Y%m%d}_"
                member = next((n for n in names if Path(n).name.startswith(stem)), None)
                if member is not None:
                    found[day] = bundle.read(member)
            return found
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _next_day_file_from_current(day) -> bytes | None:
    listing = _get(_NEXT_DAY_CURRENT_URL)
    if listing is None:
        return None
    match = re.search(rf"PUBLIC_NEXT_DAY_DISPATCH_{day:%Y%m%d}_\d+\.zip", listing.text, re.IGNORECASE)
    if match is None:
        return None
    resp = _get(_NEXT_DAY_CURRENT_URL + match.group(0))
    return None if resp is None else resp.content


def _parse_unit_solution(day_zip: bytes) -> pd.DataFrame:
    """Pull the DISPATCH UNIT_SOLUTION table out of one Next_Day_Dispatch file.

    The CSV holds several MMS tables, each with its own I (header) and D (data)
    lines; columns are found by name from the UNIT_SOLUTION header.
    """
    rows = []
    idx = None
    with zipfile.ZipFile(io.BytesIO(day_zip)) as zf:
        name = next(n for n in zf.namelist() if n.upper().endswith(".CSV"))
        with zf.open(name) as fh:
            for line in io.TextIOWrapper(fh, encoding="utf-8", errors="replace"):
                if line.startswith("I,DISPATCH,UNIT_SOLUTION,"):
                    header = next(csv.reader([line]))
                    idx = [header.index(col) for col in _UNIT_SOLUTION_COLUMNS]
                elif idx is not None and line.startswith("D,DISPATCH,UNIT_SOLUTION,"):
                    parts = next(csv.reader([line]))
                    rows.append([parts[i] for i in idx])
    return pd.DataFrame(rows, columns=_UNIT_SOLUTION_COLUMNS)
