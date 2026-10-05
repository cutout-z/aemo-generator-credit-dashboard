"""Loss factors in effect by date, from AEMO DUDETAILSUMMARY (MMSDM archive).

The MLF Tracker summary (src/fetch_mlf.py) carries ONE transmission loss
factor per DUID per financial year. That is not what settles revenue:

- AEMO revises factors mid-year, and the revision applies from its own
  effective date. Example: QPSFB1 1.019 -> 0.9176 from 2026-02-03. Applying
  the FY value to Feb-Jun 2026 overstated that revenue by 11.1% (audit
  2026-10, H5).
- Embedded generators also carry a DISTRIBUTIONLOSSFACTOR (145 of 560 units
  differ from 1 in FY26-27; CBWF1 0.9113) (audit 2026-10, H4).

DUDETAILSUMMARY holds both, one row per (DUID, START_DATE, END_DATE) period.
The monthly MMSDM archive file is a full-history snapshot (~380 KB zip,
~23k rows from 1998). This module downloads the newest published one, keeps
the factor periods, and maps each 5-minute interval to the period in effect.

Every dispatch type is kept. The MLF source used until 2026-04 kept only
DISPATCHTYPE == "GENERATOR" rows, which silently left every BIDIRECTIONAL
battery unadjusted (audit 2026-10, H3).

Bidirectional orientation: for DISPATCHTYPE == "BIDIRECTIONAL",
TRANSMISSIONLOSSFACTOR is the IMPORT (load/charging) MLF and SECONDARY_TLF
the EXPORT (generation/discharge) MLF. This was checked against AEMO's final
2026-27 MLF workbook, which labels each battery's "Import MLF" and "Export
MLF". For all 51 batteries whose two values differ, the 1 Jul 2026 records
match TLF = Import and SECONDARY_TLF = Export (CAPBES1 1.0263 / 0.9783; RESS1
0.9008 / 0.9862). Discharge revenue therefore takes TLF from SECONDARY_TLF,
falling back to TRANSMISSIONLOSSFACTOR (logged) only when it is missing.
GENERATOR rows always use TRANSMISSIONLOSSFACTOR, even where they carry a
SECONDARY_TLF. DUDETAILSUMMARY has a single DISTRIBUTIONLOSSFACTOR (no
secondary DLF column), so DLF is read the same way for every dispatch type.

Interval-to-period rule: an interval belongs to the calendar day of its END
timestamp minus one minute (interval_days.interval_calendar_day), and a
period is in effect on day d when START_DATE <= d < END_DATE.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from . import config
from .interval_days import interval_calendar_day

logger = logging.getLogger(__name__)

LOSS_FACTOR_CACHE = "loss_factor_periods.feather"
LOSS_FACTOR_COLUMNS = ("DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "TLF", "DLF")
# Same envelope fetch_mlf enforces for tracker values: outside it a value is a
# sentinel or units error and must never reach revenue.
PLAUSIBLE_RANGE = (0.5, 1.5)
# AEMO's open-ended END_DATE (2999-12-31) is outside the datetime64[ns] range.
OPEN_END = pd.Timestamp("2200-01-01")
# How many archive months to probe back from the previous calendar month.
MAX_ARCHIVE_LOOKBACK = 6


def _factor(value: pd.Series) -> pd.Series:
    lo, hi = PLAUSIBLE_RANGE
    v = pd.to_numeric(value, errors="coerce")
    return v.where((v >= lo) & (v <= hi))


def _date(value: pd.Series) -> pd.Series:
    s = value.astype(str).str.strip()
    s = s.where(~s.str.startswith("2999"), OPEN_END.strftime("%Y/%m/%d %H:%M:%S"))
    return pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S", errors="coerce")


def parse_dudetailsummary(raw: bytes | str) -> pd.DataFrame:
    """Factor periods from an MMSDM DUDETAILSUMMARY archive (zip bytes or CSV).

    Returns ``LOSS_FACTOR_COLUMNS``: TLF is the generation-side factor
    (TRANSMISSIONLOSSFACTOR; SECONDARY_TLF for BIDIRECTIONAL units) and DLF
    from DISTRIBUTIONLOSSFACTOR, NaN when absent or implausible. Where AEMO
    restated a period, the row with the latest LASTCHANGED wins.
    """
    if isinstance(raw, bytes) and raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            name = zf.namelist()[0]
            raw = zf.read(name)
            if name.lower().endswith(".zip"):  # nested archive
                return parse_dudetailsummary(raw)
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw

    header: list[str] | None = None
    rows: list[list[str]] = []
    for line in text.splitlines():
        if line.startswith("I,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,"):
            header = next(csv.reader([line]))
        elif header and line.startswith("D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,"):
            rows.append(next(csv.reader([line])))
    if not header or not rows:
        raise ValueError("no DUDETAILSUMMARY rows in archive")
    df = pd.DataFrame([r[: len(header)] for r in rows], columns=header)

    dispatch = df.get("DISPATCHTYPE", pd.Series("", index=df.index)).str.strip()
    tlf = _factor(df["TRANSMISSIONLOSSFACTOR"])
    bidir = dispatch == "BIDIRECTIONAL"
    if bidir.any():
        export = (
            _factor(df["SECONDARY_TLF"]) if "SECONDARY_TLF" in df.columns
            else pd.Series(float("nan"), index=df.index)
        )
        missing = bidir & export.isna()
        if missing.any():
            logger.warning(
                f"DUDETAILSUMMARY: {int(missing.sum())} BIDIRECTIONAL periods lack "
                "SECONDARY_TLF (export MLF) — falling back to TRANSMISSIONLOSSFACTOR "
                f"(import) for: {', '.join(sorted(set(df.loc[missing, 'DUID'].str.strip()))[:20])}"
            )
        tlf = tlf.where(~bidir | missing, export)

    out = pd.DataFrame({
        "DUID": df["DUID"].str.strip(),
        "START_DATE": _date(df["START_DATE"]),
        "END_DATE": _date(df["END_DATE"]),
        "DISPATCHTYPE": dispatch,
        "TLF": tlf,
        "DLF": (
            _factor(df["DISTRIBUTIONLOSSFACTOR"])
            if "DISTRIBUTIONLOSSFACTOR" in df.columns else float("nan")
        ),
        "_changed": pd.to_datetime(df.get("LASTCHANGED"), errors="coerce"),
    })
    out = out.dropna(subset=["DUID", "START_DATE", "END_DATE"])
    out = out[out["END_DATE"] > out["START_DATE"]]
    out = (
        out.sort_values(["DUID", "START_DATE", "_changed"])
        .drop_duplicates(subset=["DUID", "START_DATE"], keep="last")
        .drop(columns="_changed")
        .reset_index(drop=True)
    )
    return out[list(LOSS_FACTOR_COLUMNS)]


def _archive_months(today: datetime) -> list[tuple[int, int]]:
    """Candidate archive months, newest first, starting at the previous month."""
    y, m = today.year, today.month
    out = []
    for _ in range(MAX_ARCHIVE_LOOKBACK):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return out


def fetch_loss_factor_periods(
    cache_dir: str,
    force: bool = False,
    today: datetime | None = None,
) -> pd.DataFrame | None:
    """Newest published DUDETAILSUMMARY factor periods (cached).

    A cache that is no more than one archive month behind the newest possible
    archive is used as-is after one probe for that newest month. A failed
    download falls back to the cache. Returns None when neither is
    available: revenue then falls back to the tracker's per-FY factor, and
    the rows say so in their provenance columns.
    """
    today = today or datetime.now()
    cache_path = Path(cache_dir) / LOSS_FACTOR_CACHE
    cached = None
    cached_month = None
    if cache_path.exists():
        try:
            cached = pd.read_feather(cache_path)
            if "ARCHIVE_MONTH" in cached.columns and len(cached):
                cached_month = str(cached["ARCHIVE_MONTH"].iloc[0])
        except Exception as e:  # unreadable cache: refetch
            logger.warning(f"Loss-factor cache unreadable ({e}); refetching")
            cached = None

    candidates = _archive_months(today)
    labels = [f"{y:04d}-{m:02d}" for y, m in candidates]
    recent = cached is not None and not force and bool(cached_month) and cached_month >= labels[1]
    if recent:
        if cached_month >= labels[0]:
            return cached.drop(columns="ARCHIVE_MONTH")
        candidates = candidates[:1]  # only the newest archive could add anything

    for year, month in candidates:
        url = config.DUDETAILSUMMARY_URL_TEMPLATE.format(year=year, month=month)
        try:
            resp = requests.get(
                url, timeout=config.REQUEST_TIMEOUT,
                headers={"User-Agent": config.USER_AGENT},
            )
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            periods = parse_dudetailsummary(resp.content)
        except Exception as e:
            logger.warning(f"DUDETAILSUMMARY {year}-{month:02d}: {e}")
            continue
        label = f"{year:04d}-{month:02d}"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        periods.assign(ARCHIVE_MONTH=label).to_feather(cache_path)
        logger.info(
            f"DUDETAILSUMMARY {label}: {len(periods):,} factor periods, "
            f"{periods['DUID'].nunique():,} DUIDs"
        )
        return periods

    if cached is not None:
        log = logger.info if recent else logger.warning
        log(f"DUDETAILSUMMARY: no newer archive reachable — using cached {cached_month}")
        return cached.drop(columns="ARCHIVE_MONTH", errors="ignore")
    logger.warning(
        "DUDETAILSUMMARY unavailable and no cache — revenue falls back to the "
        "tracker's per-FY MLF and DLF 1.0"
    )
    return None


def interval_loss_factors(
    intervals: pd.DataFrame,
    periods: pd.DataFrame | None,
) -> pd.DataFrame:
    """TLF/DLF in effect for each (SETTLEMENTDATE, DUID) row of ``intervals``.

    Returns a frame aligned to ``intervals.index`` with columns TLF and DLF.
    Both are NaN where no period covers the interval's day (or the period's
    value is missing), so callers can tell "no published factor" from 1.0.
    """
    out = pd.DataFrame({"TLF": float("nan"), "DLF": float("nan")}, index=intervals.index)
    if periods is None or periods.empty or intervals.empty:
        return out
    left = pd.DataFrame({
        "DUID": intervals["DUID"].values,
        "_day": pd.to_datetime(interval_calendar_day(intervals["SETTLEMENTDATE"])).values,
        "_pos": range(len(intervals)),
    }).sort_values("_day")
    right = (
        periods[periods["DUID"].isin(set(left["DUID"]))]
        [["DUID", "START_DATE", "END_DATE", "TLF", "DLF"]]
        .sort_values("START_DATE")
    )
    if right.empty:
        return out
    hit = pd.merge_asof(
        left, right, left_on="_day", right_on="START_DATE", by="DUID",
        direction="backward",
    )
    covered = hit["END_DATE"].notna() & (hit["_day"] < hit["END_DATE"])
    hit.loc[~covered, ["TLF", "DLF"]] = float("nan")
    hit = hit.sort_values("_pos")
    out["TLF"] = hit["TLF"].to_numpy()
    out["DLF"] = hit["DLF"].to_numpy()
    return out


def fy_opening_tlf(periods: pd.DataFrame | None, fy_start_year: int) -> dict[str, float]:
    """{DUID: TLF of the DUID's first period in effect during the FY}.

    aggregate_month compares this with the tracker's FY factor before
    trusting DUDETAILSUMMARY's dated values for that DUID (see there).
    """
    if periods is None or periods.empty:
        return {}
    a = pd.Timestamp(year=fy_start_year, month=7, day=1)
    b = pd.Timestamp(year=fy_start_year + 1, month=7, day=1)
    q = periods[(periods["START_DATE"] < b) & (periods["END_DATE"] > a)].dropna(subset=["TLF"])
    if q.empty:
        return {}
    first = q.sort_values("START_DATE").drop_duplicates("DUID", keep="first")
    return dict(zip(first["DUID"], first["TLF"].astype(float)))
