"""Per-DUID FCAS participation factors from BIDPEROFFER_D offers.

Regional DISPATCHPRICE averages (aggregate.aggregate_fcas_prices) describe the
market, not the generator — every unit in a region gets identical numbers.
This module computes the generator-specific complement: which FCAS services a
unit actually offered into, how often, and how much capacity it offered.

Telemetry caveat: BIDPEROFFER_D carries offers, not enablement. A unit that
offers FCAS without being enabled earns nothing; treat these factors as
participation/offer behaviour, not settled FCAS revenue. Revenue-grade data
would require Next_Day_Offer_Engine dispatch enablement (future work — see
docs/FUTURE_DATA_SOURCES.md).
"""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date

import pandas as pd

from .download_bids import FCAS_BID_TYPES
from .interval_days import interval_calendar_day

logger = logging.getLogger(__name__)

# Human-readable labels, consistent with aggregate.FCAS_LABELS ordering
FCAS_SERVICE_LABELS = {
    "RAISE6SEC": "Raise 6s",
    "RAISE60SEC": "Raise 60s",
    "RAISE5MIN": "Raise 5min",
    "RAISEREG": "Raise Reg",
    "LOWER6SEC": "Lower 6s",
    "LOWER60SEC": "Lower 60s",
    "LOWER5MIN": "Lower 5min",
    "LOWERREG": "Lower Reg",
}


def compute_fcas_factors(
    bids: pd.DataFrame,
    year: int,
    month: int,
) -> pd.DataFrame:
    """Aggregate one month of FCAS offer rows to per-DUID participation factors.

    S3-05 coverage contract: zero MAXAVAIL rows are retained upstream
    (download_bids) as observed source coverage, so a month's factor row can
    distinguish intervals the unit offered INTO (observed) from intervals it
    offered positive capacity (positive). Each row carries both counts plus a
    source-completeness flag: the month counts as source-complete only when
    the frame's last observed interval calendar day is the month's final day.
    A month whose archive/cache was a mid-month fragment is flagged
    ``fcas_source_complete=False`` with ``fcas_observed_through`` set, so the
    attach layer never presents it as an unqualified full-month statistic.

    Participation and offered-capacity fields keep their historical
    semantics (computed over POSITIVE offers only), so complete-month values
    stay comparable month to month and with rows produced by older code.

    Args:
        bids: DataFrame from download_bids.fetch_fcas_bids_month
              (INTERVAL_DATETIME, DUID, BIDTYPE, MAXAVAIL, ENABLEMENTMIN/MAX)
        year, month: period (for the minutes-in-month denominator)

    Returns:
        DataFrame, one row per DUID with columns:
        duid, month,
        fcas_services_offered        (count 0-8, services with any offer incl. zero),
        fcas_offer_minutes           (POSITIVE intervals × 5, all services summed),
        fcas_participation_pct       (positive intervals / intervals in month —
                                      share of the month the unit offered positive
                                      capacity into at least one service),
        fcas_observed_intervals      (distinct intervals with ANY offer incl. zero),
        fcas_positive_intervals      (distinct intervals with MAXAVAIL > 0),
        fcas_source_complete         (frame reached the month's final calendar day),
        fcas_observed_through        (last observed interval day, only when partial),
        fcas_avg_max_avail_mw        (mean offered MW across positive offers),
        fcas_max_max_avail_mw        (peak positive offered MW),
        and per-service avg positive offered MW: fcas_avg_<BIDTYPE>_mw
    """
    month_label = f"{year}-{month:02d}"
    intervals_in_month = monthrange(year, month)[1] * 24 * 12

    if bids is None or bids.empty:
        return pd.DataFrame()

    bids = bids.copy()
    bids["INTERVAL_DATETIME"] = pd.to_datetime(bids["INTERVAL_DATETIME"])
    bids = bids.dropna(subset=["INTERVAL_DATETIME"])
    if bids.empty:
        return pd.DataFrame()

    # Source completeness at the FRAME level: did the fetched archive/cache
    # for this month reach the month's final calendar day? A mid-month
    # fragment (partial cache) stops early for every unit; a complete archive
    # always has some unit offering on the final day.
    last_day = interval_calendar_day(bids["INTERVAL_DATETIME"]).max()
    _, last_cal = monthrange(year, month)
    source_complete = bool(last_day == date(year, month, last_cal))
    observed_through = last_day.isoformat() if not source_complete else None

    positive = bids[bids["MAXAVAIL"] > 0]
    pos_by_duid = {
        duid: g for duid, g in positive.groupby("DUID")
    } if not positive.empty else {}

    rows = []
    for duid, g in bids.groupby("DUID"):
        services = g["BIDTYPE"].nunique()
        observed_intervals = g["INTERVAL_DATETIME"].nunique()
        gp = pos_by_duid.get(duid)
        positive_intervals = (
            int(gp["INTERVAL_DATETIME"].nunique())
            if gp is not None and not gp.empty else 0
        )
        # Avg per-service participation: positive intervals / intervals in month
        participation = positive_intervals / intervals_in_month if intervals_in_month else None

        per_service = {}
        for btype in sorted(g["BIDTYPE"].unique()):
            sgp = g[(g["BIDTYPE"] == btype) & (g["MAXAVAIL"] > 0)]
            if sgp.empty:
                continue  # offered only at zero — no positive-capacity average
            per_service[f"fcas_avg_{btype}_mw"] = round(float(sgp["MAXAVAIL"].mean()), 2)

        avg_mw = (
            round(float(gp["MAXAVAIL"].mean()), 2)
            if gp is not None and not gp.empty else None
        )
        max_mw = (
            round(float(gp["MAXAVAIL"].max()), 2)
            if gp is not None and not gp.empty else None
        )

        rows.append({
            "duid": duid,
            "month": month_label,
            "fcas_services_offered": int(services),
            "fcas_offer_minutes": int(positive_intervals * 5),
            "fcas_participation_pct": round(participation, 6) if participation is not None else None,
            "fcas_observed_intervals": int(observed_intervals),
            "fcas_positive_intervals": positive_intervals,
            "fcas_source_complete": source_complete,
            "fcas_observed_through": observed_through,
            "fcas_avg_max_avail_mw": avg_mw,
            "fcas_max_max_avail_mw": max_mw,
            **per_service,
        })

    result = pd.DataFrame(rows)
    logger.info(
        f"FCAS factors {month_label}: {len(result)} DUIDs, "
        f"{result['fcas_services_offered'].max() if not result.empty else 0} max services, "
        f"source_complete={source_complete}"
    )
    return result


def attach_fcas_factor_doc(
    doc: dict,
    factor_rows: pd.DataFrame | None,
) -> None:
    """Attach per-DUID FCAS participation factors to a generator JSON doc.

    S3-05 source-coverage contract: the headline month is the LATEST month
    whose source was complete (``fcas_source_complete`` True) — a complete,
    comparable month. A newer partial fragment (e.g. a mid-month archive
    cache) is never promoted to an unqualified full-month statistic; it is
    reported under ``later_partial_window`` with its observed counts and
    cutoff date. When only partial rows exist, the latest one is used but
    explicitly labelled ``window: "partial"`` with ``observed_through``.
    Rows cached before the coverage columns existed carry no flag
    (coverage unknown) and, when nothing complete is available, retain the
    historical latest-month behaviour rather than inventing coverage.
    """
    if factor_rows is None or factor_rows.empty:
        return
    rs = factor_rows.sort_values("month").reset_index(drop=True)
    has_cov = {"fcas_source_complete", "fcas_observed_intervals"} <= set(rs.columns)

    base_idx = len(rs) - 1  # legacy fallback: latest month
    if has_cov:
        complete_mask = rs["fcas_source_complete"].fillna(False).astype(bool)
        complete_idx = rs.index[complete_mask]
        if len(complete_idx):
            base_idx = complete_idx[-1]
    base = rs.loc[base_idx]

    def _f(series, idx, col):
        v = series.loc[idx, col] if col in series.columns else None
        return None if v is None or pd.isna(v) else float(v)

    def _i(series, idx, col):
        v = series.loc[idx, col] if col in series.columns else None
        return None if v is None or pd.isna(v) else int(v)

    out = {
        "month": base["month"],
        "services_offered": int(base["fcas_services_offered"]),
        "offer_minutes": int(base["fcas_offer_minutes"]),
        "participation_pct": _f(rs, base_idx, "fcas_participation_pct"),
        "avg_max_avail_mw": _f(rs, base_idx, "fcas_avg_max_avail_mw"),
        "max_max_avail_mw": _f(rs, base_idx, "fcas_max_max_avail_mw"),
        "note": (
            "BIDPEROFFER_D offer behaviour (latest complete month, or "
            "explicit partial window when none is complete). Offers, not "
            "settled revenue or enablement. FCAS prices in the 'fcas' block "
            "are REGIONAL market averages shared by all generators in the "
            "region."
        ),
    }

    if has_cov:
        flag = base.get("fcas_source_complete")
        if flag is not None and not pd.isna(flag):
            base_complete = bool(flag)
            out["observed_intervals"] = _i(rs, base_idx, "fcas_observed_intervals")
            out["positive_intervals"] = _i(rs, base_idx, "fcas_positive_intervals")
            out["source_complete"] = base_complete
            if not base_complete:
                out["window"] = "partial"
                through = base.get("fcas_observed_through")
                out["observed_through"] = None if pd.isna(through) else str(through)
        # Newer partial fragments than the chosen complete month: record, never promote.
        flagged = rs.loc[rs["fcas_source_complete"].notna()]
        if not flagged.empty:
            later_partial = flagged.loc[
                (flagged["month"] > base["month"])
                & (~flagged["fcas_source_complete"].astype(bool))
            ]
            if len(later_partial):
                lp = later_partial.iloc[-1]
                lp_through = lp.get("fcas_observed_through")
                out["later_partial_window"] = {
                    "month": lp["month"],
                    "observed_intervals": _i(later_partial, lp.name, "fcas_observed_intervals"),
                    "positive_intervals": _i(later_partial, lp.name, "fcas_positive_intervals"),
                    "observed_through": None if pd.isna(lp_through) else str(lp_through),
                }

    doc["fcas_participation"] = out
