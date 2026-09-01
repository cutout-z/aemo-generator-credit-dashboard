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

import pandas as pd

from .download_bids import FCAS_BID_TYPES

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

    Args:
        bids: DataFrame from download_bids.fetch_fcas_bids_month
              (INTERVAL_DATETIME, DUID, BIDTYPE, MAXAVAIL, ENABLEMENTMIN/MAX)
        year, month: period (for the minutes-in-month denominator)

    Returns:
        DataFrame, one row per DUID with columns:
        duid, month,
        fcas_services_offered        (count 0-8),
        fcas_offer_minutes           (intervals with MAXAVAIL > 0, all services summed),
        fcas_participation_pct       (offer_minutes / total intervals / services offered —
                                      share of intervals the unit was offering each
                                      service it offered),
        fcas_avg_max_avail_mw        (mean offered MW across offers),
        fcas_max_max_avail_mw        (peak offered MW),
        and per-service avg offered MW: fcas_avg_<BIDTYPE>_mw
    """
    month_label = f"{year}-{month:02d}"
    intervals_in_month = monthrange(year, month)[1] * 24 * 12

    if bids is None or bids.empty:
        return pd.DataFrame()

    rows = []
    for duid, g in bids.groupby("DUID"):
        services = g["BIDTYPE"].nunique()
        offer_intervals = g["INTERVAL_DATETIME"].nunique()
        # Avg per-service participation: intervals offering / intervals in month
        participation = offer_intervals / intervals_in_month if intervals_in_month else None

        per_service = {}
        for btype, sg in g.groupby("BIDTYPE"):
            per_service[f"fcas_avg_{btype}_mw"] = round(float(sg["MAXAVAIL"].mean()), 2)

        rows.append({
            "duid": duid,
            "month": month_label,
            "fcas_services_offered": int(services),
            "fcas_offer_minutes": int(offer_intervals * 5),
            "fcas_participation_pct": round(participation, 6) if participation is not None else None,
            "fcas_avg_max_avail_mw": round(float(g["MAXAVAIL"].mean()), 2),
            "fcas_max_max_avail_mw": round(float(g["MAXAVAIL"].max()), 2),
            **per_service,
        })

    result = pd.DataFrame(rows)
    logger.info(
        f"FCAS factors {month_label}: {len(result)} DUIDs, "
        f"{result['fcas_services_offered'].max() if not result.empty else 0} max services"
    )
    return result


def attach_fcas_factor_doc(
    doc: dict,
    factor_rows: pd.DataFrame | None,
) -> None:
    """Attach per-DUID FCAS participation factors to a generator JSON doc."""
    if factor_rows is None or factor_rows.empty:
        return
    latest = factor_rows.sort_values("month").iloc[-1]
    doc["fcas_participation"] = {
        "month": latest["month"],
        "services_offered": int(latest["fcas_services_offered"]),
        "offer_minutes": int(latest["fcas_offer_minutes"]),
        "participation_pct": None if pd.isna(latest["fcas_participation_pct"]) else float(latest["fcas_participation_pct"]),
        "avg_max_avail_mw": None if pd.isna(latest["fcas_avg_max_avail_mw"]) else float(latest["fcas_avg_max_avail_mw"]),
        "max_max_avail_mw": None if pd.isna(latest["fcas_max_max_avail_mw"]) else float(latest["fcas_max_max_avail_mw"]),
        "note": (
            "BIDPEROFFER_D offer behaviour (latest month). Offers, not settled "
            "revenue or enablement. FCAS prices in the 'fcas' block are REGIONAL "
            "market averages shared by all generators in the region."
        ),
    }
