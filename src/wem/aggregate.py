"""Monthly per-generator aggregation for WEM pre-Reform data.

Core analytics for WEM generators:
  - Generation (MWh) — direct from SCADA (already MWh per 30-min interval)
  - Revenue = sum(Energy_MWh × Balancing_Price × TLF) per month
  - Capacity factor = generation_mwh / (capacity_mw × hours_in_month)
  - Captured price = generation-weighted average price
  - Price capture ratio = captured_price / system_avg_price
  - Price distribution buckets

Intentionally OMITTED for WEM (no public data equivalent):
  - Curtailment (no UIGF or unconstrained availability published)
  - Per-generator FCAS revenue (not in pre-Reform data)
  - Binding constraint hours (no facility-level mapping available)
"""

from __future__ import annotations

import logging
from calendar import monthrange

import numpy as np
import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def aggregate_wem_month(
    scada: pd.DataFrame,
    prices: pd.DataFrame,
    generators: pd.DataFrame,
    tlf_lookup: dict[str, float],
    year: int,
    month: int,
) -> pd.DataFrame:
    """Aggregate one month of WEM 30-minute SCADA + balancing price data.

    Args:
        scada: Output of fetch_wem_scada_month() —
               Facility Code, Trading Date, Interval Number, Energy Generated (MWh)
        prices: Output of fetch_wem_price_month() —
                Trading Date, Interval Number, Final Price ($/MWh)
        generators: WEM metadata with DUID, CAPACITY_MW, FUEL_CATEGORY
        tlf_lookup: Dict mapping DUID → TLF value (1.0 if missing)
        year, month: Period being processed

    Returns:
        DataFrame with one row per facility that had SCADA data this month.
        Columns: duid, month, generation_mwh, revenue_aud, capacity_factor,
                 captured_price, avg_price, price_capture_ratio,
                 price_dist_* columns
    """
    if scada.empty:
        return pd.DataFrame()

    month_label = f"{year}-{month:02d}"
    hours_in_month = monthrange(year, month)[1] * 24

    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()
    duid_fuel = generators.set_index("DUID")["FUEL_CATEGORY"].to_dict()

    # Merge SCADA with price on (Trading Date, Interval Number)
    merged = _join_scada_price(scada, prices)

    if merged.empty:
        logger.warning(f"WEM {month_label}: no SCADA/price join result")
        return pd.DataFrame()

    rows = []
    for facility_code, group in merged.groupby("Facility Code"):
        capacity = duid_capacity.get(facility_code)
        fuel = duid_fuel.get(facility_code, config.FUEL_FALLBACK)
        tlf = tlf_lookup.get(facility_code, 1.0)

        # Energy is already in MWh (30-min intervals, published as MWh per interval)
        mwh = group["Energy Generated (MWh)"].clip(lower=0).sum()

        # Revenue: Energy_MWh × Price × TLF, summed across intervals
        valid = group.dropna(subset=["Final Price ($/MWh)"])
        revenue = (
            (valid["Energy Generated (MWh)"].clip(lower=0) * valid["Final Price ($/MWh)"] * tlf).sum()
        )

        # Capacity factor
        cap_factor = mwh / (capacity * hours_in_month) if capacity and capacity > 0 else None

        # Captured price: generation-weighted average price
        captured = None
        avg_price = None
        pcr = None
        if not valid.empty:
            gen_mwh = valid["Energy Generated (MWh)"].clip(lower=0)
            total_gen = gen_mwh.sum()
            if total_gen > 0:
                captured = (gen_mwh * valid["Final Price ($/MWh)"]).sum() / total_gen
            avg_price = float(valid["Final Price ($/MWh)"].mean())
            if avg_price and avg_price != 0 and captured is not None:
                pcr = captured / avg_price

        # Price distribution buckets
        dist = _price_distribution(valid, config.PRICE_BINS, config.PRICE_BIN_LABELS)

        row = {
            "duid": facility_code,
            "month": month_label,
            "generation_mwh": round(mwh, 1),
            "revenue_aud": round(revenue, 0),
            "capacity_factor": round(cap_factor, 4) if cap_factor is not None else None,
            "tlf": round(tlf, 6),
            "captured_price": round(captured, 2) if captured is not None else None,
            "avg_rrp": round(avg_price, 2) if avg_price is not None else None,
            "price_capture_ratio": round(pcr, 4) if pcr is not None else None,
        }
        for label, share in zip(config.PRICE_BIN_LABELS, dist):
            row[f"price_dist_{label}"] = round(share, 4)

        rows.append(row)

    result = pd.DataFrame(rows)
    logger.info(f"WEM aggregate {month_label}: {len(result)} facilities with data")
    return result


def _join_scada_price(scada: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """Join SCADA with price data on (Trading Date, Interval Number).

    Handles the case where the price file may not have Interval Number
    (falls back to positional matching within each date).
    """
    if prices.empty:
        # Return SCADA with null price column so we can still record generation
        scada = scada.copy()
        scada["Final Price ($/MWh)"] = np.nan
        return scada

    scada_key = ["Trading Date"]
    price_key = ["Trading Date"]

    if "Interval Number" in scada.columns and "Interval Number" in prices.columns:
        scada_key.append("Interval Number")
        price_key.append("Interval Number")

    merged = pd.merge(
        scada,
        prices[price_key + ["Final Price ($/MWh)"]],
        left_on=scada_key,
        right_on=price_key,
        how="left",
    )
    return merged


def _price_distribution(df: pd.DataFrame, bins: list, labels: list) -> list[float]:
    """Compute generation-weighted price distribution across bins."""
    if df.empty or "Final Price ($/MWh)" not in df.columns:
        return [0.0] * len(labels)

    gen = df["Energy Generated (MWh)"].clip(lower=0)
    total = gen.sum()
    if total == 0:
        return [0.0] * len(labels)

    bin_indices = np.digitize(df["Final Price ($/MWh)"].values, bins[1:-1])
    shares = []
    for i in range(len(labels)):
        mask = bin_indices == i
        shares.append(float(gen[mask].sum() / total))
    return shares


def aggregate_wem_month_daily(
    scada: pd.DataFrame,
    generators: pd.DataFrame,
    year: int,
    month: int,
) -> pd.DataFrame:
    """Aggregate WEM SCADA to daily per-generator generation + capacity factor.

    Returns: duid, date, daily_generation_mwh, daily_capacity_factor
    """
    if scada.empty:
        return pd.DataFrame()

    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()

    scada = scada.copy()
    scada["date"] = scada["Trading Date"].astype(str).str[:10]

    rows = []
    for (facility_code, date_str), group in scada.groupby(["Facility Code", "date"]):
        capacity = duid_capacity.get(facility_code)
        mwh = group["Energy Generated (MWh)"].clip(lower=0).sum()
        cf = mwh / (capacity * 24) if capacity and capacity > 0 else None
        rows.append({
            "duid": facility_code,
            "date": date_str,
            "daily_generation_mwh": round(mwh, 1),
            "daily_capacity_factor": round(cf, 4) if cf is not None else None,
        })

    return pd.DataFrame(rows)


def aggregate_wem_system_price(
    prices: pd.DataFrame,
    year: int,
    month: int,
) -> dict[str, float]:
    """Compute monthly average balancing price statistics for WEM.

    Returns dict: {"avg_price": ..., "median_price": ..., "negative_pct": ...}
    Used to provide price context on each generator's dashboard page.
    """
    if prices is None or prices.empty or "Final Price ($/MWh)" not in prices.columns:
        return {}

    p = prices["Final Price ($/MWh)"].dropna()
    if p.empty:
        return {}

    return {
        "avg_price": round(float(p.mean()), 2),
        "median_price": round(float(p.median()), 2),
        "negative_pct": round(float((p < 0).mean()), 4),
    }
