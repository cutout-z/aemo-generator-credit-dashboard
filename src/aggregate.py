"""Monthly per-generator aggregation from 5-minute SCADA + price data.

This is the core analytics module that computes:
- Generation MWh
- Revenue (spot)
- Capacity factor
- Curtailment %
- Captured price and price capture ratio
- Price distribution buckets
"""

from __future__ import annotations

import logging
from calendar import monthrange

import numpy as np
import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def aggregate_month(
    scada: pd.DataFrame,
    prices: pd.DataFrame,
    dispatchload: pd.DataFrame | None,
    generators: pd.DataFrame,
    mlf_lookup: dict[str, float] | None,
    year: int,
    month: int,
    intermittent_scada: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Aggregate a single month of 5-minute data to per-generator monthly metrics.

    Args:
        scada: DISPATCH_UNIT_SCADA with SETTLEMENTDATE, DUID, SCADAVALUE
        prices: DISPATCHPRICE with SETTLEMENTDATE, REGIONID, RRP
        dispatchload: DISPATCHLOAD with SETTLEMENTDATE, DUID, AVAILABILITY (or None)
        generators: Generator metadata with DUID, REGION, CAPACITY_MW, FUEL_CATEGORY
        mlf_lookup: Dict mapping DUID -> current MLF value (or None for no MLF adjustment)
        year: Year being processed
        month: Month being processed

    Returns:
        DataFrame with one row per DUID that had SCADA data this month.
        Columns: duid, month, generation_mwh, revenue_aud, capacity_factor,
                 curtailment_pct, captured_price, avg_rrp, price_capture_ratio,
                 price_dist_* columns
    """
    if scada.empty:
        return pd.DataFrame()

    month_label = f"{year}-{month:02d}"
    hours_in_month = monthrange(year, month)[1] * 24

    # Build DUID -> region lookup
    duid_region = generators.set_index("DUID")["REGION"].to_dict()
    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()
    duid_fuel = generators.set_index("DUID")["FUEL_CATEGORY"].to_dict()

    # Assign region to SCADA rows
    scada = scada.copy()
    scada["REGIONID"] = scada["DUID"].map(duid_region)
    scada = scada.dropna(subset=["REGIONID"])

    # Merge SCADA with prices on (SETTLEMENTDATE, REGIONID)
    merged = pd.merge(
        scada,
        prices[["SETTLEMENTDATE", "REGIONID", "RRP"]],
        on=["SETTLEMENTDATE", "REGIONID"],
        how="left",
    )

    # Merge with dispatchload for curtailment
    if dispatchload is not None and not dispatchload.empty:
        merged = pd.merge(
            merged,
            dispatchload[["SETTLEMENTDATE", "DUID", "AVAILABILITY"]],
            on=["SETTLEMENTDATE", "DUID"],
            how="left",
        )
    else:
        merged["AVAILABILITY"] = np.nan

    rows = []
    for duid, group in merged.groupby("DUID"):
        capacity = duid_capacity.get(duid)
        fuel = duid_fuel.get(duid, "Other")
        mlf = mlf_lookup.get(duid, 1.0) if mlf_lookup else 1.0

        # MWh: 5-minute intervals → divide by 12 to get MWh
        mwh = group["SCADAVALUE"].clip(lower=0).sum() / 12.0

        # Revenue: MWh_interval × RRP × MLF, summed
        group_valid = group.dropna(subset=["RRP"])
        revenue = (group_valid["SCADAVALUE"].clip(lower=0) / 12.0 * group_valid["RRP"] * mlf).sum()

        # Capacity factor
        cap_factor = mwh / (capacity * hours_in_month) if capacity and capacity > 0 else None

        # Curtailment (solar/wind only)
        curtailment = None
        econ_curtailment = None
        grid_curtailment = None
        mech_curtailment = None
        if fuel in config.CURTAILMENT_FUEL_TYPES:
            avail_valid = group.dropna(subset=["AVAILABILITY"])
            if not avail_valid.empty:
                total_actual = avail_valid["SCADAVALUE"].clip(lower=0).sum()
                total_avail = avail_valid["AVAILABILITY"].clip(lower=0).sum()
                if total_avail > 0:
                    curtailment = max(0.0, 1.0 - total_actual / total_avail)

            # Split curtailment using INTERMITTENT_GEN_SCADA quality flags.
            # We apportion the total curtailment (from DISPATCHLOAD) into grid
            # vs mechanical based on the quality flag ratio from INTERMITTENT_GEN_SCADA.
            if curtailment is not None and curtailment > 0 and intermittent_scada is not None and not intermittent_scada.empty:
                duid_int = intermittent_scada[intermittent_scada["DUID"] == duid]
                if not duid_int.empty:
                    elav = duid_int[duid_int["SCADA_TYPE"] == "ELAV"]
                    if not elav.empty:
                        total_intervals = len(elav)
                        good_intervals = len(elav[elav["SCADA_QUALITY"] == "Good"])
                        bad_intervals = total_intervals - good_intervals
                        if total_intervals > 0:
                            # Good quality = grid is constraining output
                            # Bad quality = mechanical/comms issue
                            good_ratio = good_intervals / total_intervals
                            grid_curtailment = curtailment * good_ratio
                            mech_curtailment = curtailment * (1 - good_ratio)

            # Economic curtailment: generation forgone during negative price periods
            # Intervals where AVAILABILITY > 0 AND RRP < 0 AND SCADA is low
            avail_price = group.dropna(subset=["AVAILABILITY", "RRP"])
            if not avail_price.empty:
                neg_price = avail_price[avail_price["RRP"] < 0]
                if not neg_price.empty:
                    avail_during_neg = neg_price["AVAILABILITY"].clip(lower=0).sum()
                    actual_during_neg = neg_price["SCADAVALUE"].clip(lower=0).sum()
                    total_avail_all = avail_price["AVAILABILITY"].clip(lower=0).sum()
                    if total_avail_all > 0 and avail_during_neg > 0:
                        forgone = max(0.0, avail_during_neg - actual_during_neg)
                        econ_curtailment = forgone / total_avail_all

        # Captured price (volume-weighted average RRP)
        captured = None
        avg_rrp = None
        pcr = None
        if not group_valid.empty:
            gen_mwh = group_valid["SCADAVALUE"].clip(lower=0)
            total_gen = gen_mwh.sum()
            if total_gen > 0:
                captured = (gen_mwh * group_valid["RRP"]).sum() / total_gen
            avg_rrp = group_valid["RRP"].mean()
            if avg_rrp and avg_rrp != 0 and captured is not None:
                pcr = captured / avg_rrp

        # Price distribution buckets
        dist = _price_distribution(group_valid, config.PRICE_BINS, config.PRICE_BIN_LABELS)

        row = {
            "duid": duid,
            "month": month_label,
            "generation_mwh": round(mwh, 1),
            "revenue_aud": round(revenue, 0),
            "capacity_factor": round(cap_factor, 4) if cap_factor is not None else None,
            "curtailment_pct": round(curtailment, 4) if curtailment is not None else None,
            "grid_curtailment_pct": round(grid_curtailment, 4) if grid_curtailment is not None else None,
            "mechanical_curtailment_pct": round(mech_curtailment, 4) if mech_curtailment is not None else None,
            "econ_curtailment_pct": round(econ_curtailment, 4) if econ_curtailment is not None else None,
            "captured_price": round(captured, 2) if captured is not None else None,
            "avg_rrp": round(avg_rrp, 2) if avg_rrp is not None else None,
            "price_capture_ratio": round(pcr, 4) if pcr is not None else None,
        }
        # Add price distribution
        for label, share in zip(config.PRICE_BIN_LABELS, dist):
            row[f"price_dist_{label}"] = round(share, 4)

        rows.append(row)

    result = pd.DataFrame(rows)
    logger.info(f"Aggregated {month_label}: {len(result)} generators with data")
    return result


def _price_distribution(
    df: pd.DataFrame,
    bins: list[float],
    labels: list[str],
) -> list[float]:
    """Compute generation-weighted price distribution across bins.

    Returns list of shares (summing to ~1.0) for each bin.
    """
    if df.empty or "RRP" not in df.columns:
        return [0.0] * len(labels)

    gen = df["SCADAVALUE"].clip(lower=0)
    total = gen.sum()
    if total == 0:
        return [0.0] * len(labels)

    bin_indices = np.digitize(df["RRP"].values, bins[1:-1])  # bins without -inf/inf
    shares = []
    for i in range(len(labels)):
        mask = bin_indices == i
        shares.append(float(gen[mask].sum() / total))
    return shares


def aggregate_month_daily(
    scada: pd.DataFrame,
    generators: pd.DataFrame,
    year: int,
    month: int,
) -> pd.DataFrame:
    """Aggregate a single month of 5-minute SCADA data to daily per-generator metrics.

    Returns DataFrame with columns: duid, date, daily_generation_mwh, daily_capacity_factor
    """
    if scada.empty:
        return pd.DataFrame()

    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()

    scada = scada.copy()
    scada["date"] = scada["SETTLEMENTDATE"].dt.date.astype(str)

    rows = []
    for (duid, date_str), group in scada.groupby(["DUID", "date"]):
        capacity = duid_capacity.get(duid)
        mwh = group["SCADAVALUE"].clip(lower=0).sum() / 12.0
        cf = mwh / (capacity * 24) if capacity and capacity > 0 else None
        rows.append({
            "duid": duid,
            "date": date_str,
            "daily_generation_mwh": round(mwh, 1),
            "daily_capacity_factor": round(cf, 4) if cf is not None else None,
        })

    return pd.DataFrame(rows)


def build_mlf_lookup(
    mlf_history: pd.DataFrame,
    target_fy_start: int,
) -> dict[str, float]:
    """Build DUID -> MLF lookup for a given financial year.

    Falls back to the nearest available FY if the exact one isn't found.
    """
    if mlf_history is None or mlf_history.empty:
        return {}

    # Try exact FY first
    exact = mlf_history[mlf_history["fy_start_year"] == target_fy_start]
    if not exact.empty:
        return dict(zip(exact["DUID"], exact["mlf"]))

    # Fall back to latest available FY per DUID
    latest = mlf_history.sort_values("fy_start_year").drop_duplicates("DUID", keep="last")
    return dict(zip(latest["DUID"], latest["mlf"]))


FCAS_COLS = [
    "RAISE6SECRRP", "RAISE60SECRRP", "RAISE5MINRRP", "RAISEREGRRP",
    "LOWER6SECRRP", "LOWER60SECRRP", "LOWER5MINRRP", "LOWERREGRRP",
]

FCAS_LABELS = {
    "RAISE6SECRRP": "Raise 6s",
    "RAISE60SECRRP": "Raise 60s",
    "RAISE5MINRRP": "Raise 5min",
    "RAISEREGRRP": "Raise Reg",
    "LOWER6SECRRP": "Lower 6s",
    "LOWER60SECRRP": "Lower 60s",
    "LOWER5MINRRP": "Lower 5min",
    "LOWERREGRRP": "Lower Reg",
}


def aggregate_fcas_prices(
    prices: pd.DataFrame,
    year: int,
    month: int,
) -> dict[str, dict[str, float]]:
    """Compute monthly average FCAS prices per region.

    Returns dict mapping REGIONID -> {service_label: avg_price}.
    """
    if prices is None or prices.empty:
        return {}

    available_cols = [c for c in FCAS_COLS if c in prices.columns]
    if not available_cols:
        return {}

    month_label = f"{year}-{month:02d}"
    result = {}
    for region, group in prices.groupby("REGIONID"):
        region_fcas = {}
        for col in available_cols:
            vals = pd.to_numeric(group[col], errors="coerce").dropna()
            if not vals.empty:
                region_fcas[FCAS_LABELS[col]] = round(float(vals.mean()), 2)
        if region_fcas:
            result[region] = region_fcas

    return result


def aggregate_constraints_month(
    dispatchconstraint: pd.DataFrame,
    spdcp: pd.DataFrame,
    gencondata: pd.DataFrame,
    cp_map: dict[str, str],
    year: int,
    month: int,
) -> pd.DataFrame:
    """Aggregate binding constraint hours per DUID for a single month.

    Args:
        dispatchconstraint: Binding constraints with SETTLEMENTDATE, CONSTRAINTID, MARGINALVALUE
        spdcp: Connection point → constraint mapping
        gencondata: Constraint definitions with descriptions
        cp_map: DUID → CONNECTIONPOINTID mapping
        year, month: Period being processed

    Returns:
        DataFrame with columns: duid, month, constraint_id, description, hours_bound
    """
    if dispatchconstraint.empty or spdcp.empty:
        return pd.DataFrame()

    month_label = f"{year}-{month:02d}"

    # Build connection point → set of constraint IDs
    cp_to_constraints = spdcp.groupby("CONNECTIONPOINTID")["GENCONID"].apply(set).to_dict()

    # Build constraint ID → description lookup
    desc_lookup = {}
    if not gencondata.empty:
        desc_lookup = gencondata.set_index("GENCONID")["DESCRIPTION"].to_dict()

    # Get unique constraint IDs that were binding
    binding_ids = set(dispatchconstraint["CONSTRAINTID"].unique())

    rows = []
    for duid, cp in cp_map.items():
        if not cp:
            continue
        # Find constraints associated with this connection point
        duid_constraints = cp_to_constraints.get(cp, set())
        if not duid_constraints:
            continue

        # Find which of those constraints were actually binding
        relevant = duid_constraints & binding_ids
        if not relevant:
            continue

        # Count binding intervals per constraint
        binding_rows = dispatchconstraint[
            dispatchconstraint["CONSTRAINTID"].isin(relevant)
        ]
        for cid, cgroup in binding_rows.groupby("CONSTRAINTID"):
            intervals = len(cgroup)
            hours = intervals / 12.0  # 5-min intervals
            desc = desc_lookup.get(cid, "")
            rows.append({
                "duid": duid,
                "month": month_label,
                "constraint_id": cid,
                "description": str(desc)[:200] if desc else "",
                "hours_bound": round(hours, 1),
            })

    result = pd.DataFrame(rows)
    if not result.empty:
        logger.info(
            f"Constraints {month_label}: {len(result)} DUID×constraint pairs, "
            f"{result['duid'].nunique()} DUIDs affected"
        )
    return result
