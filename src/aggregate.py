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
from .interval_days import INTERVALS_PER_DAY, interval_calendar_day_str

logger = logging.getLogger(__name__)


def aggregate_month(
    scada: pd.DataFrame,
    prices: pd.DataFrame,
    dispatchload: pd.DataFrame | None,
    generators: pd.DataFrame,
    mlf_lookup: dict[str, dict] | None,
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
        mlf_lookup: Per-DUID MLF resolution for this month's FY — dict mapping
            DUID -> {"mlf", "source_fy_start", "status"} as built by
            build_mlf_lookup (S3-11). Exact/prior-carry DUIDs are adjusted by
            their factor; status "unknown" DUIDs get UNADJUSTED revenue
            (factor 1.0) that is explicitly labelled provisional in the row's
            revenue_mlf_* provenance columns. None/empty = no MLF data at all
            (every DUID unknown — unadjusted, provisional).
        year: Year being processed
        month: Month being processed

    Returns:
        DataFrame with one row per DUID that had SCADA data this month.
        Columns: duid, month, generation_mwh, revenue_aud, capacity_factor,
                 curtailment_pct, captured_price, avg_rrp, price_capture_ratio,
                 price_dist_* columns, and (S3-04) the first-class energy
                 denominators: curtailment_actual_mwh / curtailment_potential_mwh
                 (eligible actual and availability energy behind the shortfall
                 proxy) and price_mwh_* per-bin energy behind each share.
                 (S3-11) Every row also carries the MLF provenance used for its
                 revenue_aud: revenue_mlf_status (exact/prior-carry/unknown),
                 revenue_mlf_source_fy_start (source FY or None), and
                 revenue_mlf_value (applied factor, None when unadjusted).
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

        # S3-11: resolve the factor actually applied to this DUID's revenue.
        # A missing resolution (or status "unknown") means revenue is
        # UNADJUSTED (×1.0) and is labelled provisional — never a silent 1.0
        # masquerading as an MLF-adjusted figure, never a future FY's factor.
        res = mlf_lookup.get(duid) if isinstance(mlf_lookup, dict) else None
        if (
            not isinstance(res, dict)
            or res.get("status") == MLF_STATUS_UNKNOWN
        ):
            mlf_status = MLF_STATUS_UNKNOWN
            mlf_source_fy_start = None
            mlf_value = None
            mlf = 1.0
        else:
            mlf_status = res["status"]
            mlf_source_fy_start = res.get("source_fy_start")
            mlf_value = float(res["mlf"])
            mlf = mlf_value

        # MWh: 5-minute intervals → divide by 12 to get MWh
        mwh = group["SCADAVALUE"].clip(lower=0).sum() / 12.0

        # Revenue: MWh_interval × RRP × MLF, summed
        # (unknown status: ×1.0 — unadjusted spot revenue, provisional)
        group_valid = group.dropna(subset=["RRP"])
        revenue = (group_valid["SCADAVALUE"].clip(lower=0) / 12.0 * group_valid["RRP"] * mlf).sum()

        # Capacity factor — not capped; values > 1.0 indicate stale registration or headwater physics
        cap_factor = mwh / (capacity * hours_in_month) if capacity and capacity > 0 else None
        if cap_factor is not None and cap_factor > 1.1:
            logger.warning(
                f"{duid} {month_label}: monthly CF {cap_factor:.4f} — possible registration mismatch "
                f"(SCADA {mwh:.1f} MWh > nameplate {capacity * hours_in_month:.1f} MWh)"
            )

        # Curtailment proxy (solar/wind only): 1 − actual/availability.
        # S3-01: this is a forecast-to-output shortfall PROXY, not a
        # causal measure. AVAILABILITY is the bid-in forecast (includes
        # outages), so "curtailment" here bundles grid constraints AND
        # mechanical/comms downtime. The former quality-flag split
        # (grid vs mechanical) was removed — ELAV "Good" flags signal
        # telemetry agreement, not why output was limited.
        curtailment = None
        econ_curtailment = None
        curt_actual_mwh = None
        curt_potential_mwh = None
        if fuel in config.CURTAILMENT_FUEL_TYPES:
            avail_valid = group.dropna(subset=["AVAILABILITY"])
            if not avail_valid.empty:
                total_actual = avail_valid["SCADAVALUE"].clip(lower=0).sum()
                total_avail = avail_valid["AVAILABILITY"].clip(lower=0).sum()
                # S3-04: keep the eligible actual/potential energy as
                # first-class fields so station/FY rollups can sum numerators
                # and denominators instead of averaging monthly percentages
                # (nameplate and actual generation are not the right weights).
                curt_actual_mwh = round(float(total_actual) / 12.0, 1)
                curt_potential_mwh = round(float(total_avail) / 12.0, 1)
                if total_avail > 0:
                    curtailment = max(0.0, 1.0 - total_actual / total_avail)

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
            # S3-11: revenue-MLF provenance — which factor (if any) was applied
            # to this month's revenue_aud and where it came from. Status
            # "unknown" => revenue is UNADJUSTED (factor 1.0) and provisional.
            REVENUE_MLF_STATUS_COL: mlf_status,
            REVENUE_MLF_SOURCE_FY_COL: mlf_source_fy_start,
            REVENUE_MLF_VALUE_COL: round(mlf_value, 6) if mlf_value is not None else None,
            "curtailment_pct": round(curtailment, 4) if curtailment is not None else None,
            "curtailment_actual_mwh": curt_actual_mwh,
            "curtailment_potential_mwh": curt_potential_mwh,
            "econ_curtailment_pct": round(econ_curtailment, 4) if econ_curtailment is not None else None,
            "captured_price": round(captured, 2) if captured is not None else None,
            "avg_rrp": round(avg_rrp, 2) if avg_rrp is not None else None,
            "price_capture_ratio": round(pcr, 4) if pcr is not None else None,
        }
        # Price distribution: per-month shares AND the per-bin energy behind
        # them (S3-04 — aggregated windows must be derivable from summed MWh,
        # not from an average of monthly histograms).
        dist, bin_mwh = _price_distribution(group_valid, config.PRICE_BINS, config.PRICE_BIN_LABELS)
        for label, share, mwh_bin in zip(config.PRICE_BIN_LABELS, dist, bin_mwh):
            row[f"price_dist_{label}"] = round(share, 4)
            row[f"price_mwh_{label}"] = round(mwh_bin, 1)

        rows.append(row)

    result = pd.DataFrame(rows)
    logger.info(f"Aggregated {month_label}: {len(result)} generators with data")
    return result


def _price_distribution(
    df: pd.DataFrame,
    bins: list[float],
    labels: list[str],
) -> tuple[list[float], list[float]]:
    """Compute generation-weighted price distribution across bins.

    Returns (shares, bin_mwh) — per-bin share of priced generation and the
    per-bin 5-minute energy in MWh. Shares sum to ~1.0 for a month with any
    priced generation; both are all-zero when there is no priced output.
    """
    n = len(labels)
    if df.empty or "RRP" not in df.columns:
        return [0.0] * n, [0.0] * n

    gen = df["SCADAVALUE"].clip(lower=0)
    total = gen.sum()
    if total == 0:
        return [0.0] * n, [0.0] * n

    bin_indices = np.digitize(df["RRP"].values, bins[1:-1])  # bins without -inf/inf
    bin_mw = [float(gen[bin_indices == i].sum()) for i in range(n)]
    shares = [mw / total for mw in bin_mw]
    bin_mwh = [mw / 12.0 for mw in bin_mw]  # 5-minute intervals → MWh
    return shares, bin_mwh


def aggregate_month_daily(
    scada: pd.DataFrame,
    generators: pd.DataFrame,
    year: int,
    month: int,
) -> pd.DataFrame:
    """Aggregate a single month of 5-minute SCADA data to daily per-generator metrics.

    S3-05: daily rows are INTERVAL-ENDING calendar days. AEMO stamps each
    5-minute interval with its END time and NEMOSIS month windows are
    (start, end] — so the interval ending at next month's midnight (the
    final 23:55–24:00 slice of the month) used to be dated as the first day
    of the NEXT month, fabricating an extra one-interval "day" (513 rows for
    2026-08-01 in the committed cache, max CF 0.41%). Here midnight-ending
    intervals are assigned to the preceding calendar day, duplicate interval
    keys are dropped before aggregation (an interval counts once no matter
    how fetch windows overlap), and every row publishes
    intervals_observed/intervals_expected (288 for a full day) so a partial
    day is explicit instead of masquerading as a full day.

    Capacity factor keeps the full-calendar-day denominator
    (mwh / capacity / 24 h): a constant full-day output yields CF 1.0, and a
    day that genuinely dispatched for only part of the day reads as the
    underperformance it is. Interval counts let consumers tell that apart
    from data-completeness issues.

    Returns DataFrame with columns: duid, date, daily_generation_mwh,
    daily_capacity_factor, intervals_observed, intervals_expected
    """
    if scada is None or scada.empty:
        return pd.DataFrame()

    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()

    scada = scada.copy()
    scada = scada.dropna(subset=["SETTLEMENTDATE"])
    # Dedupe interval keys first: one row per (DUID, interval-end) so an
    # interval is never double-counted after overlapping fetch windows.
    scada = scada.drop_duplicates(subset=["DUID", "SETTLEMENTDATE"], keep="last")
    scada["date"] = interval_calendar_day_str(scada["SETTLEMENTDATE"])

    rows = []
    for (duid, date_str), group in scada.groupby(["DUID", "date"]):
        capacity = duid_capacity.get(duid)
        observed = int(len(group))
        mwh = group["SCADAVALUE"].clip(lower=0).sum() / 12.0
        cf = mwh / (capacity * 24) if capacity and capacity > 0 else None
        if cf is not None and cf > 1.1:
            logger.warning(f"{duid} {date_str}: daily CF {cf:.4f} — possible registration mismatch")
        rows.append({
            "duid": duid,
            "date": date_str,
            "daily_generation_mwh": round(mwh, 1),
            "daily_capacity_factor": round(cf, 4) if cf is not None else None,
            "intervals_observed": observed,
            "intervals_expected": INTERVALS_PER_DAY,
        })

    return pd.DataFrame(rows)


# MLF resolution statuses (S3-11). Every per-DUID revenue lookup resolves to
# exactly one of these — the resolution is per DUID and NEVER consults a
# future FY for historical revenue.
MLF_STATUS_EXACT = "exact"
MLF_STATUS_PRIOR_CARRY = "prior-carry"
MLF_STATUS_UNKNOWN = "unknown"

# Feather/JSON column names that carry per-month revenue-MLF provenance on
# monthly aggregate rows (see aggregate_month / generate_json).
REVENUE_MLF_STATUS_COL = "revenue_mlf_status"
REVENUE_MLF_SOURCE_FY_COL = "revenue_mlf_source_fy_start"
REVENUE_MLF_VALUE_COL = "revenue_mlf_value"


def build_mlf_lookup(
    mlf_history: pd.DataFrame,
    target_fy_start: int,
) -> dict[str, dict]:
    """Resolve the MLF for every DUID against one target FY, PER DUID.

    Returns ``{DUID: {"mlf": float | None, "source_fy_start": int | None,
    "status": "exact" | "prior-carry" | "unknown"}}``.

    Resolution contract (S3-11):
      - ``exact``       — the DUID has a published factor for ``target_fy_start``;
      - ``prior-carry`` — no factor for the target FY, but the DUID has at
        least one EARLIER factor: the most recent earlier FY carries over
        (an explicitly allowed prior-year carry — never a silent 1.0);
      - ``unknown``     — no factor at or before the target FY (future-only
        rows or no rows at all). Revenue must then be treated as UNADJUSTED
        and PROVISIONAL, never presented as an MLF-adjusted figure.

    The old behaviour is gone: it returned the whole exact-FY slice when any
    exact row existed (so a unit missing that FY silently dropped out and
    aggregation used 1.0 even when the unit had a prior value), and when the
    whole FY was absent it fell back to each unit's LATEST available FY —
    including years AFTER the target — applying a future factor to historical
    revenue. Neither path recorded an imputation or source FY.
    """
    if mlf_history is None or mlf_history.empty:
        return {}

    out: dict[str, dict] = {}
    for duid, group in mlf_history.groupby("DUID", sort=False):
        rows = group.sort_values("fy_start_year")
        exact = rows[rows["fy_start_year"] == target_fy_start]
        if not exact.empty:
            out[duid] = {
                "mlf": float(exact["mlf"].iloc[0]),
                "source_fy_start": target_fy_start,
                "status": MLF_STATUS_EXACT,
            }
            continue
        prior = rows[rows["fy_start_year"] < target_fy_start]
        if not prior.empty:
            src = prior.iloc[-1]
            out[duid] = {
                "mlf": float(src["mlf"]),
                "source_fy_start": int(src["fy_start_year"]),
                "status": MLF_STATUS_PRIOR_CARRY,
            }
            continue
        # No factor at or before the target FY — future-only rows (or none).
        out[duid] = {
            "mlf": None,
            "source_fy_start": None,
            "status": MLF_STATUS_UNKNOWN,
        }
    return out


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
