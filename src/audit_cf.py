"""Capacity factor audit — flag DUIDs with sustained CF > 1.0.

Runs after monthly aggregation to detect stale registration capacities.
DUIDs that consistently exceed CF 1.0 are likely candidates for
CAPACITY_OVERRIDES in config.py.
"""

from __future__ import annotations

import logging

import pandas as pd

from . import config

logger = logging.getLogger(__name__)

# A DUID is flagged if it has at least this many months with CF > 1.0
MIN_MONTHS_OVER = 3
# Only flag if peak CF exceeds this threshold (filters out marginal rounding)
CF_THRESHOLD = 1.02


def audit_capacity_factors(
    monthly_aggregates: pd.DataFrame,
    generators: pd.DataFrame,
) -> list[dict]:
    """Scan monthly aggregates for DUIDs with sustained CF > 1.0.

    Returns a list of dicts with override candidates, sorted by severity.
    Each dict has: duid, registered_mw, implied_mw, months_over, max_cf,
    fuel, already_overridden.
    """
    if monthly_aggregates.empty:
        return []

    duid_capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()
    duid_fuel = generators.set_index("DUID")["FUEL_CATEGORY"].to_dict()

    candidates = []
    for duid, group in monthly_aggregates.groupby("duid"):
        cfs = group["capacity_factor"].dropna()
        if cfs.empty:
            continue

        months_over = int((cfs > 1.0).sum())
        if months_over < MIN_MONTHS_OVER:
            continue

        max_cf = float(cfs.max())
        if max_cf < CF_THRESHOLD:
            continue

        registered_mw = duid_capacity.get(duid, 0) or 0
        if registered_mw <= 0:
            continue

        # Estimate actual capacity from 95th percentile of monthly generation
        gen = group["generation_mwh"].dropna()
        if gen.empty:
            continue
        # Use p95 monthly MWh, convert back to MW using avg hours per month (~730)
        p95_mwh = float(gen.quantile(0.95))
        implied_mw = round(p95_mwh / 730, 1)

        candidates.append({
            "duid": duid,
            "registered_mw": registered_mw,
            "implied_mw": implied_mw,
            "ratio": round(implied_mw / registered_mw, 3),
            "months_over": months_over,
            "total_months": len(cfs),
            "max_cf": round(max_cf, 4),
            "fuel": duid_fuel.get(duid, "Unknown"),
            "already_overridden": duid in config.CAPACITY_OVERRIDES,
        })

    candidates.sort(key=lambda x: x["months_over"], reverse=True)
    return candidates


def log_audit_results(candidates: list[dict]) -> None:
    """Log audit results and proposed overrides."""
    if not candidates:
        logger.info("CF audit: no DUIDs with sustained CF > 1.0 — registrations look clean")
        return

    new_candidates = [c for c in candidates if not c["already_overridden"]]
    overridden = [c for c in candidates if c["already_overridden"]]

    if overridden:
        logger.info(
            f"CF audit: {len(overridden)} DUIDs already have capacity overrides: "
            + ", ".join(c["duid"] for c in overridden)
        )

    if not new_candidates:
        logger.info("CF audit: all flagged DUIDs already have overrides — no action needed")
        return

    logger.warning(
        f"CF audit: {len(new_candidates)} DUIDs need investigation "
        f"(CF > 1.0 in ≥{MIN_MONTHS_OVER} months):"
    )
    for c in new_candidates:
        logger.warning(
            f"  {c['duid']:<14} reg={c['registered_mw']:.0f} MW  "
            f"implied={c['implied_mw']:.0f} MW  "
            f"ratio={c['ratio']:.3f}  "
            f"months_over={c['months_over']}/{c['total_months']}  "
            f"max_cf={c['max_cf']:.4f}  "
            f"fuel={c['fuel']}"
        )
    logger.warning(
        "  → Add these to CAPACITY_OVERRIDES in config.py if the mismatch is confirmed."
    )
