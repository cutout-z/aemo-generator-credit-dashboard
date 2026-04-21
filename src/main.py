"""CLI orchestrator for AEMO Generator Credit Dashboard pipeline."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from . import config
from .aggregate import (
    aggregate_constraints_month, aggregate_fcas_prices,
    aggregate_month, aggregate_month_daily, build_mlf_lookup,
)
from .download_constraints import (
    fetch_binding_constraints_month, fetch_gencondata,
    fetch_spdconnectionpointconstraint,
)
from .download_dispatch import fetch_dispatch_price_month
from .download_intermittent import fetch_intermittent_month
from .download_metadata import fetch_generators
from .download_scada import fetch_dispatchload_month, fetch_scada_month
from .fetch_mlf import fetch_mlf_data
from .generate_json import generate_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _months_to_process(months_back: int, full_refresh: bool) -> list[tuple[int, int]]:
    """Determine which (year, month) pairs to process."""
    now = datetime.now()
    # Go back one more month since AEMO data has ~2 week lag
    latest = now - timedelta(days=20)

    if full_refresh:
        start = datetime(latest.year - config.HISTORY_YEARS, latest.month, 1)
    else:
        start = latest - timedelta(days=30 * months_back)

    months = []
    current = datetime(start.year, start.month, 1)
    end = datetime(latest.year, latest.month, 1)
    while current <= end:
        months.append((current.year, current.month))
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)

    return months


def main():
    parser = argparse.ArgumentParser(description="AEMO Generator Credit Dashboard pipeline")
    parser.add_argument("--full-refresh", action="store_true", help="Re-download all data (5 years)")
    parser.add_argument("--months-back", type=int, default=config.DEFAULT_MONTHS_BACK,
                        help="Number of months to reprocess (default: 2)")
    parser.add_argument("--metadata-only", action="store_true",
                        help="Only download metadata and generate index")
    parser.add_argument("--skip-scada", action="store_true",
                        help="Skip SCADA download (use cached aggregates only)")
    parser.add_argument("--fcas-rebuild", action="store_true",
                        help="Rebuild FCAS history from cached DISPATCHPRICE files (implies --skip-scada)")
    args = parser.parse_args()
    if args.fcas_rebuild:
        args.skip_scada = True

    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / config.DATA_DIR

    # Step 1: Generator metadata
    logger.info("=== Step 1: Generator metadata ===")
    # Use the same date heuristic as SCADA processing: go back ~20 days to ensure data is published
    _now = datetime.now()
    _latest = _now - timedelta(days=20)
    generators = fetch_generators(
        str(data_dir), force=args.full_refresh,
        mmsdm_year=_latest.year, mmsdm_month=_latest.month,
    )
    logger.info(f"Loaded {len(generators)} generators")

    if args.metadata_only:
        logger.info("=== Generating index (metadata only) ===")
        count = generate_all(generators)
        logger.info(f"Done. Wrote index + {count} generator files.")
        return

    # Step 2: MLF history + draft MLFs + connection points (all from MLF Tracker CSV)
    logger.info("=== Step 2: MLF data from MLF Tracker ===")
    mlf_history, draft_mlfs, draft_fy_label, cp_map = fetch_mlf_data(
        str(data_dir), force=args.full_refresh
    )
    logger.info(f"Loaded {len(mlf_history)} DUID×FY MLF records")
    if draft_mlfs:
        logger.info(f"Loaded {len(draft_mlfs)} draft MLFs for {draft_fy_label}")
    else:
        logger.info("No draft MLFs available")

    generators["CONNECTION_POINT"] = generators["DUID"].map(cp_map).fillna("")
    logger.info(f"Enriched {(generators['CONNECTION_POINT'] != '').sum()} generators with connection points")

    # Step 3: Monthly SCADA + price aggregation
    aggregates_path = data_dir / "monthly_aggregates.feather"

    daily_path = data_dir / "daily_aggregates.feather"

    fcas_by_region_month = {}
    fcas_cache_path = data_dir / "fcas_aggregates.feather"

    if args.skip_scada and aggregates_path.exists():
        logger.info("=== Step 3: Loading cached aggregates (--skip-scada) ===")
        all_monthly = pd.read_feather(aggregates_path)
        all_daily = pd.read_feather(daily_path) if daily_path.exists() else pd.DataFrame()
        # Load FCAS from cache
        if fcas_cache_path.exists() and not args.fcas_rebuild:
            cached_fcas = pd.read_feather(fcas_cache_path)
            for _, row in cached_fcas.iterrows():
                key = (row["region"], row["month"])
                fcas_by_region_month[key] = {
                    k: float(v) for k, v in row.items()
                    if k not in ("region", "month") and pd.notna(v)
                }
            logger.info(f"Loaded {len(fcas_by_region_month)} region×month FCAS entries from cache")

        # Rebuild FCAS by reading DISPATCHPRICE for all historical months
        if args.fcas_rebuild:
            logger.info("=== FCAS rebuild: reading DISPATCHPRICE from cache ===")
            fcas_months_all = _months_to_process(config.HISTORY_YEARS * 12, full_refresh=False)
            for year, month in fcas_months_all:
                month_label = f"{year}-{month:02d}"
                try:
                    prices = fetch_dispatch_price_month(
                        year, month, str(data_dir), rebuild=False
                    )
                    if not prices.empty:
                        fcas_prices = aggregate_fcas_prices(prices, year, month)
                        for region, services in fcas_prices.items():
                            fcas_by_region_month[(region, month_label)] = services
                        logger.info(f"FCAS {month_label}: {len(fcas_prices)} regions")
                    else:
                        logger.debug(f"No DISPATCHPRICE cache for {month_label}, skipping")
                except Exception as e:
                    logger.warning(f"FCAS rebuild failed for {month_label}: {e}")
            # Save rebuilt cache
            if fcas_by_region_month:
                rebuild_rows = [
                    {"region": region, "month": month, **services}
                    for (region, month), services in fcas_by_region_month.items()
                ]
                pd.DataFrame(rebuild_rows).to_feather(fcas_cache_path)
                logger.info(f"Saved rebuilt FCAS cache: {len(fcas_by_region_month)} region×month entries")
    else:
        logger.info("=== Step 3: Monthly SCADA + price aggregation ===")
        months = _months_to_process(args.months_back, args.full_refresh)
        logger.info(f"Processing {len(months)} months: {months[0]} to {months[-1]}")

        # Load existing aggregates if incremental
        if aggregates_path.exists() and not args.full_refresh:
            existing = pd.read_feather(aggregates_path)
            logger.info(f"Loaded {len(existing)} existing aggregate rows")
        else:
            existing = pd.DataFrame()

        new_rows = []
        new_daily_rows = []
        fcas_by_region_month = {}  # (region, month_label) -> {service: avg_price}
        for year, month in months:
            month_label = f"{year}-{month:02d}"
            logger.info(f"--- Processing {month_label} ---")

            try:
                # Download data for this month
                scada = fetch_scada_month(year, month, str(data_dir), rebuild=args.full_refresh)
                if scada.empty:
                    logger.warning(f"No SCADA data for {month_label}, skipping")
                    continue

                prices = fetch_dispatch_price_month(year, month, str(data_dir), rebuild=args.full_refresh)
                if prices.empty:
                    logger.warning(f"No price data for {month_label}, skipping")
                    continue

                dispatchload = fetch_dispatchload_month(year, month, str(data_dir), rebuild=args.full_refresh)

                # INTERMITTENT_GEN_SCADA for curtailment splitting (Aug 2024+)
                intermittent = None
                if (year, month) >= config.INTERMITTENT_SCADA_START:
                    try:
                        intermittent = fetch_intermittent_month(
                            year, month, str(data_dir), rebuild=args.full_refresh
                        )
                    except Exception as e:
                        logger.warning(f"Could not fetch INTERMITTENT_GEN_SCADA for {month_label}: {e}")

                # Build MLF lookup for this month's FY
                fy_start = year if month >= 7 else year - 1
                mlf_lookup = build_mlf_lookup(mlf_history, fy_start)

                # Aggregate
                monthly = aggregate_month(
                    scada, prices, dispatchload, generators, mlf_lookup, year, month,
                    intermittent_scada=intermittent,
                )
                if not monthly.empty:
                    new_rows.append(monthly)

                # Daily aggregation for capacity factor chart
                daily = aggregate_month_daily(scada, generators, year, month)
                if not daily.empty:
                    new_daily_rows.append(daily)

                # FCAS regional prices
                fcas_prices = aggregate_fcas_prices(prices, year, month)
                for region, services in fcas_prices.items():
                    fcas_by_region_month[(region, month_label)] = services

            except Exception as e:
                logger.error(f"Failed to process {month_label}: {e}")
                continue

        # Merge new with existing
        if new_rows:
            new_df = pd.concat(new_rows, ignore_index=True)
            if not existing.empty:
                # Remove months we just reprocessed
                reprocessed_months = set(new_df["month"].unique())
                existing = existing[~existing["month"].isin(reprocessed_months)]
                all_monthly = pd.concat([existing, new_df], ignore_index=True)
            else:
                all_monthly = new_df
        elif not existing.empty:
            all_monthly = existing
        else:
            all_monthly = pd.DataFrame()

        # Sort and save
        if not all_monthly.empty:
            all_monthly = all_monthly.sort_values(["duid", "month"]).reset_index(drop=True)
            all_monthly.to_feather(aggregates_path)
            logger.info(f"Saved {len(all_monthly)} aggregate rows to {aggregates_path}")

        # Daily aggregates — keep only last 12 months
        if new_daily_rows:
            new_daily = pd.concat(new_daily_rows, ignore_index=True)
            # Deduplicate boundary dates (NEMOSIS returns overlapping months)
            # Keep the max values per (duid, date) since partial days get split
            new_daily = new_daily.groupby(["duid", "date"], as_index=False).agg(
                daily_generation_mwh=("daily_generation_mwh", "max"),
                daily_capacity_factor=("daily_capacity_factor", "max"),
            )
            if daily_path.exists() and not args.full_refresh:
                existing_daily = pd.read_feather(daily_path)
                reprocessed_dates = set(new_daily["date"].unique())
                existing_daily = existing_daily[~existing_daily["date"].isin(reprocessed_dates)]
                all_daily = pd.concat([existing_daily, new_daily], ignore_index=True)
            else:
                all_daily = new_daily
            # Trim to last 12 months
            cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            all_daily = all_daily[all_daily["date"] >= cutoff]
            all_daily = all_daily.sort_values(["duid", "date"]).reset_index(drop=True)
            all_daily.to_feather(daily_path)
            logger.info(f"Saved {len(all_daily)} daily aggregate rows to {daily_path}")
        elif daily_path.exists():
            all_daily = pd.read_feather(daily_path)
        else:
            all_daily = pd.DataFrame()

        # Persist FCAS data and reload full history from cache
        if fcas_by_region_month:
            new_fcas_rows = [
                {"region": region, "month": month, **services}
                for (region, month), services in fcas_by_region_month.items()
            ]
            new_fcas = pd.DataFrame(new_fcas_rows)
            if fcas_cache_path.exists() and not args.full_refresh:
                old_fcas = pd.read_feather(fcas_cache_path)
                reprocessed_months = set(new_fcas["month"].unique())
                old_fcas = old_fcas[~old_fcas["month"].isin(reprocessed_months)]
                merged_fcas = pd.concat([old_fcas, new_fcas], ignore_index=True)
            else:
                merged_fcas = new_fcas
            merged_fcas.to_feather(fcas_cache_path)
            # Expand fcas_by_region_month to full cached history
            fcas_by_region_month = {}
            for _, row in merged_fcas.iterrows():
                key = (row["region"], row["month"])
                fcas_by_region_month[key] = {
                    k: float(v) for k, v in row.items()
                    if k not in ("region", "month") and pd.notna(v)
                }
            logger.info(f"Saved FCAS cache: {len(fcas_by_region_month)} region×month entries")

    # Step 3b: Binding constraint aggregation
    constraint_path = data_dir / "constraint_aggregates.feather"
    all_constraints = pd.DataFrame()
    if not args.metadata_only:
        logger.info("=== Step 3b: Binding constraint aggregation ===")
        try:
            gencondata = fetch_gencondata(str(data_dir), rebuild=args.full_refresh)
            spdcp = fetch_spdconnectionpointconstraint(str(data_dir), rebuild=args.full_refresh)

            if not gencondata.empty and not spdcp.empty:
                # Determine months to process for constraints
                constraint_months = _months_to_process(
                    config.CONSTRAINTS_HISTORY_MONTHS, args.full_refresh
                )
                if not args.full_refresh:
                    # If no existing constraint data, backfill the full history window;
                    # otherwise only reprocess the requested overlap period.
                    months_back = (
                        config.CONSTRAINTS_HISTORY_MONTHS
                        if not constraint_path.exists()
                        else args.months_back
                    )
                    constraint_months = constraint_months[-months_back:]

                constraint_rows = []
                for year, month in constraint_months:
                    try:
                        dc = fetch_binding_constraints_month(
                            year, month, str(data_dir), rebuild=args.full_refresh
                        )
                        if not dc.empty:
                            mc = aggregate_constraints_month(
                                dc, spdcp, gencondata, cp_map, year, month
                            )
                            if not mc.empty:
                                constraint_rows.append(mc)
                    except Exception as e:
                        logger.warning(f"Constraint processing failed for {year}-{month:02d}: {e}")

                if constraint_rows:
                    new_constraints = pd.concat(constraint_rows, ignore_index=True)
                    if constraint_path.exists() and not args.full_refresh:
                        existing_constraints = pd.read_feather(constraint_path)
                        reprocessed = set(new_constraints["month"].unique())
                        existing_constraints = existing_constraints[
                            ~existing_constraints["month"].isin(reprocessed)
                        ]
                        all_constraints = pd.concat(
                            [existing_constraints, new_constraints], ignore_index=True
                        )
                    else:
                        all_constraints = new_constraints
                    all_constraints.to_feather(constraint_path)
                    logger.info(f"Saved {len(all_constraints)} constraint aggregate rows")
                elif constraint_path.exists():
                    all_constraints = pd.read_feather(constraint_path)
            else:
                logger.warning("Skipping constraints: GENCONDATA or SPDCP unavailable")
        except Exception as e:
            logger.error(f"Constraint aggregation failed: {e}")
    elif constraint_path.exists():
        all_constraints = pd.read_feather(constraint_path)

    # Step 4: Generate JSON output
    logger.info("=== Step 4: Generating JSON output ===")
    monthly_agg = all_monthly if not all_monthly.empty else None
    daily_agg = all_daily if not all_daily.empty else None
    constraint_agg = all_constraints if not all_constraints.empty else None
    count = generate_all(generators, monthly_agg, mlf_history,
                         draft_mlfs=draft_mlfs, draft_fy_label=draft_fy_label,
                         fcas_data=fcas_by_region_month if fcas_by_region_month else None,
                         daily_aggregates=daily_agg,
                         constraint_data=constraint_agg)
    logger.info(f"Done. Wrote index + {count} generator files.")


if __name__ == "__main__":
    main()
