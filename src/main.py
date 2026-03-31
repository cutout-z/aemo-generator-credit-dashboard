"""CLI orchestrator for AEMO Generator Credit Dashboard pipeline."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from . import config
from .aggregate import aggregate_month, build_mlf_lookup
from .download_dispatch import fetch_dispatch_price_month
from .download_metadata import fetch_generators
from .download_mlf import fetch_connection_points, fetch_mlf_history
from .download_scada import fetch_dispatchload_month, fetch_scada_month
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
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / config.DATA_DIR

    # Step 1: Generator metadata
    logger.info("=== Step 1: Generator metadata ===")
    generators = fetch_generators(str(data_dir), force=args.full_refresh)
    logger.info(f"Loaded {len(generators)} generators")

    if args.metadata_only:
        logger.info("=== Generating index (metadata only) ===")
        count = generate_all(generators)
        logger.info(f"Done. Wrote index + {count} generator files.")
        return

    # Step 2: MLF history + connection points
    logger.info("=== Step 2: MLF history ===")
    mlf_history = fetch_mlf_history(str(data_dir), force=args.full_refresh)
    logger.info(f"Loaded {len(mlf_history)} DUID×FY MLF records")

    # Enrich generators with connection points from DUDETAILSUMMARY
    cp_map = fetch_connection_points(str(data_dir), force=args.full_refresh)
    generators["CONNECTION_POINT"] = generators["DUID"].map(cp_map).fillna("")
    logger.info(f"Enriched {(generators['CONNECTION_POINT'] != '').sum()} generators with connection points")

    # Step 3: Monthly SCADA + price aggregation
    aggregates_path = data_dir / "monthly_aggregates.feather"

    if args.skip_scada and aggregates_path.exists():
        logger.info("=== Step 3: Loading cached aggregates (--skip-scada) ===")
        all_monthly = pd.read_feather(aggregates_path)
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

                # Build MLF lookup for this month's FY
                fy_start = year if month >= 7 else year - 1
                mlf_lookup = build_mlf_lookup(mlf_history, fy_start)

                # Aggregate
                monthly = aggregate_month(
                    scada, prices, dispatchload, generators, mlf_lookup, year, month
                )
                if not monthly.empty:
                    new_rows.append(monthly)

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

    # Step 4: Generate JSON output
    logger.info("=== Step 4: Generating JSON output ===")
    monthly_agg = all_monthly if not all_monthly.empty else None
    count = generate_all(generators, monthly_agg, mlf_history)
    logger.info(f"Done. Wrote index + {count} generator files.")


if __name__ == "__main__":
    main()
