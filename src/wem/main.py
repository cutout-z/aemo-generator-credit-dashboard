"""CLI orchestrator for the WEM pipeline.

Produces data/wem/monthly_aggregates.feather AND writes WEM generator JSON files
to docs/data/generators/ (merged additively with NEM generator files).

Two data sources:
  Pre-Reform  (Jul 2012 – Sep 2023): AEMO CSV archive (30-min intervals)
  Post-Reform (Oct 2023 – present):  Open Electricity API (5-min intervals)

Usage:
  python -m src.wem.main                     # incremental (last 2 months, both eras)
  python -m src.wem.main --full-refresh      # all data Jul 2012 – present
  python -m src.wem.main --post-reform-only  # only post-Reform months
  python -m src.wem.main --metadata-only     # facilities + TLF only, no SCADA
  python -m src.wem.main --months-back 6     # reprocess last 6 months
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from . import config
from .aggregate import aggregate_wem_month, aggregate_wem_month_daily, aggregate_wem_system_price
from .download_dispatch import fetch_wem_price_month
from .download_metadata import fetch_wem_generators
from .download_openelectricity import fetch_post_reform_daily, fetch_post_reform_monthly
from .download_scada import fetch_wem_scada_month
from .download_tlf import build_facility_tlf_lookup, fetch_tlf_history
from ..generate_json import generate_all as generate_json_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _months_in_range(
    start: tuple[int, int], end: tuple[int, int],
) -> list[tuple[int, int]]:
    """Generate (year, month) pairs from start to end inclusive."""
    months = []
    y, m = start
    while (y, m) <= end:
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def _pre_reform_months(months_back: int, full_refresh: bool) -> list[tuple[int, int]]:
    """Pre-Reform months (Jul 2012 – Sep 2023) from AEMO CSV archive."""
    start = config.WEM_DATA_START
    end = config.WEM_PRE_REFORM_END

    if full_refresh:
        return _months_in_range(start, end)

    ys, ms = end[0], end[1] - months_back
    while ms <= 0:
        ms += 12
        ys -= 1
    return _months_in_range(max((ys, ms), start), end)


def _post_reform_months(months_back: int, full_refresh: bool) -> list[tuple[int, int]]:
    """Post-Reform months (Oct 2023 – last complete month) from Open Electricity API."""
    start = config.WEM_POST_REFORM_START
    now = datetime.now()
    # End at previous month (current month may be incomplete)
    end_y, end_m = now.year, now.month - 1
    if end_m == 0:
        end_m = 12
        end_y -= 1
    end = (end_y, end_m)

    if end < start:
        return []

    if full_refresh:
        return _months_in_range(start, end)

    ys, ms = end[0], end[1] - months_back
    while ms <= 0:
        ms += 12
        ys -= 1
    return _months_in_range(max((ys, ms), start), end)


def main():
    parser = argparse.ArgumentParser(description="WEM Generator Credit Dashboard pipeline")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Re-download and reprocess all data (Jul 2012 – present)")
    parser.add_argument("--months-back", type=int, default=2,
                        help="Number of months to reprocess (default: 2)")
    parser.add_argument("--metadata-only", action="store_true",
                        help="Only download facility metadata and TLF — skip SCADA")
    parser.add_argument("--post-reform-only", action="store_true",
                        help="Only process post-Reform months (Oct 2023+) via Open Electricity API")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent.parent
    wem_dir = project_root / config.WEM_DATA_DIR
    scada_dir = project_root / config.WEM_SCADA_CACHE_DIR
    price_dir = project_root / config.WEM_PRICE_CACHE_DIR

    for d in [wem_dir, scada_dir, price_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Step 1: Generator metadata
    logger.info("=== Step 1: WEM facility metadata ===")
    generators = fetch_wem_generators(str(wem_dir), force=args.full_refresh)
    logger.info(f"Loaded {len(generators)} WEM generators")

    # Step 2: TLF history
    logger.info("=== Step 2: TLF history ===")
    tlf_history = fetch_tlf_history(str(wem_dir), force=args.full_refresh)
    logger.info(f"Loaded {len(tlf_history)} TLF area×FY records")

    if args.metadata_only:
        logger.info("=== Metadata-only mode — done ===")
        return

    aggregates_path = project_root / config.WEM_MONTHLY_AGGREGATES_CACHE
    daily_path = wem_dir / "daily_aggregates.feather"
    system_price_path = wem_dir / "system_prices.feather"

    # Load existing if incremental
    if aggregates_path.exists() and not args.full_refresh:
        existing = pd.read_feather(aggregates_path)
        logger.info(f"Loaded {len(existing)} existing WEM aggregate rows")
    else:
        existing = pd.DataFrame()

    new_rows: list[pd.DataFrame] = []
    new_daily_rows: list[pd.DataFrame] = []
    system_price_rows: list[dict] = []

    # ── Step 3a: Pre-Reform (Jul 2012 – Sep 2023) from AEMO CSVs ──
    if not args.post_reform_only:
        pre_months = _pre_reform_months(args.months_back, args.full_refresh)
        if pre_months:
            logger.info(
                f"=== Step 3a: Pre-Reform SCADA ({len(pre_months)} months: "
                f"{pre_months[0][0]}-{pre_months[0][1]:02d} to "
                f"{pre_months[-1][0]}-{pre_months[-1][1]:02d}) ==="
            )
            for year, month in pre_months:
                month_label = f"{year}-{month:02d}"
                logger.info(f"--- WEM {month_label} (pre-Reform) ---")
                try:
                    scada = fetch_wem_scada_month(year, month, str(scada_dir), rebuild=args.full_refresh)
                    if scada.empty:
                        logger.warning(f"No SCADA data for {month_label}")
                        continue
                    prices = fetch_wem_price_month(year, month, str(price_dir), rebuild=args.full_refresh)
                    if prices.empty:
                        logger.warning(f"No price data for {month_label}")
                        continue
                    fy_start = year if month >= 7 else year - 1
                    tlf_lookup = build_facility_tlf_lookup(generators, tlf_history, fy_start)
                    monthly = aggregate_wem_month(scada, prices, generators, tlf_lookup, year, month)
                    if not monthly.empty:
                        new_rows.append(monthly)
                    daily = aggregate_wem_month_daily(scada, generators, year, month)
                    if not daily.empty:
                        new_daily_rows.append(daily)
                    sys_price = aggregate_wem_system_price(prices, year, month)
                    if sys_price:
                        system_price_rows.append({"month": month_label, **sys_price})
                except Exception as e:
                    logger.error(f"Failed to process WEM {month_label}: {e}")
                    continue

    # ── Step 3b: Post-Reform (Oct 2023+) from Open Electricity API ──
    post_months = _post_reform_months(args.months_back, args.full_refresh)
    if post_months:
        logger.info(
            f"=== Step 3b: Post-Reform via Open Electricity API ({len(post_months)} months: "
            f"{post_months[0][0]}-{post_months[0][1]:02d} to "
            f"{post_months[-1][0]}-{post_months[-1][1]:02d}) ==="
        )
        for year, month in post_months:
            month_label = f"{year}-{month:02d}"
            logger.info(f"--- WEM {month_label} (post-Reform) ---")
            try:
                monthly = fetch_post_reform_monthly(year, month, generators)
                if not monthly.empty:
                    new_rows.append(monthly)
                daily = fetch_post_reform_daily(year, month, generators)
                if not daily.empty:
                    new_daily_rows.append(daily)
            except Exception as e:
                logger.error(f"Failed to process post-Reform WEM {month_label}: {e}")
                continue

    # ── Merge and save ──
    if new_rows:
        new_df = pd.concat(new_rows, ignore_index=True)
        if not existing.empty:
            reprocessed = set(new_df["month"].unique())
            existing = existing[~existing["month"].isin(reprocessed)]
            all_monthly = pd.concat([existing, new_df], ignore_index=True)
        else:
            all_monthly = new_df

        all_monthly = all_monthly.sort_values(["duid", "month"]).reset_index(drop=True)
        all_monthly.to_feather(aggregates_path)
        logger.info(f"Saved {len(all_monthly)} WEM aggregate rows to {aggregates_path}")
    elif not existing.empty:
        all_monthly = existing
        logger.info("No new WEM data processed — existing cache unchanged")
    else:
        logger.warning("No WEM aggregate data produced")
        return

    # Save daily aggregates
    if new_daily_rows:
        new_daily = pd.concat(new_daily_rows, ignore_index=True)
        new_daily = new_daily.groupby(["duid", "date"], as_index=False).agg(
            daily_generation_mwh=("daily_generation_mwh", "sum"),
            daily_capacity_factor=("daily_capacity_factor", "max"),
        )
        if daily_path.exists() and not args.full_refresh:
            existing_daily = pd.read_feather(daily_path)
            reprocessed_dates = set(new_daily["date"].unique())
            existing_daily = existing_daily[~existing_daily["date"].isin(reprocessed_dates)]
            all_daily = pd.concat([existing_daily, new_daily], ignore_index=True)
        else:
            all_daily = new_daily
        all_daily = all_daily.sort_values(["duid", "date"]).reset_index(drop=True)
        all_daily.to_feather(daily_path)
        logger.info(f"Saved {len(all_daily)} WEM daily aggregate rows")

    # Save system prices
    if system_price_rows:
        sys_df = pd.DataFrame(system_price_rows)
        if system_price_path.exists() and not args.full_refresh:
            existing_sys = pd.read_feather(system_price_path)
            reprocessed = set(sys_df["month"].unique())
            existing_sys = existing_sys[~existing_sys["month"].isin(reprocessed)]
            sys_df = pd.concat([existing_sys, sys_df], ignore_index=True)
        sys_df = sys_df.sort_values("month").reset_index(drop=True)
        sys_df.to_feather(system_price_path)
        logger.info(f"Saved {len(sys_df)} WEM system price records")

    # Step 4: Generate JSON output
    logger.info("=== Step 4: Generating WEM JSON output ===")

    # Add market identifier to generator DataFrame
    generators_wem = generators.copy()
    generators_wem["MARKET"] = "WEM"

    # Build TLF-as-MLF DataFrame (TLF per DUID per FY → same schema as NEM mlf_history)
    tlf_as_mlf = _build_tlf_as_mlf(generators, tlf_history)

    # Load full daily aggregates for JSON output
    all_daily_for_json = pd.read_feather(daily_path) if daily_path.exists() else None

    count = generate_json_all(
        generators_wem,
        all_monthly if not all_monthly.empty else None,
        tlf_as_mlf if not tlf_as_mlf.empty else None,
        daily_aggregates=all_daily_for_json,
        market="WEM",
    )
    logger.info(f"WEM JSON output: {count} generator files written")

    # Summary
    logger.info("=== WEM pipeline complete ===")
    months_covered = sorted(all_monthly["month"].unique())
    logger.info(
        f"WEM coverage: {months_covered[0]} to {months_covered[-1]} "
        f"({len(months_covered)} months, {all_monthly['duid'].nunique()} facilities)"
    )


def _build_tlf_as_mlf(generators: pd.DataFrame, tlf_history: pd.DataFrame) -> pd.DataFrame:
    """Convert TLF history to the same schema as NEM mlf_history for generate_json.py.

    generate_json.py expects: DUID, fy_label, fy_start_year, mlf
    We map TLF per DUID per FY to the 'mlf' column.
    Only includes FYs relevant to the WEM data window (FY12-13 to FY22-23).
    """
    if tlf_history is None or tlf_history.empty or generators is None or generators.empty:
        return pd.DataFrame()

    rows = []
    for fy_label in sorted(tlf_history["fy_label"].unique()):
        fy_str = fy_label[2:]  # "FY22-23" → "22-23"
        fy_start = 2000 + int(fy_str[:2])
        # Only include FYs within the WEM data window
        if fy_start < 2012 or fy_start > 2022:
            continue
        lookup = build_facility_tlf_lookup(generators, tlf_history, fy_start)
        for duid, tlf_val in lookup.items():
            rows.append({
                "DUID": duid,
                "fy_label": fy_label,
                "fy_start_year": fy_start,
                "mlf": tlf_val,  # TLF stored in 'mlf' column for schema compatibility
            })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    main()
