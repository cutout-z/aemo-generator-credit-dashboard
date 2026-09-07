"""CLI orchestrator for AEMO Generator Credit Dashboard pipeline."""

from __future__ import annotations

import argparse
import gc
import logging
from datetime import datetime, timedelta, timezone
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
from .download_intermittent import fetch_intermittent_month  # noqa: F401  (S3-01: retained for cache-migration tooling; unused by pipeline)
from .download_metadata import fetch_generators
from .download_scada import fetch_dispatchload_month, fetch_scada_month
from .fetch_mlf import fetch_mlf_data
from .audit_cf import audit_capacity_factors, log_audit_results
from .generate_json import generate_all
from .processed_cache import publish_processed_cache, restore_processed_cache
from .market_factors import (
    build_market_factors, build_quarterly_summary, check_qed_divergence,
    QED_NEM_SPREAD_AUD_MWH,
)
from .download_bids import fcas_bids_from_raw, fetch_bidperoffer_union
from .fcas_factor import compute_fcas_factors
from .factor_cache import merge_month_rows
from .offer_curves import (
    OFFER_CURVES_CACHE,
    OFFER_CURVES_DAILY_CACHE,
    OFFER_FACTORS_CACHE,
    compute_offer_curves,
    compute_offer_curves_daily,
    compute_offer_features,
    energy_volumes_from_raw,
    fetch_energy_prices,
    write_offer_curve_files,
)
from .freshness import check_monthly_freshness, check_daily_freshness
from .generate_market_json import publish_market_json
from .run_status import (
    LaneRun,
    build_manifest,
    check_factor_block_continuity,
    read_last_good,
    write_run_manifest,
    STATUS_DEGRADED,
    STATUS_ERROR,
    STATUS_SKIPPED,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _assert_protected_months_unchanged(
    before: pd.DataFrame,
    after: pd.DataFrame,
    mutable_months: set[str],
    *,
    month_col: str,
    label: str,
) -> None:
    """Abort normal updates if settled historical months changed unexpectedly."""
    if before.empty or after.empty or month_col not in before.columns or month_col not in after.columns:
        return

    protected_months = sorted(set(before[month_col].dropna()) - mutable_months)
    if not protected_months:
        return

    before_protected = before[before[month_col].isin(protected_months)]
    after_protected = after[after[month_col].isin(protected_months)]
    columns = sorted(set(before_protected.columns) & set(after_protected.columns))

    before_fp = _dataframe_fingerprint(before_protected[columns])
    after_fp = _dataframe_fingerprint(after_protected[columns])
    if before_fp != after_fp:
        raise RuntimeError(
            f"{label} attempted to change settled months outside the mutable window. "
            "Use --full-refresh only for deliberate audited historical rewrites."
        )

    logger.info(
        "%s settled-history guard: %d protected months unchanged",
        label, len(protected_months),
    )


def _dataframe_fingerprint(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    stable = df.copy().sort_values(list(df.columns)).reset_index(drop=True)
    return int(pd.util.hash_pandas_object(stable, index=False).sum())


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


# ────────────────────────────────────────────────────────────────────────
# S3-08 optional-source lanes. Each lane records its run state (LaneRun)
# and, when the source fails or delivers nothing new, retains the
# last-known-good factor history from its cache so published per-DUID
# blocks never vanish from a "successful" run. The caller (main) runs the
# factor-block continuity guard before publishing and writes the
# machine-readable run-status manifest for the operator channel.
# ────────────────────────────────────────────────────────────────────────

def _skip_lane_retained(
    lane: LaneRun, cache_path: Path, skip_flag_desc: str,
) -> pd.DataFrame:
    """Explicit operator skip: retain the cache when it exists.

    ``skipped`` lanes are documented operator intent — they appear in the run
    manifest but do not alert. When no cache exists the frame is empty and the
    continuity guard rejects the publish if populated blocks would vanish.
    """
    retained = read_last_good(cache_path)
    lane.status = STATUS_SKIPPED
    if not retained.empty:
        lane.retained = True
        lane.frame = retained
        lane.note = f"{skip_flag_desc}; last-known-good history retained from cache"
    else:
        lane.frame = pd.DataFrame()
        lane.note = f"{skip_flag_desc}; no cache present — nothing retained"
    return lane.frame


def _finalize_factor_lane(
    lane: LaneRun,
    cache_path: Path,
    frames: list[pd.DataFrame],
    errors: list[str],
    *,
    full_refresh: bool,
    label: str,
    no_rows_error: str,
    no_rows_note: str,
    cold_start_error: str,
) -> pd.DataFrame:
    """Persist one lane's fresh per-month facts, or retain the last-known-good.

    S3-08 lane semantics shared by the FCAS / offer-factor / offer-curve
    lanes: ``frames`` = freshly computed per-month fact frames, ``errors`` =
    per-month failures. With facts, the lane merges them into its cache
    through the single persistence owner (S3-07 merge_month_rows) and stays
    ok unless some months failed (then degraded, errors surfaced). Without
    facts the last-known-good history is retained from the cache (degraded —
    blocks kept and stamped retained-stale); with neither facts nor cache the
    lane errors (cold start).
    """
    if frames:
        new = pd.concat(frames, ignore_index=True)
        lane.months_with_data = int(new["month"].nunique())
        merged = merge_month_rows(
            cache_path, new, full_refresh=full_refresh, label=label,
        )
        lane.frame = merged
        if errors:
            lane.status = STATUS_DEGRADED
            lane.error = "; ".join(errors)
        return merged
    retained = read_last_good(cache_path)
    if not retained.empty:
        lane.retained = True
        lane.frame = retained
        lane.status = STATUS_DEGRADED
        lane.error = "; ".join(errors) if errors else no_rows_error
        if not errors:
            lane.note = no_rows_note
        return retained
    lane.status = STATUS_ERROR
    lane.frame = pd.DataFrame()
    lane.error = "; ".join(errors) if errors else cold_start_error
    return lane.frame


def _run_optional_factor_lanes(
    data_dir: Path,
    months: list[tuple[int, int]],
    *,
    skip_fcas_factors: bool,
    skip_offer_factors: bool,
    full_refresh: bool,
) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame | None,
    LaneRun, LaneRun, LaneRun,
]:
    """Steps 3e/3e2/3e3 — per-DUID optional-source factor facts (S3-12).

    Month-outer single-decode driver: for each (year, month) the run fetches
    BIDPEROFFER_D and BIDDAYOFFER_D AT MOST ONCE (1+1 nemosis compiler calls
    per month, down from 4+3 spread across the FCAS lane, the offer-factor
    lane and the monthly + daily curve builders) and derives every metric from
    the month's resident frames — FCAS factors (BIDPEROFFER_D FCAS rows),
    offer factors, monthly offer curves and the bounded daily stacks
    (BIDDAYOFFER_D prices + BIDPEROFFER_D ENERGY volumes). Frames are released
    before the next month, so memory stays bounded to one month at a time.
    A thin cached parquet is healed once from its SCHEMA ONLY (no row decode).

    S3-08 per-lane semantics are preserved: explicit ``--skip-*`` retains the
    cache; a total source failure retains the last-known-good history
    (degraded, stamped retained-stale); a partial failure merges the fresh
    months and degrades with the failures surfaced; a cold-start failure
    errors. Each lane's facts are persisted by the single owner
    (merge_month_rows); the returned LaneRuns feed the continuity guard and
    the run-status manifest.

    Returns ``(fcas_factors, offer_factors, offer_curves, offer_curves_daily,
    fcas_lane, offer_lane, curve_lane)`` — ``offer_curves_daily`` is None when
    no daily stacks were computed (skipped lane or failed month), so the
    caller leaves the published per-DUID day files untouched.
    """
    fcas_cache = data_dir / "fcas_factors.feather"
    offer_cache = data_dir / OFFER_FACTORS_CACHE
    curve_cache = data_dir / OFFER_CURVES_CACHE

    fcas_lane = LaneRun(source="fcas_factors", block="fcas_participation")
    offer_lane = LaneRun(source="offer_factors", block="offers")
    curve_lane = LaneRun(source="offer_curves", block="offer_curve")

    fcas_factors = pd.DataFrame()
    offer_factors = pd.DataFrame()
    offer_curves = pd.DataFrame()
    offer_curves_daily: pd.DataFrame | None = None

    if skip_fcas_factors:
        fcas_factors = _skip_lane_retained(
            fcas_lane, fcas_cache, "explicit --skip-fcas-factors",
        )
    if skip_offer_factors:
        offer_factors = _skip_lane_retained(
            offer_lane, offer_cache, "explicit --skip-offer-factors",
        )
        offer_curves = _skip_lane_retained(
            curve_lane, curve_cache, "explicit --skip-offer-factors",
        )
    if skip_fcas_factors and skip_offer_factors:
        return (fcas_factors, offer_factors, offer_curves, None,
                fcas_lane, offer_lane, curve_lane)

    want_fcas = not skip_fcas_factors
    want_offer = not skip_offer_factors
    cache_dir = str(data_dir)
    # None = auto-heal: inspect the cached parquet's schema and re-download
    # once when it lacks required columns; True = full-refresh re-download.
    rebuild = True if full_refresh else None
    attempted = len(months)

    fcas_frames: list[pd.DataFrame] = []
    offer_frames: list[pd.DataFrame] = []
    curve_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    fcas_errors: list[str] = []
    offer_errors: list[str] = []
    curve_errors: list[str] = []

    for year, month in months:
        month_label = f"{year}-{month:02d}"
        # ONE BIDPEROFFER_D fetch/decode per month, shared by every consumer.
        try:
            raw = fetch_bidperoffer_union(year, month, cache_dir, rebuild=rebuild)
        except Exception as e:
            msg = f"{month_label}: {e}"
            logger.warning("Bid fetch failed for %s: %s", month_label, e)
            if want_fcas:
                fcas_errors.append(msg)
            if want_offer:
                offer_errors.append(msg)
                curve_errors.append(msg)
            continue
        if raw is None or raw.empty:
            continue  # unpublished month / no rows — not a lane failure

        if want_fcas:
            try:
                bids = fcas_bids_from_raw(raw, year, month)
                if bids is not None and not bids.empty:
                    facts = compute_fcas_factors(bids, year, month)
                    if not facts.empty:
                        fcas_frames.append(facts)
            except Exception as e:
                fcas_errors.append(f"{month_label}: {e}")
                logger.warning(
                    "FCAS factor computation failed for %s: %s", month_label, e,
                )

        if want_offer:
            # ONE BIDDAYOFFER_D fetch/decode per month; the ENERGY volumes are
            # sliced from the resident BIDPEROFFER_D frame above — never a
            # second decode of the volume table. A fetch failure for the
            # shared source degrades both offer lanes; compute failures after
            # that degrade only the lane that failed.
            try:
                prices = fetch_energy_prices(year, month, cache_dir)
                if prices is None or prices.empty:
                    logger.warning(
                        f"BIDDAYOFFER_D {month_label}: no price rows — "
                        "offer lanes skip this month"
                    )
                    continue
                volumes = energy_volumes_from_raw(raw, year, month)
                if volumes is None or volumes.empty:
                    logger.warning(
                        f"Offers {month_label}: prices without volumes — "
                        "offer lanes skip this month"
                    )
                    continue
            except Exception as e:
                msg = f"{month_label}: {e}"
                logger.warning("Bid fetch failed for %s: %s", month_label, e)
                offer_errors.append(msg)
                curve_errors.append(msg)
                continue
            try:  # offer-factor lane
                feats = compute_offer_features(prices, volumes)
                if not feats.empty:
                    logger.info(
                        f"Offer factors {month_label}: {feats['duid'].nunique()} DUIDs"
                    )
                    offer_frames.append(feats)
            except Exception as e:
                offer_errors.append(f"{month_label}: {e}")
                logger.warning(
                    "Offer factor computation failed for %s: %s", month_label, e,
                )
            try:  # offer-curve lane: monthly + daily share the resident frames
                # in ONE guard — a failed month must not advance the daily
                # window while its monthly curve could not be derived.
                curves = compute_offer_curves(prices, volumes, month_label)
                if not curves.empty:
                    curve_frames.append(curves)
                    logger.info(
                        f"Offer curves {month_label}: {curves['duid'].nunique()} DUIDs"
                    )
                daily = compute_offer_curves_daily(prices, volumes, month_label)
                if not daily.empty:
                    daily_frames.append(daily)
            except Exception as e:
                curve_errors.append(f"{month_label}: {e}")
                logger.warning(
                    "Offer curve computation failed for %s: %s", month_label, e,
                )

    # S3-05 offer-factor note: adjacent-month fetches can each contribute a
    # row for the same (duid, month) — the next month's first trading day
    # rides in the prior month's inclusive-end window. Frames append in
    # ascending month order, so keep the LAST row per (duid, month): the
    # fuller copy wins (matches the pure builder's contract).
    if len(offer_frames) > 1:
        merged_feats = pd.concat(offer_frames, ignore_index=True)
        merged_feats = merged_feats.drop_duplicates(
            subset=["duid", "month"], keep="last",
        )
        offer_frames = [merged_feats]

    if want_fcas:
        fcas_lane.attempted_months = attempted
        fcas_factors = _finalize_factor_lane(
            fcas_lane, fcas_cache, fcas_frames, fcas_errors,
            full_refresh=full_refresh, label="FCAS factor",
            no_rows_error="no source rows for any attempted month",
            no_rows_note="source returned no rows (unpublished months?) — "
                         "last-known-good retained",
            cold_start_error="no source rows and no cache to retain (cold start)",
        )
    if want_offer:
        offer_lane.attempted_months = attempted
        curve_lane.attempted_months = attempted
        offer_factors = _finalize_factor_lane(
            offer_lane, offer_cache, offer_frames, offer_errors,
            full_refresh=full_refresh, label="offer factor",
            no_rows_error="no offer rows for any attempted month",
            no_rows_note="no offer rows (unpublished months?) — last-known-good retained",
            cold_start_error="no offer rows and no cache to retain (cold start)",
        )
        offer_curves = _finalize_factor_lane(
            curve_lane, curve_cache, curve_frames, curve_errors,
            full_refresh=full_refresh, label="offer curve",
            no_rows_error="no offer-curve rows for any attempted month",
            no_rows_note="no offer-curve rows (unpublished months?) — "
                         "last-known-good retained",
            cold_start_error="no offer-curve rows and no cache to retain (cold start)",
        )
        if daily_frames:
            offer_curves_daily = pd.concat(daily_frames, ignore_index=True)

    return (fcas_factors, offer_factors, offer_curves, offer_curves_daily,
            fcas_lane, offer_lane, curve_lane)


def _factor_source_status_map(lanes: list[LaneRun]) -> dict:
    """{source: {status, retained}} for the JSON writer's retained-stale stamp."""
    return {
        lane.source: {"status": lane.status, "retained": lane.retained}
        for lane in lanes
        if lane is not None and lane.block is not None
    }


def main():
    parser = argparse.ArgumentParser(description="AEMO Generator Credit Dashboard pipeline")
    parser.add_argument("--full-refresh", action="store_true", help="Re-download all data (5 years)")
    parser.add_argument("--months-back", type=int, default=config.DEFAULT_MONTHS_BACK,
                        help="Number of months to reprocess (default: 2)")
    parser.add_argument("--metadata-only", action="store_true",
                        help="Only download metadata and generate index")
    parser.add_argument("--refresh-metadata", action="store_true",
                        help="Refresh generator metadata without forcing a full data rebuild")
    parser.add_argument("--refresh-mlf", action="store_true",
                        help="Refresh MLF tracker data without forcing a full data rebuild")
    parser.add_argument("--skip-scada", action="store_true",
                        help="Skip SCADA download (use cached aggregates only)")
    parser.add_argument("--skip-constraints", action="store_true",
                        help="Skip constraint downloads (use cached constraint aggregates only)")
    parser.add_argument("--fcas-rebuild", action="store_true",
                        help="Rebuild FCAS history from cached DISPATCHPRICE files (implies --skip-scada)")
    parser.add_argument("--no-processed-cache-snapshot", action="store_true",
                        help="Do not restore/publish compact processed cache snapshots")
    parser.add_argument("--skip-offer-factors", action="store_true",
                        help="Skip per-DUID energy offer-curve factors")
    parser.add_argument("--skip-fcas-factors", action="store_true",
                        help="Skip per-DUID FCAS participation factors (BIDPEROFFER_D download)")
    args = parser.parse_args()
    if args.fcas_rebuild:
        args.skip_scada = True

    # S3-08: run-start timestamp for the run-status manifest (recorded up front
    # so a long run's manifest reflects when the run actually started).
    _run_started_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / config.DATA_DIR
    docs_data_dir = project_root / config.DOCS_DATA_DIR

    if not args.full_refresh and not args.no_processed_cache_snapshot:
        restore_processed_cache(data_dir, docs_data_dir)

    # Step 1: Generator metadata
    logger.info("=== Step 1: Generator metadata ===")
    generators = fetch_generators(
        str(data_dir), force=(args.full_refresh or args.refresh_metadata),
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
        str(data_dir), force=(args.full_refresh or args.refresh_mlf)
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

        # Load existing aggregates if incremental. These are the settled project
        # history; normal runs may only replace the recent mutable overlap window.
        if aggregates_path.exists() and not args.full_refresh:
            existing = pd.read_feather(aggregates_path)
            existing_before_update = existing.copy()
            logger.info(f"Loaded {len(existing)} existing aggregate rows")
        else:
            existing = pd.DataFrame()
            existing_before_update = pd.DataFrame()

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

                # S3-01: INTERMITTENT_GEN_SCADA quality summaries are no longer
                # fetched here — the grid/mechanical curtailment split was
                # unsupported causal inference and has been removed. Cached
                # quality feather files remain on disk (unused by the pipeline).

                # Build MLF lookup for this month's FY
                fy_start = year if month >= 7 else year - 1
                mlf_lookup = build_mlf_lookup(mlf_history, fy_start)

                # Aggregate
                monthly = aggregate_month(
                    scada, prices, dispatchload, generators, mlf_lookup, year, month,
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
            finally:
                # Free large DataFrames between months to stay within
                # GitHub Actions runner memory limits (~7 GB).
                scada = prices = dispatchload = None
                gc.collect()

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
            if not args.full_refresh:
                mutable_months = {f"{year}-{month:02d}" for year, month in months}
                _assert_protected_months_unchanged(
                    existing_before_update,
                    all_monthly,
                    mutable_months,
                    month_col="month",
                    label="Monthly aggregates",
                )
            all_monthly = all_monthly.sort_values(["duid", "month"]).reset_index(drop=True)
            all_monthly.to_feather(aggregates_path)
            logger.info(f"Saved {len(all_monthly)} aggregate rows to {aggregates_path}")

        # Daily aggregates — keep only last 12 months
        if new_daily_rows:
            new_daily = pd.concat(new_daily_rows, ignore_index=True)
            # S3-05: with interval-ending calendar days, consecutive monthly
            # fetches produce disjoint date sets — no boundary day is split
            # across months anymore, so the old max-merge of partial days is
            # gone. Guard against legacy/rerun duplicates instead: keep ONE
            # row per (duid, date). Month frames append in ascending order, so
            # a re-derived full day replaces any earlier partial copy.
            new_daily = new_daily.drop_duplicates(subset=["duid", "date"], keep="last")
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
                old_fcas_before_update = old_fcas.copy()
                reprocessed_months = set(new_fcas["month"].unique())
                old_fcas = old_fcas[~old_fcas["month"].isin(reprocessed_months)]
                merged_fcas = pd.concat([old_fcas, new_fcas], ignore_index=True)
                _assert_protected_months_unchanged(
                    old_fcas_before_update,
                    merged_fcas,
                    reprocessed_months,
                    month_col="month",
                    label="FCAS aggregates",
                )
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
    if args.skip_constraints:
        logger.info("=== Step 3b: Loading cached constraints (--skip-constraints) ===")
        if constraint_path.exists():
            all_constraints = pd.read_feather(constraint_path)
            logger.info(f"Loaded {len(all_constraints)} cached constraint aggregate rows")
        else:
            logger.warning("No cached constraint aggregates available")
    elif not args.metadata_only:
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
                        existing_constraints_before_update = existing_constraints.copy()
                        reprocessed = set(new_constraints["month"].unique())
                        existing_constraints = existing_constraints[
                            ~existing_constraints["month"].isin(reprocessed)
                        ]
                        all_constraints = pd.concat(
                            [existing_constraints, new_constraints], ignore_index=True
                        )
                        _assert_protected_months_unchanged(
                            existing_constraints_before_update,
                            all_constraints,
                            reprocessed,
                            month_col="month",
                            label="Constraint aggregates",
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

    # Step 3b-fix: apply capacity overrides retroactively so retained history
    # (monthly + daily CF) is consistent with corrected registration. This is
    # idempotent: re-deriving CF from stored generation and the override value
    # yields identical numbers every run, so settled-history invariants hold.
    if not all_monthly.empty and config.CAPACITY_OVERRIDES:
        from calendar import monthrange as _monthrange

        corrected_rows = 0
        for _duid, _new_cap in config.CAPACITY_OVERRIDES.items():
            m_mask = all_monthly["duid"] == _duid
            if m_mask.any():
                _hours = all_monthly.loc[m_mask, "month"].map(
                    lambda lbl: _monthrange(int(lbl[:4]), int(lbl[5:]))[1] * 24
                )
                all_monthly.loc[m_mask, "capacity_factor"] = (
                    all_monthly.loc[m_mask, "generation_mwh"] / (_new_cap * _hours)
                ).round(4)
                corrected_rows += int(m_mask.sum())
            if not all_daily.empty:
                d_mask = all_daily["duid"] == _duid
                if d_mask.any():
                    all_daily.loc[d_mask, "daily_capacity_factor"] = (
                        all_daily.loc[d_mask, "daily_generation_mwh"] / (_new_cap * 24)
                    ).round(4)
                    corrected_rows += int(d_mask.sum())
        if corrected_rows:
            logger.info(
                "Capacity overrides applied retroactively to %d aggregate rows",
                corrected_rows,
            )
            # Persist the corrected feathers: this block runs AFTER the Step-3
            # saves, so without re-saving, the on-disk history (which the
            # bounds tests and the processed cache read) keeps stale CF values
            # and the daily-CF bounds test fails on corrected units.
            all_monthly.to_feather(aggregates_path)
            if not all_daily.empty:
                all_daily.to_feather(daily_path)

    # Step 3c: Capacity factor audit
    if not all_monthly.empty:
        logger.info("=== Step 3c: Capacity factor audit ===")
        cf_candidates = audit_capacity_factors(all_monthly, generators)
        log_audit_results(cf_candidates)

    # Step 3d: Market-level credit-risk factors (daily spreads per region).
    # Uses the raw price cache for every month it holds, so history accumulates
    # even though the raw cache is pruned after 120 days.
    logger.info("=== Step 3d: Market spread factors ===")
    market_factors = pd.DataFrame()
    market_quarterly = pd.DataFrame()
    market_lane = LaneRun(source="market_factors")
    try:
        available_price_months = []
        for mdir in sorted((data_dir / "nemosis_cache").glob(
                "PUBLIC_ARCHIVE#DISPATCHPRICE#FILE01#*.parquet")):
            stem = mdir.name.split("#")[-1]
            available_price_months.append((int(stem[:4]), int(stem[4:6])))
        market_factors = build_market_factors(str(data_dir), available_price_months)
        market_quarterly = build_quarterly_summary(market_factors)
        check_qed_divergence(market_quarterly)
    except Exception as e:
        # S3-08: publish_market_json refuses empty factors (line 56) so the
        # published market_daily.json is left untouched on failure — but the
        # failure is recorded for the operator channel, never hidden.
        logger.error("Market factor computation failed: %s", e)
        market_lane.status = STATUS_DEGRADED
        market_lane.error = str(e)
        market_quarterly = pd.DataFrame()
    market_lane.frame = market_factors

    # Step 3e/3e2/3e3: Per-DUID optional-source factor facts.
    # S3-12: the month-outer driver fetches BIDPEROFFER_D + BIDDAYOFFER_D once
    # per (table, month) and derives the FCAS participation factors, energy
    # offer-curve factors, monthly offer curves and the bounded daily stacks
    # from the same resident frames (1+1 compiler calls per month, was 4+3).
    # S3-08: each lane still records its run state and retains the
    # last-known-good factor history from its cache when the source fails or
    # delivers nothing new — a failure may never erase populated panels from
    # the published generator docs. The factor-block continuity guard (Step
    # 3g) rejects the publish outright when a populated block would still
    # vanish, and the machine-readable run-status manifest (written at the
    # end) carries each lane's success/coverage/as-of/error state to the
    # operator channel.
    factor_months = _months_to_process(args.months_back, args.full_refresh)
    logger.info(
        "=== Steps 3e/3e2/3e3: FCAS + offer factors and offer curves "
        "(%d month(s), one fetch per table/month) ===", len(factor_months),
    )
    (fcas_factors, offer_factors, offer_curves, offer_curves_daily,
     fcas_lane, offer_lane, curve_lane) = _run_optional_factor_lanes(
        data_dir, factor_months,
        skip_fcas_factors=args.skip_fcas_factors,
        skip_offer_factors=args.skip_offer_factors,
        full_refresh=args.full_refresh,
    )
    if offer_curves_daily is not None and not offer_curves_daily.empty:
        # Daily stacks are an explicitly BOUNDED latest-window cache
        # (S3-07): recomputed each run for the processed window only,
        # never accumulated into five years of per-day history, and never
        # part of the durable processed-cache snapshot. The published
        # docs/data/offer_curves/{DUID}.json day files are rewritten from
        # this window each run through the semantic-diff publish gate
        # (S3-12: unchanged stacks leave the file — and its updated date —
        # untouched). On a failed lane the old day files are left untouched
        # (last-known-good) and the manifest records the lane as degraded —
        # they are never rewritten from stale frames.
        offer_curves_daily.to_feather(data_dir / OFFER_CURVES_DAILY_CACHE)
        write_offer_curve_files(offer_curves_daily, str(docs_data_dir))

    factor_lanes = [
        fcas_lane, offer_lane, curve_lane,
    ]

    # Step 3f: Freshness guards — fail BEFORE publishing stale data.
    # The daily commit is not a freshness signal: the pipeline re-processes the
    # most recent archive months, so a total download failure still "succeeds".
    logger.info("=== Step 3f: Freshness guards ===")
    check_monthly_freshness(all_monthly)
    check_daily_freshness(all_daily)

    # Step 3g: Factor-block continuity guard — reject publication when a
    # populated factor metric would vanish without a documented reason.
    # Core freshness passing must not hide a degraded optional source.
    logger.info("=== Step 3g: Factor-block continuity guard ===")
    gen_dir = docs_data_dir / "generators"
    violations = check_factor_block_continuity(
        gen_dir, {lane.source: lane for lane in factor_lanes},
    )
    if violations:
        raise RuntimeError(
            "S3-08 factor-block continuity guard: refusing to publish — "
            + " | ".join(violations)
        )

    # Step 4: Generate JSON output
    logger.info("=== Step 4: Generating JSON output ===")
    monthly_agg = all_monthly if not all_monthly.empty else None
    daily_agg = all_daily if not all_daily.empty else None
    constraint_agg = all_constraints if not all_constraints.empty else None
    count = generate_all(generators, monthly_agg, mlf_history,
                         draft_mlfs=draft_mlfs, draft_fy_label=draft_fy_label,
                         fcas_data=fcas_by_region_month if fcas_by_region_month else None,
                         daily_aggregates=daily_agg,
                         constraint_data=constraint_agg,
                         fcas_factors=fcas_factors if not fcas_factors.empty else None,
                         offer_factors=offer_factors if not offer_factors.empty else None,
                         offer_curves=offer_curves if offer_curves is not None and not offer_curves.empty else None,
                         factor_source_status=_factor_source_status_map(factor_lanes))
    publish_market_json(
        market_factors, market_quarterly, str(docs_data_dir),
        qed_benchmarks=QED_NEM_SPREAD_AUD_MWH,
    )
    if not args.no_processed_cache_snapshot:
        publish_processed_cache(data_dir, docs_data_dir)

    # Step 5: Machine-readable run-status manifest for the operator channel.
    # Written only when the publish actually happened (any guard failure above
    # raised and aborted the run before this point). Committed with the rest of
    # docs/data on the NAS; the Mac-side staleness check surfaces degraded or
    # errored lanes as alerts.
    all_lanes = factor_lanes + [market_lane]
    manifest = build_manifest(
        {lane.source: lane for lane in all_lanes if lane is not None},
        mode="full_refresh" if args.full_refresh else "incremental",
        months_back=None if args.full_refresh else args.months_back,
        guards={"monthly_freshness": "pass", "daily_freshness": "pass"},
        outputs={
            "generator_files": int(count),
            "processed_cache_snapshot": not args.no_processed_cache_snapshot,
        },
        started_utc=_run_started_utc,
    )
    write_run_manifest(manifest, docs_data_dir)
    logger.info("Done. Wrote index + %d generator files.", count)


if __name__ == "__main__":
    main()
