"""Energy offer-curve factors (BIDDAYOFFER_D prices + BIDPEROFFER_D volumes).

Builds per-DUID monthly offer-behaviour factors:
  - avg/peak offered energy availability (MW) from band volumes
  - price-band positioning (band 1 / band 10 averages)
  - negative-band day share (willingness to bid below $0)
  - top-2 band volume concentration (scarcity-band exposure)

Rebids/day is intentionally ABSENT (S3-09): _fetch_bid_table dedupes each
frame to the latest offer version per (DUID, day), so any post-dedupe
per-day row count is structurally 1.0 — a precise-looking constant, not
observed rebid intensity. Reintroduce only from RETAINED pre-deduplication
offer history (distinct actual offer versions/events counted before final
price-band selection).

All figures are OFFER-BASED ESTIMATES: intent expressed to the market, not
dispatch outcomes. Enablement/settlement remain participant-only.

Cache note: nemosis caches one parquet per (table, month) holding only the
columns previously requested. A thin cached parquet (written by an older
narrow fetch) is healed ONCE by re-downloading it "fat" — the column set is
inspected from the parquet SCHEMA ONLY (no row decode). See
src/download_bids.py for the S3-12 single-decode contract: one compiler call
per table/month serves the FCAS lane, offer-factor lane AND the monthly +
daily curve builders.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .download_bids import fetch_bidperoffer_union
from .interval_days import interval_calendar_day, interval_month
from .semantic_publish import write_json_if_facts_changed

logger = logging.getLogger(__name__)

BAND_AVAIL_COLS = [f"BANDAVAIL{i}" for i in range(1, 11)]
BAND_PRICE_COLS = [f"PRICEBAND{i}" for i in range(1, 11)]
PRICE_SELECT = (
    ["SETTLEMENTDATE", "DUID", "BIDTYPE", "OFFERDATE", "VERSIONNO"]
    + BAND_PRICE_COLS
)
OFFER_FACTORS_CACHE = "offer_factors.feather"


def _month_window(year: int, month: int) -> tuple[str, str]:
    start = f"{year}/{month:02d}/01 00:00:00"
    if month == 12:
        end = f"{year + 1}/01/01 00:00:00"
    else:
        end = f"{year}/{month + 1:02d}/01 00:00:00"
    return start, end


def _fetch_bid_table(
    table: str,
    year: int,
    month: int,
    cache_dir: str,
    select_columns: list[str],
    rebuild: bool = False,
) -> pd.DataFrame:
    from nemosis import dynamic_data_compiler

    start_time, end_time = _month_window(year, month)
    raw_data_location = str(Path(cache_dir) / "nemosis_cache")
    Path(raw_data_location).mkdir(parents=True, exist_ok=True)
    df = dynamic_data_compiler(
        start_time=start_time,
        end_time=end_time,
        table_name=table,
        raw_data_location=raw_data_location,
        select_columns=select_columns,
        fformat="parquet",
        rebuild=rebuild,
    )
    if df is None or df.empty:
        logger.warning(f"{table} {year}-{month:02d}: no data (unpublished month?)")
        return pd.DataFrame()
    missing = {"DUID", "BIDTYPE"} - set(df.columns)
    if missing:
        logger.warning(f"{table} {year}-{month:02d}: missing {sorted(missing)} — skipping")
        return pd.DataFrame()
    df = df[df["BIDTYPE"] == "ENERGY"].copy()
    if df.empty:
        logger.warning(f"{table} {year}-{month:02d}: no ENERGY rows")
        return pd.DataFrame()
    key = "INTERVAL_DATETIME" if "INTERVAL_DATETIME" in df.columns else "SETTLEMENTDATE"
    if "VERSIONNO" in df.columns:
        # Latest offer version wins (rebids); OFFERDATE breaks ties.
        df["VERSIONNO"] = pd.to_numeric(df["VERSIONNO"], errors="coerce")
        sort_cols = ["VERSIONNO"] + (["OFFERDATE"] if "OFFERDATE" in df.columns else [])
        df = df.sort_values(sort_cols).drop_duplicates(subset=["DUID", key], keep="last")
    else:
        # Monthly BIDPEROFFER_D archive CSVs lack VERSIONNO (daily files have
        # it). Rows appear in version order, so keep the last row per
        # (DUID, interval) — same no-guarantee caveat as the FCAS lane.
        logger.warning(
            f"{table} {year}-{month:02d}: VERSIONNO absent — deduping by file "
            "order (last row per DUID/interval)"
        )
        df = df.drop_duplicates(subset=["DUID", key], keep="last")
    return df


def fetch_energy_prices(year: int, month: int, cache_dir: str, rebuild: bool = False) -> pd.DataFrame:
    """Daily ENERGY offer price bands per DUID (latest version only)."""
    prices = _fetch_bid_table(
        "BIDDAYOFFER_D", year, month, cache_dir, PRICE_SELECT, rebuild=rebuild
    )
    if prices.empty:
        return prices
    for col in BAND_PRICE_COLS:
        if col not in prices.columns:
            logger.warning(f"BIDDAYOFFER_D {year}-{month:02d}: {col} missing")
            return pd.DataFrame()
        prices[col] = pd.to_numeric(prices[col], errors="coerce")
    prices["SETTLEMENTDATE"] = pd.to_datetime(prices["SETTLEMENTDATE"])
    return prices.dropna(subset=BAND_PRICE_COLS, how="all")


def energy_volumes_from_raw(raw: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """ENERGY volume rows sliced from a raw BIDPEROFFER_D frame (S3-12).

    Applies the same post-processing as the historical
    ``fetch_energy_volumes`` / ``_fetch_bid_table`` path — ENERGY-only filter,
    latest-offer-version dedupe, band numeric coercion — but to the run's ONE
    decoded frame, so no consumer re-invokes the nemosis compiler for a month
    the run already fetched.
    """
    if raw is None or raw.empty:
        return pd.DataFrame()
    missing = {"DUID", "BIDTYPE"} - set(raw.columns)
    if missing:
        logger.warning(f"BIDPEROFFER_D {year}-{month:02d}: missing {sorted(missing)} — skipping")
        return pd.DataFrame()
    volumes = raw[raw["BIDTYPE"] == "ENERGY"].copy()
    if volumes.empty:
        logger.warning(f"BIDPEROFFER_D {year}-{month:02d}: no ENERGY rows")
        return pd.DataFrame()
    if "VERSIONNO" in volumes.columns:
        # Latest offer version wins (rebids); OFFERDATE breaks ties.
        volumes["VERSIONNO"] = pd.to_numeric(volumes["VERSIONNO"], errors="coerce")
        sort_cols = ["VERSIONNO"] + (["OFFERDATE"] if "OFFERDATE" in volumes.columns else [])
        volumes = volumes.sort_values(sort_cols).drop_duplicates(
            subset=["DUID", "INTERVAL_DATETIME"], keep="last"
        )
    else:
        # Monthly BIDPEROFFER_D archive CSVs lack VERSIONNO (daily files have
        # it). Rows appear in version order, so keep the last row per
        # (DUID, interval) — same no-guarantee caveat as the FCAS lane.
        logger.warning(
            f"BIDPEROFFER_D {year}-{month:02d}: VERSIONNO absent — deduping by file "
            "order (last row per DUID/interval)"
        )
        volumes = volumes.drop_duplicates(subset=["DUID", "INTERVAL_DATETIME"], keep="last")
    for col in BAND_AVAIL_COLS:
        if col not in volumes.columns:
            logger.warning(f"BIDPEROFFER_D {year}-{month:02d}: {col} missing — fat rebuild required")
            return pd.DataFrame()
        volumes[col] = pd.to_numeric(volumes[col], errors="coerce")
    volumes["INTERVAL_DATETIME"] = pd.to_datetime(volumes["INTERVAL_DATETIME"])
    return volumes


def fetch_energy_volumes(year: int, month: int, cache_dir: str, rebuild: bool | None = None) -> pd.DataFrame:
    """Per-interval ENERGY offered volumes per DUID (latest version only).

    S3-12: never a second decode — this slices the ENERGY rows out of the
    run's single BIDPEROFFER_D union fetch (download_bids).

    rebuild=None: auto-detect a thin cached parquet (missing required
    columns) from its SCHEMA ONLY and rebuild it fat once.
    """
    raw = fetch_bidperoffer_union(year, month, cache_dir, rebuild=rebuild)
    return energy_volumes_from_raw(raw, year, month)


def compute_offer_features(prices: pd.DataFrame, volumes: pd.DataFrame) -> pd.DataFrame:
    """Per-DUID monthly offer-behaviour features from deduped prices+volumes.

    Prices: BIDDAYOFFER_D ENERGY rows (DUID, SETTLEMENTDATE, PRICEBAND1-10).
    Volumes: BIDPEROFFER_D ENERGY rows (DUID, INTERVAL_DATETIME, BANDAVAIL1-10).

    S3-05 trading-day alignment: volume intervals are stamped at their END,
    so a volume's month/calendar day is assigned from
    ``interval_month``/``interval_calendar_day`` (the interval ending at
    midnight belongs to the preceding day). Price rows name a whole trading
    day and keep their own SETTLEMENTDATE period.

    S3-05 coverage contract: each row gains ``vol_intervals_observed`` (the
    distinct volume intervals the unit offered into that month) and
    ``vol_source_complete`` — whether the fetched volume frame for that month
    reached the month's final calendar day. A mid-month archive/cache
    fragment is flagged incomplete so the attach layer never promotes it to
    an unqualified full-month statistic.
    """
    if prices is None or prices.empty or volumes is None or volumes.empty:
        return pd.DataFrame()

    p = prices.copy()
    p["month"] = p["SETTLEMENTDATE"].dt.to_period("M").astype(str)
    per_day = p.groupby(["DUID", "month"]).agg(
        n_days=("SETTLEMENTDATE", "nunique"),
        price_band_min_avg=("PRICEBAND1", "mean"),
        price_band_max_avg=("PRICEBAND10", "mean"),
    )
    per_day["negative_band_day_share"] = p.assign(neg=p["PRICEBAND1"] < 0).groupby(
        ["DUID", "month"]
    )["neg"].mean()
    # S3-09: no rebids_per_day here — p is already deduped to one row per
    # (DUID, day) by _fetch_bid_table, so a day-row count would be 1.0 always.

    v = volumes.copy()
    v["month"] = interval_month(v["INTERVAL_DATETIME"])
    v["_day"] = interval_calendar_day(v["INTERVAL_DATETIME"])
    # Source completeness per month present in this volume frame: complete
    # only if some unit offered on the month's final calendar day.
    last_by_month = v.groupby("month")["_day"].max()
    vol_complete: dict[str, bool] = {}
    for m, last in last_by_month.items():
        period = pd.Period(m, freq="M")
        vol_complete[m] = bool(
            (last.year, last.month) == (period.year, period.month)
            and last.day == period.days_in_month
        )
    v["offered_mw"] = v[BAND_AVAIL_COLS].sum(axis=1)
    band_total = v["offered_mw"].where(v["offered_mw"] > 0)
    v["top2_share"] = (
        (v["BANDAVAIL9"].fillna(0) + v["BANDAVAIL10"].fillna(0)) / band_total
    )
    vol = v.groupby(["DUID", "month"]).agg(
        offered_mw_avg=("offered_mw", "mean"),
        offered_mw_p95=("offered_mw", lambda s: s.quantile(0.95)),
        top2_band_volume_share=("top2_share", "mean"),
        intervals_offering=("offered_mw", "count"),
        vol_intervals_observed=("INTERVAL_DATETIME", "nunique"),
    )

    out = per_day.join(vol, how="outer").reset_index()
    out["top2_band_volume_share"] = out["top2_band_volume_share"].fillna(0).round(4)
    out["vol_source_complete"] = out["month"].map(vol_complete).fillna(False).astype(bool)
    for col in ("price_band_min_avg", "price_band_max_avg", "offered_mw_avg", "offered_mw_p95"):
        out[col] = out[col].round(2)
    out["negative_band_day_share"] = out["negative_band_day_share"].round(4)
    # Repo convention (matches fcas_factors): lowercase duid column
    return out.rename(columns={"DUID": "duid"})


def build_offer_factors(months: list[tuple[int, int]], cache_dir: str) -> pd.DataFrame:
    """Fetch + compute offer factors for each (year, month).

    S3-07 pure-builder contract: returns the computed facts and NEVER writes
    a cache file — the caller (main.py via src/factor_cache.merge_month_rows)
    owns persistence, so a warm run cannot overwrite the history file it then
    reads back to merge.
    """
    frames: list[pd.DataFrame] = []
    for year, month in months:
        prices = fetch_energy_prices(year, month, cache_dir)
        if prices.empty:
            continue
        volumes = fetch_energy_volumes(year, month, cache_dir)
        if volumes.empty:
            logger.warning(f"Offers {year}-{month:02d}: prices without volumes — skipping month")
            continue
        feats = compute_offer_features(prices, volumes)
        if not feats.empty:
            logger.info(
                f"Offer factors {year}-{month:02d}: {len(feats)} DUIDs"
            )
            frames.append(feats)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # S3-05: adjacent month fetches can each contribute a row for the same
    # (duid, month) — the next month's first trading day rides in the prior
    # month's inclusive-end window. Frames append in ascending month order, so
    # keep the LAST row per (duid, month): the fuller copy wins.
    out = out.drop_duplicates(subset=["duid", "month"], keep="last")
    logger.info("Computed %d offer factor rows for %d month(s)", len(out), len(months))
    return out


def attach_offer_factors(generators: list[dict], offer_factors: pd.DataFrame | None) -> list[dict]:
    """Attach doc['offers'] to generator dicts (offer-based estimates)."""
    if offer_factors is None or offer_factors.empty:
        return generators
    by_duid = {
        duid: g for duid, g in offer_factors.groupby("duid")
    }
    attached = 0
    for gen in generators:
        duid = gen.get("duid")
        if not duid or duid not in by_duid:
            continue
        rows = by_duid[duid].sort_values("month")
        latest = rows.iloc[-1]
        gen["offers"] = {
            "scope": "offer_based_estimate",
            "month": latest["month"],
            "avg_offered_mw": float(latest["offered_mw_avg"]),
            "offered_mw_p95": float(latest["offered_mw_p95"]),
            "price_band_min_avg": float(latest["price_band_min_avg"]),
            "price_band_max_avg": float(latest["price_band_max_avg"]),
            "negative_band_day_share": float(latest["negative_band_day_share"]),
            "top2_band_volume_share": float(latest["top2_band_volume_share"]),
        }
        attached += 1
    logger.info(f"Attached offer factors to {attached} generators")
    return generators


def attach_offer_factor_doc(doc: dict, rows: pd.DataFrame | None) -> None:
    """Attach doc['offers'] from this DUID's factor rows.

    S3-05 source-coverage contract: the headline month is the LATEST month
    whose volume source was complete (``vol_source_complete`` True) — a
    complete, comparable month. A newer partial fragment is never promoted to
    an unqualified full-month statistic; it is reported under
    ``later_partial_window`` instead. When only partial rows exist the latest
    one is used with ``window: \"partial\"``. Rows cached before the coverage
    columns existed (flag NaN) retain the historical latest-month behaviour —
    coverage is unknown, not invented.

    Each field falls back to the most recent month AT OR BEFORE the headline
    month that has it — e.g. the newest complete month may lack prices
    because BIDDAYOFFER_D is published ~2 weeks behind the volume table (and
    a newer partial month must never leak its fields into the headline). The
    month each price field came from is recorded in price_asof_month.
    """
    if rows is None or rows.empty:
        return
    rs = rows.sort_values("month").reset_index(drop=True)
    has_cov = {"vol_source_complete"} <= set(rs.columns)

    base_idx = len(rs) - 1  # legacy fallback: latest month
    if has_cov:
        complete_idx = rs.index[rs["vol_source_complete"].fillna(False).astype(bool)]
        if len(complete_idx):
            base_idx = complete_idx[-1]
    latest = rs.loc[base_idx]
    head = rs.loc[rs["month"] <= latest["month"]]

    def _latest_val(col):
        for _, row in head.iloc[::-1].iterrows():
            v = row.get(col)
            if pd.notna(v):
                return float(v), row["month"]
        return None, None

    out = {"scope": "offer_based_estimate", "month": latest["month"]}
    price_fields = {}
    for col, key in (
        ("offered_mw_avg", "avg_offered_mw"),
        ("offered_mw_p95", "offered_mw_p95"),
        ("price_band_min_avg", "price_band_min_avg"),
        ("price_band_max_avg", "price_band_max_avg"),
        ("negative_band_day_share", "negative_band_day_share"),
        ("top2_band_volume_share", "top2_band_volume_share"),
    ):
        v, asof = _latest_val(col)
        out[key] = v
        if key.startswith("price_band") or key in ("negative_band_day_share",):
            price_fields[key] = asof
    asof_months = sorted({m for m in price_fields.values() if m})
    if asof_months and asof_months[-1] != out["month"]:
        out["price_asof_month"] = asof_months[-1]

    if has_cov:
        flag = latest.get("vol_source_complete")
        if flag is not None and not pd.isna(flag):
            base_complete = bool(flag)
            out["source_complete"] = base_complete
            obs = latest.get("vol_intervals_observed")
            if obs is not None and not pd.isna(obs):
                out["intervals_observed"] = int(obs)
            if not base_complete:
                out["window"] = "partial"
        # Newer partial fragments than the chosen complete month: record, never promote.
        flagged = rs.loc[rs["vol_source_complete"].notna()]
        if not flagged.empty:
            later_partial = flagged.loc[
                (flagged["month"] > latest["month"])
                & (~flagged["vol_source_complete"].astype(bool))
            ]
            if len(later_partial):
                lp = later_partial.iloc[-1]
                lp_window = {"month": lp["month"]}
                obs = lp.get("vol_intervals_observed")
                if obs is not None and not pd.isna(obs):
                    lp_window["intervals_observed"] = int(obs)
                out["later_partial_window"] = lp_window
    doc["offers"] = out


OFFER_CURVES_CACHE = "offer_curves.feather"


def compute_offer_curves(prices: pd.DataFrame, volumes: pd.DataFrame, month: str) -> pd.DataFrame:
    """Per-DUID 10-band bid stack: mean offered price + cumulative mean MW.

    Both inputs must be version-deduped (fetch_energy_* guarantees this).
    DUIDs need BOTH price and volume rows for the month — an incomplete pair
    (e.g. price table lags the volume table) yields no curve rows.
    """
    if prices.empty or volumes.empty:
        return pd.DataFrame()
    missing_p = set(BAND_PRICE_COLS) - set(prices.columns)
    missing_v = set(BAND_AVAIL_COLS) - set(volumes.columns)
    if missing_p or missing_v:
        logger.warning(
            f"offer curves {month}: missing price cols {sorted(missing_p)} / "
            f"volume cols {sorted(missing_v)} — skipping"
        )
        return pd.DataFrame()
    # S3-05 trading-day alignment: BIDDAYOFFER price rows name a whole
    # trading day and belong to their OWN month — drop a next-month day-1 row
    # that the inclusive-end (start, end] fetch window dragged into this
    # month's frame. Volume intervals are assigned via interval_calendar_day
    # (midnight-ending intervals belong to the preceding day).
    pr = prices.copy()
    pr["_period"] = pr["SETTLEMENTDATE"].dt.to_period("M").astype(str)
    pr = pr[pr["_period"] == month]
    pv = pr.groupby("DUID")[BAND_PRICE_COLS].mean()
    vv = volumes.groupby("DUID")[BAND_AVAIL_COLS].mean().clip(lower=0)
    # Source completeness for this month's volume frame (mid-month fragments
    # never reach the month's final calendar day).
    vol_last = interval_calendar_day(volumes["INTERVAL_DATETIME"]).max()
    period = pd.Period(month, freq="M")
    source_complete = bool(
        (vol_last.year, vol_last.month) == (period.year, period.month)
        and vol_last.day == period.days_in_month
    )
    duids = pv.index.intersection(vv.index)
    rows = []
    for duid in duids:
        cum = 0.0
        for i in range(1, 11):
            cum += float(vv.loc[duid, f"BANDAVAIL{i}"])
            rows.append({
                "duid": duid,
                "month": month,
                "band": i,
                "price": float(pv.loc[duid, f"PRICEBAND{i}"]),
                "cum_mw": round(cum, 3),
                "source_complete": source_complete,
            })
    return pd.DataFrame(rows)


def build_offer_curves(
    months: list[tuple[int, int]], cache_dir: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch + compute per-DUID monthly offer curves and the bounded daily window.

    S3-07 pure-builder contract: returns ``(monthly, daily)`` facts and NEVER
    writes a cache file or the published day JSONs — the caller (main.py)
    owns persistence: monthly rows merge into the versioned history via
    src/factor_cache.merge_month_rows; the daily frame is an explicitly
    BOUNDED latest-window cache, overwritten each run for the processed
    window only (never five years of per-day stacks) and re-published to
    docs/data/offer_curves/{DUID}.json by write_offer_curve_files.

    ``daily`` is the bounded display window for the processed months; an
    empty frame means no daily stacks were computable this run.
    """
    frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    for year, month in months:
        try:
            # S3-12: fetch each table ONCE for this month; both the monthly
            # curve and the daily stack derive from the same resident frames
            # (no second decode — the old per-phase re-fetch is gone).
            prices = fetch_energy_prices(year, month, cache_dir)
            volumes = fetch_energy_volumes(year, month, cache_dir)
            month_label = f"{year}-{month:02d}"
            curves = compute_offer_curves(prices, volumes, month_label)
            if not curves.empty:
                frames.append(curves)
                n = curves["duid"].nunique()
                logger.info(f"Offer curves {year}-{month:02d}: {n} DUIDs")
            daily = compute_offer_curves_daily(prices, volumes, month_label)
            if not daily.empty:
                daily_frames.append(daily)
        except Exception as e:
            logger.warning(f"Offer curves {year}-{month:02d} failed: {e}")
    monthly = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    logger.info("Computed %d monthly offer-curve rows", len(monthly))

    daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    logger.info("Computed %d daily offer-curve rows", len(daily))
    return monthly, daily


def attach_offer_curve_doc(doc: dict, rows: pd.DataFrame | None) -> None:
    """Attach doc['offer_curve'] (latest COMPLETE month) for the bid-stack step chart.

    S3-05 source-coverage contract: rows carry ``source_complete`` (the month's
    volume frame reached its final calendar day). The chart month is the
    latest complete month; when only partial months exist the latest one is
    used and labelled ``window: \"partial\"``. Rows cached before the coverage
    column existed (flag absent/NaN) keep the historical latest-month
    behaviour — coverage unknown, not invented.
    """
    if rows is None or rows.empty:
        return
    rs = rows.sort_values(["month", "band"]).reset_index(drop=True)
    has_cov = "source_complete" in rs.columns

    base_month = rs["month"].iloc[-1]
    if has_cov:
        complete = rs.loc[rs["source_complete"].fillna(False).astype(bool)]
        if not complete.empty:
            base_month = complete["month"].iloc[-1]
    cur = rs[rs["month"] == base_month]
    if len(cur) < 10:
        return
    out = {
        "scope": "offer_based_estimate",
        "month": base_month,
        "bands": [
            {"band": int(r["band"]), "price": float(r["price"]), "cum_mw": float(r["cum_mw"])}
            for _, r in cur.iterrows()
        ],
    }
    if has_cov:
        flag = cur["source_complete"].iloc[0]
        if flag is not None and not pd.isna(flag) and not bool(flag):
            out["window"] = "partial"
    doc["offer_curve"] = out


OFFER_CURVES_DAILY_CACHE = "offer_curves_daily.feather"


def compute_offer_curves_daily(prices: pd.DataFrame, volumes: pd.DataFrame, month: str) -> pd.DataFrame:
    """Per-DUID PER-DAY 10-band bid stacks (mean of the day's intervals).

    BIDDAYOFFER_D carries one row per DUID/day (version-deduped upstream), so
    the day's prices are that row's bands; volumes are averaged across the
    day's intervals. S3-05: volume intervals are END-stamped — the day a
    volume belongs to is interval_calendar_day(INTERVAL_DATETIME), so the
    interval ending at midnight joins the preceding calendar day. Price days
    keep their BIDDAYOFFER_D trading-day date. Zero-width bands are dropped
    HERE (cum == previous cum), so downstream files stay compact and axes
    stay sane.
    """
    if prices.empty or volumes.empty:
        return pd.DataFrame()
    missing_p = set(BAND_PRICE_COLS) - set(prices.columns)
    missing_v = set(BAND_AVAIL_COLS) - set(volumes.columns)
    if missing_p or missing_v:
        logger.warning(
            f"daily offer curves {month}: missing cols {sorted(missing_p)}/{sorted(missing_v)} — skipping"
        )
        return pd.DataFrame()
    pr = prices.copy()
    vr = volumes.copy()
    pr["date"] = pd.to_datetime(pr["SETTLEMENTDATE"]).dt.strftime("%Y-%m-%d")
    vr["date"] = interval_calendar_day(pd.to_datetime(vr["INTERVAL_DATETIME"])).astype(str)
    pv = pr.groupby(["DUID", "date"])[BAND_PRICE_COLS].mean()
    vv = vr.groupby(["DUID", "date"])[BAND_AVAIL_COLS].mean().clip(lower=0)
    # BANDAVAIL tranches are INCREMENTAL (each band = additional MW on top of
    # the previous), matching compute_offer_features which sums them. Emit the
    # running sum as cum_mw; skip non-positive tranches.
    rows = []
    for (duid, date) in pv.index.intersection(vv.index):
        cum = 0.0
        for i in range(1, 11):
            inc = float(vv.loc[(duid, date), f"BANDAVAIL{i}"])
            if inc <= 1e-9:
                continue
            cum += inc
            rows.append({
                "duid": duid,
                "month": month,
                "date": date,
                "band": i,
                "price": float(pv.loc[(duid, date), f"PRICEBAND{i}"]),
                "cum_mw": round(cum, 3),
            })
    return pd.DataFrame(rows)


def write_offer_curve_files(curves: pd.DataFrame, docs_data_dir: str) -> int:
    """Write compact per-DUID day-stack JSONs: docs/data/offer_curves/{DUID}.json.

    S3-12 semantic-diff publish gate: each file is written only when its facts
    (the day stacks) changed — the ``updated`` date stamps data-as-of, not the
    run attempt, so a no-change run never bumps it and never manufactures a
    publishable diff. Returns the number of files actually written.
    """
    if curves is None or curves.empty:
        return 0
    out_dir = Path(docs_data_dir) / "offer_curves"
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    unchanged = 0
    stamp = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    for duid, grp in curves.sort_values(["date", "price"]).groupby("duid"):
        # Sanitize path-hostile characters (some DUIDs contain '/' or '#')
        safe = str(duid).replace("/", "_")
        days = []
        for date, dgrp in grp.groupby("date"):
            days.append({
                "date": date,
                "stack": [[float(r["price"]), float(r["cum_mw"])] for _, r in dgrp.iterrows()],
            })
        if write_json_if_facts_changed(
            out_dir / f"{safe}.json",
            {"scope": "offer_based_estimate", "updated": stamp, "days": days},
            stamp_keys=("updated",),
        ):
            n += 1
        else:
            unchanged += 1
    logger.info(
        "Wrote %d per-DUID offer-curve file(s) to %s (%d unchanged — stamp not bumped)",
        n, out_dir, unchanged,
    )
    return n
