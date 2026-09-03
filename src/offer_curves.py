"""Energy offer-curve factors (BIDDAYOFFER_D prices + BIDPEROFFER_D volumes).

Builds per-DUID monthly offer-behaviour factors:
  - avg/peak offered energy availability (MW) from band volumes
  - price-band positioning (band 1 / band 10 averages)
  - negative-band day share (willingness to bid below $0)
  - rebid intensity (distinct offer versions per day)
  - top-2 band volume concentration (scarcity-band exposure)

All figures are OFFER-BASED ESTIMATES: intent expressed to the market, not
dispatch outcomes. Enablement/settlement remain participant-only.

Cache note: nemosis caches one parquet per (table, month) holding only the
columns previously requested. The volumes fetch requires BANDAVAIL1-10, so on
first run it rebuilds the month's parquet "fat" (one-time re-download). The
FCAS lane reads a subset and never rewrites (rebuild=False), so the fat cache
persists and both lanes share one download per month.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

BAND_AVAIL_COLS = [f"BANDAVAIL{i}" for i in range(1, 11)]
BAND_PRICE_COLS = [f"PRICEBAND{i}" for i in range(1, 11)]
VOLUME_SELECT = (
    ["INTERVAL_DATETIME", "DUID", "BIDTYPE", "VERSIONNO"] + BAND_AVAIL_COLS
)
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


def fetch_energy_volumes(year: int, month: int, cache_dir: str, rebuild: bool | None = None) -> pd.DataFrame:
    """Per-interval ENERGY offered volumes per DUID (latest version only).

    rebuild=None: auto-detect a thin cached parquet (no BANDAVAIL columns,
    written by an older narrow fetch) and rebuild it fat once.
    """
    if rebuild is None:
        parquet = (
            Path(cache_dir) / "nemosis_cache"
            / f"PUBLIC_ARCHIVE#BIDPEROFFER_D#FILE01#{year}{month:02d}010000.parquet"
        )
        if parquet.exists():
            try:
                cached_cols = set(pd.read_parquet(parquet).columns)
            except Exception:
                cached_cols = set()
            rebuild = not set(BAND_AVAIL_COLS).issubset(cached_cols)
            if rebuild:
                logger.info(
                    f"BIDPEROFFER_D {year}-{month:02d}: cached parquet lacks band "
                    "columns — rebuilding fat (one-time)"
                )
        else:
            rebuild = False
    volumes = _fetch_bid_table(
        "BIDPEROFFER_D", year, month, cache_dir, VOLUME_SELECT, rebuild=rebuild
    )
    if volumes.empty:
        return volumes
    for col in BAND_AVAIL_COLS:
        if col not in volumes.columns:
            logger.warning(f"BIDPEROFFER_D {year}-{month:02d}: {col} missing — fat rebuild required")
            return pd.DataFrame()
        volumes[col] = pd.to_numeric(volumes[col], errors="coerce")
    volumes["INTERVAL_DATETIME"] = pd.to_datetime(volumes["INTERVAL_DATETIME"])
    return volumes


def compute_offer_features(prices: pd.DataFrame, volumes: pd.DataFrame) -> pd.DataFrame:
    """Per-DUID monthly offer-behaviour features from deduped prices+volumes.

    Prices: BIDDAYOFFER_D ENERGY rows (DUID, SETTLEMENTDATE, PRICEBAND1-10).
    Volumes: BIDPEROFFER_D ENERGY rows (DUID, INTERVAL_DATETIME, BANDAVAIL1-10).
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
    # rebids = offer versions per day; grouping keys are excluded inside
    # apply, so count rows per day with size() rather than a named column.
    per_day["rebids_per_day"] = p.groupby(["DUID", "month"]).apply(
        lambda g: g.groupby("SETTLEMENTDATE").size().mean(), include_groups=False
    )

    v = volumes.copy()
    v["month"] = v["INTERVAL_DATETIME"].dt.to_period("M").astype(str)
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
    )

    out = per_day.join(vol, how="outer").reset_index()
    out["top2_band_volume_share"] = out["top2_band_volume_share"].fillna(0).round(4)
    for col in ("price_band_min_avg", "price_band_max_avg", "offered_mw_avg", "offered_mw_p95"):
        out[col] = out[col].round(2)
    out["rebids_per_day"] = out["rebids_per_day"].round(3)
    out["negative_band_day_share"] = out["negative_band_day_share"].round(4)
    # Repo convention (matches fcas_factors): lowercase duid column
    return out.rename(columns={"DUID": "duid"})


def build_offer_factors(months: list[tuple[int, int]], cache_dir: str, data_dir: str) -> pd.DataFrame:
    """Fetch + compute offer factors for each (year, month); save feather."""
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
    out_path = Path(data_dir) / OFFER_FACTORS_CACHE
    out.to_feather(out_path)
    logger.info(f"Saved {len(out)} offer factor rows to {out_path}")
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
            "rebids_per_day": float(latest["rebids_per_day"]),
            "top2_band_volume_share": float(latest["top2_band_volume_share"]),
        }
        attached += 1
    logger.info(f"Attached offer factors to {attached} generators")
    return generators


def attach_offer_factor_doc(doc: dict, rows: pd.DataFrame | None) -> None:
    """Attach doc['offers'] from this DUID's factor rows.

    The summary takes the latest month, but each field falls back to the most
    recent month that has it — e.g. the newest month may lack prices because
    BIDDAYOFFER_D is published ~2 weeks behind the volume table. The month
    each price field came from is recorded in price_asof_month.
    """
    if rows is None or rows.empty:
        return
    rs = rows.sort_values("month")
    latest = rs.iloc[-1]

    def _latest_val(col):
        for _, row in rs.iloc[::-1].iterrows():
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
        ("rebids_per_day", "rebids_per_day"),
        ("top2_band_volume_share", "top2_band_volume_share"),
    ):
        v, asof = _latest_val(col)
        out[key] = v
        if key.startswith("price_band") or key in (
            "negative_band_day_share", "rebids_per_day"
        ):
            price_fields[key] = asof
    asof_months = sorted({m for m in price_fields.values() if m})
    if asof_months and asof_months[-1] != out["month"]:
        out["price_asof_month"] = asof_months[-1]
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
    pv = prices.groupby("DUID")[BAND_PRICE_COLS].mean()
    vv = volumes.groupby("DUID")[BAND_AVAIL_COLS].mean().clip(lower=0)
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
            })
    return pd.DataFrame(rows)


def build_offer_curves(months: list[tuple[int, int]], cache_dir: str, data_dir: str) -> pd.DataFrame:
    """Fetch + compute per-DUID offer curves for each month; cache-merge like factors."""
    frames = []
    for year, month in months:
        try:
            prices = fetch_energy_prices(year, month, cache_dir)
            volumes = fetch_energy_volumes(year, month, cache_dir)
            curves = compute_offer_curves(prices, volumes, f"{year}-{month:02d}")
            if not curves.empty:
                frames.append(curves)
                n = curves["duid"].nunique()
                logger.info(f"Offer curves {year}-{month:02d}: {n} DUIDs")
        except Exception as e:
            logger.warning(f"Offer curves {year}-{month:02d} failed: {e}")
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out_path = Path(data_dir) / OFFER_CURVES_CACHE
    out.to_feather(out_path)
    logger.info(f"Saved {len(out)} offer curve rows to {out_path}")

    # Daily stacks: reuse the same fetched frames, no extra downloads
    daily_frames = []
    for year, month in months:
        try:
            prices = fetch_energy_prices(year, month, cache_dir)
            volumes = fetch_energy_volumes(year, month, cache_dir)
            d = compute_offer_curves_daily(prices, volumes, f"{year}-{month:02d}")
            if not d.empty:
                daily_frames.append(d)
        except Exception as e:
            logger.warning(f"Daily offer curves {year}-{month:02d} failed: {e}")
    if daily_frames:
        daily = pd.concat(daily_frames, ignore_index=True)
        daily.to_feather(Path(data_dir) / OFFER_CURVES_DAILY_CACHE)
        logger.info(f"Saved {len(daily)} daily offer-curve rows")
        write_offer_curve_files(daily, str(Path(data_dir).parent / "docs" / "data"))
    return out


def attach_offer_curve_doc(doc: dict, rows: pd.DataFrame | None) -> None:
    """Attach doc['offer_curve'] (latest month) for the bid-stack step chart."""
    if rows is None or rows.empty:
        return
    rs = rows.sort_values(["month", "band"])
    latest_month = rs["month"].iloc[-1]
    cur = rs[rs["month"] == latest_month]
    if len(cur) < 10:
        return
    doc["offer_curve"] = {
        "scope": "offer_based_estimate",
        "month": latest_month,
        "bands": [
            {"band": int(r["band"]), "price": float(r["price"]), "cum_mw": float(r["cum_mw"])}
            for _, r in cur.iterrows()
        ],
    }


OFFER_CURVES_DAILY_CACHE = "offer_curves_daily.feather"


def compute_offer_curves_daily(prices: pd.DataFrame, volumes: pd.DataFrame, month: str) -> pd.DataFrame:
    """Per-DUID PER-DAY 10-band bid stacks (mean of the day's intervals).

    BIDDAYOFFER_D carries one row per DUID/day (version-deduped upstream), so
    the day's prices are that row's bands; volumes are averaged across the
    day's intervals. Zero-width bands are dropped HERE (cum == previous cum),
    so downstream files stay compact and axes stay sane.
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
    vr["date"] = pd.to_datetime(vr["INTERVAL_DATETIME"]).dt.strftime("%Y-%m-%d")
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
    """Write compact per-DUID day-stack JSONs: docs/data/offer_curves/{DUID}.json."""
    if curves is None or curves.empty:
        return 0
    out_dir = Path(docs_data_dir) / "offer_curves"
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for duid, grp in curves.sort_values(["date", "price"]).groupby("duid"):
        days = []
        for date, dgrp in grp.groupby("date"):
            days.append({
                "date": date,
                "stack": [[float(r["price"]), float(r["cum_mw"])] for _, r in dgrp.iterrows()],
            })
        (out_dir / f"{duid}.json").write_text(json.dumps({
            "scope": "offer_based_estimate",
            "updated": pd.Timestamp.utcnow().strftime("%Y-%m-%d"),
            "days": days,
        }, separators=(",", ":")))
        n += 1
    logger.info(f"Wrote {n} per-DUID offer-curve files to {out_dir}")
    return n
