"""Market-level credit-risk factors from regional 5-minute prices.

Computes (per region, per day):
- VWAP of top-decile price intervals  (discharge-window value)
- VWAP of bottom-decile price intervals (charge-window cost)
- Decile spread (realizable arbitrage proxy) and max-min spread
- Negative-price interval share, price volatility

Also rolls up to quarterly summaries and cross-checks the derived NEM-wide
spread against AEMO's published Quarterly Energy Dynamics (QED) figures.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from . import config
from .download_dispatch import fetch_dispatch_price_month
from .interval_days import interval_calendar_day_str

logger = logging.getLogger(__name__)

MARKET_FACTORS_CACHE = "market_factors_daily.feather"
MARKET_DAILY_JSON = "market_daily.json"
MARKET_QUARTERLY_JSON = "market_quarterly.json"

# AEMO Quarterly Energy Dynamics — NEM-wide average battery charge/discharge
# price spread, AUD/MWh. Used as an external benchmark: if our derived spread
# diverges wildly from QED's published figure for the same quarter, either our
# derivation or the market's behaviour has shifted and someone should look.
# Source: AEMO QED reports, https://www.aemo.com.au/energy-systems/major-publications/quarterly-energy-dynamics-qed
QED_NEM_SPREAD_AUD_MWH = {
    "2025Q2": 342.0,
    "2026Q1": 121.0,
    "2026Q2": 51.0,
}
QED_DIVERGENCE_RATIO_MIN = 0.4  # derived below 40% of QED -> investigate
QED_DIVERGENCE_RATIO_MAX = 2.5  # derived above 250% of QED -> investigate


def compute_daily_spreads(prices: pd.DataFrame) -> pd.DataFrame:
    """Compute per-region per-day spread metrics from 5-minute DISPATCHPRICE rows.

    The "vwap_*" fields are SIMPLE means of the window's 5-minute prices (each
    interval weighted equally; there is no volume), kept under their published
    names.

    Pure function — no I/O, unit-testable.

    Args:
        prices: DataFrame with SETTLEMENTDATE (datetime), REGIONID (str), RRP (float)

    Returns:
        DataFrame with one row per (region, date) and columns:
        date, region, vwap_high, vwap_low, spread_decile, spread_max,
        neg_price_share, price_std, intervals
    """
    if prices is None or prices.empty:
        return pd.DataFrame()

    df = prices.copy()
    # Interval-ENDING days: the interval ending at 00:00 is the previous day's
    # last five minutes. dt.date put it in the next day, so every day had 287
    # of its own intervals plus the previous day's last one.
    df["date"] = interval_calendar_day_str(df["SETTLEMENTDATE"])

    out = []
    for (region, date_str), g in df.groupby(["REGIONID", "date"]):
        rrp = g["RRP"].to_numpy(dtype=float)
        n = len(rrp)
        # Skip partial days (e.g. month files that include the next month's
        # first interval) — a 1-interval "day" would publish spread 0.
        if n < 72:
            continue
        # Capture-window VWAPs. The decile (~2.4h at 288 intervals) is the
        # legacy 1-2h-battery proxy; the duration series parameterizes the
        # window so spreads match the battery being assessed. Window means
        # are monotone in window size: spread_1h >= spread_2h >= 4h >= 8h.
        sorted_desc = np.sort(rrp)[::-1]
        row = {
            "date": date_str,
            "region": region,
            "day_basis": DAY_BASIS,
            # Time-weighted (simple interval) mean price of the day: the
            # like-for-like counterpart the AER VWA cross-check divides by.
            "avg_price": round(float(rrp.mean()), 2),
            "neg_price_share": round(float((rrp < 0).mean()), 4),
            "price_std": round(float(rrp.std()), 2),
            "intervals": int(n),
        }
        for dur, hours in (("1h", 24), ("2h", 12), ("4h", 6), ("8h", 3)):
            k_d = max(1, round(n / hours))
            hi = float(sorted_desc[:k_d].mean())
            lo = float(sorted_desc[-k_d:].mean())
            row[f"vwap_high_{dur}"] = round(hi, 2)
            row[f"vwap_low_{dur}"] = round(lo, 2)
            row[f"spread_{dur}"] = round(hi - lo, 2)
        k = max(1, n // 10)
        row["vwap_high"] = round(float(sorted_desc[:k].mean()), 2)
        row["vwap_low"] = round(float(sorted_desc[-k:].mean()), 2)
        row["spread_decile"] = round(row["vwap_high"] - row["vwap_low"], 2)
        row["spread_max"] = round(float(rrp.max() - rrp.min()), 2)
        out.append(row)

    return pd.DataFrame(out)


# A cached month missing any of these is recomputed whenever its prices can be
# read (raw cache, or a re-download for months_needing_backfill).
DAILY_FACTOR_REQUIRED_COLS = ("spread_8h", "avg_price", "day_basis")
# Days are interval-ending calendar days (interval_days); rows without this
# marker were built on the old timestamp-date basis and are recomputed.
DAY_BASIS = "interval-ending"


def months_needing_backfill(data_dir: str) -> list[tuple[int, int]]:
    """Months in the accumulated daily factors that lack a required column.

    The raw price cache is pruned after 120 days, so a column added later
    (avg_price, for the AER cross-check) would otherwise never reach older
    months. Listing them lets the pipeline re-fetch those months' DISPATCHPRICE
    once (about 2 MB a month) and fill them in.
    """
    path = Path(data_dir) / MARKET_FACTORS_CACHE
    if not path.exists():
        return []
    df = pd.read_feather(path)
    if df.empty or "date" not in df.columns:
        return []
    incomplete = pd.Series(False, index=df.index)
    for col in DAILY_FACTOR_REQUIRED_COLS:
        incomplete |= df[col].isna() if col in df.columns else True
    months = sorted({d[:7] for d in df.loc[incomplete, "date"].astype(str)})
    return [(int(m[:4]), int(m[5:7])) for m in months]


def build_market_factors(data_dir: str, months: list[tuple[int, int]]) -> pd.DataFrame:
    """Compute daily spread metrics for all cached price months and persist.

    Reads DISPATCHPRICE from the raw cache (no new downloads for months not
    in `months`); merges results into the accumulated daily factors feather so
    history survives raw-cache pruning. S3-07: this accumulated history is
    part of the versioned processed-cache snapshot
    (docs/data/processed-cache/market_factors_daily.feather) — a cold runner
    restores it before recomputing the recent window, so the long regional
    trend behind the published market_daily.json survives bounded raw
    retention. Restore fills gaps only; a local file is never clobbered.

    Returns the full accumulated DataFrame.
    """
    data_path = Path(data_dir) / MARKET_FACTORS_CACHE
    existing = pd.DataFrame()
    if data_path.exists():
        existing = pd.read_feather(data_path)
        logger.info(f"Loaded {len(existing)} existing market-factor rows")

    new_frames = []
    for year, month in months:
        month_label = f"{year}-{month:02d}"
        # Skip months already computed (settled market data never changes),
        # unless they predate the duration columns — recompute those from the
        # raw price cache so by_duration backfills wherever prices survive.
        in_month = (
            existing["date"].str.startswith(month_label)
            if not existing.empty else pd.Series(dtype=bool)
        )
        already = bool(in_month.any())
        complete = already and all(
            col in existing.columns and existing.loc[in_month, col].notna().all()
            for col in DAILY_FACTOR_REQUIRED_COLS
        )
        if complete:
            continue
        # else fall through: fetch_dispatch_price_month(rebuild=False) uses the
        # raw cache only and raises if pruned — months keep decile-only history.
        try:
            prices = fetch_dispatch_price_month(year, month, data_dir, rebuild=False)
        except Exception as e:
            logger.warning(f"Market factors: no price data for {month_label}: {e}")
            continue
        if prices.empty:
            logger.warning(f"Market factors: price cache empty for {month_label}, skipping")
            continue
        daily = compute_daily_spreads(prices)
        if not daily.empty:
            new_frames.append(daily)
            logger.info(f"Market factors {month_label}: {len(daily)} region-days")

    if new_frames:
        new_df = pd.concat(new_frames, ignore_index=True)
        merged = (
            pd.concat([existing, new_df], ignore_index=True)
            .drop_duplicates(subset=["region", "date"], keep="last")
            .sort_values(["region", "date"])
            .reset_index(drop=True)
        )
        merged.to_feather(data_path)
        logger.info(f"Saved {len(merged)} market-factor rows to {data_path}")
        return merged
    return existing


def quarter_label(date_str: str) -> str:
    ts = pd.Timestamp(date_str)
    return f"{ts.year}Q{(ts.month - 1) // 3 + 1}"


def build_quarterly_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Roll daily market factors up to region x quarter summary rows."""
    if daily is None or daily.empty:
        return pd.DataFrame()
    df = daily.copy()
    df["quarter"] = df["date"].map(quarter_label)
    spec = dict(
        avg_spread_decile=("spread_decile", "mean"),
        avg_spread_max=("spread_max", "mean"),
        avg_vwap_high=("vwap_high", "mean"),
        avg_vwap_low=("vwap_low", "mean"),
        neg_price_share=("neg_price_share", "mean"),
        days_covered=("date", "nunique"),
    )
    if "avg_price" in df.columns:
        spec["avg_price"] = ("avg_price", "mean")
        spec["avg_price_days"] = ("avg_price", "count")
    grouped = df.groupby(["region", "quarter"]).agg(**spec).reset_index()
    # Dollar fields to the cent; the negative-price SHARE keeps 4 dp — rounded
    # to 2 dp it moved the AER ratio check by up to 8% (TAS1 2025Q4 0.0639 -> 0.06).
    share = grouped["neg_price_share"].round(4)
    grouped = grouped.round(2)
    grouped["neg_price_share"] = share
    return grouped


def check_qed_divergence(quarterly: pd.DataFrame) -> None:
    """Compare derived NEM-wide spread for QED-covered quarters against QED.

    Logs a warning (does not raise) when the derived spread falls outside the
    tolerance band — methodology differences mean this is an investigate flag,
    not a hard failure.
    """
    if quarterly is None or quarterly.empty:
        return
    nem = (
        quarterly.groupby("quarter")
        .apply(lambda g: np.average(g["avg_spread_decile"], weights=g["days_covered"]))
        if "quarter" in quarterly.columns else {}
    )
    for quarter, qed_spread in QED_NEM_SPREAD_AUD_MWH.items():
        if quarter not in nem.index:
            continue
        derived = float(nem.loc[quarter])
        if derived <= 0:
            continue
        ratio = derived / qed_spread
        if ratio < QED_DIVERGENCE_RATIO_MIN or ratio > QED_DIVERGENCE_RATIO_MAX:
            logger.warning(
                "QED divergence: derived NEM avg decile spread for %s is %.0f AUD/MWh "
                "vs AEMO QED published %.0f AUD/MWh (ratio %.2f outside [%.1f, %.1f]). "
                "Investigate derivation or check for methodology/market regime shift.",
                quarter, derived, qed_spread, ratio,
                QED_DIVERGENCE_RATIO_MIN, QED_DIVERGENCE_RATIO_MAX,
            )
        else:
            logger.info(
                "QED benchmark %s: derived %.0f vs published %.0f AUD/MWh (ratio %.2f) — OK",
                quarter, derived, qed_spread, ratio,
            )
