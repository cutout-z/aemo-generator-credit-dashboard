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
    df["date"] = df["SETTLEMENTDATE"].dt.date.astype(str)

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


def build_market_factors(data_dir: str, months: list[tuple[int, int]]) -> pd.DataFrame:
    """Compute daily spread metrics for all cached price months and persist.

    Reads DISPATCHPRICE from the raw cache (no new downloads for months not
    in `months`); merges results into the accumulated daily factors feather so
    history survives raw-cache pruning.

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
        already = not existing.empty and (existing["date"].str.startswith(month_label)).any()
        if already and "spread_8h" in existing.columns:
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
    grouped = df.groupby(["region", "quarter"]).agg(
        avg_spread_decile=("spread_decile", "mean"),
        avg_spread_max=("spread_max", "mean"),
        avg_vwap_high=("vwap_high", "mean"),
        avg_vwap_low=("vwap_low", "mean"),
        neg_price_share=("neg_price_share", "mean"),
        days_covered=("date", "nunique"),
    ).reset_index().round(2)
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
