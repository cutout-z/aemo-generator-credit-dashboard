"""Publish market-level credit-risk factors as dashboard JSON."""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

MARKET_DAILY_JSON = "market_daily.json"
MARKET_QUARTERLY_JSON = "market_quarterly.json"


def _sanitize(obj):
    """Replace NaN/Inf floats with None for valid JSON."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def publish_market_json(
    daily_factors: pd.DataFrame,
    quarterly: pd.DataFrame,
    docs_data_dir: str,
    qed_benchmarks: dict[str, float] | None = None,
) -> None:
    """Write market_daily.json and market_quarterly.json under docs/data."""
    if daily_factors is None or daily_factors.empty:
        logger.warning("No market factors computed — skipping market JSON publish")
        return

    out_dir = Path(docs_data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    regions = {}
    for region, g in daily_factors.groupby("region"):
        g = g.sort_values("date")
        regions[region] = {
            "dates": g["date"].tolist(),
            "vwap_high": g["vwap_high"].tolist(),
            "vwap_low": g["vwap_low"].tolist(),
            "spread_decile": g["spread_decile"].tolist(),
            "spread_max": g["spread_max"].tolist(),
            "neg_price_share": g["neg_price_share"].tolist(),
            "price_std": g["price_std"].tolist(),
        }

    daily_doc = {
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "description": (
            "Daily market credit-risk factors per region: top/bottom-decile VWAP "
            "(discharge value / charge cost), decile spread (realizable BESS "
            "arbitrage proxy), max-min spread, negative-price interval share, "
            "price volatility. Derived from AEMO DISPATCHPRICE 5-minute data."
        ),
        "regions": regions,
    }
    (out_dir / MARKET_DAILY_JSON).write_text(
        json.dumps(_sanitize(daily_doc), separators=(",", ":"))
    )
    logger.info(f"Wrote {MARKET_DAILY_JSON}: {len(regions)} regions, "
                f"{daily_factors['date'].nunique()} days")

    if quarterly is not None and not quarterly.empty:
        quarterly_doc = {
            "updated_utc": daily_doc["updated_utc"],
            "rows": quarterly.to_dict(orient="records"),
            "qed_benchmark_nem_spread_aud_mwh": qed_benchmarks or {},
        }
        (out_dir / MARKET_QUARTERLY_JSON).write_text(
            json.dumps(_sanitize(quarterly_doc), separators=(",", ":"))
        )
        logger.info(f"Wrote {MARKET_QUARTERLY_JSON}: {len(quarterly)} region-quarter rows")
