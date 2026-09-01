"""Publish market-level credit-risk factors as dashboard JSON.

Schema (per region):
    dates, vwap_high/vwap_low/spread_decile/spread_max/neg_price_share/price_std
        — legacy decile (~2.4h window) series, kept for continuity + QED check
    by_duration: {"1h"|"2h"|"4h"|"8h": {vwap_high[], vwap_low[], spread[]}}
        — duration-parameterized capture windows (4.2%/8.3%/16.7%/33.3% of
          the day's intervals), so spreads match the battery being assessed.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

from .market_factors import (
    MARKET_FACTORS_CACHE,
    QED_NEM_SPREAD_AUD_MWH,
    quarter_label,
    build_quarterly_summary as quarterly_spreads,
)

logger = logging.getLogger(__name__)

DURATIONS = ["1h", "2h", "4h", "8h"]


def _clean(value) -> float | None:
    """None out NaN/inf so json.dump emits null, not NaN (invalid JSON)."""
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def publish_market_json(
    market_factors,
    market_quarterly,
    out_dir: str,
    qed_benchmarks: dict | None = None,
) -> None:
    """Write market_daily.json from in-memory factor DataFrames.

    Args:
        market_factors: daily region factors (incl. duration columns) as built
            by build_market_factors.
        market_quarterly: region x quarter summary as built by
            build_quarterly_summary.
        out_dir: directory to write market_daily.json into (docs/data).
        qed_benchmarks: optional {quarter: AUD/MWh} override of the built-in
            QED reference table.
    """
    if market_factors is None or len(market_factors) == 0:
        logger.warning("publish_market_json: no market factors to publish")
        return
    df = market_factors.copy()

    regions: dict[str, dict] = {}
    for region, g in df.groupby("region"):
        g = g.sort_values("date")
        entry = {
            "dates": g["date"].tolist(),
            "vwap_high": [_clean(v) for v in g["vwap_high"]],
            "vwap_low": [_clean(v) for v in g["vwap_low"]],
            "spread_decile": [_clean(v) for v in g["spread_decile"]],
            "spread_max": [_clean(v) for v in g["spread_max"]],
            "neg_price_share": [_clean(v) for v in g["neg_price_share"]],
            "price_std": [_clean(v) for v in g["price_std"]],
        }
        by_duration = {}
        for dur in DURATIONS:
            hi, lo, sp = f"vwap_high_{dur}", f"vwap_low_{dur}", f"spread_{dur}"
            if hi in g.columns:
                by_duration[dur] = {
                    "vwap_high": [_clean(v) for v in g[hi]],
                    "vwap_low": [_clean(v) for v in g[lo]],
                    "spread": [_clean(v) for v in g[sp]],
                }
        entry["by_duration"] = by_duration
        regions[region] = entry

    quarterly = (
        market_quarterly.to_dict("records")
        if market_quarterly is not None and len(market_quarterly) > 0
        else quarterly_spreads(df).to_dict("records")
    )
    # Per-duration quarterly averages (dashboard can toggle duration later)
    dq = df.copy()
    dq["quarter"] = dq["date"].map(quarter_label)
    dur_avg = (
        dq.groupby(["region", "quarter"])[["spread_2h", "spread_4h", "spread_8h"]]
        .mean()
        .round(2)
    )
    for row in quarterly:
        key = (row["region"], row["quarter"])
        if key in dur_avg.index:
            for c in ("spread_2h", "spread_4h", "spread_8h"):
                row[c] = float(dur_avg.loc[key, c])
        ref = (qed_benchmarks or QED_NEM_SPREAD_AUD_MWH).get(row["quarter"])
        row["qed_reference"] = ref
        row["qed_divergence_ratio"] = (
            round(row["avg_spread_decile"] / ref, 2) if ref else None
        )

    payload = {
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "description": (
            "Regional daily price-spread factors from AEMO 5-minute DISPATCHPRICE "
            "data. spread_decile uses a fixed top/bottom-10% window (~2.4h, a "
            "1-2h battery proxy); by_duration provides 1h/2h/4h/8h capture "
            "windows for duration-matched BESS analysis. Market-level: applies "
            "to every unit in the region."
        ),
        "regions": regions,
        "quarterly": quarterly,
    }

    out = Path(out_dir) / "market_daily.json"
    out.write_text(json.dumps(payload, separators=(",", ":")))
    logger.info("Wrote %s (%d regions, %d quarters)", out, len(regions), len(quarterly))


