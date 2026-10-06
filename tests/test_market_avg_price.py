"""Daily/quarterly average price for the AER cross-check (audit 2026-10, M6)."""

import numpy as np
import pandas as pd

from src import market_factors as mf


def _day(date="2026-08-03", region="NSW1", prices=None):
    ts = pd.date_range(f"{date} 00:05", periods=288, freq="5min")
    rrp = prices if prices is not None else np.linspace(-20, 300, 288)
    return pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": region, "RRP": rrp})


def test_daily_spreads_carry_the_time_weighted_average():
    # Half the day at $10, half at $90 (the midnight interval at the mean, so
    # the check does not depend on which day that interval is assigned to).
    prices = np.r_[np.full(143, 10.0), np.full(143, 90.0), [50.0, 50.0]]
    out = mf.compute_daily_spreads(_day(prices=prices))
    # Simple interval mean, not volume weighted.
    assert out.loc[out["date"] == "2026-08-03", "avg_price"].tolist() == [50.0]


def test_quarterly_summary_averages_price_and_keeps_share_precision():
    daily = pd.DataFrame({
        "date": ["2025-10-01", "2025-11-01", "2025-12-01"], "region": "TAS1",
        "spread_decile": 1.0, "spread_max": 1.0, "vwap_high": 1.0, "vwap_low": 1.0,
        "neg_price_share": [0.128, 0.029, 0.0362], "avg_price": [40.0, 41.0, None],
    })
    q = mf.build_quarterly_summary(daily).iloc[0]
    assert q["avg_price"] == 40.5
    assert q["avg_price_days"] == 2
    # 0.0644 at 4 dp; the old blanket round(2) published 0.06.
    assert q["neg_price_share"] == 0.0644


def test_months_without_avg_price_are_listed_for_backfill(tmp_path):
    old = pd.DataFrame({
        "date": ["2024-08-01", "2024-08-02", "2026-08-01"], "region": "NSW1",
        "spread_8h": [1.0, 1.0, 1.0], "avg_price": [None, None, 50.0],
    })
    old.to_feather(tmp_path / mf.MARKET_FACTORS_CACHE)
    assert mf.months_needing_backfill(str(tmp_path)) == [(2024, 8)]


def test_cached_month_without_avg_price_is_recomputed(tmp_path, monkeypatch):
    old = pd.DataFrame({
        "date": ["2026-08-03"], "region": "NSW1", "spread_8h": [1.0],
        "vwap_high": [1.0], "vwap_low": [1.0], "spread_decile": [0.0],
        "spread_max": [0.0], "neg_price_share": [0.0],
    })
    old.to_feather(tmp_path / mf.MARKET_FACTORS_CACHE)
    monkeypatch.setattr(mf, "fetch_dispatch_price_month",
                        lambda y, m, d, rebuild=False: _day())
    out = mf.build_market_factors(str(tmp_path), [(2026, 8)])
    assert out["avg_price"].notna().all()
