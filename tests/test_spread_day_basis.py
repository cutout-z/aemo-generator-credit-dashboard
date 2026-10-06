"""Market spreads use interval-ending days; "VWAP" is a simple mean (L2)."""

import numpy as np
import pandas as pd

from src import market_factors as mf

ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent


def _two_days():
    # Intervals ending 2026-08-03 00:05 .. 2026-08-05 00:00: exactly two days.
    ts = pd.date_range("2026-08-03 00:05", periods=576, freq="5min")
    rrp = np.r_[np.full(288, 50.0), np.full(288, 100.0)]
    rrp[287] = 5000.0  # the interval ENDING at 2026-08-04 00:00: 3 Aug's last
    return pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "SA1", "RRP": rrp})


def test_interval_ending_at_midnight_belongs_to_the_day_it_closes():
    out = mf.compute_daily_spreads(_two_days()).set_index("date")
    assert list(out.index) == ["2026-08-03", "2026-08-04"]
    assert out["intervals"].tolist() == [288, 288]
    assert out.loc["2026-08-03", "spread_max"] == 4950.0   # 5000 - 50
    assert out.loc["2026-08-04", "spread_max"] == 0.0      # flat $100 day
    assert (out["day_basis"] == mf.DAY_BASIS).all()


def test_rows_on_the_old_day_basis_are_listed_for_recompute(tmp_path):
    pd.DataFrame({"date": ["2026-07-01"], "region": "SA1", "spread_8h": [1.0],
                  "avg_price": [50.0]}).to_feather(tmp_path / mf.MARKET_FACTORS_CACHE)
    assert mf.months_needing_backfill(str(tmp_path)) == [(2026, 7)]


def test_window_prices_are_described_as_simple_means():
    page = (ROOT / "docs" / "index.html").read_text()
    panel = page[page.index('id="panelMarketSpread"'):page.index('id="spreadDuration"')]
    assert "VWAP" not in panel and "simple mean" in panel
    from src import generate_market_json
    import inspect
    assert "not volume-weighted" in inspect.getsource(generate_market_json.publish_market_json)
