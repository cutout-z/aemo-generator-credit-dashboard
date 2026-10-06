"""Months before five-minute settlement are valued at the 30-minute trading price.

Audit 2026-10 (M4): energy was settled at the 30-minute TRADING price until
1 October 2021, but every month was valued at the 5-minute dispatch price.
September 2021 is the one such month still inside the five-year history: the
published OAKEY1 captured price was -$18.00 (dispatch) against $313.76 at the
trading prices it was paid (raw MMSDM 2021-09 SCADA, DISPATCHPRICE and
TRADINGPRICE).
"""

import pandas as pd
import pytest

from src import main as pipeline
from src.aggregate import aggregate_month, settlement_prices, settles_on_trading_price


def _gens():
    return pd.DataFrame({
        "DUID": ["GT1"], "REGION": ["QLD1"], "CAPACITY_MW": [10.0],
        "FUEL_CATEGORY": ["Fossil"],
    })


def _september_2021():
    # Two trading intervals (ending 10:30 and 11:00), six dispatch intervals each.
    ts = pd.date_range("2021-09-15 10:05", periods=12, freq="5min")
    # The unit runs only in the last dispatch interval of the first half hour,
    # when the dispatch price was negative; the trading price for that half
    # hour was high. That is the OAKEY1 pattern.
    scada = pd.DataFrame({
        "SETTLEMENTDATE": ts, "DUID": "GT1",
        "SCADAVALUE": [0, 0, 0, 0, 0, 12.0, 0, 0, 0, 0, 0, 0],
    })
    dispatch = pd.DataFrame({
        "SETTLEMENTDATE": ts, "REGIONID": "QLD1",
        "RRP": [3000.0] * 5 + [-50.0] + [40.0] * 6,
    })
    trading = pd.DataFrame({
        "SETTLEMENTDATE": pd.to_datetime(["2021-09-15 10:30", "2021-09-15 11:00"]),
        "REGIONID": "QLD1", "RRP": [2491.67, 40.0],
    })
    return scada, dispatch, trading


def test_only_months_before_october_2021_settle_on_trading_price():
    assert settles_on_trading_price(2021, 9)
    assert not settles_on_trading_price(2021, 10)
    assert not settles_on_trading_price(2026, 8)


def test_each_dispatch_interval_takes_its_trading_interval_price():
    _scada, dispatch, trading = _september_2021()
    out = settlement_prices(2021, 9, dispatch, trading)
    assert out["RRP"].tolist() == [2491.67] * 6 + [40.0] * 6
    # The input frame is not modified.
    assert dispatch["RRP"].iloc[5] == -50.0


def test_revenue_and_captured_price_use_the_trading_price_in_september_2021():
    scada, dispatch, trading = _september_2021()
    row = aggregate_month(
        scada, settlement_prices(2021, 9, dispatch, trading), None, _gens(), {}, 2021, 9,
    ).iloc[0]
    # 12 MW for one 5-minute interval = 1 MWh, paid the half-hour price.
    assert row["generation_mwh"] == 1.0
    assert row["captured_price"] == 2491.67
    assert row["revenue_aud"] == 2492
    # Valued at the dispatch price it would have read -$50.
    wrong = aggregate_month(scada, dispatch, None, _gens(), {}, 2021, 9).iloc[0]
    assert wrong["captured_price"] == -50.0


def test_five_minute_months_keep_dispatch_prices():
    _scada, dispatch, _trading = _september_2021()
    later = dispatch.assign(SETTLEMENTDATE=dispatch["SETTLEMENTDATE"] + pd.DateOffset(months=1))
    out = settlement_prices(2021, 10, later, None)
    assert out is later


def test_missing_trading_prices_refuse_rather_than_fall_back():
    _scada, dispatch, _trading = _september_2021()
    with pytest.raises(RuntimeError, match="trading prices"):
        settlement_prices(2021, 9, dispatch, pd.DataFrame())


def test_pipeline_fetches_trading_prices_for_pre_5ms_months(monkeypatch, tmp_path):
    _scada, dispatch, trading = _september_2021()
    calls = []

    def fake_fetch(year, month, cache_dir, rebuild=False):
        calls.append((year, month))
        return trading

    monkeypatch.setattr(pipeline, "fetch_trading_price_month", fake_fetch)
    out = pipeline._settlement_prices_for(2021, 9, dispatch, tmp_path, rebuild=False)
    assert calls == [(2021, 9)]
    assert out["RRP"].iloc[5] == 2491.67
    # From October 2021 the dispatch price is the settlement price: no fetch.
    assert pipeline._settlement_prices_for(2021, 10, dispatch, tmp_path, rebuild=False) is dispatch
    assert calls == [(2021, 9)]
