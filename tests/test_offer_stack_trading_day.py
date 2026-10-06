"""Daily offer stacks pair a trading day's prices with that day's volumes (L10).

BIDDAYOFFER_D prices are keyed by NEM trading day (04:00-04:00); the per-
interval BIDPEROFFER_D volumes were grouped by calendar day, so every day's
stack used prices from one trading day and volumes shifted by four hours.
"""

import pandas as pd

from src.interval_days import interval_trading_day_str
from src.offer_curves import BAND_AVAIL_COLS, BAND_PRICE_COLS, compute_offer_curves_daily


def test_trading_day_boundaries():
    ts = pd.Series(pd.to_datetime([
        "2026-07-02 04:00", "2026-07-02 04:05", "2026-07-03 00:00", "2026-07-03 04:00",
    ]))
    assert interval_trading_day_str(ts).tolist() == [
        "2026-07-01", "2026-07-02", "2026-07-02", "2026-07-02",
    ]


def _prices():
    rows = []
    for day, p1 in (("2026-07-01", -50.0), ("2026-07-02", 20.0)):
        rows.append({"DUID": "U1", "SETTLEMENTDATE": pd.Timestamp(day),
                     **{c: p1 + 10 * i for i, c in enumerate(BAND_PRICE_COLS)}})
    return pd.DataFrame(rows)


def _volumes():
    # Trading day 1 Jul offers 100 MW in band 1; trading day 2 Jul offers 40 MW.
    ts = pd.date_range("2026-07-01 04:05", "2026-07-03 04:00", freq="5min")
    band1 = [100.0 if t <= pd.Timestamp("2026-07-02 04:00") else 40.0 for t in ts]
    df = pd.DataFrame({"DUID": "U1", "INTERVAL_DATETIME": ts})
    for c in BAND_AVAIL_COLS:
        df[c] = 0.0
    df["BANDAVAIL1"] = band1
    return df


def test_each_days_stack_uses_that_trading_days_volumes():
    out = compute_offer_curves_daily(_prices(), _volumes(), "2026-07")
    by_day = out.set_index("date")["cum_mw"].to_dict()
    # Calendar days blended 4 hours of the other trading day into each stack:
    # 2 Jul read (20 x 100 + 268 x 40) / 288 = 44.2 MW.
    assert by_day == {"2026-07-01": 100.0, "2026-07-02": 40.0}


def test_previous_months_last_trading_day_is_not_a_stub():
    vols = _volumes()
    early = vols.iloc[:1].assign(INTERVAL_DATETIME=pd.Timestamp("2026-07-01 02:00"))
    prices = pd.concat([_prices(), _prices().iloc[:1].assign(SETTLEMENTDATE=pd.Timestamp("2026-06-30"))])
    out = compute_offer_curves_daily(prices, pd.concat([early, vols]), "2026-07")
    assert "2026-06-30" not in set(out["date"])
