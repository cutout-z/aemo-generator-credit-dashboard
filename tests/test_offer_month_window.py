"""Energy-offer history must not be overwritten by a neighbouring month's stub.

Audit 2026-10 (H2): nemosis widens a BIDDAYOFFER_D month window back to the
previous trading day, so September's fetch carries 31 August's prices. The
offer-factor builder grouped every row by its own month, so that one day
became a "2026-08" row which (a) won the in-run keep-last dedupe and (b)
replaced the full August month in offer_factors.feather through
merge_month_rows. offer_factors 2026-05..08 ended up with 1 price day each,
and the published headline fell to a 48-interval September fragment while
run_status said ok.

These tests pin: each fetch is filtered to its own month; a less complete
month never replaces a more complete cached one; a finished month left
partial (or kept from the cache over a worse fetch) degrades the lane.
"""

from datetime import datetime

import pandas as pd

from src.factor_cache import merge_month_rows, month_completeness
from src.main import _finalize_factor_lane
from src.offer_curves import (
    OFFER_FACTOR_COMPLETENESS_COLS,
    compute_offer_features,
    keep_most_complete_offer_rows,
)
from src.run_status import STATUS_DEGRADED, STATUS_OK, LaneRun


def _prices(days, duid="T1", band1=-50.0):
    return pd.DataFrame({
        "DUID": duid,
        "SETTLEMENTDATE": pd.to_datetime(list(days)),
        **{f"PRICEBAND{i}": (band1 if i == 1 else 10.0 * i) for i in range(1, 11)},
    })


def _volumes(stamps, duid="T1", mw=100.0):
    return pd.DataFrame({
        "DUID": duid,
        "INTERVAL_DATETIME": pd.to_datetime(list(stamps)),
        **{f"BANDAVAIL{i}": (mw if i == 1 else 0.0) for i in range(1, 11)},
    })


def _august_fetch():
    """What the August fetch returns: prices 31 Jul..31 Aug (nemosis widens
    the trading-day window back one day), volumes (1 Aug 00:00, 1 Sep 00:00]."""
    days = pd.date_range("2026-07-31", "2026-08-31", freq="D")
    stamps = pd.date_range("2026-08-01 00:05", "2026-09-01 00:00", freq="6h")
    return _prices(days), _volumes(stamps)


def _september_stub_fetch():
    """The 30 Sep run's September fetch: only trading day 31 Aug is published,
    so prices hold 31 Aug and volumes hold 1 Sep 00:05-04:00 (48 intervals)."""
    stamps = pd.date_range("2026-09-01 00:05", "2026-09-01 04:00", freq="5min")
    return _prices(["2026-08-31"], band1=500.0), _volumes(stamps, mw=685.0)


class TestOwnMonthFilter:
    def test_september_fetch_emits_no_august_row(self):
        prices, vols = _september_stub_fetch()
        unfiltered = compute_offer_features(prices, vols)
        assert set(unfiltered["month"]) == {"2026-08", "2026-09"}  # the old stub

        feats = compute_offer_features(prices, vols, "2026-09")
        assert set(feats["month"]) == {"2026-09"}
        sep = feats.iloc[0]
        assert sep["vol_intervals_observed"] == 48
        assert not bool(sep["vol_source_complete"])

    def test_august_fetch_keeps_all_31_price_days_and_drops_july(self):
        prices, vols = _august_fetch()
        feats = compute_offer_features(prices, vols, "2026-08")
        assert set(feats["month"]) == {"2026-08"}
        aug = feats.iloc[0]
        assert aug["n_days"] == 31
        assert bool(aug["vol_source_complete"])

    def test_history_survives_the_next_months_run(self, tmp_path):
        """End to end through the cache: August's run, then September's."""
        cache = tmp_path / "offer_factors.feather"
        aug = compute_offer_features(*_august_fetch(), "2026-08")
        merge_month_rows(cache, aug, label="offer factor",
                         completeness_cols=OFFER_FACTOR_COMPLETENESS_COLS)
        sep = compute_offer_features(*_september_stub_fetch(), "2026-09")
        merged = merge_month_rows(cache, sep, label="offer factor",
                                  completeness_cols=OFFER_FACTOR_COMPLETENESS_COLS)
        by_month = merged.set_index("month")
        assert by_month.loc["2026-08", "n_days"] == 31
        assert by_month.loc["2026-08", "negative_band_day_share"] == 1.0
        assert set(merged["month"]) == {"2026-08", "2026-09"}


class TestNeverReplaceWithLessComplete:
    def _full_and_stub(self):
        full = compute_offer_features(*_august_fetch(), "2026-08")
        stub = compute_offer_features(*_september_stub_fetch())  # unfiltered: has the Aug stub
        return full, stub[stub["month"] == "2026-08"]

    def test_merge_keeps_the_complete_cached_month(self, tmp_path):
        cache = tmp_path / "offer_factors.feather"
        full, stub = self._full_and_stub()
        full.to_feather(cache)
        kept: list[str] = []
        merged = merge_month_rows(cache, stub, label="offer factor",
                                  completeness_cols=OFFER_FACTOR_COMPLETENESS_COLS,
                                  kept_months=kept)
        assert kept == ["2026-08"]
        assert merged.loc[merged["month"] == "2026-08", "n_days"].tolist() == [31]
        assert pd.read_feather(cache)["n_days"].tolist() == [31]

    def test_equal_or_better_fresh_month_still_replaces(self, tmp_path):
        cache = tmp_path / "offer_factors.feather"
        full, stub = self._full_and_stub()
        stub.to_feather(cache)
        merged = merge_month_rows(cache, full, label="offer factor",
                                  completeness_cols=OFFER_FACTOR_COMPLETENESS_COLS)
        assert merged["n_days"].tolist() == [31]
        revised = full.assign(price_band_max_avg=999.0)
        merged = merge_month_rows(cache, revised, label="offer factor",
                                  completeness_cols=OFFER_FACTOR_COMPLETENESS_COLS)
        assert merged["price_band_max_avg"].tolist() == [999.0]

    def test_without_completeness_cols_merge_is_unchanged(self, tmp_path):
        cache = tmp_path / "offer_factors.feather"
        full, stub = self._full_and_stub()
        full.to_feather(cache)
        merged = merge_month_rows(cache, stub, label="offer factor")
        assert merged["n_days"].tolist() == [1]

    def test_in_run_dedupe_keeps_most_complete_not_last(self):
        full, stub = self._full_and_stub()
        out = keep_most_complete_offer_rows(pd.concat([full, stub], ignore_index=True))
        assert out["n_days"].tolist() == [31]

    def test_completeness_score_orders_flag_first(self):
        full, stub = self._full_and_stub()
        assert (month_completeness(full, OFFER_FACTOR_COMPLETENESS_COLS)["2026-08"]
                > month_completeness(stub, OFFER_FACTOR_COMPLETENESS_COLS)["2026-08"])


def _finalize(tmp_path, frames, today):
    lane = LaneRun(source="offer_factors", block="offers")
    out = _finalize_factor_lane(
        lane, tmp_path / "offer_factors.feather", frames, [],
        full_refresh=False, label="offer factor", no_rows_error="x",
        no_rows_note="x", cold_start_error="x",
        completeness_cols=OFFER_FACTOR_COMPLETENESS_COLS, today=today,
    )
    return lane, out


class TestLaneStatus:
    def test_complete_finished_month_is_ok(self, tmp_path):
        full = compute_offer_features(*_august_fetch(), "2026-08")
        lane, _ = _finalize(tmp_path, [full], datetime(2026, 9, 30))
        assert lane.status == STATUS_OK

    def test_running_month_partial_is_ok(self, tmp_path):
        sep = compute_offer_features(*_september_stub_fetch(), "2026-09")
        lane, _ = _finalize(tmp_path, [sep], datetime(2026, 9, 30))
        assert lane.status == STATUS_OK

    def test_finished_month_partial_degrades(self, tmp_path):
        sep = compute_offer_features(*_september_stub_fetch(), "2026-09")
        lane, _ = _finalize(tmp_path, [sep], datetime(2026, 10, 5))
        assert lane.status == STATUS_DEGRADED
        assert "2026-09" in lane.error

    def test_cached_month_kept_over_worse_fetch_degrades(self, tmp_path):
        full = compute_offer_features(*_august_fetch(), "2026-08")
        full.to_feather(tmp_path / "offer_factors.feather")
        stub = compute_offer_features(*_september_stub_fetch())
        lane, out = _finalize(tmp_path, [stub[stub["month"] == "2026-08"]],
                              datetime(2026, 8, 31))
        assert lane.status == STATUS_DEGRADED
        assert "cached month kept" in lane.error
        assert out["n_days"].tolist() == [31]
