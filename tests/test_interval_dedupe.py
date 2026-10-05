"""Duplicate-interval protection for the monthly joins (June 2022 audit).

The MMSDM June 2022 DISPATCHLOAD archive repeats 614,466 (SETTLEMENTDATE,
DUID) rows with INTERVENTION=0. aggregate_month left-joined it onto SCADA
without a dedupe, so every repeat multiplied that interval's generation and
revenue (NEM June 2022 published +18.2%, BW01 CF 1.152). These tests pin:

  - repeated DISPATCHLOAD / DISPATCHPRICE / SCADA intervals count once,
  - repeats that disagree resolve to the LAST row (documented rule),
  - a join that still multiplies rows raises instead of aggregating,
  - DISPATCHCONSTRAINT repeats do not inflate binding hours.
"""

import pandas as pd
import pytest

from src import aggregate as agg
from src.aggregate import (
    aggregate_constraints_month,
    aggregate_month,
    dedupe_interval_keys,
)


def _gens(duid="SOLAR1", fuel="Solar"):
    return pd.DataFrame({
        "DUID": [duid], "REGION": ["NSW1"], "CAPACITY_MW": [10.0],
        "FUEL_CATEGORY": [fuel],
    })


def _frames(n=12, duid="SOLAR1"):
    ts = pd.date_range("2022-06-10 10:05", periods=n, freq="5min")
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": duid, "SCADAVALUE": 6.0})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "NSW1", "RRP": 100.0})
    load = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": duid, "AVAILABILITY": 8.0})
    return scada, prices, load


def _row(scada, prices, load, gens=None):
    out = aggregate_month(scada, prices, load, gens if gens is not None else _gens(),
                          {}, 2022, 6)
    assert len(out) == 1
    return out.iloc[0]


class TestDuplicatedDispatchload:
    def test_identical_repeats_do_not_inflate_generation_or_revenue(self):
        scada, prices, load = _frames()
        clean = _row(scada, prices, load)
        # The June 2022 shape: every interval appears four times, INTERVENTION=0.
        dup_load = pd.concat([load] * 4, ignore_index=True)
        dirty = _row(scada, prices, dup_load)
        # 12 intervals × 6 MW / 12 = 6 MWh; ×$100 = $600 (unadjusted).
        assert clean["generation_mwh"] == 6.0
        assert dirty["generation_mwh"] == clean["generation_mwh"]
        assert dirty["revenue_aud"] == clean["revenue_aud"] == 600
        assert dirty["capacity_factor"] == clean["capacity_factor"]
        assert dirty["curtailment_pct"] == clean["curtailment_pct"]
        assert dirty["curtailment_potential_mwh"] == clean["curtailment_potential_mwh"]

    def test_partial_repeats_only_on_some_intervals(self):
        """Repeats confined to part of the month (June 2022: 3rd-23rd) must
        not reweight those intervals in captured price or the histogram."""
        scada, prices, load = _frames()
        prices.loc[6:, "RRP"] = 300.0
        dup_load = pd.concat([load, load.iloc[6:], load.iloc[6:]], ignore_index=True)
        clean = _row(scada, prices, load)
        dirty = _row(scada, prices, dup_load)
        assert dirty["captured_price"] == clean["captured_price"] == 200.0
        assert dirty["generation_mwh"] == clean["generation_mwh"]

    def test_disagreeing_repeats_keep_the_last_row(self, caplog):
        scada, prices, load = _frames(n=1)
        load2 = load.assign(AVAILABILITY=12.0)
        both = pd.concat([load, load2], ignore_index=True)
        with caplog.at_level("WARNING"):
            row = _row(scada, prices, both)
        # Last row wins: availability 12 MW → shortfall 1 − 6/12 = 0.5.
        assert row["curtailment_pct"] == 0.5
        assert row["curtailment_potential_mwh"] == 1.0
        assert "disagreed" in caplog.text

    def test_duplicated_scada_and_price_rows_count_once(self):
        scada, prices, load = _frames()
        clean = _row(scada, prices, load)
        dirty = _row(
            pd.concat([scada, scada], ignore_index=True),
            pd.concat([prices, prices], ignore_index=True),
            load,
        )
        assert dirty["generation_mwh"] == clean["generation_mwh"]
        assert dirty["revenue_aud"] == clean["revenue_aud"]

    def test_join_fan_out_fails_loudly(self, monkeypatch):
        """Backstop: if a duplicate ever slips past the dedupe, the join must
        raise rather than silently aggregate multiplied rows."""
        monkeypatch.setattr(agg, "dedupe_interval_keys", lambda df, keys, **kw: df)
        scada, prices, load = _frames()
        with pytest.raises((RuntimeError, pd.errors.MergeError)):
            aggregate_month(scada, prices, pd.concat([load, load]), _gens(), {}, 2022, 6)


class TestDedupeHelper:
    def test_no_duplicates_returns_input_unchanged(self):
        _, _, load = _frames()
        assert dedupe_interval_keys(load, ["SETTLEMENTDATE", "DUID"], label="t") is load

    def test_collapses_to_one_row_per_key(self):
        _, _, load = _frames()
        out = dedupe_interval_keys(
            pd.concat([load] * 3, ignore_index=True), ["SETTLEMENTDATE", "DUID"],
            label="t", value_cols=["AVAILABILITY"],
        )
        assert len(out) == len(load)
        assert not out.duplicated(["SETTLEMENTDATE", "DUID"]).any()


class TestDuplicatedDispatchconstraint:
    def test_repeated_binding_rows_do_not_inflate_hours(self):
        ts = pd.date_range("2022-06-10 10:05", periods=12, freq="5min")
        dc = pd.DataFrame({"SETTLEMENTDATE": ts, "CONSTRAINTID": "N>>X",
                           "MARGINALVALUE": 5.0})
        spdcp = pd.DataFrame({"CONNECTIONPOINTID": ["CP1"], "GENCONID": ["N>>X"]})
        gencon = pd.DataFrame({"GENCONID": ["N>>X"], "DESCRIPTION": ["test"]})
        clean = aggregate_constraints_month(dc, spdcp, gencon, {"SOLAR1": "CP1"}, 2022, 6)
        dirty = aggregate_constraints_month(
            pd.concat([dc] * 4, ignore_index=True), spdcp, gencon,
            {"SOLAR1": "CP1"}, 2022, 6,
        )
        assert clean["hours_bound"].tolist() == [1.0]
        assert dirty["hours_bound"].tolist() == [1.0]
