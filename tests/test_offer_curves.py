"""Tests for offer-based energy offer-curve factors."""

import pandas as pd
import pytest

from src.offer_curves import compute_offer_features, attach_offer_factors, attach_offer_factor_doc


def _prices():
    return pd.DataFrame({
        "DUID": ["TEST1", "TEST1", "TEST2"],
        "SETTLEMENTDATE": pd.to_datetime(
            ["2026-07-01", "2026-07-02", "2026-07-01"]
        ),
        "PRICEBAND1": [-50.0, 10.0, 5.0],
        "PRICEBAND10": [15000.0, 300.0, 500.0],
    })


def _volumes():
    return pd.DataFrame({
        "DUID": ["TEST1", "TEST1"],
        "INTERVAL_DATETIME": pd.to_datetime(
            ["2026-07-01 00:05:00", "2026-07-01 00:10:00"]
        ),
        "BANDAVAIL1": [10.0, 20.0],
        "BANDAVAIL2": [0.0, 0.0],
        "BANDAVAIL3": [0.0, 0.0],
        "BANDAVAIL4": [0.0, 0.0],
        "BANDAVAIL5": [0.0, 0.0],
        "BANDAVAIL6": [0.0, 0.0],
        "BANDAVAIL7": [0.0, 0.0],
        "BANDAVAIL8": [0.0, 0.0],
        "BANDAVAIL9": [30.0, 0.0],
        "BANDAVAIL10": [40.0, 180.0],
    })


class TestComputeOfferFeatures:
    def test_basic_features(self):
        out = compute_offer_features(_prices(), _volumes())
        t1 = out[out["duid"] == "TEST1"].iloc[0]
        assert t1["month"] == "2026-07"
        assert t1["negative_band_day_share"] == 0.5  # 1 of 2 days band1 < 0
        assert t1["offered_mw_avg"] == pytest.approx(140.0)  # 80 then 200
        assert t1["offered_mw_p95"] == pytest.approx(194.0)
        # interval 1: top2 = 70/80 = 0.875; interval 2: 180/200 = 0.9
        assert t1["top2_band_volume_share"] == pytest.approx(0.8875)
        assert t1["price_band_min_avg"] == pytest.approx(-20.0)
        assert t1["price_band_max_avg"] == pytest.approx(7650.0)

    def test_test2_volumeless_row(self):
        """TEST2 has price days but no volume intervals — row kept, volumes NaN."""
        out = compute_offer_features(_prices(), _volumes())
        t2 = out[out["duid"] == "TEST2"].iloc[0]
        assert pd.isna(t2["offered_mw_avg"])
        assert t2["negative_band_day_share"] == 0.0

    def test_empty_inputs(self):
        assert compute_offer_features(pd.DataFrame(), pd.DataFrame()).empty


class TestAttachOfferFactors:
    def test_attach_and_scope(self):
        factors = compute_offer_features(_prices(), _volumes())
        gens = [{"duid": "TEST1"}, {"duid": "OTHER"}]
        out = attach_offer_factors(gens, factors)
        assert "offers" in out[0]
        assert out[0]["offers"]["scope"] == "offer_based_estimate"
        assert out[0]["offers"]["avg_offered_mw"] == pytest.approx(140.0)
        assert "offers" not in out[1]

    def test_attach_none_is_noop(self):
        gens = [{"duid": "TEST1"}]
        assert attach_offer_factors(gens, None) == gens
        assert attach_offer_factors(gens, pd.DataFrame()) == gens

    def test_doc_attach(self):
        factors = compute_offer_features(_prices(), _volumes())
        rows = factors[factors["duid"] == "TEST1"]
        doc = {}
        attach_offer_factor_doc(doc, rows)
        assert doc["offers"]["scope"] == "offer_based_estimate"
        assert doc["offers"]["month"] == "2026-07"
        assert doc["offers"]["avg_offered_mw"] == pytest.approx(140.0)
        doc2 = {}
        attach_offer_factor_doc(doc2, None)
        assert "offers" not in doc2
