"""Tests for credit-risk factors, per-DUID FCAS factors, and freshness guards.

Covers the Apr-2026 audit remediations:
- WANDSF1/EMERASF1 "FCAS duplication" — regional price context must be labelled
  as regional, and per-DUID FCAS participation must actually differ between units.
- Freshness: missing latest month / stale daily data must fail loudly.
"""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from src.market_factors import (
    compute_daily_spreads,
    quarter_label,
    build_quarterly_summary,
)
from src.fcas_factor import compute_fcas_factors, attach_fcas_factor_doc
from src import freshness


# ─── Daily spread computation ────────────────────────────────────────────────


def _price_frame(rrp_values, region="NSW1", date="2026-07-15"):
    ts = pd.Timestamp(date)
    return pd.DataFrame({
        "SETTLEMENTDATE": [ts + pd.Timedelta(minutes=5 * i) for i in range(len(rrp_values))],
        "REGIONID": region,
        "RRP": np.asarray(rrp_values, dtype=float),
    })


class TestComputeDailySpreads:
    def test_known_values(self):
        # 100 intervals: 90 @ $50, 10 @ $500 → top decile mean 500, bottom 50
        prices = _price_frame([50.0] * 90 + [500.0] * 10)
        out = compute_daily_spreads(prices)
        assert len(out) == 1
        row = out.iloc[0]
        assert row["region"] == "NSW1"
        assert row["date"] == "2026-07-15"
        assert row["vwap_high"] == 500.0
        assert row["vwap_low"] == 50.0
        assert row["spread_decile"] == 450.0
        assert row["spread_max"] == 450.0
        assert row["neg_price_share"] == 0.0
        assert row["intervals"] == 100

    def test_negative_price_share(self):
        prices = _price_frame([-10.0] * 5 + [50.0] * 5)
        out = compute_daily_spreads(prices)
        row = out.iloc[0]
        assert row["neg_price_share"] == 0.5
        assert row["vwap_low"] == -10.0
        assert row["spread_decile"] == 60.0

    def test_regions_and_dates_split(self):
        frames = []
        for region in ("NSW1", "SA1"):
            for date in ("2026-07-15", "2026-07-16"):
                frames.append(_price_frame([20.0, 80.0], region=region, date=date))
        out = compute_daily_spreads(pd.concat(frames, ignore_index=True))
        assert len(out) == 4
        assert set(out["region"]) == {"NSW1", "SA1"}

    def test_empty_input(self):
        assert compute_daily_spreads(pd.DataFrame()).empty


class TestQuarterlyRollup:
    def test_quarter_label(self):
        assert quarter_label("2026-07-15") == "2026Q3"
        assert quarter_label("2025-04-01") == "2025Q2"
        assert quarter_label("2025-12-31") == "2025Q4"

    def test_summary_one_row_per_region_quarter(self):
        daily = pd.DataFrame([
            {"date": "2026-04-10", "region": "NSW1", "spread_decile": 100.0,
             "spread_max": 9000.0, "vwap_high": 200.0, "vwap_low": 100.0,
             "neg_price_share": 0.1, "price_std": 40.0},
            {"date": "2026-04-11", "region": "NSW1", "spread_decile": 200.0,
             "spread_max": 9500.0, "vwap_high": 300.0, "vwap_low": 100.0,
             "neg_price_share": 0.3, "price_std": 60.0},
            {"date": "2026-07-10", "region": "NSW1", "spread_decile": 50.0,
             "spread_max": 400.0, "vwap_high": 90.0, "vwap_low": 40.0,
             "neg_price_share": 0.2, "price_std": 25.0},
        ])
        q = build_quarterly_summary(daily)
        assert len(q) == 2
        nsw_q2 = q[(q["region"] == "NSW1") & (q["quarter"] == "2026Q2")].iloc[0]
        assert nsw_q2["avg_spread_decile"] == 150.0
        assert nsw_q2["days_covered"] == 2


# ─── Per-DUID FCAS participation ─────────────────────────────────────────────


def _bids_frame(rows):
    return pd.DataFrame(
        rows,
        columns=["INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL"],
    )


class TestComputeFcasFactors:
    def test_units_differ(self):
        # WANDSF1 offers Raise Reg in 2 intervals; EMERASF1 offers nothing —
        # their factors must differ (the Apr-2026 audit flagged identical
        # regional numbers as indistinguishable per-unit data).
        t0 = pd.Timestamp("2026-07-01 00:05:00")
        bids = _bids_frame([
            (t0, "WANDSF1", "RAISEREG", 30.0),
            (t0 + pd.Timedelta(minutes=5), "WANDSF1", "RAISEREG", 40.0),
        ])
        out = compute_fcas_factors(bids, 2026, 7)
        assert len(out) == 1
        row = out.iloc[0]
        assert row["duid"] == "WANDSF1"
        assert row["fcas_services_offered"] == 1
        assert row["fcas_offer_minutes"] == 10
        expected_part = 2 / (31 * 24 * 12)
        assert abs(row["fcas_participation_pct"] - expected_part) < 1e-6
        assert row["fcas_avg_max_avail_mw"] == 35.0
        assert row["fcas_max_max_avail_mw"] == 40.0
        # EMERASF1 must be absent (no offers) — not given WANDSF1's numbers
        assert "EMERASF1" not in out["duid"].values

    def test_per_service_averages(self):
        t0 = pd.Timestamp("2026-07-01 00:05:00")
        bids = _bids_frame([
            (t0, "HPRB1", "RAISEREG", 100.0),
            (t0, "HPRB1", "LOWERREG", 120.0),
            (t0, "HPRB1", "LOWERREG", 140.0),
        ])
        out = compute_fcas_factors(bids, 2026, 7)
        row = out.iloc[0]
        assert row["fcas_services_offered"] == 2
        assert row["fcas_avg_RAISEREG_mw"] == 100.0
        assert row["fcas_avg_LOWERREG_mw"] == 130.0

    def test_empty_bids(self):
        assert compute_fcas_factors(pd.DataFrame(), 2026, 7).empty


class TestRegionalFcasSemanticsRegression:
    """The Apr-2026 P1: WANDSF1/EMERASF1 carried identical FCAS blocks."""

    def test_participation_docs_differ_between_units(self):
        t0 = pd.Timestamp("2026-07-01 00:05:00")
        bids = _bids_frame([
            (t0, "WANDSF1", "RAISEREG", 30.0),
            (t0, "EMERASF1", "LOWER6SEC", 10.0),
        ])
        factors = compute_fcas_factors(bids, 2026, 7)

        doc_w = {}
        attach_fcas_factor_doc(doc_w, factors[factors["duid"] == "WANDSF1"])
        doc_e = {}
        attach_fcas_factor_doc(doc_e, factors[factors["duid"] == "EMERASF1"])

        assert doc_w["fcas_participation"]["avg_max_avail_mw"] == 30.0
        assert doc_e["fcas_participation"]["avg_max_avail_mw"] == 10.0
        assert doc_w["fcas_participation"] != doc_e["fcas_participation"]
        # The regional context must be explicitly labelled as regional
        assert "REGIONAL" in doc_w["fcas_participation"]["note"]

    def test_attach_noop_on_empty(self):
        doc = {}
        attach_fcas_factor_doc(doc, pd.DataFrame())
        assert "fcas_participation" not in doc


# ─── Freshness guards ────────────────────────────────────────────────────────


class TestFreshness:
    def test_recent_month_passes(self):
        agg = pd.DataFrame({"duid": ["X"], "month": ["2026-07"]})
        freshness.check_monthly_freshness(agg, now=datetime(2026, 9, 1))

    def test_current_pointer_month_passes(self):
        # Aug 2026 is the pipeline's pointer month on 1 Sep but not yet
        # published; if it IS present, that must also pass.
        agg = pd.DataFrame({"duid": ["X"], "month": ["2026-08"]})
        freshness.check_monthly_freshness(agg, now=datetime(2026, 9, 1))

    def test_stale_month_raises(self):
        agg = pd.DataFrame({"duid": ["X"], "month": ["2025-06"]})
        with pytest.raises(RuntimeError, match="Freshness guard FAILED"):
            freshness.check_monthly_freshness(agg, now=datetime(2026, 9, 1))

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            freshness.check_monthly_freshness(pd.DataFrame(), now=datetime(2026, 9, 1))

    def test_missing_daily_warns_only(self):
        freshness.check_daily_freshness(pd.DataFrame(), now=datetime(2026, 9, 1))

    def test_stale_daily_raises(self):
        daily = pd.DataFrame({"duid": ["X"], "date": ["2026-01-15"]})
        with pytest.raises(RuntimeError, match="Freshness guard FAILED"):
            freshness.check_daily_freshness(daily, now=datetime(2026, 9, 1))

    def test_recent_daily_passes(self):
        daily = pd.DataFrame({"duid": ["X"], "date": ["2026-07-31"]})
        freshness.check_daily_freshness(daily, now=datetime(2026, 9, 1))

    def test_mac_side_alerts_on_stale(self, tmp_path):
        stale = pd.DataFrame({"duid": ["X"], "month": ["2025-06"]})
        stale.to_feather(tmp_path / "monthly_aggregates.feather")
        alerts = freshness.mac_side_staleness_check(str(tmp_path), now=datetime(2026, 9, 1))
        assert alerts, "stale published data must produce an alert"
        assert "AEMO ALERT" in alerts[0]

    def test_mac_side_silent_when_fresh(self, tmp_path):
        pd.DataFrame({"duid": ["X"], "month": ["2026-07"]}).to_feather(
            tmp_path / "monthly_aggregates.feather")
        pd.DataFrame({"duid": ["X"], "date": ["2026-07-31"]}).to_feather(
            tmp_path / "daily_aggregates.feather")
        alerts = freshness.mac_side_staleness_check(str(tmp_path), now=datetime(2026, 9, 1))
        assert alerts == []

    def test_mac_side_alerts_on_missing_daily(self, tmp_path):
        pd.DataFrame({"duid": ["X"], "month": ["2026-07"]}).to_feather(
            tmp_path / "monthly_aggregates.feather")
        alerts = freshness.mac_side_staleness_check(str(tmp_path), now=datetime(2026, 9, 1))
        assert len(alerts) == 1 and "daily" in alerts[0]
