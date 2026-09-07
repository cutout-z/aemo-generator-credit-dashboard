"""S3-05 tests: interval-ending calendar days + offer/FCAS source-coverage contract.

Review acceptance (astra-review-2026-09-04-surface-3.md, S3-05):
- A complete July fixture has no fabricated 1 August day; constant full-day
  output gives CF 1.0.
- Missing intervals are explicit (observed/expected counts published).
- Zero FCAS offers survive the pre-filter as observed source coverage.
- A 48-interval August source must not become an unqualified full-August
  statistic: the attach layer promotes the latest COMPLETE comparable month
  and reports a newer partial fragment explicitly.
"""

import json

import pandas as pd
import pytest

from src.aggregate import aggregate_month_daily
from src.fcas_factor import compute_fcas_factors, attach_fcas_factor_doc
from src.offer_curves import (
    compute_offer_features,
    compute_offer_curves,
    compute_offer_curves_daily,
    attach_offer_factor_doc,
    attach_offer_curve_doc,
)
from src.generate_json import generate_all

INTERVALS_PER_DAY = 288


# ─── Fixtures ────────────────────────────────────────────────────────────────

def _interval_ends(start_ts, n):
    """n interval-END timestamps stepping 5 minutes from start_ts."""
    return [pd.Timestamp(start_ts) + pd.Timedelta(minutes=5 * i) for i in range(n)]


def _full_july_scada(duid="BW01", capacity=100.0, mw=100.0):
    """A complete July 2026 SCADA month: 288 intervals/day, 31 days.

    First interval ends 2026-07-01 00:05; the last ends 2026-08-01 00:00:00
    (the interval the NEMOSIS (start, end] window includes at its boundary —
    the one that used to fabricate a phantom 2026-08-01 "day"). Constant
    output at nameplate → CF 1.0 every day.
    """
    gens = pd.DataFrame({
        "DUID": [duid],
        "CAPACITY_MW": [capacity],
    })
    rows = []
    ts = pd.Timestamp("2026-07-01 00:05:00")
    for i in range(31 * INTERVALS_PER_DAY):
        rows.append((ts, duid, mw))
        ts += pd.Timedelta(minutes=5)
    scada = pd.DataFrame(rows, columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"])
    scada["SETTLEMENTDATE"] = pd.to_datetime(scada["SETTLEMENTDATE"])
    return scada, gens


def _bids_frame(rows):
    return pd.DataFrame(
        rows, columns=["INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL"]
    )


# ─── Half A: interval-ending calendar days for daily generation ─────────────

class TestDailyIntervalEndingDays:
    def test_no_fabricated_august_first(self):
        scada, gens = _full_july_scada()
        out = aggregate_month_daily(scada, gens, 2026, 7)
        dates = set(out["date"])
        assert "2026-08-01" not in dates
        assert min(dates) == "2026-07-01"
        assert max(dates) == "2026-07-31"
        assert len(dates) == 31
        # Every day of a constant-output month is a FULL day: 288 intervals.
        assert (out["intervals_observed"] == INTERVALS_PER_DAY).all()
        assert (out["intervals_expected"] == INTERVALS_PER_DAY).all()

    def test_constant_full_day_cf_is_1(self):
        scada, gens = _full_july_scada()
        out = aggregate_month_daily(scada, gens, 2026, 7)
        # 100 MW × 24 h = 2400 MWh per day → CF exactly 1.0
        assert (out["daily_generation_mwh"] == 2400.0).all()
        assert (out["daily_capacity_factor"] == 1.0).all()

    def test_midnight_interval_belongs_to_preceding_day(self):
        # A unit whose ONLY activity is the 23:55–24:00 interval of 31 July
        # (rows ending 31 Jul 23:55 and 1 Aug 00:00). Old code dated the
        # 00:00-ending row as 1 August → 8.3 MWh / 0.35% phantom day.
        # S3-05: both intervals are 31 July.
        cap = 100.0
        gens = pd.DataFrame({"DUID": ["MIDN1"], "CAPACITY_MW": [cap]})
        scada = pd.DataFrame({
            "SETTLEMENTDATE": pd.to_datetime(
                ["2026-07-31 23:55:00", "2026-08-01 00:00:00"]
            ),
            "DUID": ["MIDN1", "MIDN1"],
            "SCADAVALUE": [100.0, 100.0],
        })
        out = aggregate_month_daily(scada, gens, 2026, 7)
        assert "2026-08-01" not in set(out["date"])
        row = out.iloc[0]
        assert row["date"] == "2026-07-31"
        assert row["intervals_observed"] == 2
        # 2 intervals × 100 MW × 5/60 h = 16.7 MWh — NOT the full-day 0.35% lie.
        assert row["daily_generation_mwh"] == pytest.approx(16.7, abs=0.05)

    def test_duplicate_interval_keys_deduped(self):
        scada, gens = _full_july_scada()
        doubled = pd.concat([scada, scada], ignore_index=True)
        out = aggregate_month_daily(doubled, gens, 2026, 7)
        assert (out["intervals_observed"] == INTERVALS_PER_DAY).all()
        assert (out["daily_generation_mwh"] == 2400.0).all()

    def test_partial_day_is_explicit(self):
        # A fragment ending mid-month must not masquerade as full days:
        # 14 days of data → 14 daily rows, the last still 288 (full), and a
        # genuinely partial trailing day (here simulated directly) shows it.
        scada, gens = _full_july_scada()
        # Truncate to the first 14 days + 1 extra interval of day 15.
        cut = scada[scada["SETTLEMENTDATE"] <= pd.Timestamp("2026-07-14 23:55:00")]
        out = aggregate_month_daily(cut, gens, 2026, 7)
        assert "2026-07-15" not in set(out["date"])
        assert max(out["date"]) == "2026-07-14"
        # Direct partial-day check: a day with half the intervals reports it.
        half = scada[scada["SETTLEMENTDATE"] <= pd.Timestamp("2026-07-01 12:00:00")]
        out_half = aggregate_month_daily(half, gens, 2026, 7)
        day1 = out_half[out_half["date"] == "2026-07-01"].iloc[0]
        assert 0 < day1["intervals_observed"] < INTERVALS_PER_DAY
        assert day1["intervals_expected"] == INTERVALS_PER_DAY


class TestDailyCoveragePublishing:
    def _gens(self):
        return pd.DataFrame({
            "DUID": ["GEN_A"],
            "STATION_NAME": ["Single Gen"],
            "REGION": ["QLD1"],
            "FUEL_CATEGORY": ["Solar"],
            "CAPACITY_MW": [100.0],
            "TECHNOLOGY": ["Solar"],
            "CONNECTION_POINT": ["CP"],
            "MARKET": ["NEM"],
        })

    def test_counts_published_when_present(self, tmp_path):
        daily = pd.DataFrame({
            "duid": ["GEN_A", "GEN_A"],
            "date": ["2026-07-30", "2026-07-31"],
            "daily_generation_mwh": [2400.0, 1200.0],
            "daily_capacity_factor": [1.0, 0.5],
            "intervals_observed": [288, 144],
            "intervals_expected": [288, 288],
        })
        generate_all(self._gens(), daily_aggregates=daily, output_dir=str(tmp_path / "generators"))
        doc = json.loads((tmp_path / "generators" / "GEN_A.json").read_text())
        assert doc["daily"]["intervals_observed"] == [288, 144]
        assert doc["daily"]["intervals_expected"] == [288, 288]

    def test_counts_omitted_for_legacy_rows(self, tmp_path):
        # Pre-S3-05 daily rows carry no interval counts → keys absent
        # (coverage unknown ≠ all complete).
        daily = pd.DataFrame({
            "duid": ["GEN_A", "GEN_A"],
            "date": ["2026-07-30", "2026-07-31"],
            "daily_generation_mwh": [2400.0, 2400.0],
            "daily_capacity_factor": [1.0, 1.0],
        })
        generate_all(self._gens(), daily_aggregates=daily, output_dir=str(tmp_path / "generators"))
        doc = json.loads((tmp_path / "generators" / "GEN_A.json").read_text())
        assert "intervals_observed" not in doc["daily"]
        assert "intervals_expected" not in doc["daily"]

    def test_mixed_legacy_counts_nullable(self, tmp_path):
        # New rows (counts) + legacy rows (NaN) → arrays with nulls.
        daily = pd.DataFrame({
            "duid": ["GEN_A", "GEN_A"],
            "date": ["2026-07-30", "2026-07-31"],
            "daily_generation_mwh": [2400.0, 2400.0],
            "daily_capacity_factor": [1.0, 1.0],
            "intervals_observed": [None, 288],
            "intervals_expected": [None, 288],
        })
        generate_all(self._gens(), daily_aggregates=daily, output_dir=str(tmp_path / "generators"))
        doc = json.loads((tmp_path / "generators" / "GEN_A.json").read_text())
        assert doc["daily"]["intervals_observed"] == [None, 288]


# ─── Half B: offer/FCAS source-coverage contract ────────────────────────────

def _july_complete_bids():
    """July 2026 frame reaching the final calendar day (31st)."""
    t0 = pd.Timestamp("2026-07-01 00:05:00")
    rows = []
    # ER01: 100 positive RAISEREG intervals on 1 July.
    for i in range(100):
        rows.append((t0 + pd.Timedelta(minutes=5 * i), "ER01", "RAISEREG", 30.0))
    # ER01: 50 explicit ZERO RAISEREG offers on 2 July (observed, not positive).
    z0 = pd.Timestamp("2026-07-02 00:05:00")
    for i in range(50):
        rows.append((z0 + pd.Timedelta(minutes=5 * i), "ER01", "RAISEREG", 0.0))
    # CONT bids on the month's final day so the frame is source-complete.
    last = pd.Timestamp("2026-07-31 00:05:00")
    for i in range(INTERVALS_PER_DAY):
        rows.append((last + pd.Timedelta(minutes=5 * i), "CONT", "RAISEREG", 10.0))
    return _bids_frame(rows)


class TestFcasZeroOffersRetained:
    def test_observed_vs_positive_counts(self):
        out = compute_fcas_factors(_july_complete_bids(), 2026, 7)
        er01 = out[out["duid"] == "ER01"].iloc[0]
        assert er01["fcas_observed_intervals"] == 150   # 100 positive + 50 zero
        assert er01["fcas_positive_intervals"] == 100
        assert er01["fcas_offer_minutes"] == 500        # positive only
        assert er01["fcas_avg_max_avail_mw"] == 30.0    # positive-only mean
        # Zero offers must NOT dilute the "avg offered capacity" figure.
        assert er01["fcas_avg_RAISEREG_mw"] == 30.0

    def test_zero_only_unit_is_observed_not_absent(self):
        # A unit offering ONLY zeros all month is source coverage with zero
        # participation — it must appear, not vanish pre-aggregation.
        t0 = pd.Timestamp("2026-07-01 00:05:00")
        rows = [(t0 + pd.Timedelta(minutes=5 * i), "ZERO1", "RAISEREG", 0.0)
                for i in range(50)]
        last = pd.Timestamp("2026-07-31 00:05:00")
        rows += [(last + pd.Timedelta(minutes=5 * i), "CONT", "RAISEREG", 10.0)
                 for i in range(INTERVALS_PER_DAY)]
        out = compute_fcas_factors(_bids_frame(rows), 2026, 7)
        z = out[out["duid"] == "ZERO1"]
        assert len(z) == 1
        zrow = z.iloc[0]
        assert zrow["fcas_observed_intervals"] == 50
        assert zrow["fcas_positive_intervals"] == 0
        assert zrow["fcas_offer_minutes"] == 0
        assert zrow["fcas_participation_pct"] == 0.0
        assert pd.isna(zrow["fcas_avg_max_avail_mw"])


class TestFcasSourceCompleteness:
    def test_complete_month_flagged(self):
        out = compute_fcas_factors(_july_complete_bids(), 2026, 7)
        assert out["fcas_source_complete"].all()
        assert out["fcas_observed_through"].isna().all()

    def test_fragment_flagged_partial(self):
        # Data only through 14 July → source incomplete, cutoff recorded.
        rows = [
            (pd.Timestamp("2026-07-01 00:05:00") + pd.Timedelta(minutes=5 * i),
             "ER01", "RAISEREG", 30.0)
            for i in range(48)
        ]
        out = compute_fcas_factors(_bids_frame(rows), 2026, 7)
        er01 = out.iloc[0]
        assert er01["fcas_source_complete"] == False
        assert er01["fcas_observed_through"] == "2026-07-01"


class TestFcasAttachContract:
    def test_latest_complete_month_wins_over_newer_fragment(self):
        # ER01-style reproduction: July complete + an August fragment of 48
        # observed intervals. The doc must headline July (complete comparable
        # month) and record August as an explicit later partial window —
        # never 48/8928 intervals as an unqualified "full August" figure.
        jul = compute_fcas_factors(_july_complete_bids(), 2026, 7)
        aug_rows = [
            (pd.Timestamp("2026-08-01 00:05:00") + pd.Timedelta(minutes=5 * i),
             "ER01", "RAISEREG", 30.0)
            for i in range(48)
        ]
        aug = compute_fcas_factors(_bids_frame(aug_rows), 2026, 8)
        rows = pd.concat([jul, aug], ignore_index=True)
        er01 = rows[rows["duid"] == "ER01"]
        doc = {}
        attach_fcas_factor_doc(doc, er01)
        part = doc["fcas_participation"]
        assert part["month"] == "2026-07"
        assert part["source_complete"] == True
        assert part["observed_intervals"] == 150
        lp = part["later_partial_window"]
        assert lp["month"] == "2026-08"
        assert lp["observed_intervals"] == 48
        assert lp["observed_through"] == "2026-08-01"

    def test_only_partial_rows_are_explicit_partial(self):
        aug_rows = [
            (pd.Timestamp("2026-08-01 00:05:00") + pd.Timedelta(minutes=5 * i),
             "ER01", "RAISEREG", 30.0)
            for i in range(48)
        ]
        aug = compute_fcas_factors(_bids_frame(aug_rows), 2026, 8)
        doc = {}
        attach_fcas_factor_doc(doc, aug)
        part = doc["fcas_participation"]
        assert part["month"] == "2026-08"
        assert part["window"] == "partial"
        assert part["observed_through"] == "2026-08-01"
        assert part["source_complete"] == False

    def test_legacy_rows_keep_historical_behavior(self):
        # Rows without coverage columns (pre-S3-05 cache) → latest month,
        # coverage unknown, no window claim.
        jul = compute_fcas_factors(_july_complete_bids(), 2026, 7)
        jul_er01 = jul[jul["duid"] == "ER01"]
        legacy = jul_er01[["duid", "month", "fcas_services_offered", "fcas_offer_minutes",
                           "fcas_participation_pct", "fcas_avg_max_avail_mw",
                           "fcas_max_max_avail_mw"]]
        doc = {}
        attach_fcas_factor_doc(doc, legacy)
        part = doc["fcas_participation"]
        assert part["month"] == "2026-07"
        assert part["offer_minutes"] == 500
        assert "window" not in part
        assert "source_complete" not in part


def _offer_frames(complete=True, boundary_midnight=False):
    """Minimal price + volume frames for offer-side coverage tests.

    Prices: whole trading days (BIDDAYOFFER semantics).
    Volumes: END-stamped 5-min intervals (BIDPEROFFER_D semantics).
    complete=True adds volume rows on the final day of the month.
    """
    prices = pd.DataFrame({
        "DUID": ["T1", "T1", "T1"],
        "SETTLEMENTDATE": pd.to_datetime(
            ["2026-07-01", "2026-07-02", "2026-07-03"]
        ),
        **{f"PRICEBAND{i}": [10.0 * i, 20.0 * i, 30.0 * i] for i in range(1, 11)},
    })
    vrows = []
    # Day 1 volumes: 3 intervals ending 00:05/00:10/00:15.
    for i in range(3):
        vrows.append(("2026-07-01 00:%02d:00" % (5 + 5 * i), 10.0))
    if boundary_midnight:
        # The interval ending 2026-08-01 00:00 is 31 July's final 5 minutes.
        vrows.append(("2026-08-01 00:00:00", 10.0))
    if complete:
        # Some unit activity on the final day of July → frame source-complete.
        for i in range(3):
            vrows.append(("2026-07-31 23:%02d:00" % (45 + 5 * i), 10.0))
    vols = pd.DataFrame(vrows, columns=["INTERVAL_DATETIME", "mw"])
    for i in range(1, 11):
        vols[f"BANDAVAIL{i}"] = vols["mw"] if i == 1 else 0.0
    vols["DUID"] = "T1"
    vols["INTERVAL_DATETIME"] = pd.to_datetime(vols["INTERVAL_DATETIME"])
    return prices, vols[["DUID", "INTERVAL_DATETIME"] + [f"BANDAVAIL{i}" for i in range(1, 11)]]


class TestOfferVolumeDayAlignment:
    def test_boundary_midnight_volume_stays_in_prior_month(self):
        # A volume interval ending 2026-08-01 00:00 belongs to July (the
        # 23:55–24:00 slice of 31 July), so it must not create an August
        # offer-factor month or a phantom 2026-08-01 daily curve date.
        prices, vols = _offer_frames(boundary_midnight=True, complete=False)
        # Give 31 July a price day so the boundary volume can pair with it.
        jul31 = pd.DataFrame({
            "DUID": ["T1"],
            "SETTLEMENTDATE": pd.to_datetime(["2026-07-31"]),
            **{f"PRICEBAND{i}": [30.0 * i] for i in range(1, 11)},
        })
        prices = pd.concat([prices, jul31], ignore_index=True)
        feats = compute_offer_features(prices, vols)
        assert set(feats["month"]) == {"2026-07"}

        daily = compute_offer_curves_daily(prices, vols, "2026-07")
        assert set(daily["date"]) == {"2026-07-01", "2026-07-31"}
        assert "2026-08-01" not in set(daily["date"])

    def test_volume_month_alignment_and_completeness_flag(self):
        prices, vols = _offer_frames(complete=True)
        feats = compute_offer_features(prices, vols)
        t1 = feats[feats["duid"] == "T1"].iloc[0]
        assert t1["month"] == "2026-07"
        assert t1["vol_source_complete"] == True
        assert t1["vol_intervals_observed"] == 6  # 3 day-1 + 3 on the 31st

        prices_frag, vols_frag = _offer_frames(complete=False)
        feats_frag = compute_offer_features(prices_frag, vols_frag)
        t1f = feats_frag[feats_frag["duid"] == "T1"].iloc[0]
        assert t1f["vol_source_complete"] == False

    def test_monthly_curve_excludes_next_month_trading_day_price(self):
        # BIDDAYOFFER price row timestamped 2026-08-01 (a whole August
        # trading day) rides in July's inclusive-end fetch window — the July
        # curve mean must NOT include it.
        prices, vols = _offer_frames(complete=False)
        prices = pd.concat([
            prices,
            pd.DataFrame({
                "DUID": ["T1"],
                "SETTLEMENTDATE": pd.to_datetime(["2026-08-01"]),
                **{f"PRICEBAND{i}": [1000.0 * i] for i in range(1, 11)},
            }),
        ], ignore_index=True)
        curves = compute_offer_curves(prices, vols, "2026-07")
        b1 = curves[curves["duid"] == "T1"].sort_values("band").iloc[0]
        # Mean of 10/20/30 = 20, NOT diluted by the 1000 August day.
        assert b1["price"] == pytest.approx(20.0)
        assert b1["source_complete"] == False

    def test_offer_factor_doc_complete_month_contract(self):
        # July complete + newer August fragment → headline July, fragment noted.
        prices, vols = _offer_frames(complete=True)
        jul = compute_offer_features(prices, vols)
        aug_prices = pd.DataFrame({
            "DUID": ["T1"],
            "SETTLEMENTDATE": pd.to_datetime(["2026-08-01"]),
            **{f"PRICEBAND{i}": [5.0 * i] for i in range(1, 11)},
        })
        aug_vols = pd.DataFrame({
            "DUID": ["T1", "T1"],
            "INTERVAL_DATETIME": pd.to_datetime(["2026-08-01 00:05:00", "2026-08-01 00:10:00"]),
            **{f"BANDAVAIL{i}": [10.0, 20.0] if i == 1 else [0.0, 0.0] for i in range(1, 11)},
        })
        aug = compute_offer_features(aug_prices, aug_vols)
        rows = pd.concat([jul, aug], ignore_index=True)
        doc = {}
        attach_offer_factor_doc(doc, rows[rows["duid"] == "T1"])
        offers = doc["offers"]
        assert offers["month"] == "2026-07"
        assert offers["source_complete"] == True
        assert offers["later_partial_window"]["month"] == "2026-08"

    def test_offer_curve_doc_partial_label(self):
        prices, vols = _offer_frames(complete=False)
        curves = compute_offer_curves(prices, vols, "2026-07")
        doc = {}
        attach_offer_curve_doc(doc, curves)
        oc = doc["offer_curve"]
        assert oc["month"] == "2026-07"
        assert oc["window"] == "partial"
        assert len(oc["bands"]) == 10
