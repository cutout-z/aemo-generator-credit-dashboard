"""Mid-year MLF revisions apply from their effective date (audit 2026-10, H5).

build_mlf_lookup picks ONE factor per DUID per FY from the MLF Tracker, so a
revision AEMO makes during the year was ignored: QPSFB1 1.019 -> 0.9176 from
2026-02-03 left Feb-Jun 2026 revenue 11.1% high (7 units in FY25-26, incl.
LIMOSF11 and NUMURSF1). Revenue now applies the DUDETAILSUMMARY factor in
effect on each interval's day. The dated values are used only where they
agree with the tracker's opening factor for the FY. These tests pin the
parser, the interval-to-period rule, the revenue/provenance arithmetic and
the fetch/cache fallbacks.
"""

import io
import logging
import zipfile
from datetime import datetime

import pandas as pd
import pytest

from src import loss_factors as lf
from src.aggregate import (
    MLF_STATUS_EXACT,
    MLF_STATUS_UNKNOWN,
    REVENUE_MLF_SOURCE_COL,
    REVENUE_MLF_SOURCE_FY_COL,
    REVENUE_MLF_STATUS_COL,
    REVENUE_MLF_VALUE_COL,
    aggregate_month,
    build_mlf_lookup,
)
from src.loss_factors import (
    fy_opening_tlf,
    interval_loss_factors,
    parse_dudetailsummary,
)

HEADER = (
    "I,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,DUID,START_DATE,END_DATE,"
    "DISPATCHTYPE,CONNECTIONPOINTID,REGIONID,STATIONID,PARTICIPANTID,LASTCHANGED,"
    "TRANSMISSIONLOSSFACTOR,STARTTYPE,DISTRIBUTIONLOSSFACTOR,SECONDARY_TLF"
)


def _d(duid, start, end, tlf, dlf="1", dtype="GENERATOR", changed="2026/09/24 11:50:44",
       secondary=""):
    return (f"D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,{duid},{start} 00:00:00,"
            f"{end} 00:00:00,{dtype},CP1,QLD1,ST1,P1,{changed},{tlf},SLOW,{dlf},{secondary}")


def _archive(lines):
    return "\n".join(
        ["C,SETP.WORLD,DVD_DUDETAILSUMMARY,AEMO,PUBLIC,2026/09/25,13:40:25,1,MONTHLY_ARCHIVE,1",
         HEADER, *lines, "C,END OF REPORT,5"]
    )


QPSFB1_ROWS = [
    _d("QPSFB1", "2026/01/20", "2026/02/03", "1.019", "0.9993", secondary="0.9176"),
    _d("QPSFB1", "2026/02/03", "2026/07/01", "0.9176", "0.9993"),
    _d("QPSFB1", "2026/07/01", "2999/12/31", "0.9032", "0.9995"),
]


def _periods(lines=QPSFB1_ROWS):
    return parse_dudetailsummary(_archive(lines))


class TestParse:
    def test_columns_open_end_and_bidirectional_kept(self):
        p = parse_dudetailsummary(_archive(QPSFB1_ROWS + [
            _d("RESS1", "2025/07/01", "2026/07/01", "0.9396", dtype="BIDIRECTIONAL",
               secondary="0.8781"),
        ]))
        assert list(p.columns) == list(lf.LOSS_FACTOR_COLUMNS)
        assert p["END_DATE"].max() == lf.OPEN_END
        # Every dispatch type is kept (the old GENERATOR-only filter was H3).
        ress = p[p["DUID"] == "RESS1"].iloc[0]
        assert ress["DISPATCHTYPE"] == "BIDIRECTIONAL"
        assert ress["TLF"] == 0.9396  # generation side, not SECONDARY_TLF

    def test_restated_period_keeps_latest_lastchanged(self):
        p = parse_dudetailsummary(_archive([
            _d("A1", "2025/07/01", "2026/07/01", "0.95", changed="2026/01/01 00:00:00"),
            _d("A1", "2025/07/01", "2026/07/01", "0.97", changed="2026/03/01 00:00:00"),
        ]))
        assert p["TLF"].tolist() == [0.97]

    def test_implausible_factor_is_nan_not_used(self):
        p = parse_dudetailsummary(_archive([_d("A1", "2025/07/01", "2026/07/01", "-999")]))
        assert p["TLF"].isna().all()

    def test_zip_bytes_accepted(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("PUBLIC_ARCHIVE#DUDETAILSUMMARY#FILE01#202608010000.CSV",
                        _archive(QPSFB1_ROWS))
        assert len(parse_dudetailsummary(buf.getvalue())) == 3

    def test_no_rows_raises(self):
        with pytest.raises(ValueError):
            parse_dudetailsummary("C,nothing\n")


class TestIntervalMapping:
    def test_effective_date_boundary_uses_interval_ending_day(self):
        iv = pd.DataFrame({
            "SETTLEMENTDATE": pd.to_datetime([
                "2026-02-03 00:00", "2026-02-03 00:05", "2026-07-01 00:05", "2026-01-19 12:00",
            ]),
            "DUID": "QPSFB1",
        })
        out = interval_loss_factors(iv, _periods())
        # 00:00 on 3 Feb ends 2 Feb's last interval -> the old factor still applies.
        assert out["TLF"].tolist()[:3] == [1.019, 0.9176, 0.9032]
        assert pd.isna(out["TLF"].iloc[3])  # before registration: no dated factor
        assert out["DLF"].tolist()[:3] == [0.9993, 0.9993, 0.9995]

    def test_index_alignment_and_unknown_duid(self):
        iv = pd.DataFrame({
            "SETTLEMENTDATE": pd.to_datetime(["2026-03-01 10:00", "2026-03-01 10:00"]),
            "DUID": ["NOPE1", "QPSFB1"],
        }, index=[7, 3])
        out = interval_loss_factors(iv, _periods())
        assert list(out.index) == [7, 3]
        assert pd.isna(out.loc[7, "TLF"]) and out.loc[3, "TLF"] == 0.9176

    def test_fy_opening_factor(self):
        assert fy_opening_tlf(_periods(), 2025) == {"QPSFB1": 1.019}
        assert fy_opening_tlf(_periods(), 2026) == {"QPSFB1": 0.9032}
        assert fy_opening_tlf(None, 2025) == {}


def _history(rows):
    return pd.DataFrame({
        "DUID": [r[0] for r in rows],
        "fy_label": [f"FY{r[1] % 100:02d}-{(r[1] + 1) % 100:02d}" for r in rows],
        "fy_start_year": [r[1] for r in rows],
        "mlf": [r[2] for r in rows],
    })


def _month(duid, stamps, year, month, history, periods, mw=60.0, rrp=100.0):
    ts = pd.to_datetime(stamps)
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": duid, "SCADAVALUE": mw})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "QLD1", "RRP": rrp})
    gens = pd.DataFrame({"DUID": [duid], "REGION": ["QLD1"], "CAPACITY_MW": [100.0],
                         "FUEL_CATEGORY": ["Solar"]})
    fy = year if month >= 7 else year - 1
    return aggregate_month(scada, prices, None, gens, build_mlf_lookup(history, fy),
                           year, month, loss_factor_periods=periods).iloc[0]


QPSFB1_HISTORY = _history([("QPSFB1", 2025, 1.019), ("QPSFB1", 2026, 0.9032)])


class TestRevenueByEffectiveDate:
    def test_month_after_the_revision_uses_the_new_factor(self):
        row = _month("QPSFB1", ["2026-03-10 12:00"] * 1, 2026, 3, QPSFB1_HISTORY, _periods())
        # 60 MW / 12 × $100 = $500 × 0.9176 = $458.8 (was × 1.019 = $509.5)
        assert row["revenue_aud"] == 459
        assert row[REVENUE_MLF_VALUE_COL] == 0.9176
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_EXACT
        assert row[REVENUE_MLF_SOURCE_FY_COL] == 2025
        assert row[REVENUE_MLF_SOURCE_COL] == "dudetailsummary"

    def test_month_straddling_the_revision_splits_by_day(self):
        stamps = ["2026-02-01 12:00", "2026-02-02 12:00", "2026-02-10 12:00", "2026-02-11 12:00"]
        row = _month("QPSFB1", stamps, 2026, 2, QPSFB1_HISTORY, _periods())
        # 2 intervals at 1.019 + 2 at 0.9176, $500 each.
        assert row["revenue_aud"] == round(1000 * 1.019 + 1000 * 0.9176)
        assert row[REVENUE_MLF_VALUE_COL] == pytest.approx((1.019 + 0.9176) / 2)
        implied = row["revenue_aud"] / (row["generation_mwh"] * row["captured_price"])
        assert implied == pytest.approx((1.019 + 0.9176) / 2, abs=1e-3)

    def test_without_periods_the_fy_factor_is_unchanged(self):
        row = _month("QPSFB1", ["2026-03-10 12:00"], 2026, 3, QPSFB1_HISTORY, None)
        assert row["revenue_aud"] == round(500 * 1.019)  # the FY factor
        assert row[REVENUE_MLF_SOURCE_COL] == "mlf-tracker"

    def test_opening_disagreement_keeps_the_tracker_factor(self, caplog):
        """Tracker and DUDETAILSUMMARY disagree on the FY's opening factor
        (FY26-27 bidirectional units: generation and load swapped) -> the
        tracker value stays, the row says so, and the run logs it."""
        periods = _periods([_d("RESS1", "2026/07/01", "2999/12/31", "0.9008",
                               dtype="BIDIRECTIONAL", secondary="0.9862")])
        hist = _history([("RESS1", 2026, 0.9862)])
        with caplog.at_level(logging.WARNING):
            row = _month("RESS1", ["2026-08-10 12:00"], 2026, 8, hist, periods)
        assert row[REVENUE_MLF_VALUE_COL] == 0.9862
        assert row[REVENUE_MLF_SOURCE_COL] == "mlf-tracker"
        assert "RESS1" in caplog.text

    def test_no_tracker_factor_uses_the_dated_factor(self):
        row = _month("QPSFB1", ["2026-03-10 12:00"], 2026, 3, _history([("X", 2025, 0.9)]),
                     _periods())
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_EXACT
        assert row[REVENUE_MLF_VALUE_COL] == 0.9176

    def test_no_factor_anywhere_stays_unknown(self):
        row = _month("GHOST1", ["2026-03-10 12:00"], 2026, 3, QPSFB1_HISTORY, _periods())
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_UNKNOWN
        assert row[REVENUE_MLF_VALUE_COL] is None
        assert row[REVENUE_MLF_SOURCE_COL] is None
        assert row["revenue_aud"] == 500

    def test_partial_coverage_falls_back_per_interval(self):
        """Registered on 20 Jan: January intervals before then carry the
        tracker factor, later ones the dated factor (here equal)."""
        stamps = ["2026-01-10 12:00", "2026-01-25 12:00"]
        row = _month("QPSFB1", stamps, 2026, 1, QPSFB1_HISTORY, _periods())
        assert row[REVENUE_MLF_SOURCE_COL] == "mixed"
        assert row[REVENUE_MLF_VALUE_COL] == 1.019
        assert row["revenue_aud"] == round(1000 * 1.019)


class FakeResponse:
    def __init__(self, status, content=b""):
        self.status_code = status
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class TestFetch:
    def _serve(self, monkeypatch, available):
        calls = []

        def fake_get(url, **kw):
            calls.append(url)
            for label in available:
                if label in url:
                    return FakeResponse(200, _archive(QPSFB1_ROWS).encode())
            return FakeResponse(404)

        monkeypatch.setattr(lf.requests, "get", fake_get)
        return calls

    def test_newest_published_archive_is_used_and_cached(self, tmp_path, monkeypatch):
        calls = self._serve(monkeypatch, ["202608010000"])
        p = lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5))
        assert len(p) == 3 and len(calls) == 2  # Sep 404, Aug ok
        assert (tmp_path / lf.LOSS_FACTOR_CACHE).exists()
        # Next day: cache is recent -> one probe for the newer month only.
        calls.clear()
        p2 = lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 6))
        assert len(p2) == 3 and len(calls) == 1 and "202609" in calls[0]

    def test_cache_used_when_download_fails(self, tmp_path, monkeypatch):
        self._serve(monkeypatch, ["202608010000"])
        lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5))
        self._serve(monkeypatch, [])
        p = lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2027, 3, 1))
        assert len(p) == 3

    def test_nothing_available_returns_none(self, tmp_path, monkeypatch):
        self._serve(monkeypatch, [])
        assert lf.fetch_loss_factor_periods(str(tmp_path), today=datetime(2026, 10, 5)) is None


def test_unit_json_publishes_the_factor_source(tmp_path):
    import json

    from src.generate_json import generate_generator_json

    stamps = ["2026-02-02 12:00", "2026-02-10 12:00"]
    feb = _month("QPSFB1", stamps, 2026, 2, QPSFB1_HISTORY, _periods())
    mar = _month("QPSFB1", ["2026-03-10 12:00"], 2026, 3, QPSFB1_HISTORY, None)
    cols = ["month", "generation_mwh", "revenue_aud", "capacity_factor",
            REVENUE_MLF_STATUS_COL, REVENUE_MLF_SOURCE_FY_COL, REVENUE_MLF_VALUE_COL,
            REVENUE_MLF_SOURCE_COL]
    monthly = pd.DataFrame([feb[cols], mar[cols]]).astype(
        {"generation_mwh": float, "revenue_aud": float, "capacity_factor": float}
    )
    path = generate_generator_json(
        "QPSFB1",
        {"station_name": "Q", "region": "QLD1", "fuel_category": "Solar",
         "capacity_mw": 100.0, "technology": "", "connection_point": "CP1", "market": "NEM"},
        monthly_data=monthly, output_dir=str(tmp_path),
    )
    doc = json.loads(path.read_text())["monthly"]
    assert doc["revenue_mlf_source"] == ["dudetailsummary", "mlf-tracker"]
    assert doc["revenue_mlf_value"][0] == pytest.approx((1.019 + 0.9176) / 2)
