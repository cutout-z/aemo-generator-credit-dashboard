"""Distribution loss factors reach revenue in a NEW field (audit 2026-10, H4).

Revenue applied only the transmission MLF. For FY26-27, 145 of 560 units have a
DISTRIBUTIONLOSSFACTOR != 1 (54 below 0.99, min 0.8688). CBWF1 (DLF 0.9113)
reads 9.7% high. revenue_aud is published and labelled "MLF-adjusted", so it
keeps that meaning. The settled-equivalent figure is the new
revenue_loss_adjusted_aud (MLF x DLF per interval), together with the DLF
applied and whether it was published or defaulted to 1.0.
"""

import json

import pandas as pd
import pytest

from src.aggregate import (
    DLF_STATUS_DEFAULT,
    DLF_STATUS_PUBLISHED,
    REVENUE_DLF_STATUS_COL,
    REVENUE_DLF_VALUE_COL,
    REVENUE_LOSS_ADJUSTED_COL,
    REVENUE_MLF_VALUE_COL,
    aggregate_month,
    build_mlf_lookup,
)
from src.generate_json import _aggregate_station_monthly, generate_generator_json
from src.loss_factors import parse_dudetailsummary

HEADER = (
    "I,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,DUID,START_DATE,END_DATE,"
    "DISPATCHTYPE,CONNECTIONPOINTID,REGIONID,STATIONID,PARTICIPANTID,LASTCHANGED,"
    "TRANSMISSIONLOSSFACTOR,STARTTYPE,DISTRIBUTIONLOSSFACTOR"
)


def _periods(*rows):
    lines = [HEADER] + [
        f"D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,{d},{a} 00:00:00,{b} 00:00:00,"
        f"GENERATOR,CP,VIC1,S,P,2026/09/24 11:50:44,{tlf},SLOW,{dlf}"
        for d, a, b, tlf, dlf in rows
    ]
    return parse_dudetailsummary("\n".join(lines))


def _history(duid, fy, mlf):
    return pd.DataFrame({"DUID": [duid], "fy_label": ["FY"], "fy_start_year": [fy],
                         "mlf": [mlf]})


def _row(duid, hist, periods, year=2026, month=8):
    ts = pd.date_range(f"{year}-{month:02d}-12 10:05", periods=12, freq="5min")
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": duid, "SCADAVALUE": 120.0})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "VIC1", "RRP": 100.0})
    gens = pd.DataFrame({"DUID": [duid], "REGION": ["VIC1"], "CAPACITY_MW": [140.0],
                         "FUEL_CATEGORY": ["Wind"]})
    fy = year if month >= 7 else year - 1
    return aggregate_month(scada, prices, None, gens, build_mlf_lookup(hist, fy),
                           year, month, loss_factor_periods=periods).iloc[0]


CBWF1 = _periods(("CBWF1", "2026/07/01", "2999/12/31", "1.0031", "0.9113"))


class TestAggregate:
    def test_dlf_applied_in_new_field_only(self):
        row = _row("CBWF1", _history("CBWF1", 2026, 1.0031), CBWF1)
        # 12 intervals × 120 MW / 12 = 120 MWh × $100 = $12,000
        assert row["revenue_aud"] == round(12000 * 1.0031)
        assert row[REVENUE_LOSS_ADJUSTED_COL] == round(12000 * 1.0031 * 0.9113)
        assert row[REVENUE_DLF_VALUE_COL] == 0.9113
        assert row[REVENUE_DLF_STATUS_COL] == DLF_STATUS_PUBLISHED
        assert row[REVENUE_MLF_VALUE_COL] == 1.0031
        # The CBWF1 audit figure: revenue_aud reads ~9.7% above MLF x DLF.
        assert row["revenue_aud"] / row[REVENUE_LOSS_ADJUSTED_COL] == pytest.approx(
            1 / 0.9113, rel=1e-4)

    def test_no_published_dlf_defaults_to_one_and_says_so(self):
        row = _row("CBWF1", _history("CBWF1", 2026, 1.0031), None)
        assert row[REVENUE_LOSS_ADJUSTED_COL] == row["revenue_aud"]
        assert row[REVENUE_DLF_VALUE_COL] == 1.0
        assert row[REVENUE_DLF_STATUS_COL] == DLF_STATUS_DEFAULT

    def test_dlf_applies_even_when_the_tracker_mlf_is_kept(self):
        """A disagreement on the opening TLF keeps the tracker MLF; the
        distribution factor is independent of that and still applies."""
        periods = _periods(("EMB1", "2026/07/01", "2999/12/31", "0.98", "0.95"))
        row = _row("EMB1", _history("EMB1", 2026, 1.01), periods)
        assert row[REVENUE_MLF_VALUE_COL] == 1.01
        assert row[REVENUE_LOSS_ADJUSTED_COL] == round(12000 * 1.01 * 0.95)


def _frame(loss_adj):
    return pd.DataFrame({
        "duid": ["CBWF1", "CBWF1"], "month": ["2026-07", "2026-08"],
        "generation_mwh": [100.0, 120.0], "revenue_aud": [10000.0, 12037.0],
        "capacity_factor": [0.1, 0.12], "revenue_mlf_status": ["exact", "exact"],
        REVENUE_LOSS_ADJUSTED_COL: loss_adj,
        REVENUE_DLF_VALUE_COL: [None, 0.9113], REVENUE_DLF_STATUS_COL: [None, "published"],
    })


META = {"station_name": "C", "region": "VIC1", "fuel_category": "Wind", "capacity_mw": 140.0,
        "technology": "", "connection_point": "CP", "market": "NEM"}


class TestJson:
    def test_unit_json_carries_new_arrays_alongside_revenue_aud(self, tmp_path):
        path = generate_generator_json("CBWF1", META, monthly_data=_frame([None, 10970.0]),
                                       output_dir=str(tmp_path))
        monthly = json.loads(path.read_text())["monthly"]
        assert monthly["revenue_aud"] == [10000.0, 12037.0]  # meaning unchanged
        assert monthly["revenue_loss_adjusted_aud"] == [None, 10970.0]  # legacy month: None
        assert monthly["revenue_dlf_value"] == [None, 0.9113]
        assert monthly["revenue_dlf_status"] == [None, "published"]

    def test_legacy_frame_omits_the_arrays(self, tmp_path):
        frame = _frame([None, None]).drop(
            columns=[REVENUE_DLF_VALUE_COL, REVENUE_DLF_STATUS_COL])
        path = generate_generator_json("CBWF1", META, monthly_data=frame,
                                       output_dir=str(tmp_path))
        assert "revenue_loss_adjusted_aud" not in json.loads(path.read_text())["monthly"]

    def test_station_sum_is_none_when_a_member_lacks_the_field(self):
        a = _frame([9000.0, 10970.0])
        b = _frame([None, 5000.0]).assign(duid="CBWF2")
        station = _aggregate_station_monthly(pd.concat([a, b], ignore_index=True), 280.0, "Wind")
        assert station["revenue_loss_adjusted_aud"] == [None, 15970.0]
