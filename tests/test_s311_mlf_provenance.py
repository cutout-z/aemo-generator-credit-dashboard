"""S3-11 tests: per-DUID MLF revenue resolution + provenance in exports.

Review S3-11: ``build_mlf_lookup`` returned only exact-FY rows when any exact
row existed (a unit missing that FY was silently dropped to 1.0 in revenue
aggregation even when it had a prior factor), and fell back to each unit's
LATEST FY — including future years — when the whole FY was absent. Neither
path recorded the imputation or its source FY in the revenue facts.

This suite pins the replacement contract:
  - resolution is PER DUID: exact / prior-carry / unknown,
  - a future FY's factor is never applied to historical revenue,
  - unknown factors -> revenue is UNADJUSTED spot revenue, labelled
    provisional (``revenue_mlf_status == "unknown"``, value None),
  - the provenance travels into generator JSON monthly blocks, station
    monthly blocks and the CSV/XLSX export row builder.
"""

import json

import numpy as np
import pandas as pd
import pytest

from src.aggregate import (
    MLF_STATUS_EXACT,
    MLF_STATUS_PRIOR_CARRY,
    MLF_STATUS_UNKNOWN,
    REVENUE_MLF_SOURCE_FY_COL,
    REVENUE_MLF_STATUS_COL,
    REVENUE_MLF_VALUE_COL,
    aggregate_month,
    build_mlf_lookup,
)
from src.generate_json import generate_generator_json, generate_all


def _history(records):
    """records: list of (DUID, fy_start_year, mlf) -> long MLF frame."""
    return pd.DataFrame(
        {
            "DUID": [r[0] for r in records],
            "fy_label": [f"FY{str(r[1])[2:]}-{str(r[1]+1)[2:]}" for r in records],
            "fy_start_year": [r[1] for r in records],
            "mlf": [float(r[2]) for r in records],
        }
    )


def _gens(duids):
    return pd.DataFrame(
        {
            "DUID": duids,
            "REGION": ["NSW1"] * len(duids),
            "CAPACITY_MW": [100.0] * len(duids),
            "FUEL_CATEGORY": ["Battery"] * len(duids),
        }
    )


def _scada_prices(duid):
    ts = pd.to_datetime(["2026-03-01 00:05:00", "2026-03-01 00:10:00"])
    scada = pd.DataFrame(
        {"SETTLEMENTDATE": ts, "DUID": [duid, duid], "SCADAVALUE": [5.0, 5.0]}
    )
    prices = pd.DataFrame(
        {"SETTLEMENTDATE": ts, "REGIONID": ["NSW1", "NSW1"], "RRP": [100.0, 100.0]}
    )
    return scada, prices


# ─── build_mlf_lookup: per-DUID resolution ────────────────────────────────


class TestBuildMlfLookup:
    def test_mixed_year_fixture_resolves_per_duid(self):
        """Review's A/B repro: A=0.9 FY2026, B=0.8 FY2025. A FY2026 lookup must
        return B as prior-carry (0.8, source FY2025) — B's coverage must NOT
        depend on A's, and B must not silently become 1.0."""
        hist = _history([("A", 2026, 0.9), ("B", 2025, 0.8)])
        lookup = build_mlf_lookup(hist, 2026)
        assert lookup["A"] == {
            "mlf": 0.9, "source_fy_start": 2026, "status": MLF_STATUS_EXACT,
        }
        assert lookup["B"] == {
            "mlf": 0.8, "source_fy_start": 2025, "status": MLF_STATUS_PRIOR_CARRY,
        }

    def test_prior_carry_uses_most_recent_prior_only(self):
        """A unit with FY2018 and FY2025 rows, target FY2020 -> carry 2018's
        factor (the most recent PRIOR), never the later FY2025 row."""
        hist = _history([("A", 2018, 0.7), ("A", 2025, 0.85)])
        lookup = build_mlf_lookup(hist, 2020)
        assert lookup["A"] == {
            "mlf": 0.7, "source_fy_start": 2018, "status": MLF_STATUS_PRIOR_CARRY,
        }

    def test_no_future_factor_on_historical_revenue(self):
        """A FY2020 request must not silently consume FY2026: a unit whose only
        rows are AFTER the target resolves unknown, not to a future value."""
        hist = _history([("A", 2026, 0.9), ("B", 2025, 0.8)])
        lookup = build_mlf_lookup(hist, 2020)
        assert lookup["A"]["status"] == MLF_STATUS_UNKNOWN
        assert lookup["A"]["mlf"] is None
        assert lookup["A"]["source_fy_start"] is None
        assert lookup["B"]["status"] == MLF_STATUS_UNKNOWN
        assert lookup["B"]["mlf"] is None

    def test_whole_fy_absent_never_falls_back_to_future(self):
        """The old 'whole FY absent -> each unit's latest (incl. future)' path
        is gone: every unit is resolved independently and only rows <= target
        may feed exact/prior-carry."""
        hist = _history([("A", 2025, 0.8), ("B", 2024, 0.75), ("C", 2026, 0.9)])
        lookup = build_mlf_lookup(hist, 2025)
        assert lookup["A"]["status"] == MLF_STATUS_EXACT
        assert lookup["B"]["status"] == MLF_STATUS_PRIOR_CARRY  # 2024 < 2025
        assert lookup["C"]["status"] == MLF_STATUS_UNKNOWN  # 2026 is future

    def test_empty_history_returns_empty(self):
        assert build_mlf_lookup(None, 2026) == {}
        assert build_mlf_lookup(pd.DataFrame(), 2026) == {}

    def test_duid_with_no_prior_rows_is_unknown_even_when_others_exact(self):
        """Regression for the exact-frame masking: when OTHER units have the
        target FY but this DUID does not, it must not be omitted from the
        lookup result as if it had no entry at all."""
        hist = _history([("A", 2026, 0.9), ("NEW1", 2025, 0.8)])
        lookup = build_mlf_lookup(hist, 2026)
        assert "A" in lookup
        assert lookup["NEW1"]["status"] == MLF_STATUS_PRIOR_CARRY
        hist2 = _history([("A", 2026, 0.9)])
        lookup2 = build_mlf_lookup(hist2, 2026)
        # A DUID entirely absent from history is not in the dict; aggregate
        # treats absence identically to an explicit unknown (tested below).
        assert "NEW1" not in lookup2


# ─── aggregate_month: provenance columns + revenue semantics ───────────────


def _aggregate_one(duid, lookup, fuel="Battery"):
    scada, prices = _scada_prices(duid)
    return aggregate_month(
        scada, prices, None, _gens([duid]), lookup, 2026, 3
    ).iloc[0]


class TestAggregateMonthProvenance:
    def test_exact_factor_applied_and_recorded(self):
        hist = _history([("A", 2026, 0.9)])
        row = _aggregate_one("A", build_mlf_lookup(hist, 2026))
        # 2 intervals × 5 MW / 12 × $100 × 0.9 = 75.0
        assert row["revenue_aud"] == 75
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_EXACT
        assert row[REVENUE_MLF_SOURCE_FY_COL] == 2026
        assert row[REVENUE_MLF_VALUE_COL] == 0.9

    def test_prior_carry_factor_applied_with_source_fy(self):
        """B (no FY2026 row, FY2025=0.8) must be adjusted by 0.8, NOT 1.0."""
        hist = _history([("B", 2025, 0.8)])
        row = _aggregate_one("B", build_mlf_lookup(hist, 2026))
        assert row["revenue_aud"] == 67  # 83.33 × 0.8 rounded
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_PRIOR_CARRY
        assert row[REVENUE_MLF_SOURCE_FY_COL] == 2025
        assert row[REVENUE_MLF_VALUE_COL] == pytest.approx(0.8)

    def test_unknown_is_unadjusted_and_provisional(self):
        """Unknown factor -> revenue ×1.0 (unadjusted spot revenue) labelled
        provisional: value None, status unknown, source FY None."""
        hist = _history([("C", 2026, 0.9)])  # C only has a FUTURE row for 2026-03's FY
        # 2026-03 falls in FY25-26 (fy_start 2025); C has no 2025 row.
        row = _aggregate_one("C", build_mlf_lookup(hist, 2025))
        assert row["revenue_aud"] == 83  # 83.33 × 1.0 unadjusted
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_UNKNOWN
        assert row[REVENUE_MLF_SOURCE_FY_COL] is None
        assert row[REVENUE_MLF_VALUE_COL] is None

    def test_absent_from_lookup_is_unknown_like_explicit_unknown(self):
        """A DUID with SCADA but no MLF history at all behaves exactly like an
        explicit unknown — unadjusted, provisional, provenance recorded."""
        hist = _history([("OTHER", 2026, 0.9)])
        row = _aggregate_one("GHOST1", build_mlf_lookup(hist, 2025))
        assert row["revenue_aud"] == 83
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_UNKNOWN
        assert row[REVENUE_MLF_VALUE_COL] is None

    def test_empty_lookup_dict_keeps_prior_semantics(self):
        """Existing callers pass {} (e.g. intermittent-quality tests): no MLF
        data at all -> every DUID unknown/unadjusted (was silently 1.0)."""
        row = _aggregate_one("A", {})
        assert row["revenue_aud"] == 83
        assert row[REVENUE_MLF_STATUS_COL] == MLF_STATUS_UNKNOWN

    def test_no_mixing_between_units_in_one_aggregation(self):
        """A and B in the SAME aggregation resolve independently — B is not
        affected by A having the target FY (the S3-11 masking repro)."""
        scada_a, prices = _scada_prices("A")
        scada_b, _prices = _scada_prices("B")
        scada = pd.concat([scada_a, scada_b], ignore_index=True)
        gens = _gens(["A", "B"])
        hist = _history([("A", 2026, 0.9), ("B", 2025, 0.8)])
        result = aggregate_month(
            scada, prices, None, gens, build_mlf_lookup(hist, 2026), 2026, 3
        ).set_index("duid")
        assert result.loc["A", REVENUE_MLF_STATUS_COL] == MLF_STATUS_EXACT
        assert result.loc["A", "revenue_aud"] == 75
        assert result.loc["B", REVENUE_MLF_STATUS_COL] == MLF_STATUS_PRIOR_CARRY
        assert result.loc["B", "revenue_aud"] == 67
        # The column list also still carries energy/curtailment fields.
        assert REVENUE_MLF_VALUE_COL in result.columns


# ─── JSON emission: unit + station monthly blocks ─────────────────────────


def _unit_monthly_frame():
    """One DUID across two months with full provenance columns (as produced by
    aggregate_month under S3-11)."""
    return pd.DataFrame(
        {
            "duid": ["GEN1", "GEN1"],
            "month": ["2026-02", "2026-03"],
            "generation_mwh": [100.0, 200.0],
            "revenue_aud": [90000.0, 160000.0],
            "capacity_factor": [0.5, 0.6],
            "captured_price": [50.0, 40.0],
            "avg_rrp": [55.0, 45.0],
            "price_capture_ratio": [0.9091, 0.8889],
            REVENUE_MLF_STATUS_COL: [MLF_STATUS_EXACT, MLF_STATUS_PRIOR_CARRY],
            REVENUE_MLF_SOURCE_FY_COL: [2026, 2025],
            REVENUE_MLF_VALUE_COL: [0.9, 0.8],
        }
    )


class TestJsonEmission:
    def test_unit_json_monthly_block_carries_provenance_arrays(self, tmp_path):
        gen = _gens(["GEN1"]).assign(
            STATION_NAME=["Test One"], TECHNOLOGY=["Battery"], CONNECTION_POINT=["CP1"],
            MARKET=["NEM"],
        )
        out = tmp_path / "generators"
        path = generate_generator_json(
            "GEN1",
            {
                "station_name": "Test One", "region": "NSW1", "fuel_category": "Battery",
                "capacity_mw": 100.0, "technology": "Battery", "connection_point": "CP1",
                "market": "NEM",
            },
            monthly_data=_unit_monthly_frame(),
            mlf_data={"years": ["FY25-26", "FY26-27"], "values": [0.8, 0.9]},
            output_dir=str(out),
        )
        doc = json.loads(path.read_text())
        monthly = doc["monthly"]
        assert monthly["revenue_mlf_status"] == ["exact", "prior-carry"]
        assert monthly["revenue_mlf_source_fy"] == ["FY26-27", "FY25-26"]
        assert monthly["revenue_mlf_value"] == [0.9, 0.8]
        # Parallel arrays stay aligned with months/revenue.
        assert len(monthly["revenue_mlf_status"]) == len(monthly["months"])
        assert monthly["months"] == ["2026-02", "2026-03"]

    def test_unit_json_without_provenance_columns_omits_arrays(self, tmp_path):
        """Legacy monthly frames (pre-S3-11 aggregates) must not fabricate
        provenance — arrays are simply absent, and the JSON stays valid."""
        gen = _gens(["GEN1"]).assign(
            STATION_NAME=["Test One"], TECHNOLOGY=["Battery"], CONNECTION_POINT=["CP1"],
            MARKET=["NEM"],
        )
        legacy = _unit_monthly_frame().drop(
            columns=[REVENUE_MLF_STATUS_COL, REVENUE_MLF_SOURCE_FY_COL, REVENUE_MLF_VALUE_COL]
        )
        out = tmp_path / "generators"
        path = generate_generator_json(
            "GEN1",
            {
                "station_name": "Test One", "region": "NSW1", "fuel_category": "Battery",
                "capacity_mw": 100.0, "technology": "Battery", "connection_point": "CP1",
                "market": "NEM",
            },
            monthly_data=legacy,
            mlf_data={"years": ["FY25-26"], "values": [0.8]},
            output_dir=str(out),
        )
        doc = json.loads(path.read_text())
        assert "revenue_mlf_status" not in doc["monthly"]
        assert "revenue_mlf_value" not in doc["monthly"]

    def test_station_monthly_marks_unknown_member_provisional(self, tmp_path):
        """Station revenue = sum of member revenues; if ANY member month is
        unknown (unadjusted), the station month is itself unknown/provisional.
        If all members are exact, the station month is exact."""
        generators = pd.DataFrame(
            {
                "DUID": ["GEN_A", "GEN_B", "GEN_C"],
                "STATION_NAME": ["Two Unit Wind Farm", "Two Unit Wind Farm", "Solo"],
                "REGION": ["NSW1", "NSW1", "NSW1"],
                "FUEL_CATEGORY": ["Wind", "Wind", "Wind"],
                "CAPACITY_MW": [100.0, 100.0, 100.0],
                "TECHNOLOGY": ["Wind", "Wind", "Wind"],
                "CONNECTION_POINT": ["CP_A", "CP_B", "CP_C"],
            }
        )
        monthly = pd.DataFrame(
            {
                "duid": ["GEN_A", "GEN_B", "GEN_C"],
                "month": ["2026-03", "2026-03", "2026-03"],
                "generation_mwh": [1000.0, 1000.0, 1000.0],
                "revenue_aud": [50000.0, 50000.0, 50000.0],
                "capacity_factor": [0.5, 0.5, 0.5],
                "curtailment_pct": [0.2, 0.2, 0.2],
                "curtailment_actual_mwh": [900.0, 900.0, 900.0],
                "curtailment_potential_mwh": [1100.0, 1100.0, 1100.0],
                "econ_curtailment_pct": [0.02, 0.02, 0.02],
                "captured_price": [50.0, 50.0, 50.0],
                "avg_rrp": [55.0, 55.0, 55.0],
                "price_capture_ratio": [0.9091, 0.9091, 0.9091],
                REVENUE_MLF_STATUS_COL: [MLF_STATUS_EXACT, MLF_STATUS_UNKNOWN, MLF_STATUS_EXACT],
                REVENUE_MLF_SOURCE_FY_COL: [2026, None, 2026],
                REVENUE_MLF_VALUE_COL: [0.9, None, 0.9],
            }
        )
        out = tmp_path / "generators"
        generate_all(generators, monthly_aggregates=monthly, output_dir=str(out))
        station_path = out / "station_Two_Unit_Wind_Farm.json"
        station = json.loads(station_path.read_text())
        # Any unknown member -> station month provisional (status unknown).
        assert station["monthly"]["revenue_mlf_status"] == ["unknown"]
        # Solo unit keeps its own DUID file (single-DUID stations are not
        # aggregated into a station doc) and stays exact.
        solo = json.loads((out / "GEN_C.json").read_text())
        assert solo["monthly"]["revenue_mlf_status"] == ["exact"]

    def test_station_monthly_without_provenance_columns_is_unchanged(self, tmp_path):
        """Legacy member rows (no provenance columns) produce a station block
        with no revenue_mlf arrays — identical to pre-S3-11 output."""
        generators = pd.DataFrame(
            {
                "DUID": ["GEN_A", "GEN_B"],
                "STATION_NAME": ["Two Unit Wind Farm", "Two Unit Wind Farm"],
                "REGION": ["NSW1", "NSW1"],
                "FUEL_CATEGORY": ["Wind", "Wind"],
                "CAPACITY_MW": [100.0, 300.0],
                "TECHNOLOGY": ["Wind", "Wind"],
                "CONNECTION_POINT": ["CP_A", "CP_B"],
            }
        )
        monthly = pd.DataFrame(
            {
                "duid": ["GEN_A", "GEN_B"],
                "month": ["2026-04", "2026-04"],
                "generation_mwh": [1000.0, 3000.0],
                "revenue_aud": [50000.0, 150000.0],
                "capacity_factor": [0.1, 0.2],
                "curtailment_pct": [0.2, 0.4],
                "econ_curtailment_pct": [0.02, 0.06],
                "captured_price": [50.0, 60.0],
                "avg_rrp": [55.0, 55.0],
                "price_capture_ratio": [0.9091, 1.0909],
            }
        )
        out = tmp_path / "generators"
        generate_all(generators, monthly_aggregates=monthly, output_dir=str(out))
        station = json.loads((out / "station_Two_Unit_Wind_Farm.json").read_text())
        assert "revenue_mlf_status" not in station["monthly"]
        assert station["monthly"]["revenue_aud"] == [200000.0]


# ─── Live-frame check: current final FY must resolve without future factors ─


class TestLiveResolution:
    def test_live_tracker_resolves_current_final_fy_without_future(self):
        """Against the real tracker cache, resolving the current final FY must
        never return a source FY after the target (no future factor)."""
        from pathlib import Path

        from src.fetch_mlf import fetch_mlf_data

        root = Path(__file__).resolve().parent.parent
        csv_path = root / "data" / "mlf_tracker_summary.csv"
        if not csv_path.exists():
            pytest.skip("live MLF tracker cache not present")
        frame, _draft, _draft_fy, _cp = fetch_mlf_data(str(root / "data"), force=False)
        target = int(frame["fy_start_year"].max())  # current final FY
        lookup = build_mlf_lookup(frame, target)
        assert lookup, "live lookup must resolve at least one DUID"
        for duid, res in lookup.items():
            assert res["status"] in (
                MLF_STATUS_EXACT, MLF_STATUS_PRIOR_CARRY, MLF_STATUS_UNKNOWN,
            ), duid
            if res["source_fy_start"] is not None:
                assert res["source_fy_start"] <= target, (
                    f"{duid} resolved from future FY {res['source_fy_start']} > {target}"
                )
            if res["status"] == MLF_STATUS_EXACT:
                assert res["source_fy_start"] == target
        # The lookup must cover every DUID that has a factor in this FY or any
        # prior one (the old code dropped exact-FY-missing DUIDs entirely).
        covered = set(lookup.keys())
        assert len(covered) >= 400, f"live resolution implausibly small: {len(covered)}"
