"""Post-pipeline output validation tests.

Run after the pipeline generates data but before committing/deploying.
These are fast, deterministic checks — no re-derivation from raw data.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DOCS_DATA_DIR = ROOT / "docs" / "data"
GENERATORS_DIR = DOCS_DATA_DIR / "generators"

EXPECTED_REGIONS = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}
EXPECTED_AGGREGATES_COLS = {
    "duid", "month", "generation_mwh", "revenue_aud",
    "capacity_factor", "curtailment_pct", "captured_price", "avg_rrp",
}
EXPECTED_GENERATORS_COLS = {
    "DUID", "STATION_NAME", "REGION", "FUEL_CATEGORY", "CAPACITY_MW",
}
EXPECTED_JSON_KEYS = {
    "duid", "station_name", "region", "fuel_category", "capacity_mw", "monthly",
}

# ─── Feather file existence and non-emptiness ────────────────────────────────


class TestFeatherFiles:
    def test_monthly_aggregates_exists(self):
        path = DATA_DIR / "monthly_aggregates.feather"
        assert path.exists(), "monthly_aggregates.feather missing"
        df = pd.read_feather(path)
        assert len(df) > 0, "monthly_aggregates.feather is empty"

    def test_generators_exists(self):
        path = DATA_DIR / "generators.feather"
        assert path.exists(), "generators.feather missing"
        df = pd.read_feather(path)
        assert len(df) > 0, "generators.feather is empty"

    def test_mlf_history_exists(self):
        path = DATA_DIR / "mlf_history.feather"
        assert path.exists(), "mlf_history.feather missing"
        df = pd.read_feather(path)
        assert len(df) > 0, "mlf_history.feather is empty"


# ─── Schema stability ────────────────────────────────────────────────────────


class TestSchema:
    def test_aggregates_columns(self):
        df = pd.read_feather(DATA_DIR / "monthly_aggregates.feather")
        missing = EXPECTED_AGGREGATES_COLS - set(df.columns)
        assert not missing, f"monthly_aggregates missing columns: {missing}"

    def test_generators_columns(self):
        df = pd.read_feather(DATA_DIR / "generators.feather")
        missing = EXPECTED_GENERATORS_COLS - set(df.columns)
        assert not missing, f"generators.feather missing columns: {missing}"


# ─── Value bounds ─────────────────────────────────────────────────────────────


class TestValueBounds:
    @pytest.fixture(autouse=True)
    def load_data(self):
        self.agg = pd.read_feather(DATA_DIR / "monthly_aggregates.feather")
        self.gen = pd.read_feather(DATA_DIR / "generators.feather")

    def test_capacity_factor_range(self):
        cf = self.agg["capacity_factor"].dropna()
        assert (cf >= 0).all(), f"Negative capacity factors found"
        assert (cf <= 100).all(), f"Capacity factors > 100% found"

    def test_generation_non_negative(self):
        gen = self.agg["generation_mwh"].dropna()
        assert (gen >= 0).all(), "Negative generation_mwh found"

    def test_capacity_mw_positive(self):
        cap = self.gen["CAPACITY_MW"].dropna()
        assert (cap > 0).all(), "Non-positive CAPACITY_MW found"

    def test_mlf_range(self):
        mlf = pd.read_feather(DATA_DIR / "mlf_history.feather")
        mlf_cols = [c for c in mlf.columns if c.startswith("FY")]
        for col in mlf_cols:
            vals = mlf[col].dropna()
            if len(vals) == 0:
                continue
            assert (vals >= 0.5).all() and (vals <= 1.5).all(), (
                f"MLF values outside 0.5-1.5 range in {col}"
            )

    def test_no_placeholder_duids(self):
        assert "-" not in self.gen["DUID"].values, "DUID '-' placeholder still present"
        assert "-" not in self.agg["duid"].values, "DUID '-' placeholder in aggregates"


# ─── Completeness ─────────────────────────────────────────────────────────────


class TestCompleteness:
    def test_all_regions_present(self):
        gen = pd.read_feather(DATA_DIR / "generators.feather")
        regions = set(gen["REGION"].unique())
        missing = EXPECTED_REGIONS - regions
        assert not missing, f"Missing NEM regions: {missing}"

    def test_no_month_gaps_recent(self):
        agg = pd.read_feather(DATA_DIR / "monthly_aggregates.feather")
        months = sorted(agg["month"].unique())
        recent = months[-12:]
        for i in range(1, len(recent)):
            prev = pd.Timestamp(recent[i - 1])
            curr = pd.Timestamp(recent[i])
            gap = (curr.year - prev.year) * 12 + (curr.month - prev.month)
            assert gap == 1, f"Month gap between {recent[i-1]} and {recent[i]}"

    def test_generator_count_stable(self):
        gen = pd.read_feather(DATA_DIR / "generators.feather")
        # Baseline: 559 generators as of initial build. Allow ±15% for
        # registration changes, but catch catastrophic drops.
        assert len(gen) >= 450, f"Generator count dropped to {len(gen)} (expected ~559+)"


# ─── JSON outputs ─────────────────────────────────────────────────────────────


class TestJsonOutputs:
    def test_index_json_valid(self):
        path = DOCS_DATA_DIR / "index.json"
        assert path.exists(), "index.json missing"
        with open(path) as f:
            idx = json.load(f)
        assert isinstance(idx, list), "index.json should be a list"
        assert len(idx) > 0, "index.json is empty"

    def test_index_entries_have_required_fields(self):
        with open(DOCS_DATA_DIR / "index.json") as f:
            idx = json.load(f)
        for entry in idx[:10]:
            missing = {"duid", "region", "fuel_category"} - set(entry.keys())
            assert not missing, f"Index entry {entry.get('duid')} missing: {missing}"

    def test_generator_json_files_exist(self):
        assert GENERATORS_DIR.exists(), "generators/ directory missing"
        files = list(GENERATORS_DIR.glob("*.json"))
        assert len(files) > 0, "No generator JSON files found"

    def test_sample_generator_json_valid(self):
        files = sorted(GENERATORS_DIR.glob("*.json"))[:20]
        checked = 0
        for path in files:
            with open(path) as f:
                data = json.load(f)
            # All generators must have core metadata
            core = {"duid", "station_name", "region", "fuel_category", "capacity_mw"}
            missing = core - set(data.keys())
            assert not missing, f"{path.name} missing keys: {missing}"
            # Generators with SCADA data should have valid monthly structure
            if "monthly" in data:
                assert len(data["monthly"].get("months", [])) > 0, (
                    f"{path.name} has monthly key but no months"
                )
                checked += 1
        assert checked >= 3, f"Only {checked}/20 sampled generators had monthly data"

    def test_json_feather_consistency(self):
        """Spot-check that generator JSON monthly data matches feather aggregates."""
        agg = pd.read_feather(DATA_DIR / "monthly_aggregates.feather")
        with open(DOCS_DATA_DIR / "index.json") as f:
            idx = json.load(f)

        # Pick first 3 non-station entries
        duids = [e["duid"] for e in idx if not e["duid"].startswith("station_")][:3]

        # Pick DUIDs that have aggregation data
        duids_with_data = [
            e["duid"] for e in idx
            if not e["duid"].startswith("station_")
            and e["duid"] in agg["duid"].values
        ][:3]

        for duid in duids_with_data:
            json_path = GENERATORS_DIR / f"{duid}.json"
            if not json_path.exists():
                continue
            with open(json_path) as f:
                data = json.load(f)

            assert "monthly" in data, f"{duid}: has aggregation data but no monthly in JSON"
            json_months = data["monthly"]["months"]

            feather_rows = agg[agg["duid"] == duid].sort_values("month")
            feather_months = feather_rows["month"].tolist()

            # JSON should contain all feather months
            for month in feather_months:
                assert month in json_months, (
                    f"{duid}: month {month} in feather but not in JSON"
                )


# ─── Curtailment FY rollup (consumed by renewable dashboard) ─────────────────


class TestCurtailmentByFY:
    def test_file_exists_and_non_empty(self):
        path = DOCS_DATA_DIR / "curtailment_by_fy.csv"
        assert path.exists(), "curtailment_by_fy.csv missing"
        df = pd.read_csv(path)
        assert len(df) > 0, "curtailment_by_fy.csv is empty"

    def test_expected_columns(self):
        df = pd.read_csv(DOCS_DATA_DIR / "curtailment_by_fy.csv")
        expected = {"duid", "fy_start", "fy_label", "curtailment_pct",
                    "grid_curtailment_pct", "generation_mwh", "months_covered"}
        missing = expected - set(df.columns)
        assert not missing, f"curtailment_by_fy.csv missing columns: {missing}"

    def test_curtailment_in_range(self):
        df = pd.read_csv(DOCS_DATA_DIR / "curtailment_by_fy.csv")
        vals = df["curtailment_pct"].dropna()
        assert (vals >= 0).all() and (vals <= 1).all(), (
            "curtailment_pct outside [0, 1]"
        )

    def test_covers_recent_complete_fys(self):
        """Must publish both of the last 2 complete FYs for >=100 DUIDs each."""
        import datetime as _dt
        df = pd.read_csv(DOCS_DATA_DIR / "curtailment_by_fy.csv")
        now = _dt.datetime.now()
        current_fy = now.year if now.month >= 7 else now.year - 1
        for fy_start in (current_fy - 2, current_fy - 1):
            complete = df[(df["fy_start"] == fy_start) & (df["months_covered"] == 12)]
            assert len(complete) >= 100, (
                f"FY{fy_start} has only {len(complete)} complete-12mo DUIDs"
            )
