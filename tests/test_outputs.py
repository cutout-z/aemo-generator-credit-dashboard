"""Post-pipeline output validation tests.

Run after the pipeline generates data but before committing/deploying.
These are fast, deterministic checks — no re-derivation from raw data.
"""

import json
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src import config as _config
from src.fetch_mlf import MLF_HISTORY_COLUMNS, fetch_mlf_data, validate_mlf_history
from src.freshness import check_monthly_freshness, check_daily_freshness

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

# ─── Fuel-aware monthly capacity-factor policy ──────────────────────────────
# Mirrors the daily policy (test_daily_capacity_factor_range): capacity factor
# is a RATIO (fraction, ~0-1), never a percentage — the old test allowing
# CF <= 100 would pass a synthetic CF of 50 (5000%). Hydro is the documented
# fuel-aware exception (headwater/short-release physics; daily peaks ~1.19,
# sustained monthly outliers like POAT110 ~1.06) and is allowed to 1.25; every
# other fuel hard-fails above 1.10 — a whole-month ratio past that means
# sustained output above registered capacity, not physics.
MONTHLY_CF_BOUNDS: dict[str, float] = {"Hydro": 1.25}
NON_HYDRO_MONTHLY_CF_BOUND = 1.10

# The one month ever observed above those bounds (2022-06, NEM market
# suspension): fleet-wide SCADA anomalies push post-override monthly CF to
# 1.12-1.15 across ~7 fossil units while those same units have NO daily rows
# that month — a documented archive artifact, not a registration signal.
# Delete this exemption only if AEMO ever republishes corrected 2022-06 SCADA;
# never widen it to mask a new month.
CF_SUSPENSION_ANOMALY_MONTH = "2022-06"

# Producer rounding contract for generator-JSON monthly fields (generate_json.py)
# and the mutable tail the pipeline may still correct (main.py DEFAULT_MONTHS_BACK
# = 2): a JSON published from a later run can legitimately differ there, so
# numeric parity is asserted only on the settled overlap.
JSON_MONTHLY_ROUNDING = {
    "generation_mwh": 0,
    "revenue_aud": 0,
    "capacity_factor": 4,
    "captured_price": 2,
    "avg_rrp": 2,
    "price_capture_ratio": 4,
}
MUTABLE_TAIL_MONTHS = 2


def _month_hours(month_labels) -> pd.Series:
    return pd.Series(
        [monthrange(int(lbl[:4]), int(lbl[5:]))[1] * 24 for lbl in month_labels],
        index=month_labels.index,
    )


def _fuel_aware_monthly_cf_offenders(
    monthly: pd.DataFrame, generators: pd.DataFrame
) -> pd.DataFrame:
    """Rows breaching the monthly fuel-aware CF policy (empty when clean).

    Producer parity: capacity_factor is recomputed from generation ÷ (override
    capacity × month hours) for CAPACITY_OVERRIDES DUIDs — main.py Step 3b-fix
    rewrites exactly those rows before publishing, so a stale local cache and a
    fresh NAS publish are judged on the same corrected ratio.

    Returns rows with the offending ``cf`` (override-corrected), fuel and bound.
    """
    if monthly is None or monthly.empty or "capacity_factor" not in monthly.columns:
        return pd.DataFrame(columns=["duid", "month", "cf", "fuel", "bound"])
    out = monthly[["duid", "month", "capacity_factor"]].copy()
    out["cf"] = out["capacity_factor"]
    hrs = _month_hours(monthly["month"])
    for duid, new_cap in _config.CAPACITY_OVERRIDES.items():
        mask = monthly["duid"] == duid
        if mask.any():
            out.loc[mask, "cf"] = (
                monthly.loc[mask, "generation_mwh"] / (new_cap * hrs[mask])
            ).round(4)
    out["fuel"] = out["duid"].map(
        generators.set_index("DUID")["FUEL_CATEGORY"]
    ).fillna("Other")
    out["bound"] = out["fuel"].map(MONTHLY_CF_BOUNDS).fillna(NON_HYDRO_MONTHLY_CF_BOUND)
    out["month"] = monthly["month"].astype(str)
    flagged = (
        out["cf"].notna()
        & (out["month"] != CF_SUSPENSION_ANOMALY_MONTH)
        & (out["cf"] > out["bound"])
    )
    return out.loc[flagged, ["duid", "month", "cf", "fuel", "bound"]]


def _assert_selected_json_present(entries, gen_dir: Path) -> None:
    """Every selected index entry must have its JSON target on disk.

    Raises FileNotFoundError listing every missing target — a missing selected
    JSON FAILS the gate; it never skips (S3-10).
    """
    missing = [
        f"{e['duid']} -> {e['file']}.json"
        for e in entries
        if not (gen_dir / f"{e['file']}.json").exists()
    ]
    if missing:
        raise FileNotFoundError(
            "selected generator JSON files missing: " + ", ".join(missing[:10])
        )


def _live_mlf_frame() -> pd.DataFrame:
    """The actual pipeline MLF frame: fetch_mlf_data over the tracker cache.

    The cache file must exist (it is the pipeline's own input cache); a
    missing cache means the pipeline never fetched MLF input and the gate
    should FAIL, not skip.
    """
    csv_path = DATA_DIR / "mlf_tracker_summary.csv"
    assert csv_path.exists(), (
        "mlf_tracker_summary.csv missing — cannot validate the live MLF frame"
    )
    frame, _draft, _draft_fy, _cp = fetch_mlf_data(str(DATA_DIR), force=False)
    return frame


def _settled_feather_rows(feather_rows: pd.DataFrame) -> list[str]:
    """Settled (non-mutable) month labels for a DUID's feather rows."""
    months = sorted(str(m) for m in feather_rows["month"])
    return months[: max(0, len(months) - MUTABLE_TAIL_MONTHS)]


def _json_feather_monthly_mismatches(
    duid: str, json_monthly: dict, feather_rows: pd.DataFrame
) -> list[tuple]:
    """Numeric mismatches between a generator JSON monthly block and the feather.

    Compares only the settled overlap (months in BOTH artifacts, excluding the
    feather's mutable tail) and applies the producer's rounding contract, so
    identical month labels with changed numeric values are caught while vintage
    skew and legitimate float rounding are not. ``capacity_factor`` is compared
    against the override-corrected feather value (producer parity).

    Returns a list of (month, metric, json_value, feather_value) tuples.
    """
    months = list(json_monthly.get("months") or [])
    if not months:
        return []
    settled = set(_settled_feather_rows(feather_rows))
    if not any(m in settled for m in months):
        return []

    f = feather_rows.copy()
    hrs = _month_hours(f["month"])
    f["month"] = f["month"].astype(str)
    for duid_ov, new_cap in _config.CAPACITY_OVERRIDES.items():
        mask = f["duid"] == duid_ov
        if mask.any() and "generation_mwh" in f.columns:
            f.loc[mask, "capacity_factor"] = (
                f.loc[mask, "generation_mwh"] / (new_cap * hrs[mask])
            ).round(4)
    f = f.set_index("month")

    mismatches: list[tuple] = []
    for metric, dp in JSON_MONTHLY_ROUNDING.items():
        if metric not in json_monthly or metric not in f.columns:
            continue
        json_values = json_monthly[metric]
        for j, m in enumerate(months):
            if m not in settled or j >= len(json_values):
                continue
            jv = json_values[j]
            fv = f.loc[m, metric]
            if pd.isna(jv) and pd.isna(fv):
                continue
            if pd.isna(jv) or pd.isna(fv):
                mismatches.append((m, metric, jv, fv))
                continue
            expected = round(float(fv), dp)
            if abs(float(jv) - expected) > 1e-9:
                mismatches.append((m, metric, jv, expected))
    return mismatches

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

    def test_mlf_tracker_input_exists(self):
        # The MLF frame of record is fetch_mlf_data()'s long output from the MLF
        # Tracker summary CSV. data/mlf_history.feather is a legacy wide-era
        # leftover (last refreshed FY25-26; nothing in src/ writes or reads it)
        # and is deliberately NOT asserted here.
        path = DATA_DIR / "mlf_tracker_summary.csv"
        assert path.exists(), "mlf_tracker_summary.csv missing — pipeline never fetched MLF input"
        df = pd.read_csv(path)
        assert len(df) > 0, "mlf_tracker_summary.csv is empty"
        assert "DUID" in df.columns, "mlf_tracker_summary.csv has no DUID column"


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
        """Monthly CF bounded near 1, fuel-aware (hydro is the exception class).

        CF is a ratio in [0, ~1]; the old assertion allowed 100 — a synthetic
        CF of 50 (5000%) sailed through. Now the fuel-aware monthly policy runs
        (see _fuel_aware_monthly_cf_offenders): non-hydro hard-fails above 1.10,
        hydro above 1.25, with CAPACITY_OVERRIDES recomputed for producer parity
        and the documented 2022-06 market-suspension anomaly month exempted.
        """
        cf = self.agg["capacity_factor"].dropna()
        assert (cf >= 0).all(), "Negative capacity factors found"
        offenders = _fuel_aware_monthly_cf_offenders(self.agg, self.gen)
        assert offenders.empty, (
            "Monthly CF breaches fuel-aware bounds: "
            f"{offenders.to_dict('records')[:10]}"
        )

    def test_generation_non_negative(self):
        gen = self.agg["generation_mwh"].dropna()
        assert (gen >= 0).all(), "Negative generation_mwh found"

    def test_capacity_mw_positive(self):
        cap = self.gen["CAPACITY_MW"].dropna()
        assert (cap > 0).all(), "Non-positive CAPACITY_MW found"

    def test_monthly_cf_rejects_nonsense_scale(self):
        """S3-10 negative regression: CF 50 (percent scale) must FAIL the policy.

        The review reproduced a synthetic CF of 50.0 passing the old
        ``cf <= 100`` assertion. The fuel-aware policy must flag it, while
        intentional hydro exceptions (<= 1.25), override-corrected
        registrations and the documented 2022-06 anomaly month still pass.
        """
        gen = pd.DataFrame(
            {
                "DUID": ["GENX", "GENH", "HUMENSW", "GENY"],
                "FUEL_CATEGORY": ["Solar", "Hydro", "Hydro", "Fossil"],
                "CAPACITY_MW": [100.0, 100.0, 29.0, 500.0],
            }
        )
        monthly = pd.DataFrame(
            {
                "duid": ["GENX", "GENH", "HUMENSW", "GENY"],
                "month": ["2024-05", "2024-05", "2024-05", "2022-06"],
                # GENX: percent-scale 50.0 (review repro). GENH: hydro 1.19
                # (documented exception). HUMENSW: raw 1.9 from the stale 29 MW
                # registration; ~41 GWh clears to ~0.95 under the 58 MW
                # override. GENY: 2022-06 anomaly month (exempted).
                "generation_mwh": [0.0, 0.0, 41000.0, 0.0],
                "capacity_factor": [50.0, 1.19, 1.9, 1.1519],
            }
        )
        offenders = _fuel_aware_monthly_cf_offenders(monthly, gen)
        flagged = set(offenders["duid"])
        assert "GENX" in flagged, "CF 50.0 (percent-scale) must fail the monthly policy"
        # GENY sits in the documented 2022-06 anomaly month: exempted.
        assert "GENY" not in flagged
        # Intentional hydro exception preserved.
        assert "GENH" not in flagged

    def test_monthly_cf_override_correction_clears_registration_bug(self):
        """A raw CF inflated by a stale registration must pass once the
        documented CAPACITY_OVERRIDES recompute is applied (producer parity)."""
        gen = pd.DataFrame(
            {
                "DUID": ["HUMENSW"],
                "FUEL_CATEGORY": ["Hydro"],
                "CAPACITY_MW": [29.0],
            }
        )
        monthly = pd.DataFrame(
            {
                "duid": ["HUMENSW"],
                "month": ["2024-05"],
                # Raw CF 1.9 against the stale 29 MW registration; ~41 GWh in
                # May (744 h) = 55.1 MW avg, i.e. 0.95 of the real 58 MW.
                "generation_mwh": [41000.0],
                "capacity_factor": [1.9],
            }
        )
        offenders = _fuel_aware_monthly_cf_offenders(monthly, gen)
        assert offenders.empty

    def test_mlf_range(self):
        """MLF values in the LIVE long frame (mlf column) stay in [0.5, 1.5].

        The old check looped over wide FY* columns of the legacy
        data/mlf_history.feather, which is long-schema — the loop found no
        columns and asserted nothing (MLF -999 sailed through). The live frame
        is fetch_mlf_data()'s melt of the tracker CSV; validate_mlf_history
        (also invoked by the fetch itself) enforces the schema and range.
        """
        frame = _live_mlf_frame()
        assert validate_mlf_history(frame) is not None

    def test_mlf_sentinel_negative_fails(self, tmp_path):
        """S3-10 negative regression: MLF -999 in the live long schema must fail.

        Two paths: validate_mlf_history directly on a long frame carrying -999,
        and the full fetch_mlf_data() read of a tracker CSV that contains -999
        (hermetic — the fixture CSV is written first so no network is touched).
        """
        long_frame = pd.DataFrame(
            {
                "DUID": ["GEN1", "GEN1", "GEN1", "GEN2"],
                "fy_label": ["FY24-25", "FY25-26", "FY26-27", "FY26-27"],
                "fy_start_year": [2024, 2025, 2026, 2026],
                "mlf": [0.95, 0.98, -999.0, 0.97],
            }
        )
        with pytest.raises(ValueError, match="plausible range"):
            validate_mlf_history(long_frame)

        # Through the real fetch path: fixture tracker CSV with a -999 sentinel.
        csv_path = tmp_path / "mlf_tracker_summary.csv"
        csv_path.write_text(
            "DUID,REGIONID,FY24-25,FY25-26,FY26-27\n"
            "GEN1,NSW1,0.95,0.98,0.97\n"
            "GEN2,VIC1,0.90,0.91,-999\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="plausible range"):
            fetch_mlf_data(str(tmp_path), force=False)

    def test_no_placeholder_duids(self):
        assert "-" not in self.gen["DUID"].values, "DUID '-' placeholder still present"
        assert "-" not in self.agg["duid"].values, "DUID '-' placeholder in aggregates"

    def test_daily_capacity_factor_range(self):
        """Daily CF bounds, fuel-aware.

        Bounds are fuel-aware. Hydro units peak above nameplate during
        short-duration releases (CETHANA/GUTHEGA/POAT*/REPULSE max ~1.19)
        and are allowed to 1.25. Non-hydro hard-fails at 1.10: catches
        material (>10%) registration understatement. The subtle 2-10% class
        (BW02 1.036, OSB-AG ~1.07, QPS3 ~1.095) is deliberately delegated to
        the monthly CF audit (src/audit_cf.py: flags sustained CF > 1.02 over
        3+ months) plus manual CAPACITY_OVERRIDES review - a hard bound there
        would fail every run without a confirmed registration correction.
        """
        path = DATA_DIR / "daily_aggregates.feather"
        if not path.exists():
            pytest.skip("daily_aggregates.feather not present")
        daily = pd.read_feather(path)
        cf = daily["daily_capacity_factor"].dropna()
        assert (cf >= 0).all(), "Negative daily capacity factors found"
        fuel = self.gen.set_index("DUID")["FUEL_CATEGORY"]
        kind = daily["duid"].map(fuel).fillna("Other")
        is_hydro = kind == "Hydro"
        for label, mask, bound in (("non-hydro", ~is_hydro, 1.10), ("hydro", is_hydro, 1.25)):
            sub = cf[mask.reindex(cf.index, fill_value=False)]
            offenders = sub[sub > bound]
            assert offenders.empty, (
                f"Daily CF > {bound} for {label}: "
                f"{daily.loc[offenders.index, ['duid', 'date', 'daily_capacity_factor']].to_dict('records')[:5]}"
            )


# ─── Live MLF frame (schema / as-of / values) ────────────────────────────────


class TestMlfLiveFrame:
    """The MLF frame of record is fetch_mlf_data()'s LONG output from the MLF
    Tracker summary CSV (data/mlf_tracker_summary.csv) — the pipeline's current
    input, refreshed with each final-FY publication. The legacy
    data/mlf_history.feather (wide-era leftover, ends FY25-26, written/read by
    nothing in src/) is deliberately not asserted anywhere here."""

    def _live_frame(self):
        return _live_mlf_frame()

    def test_live_mlf_frame_is_long_schema_and_current(self):
        frame = self._live_frame()
        # Long schema: DUID, fy_label, fy_start_year, mlf — one row per DUID×FY.
        assert set(frame.columns) == set(MLF_HISTORY_COLUMNS), (
            f"live MLF frame schema drifted: {list(frame.columns)}"
        )
        assert len(frame) > 1000, f"live MLF frame implausibly small: {len(frame)}"
        assert frame["mlf"].notna().all() and frame["fy_start_year"].notna().all()
        # The review's repro: a wide FY-per-column frame must not pass as "range
        # validated" — validate_mlf_history enforces the long schema exactly.
        assert validate_mlf_history(frame) is not None

    def test_live_mlf_frame_covers_current_final_fy(self):
        """As-of coverage: the frame must include the current FINAL FY.

        AEMO publishes final MLFs each April for the FY starting that July, so
        the newest final FY starts in the current year from April onward (else
        the prior year). The stale leftover feather ends FY25-26 and would fail
        this today; the live tracker CSV carries FY26-27.
        """
        frame = self._live_frame()
        now = datetime.now()
        expected_final_fy = now.year if now.month >= 4 else now.year - 1
        fys = sorted(frame["fy_start_year"].unique())
        assert len(fys) >= 10, f"MLF history too shallow: {fys}"
        current = frame[frame["fy_start_year"] == expected_final_fy]
        assert len(current) >= 400, (
            f"live MLF frame has no/mostly-missing FY{expected_final_fy}-"
            f"{(expected_final_fy + 1) % 100:02d} rows ({len(current)}); "
            f"the tested MLF input is stale (max fy {fys[-1]})"
        )

    def test_live_mlf_wide_schema_is_rejected(self):
        """A wide FY-per-column frame (legacy download_mlf-era shape) must not
        pass MLF validation — that shape is exactly what the old test looped
        over and asserted nothing on."""
        wide = pd.DataFrame(
            {
                "DUID": ["GEN1", "GEN2"],
                "FY24-25": [0.95, 0.90],
                "FY25-26": [0.98, 0.91],
            }
        )
        with pytest.raises(ValueError, match="schema mismatch"):
            validate_mlf_history(wide)


# ─── Freshness (systematic-gap fix: stale data must fail the pipeline) ──────


class TestFreshness:
    def test_monthly_data_is_current(self):
        agg = pd.read_feather(DATA_DIR / "monthly_aggregates.feather")
        # Raises RuntimeError when the latest month is too far behind
        check_monthly_freshness(agg, now=datetime.now())

    def test_daily_data_is_current(self):
        path = DATA_DIR / "daily_aggregates.feather"
        if not path.exists():
            pytest.skip("daily_aggregates.feather not present")
        daily = pd.read_feather(path)
        check_daily_freshness(daily, now=datetime.now())


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
        # Every entry — the old check stopped at idx[:10], a false-green prefix.
        required = {"duid", "region", "fuel_category", "file"}
        for entry in idx:
            missing = required - set(entry.keys())
            assert not missing, f"Index entry {entry.get('duid')} missing: {missing}"

    def test_index_duids_unique(self):
        with open(DOCS_DATA_DIR / "index.json") as f:
            idx = json.load(f)
        duids = [e["duid"] for e in idx]
        assert len(duids) == len(set(duids)), "index.json contains duplicate duids"

    def test_index_file_targets_exist(self):
        """Every index entry's JSON target must exist — missing selected JSON
        fails, it never skips."""
        with open(DOCS_DATA_DIR / "index.json") as f:
            idx = json.load(f)
        missing = [
            f"{e['duid']} -> {e.get('file')}.json"
            for e in idx
            if not (GENERATORS_DIR / f"{e.get('file')}.json").exists()
        ]
        assert not missing, f"index entries with missing JSON files: {missing[:10]}"

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
        """Numeric parity between generator JSON monthly blocks and the feather.

        The old check only verified month MEMBERSHIP for three units and
        ``continue``d past missing JSON files (false green). This compares
        NUMERIC values per month (producer rounding contract, override-corrected
        capacity_factor) over the settled overlap of a deterministic sample, and
        fails when a selected JSON file is missing.
        """
        agg = pd.read_feather(DATA_DIR / "monthly_aggregates.feather")
        with open(DOCS_DATA_DIR / "index.json") as f:
            idx = json.load(f)

        agg_duids = set(agg["duid"])
        data_units = sorted(
            (e for e in idx
             if not e["duid"].startswith("station_")
             and e["duid"] in agg_duids
             and e.get("file")),
            key=lambda e: e["duid"],
        )
        # Deterministic spread across the full index (not the first N files).
        step = max(1, len(data_units) // 60)
        selected = data_units[::step][:60]
        assert len(selected) >= 40, f"Only {len(selected)} data-bearing units sampled"

        _assert_selected_json_present(selected, GENERATORS_DIR)  # fails, never skips

        all_mismatches: list[tuple] = []
        all_coverage: list[tuple] = []
        checked_months = 0
        for entry in selected:
            with open(GENERATORS_DIR / f"{entry['file']}.json") as f:
                data = json.load(f)
            if "monthly" not in data:
                all_coverage.append((entry["duid"], "feather data but no monthly block"))
                continue
            feather_rows = agg[agg["duid"] == entry["duid"]].sort_values("month")
            mm = _json_feather_monthly_mismatches(entry["duid"], data["monthly"], feather_rows)
            all_mismatches.extend((entry["duid"],) + m for m in mm)
            # Coverage: every settled feather month must appear in the JSON.
            settled = _settled_feather_rows(feather_rows)
            json_months = set(data["monthly"].get("months") or [])
            dropped = [m for m in settled if m not in json_months]
            if dropped:
                all_coverage.append((entry["duid"], "months missing from JSON", dropped[:3]))
            checked_months += len(settled)

        assert not all_mismatches, (
            f"JSON/feather numeric mismatches (identical month labels, changed values): "
            f"{all_mismatches[:10]}"
        )
        assert not all_coverage, f"JSON/feather coverage failures: {all_coverage[:10]}"
        assert checked_months >= 1000, (
            f"Only {checked_months} settled month-cells compared — check is vacuous"
        )

    def test_json_feather_changed_value_fails(self):
        """S3-10 negative regression: a JSON numeric change with IDENTICAL month
        labels must be caught by the numeric comparison."""
        # 3 months so the mutable-tail exclusion (last 2) leaves 2024-03 settled.
        feather_rows = pd.DataFrame(
            {
                "duid": ["GENX", "GENX", "GENX"],
                "month": ["2024-03", "2024-04", "2024-05"],
                "generation_mwh": [50.0, 100.0, 200.0],
                "revenue_aud": [2000.0, 5000.0, 9000.0],
                "capacity_factor": [0.3, 0.5, 0.4],
            }
        )
        json_monthly = {
            "months": ["2024-03", "2024-04", "2024-05"],
            "generation_mwh": [50.0, 100.0, 200.0],
            "revenue_aud": [2000.0, 5000.0, 9000.0],
            "capacity_factor": [0.3, 0.5, 0.4],
        }
        assert _json_feather_monthly_mismatches("GENX", json_monthly, feather_rows) == []
        # Same labels, changed numeric value on a SETTLED month -> must report.
        mutated = dict(json_monthly, capacity_factor=[0.9, 0.5, 0.4])
        mismatches = _json_feather_monthly_mismatches("GENX", mutated, feather_rows)
        assert any(m[0] == "2024-03" and m[1] == "capacity_factor" for m in mismatches), (
            f"changed capacity_factor with identical month labels not caught: {mismatches}"
        )

    def test_json_feather_missing_selected_file_fails(self):
        """S3-10 negative regression: a selected data-bearing unit with NO JSON
        file must fail the presence gate (never skip)."""
        agg = pd.read_feather(DATA_DIR / "monthly_aggregates.feather")
        with open(DOCS_DATA_DIR / "index.json") as f:
            idx = json.load(f)
        data_units = sorted(
            (e for e in idx
             if not e["duid"].startswith("station_")
             and e["duid"] in set(agg["duid"])
             and e.get("file")),
            key=lambda e: e["duid"],
        )
        assert data_units, "no data-bearing index units to sample"
        bogus = dict(data_units[0], file="__definitely_missing__")
        with pytest.raises(FileNotFoundError, match="__definitely_missing__"):
            _assert_selected_json_present([bogus], GENERATORS_DIR)


# ─── Curtailment FY rollup (consumed by renewable dashboard) ─────────────────


class TestCurtailmentByFY:
    def test_file_exists_and_non_empty(self):
        path = DOCS_DATA_DIR / "curtailment_by_fy.csv"
        assert path.exists(), "curtailment_by_fy.csv missing"
        df = pd.read_csv(path)
        assert len(df) > 0, "curtailment_by_fy.csv is empty"

    def test_expected_columns_transition(self):
        """S3-01 schema migration: the live CSV is produced by the NAS pipeline,
        so it lags code until the next scheduled run. Accept either the legacy
        schema or the v2 proxy schema, but never both split columns absent AND
        no version marker (i.e. some recognised schema must be present)."""
        df = pd.read_csv(DOCS_DATA_DIR / "curtailment_by_fy.csv")
        legacy = {"duid", "fy_start", "fy_label", "curtailment_pct",
                  "grid_curtailment_pct", "generation_mwh", "months_covered"}
        v2 = {"duid", "fy_start", "fy_label", "curtailment_pct",
              "metric_version", "generation_mwh", "months_covered"}
        assert legacy.issubset(set(df.columns)) or v2.issubset(set(df.columns)), (
            f"curtailment_by_fy.csv matches neither legacy nor v2 schema: {list(df.columns)}"
        )

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
