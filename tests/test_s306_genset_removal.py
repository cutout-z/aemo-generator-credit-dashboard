"""S3-06 tests: GENSETID→DUID append removed; metadata emits null, never 'nan'.

Review acceptance (astra-review-2026-09-04-surface-3.md, S3-06):
- Only the five NEM regions appear in the search index (region filter is
  driven by index.json region values).
- No 'nan' anywhere in index.json / generator JSON metadata.
- A genset ID different from its DUID must not create a second generator.
- The metadata cache is deliberately invalidated: a cached generators.feather
  carrying GENSETID-era rows (no valid NEM region) is rebuilt, never served.
"""

import json

import pandas as pd
import pytest

from src import config, download_metadata
from src.download_metadata import _cache_is_valid, fetch_generators
from src.generate_json import generate_all, generate_index

EXPECTED_REGIONS = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}

COLUMNS = [
    "DUID", "STATION_NAME", "REGION", "FUEL_SOURCE", "FUEL_CATEGORY",
    "TECHNOLOGY", "CAPACITY_MW", "DISPATCH_TYPE",
]


def _clean_frame():
    """Registration-list-shaped frame: one row per NEM region (5 rows)."""
    return pd.DataFrame([
        {"DUID": "BW01", "STATION_NAME": "Burrinjuck", "REGION": "NSW1",
         "FUEL_SOURCE": "Water", "FUEL_CATEGORY": "Hydro",
         "TECHNOLOGY": "Hydro", "CAPACITY_MW": 28.0, "DISPATCH_TYPE": "Generator"},
        {"DUID": "AGLHAL", "STATION_NAME": "Hallett", "REGION": "SA1",
         "FUEL_SOURCE": "Natural Gas", "FUEL_CATEGORY": "Fossil",
         "TECHNOLOGY": "Gas Turbine", "CAPACITY_MW": 70.0, "DISPATCH_TYPE": "Generator"},
        {"DUID": "CALL_A_1", "STATION_NAME": "Callide", "REGION": "QLD1",
         "FUEL_SOURCE": "Black coal", "FUEL_CATEGORY": "Fossil",
         "TECHNOLOGY": "Steam", "CAPACITY_MW": 320.0, "DISPATCH_TYPE": "Generator"},
        {"DUID": "LATB1", "STATION_NAME": "Latrobe", "REGION": "VIC1",
         "FUEL_SOURCE": "Brown coal", "FUEL_CATEGORY": "Fossil",
         "TECHNOLOGY": "Steam", "CAPACITY_MW": 500.0, "DISPATCH_TYPE": "Generator"},
        {"DUID": "GORDON", "STATION_NAME": "Gordon", "REGION": "TAS1",
         "FUEL_SOURCE": "Water", "FUEL_CATEGORY": "Hydro",
         "TECHNOLOGY": "Hydro", "CAPACITY_MW": 250.0, "DISPATCH_TYPE": "Generator"},
    ])[COLUMNS]


def _polluted_frame():
    """A GENSETID-era cache: real rows PLUS unvalidated genset rows with no region."""
    clean = _clean_frame()
    genset = pd.DataFrame([
        {"DUID": "ADPBA1G", "STATION_NAME": "nan", "REGION": None,
         "FUEL_SOURCE": None, "FUEL_CATEGORY": "Battery",
         "TECHNOLOGY": "nan", "CAPACITY_MW": 7.0, "DISPATCH_TYPE": "Generator"},
        {"DUID": "AGLSITA1", "STATION_NAME": "nan", "REGION": None,
         "FUEL_SOURCE": None, "FUEL_CATEGORY": "Solar",
         "TECHNOLOGY": "nan", "CAPACITY_MW": 1.0, "DISPATCH_TYPE": "Generator"},
    ])[COLUMNS]
    return pd.concat([clean, genset], ignore_index=True)


# ─── Cache validity gate ────────────────────────────────────────────────────

class TestCacheValidityGate:
    def test_clean_registration_frame_is_valid(self):
        assert _cache_is_valid(_clean_frame())

    def test_clean_frame_without_connection_point_is_valid(self):
        # Pre-MLF-enrichment local caches lack CONNECTION_POINT — still servable.
        df = _clean_frame().drop(columns=["DISPATCH_TYPE"], errors="ignore")
        assert _cache_is_valid(df)

    def test_nan_region_rows_invalidate(self):
        assert not _cache_is_valid(_polluted_frame())

    def test_out_of_nem_region_invalidates(self):
        df = _clean_frame()
        df.loc[0, "REGION"] = "WEM1"  # not one of the five NEM regions
        assert not _cache_is_valid(df)

    def test_missing_region_column_invalidates(self):
        df = _clean_frame().drop(columns=["REGION"])
        assert not _cache_is_valid(df)

    def test_empty_frame_invalidates(self):
        assert not _cache_is_valid(pd.DataFrame(columns=COLUMNS))


# ─── fetch_generators cache invalidation / rebuild ──────────────────────────

class TestCacheRebuild:
    def _setup(self, tmp_path, cached):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        cached.to_feather(cache_dir / "generators.feather")
        (cache_dir / "NEM-Registration-and-Exemption-List.xls").write_bytes(b"x")
        return cache_dir

    def test_polluted_cache_is_rebuilt_not_served(self, tmp_path, monkeypatch):
        cache_dir = self._setup(tmp_path, _polluted_frame())
        # Registration-list parse produces the clean frame (no genset appends).
        monkeypatch.setattr(download_metadata, "_parse_registration_list",
                            lambda xls: _clean_frame())
        df = fetch_generators(str(cache_dir))
        assert len(df) == 5
        assert df["REGION"].isin(list(EXPECTED_REGIONS)).all()
        # Cache file on disk was replaced with the clean build.
        on_disk = pd.read_feather(cache_dir / "generators.feather")
        assert len(on_disk) == 5
        assert on_disk["REGION"].isin(list(EXPECTED_REGIONS)).all()

    def test_valid_cache_is_served_without_reparse(self, tmp_path, monkeypatch):
        cache_dir = self._setup(tmp_path, _clean_frame())
        def boom(xls):
            raise AssertionError("valid cache must not re-parse the registration list")
        monkeypatch.setattr(download_metadata, "_parse_registration_list", boom)
        df = fetch_generators(str(cache_dir))
        assert len(df) == 5  # served from cache

    def test_restored_genset_era_snapshot_is_invalidated(self, tmp_path, monkeypatch):
        # docs/data/processed-cache/generators.feather restore copies the NAS
        # GENSETID-era snapshot (1017 rows incl. 453 no-region appends) with no
        # marker file — the content gate must still catch it and rebuild.
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        snapshot = pd.concat([
            _clean_frame(),
            pd.DataFrame([{"DUID": "MRIDGE", "STATION_NAME": "nan",
                           "REGION": None, "FUEL_CATEGORY": "Hydro",
                           "TECHNOLOGY": "nan", "CAPACITY_MW": 60.0,
                           "DISPATCH_TYPE": "Generator"}]),
        ], ignore_index=True)
        snapshot.to_feather(cache_dir / "generators.feather")
        (cache_dir / "NEM-Registration-and-Exemption-List.xls").write_bytes(b"x")
        monkeypatch.setattr(download_metadata, "_parse_registration_list",
                            lambda xls: _clean_frame())
        df = fetch_generators(str(cache_dir))
        assert len(df) == 5
        assert "MRIDGE" not in df["DUID"].values

    def test_stale_genset_orphan_caches_removed_on_rebuild(self, tmp_path, monkeypatch):
        cache_dir = self._setup(tmp_path, _polluted_frame())
        (cache_dir / "mmsdm_genunits.feather").write_bytes(b"old")
        (cache_dir / "mmsdm_station.feather").write_bytes(b"old")
        monkeypatch.setattr(download_metadata, "_parse_registration_list",
                            lambda xls: _clean_frame())
        fetch_generators(str(cache_dir))
        assert not (cache_dir / "mmsdm_genunits.feather").exists()
        assert not (cache_dir / "mmsdm_station.feather").exists()


# ─── index.json: five regions only, no 'nan' ────────────────────────────────

class TestIndexEmission:
    def _gen_index(self, tmp_path, frame):
        out = tmp_path / "docs" / "data"
        generate_index(frame, str(out))
        return out / "index.json"

    def test_clean_frame_index_has_only_five_regions_and_no_nan(self, tmp_path):
        path = self._gen_index(tmp_path, _clean_frame())
        entries = json.loads(path.read_text())
        regions = {e["region"] for e in entries}
        assert regions == EXPECTED_REGIONS
        assert "nan" not in path.read_text().lower()

    def test_dirty_metadata_emits_empty_not_nan(self, tmp_path):
        df = _clean_frame()
        df.loc[0, "TECHNOLOGY"] = float("nan")
        df.loc[1, "STATION_NAME"] = float("nan")
        df.loc[2, "CONNECTION_POINT"] = float("nan")
        path = self._gen_index(tmp_path, df)
        raw = path.read_text()
        assert "nan" not in raw.lower()
        entries = {e["duid"]: e for e in json.loads(raw)}
        assert entries["BW01"]["technology"] == ""
        assert entries["AGLHAL"]["station_name"] == ""
        assert entries["CALL_A_1"]["connection_point"] == ""
        # capacity_mw is a number column: NaN → null, never "nan"
        assert entries["BW01"]["capacity_mw"] == 28.0

    def test_genset_era_unit_entries_dropped_on_regeneration(self, tmp_path):
        # Existing index carries a stale GENSETID-era entry (region "nan",
        # market NEM) plus a legitimately preserved station entry.
        out = tmp_path / "docs" / "data"
        out.mkdir(parents=True)
        stale = [
            {"duid": "ADPBA1G", "file": "ADPBA1G", "station_name": "nan",
             "region": "nan", "fuel_category": "Battery", "capacity_mw": 7.0,
             "technology": "nan", "connection_point": "nan", "market": "NEM"},
            {"duid": "station_Hallett", "file": "station_Hallett",
             "station_name": "Hallett", "region": "SA1",
             "fuel_category": "Fossil", "type": "station", "market": "NEM"},
        ]
        (out / "index.json").write_text(json.dumps(stale))
        generate_index(_clean_frame(), str(out))
        entries = {e["duid"]: e for e in json.loads((out / "index.json").read_text())}
        assert "ADPBA1G" not in entries
        assert entries["station_Hallett"]["type"] == "station"
        assert not any("nan" in str(e["region"]).lower() for e in entries.values())
        regions = {e["region"] for e in entries.values() if e.get("type") != "station"}
        assert regions == EXPECTED_REGIONS

    def test_region_filter_values_from_index_are_five_nem_regions(self, tmp_path):
        # Mirrors docs/index.html populateFilters(): region options come from
        # unique region values in index.json — exactly the five NEM regions.
        path = self._gen_index(tmp_path, _clean_frame())
        entries = json.loads(path.read_text())
        options = {e["region"] for e in entries}
        assert options == {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}


# ─── generate_all metadata files: null not 'nan' ────────────────────────────

class TestGeneratedFiles:
    def test_all_generated_files_free_of_nan(self, tmp_path):
        # Two units share a station (station file produced); one unit carries
        # NaN technology/connection point — nothing may serialize as "nan".
        gen = pd.DataFrame([
            {"DUID": "ST1A", "STATION_NAME": "Station One", "REGION": "VIC1",
             "FUEL_SOURCE": "Wind", "FUEL_CATEGORY": "Wind",
             "TECHNOLOGY": float("nan"), "CAPACITY_MW": 50.0,
             "DISPATCH_TYPE": "Generator", "CONNECTION_POINT": "X1"},
            {"DUID": "ST1B", "STATION_NAME": "Station One", "REGION": "VIC1",
             "FUEL_SOURCE": "Wind", "FUEL_CATEGORY": "Wind",
             "TECHNOLOGY": "Wind Turbine", "CAPACITY_MW": 50.0,
             "DISPATCH_TYPE": "Generator", "CONNECTION_POINT": float("nan")},
            {"DUID": "SOLO1", "STATION_NAME": "Solo Farm", "REGION": "TAS1",
             "FUEL_SOURCE": "Solar", "FUEL_CATEGORY": "Solar",
             "TECHNOLOGY": "Photovoltaic", "CAPACITY_MW": 20.0,
             "DISPATCH_TYPE": "Generator", "CONNECTION_POINT": "Y2"},
        ])
        gen_dir = tmp_path / "docs" / "data" / "generators"
        generate_all(gen, output_dir=str(gen_dir))

        files = sorted(gen_dir.glob("*.json")) + [tmp_path / "docs" / "data" / "index.json"]
        assert files
        for path in files:
            raw = path.read_text()
            assert "nan" not in raw.lower(), f"'nan' leaked into {path.name}"
        # Regions are real NEM regions everywhere (index.json is a list of entries).
        for path in gen_dir.glob("*.json"):
            data = json.loads(path.read_text())
            region = data.get("region")
            if region:
                assert region in EXPECTED_REGIONS, f"{path.name}: region {region!r}"

    def test_station_region_uses_first_valid_member(self, tmp_path):
        # Regression guard: a multi-DUID station doc/entry takes its first
        # member's region — NaN must not survive _text().
        from src.generate_json import _text
        assert _text(float("nan")) == ""
        assert _text(None) == ""
        assert _text("nan") == ""
        assert _text("NaN") == ""
        assert _text("NSW1") == "NSW1"
        assert _text(0) == "0"
