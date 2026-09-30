"""Regression tests for a blank Region cell inflating the dashboard filter.

Open loop df4d20dd76f90960 — AEMO Credit Dashboard.

Observed: ``docs/data/index.json`` published 630 units across 5 real regions
plus one blank entry (ADPPV3, Adelaide Desalination Plant solar, 0.02 MW — a
registration stub row). The blank was upstream of the index, in the unit
document itself (``"region": ""``), while all four sibling units on the same
station resolved to SA1 and the station entry read SA1. The app bar reads 6
regions because docs/index.html counts the distinct ``region`` values in the
index.

The source row that failed was the Registration List vintage: its Region cell
for ADPPV3 was empty (the HEAD-committed docs/data/processed-cache snapshot
carries ADPPV3 REGION = NaN as the only row outside the five NEM regions, and
the parse had no region guard — only the *cache* gate checked regions). These
tests pin the guard that now recovers the cell from the same-station sibling
units and keeps the filter at five regions.
"""

import json

import pandas as pd

from src import config, download_metadata
from src.download_metadata import (
    _cache_is_valid,
    _parse_registration_list,
    _resolve_missing_regions,
    fetch_generators,
)
from src.generate_json import generate_all, generate_index

EXPECTED_REGIONS = {"NSW1", "QLD1", "VIC1", "SA1", "TAS1"}

COLUMNS = [
    "DUID", "STATION_NAME", "REGION", "FUEL_SOURCE", "FUEL_CATEGORY",
    "TECHNOLOGY", "CAPACITY_MW", "DISPATCH_TYPE",
]


def _adelaide_frame():
    """The ADPPV3 shape: four SA1 siblings plus one blank-region stub unit."""
    rows = [
        {"DUID": "ADPBA1", "STATION_NAME": "Adelaide Desalination Plant",
         "REGION": "SA1", "FUEL_SOURCE": "Battery", "FUEL_CATEGORY": "Battery",
         "TECHNOLOGY": "Storage", "CAPACITY_MW": 7.76,
         "DISPATCH_TYPE": "Generating Unit"},
        {"DUID": "ADPMH1", "STATION_NAME": "Adelaide Desalination Plant",
         "REGION": "SA1", "FUEL_SOURCE": "Water", "FUEL_CATEGORY": "Hydro",
         "TECHNOLOGY": "Hydro", "CAPACITY_MW": 1.44,
         "DISPATCH_TYPE": "Generating Unit"},
        {"DUID": "ADPPV1", "STATION_NAME": "Adelaide Desalination Plant",
         "REGION": "SA1", "FUEL_SOURCE": "Solar", "FUEL_CATEGORY": "Solar",
         "TECHNOLOGY": "Renewable", "CAPACITY_MW": 24.75,
         "DISPATCH_TYPE": "Generating Unit"},
        {"DUID": "ADPPV2", "STATION_NAME": "Adelaide Desalination Plant",
         "REGION": "SA1", "FUEL_SOURCE": "Solar", "FUEL_CATEGORY": "Solar",
         "TECHNOLOGY": "Renewable", "CAPACITY_MW": 0.20,
         "DISPATCH_TYPE": "Generating Unit"},
        # Stub row — AEMO left Region empty for this vintage.
        {"DUID": "ADPPV3", "STATION_NAME": "Adelaide Desalination Plant",
         "REGION": None, "FUEL_SOURCE": "Solar", "FUEL_CATEGORY": "Solar",
         "TECHNOLOGY": "Renewable", "CAPACITY_MW": 0.02,
         "DISPATCH_TYPE": "Generating Unit"},
    ]
    return pd.DataFrame(rows)[COLUMNS]


def _other_region_row():
    return pd.DataFrame([
        {"DUID": "BW01", "STATION_NAME": "Burrinjuck", "REGION": "NSW1",
         "FUEL_SOURCE": "Water", "FUEL_CATEGORY": "Hydro",
         "TECHNOLOGY": "Hydro", "CAPACITY_MW": 28.0,
         "DISPATCH_TYPE": "Generator"},
    ])[COLUMNS]


def _region_values(entries):
    return [e["region"] for e in entries]


# ─── parse-time guard ──────────────────────────────────────────────────────

class TestResolveMissingRegions:
    def test_blank_cell_is_backfilled_from_same_station_siblings(self):
        df = _resolve_missing_regions(_adelaide_frame())
        by_duid = dict(zip(df["DUID"], df["REGION"]))
        assert by_duid["ADPPV3"] == "SA1"
        assert set(df["REGION"]) <= EXPECTED_REGIONS
        assert _cache_is_valid(df)

    def test_blank_region_variants_are_all_recovered(self):
        # AEMO cells arrive as None, NaN or a whitespace string across vintages.
        for blank in (None, float("nan"), "", "  ", "nan"):
            df = _adelaide_frame()
            df.loc[df["DUID"] == "ADPPV3", "REGION"] = blank
            resolved = _resolve_missing_regions(df)
            got = dict(zip(resolved["DUID"], resolved["REGION"]))["ADPPV3"]
            assert got == "SA1", f"blank {blank!r} not recovered (got {got!r})"

    def test_no_sibling_region_is_left_unset_not_guessed(self):
        df = pd.concat([
            _adelaide_frame().drop(index=4),  # keep only the SA1 siblings
            pd.DataFrame([{"DUID": "LONER1", "STATION_NAME": "Unlisted Unit",
                           "REGION": None, "FUEL_SOURCE": "Solar",
                           "FUEL_CATEGORY": "Solar", "TECHNOLOGY": "Renewable",
                           "CAPACITY_MW": 1.0,
                           "DISPATCH_TYPE": "Generating Unit"}])[COLUMNS],
        ], ignore_index=True)
        resolved = _resolve_missing_regions(df)
        got = dict(zip(resolved["DUID"], resolved["REGION"]))["LONER1"]
        assert got is None or pd.isna(got)
        # Unresolved rows must keep failing the cache gate — never be served.
        assert not _cache_is_valid(resolved)

    def test_station_with_conflicting_regions_is_not_backfilled(self):
        df = _adelaide_frame()
        # Same station name, two different regions → ambiguous, no consensus.
        df.loc[3, "REGION"] = "VIC1"
        resolved = _resolve_missing_regions(df)
        got = dict(zip(resolved["DUID"], resolved["REGION"]))["ADPPV3"]
        assert got is None or pd.isna(got)

    def test_present_but_non_nem_region_is_never_overwritten(self):
        df = _adelaide_frame()
        df.loc[df["DUID"] == "ADPPV3", "REGION"] = "WEM1"
        resolved = _resolve_missing_regions(df)
        got = dict(zip(resolved["DUID"], resolved["REGION"]))["ADPPV3"]
        assert got == "WEM1"

    def test_clean_frame_is_unchanged(self):
        df = pd.concat([_other_region_row(), _adelaide_frame()], ignore_index=True)
        clean = pd.concat([_other_region_row(), _adelaide_frame().drop(index=4)],
                          ignore_index=True)
        resolved = _resolve_missing_regions(clean)
        assert list(resolved["REGION"]) == list(clean["REGION"])


def _write_registration_xls(path, frame: pd.DataFrame) -> None:
    """Write a synthetic NEM Registration List sheet from an internal-shape frame."""
    frame = frame.rename(columns={
        "STATION_NAME": "Station Name",
        "REGION": "Region",
        "FUEL_SOURCE": "Fuel Source - Descriptor",
        "TECHNOLOGY": "Technology Type - Descriptor",
        "CAPACITY_MW": "Reg Cap generation (MW)",
        "DISPATCH_TYPE": "Dispatch Type",
    })
    frame.insert(0, "Participant", "South Australian Water Corporation")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name=config.REGISTRATION_SHEET, index=False)


class TestParseWiring:
    def test_parse_registration_list_applies_the_guard(self, tmp_path):
        """The guard must run inside the parse, not only on a cache rebuild."""
        xls = tmp_path / "NEM-Registration-and-Exemption-List.xls"
        _write_registration_xls(xls, _adelaide_frame())

        parsed = _parse_registration_list(xls)
        assert set(parsed["REGION"]) <= EXPECTED_REGIONS
        assert dict(zip(parsed["DUID"], parsed["REGION"]))["ADPPV3"] == "SA1"


class TestFetchGeneratorsBackfill:
    def test_rebuild_from_registration_list_has_no_blank_region(self, tmp_path):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        _write_registration_xls(
            cache_dir / "NEM-Registration-and-Exemption-List.xls", _adelaide_frame()
        )

        df = fetch_generators(str(cache_dir))
        assert dict(zip(df["DUID"], df["REGION"]))["ADPPV3"] == "SA1"
        # The frame written to cache — the one later runs serve — is complete.
        assert _cache_is_valid(pd.read_feather(cache_dir / "generators.feather"))
        assert dict(zip(df["DUID"], df["REGION"]))["ADPPV3"] == "SA1"


# ─── published index: five regions, no blank option ────────────────────────

class TestPublishedIndex:
    def test_index_and_unit_docs_never_carry_a_blank_region(self, tmp_path):
        gen_dir = tmp_path / "docs" / "data" / "generators"
        generate_all(_resolve_missing_regions(_adelaide_frame()),
                     output_dir=str(gen_dir))
        docs = tmp_path / "docs" / "data"

        entries = json.loads((docs / "index.json").read_text())
        regions = set(_region_values(entries))
        assert regions == {"SA1"}
        assert "" not in regions

        # The distinct-region count the app bar renders (docs/index.html
        # updateStats) must be 5 NEM regions, never 5 + blank.
        unit_regions = {e["region"] for e in entries if e.get("type") != "station"}
        assert len(unit_regions) == 1 and unit_regions <= EXPECTED_REGIONS

        unit = json.loads((gen_dir / "ADPPV3.json").read_text())
        assert unit["region"] == "SA1"
        assert "nan" not in (gen_dir / "ADPPV3.json").read_text().lower()

    def test_pre_guard_artifacts_fail_this_contract(self, tmp_path):
        """Guard-rail: the guard is load-bearing — without it the blank ships."""
        gen_dir = tmp_path / "docs" / "data" / "generators"
        generate_all(_adelaide_frame(), output_dir=str(gen_dir))
        entries = json.loads((tmp_path / "docs" / "data" / "index.json").read_text())
        regions = set(_region_values(entries))
        # Regression description: blank region present → distinct count inflated.
        assert "" in regions
        assert len(regions) == 2  # {'SA1', ''} → the "6 regions" bug shape
