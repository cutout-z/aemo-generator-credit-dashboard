"""Generator files the run no longer writes are removed (audit 2026-10, M8).

docs/data/generators held 458 files no index entry points at: 450 GENSETID-era
stubs, TORRB1 and WKIEWA2 (data frozen at 2026-03 / 2026-05) and a West Kiewa
station file frozen at 2026-07.
"""

import json

import pandas as pd

from src.generate_json import generate_all


def _gens():
    return pd.DataFrame({
        "DUID": ["BW01", "BW02"], "STATION_NAME": ["Bayswater", "Bayswater"],
        "REGION": ["NSW1", "NSW1"], "FUEL_CATEGORY": ["Fossil", "Fossil"],
        "CAPACITY_MW": [660.0, 660.0], "TECHNOLOGY": ["Steam", "Steam"],
        "CONNECTION_POINT": ["", ""],
    })


def test_orphans_of_this_market_are_removed_and_others_kept(tmp_path):
    out = tmp_path / "generators"
    out.mkdir()
    (out / "TORRB1.json").write_text(json.dumps({"duid": "TORRB1", "market": "NEM",
                                                "monthly": {"months": ["2026-03"]}}))
    (out / "station_West_Kiewa_Power_Station.json").write_text(json.dumps({"type": "station"}))
    (out / "WEMUNIT.json").write_text(json.dumps({"duid": "WEMUNIT", "market": "WEM"}))
    (out / "broken.json").write_text("{not json")
    generate_all(_gens(), output_dir=str(out))
    names = {p.name for p in out.glob("*.json")}
    assert {"BW01.json", "BW02.json", "station_Bayswater.json"} <= names
    assert "TORRB1.json" not in names
    assert "station_West_Kiewa_Power_Station.json" not in names
    assert "WEMUNIT.json" in names        # another market's file
    assert "broken.json" in names         # unreadable: left for a person


def test_every_index_entry_still_has_its_file(tmp_path):
    out = tmp_path / "generators"
    generate_all(_gens(), output_dir=str(out))
    index = json.loads((tmp_path / "index.json").read_text())
    assert all((out / f"{e['file']}.json").exists() for e in index)
