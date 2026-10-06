"""The processed-cache snapshot drops the stale mlf_history.feather (L4).

docs/data/processed-cache/mlf_history.feather ended FY25-26: nothing in src/
writes or reads it, but it was copied into every snapshot from the lane's
data/ dir. The live MLF frame comes from mlf_tracker_summary.csv (FY26-27).
"""

import json

import pandas as pd

from src.processed_cache import SNAPSHOT_FILES, publish_processed_cache


def test_stale_mlf_history_is_not_published_and_is_removed(tmp_path):
    data = tmp_path / "data"
    docs = tmp_path / "docs" / "data"
    data.mkdir(parents=True)
    snap = docs / "processed-cache"
    snap.mkdir(parents=True)
    stale = pd.DataFrame({"DUID": ["X"], "fy_label": ["FY25-26"], "fy_start_year": [2025], "mlf": [1.0]})
    stale.to_feather(data / "mlf_history.feather")
    stale.to_feather(snap / "mlf_history.feather")
    (data / "mlf_tracker_summary.csv").write_text("DUID,FY26-27\nX,0.99\n")

    published = publish_processed_cache(data, docs)

    assert "mlf_history.feather" not in SNAPSHOT_FILES
    assert "mlf_history.feather" not in published
    assert not (snap / "mlf_history.feather").exists()
    names = [f["name"] for f in json.loads((snap / "manifest.json").read_text())["files"]]
    assert "mlf_tracker_summary.csv" in names and "mlf_history.feather" not in names
