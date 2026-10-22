from pathlib import Path

import pandas as pd

from src.processed_cache import publish_processed_cache, restore_processed_cache


def test_processed_cache_publish_and_restore(tmp_path: Path):
    data_dir = tmp_path / "data"
    docs_data_dir = tmp_path / "docs" / "data"
    data_dir.mkdir(parents=True)

    pd.DataFrame({"duid": ["GEN1"], "month": ["2026-03"]}).to_feather(
        data_dir / "monthly_aggregates.feather"
    )
    pd.DataFrame({"DUID": ["GEN1"], "total_intervals": [10], "good_intervals": [9]}).to_feather(
        data_dir / "intermittent_quality_2026_03.feather"
    )

    published = publish_processed_cache(data_dir, docs_data_dir)
    assert "monthly_aggregates.feather" in published
    # Retired (code pass 2026-10-20, R3 L): never published, never restored.
    assert "intermittent_quality_2026_03.feather" not in published
    manifest_path = docs_data_dir / "processed-cache" / "manifest.json"
    assert manifest_path.exists()
    first_manifest = manifest_path.read_text()
    publish_processed_cache(data_dir, docs_data_dir)
    assert manifest_path.read_text() == first_manifest

    cold_data_dir = tmp_path / "cold-data"
    restored = restore_processed_cache(cold_data_dir, docs_data_dir)

    assert "monthly_aggregates.feather" in restored
    assert "intermittent_quality_2026_03.feather" not in restored
    restored_monthly = pd.read_feather(cold_data_dir / "monthly_aggregates.feather")
    assert restored_monthly.iloc[0]["duid"] == "GEN1"


def test_retired_intermittent_quality_copies_are_removed(tmp_path: Path):
    """The six frozen intermittent_quality_* snapshots leave docs/data on the next publish."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snap = tmp_path / "docs" / "data" / "processed-cache"
    snap.mkdir(parents=True)
    stale = snap / "intermittent_quality_2026_02.feather"
    stale.write_bytes(b"x")
    publish_processed_cache(data_dir, tmp_path / "docs" / "data")
    assert not stale.exists()
    assert "intermittent_quality" not in (snap / "manifest.json").read_text()
