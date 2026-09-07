"""S3-07 cache-persistence tests: factor-history snapshot membership, the
single-persistence-owner merge (builders never write the cache), and cold
restore of the factor feathers from the published snapshot alone.

Hermetic: synthetic DataFrames and tmp dirs only — no network, no repo writes
(standing rule 9).
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.factor_cache import merge_month_rows
from src.processed_cache import (
    LATEST_WINDOW_CACHE_FILES,
    SNAPSHOT_FILES,
    publish_processed_cache,
    restore_processed_cache,
)

FACTOR_SNAPSHOT_FILES = (
    "market_factors_daily.feather",
    "fcas_factors.feather",
    "offer_factors.feather",
    "offer_curves.feather",
)


def _factor_row_frame(months: list[str], value_col: float = 1.0) -> pd.DataFrame:
    """A month-keyed factor-history frame shaped like offer/fcas factor rows."""
    rows = []
    for m in months:
        rows.append({"duid": "GEN1", "month": m, "avg": value_col})
        rows.append({"duid": "GEN2", "month": m, "avg": value_col})
    return pd.DataFrame(rows)


def _market_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "region": ["NSW1", "NSW1", "QLD1"],
        "date": ["2025-01-01", "2025-01-02", "2025-01-01"],
        "spread_decile": [80.0, 90.0, 70.0],
    })


def _curve_frame(month: str) -> pd.DataFrame:
    rows = []
    for duid in ("GEN1", "GEN2"):
        for band in range(1, 11):
            rows.append({
                "duid": duid, "month": month, "band": band,
                "price": float(band * 10), "cum_mw": float(band),
            })
    return pd.DataFrame(rows)


class TestMergeMonthRows:
    def test_warm_merge_preserves_out_of_window_history(self, tmp_path):
        """The S3-07 reproduction: warm cache with Jan-2025 history merged with
        July-2026 new rows must keep BOTH — the old self-overwrite left only
        July (the builder clobbered the file, the merge read the clobbered
        window back as 'existing', dropped it, and wrote July twice)."""
        cache = tmp_path / "offer_factors.feather"
        _factor_row_frame(["2025-01"]).to_feather(cache)

        merged = merge_month_rows(
            cache, _factor_row_frame(["2026-07"]), label="offer factor"
        )

        assert set(merged["month"]) == {"2025-01", "2026-07"}
        assert len(merged) == 4  # 2 DUIDs × 2 months
        # The on-disk history equals the returned frame (single owner wrote once).
        on_disk = pd.read_feather(cache)
        assert set(on_disk["month"]) == {"2025-01", "2026-07"}
        assert len(on_disk) == 4

    def test_reprocess_replaces_only_the_recomputed_months(self, tmp_path):
        cache = tmp_path / "offer_factors.feather"
        _factor_row_frame(["2025-01", "2026-07"], value_col=1.0).to_feather(cache)

        merged = merge_month_rows(
            cache, _factor_row_frame(["2026-07"], value_col=9.0), label="offer factor"
        )

        old = merged[merged["month"] == "2025-01"]
        new = merged[merged["month"] == "2026-07"]
        assert len(merged) == 4
        assert (old["avg"] == 1.0).all()          # untouched history
        assert (new["avg"] == 9.0).all()          # recomputed month won
        assert len(new) == 2                      # no stale duplicate of 2026-07

    def test_identical_rerun_is_idempotent(self, tmp_path):
        cache = tmp_path / "offer_factors.feather"
        _factor_row_frame(["2025-01"]).to_feather(cache)
        new = _factor_row_frame(["2026-07"])

        first = merge_month_rows(cache, new, label="offer factor")
        second = merge_month_rows(cache, new, label="offer factor")

        pd.testing.assert_frame_equal(first, second)
        pd.testing.assert_frame_equal(pd.read_feather(cache), second)

    def test_full_refresh_replaces_wholesale(self, tmp_path):
        cache = tmp_path / "offer_factors.feather"
        _factor_row_frame(["2025-01"]).to_feather(cache)

        merged = merge_month_rows(
            cache, _factor_row_frame(["2026-07"]), full_refresh=True, label="offer factor"
        )

        assert set(merged["month"]) == {"2026-07"}
        assert set(pd.read_feather(cache)["month"]) == {"2026-07"}

    def test_empty_new_rows_leave_file_untouched(self, tmp_path):
        cache = tmp_path / "offer_factors.feather"
        _factor_row_frame(["2025-01"]).to_feather(cache)
        before = cache.read_bytes()

        merged = merge_month_rows(cache, pd.DataFrame(), label="offer factor")

        assert merged.empty
        assert cache.read_bytes() == before

    def test_missing_month_column_rejected(self, tmp_path):
        with pytest.raises(ValueError):
            merge_month_rows(
                tmp_path / "x.feather", pd.DataFrame({"duid": ["G1"]}),
                label="offer factor",
            )


def _price_frame() -> pd.DataFrame:
    """Two DUIDs, two days, all 10 price bands (shape of fetch_energy_prices output)."""
    days = pd.to_datetime(["2026-07-01", "2026-07-02"])
    rows = []
    for duid in ("T1", "T2"):
        for day in days:
            row = {"DUID": duid, "SETTLEMENTDATE": day}
            for i in range(1, 11):
                row[f"PRICEBAND{i}"] = float(i * 10) if day.day == 1 else float(-i * 5)
            rows.append(row)
    return pd.DataFrame(rows)


def _volume_frame() -> pd.DataFrame:
    """Two DUIDs, intervals on two days, band volumes only in bands 1/2."""
    stamps = pd.to_datetime([
        "2026-07-01 00:05:00", "2026-07-01 00:10:00",
        "2026-07-02 00:05:00", "2026-07-02 00:10:00",
    ])
    rows = []
    for duid in ("T1", "T2"):
        for idx, stamp in enumerate(stamps):
            row = {"DUID": duid, "INTERVAL_DATETIME": stamp}
            for i in range(1, 11):
                row[f"BANDAVAIL{i}"] = 0.0
            row["BANDAVAIL1"] = 10.0 if idx % 2 == 0 else 20.0
            row["BANDAVAIL2"] = 0.0 if idx % 2 == 0 else 25.0
            rows.append(row)
    return pd.DataFrame(rows)


class TestBuildersArePure:
    """S3-07: builders return facts; they must never write a cache file."""

    def test_build_offer_factors_writes_nothing(self, tmp_path, monkeypatch):
        from src.offer_curves import build_offer_factors

        monkeypatch.setattr(
            "src.offer_curves.fetch_energy_prices",
            lambda *a, **k: _price_frame(),
        )
        monkeypatch.setattr(
            "src.offer_curves.fetch_energy_volumes",
            lambda *a, **k: _volume_frame(),
        )

        out = build_offer_factors([(2026, 7)], str(tmp_path / "cache"))

        assert not out.empty
        assert set(out["duid"]) == {"T1", "T2"}
        # Nothing persisted — no feather anywhere under the run's dirs.
        assert list(tmp_path.rglob("*.feather")) == []

    def test_build_offer_curves_returns_monthly_and_daily_and_writes_nothing(
        self, tmp_path, monkeypatch
    ):
        from src.offer_curves import build_offer_curves

        monkeypatch.setattr(
            "src.offer_curves.fetch_energy_prices",
            lambda *a, **k: _price_frame(),
        )
        monkeypatch.setattr(
            "src.offer_curves.fetch_energy_volumes",
            lambda *a, **k: _volume_frame(),
        )

        monthly, daily = build_offer_curves([(2026, 7)], str(tmp_path / "cache"))

        assert not monthly.empty
        assert set(monthly.columns) >= {"duid", "month", "band", "price", "cum_mw"}
        assert not daily.empty
        assert set(daily.columns) >= {"duid", "date", "band"}
        assert list(tmp_path.rglob("*.feather")) == []
        assert list(tmp_path.rglob("*.json")) == []


class TestSnapshotMembership:
    def test_factor_feathers_in_snapshot_and_cold_restore_alone(self, tmp_path):
        """Cold restore from the published snapshot alone brings back the four
        factor histories (the five factor files minus the declared
        latest-window daily cache)."""
        data_dir = tmp_path / "data"
        docs_data_dir = tmp_path / "docs" / "data"
        data_dir.mkdir(parents=True)

        pd.DataFrame({"duid": ["GEN1"], "month": ["2026-03"]}).to_feather(
            data_dir / "monthly_aggregates.feather"
        )
        _market_frame().to_feather(data_dir / "market_factors_daily.feather")
        _factor_row_frame(["2025-01"]).to_feather(data_dir / "fcas_factors.feather")
        _factor_row_frame(["2025-01"]).to_feather(data_dir / "offer_factors.feather")
        _curve_frame("2025-01").to_feather(data_dir / "offer_curves.feather")

        for name in FACTOR_SNAPSHOT_FILES:
            assert name in SNAPSHOT_FILES, f"{name} must be in the versioned snapshot"

        published = publish_processed_cache(data_dir, docs_data_dir)
        for name in FACTOR_SNAPSHOT_FILES:
            assert name in published

        manifest = (docs_data_dir / "processed-cache" / "manifest.json").read_text()
        for name in FACTOR_SNAPSHOT_FILES:
            assert name in manifest, f"{name} missing from the snapshot manifest"

        # Wipe everything and restore from the snapshot alone.
        cold = tmp_path / "cold"
        restored = restore_processed_cache(cold, docs_data_dir)
        for name in FACTOR_SNAPSHOT_FILES:
            assert name in restored
            assert (cold / name).exists()

        market = pd.read_feather(cold / "market_factors_daily.feather")
        assert set(market["region"]) == {"NSW1", "QLD1"}
        factors = pd.read_feather(cold / "offer_factors.feather")
        assert set(factors["month"]) == {"2025-01"}
        curves = pd.read_feather(cold / "offer_curves.feather")
        assert len(curves[curves["duid"] == "GEN1"]) == 10  # 10 bands restored

    def test_daily_offer_curves_declared_latest_window_not_snapshotted(self, tmp_path):
        data_dir = tmp_path / "data"
        docs_data_dir = tmp_path / "docs" / "data"
        data_dir.mkdir(parents=True)

        pd.DataFrame({"duid": ["GEN1"], "month": ["2026-03"]}).to_feather(
            data_dir / "monthly_aggregates.feather"
        )
        # The bounded per-day window cache must never join the durable snapshot.
        _curve_frame("2026-07").to_feather(data_dir / "offer_curves_daily.feather")

        assert "offer_curves_daily.feather" in LATEST_WINDOW_CACHE_FILES
        assert "offer_curves_daily.feather" not in SNAPSHOT_FILES

        published = publish_processed_cache(data_dir, docs_data_dir)
        manifest = (docs_data_dir / "processed-cache" / "manifest.json").read_text()

        assert "offer_curves_daily.feather" not in published
        assert "offer_curves_daily.feather" not in manifest

        restored = restore_processed_cache(tmp_path / "cold", docs_data_dir)
        assert "offer_curves_daily.feather" not in restored

    def test_restore_never_clobbers_present_local_file(self, tmp_path):
        """Cache authority: local-present wins — restore fills gaps only."""
        data_dir = tmp_path / "data"
        docs_data_dir = tmp_path / "docs" / "data"
        data_dir.mkdir(parents=True)
        _factor_row_frame(["2025-01"], value_col=1.0).to_feather(
            data_dir / "offer_factors.feather"
        )
        publish_processed_cache(data_dir, docs_data_dir)

        # Cold-ish data dir that already holds a NEWER local offer_factors.
        local = tmp_path / "local"
        local.mkdir()
        _factor_row_frame(["2026-07"], value_col=9.0).to_feather(
            local / "offer_factors.feather"
        )

        restore_processed_cache(local, docs_data_dir)

        kept = pd.read_feather(local / "offer_factors.feather")
        assert set(kept["month"]) == {"2026-07"}  # newer local untouched
