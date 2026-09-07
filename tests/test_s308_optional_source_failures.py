"""S3-08 optional-source failure tests: last-known-good retention, the
factor-block continuity (vanish) guard, retained-stale stamping in published
generator docs, and the machine-readable run-status manifest consumed by the
operator channel.

Hermetic: synthetic DataFrames + tmp dirs only — no network, no repo writes
(standing rule 9). Fetchers/builders are monkeypatched so the failure branches
of the real lane helpers run without touching NEMWEB or the repo's data.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src import freshness
from src.generate_json import generate_all
from src.main import _run_optional_factor_lanes
from src.run_status import (
    LaneRun,
    build_manifest,
    check_factor_block_continuity,
    load_run_manifest,
    manifest_alerts,
    validate_manifest,
    write_run_manifest,
    STATUS_DEGRADED,
    STATUS_ERROR,
    STATUS_OK,
    STATUS_SKIPPED,
)

# ────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ────────────────────────────────────────────────────────────────────────

def _gens() -> pd.DataFrame:
    """Two single-DUID stations (no multi-DUID station aggregation)."""
    return pd.DataFrame({
        "DUID": ["GEN1", "GEN2"],
        "STATION_NAME": ["Gen One", "Gen Two"],
        "REGION": ["NSW1", "NSW1"],
        "FUEL_CATEGORY": ["Battery", "Battery"],
        "CAPACITY_MW": [50.0, 100.0],
        "TECHNOLOGY": ["Battery", "Battery"],
        "MARKET": ["NEM", "NEM"],
    })


def _fcas_history(months: list[str], duids=("GEN1", "GEN2")) -> pd.DataFrame:
    rows = []
    for duid in duids:
        for m in months:
            rows.append({
                "duid": duid, "month": m,
                "fcas_services_offered": 8, "fcas_offer_minutes": 240,
                "fcas_participation_pct": 0.5, "fcas_observed_intervals": 100,
                "fcas_positive_intervals": 50, "fcas_source_complete": True,
                "fcas_observed_through": None, "fcas_avg_max_avail_mw": 10.0,
                "fcas_max_max_avail_mw": 20.0,
            })
    return pd.DataFrame(rows)


def _offer_history(months: list[str], duids=("GEN1", "GEN2")) -> pd.DataFrame:
    rows = []
    for duid in duids:
        for m in months:
            rows.append({
                "duid": duid, "month": m, "offered_mw_avg": 5.0,
                "offered_mw_p95": 8.0, "price_band_min_avg": -100.0,
                "price_band_max_avg": 1000.0, "negative_band_day_share": 0.1,
                "top2_band_volume_share": 0.5,
                "vol_intervals_observed": 288, "vol_source_complete": True,
            })
    return pd.DataFrame(rows)


def _curve_history(month: str, duids=("GEN1", "GEN2")) -> pd.DataFrame:
    rows = []
    for duid in duids:
        for band in range(1, 11):
            rows.append({
                "duid": duid, "month": month, "band": band,
                "price": float(band * 10), "cum_mw": float(band * 5),
                "source_complete": True,
            })
    return pd.DataFrame(rows)


def _new_facts(month: str) -> pd.DataFrame:
    """Fresh factor rows for one month (the shape the lane merge consumes)."""
    return _fcas_history([month])


def _months(*pairs: tuple[int, int]) -> list[tuple[int, int]]:
    return list(pairs)


def _write_published_unit(gen_dir, duid: str, blocks: dict) -> None:
    gen_dir.mkdir(parents=True, exist_ok=True)
    doc = {"duid": duid, "station_name": duid}
    for key, value in blocks.items():
        doc[key] = value
    (gen_dir / f"{duid}.json").write_text(json.dumps(doc))


# ────────────────────────────────────────────────────────────────────────
# Optional-factor lanes (Steps 3e/3e2/3e3 — S3-12 month-outer driver)
# ────────────────────────────────────────────────────────────────────────

def _run_factor_lanes(data_dir, months, *, skip_fcas=False, skip_offer=False,
                      full_refresh=False):
    """Drive the S3-12 single-decode factor driver; unpack its 7-tuple."""
    return _run_optional_factor_lanes(
        data_dir, months,
        skip_fcas_factors=skip_fcas, skip_offer_factors=skip_offer,
        full_refresh=full_refresh,
    )


def _fcas_raw(year=2026, month=7, duid="GEN1"):
    """One FCAS offer row for a month (passes download_bids.fcas_bids_from_raw)."""
    row = {
        "INTERVAL_DATETIME": pd.to_datetime([f"{year}-{month:02d}-15 12:00:00"]),
        "DUID": [duid], "BIDTYPE": ["RAISE6SEC"],
        "MAXAVAIL": [5.0], "ENABLEMENTMIN": [0.0], "ENABLEMENTMAX": [5.0],
        "VERSIONNO": [1],
    }
    for i in range(1, 11):
        row[f"BANDAVAIL{i}"] = [0.0]
    return pd.DataFrame(row)


def _energy_raw(year=2026, month=7, duid="GEN1"):
    """One ENERGY volume row for a month (passes energy_volumes_from_raw).

    Stamped 01 00:05 (interval END) → calendar day 01, matching the price day.
    """
    row = {
        "INTERVAL_DATETIME": pd.to_datetime([f"{year}-{month:02d}-01 00:05:00"]),
        "DUID": [duid], "BIDTYPE": ["ENERGY"],
        "MAXAVAIL": [0.0], "ENABLEMENTMIN": [0.0], "ENABLEMENTMAX": [0.0],
        "VERSIONNO": [1],
    }
    for i in range(1, 11):
        row[f"BANDAVAIL{i}"] = [10.0 if i == 1 else 0.0]
    return pd.DataFrame(row)


def _prices_frame(year=2026, month=7, duid="GEN1"):
    """One trading day of BIDDAYOFFER price bands for a month."""
    row = {"DUID": [duid], "SETTLEMENTDATE": pd.to_datetime([f"{year}-{month:02d}-01"])}
    for i in range(1, 11):
        row[f"PRICEBAND{i}"] = [float(i * 10)]
    return pd.DataFrame(row)


class TestFcasLaneRetention:
    def test_total_failure_retains_last_good(self, tmp_path, monkeypatch):
        cache = tmp_path / "fcas_factors.feather"
        _fcas_history(["2026-04", "2026-05", "2026-06", "2026-07"]).to_feather(cache)

        def boom(*a, **k):
            raise RuntimeError("NEMWEB timeout")

        monkeypatch.setattr("src.main.fetch_bidperoffer_union", boom)
        fcas, _offer, _curves, daily, lane, _ol, _cl = _run_factor_lanes(
            tmp_path, _months((2026, 7), (2026, 8)), skip_offer=True,
        )
        # The S3-08 core: a whole-lane failure keeps the last-known-good history.
        assert not fcas.empty
        assert set(fcas["month"]) == {"2026-04", "2026-05", "2026-06", "2026-07"}
        assert len(fcas) == 8  # 2 DUIDs × 4 months — nothing vanished
        assert lane.status == STATUS_DEGRADED
        assert lane.retained is True
        assert lane.attempted_months == 2
        assert lane.months_with_data == 0
        assert "NEMWEB timeout" in (lane.error or "")
        assert lane.asof_month() == "2026-07"
        assert daily is None  # offer lanes skipped — no daily window
        # cache untouched by the failure path
        on_disk = pd.read_feather(cache)
        assert len(on_disk) == 8

    def test_partial_failure_merges_and_degrades(self, tmp_path, monkeypatch):
        cache = tmp_path / "fcas_factors.feather"
        _fcas_history(["2026-05", "2026-07"]).to_feather(cache)

        def fake_fetch(year, month, cache_dir, rebuild=None):
            if (year, month) == (2026, 7):
                raise RuntimeError("archive 500")
            return _fcas_raw(year, month)

        def fake_compute(bids, year, month):
            return _fcas_history([f"{year}-{month:02d}"])  # fresh facts

        monkeypatch.setattr("src.main.fetch_bidperoffer_union", fake_fetch)
        monkeypatch.setattr("src.main.compute_fcas_factors", fake_compute)
        fcas, _offer, _curves, _daily, lane, _ol, _cl = _run_factor_lanes(
            tmp_path, _months((2026, 6), (2026, 7)), skip_offer=True,
        )
        # June computed fresh; July failed → its old cached rows survive.
        assert set(fcas["month"]) == {"2026-05", "2026-06", "2026-07"}
        assert lane.status == STATUS_DEGRADED  # partial failure surfaced, never hidden
        assert lane.retained is False
        assert lane.months_with_data == 1
        assert "archive 500" in (lane.error or "")

    def test_total_failure_no_cache_is_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "src.main.fetch_bidperoffer_union",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")),
        )
        fcas, _offer, _curves, _daily, lane, _ol, _cl = _run_factor_lanes(
            tmp_path, _months((2026, 7), (2026, 8)), skip_offer=True,
        )
        assert fcas.empty
        assert lane.status == STATUS_ERROR
        assert lane.retained is False

    def test_skip_with_cache_retains(self, tmp_path):
        cache = tmp_path / "fcas_factors.feather"
        _fcas_history(["2026-06", "2026-07"]).to_feather(cache)
        fcas, offer, curves, daily, lane, ol, cl = _run_factor_lanes(
            tmp_path, _months((2026, 7), (2026, 8)),
            skip_fcas=True, skip_offer=True,
        )
        assert set(fcas["month"]) == {"2026-06", "2026-07"}
        assert lane.status == STATUS_SKIPPED
        assert lane.retained is True
        # offer lanes skipped too — no caches, nothing retained, no daily window
        assert offer.empty and curves.empty and daily is None
        assert ol.status == STATUS_SKIPPED and ol.retained is False
        assert cl.status == STATUS_SKIPPED and cl.retained is False

    def test_all_months_empty_retains(self, tmp_path, monkeypatch):
        cache = tmp_path / "fcas_factors.feather"
        _fcas_history(["2026-06", "2026-07"]).to_feather(cache)
        monkeypatch.setattr(
            "src.main.fetch_bidperoffer_union",
            lambda *a, **k: pd.DataFrame(),  # source reachable but no rows
        )
        fcas, _offer, _curves, _daily, lane, _ol, _cl = _run_factor_lanes(
            tmp_path, _months((2026, 7), (2026, 8)), skip_offer=True,
        )
        assert not fcas.empty  # retained — absence never becomes "zero"
        assert lane.status == STATUS_DEGRADED
        assert lane.retained is True
        assert lane.asof_month() == "2026-07"


# ────────────────────────────────────────────────────────────────────────
# Lane retention: offer factors / offer curves (Steps 3e2/3e3)
# ────────────────────────────────────────────────────────────────────────

class TestOfferLanesRetention:
    def test_offer_factor_compute_failure_retains(self, tmp_path, monkeypatch):
        cache = tmp_path / "offer_factors.feather"
        _offer_history(["2026-07"]).to_feather(cache)
        monkeypatch.setattr(
            "src.main.fetch_bidperoffer_union",
            lambda year, month, cache_dir, rebuild=None: _energy_raw(year, month),
        )
        monkeypatch.setattr(
            "src.main.fetch_energy_prices",
            lambda year, month, cache_dir, rebuild=False: _prices_frame(year, month),
        )
        monkeypatch.setattr(
            "src.main.compute_offer_features",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("offer down")),
        )
        _fcas, offer, _curves, daily, _fl, ol, cl = _run_factor_lanes(
            tmp_path, _months((2026, 7), (2026, 8)), skip_fcas=True,
        )
        assert not offer.empty
        assert set(offer["month"]) == {"2026-07"}
        assert ol.status == STATUS_DEGRADED
        assert ol.retained is True
        assert "offer down" in (ol.error or "")
        # the curve lane is independent: it computed fresh from the same frames
        assert cl.status == STATUS_OK
        assert cl.months_with_data == 2
        assert daily is not None

    def test_offer_curve_failure_retains_monthly_no_daily(self, tmp_path, monkeypatch):
        cache = tmp_path / "offer_curves.feather"
        _curve_history("2026-07").to_feather(cache)
        monkeypatch.setattr(
            "src.main.fetch_bidperoffer_union",
            lambda year, month, cache_dir, rebuild=None: _energy_raw(year, month),
        )
        monkeypatch.setattr(
            "src.main.fetch_energy_prices",
            lambda year, month, cache_dir, rebuild=False: _prices_frame(year, month),
        )
        monkeypatch.setattr(
            "src.main.compute_offer_curves",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("curves down")),
        )
        _fcas, _offer, curves, daily, _fl, ol, cl = _run_factor_lanes(
            tmp_path, _months((2026, 7), (2026, 8)), skip_fcas=True,
        )
        assert not curves.empty
        assert set(curves["month"]) == {"2026-07"}
        assert daily is None  # failed month must not advance the daily window
        assert cl.status == STATUS_DEGRADED
        assert cl.retained is True
        assert "curves down" in (cl.error or "")
        # offer-factor lane unaffected by the curve-lane failure
        assert ol.status == STATUS_OK
        assert ol.months_with_data == 2

    def test_offer_curve_skip_no_cache_empty(self, tmp_path):
        fcas, offer, curves, daily, fl, ol, cl = _run_factor_lanes(
            tmp_path, _months((2026, 7), (2026, 8)),
            skip_fcas=True, skip_offer=True,
        )
        assert fcas.empty and offer.empty and curves.empty and daily is None
        assert fl.status == STATUS_SKIPPED and fl.retained is False
        assert ol.status == STATUS_SKIPPED and ol.retained is False
        assert cl.status == STATUS_SKIPPED and cl.retained is False


# ────────────────────────────────────────────────────────────────────────
# Factor-block continuity guard (reject vanishing populated metrics)
# ────────────────────────────────────────────────────────────────────────

POPULATED = {
    "fcas_participation": {"month": "2026-07", "services_offered": 8},
    "offers": {"month": "2026-07", "avg_offered_mw": 5.0},
    "offer_curve": {"month": "2026-07", "bands": [{"band": 1}]},
}


class TestContinuityGuard:
    def test_degraded_empty_frame_rejects_vanishing_blocks(self, tmp_path):
        _write_published_unit(tmp_path, "GEN1", POPULATED)
        lanes = {
            "fcas_factors": LaneRun("fcas_factors", "fcas_participation",
                                    status=STATUS_DEGRADED, frame=pd.DataFrame()),
            "offer_factors": LaneRun("offer_factors", "offers",
                                     status=STATUS_DEGRADED, frame=pd.DataFrame()),
            "offer_curves": LaneRun("offer_curves", "offer_curve",
                                    status=STATUS_ERROR, frame=pd.DataFrame()),
        }
        violations = check_factor_block_continuity(tmp_path, lanes)
        assert len(violations) == 3
        joined = " | ".join(violations)
        assert "fcas_participation" in joined and "GEN1" in joined
        assert "offers" in joined and "offer_curve" in joined
        assert "refusing" in violations[0] or "would erase" in violations[0]

    def test_ok_empty_frame_allows_absence(self, tmp_path):
        # A lane whose source ran cleanly attests absence (observed-zero /
        # not-applicable): absence is the source's verdict, not a vanish.
        _write_published_unit(tmp_path, "GEN1", POPULATED)
        lanes = {
            "fcas_factors": LaneRun("fcas_factors", "fcas_participation",
                                    status=STATUS_OK, frame=pd.DataFrame()),
        }
        assert check_factor_block_continuity(tmp_path, lanes) == []

    def test_degraded_with_retained_rows_allows(self, tmp_path):
        _write_published_unit(tmp_path, "GEN1", POPULATED)
        retained = _fcas_history(["2026-07"])  # GEN1 + GEN2 rows present
        lanes = {
            "fcas_factors": LaneRun("fcas_factors", "fcas_participation",
                                    status=STATUS_DEGRADED, retained=True,
                                    frame=retained),
        }
        assert check_factor_block_continuity(tmp_path, lanes) == []

    def test_unpopulated_old_file_allows(self, tmp_path):
        _write_published_unit(tmp_path, "GEN1", {})  # metadata-only doc
        lanes = {
            "fcas_factors": LaneRun("fcas_factors", "fcas_participation",
                                    status=STATUS_ERROR, frame=pd.DataFrame()),
        }
        assert check_factor_block_continuity(tmp_path, lanes) == []

    def test_guard_is_per_block(self, tmp_path):
        # fcas lane errored with nothing to retain → only fcas_participation is
        # threatened; offers/offer_curve still have retained frames.
        _write_published_unit(tmp_path, "GEN1", POPULATED)
        lanes = {
            "fcas_factors": LaneRun("fcas_factors", "fcas_participation",
                                    status=STATUS_ERROR, frame=pd.DataFrame()),
            "offer_factors": LaneRun("offer_factors", "offers",
                                     status=STATUS_DEGRADED, retained=True,
                                     frame=_offer_history(["2026-07"])),
            "offer_curves": LaneRun("offer_curves", "offer_curve",
                                    status=STATUS_DEGRADED, retained=True,
                                    frame=_curve_history("2026-07")),
        }
        violations = check_factor_block_continuity(tmp_path, lanes)
        assert len(violations) == 1
        assert "fcas_participation" in violations[0]

    def test_station_and_missing_duid_files_ignored(self, tmp_path):
        _write_published_unit(tmp_path, "GEN1", POPULATED)
        (tmp_path / "station_multi.json").write_text(json.dumps({
            "type": "station", "duids": ["GEN1"], "fcas_participation": POPULATED["fcas_participation"],
        }))
        lanes = {
            "fcas_factors": LaneRun("fcas_factors", "fcas_participation",
                                    status=STATUS_ERROR, frame=pd.DataFrame()),
        }
        violations = check_factor_block_continuity(tmp_path, lanes)
        assert len(violations) == 1  # only the GEN1 unit file counts
        assert "GEN1" in violations[0]

    def test_documents_previously_populated_units_from_sanitized_filename(self, tmp_path):
        # DUIDs may contain '/' / '#' — the guard resolves via the doc's duid
        # field, never the filename.
        (tmp_path / "W_HOE_1.json").write_text(json.dumps({
            "duid": "W/HOE#1", "fcas_participation": {"month": "2026-07"},
        }))
        lanes = {
            "fcas_factors": LaneRun("fcas_factors", "fcas_participation",
                                    status=STATUS_ERROR, frame=pd.DataFrame()),
        }
        violations = check_factor_block_continuity(tmp_path, lanes)
        assert len(violations) == 1 and "W/HOE#1" in violations[0]


# ────────────────────────────────────────────────────────────────────────
# Retained-stale stamping in published generator docs
# ────────────────────────────────────────────────────────────────────────

class TestRetainedStaleStamp:
    def _run_generate(self, tmp_path, lane_states: dict | None = None) -> dict:
        gen_dir = tmp_path / "docs" / "data" / "generators"
        generate_all(
            _gens(),
            fcas_factors=_fcas_history(["2026-04"]),
            offer_factors=_offer_history(["2026-04"]),
            offer_curves=_curve_history("2026-04"),
            factor_source_status=lane_states,
            output_dir=str(gen_dir),
        )
        return json.loads((gen_dir / "GEN1.json").read_text())

    def test_retained_lanes_stamped(self, tmp_path):
        doc = self._run_generate(tmp_path, {
            "fcas_factors": {"status": STATUS_DEGRADED, "retained": True},
            "offer_factors": {"status": STATUS_OK, "retained": False},
            "offer_curves": {"status": STATUS_DEGRADED, "retained": True},
        })
        assert doc["fcas_participation"]["source_status"] == "retained_stale"
        assert doc["fcas_participation"]["month"] == "2026-04"  # as-of kept
        assert "source_status" not in doc["offers"]  # fresh lane — no stamp
        assert doc["offer_curve"]["source_status"] == "retained_stale"

    def test_absent_lane_states_stamp_nothing(self, tmp_path):
        doc = self._run_generate(tmp_path, None)
        assert "source_status" not in doc["fcas_participation"]
        assert "source_status" not in doc["offer_curve"]

    def test_ok_fresh_lane_no_stamp(self, tmp_path):
        doc = self._run_generate(tmp_path, {
            "fcas_factors": {"status": STATUS_OK, "retained": False},
        })
        assert "source_status" not in doc["fcas_participation"]


# ────────────────────────────────────────────────────────────────────────
# Run-status manifest + operator-channel consumption
# ────────────────────────────────────────────────────────────────────────

class TestRunStatusManifest:
    def _ok_lane(self):
        lane = LaneRun("fcas_factors", "fcas_participation", status=STATUS_OK,
                       frame=_fcas_history(["2026-07"]), attempted_months=2,
                       months_with_data=1)
        return lane

    def test_ok_run_roundtrip(self, tmp_path):
        lane = self._ok_lane()
        manifest = build_manifest({"fcas_factors": lane}, guards={"monthly_freshness": "pass"})
        assert manifest["run"]["result"] == STATUS_OK
        assert validate_manifest(manifest) == []
        path = write_run_manifest(manifest, tmp_path)
        loaded = load_run_manifest(path)
        assert loaded is not None
        assert loaded["sources"]["fcas_factors"]["status"] == STATUS_OK
        assert loaded["sources"]["fcas_factors"]["asof_month"] == "2026-07"
        assert loaded["sources"]["fcas_factors"]["attempted_months"] == 2

    def test_degraded_retained_run_result_and_detail(self):
        lane = LaneRun("offer_factors", "offers", status=STATUS_DEGRADED,
                       retained=True, error="NEMWEB 500", attempted_months=2,
                       frame=_offer_history(["2026-06"]))
        manifest = build_manifest({"offer_factors": lane})
        assert manifest["run"]["result"] == STATUS_DEGRADED
        assert any("offer_factors" in p and "degraded" in p for p in manifest["run"]["problems"])
        rec = manifest["sources"]["offer_factors"]
        assert rec["retained_last_good"] is True
        assert rec["asof_month"] == "2026-06"
        assert rec["error"] == "NEMWEB 500"

    def test_invalid_manifest_rejected(self):
        bad = {"run": {"result": "shiny"}, "sources": {"x": {"status": "weird"}}}
        assert validate_manifest(bad)
        with pytest.raises(ValueError, match="invalid run-status manifest"):
            write_run_manifest(bad, "/tmp/nowhere-c20")

    def test_manifest_alerts_degraded_and_error(self, tmp_path):
        degraded = LaneRun("fcas_factors", "fcas_participation", status=STATUS_DEGRADED,
                           retained=True, error="timeout", frame=_fcas_history(["2026-06"]))
        errored = LaneRun("offer_factors", "offers", status=STATUS_ERROR,
                          error="cold start no cache")
        path = write_run_manifest(
            build_manifest({"fcas_factors": degraded, "offer_factors": errored}),
            tmp_path,
        )
        alerts = manifest_alerts(path)
        assert any("fcas_factors degraded" in a and "retaining last-known-good through 2026-06" in a
                   for a in alerts)
        assert any("offer_factors error" in a and "cold start no cache" in a for a in alerts)

    def test_manifest_alerts_silent_when_healthy_or_absent(self, tmp_path):
        path = write_run_manifest(build_manifest({"fcas_factors": self._ok_lane()}), tmp_path)
        assert manifest_alerts(path) == []
        assert manifest_alerts(tmp_path / "missing.json") == []

    def test_skipped_lane_visible_but_not_alerted(self, tmp_path):
        lane = LaneRun("offer_curves", "offer_curve", status=STATUS_SKIPPED,
                       retained=True, frame=_curve_history("2026-07"))
        path = write_run_manifest(build_manifest({"offer_curves": lane}), tmp_path)
        loaded = load_run_manifest(path)
        assert loaded is not None
        assert loaded["sources"]["offer_curves"]["status"] == STATUS_SKIPPED
        assert manifest_alerts(path) == []  # explicit operator intent


class TestOperatorChannelConsumption:
    def _processed_cache(self, tmp_path):
        """docs/data layout: processed-cache/feathers + sibling run_status.json."""
        cache_dir = tmp_path / "processed-cache"
        cache_dir.mkdir()
        pd.DataFrame({"duid": ["X"], "month": ["2026-07"]}).to_feather(
            cache_dir / "monthly_aggregates.feather")
        pd.DataFrame({"duid": ["X"], "date": ["2026-07-31"]}).to_feather(
            cache_dir / "daily_aggregates.feather")
        return cache_dir

    def test_mac_side_alerts_on_manifest_degradation(self, tmp_path):
        cache_dir = self._processed_cache(tmp_path)
        lane = LaneRun("fcas_factors", "fcas_participation", status=STATUS_DEGRADED,
                       retained=True, error="NEMWEB timeout",
                       frame=_fcas_history(["2026-06"]))
        write_run_manifest(build_manifest({"fcas_factors": lane}), tmp_path)
        alerts = freshness.mac_side_staleness_check(str(cache_dir), now=pd.Timestamp("2026-09-01").to_pydatetime())
        assert any("AEMO ALERT: source fcas_factors degraded" in a for a in alerts)

    def test_mac_side_healthy_manifest_no_new_alerts(self, tmp_path):
        cache_dir = self._processed_cache(tmp_path)
        lane = LaneRun("fcas_factors", "fcas_participation", status=STATUS_OK,
                       frame=_fcas_history(["2026-07"]))
        write_run_manifest(build_manifest({"fcas_factors": lane}), tmp_path)
        alerts = freshness.mac_side_staleness_check(str(cache_dir), now=pd.Timestamp("2026-09-01").to_pydatetime())
        assert alerts == []
