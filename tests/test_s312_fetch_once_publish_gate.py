"""S3-12 tests — fetch/decode-once-per-month + semantic-diff publish gate.

Covers the D-d plan line in aemo-generator-credit-dashboard:

1. The month-outer factor driver (src.main._run_optional_factor_lanes) makes
   exactly ONE BIDPEROFFER_D and ONE BIDDAYOFFER_D nemosis compiler call per
   processed month across the FCAS / offer-factor / monthly-curve / daily-curve
   consumers (was 4+3), with all metrics derived from the resident frames.
2. Thin cached parquets are healed from their SCHEMA ONLY — pandas never
   decodes a full parquet just to inspect its columns.
3. No-change runs do NOT bump publish stamps: market_daily.json's
   ``updated_utc`` and the per-DUID offer-curve files' ``updated`` are written
   only when the semantic facts changed (semantic-diff publish gate).

Hermetic: patched nemosis compiler + synthetic frames + tmp dirs only — no
network, no repo writes (standing rule 9).
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.main import _run_optional_factor_lanes


def _bidperoffer_raw(year: int, month: int) -> pd.DataFrame:
    """One FCAS row + one ENERGY volume row for the month (union columns).

    The ENERGY volume is stamped 01 00:05 (interval END) → calendar day 01,
    matching the BIDDAYOFFER trading day so daily stacks have an intersection.
    """
    fcas_day = f"{year}-{month:02d}-15"
    vol_day = f"{year}-{month:02d}-01"
    fcas = {
        "INTERVAL_DATETIME": pd.to_datetime([f"{fcas_day} 12:00:00"]),
        "DUID": ["GEN1"], "BIDTYPE": ["RAISE6SEC"],
        "MAXAVAIL": [5.0], "ENABLEMENTMIN": [0.0], "ENABLEMENTMAX": [5.0],
        "VERSIONNO": [1],
    }
    eng = {
        "INTERVAL_DATETIME": pd.to_datetime([f"{vol_day} 00:05:00"]),
        "DUID": ["GEN1"], "BIDTYPE": ["ENERGY"],
        "MAXAVAIL": [0.0], "ENABLEMENTMIN": [0.0], "ENABLEMENTMAX": [0.0],
        "VERSIONNO": [1],
    }
    for i in range(1, 11):
        fcas[f"BANDAVAIL{i}"] = [0.0]
        eng[f"BANDAVAIL{i}"] = [10.0 if i == 1 else 0.0]
    return pd.concat([pd.DataFrame(fcas), pd.DataFrame(eng)], ignore_index=True)


def _biddayoffer_raw(year: int, month: int) -> pd.DataFrame:
    """One BIDDAYOFFER_D trading-day row for the month."""
    row = {
        "DUID": ["GEN1"],
        "SETTLEMENTDATE": pd.to_datetime([f"{year}-{month:02d}-01"]),
        "BIDTYPE": ["ENERGY"], "OFFERDATE": ["2026-01-01 00:00:00"],
        "VERSIONNO": [1],
    }
    for i in range(1, 11):
        row[f"PRICEBAND{i}"] = [float(i * 10)]
    return pd.DataFrame(row)


def _patch_compiler(monkeypatch, calls: list) -> None:
    """Replace nemosis.dynamic_data_compiler with a recorder returning frames."""

    def fake_compiler(
        start_time=None, end_time=None, table_name=None, raw_data_location=None,
        select_columns=None, fformat=None, rebuild=False, **kwargs,
    ):
        calls.append((table_name, start_time[:7], bool(rebuild)))
        if table_name == "BIDPEROFFER_D":
            year, month = int(start_time[:4]), int(start_time[5:7])
            return _bidperoffer_raw(year, month)
        if table_name == "BIDDAYOFFER_D":
            year, month = int(start_time[:4]), int(start_time[5:7])
            return _biddayoffer_raw(year, month)
        return pd.DataFrame()

    monkeypatch.setattr("nemosis.dynamic_data_compiler", fake_compiler)
    # Schema-only cache inspection must never fall back to a full decode.
    monkeypatch.setattr(
        "pandas.read_parquet",
        lambda *a, **k: pytest.fail("full parquet decode used for column check"),
    )


def _market_row(vwap_high: float = 98.0) -> pd.DataFrame:
    return pd.DataFrame([{
        "region": "NSW1", "date": "2026-01-15", "vwap_high": vwap_high,
        "vwap_low": 3.0, "spread_decile": 95.0, "spread_max": 99.0,
        "neg_price_share": 0.0, "price_std": 29.0, "intervals": 100,
        "vwap_high_1h": vwap_high, "vwap_low_1h": 3.0, "spread_1h": 95.0,
        "vwap_high_2h": 96.5, "vwap_low_2h": 4.5, "spread_2h": 92.0,
        "vwap_high_4h": 94.0, "vwap_low_4h": 7.0, "spread_4h": 87.0,
        "vwap_high_8h": 90.0, "vwap_low_8h": 13.0, "spread_8h": 77.0,
    }])


# ────────────────────────────────────────────────────────────────────────
# 1. Fetch/decode once per month (4+3 → 1+1)
# ────────────────────────────────────────────────────────────────────────

class TestFetchOncePerMonth:
    def test_driver_fetches_each_table_once_per_month(self, tmp_path, monkeypatch):
        calls: list = []
        _patch_compiler(monkeypatch, calls)
        months = [(2026, 6), (2026, 7), (2026, 8)]

        (fcas, offer, curves, daily,
         fcas_lane, offer_lane, curve_lane) = _run_optional_factor_lanes(
            tmp_path, months,
            skip_fcas_factors=False, skip_offer_factors=False,
            full_refresh=False,
        )

        bidper = [c for c in calls if c[0] == "BIDPEROFFER_D"]
        bidday = [c for c in calls if c[0] == "BIDDAYOFFER_D"]
        # 1+1 per month across ALL consumers — the S3-12 core claim.
        assert len(bidper) == 3 and len(bidday) == 3
        assert sorted(c[1] for c in bidper) == ["2026/06", "2026/07", "2026/08"]
        assert sorted(c[1] for c in bidday) == ["2026/06", "2026/07", "2026/08"]
        # No thin cache exists → no forced rebuilds on an incremental run.
        assert all(not c[2] for c in calls)
        # Every consumer derived facts from the resident frames.
        assert not fcas.empty and fcas_lane.status == "ok"
        assert not offer.empty and offer_lane.status == "ok"
        assert not curves.empty and curve_lane.status == "ok"
        assert daily is not None and not daily.empty
        assert fcas_lane.months_with_data == 3
        assert offer_lane.months_with_data == 3
        assert curve_lane.months_with_data == 3
        # Each lane's facts were persisted through the single-owner merge.
        assert set(pd.read_feather(tmp_path / "fcas_factors.feather")["month"]) == {
            "2026-06", "2026-07", "2026-08",
        }
        assert set(pd.read_feather(tmp_path / "offer_factors.feather")["month"]) == {
            "2026-06", "2026-07", "2026-08",
        }
        assert set(pd.read_feather(tmp_path / "offer_curves.feather")["month"]) == {
            "2026-06", "2026-07", "2026-08",
        }

    def test_fcas_only_run_fetches_no_biddayoffer(self, tmp_path, monkeypatch):
        calls: list = []
        _patch_compiler(monkeypatch, calls)

        (fcas, _offer, _curves, daily,
         fcas_lane, offer_lane, curve_lane) = _run_optional_factor_lanes(
            tmp_path, [(2026, 7), (2026, 8)],
            skip_fcas_factors=False, skip_offer_factors=True,
            full_refresh=False,
        )

        assert [c[0] for c in calls] == ["BIDPEROFFER_D", "BIDPEROFFER_D"]
        assert not fcas.empty and fcas_lane.status == "ok"
        assert offer_lane.status == "skipped" and curve_lane.status == "skipped"
        assert daily is None

    def test_build_offer_curves_fetches_once_per_month(self, tmp_path, monkeypatch):
        """The pure builder path also fetches each table once per month (its
        monthly and daily outputs share the resident frames)."""
        from src.offer_curves import build_offer_curves

        calls: list = []
        _patch_compiler(monkeypatch, calls)
        monthly, daily = build_offer_curves([(2026, 7), (2026, 8)], str(tmp_path))

        assert [c[0] for c in calls] == [
            "BIDDAYOFFER_D", "BIDPEROFFER_D",
            "BIDDAYOFFER_D", "BIDPEROFFER_D",
        ]
        assert not monthly.empty and not daily.empty


class TestSchemaOnlyCacheInspection:
    def test_thin_cache_healed_by_schema_only_read(self, tmp_path, monkeypatch):
        """A cached parquet missing required columns is rebuilt once — detected
        from the parquet SCHEMA ONLY, never by decoding all rows."""
        from src.download_bids import fetch_bidperoffer_union

        nem = tmp_path / "nemosis_cache"
        nem.mkdir(parents=True)
        # Thin cache: FCAS columns only, no BANDAVAIL bands.
        thin = pd.DataFrame({
            "INTERVAL_DATETIME": pd.to_datetime(["2026-07-15 12:00:00"]),
            "DUID": ["GEN1"], "BIDTYPE": ["RAISE6SEC"],
            "MAXAVAIL": [5.0], "ENABLEMENTMIN": [0.0], "ENABLEMENTMAX": [5.0],
            "VERSIONNO": [1],
        })
        thin.to_parquet(nem / "PUBLIC_ARCHIVE#BIDPEROFFER_D#FILE01#202607010000.parquet")

        seen: dict = {}

        def fake_compiler(**kwargs):
            seen["rebuild"] = kwargs.get("rebuild")
            return _bidperoffer_raw(2026, 7)

        monkeypatch.setattr("nemosis.dynamic_data_compiler", fake_compiler)
        monkeypatch.setattr(
            "pandas.read_parquet",
            lambda *a, **k: pytest.fail("full parquet decode used for column check"),
        )

        raw = fetch_bidperoffer_union(2026, 7, str(tmp_path), rebuild=None)

        assert seen.get("rebuild") is True  # healed once, fat
        assert not raw.empty
        # (The fake compiler does not rewrite the parquet, so the on-disk file
        # stays thin — the heal decision is purely schema-driven, as asserted.)


# ────────────────────────────────────────────────────────────────────────
# 2. Semantic-diff publish gate (no-change runs never bump stamps)
# ────────────────────────────────────────────────────────────────────────

class TestMarketJsonPublishGate:
    def test_unchanged_run_keeps_updated_utc(self, tmp_path):
        from src.generate_market_json import publish_market_json
        from src.market_factors import build_quarterly_summary

        out = tmp_path
        df = _market_row()
        publish_market_json(df, build_quarterly_summary(df), str(out),
                            qed_benchmarks={"2026Q1": 121.0})
        p = out / "market_daily.json"
        first = p.read_bytes()
        first_utc = json.loads(first)["updated_utc"]

        # Re-publishing identical facts at a later clock must not rewrite.
        publish_market_json(df, build_quarterly_summary(df), str(out),
                            qed_benchmarks={"2026Q1": 121.0})
        assert p.read_bytes() == first

        # Even when the on-disk stamp is OLD, identical facts leave it alone.
        seeded = json.loads(first)
        seeded["updated_utc"] = "2020-01-01T00:00:00Z"
        p.write_text(json.dumps(seeded, separators=(",", ":")))
        publish_market_json(df, build_quarterly_summary(df), str(out),
                            qed_benchmarks={"2026Q1": 121.0})
        assert json.loads(p.read_text())["updated_utc"] == "2020-01-01T00:00:00Z"

        # A real fact change rewrites with a fresh stamp.
        changed = _market_row(vwap_high=140.0)
        publish_market_json(changed, build_quarterly_summary(changed), str(out),
                            qed_benchmarks={"2026Q1": 121.0})
        payload = json.loads(p.read_text())
        assert payload["updated_utc"] != "2020-01-01T00:00:00Z"
        assert payload["regions"]["NSW1"]["vwap_high"] == [140.0]

    def test_changed_facts_rewrite(self, tmp_path):
        from src.generate_market_json import publish_market_json
        from src.market_factors import build_quarterly_summary

        out = tmp_path
        publish_market_json(_market_row(98.0), build_quarterly_summary(_market_row(98.0)),
                            str(out), qed_benchmarks={"2026Q1": 121.0})
        publish_market_json(_market_row(55.0), build_quarterly_summary(_market_row(55.0)),
                            str(out), qed_benchmarks={"2026Q1": 121.0})
        payload = json.loads((out / "market_daily.json").read_text())
        assert payload["regions"]["NSW1"]["vwap_high"] == [55.0]


class TestOfferCurveFilesPublishGate:
    def _curves(self) -> pd.DataFrame:
        from src.offer_curves import compute_offer_curves_daily

        prices = pd.DataFrame({
            "DUID": ["T1"], "SETTLEMENTDATE": pd.to_datetime(["2026-07-01"]),
            **{f"PRICEBAND{i}": [float(i)] for i in range(1, 11)},
        })
        vols = {"DUID": ["T1"], "INTERVAL_DATETIME": pd.to_datetime(["2026-07-01 00:05"])}
        for i in range(1, 11):
            vols[f"BANDAVAIL{i}"] = [10.0] if i == 1 else [0.0]
        return compute_offer_curves_daily(prices, pd.DataFrame(vols), "2026-07")

    def test_unchanged_run_keeps_updated_date(self, tmp_path):
        from src.offer_curves import write_offer_curve_files

        curves = self._curves()
        n1 = write_offer_curve_files(curves, str(tmp_path))
        assert n1 == 1
        p = tmp_path / "offer_curves" / "T1.json"
        first = p.read_bytes()

        # Identical stacks again → nothing written, file byte-identical.
        n2 = write_offer_curve_files(curves, str(tmp_path))
        assert n2 == 0
        assert p.read_bytes() == first

        # An OLD on-disk updated date with identical facts is preserved — the
        # S3-12 bug (clock-only churn) is gone.
        seeded = json.loads(first)
        seeded["updated"] = "2026-01-01"
        p.write_text(json.dumps(seeded, separators=(",", ":")))
        n3 = write_offer_curve_files(curves, str(tmp_path))
        assert n3 == 0
        assert json.loads(p.read_text())["updated"] == "2026-01-01"

    def test_changed_stacks_rewrite(self, tmp_path):
        from src.offer_curves import write_offer_curve_files

        curves = self._curves()
        write_offer_curve_files(curves, str(tmp_path))
        # Change the stack price for band 1 → fact change → rewritten.
        curves.loc[0, "price"] = 99.0
        n = write_offer_curve_files(curves, str(tmp_path))
        assert n == 1
        doc = json.loads((tmp_path / "offer_curves" / "T1.json").read_text())
        assert doc["days"][0]["stack"] == [[99.0, 10.0]]
