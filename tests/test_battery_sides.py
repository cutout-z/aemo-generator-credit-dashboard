"""A battery's charging (LOAD) side, published beside its generating side.

Since IESS (June 2024) a bidirectional unit (a battery) offers ENERGY and
regulation FCAS twice per interval in BIDPEROFFER_D, one row with
DIRECTION "GEN" (discharging) and one with "LOAD" (charging); BIDDAYOFFER_D
price bands differ by DIRECTION too. Contingency FCAS carries one
BIDIRECTIONAL row. PR #13 kept the GEN side only. These tests pin the
addition on top of it:

- a battery's LOAD rows become separate ``*_load`` columns / ``load`` keys
  (offer factors, monthly offer curve, daily stacks, regulation FCAS);
- an ordinary generator, and a pre-IESS frame without DIRECTION, get no load
  side at all;
- the generating side is published exactly as before (a regression pin with
  values produced by the code before this change, on the same frames).

Hermetic: synthetic frames; the BIDPEROFFER_D read and nemosis are stubbed.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src import download_bids as db
from src import fcas_factor as ff
from src import offer_curves as oc
from src.main import _run_optional_factor_lanes

BANDS = [f"BANDAVAIL{i}" for i in range(1, 11)]
PRICES = [f"PRICEBAND{i}" for i in range(1, 11)]
# Intervals on the month's first and last trading days (a source-complete month).
TS = pd.to_datetime(["2026-09-01 04:05:00", "2026-09-01 04:10:00", "2026-09-30 12:00:00"])
DAYS = ("2026/09/01 00:00:00", "2026/09/30 00:00:00")

# Per side: band volumes (MW) and price bands ($/MWh).
BAT_GEN_MW = {1: 50, 2: 10}
BAT_LOAD_MW = {1: 30, 3: 5}
GEN1_MW = {1: 80}
BAT_GEN_P1, BAT_LOAD_P1, GEN1_P1 = -50.0, -1000.0, -20.0


def _row(duid, bidtype, direction, t, bands=None, maxavail=0):
    row = {
        "INTERVAL_DATETIME": t, "DUID": duid, "BIDTYPE": bidtype,
        "MAXAVAIL": maxavail, "ENABLEMENTMIN": 0.0, "ENABLEMENTMAX": 100.0,
    }
    if direction is not None:
        row["DIRECTION"] = direction
    for i, band in enumerate(BANDS, 1):
        row[band] = (bands or {}).get(i, 0)
    return row


def raw_month(with_direction: bool = True, battery: bool = True) -> pd.DataFrame:
    """A raw BIDPEROFFER_D union frame for 2026-09: battery BAT1 (ENERGY GEN +
    LOAD, regulation GEN + LOAD, contingency BIDIRECTIONAL) and generator GEN1.
    LOAD rows come last, so a file-order dedupe would pick them."""
    d = (lambda x: x) if with_direction else (lambda x: None)
    rows = []
    for t in TS:
        if battery:
            rows += [
                _row("BAT1", "ENERGY", d("GEN"), t, BAT_GEN_MW),
                _row("BAT1", "ENERGY", d("LOAD"), t, BAT_LOAD_MW),
                _row("BAT1", "RAISEREG", d("GEN"), t, maxavail=20),
                _row("BAT1", "RAISEREG", d("LOAD"), t, maxavail=60),
                _row("BAT1", "LOWERREG", d("GEN"), t, maxavail=15),
                _row("BAT1", "LOWERREG", d("LOAD"), t, maxavail=0),
                _row("BAT1", "RAISE6SEC", d("BIDIRECTIONAL"), t, maxavail=40),
            ]
        rows += [
            _row("GEN1", "ENERGY", d("GEN"), t, GEN1_MW),
            _row("GEN1", "RAISE6SEC", d("GEN"), t, maxavail=10),
        ]
    return pd.DataFrame(rows)


def prices_month(with_direction: bool = True, battery: bool = True) -> pd.DataFrame:
    """BIDDAYOFFER_D as nemosis returns it (strings), LOAD rows last."""
    sides = [("GEN1", "GEN", GEN1_P1)]
    if battery:
        sides = [("BAT1", "GEN", BAT_GEN_P1), ("BAT1", "LOAD", BAT_LOAD_P1)] + sides
    rows = []
    for day in DAYS:
        for duid, direction, p1 in sides:
            row = {
                "SETTLEMENTDATE": day, "DUID": duid, "BIDTYPE": "ENERGY",
                "OFFERDATE": "2026/08/31 12:00:00", "VERSIONNO": "1",
            }
            if with_direction:
                row["DIRECTION"] = direction
            for i, col in enumerate(PRICES, 1):
                row[col] = str(p1 if i == 1 else p1 + 100 * i)
            rows.append(row)
    return pd.DataFrame(rows)


def run_month(tmp_path, monkeypatch, *, with_direction=True, battery=True):
    """The factor-lane driver on one synthetic month. Returns the facts and the
    published blocks: {duid: doc}, {duid: day file}, load day frames."""
    raw = raw_month(with_direction, battery)
    prices = prices_month(with_direction, battery)
    calls = []

    def fake_compiler(**kwargs):
        calls.append(kwargs["table_name"])
        return prices.copy()

    monkeypatch.setattr("src.main.fetch_bidperoffer_union", lambda *a, **k: raw.copy())
    monkeypatch.setattr("nemosis.dynamic_data_compiler", fake_compiler)
    load_daily: list = []
    kwargs = {"load_daily_out": load_daily} if _has_load_seam() else {}
    fcas, offers, curves, daily, *_ = _run_optional_factor_lanes(
        tmp_path, [(2026, 9)], skip_fcas_factors=False, skip_offer_factors=False,
        full_refresh=False, **kwargs,
    )
    docs = {}
    for duid in sorted(set(fcas["duid"])):
        doc: dict = {}
        ff.attach_fcas_factor_doc(doc, fcas[fcas["duid"] == duid])
        oc.attach_offer_factor_doc(doc, offers[offers["duid"] == duid])
        oc.attach_offer_curve_doc(doc, curves[curves["duid"] == duid])
        docs[duid] = json.loads(json.dumps(doc))
    load = pd.concat(load_daily, ignore_index=True) if load_daily else None
    out_dir = tmp_path / "docs"
    if load is not None:
        oc.write_offer_curve_files(daily, str(out_dir), load_curves=load)
    else:
        oc.write_offer_curve_files(daily, str(out_dir))
    files = {
        p.stem: {k: v for k, v in json.loads(p.read_text()).items() if k != "updated"}
        for p in sorted((out_dir / "offer_curves").glob("*.json"))
    }
    return {"fcas": fcas, "offers": offers, "curves": curves, "docs": docs,
            "files": files, "load_daily": load, "compiler_calls": calls}


def _has_load_seam() -> bool:
    import inspect
    return "load_daily_out" in inspect.signature(_run_optional_factor_lanes).parameters


# Keys and columns this change adds; everything else is the generating side.
LOAD_DOC_KEYS = {"fcas_participation": "regulation_by_direction",
                 "offers": "load", "offer_curve": "load_bands"}


def gen_side(docs: dict, files: dict) -> dict:
    """The published generating side: docs and day files minus the load keys."""
    out = {}
    for duid, doc in docs.items():
        out[duid] = {k: {kk: vv for kk, vv in v.items() if kk != LOAD_DOC_KEYS.get(k)}
                     for k, v in doc.items()}
    for duid, f in files.items():
        out[duid]["day_file"] = {k: v for k, v in f.items() if k != "load_days"}
    return out


# ── Regression pin: the generating side exactly as the code before this change
# published it for this frame (captured by running ``run_month`` + ``gen_side``
# on the parent commit). ─────────────────────────────────────────────────────
_NOTE = (
    "BIDPEROFFER_D offer behaviour (latest complete month, or explicit partial "
    "window when none is complete). Offers, not settled revenue or enablement. "
    "FCAS prices in the 'fcas' block are REGIONAL market averages shared by all "
    "generators in the region."
)
GEN_SIDE_PIN = {
    "BAT1": {
        "fcas_participation": {
            "month": "2026-09", "services_offered": 3, "offer_minutes": 15,
            "participation_pct": 0.000347, "avg_max_avail_mw": 25.0,
            "max_max_avail_mw": 40.0, "note": _NOTE, "observed_intervals": 3,
            "positive_intervals": 3, "source_complete": True,
        },
        "offers": {
            "scope": "offer_based_estimate", "month": "2026-09",
            "avg_offered_mw": 60.0, "offered_mw_p95": 60.0,
            "price_band_min_avg": -50.0, "price_band_max_avg": 950.0,
            "negative_band_day_share": 1.0, "top2_band_volume_share": 0.0,
            "source_complete": True, "intervals_observed": 3,
        },
        "offer_curve": {
            "scope": "offer_based_estimate", "month": "2026-09",
            "bands": [
                {"band": 1, "price": -50.0, "cum_mw": 50.0},
                {"band": 2, "price": 150.0, "cum_mw": 60.0},
                {"band": 3, "price": 250.0, "cum_mw": 60.0},
                {"band": 4, "price": 350.0, "cum_mw": 60.0},
                {"band": 5, "price": 450.0, "cum_mw": 60.0},
                {"band": 6, "price": 550.0, "cum_mw": 60.0},
                {"band": 7, "price": 650.0, "cum_mw": 60.0},
                {"band": 8, "price": 750.0, "cum_mw": 60.0},
                {"band": 9, "price": 850.0, "cum_mw": 60.0},
                {"band": 10, "price": 950.0, "cum_mw": 60.0},
            ],
        },
        "day_file": {
            "scope": "offer_based_estimate",
            "days": [
                {"date": "2026-09-01", "stack": [[-50.0, 50.0], [150.0, 60.0]]},
                {"date": "2026-09-30", "stack": [[-50.0, 50.0], [150.0, 60.0]]},
            ],
        },
    },
    "GEN1": {
        "fcas_participation": {
            "month": "2026-09", "services_offered": 1, "offer_minutes": 15,
            "participation_pct": 0.000347, "avg_max_avail_mw": 10.0,
            "max_max_avail_mw": 10.0, "note": _NOTE, "observed_intervals": 3,
            "positive_intervals": 3, "source_complete": True,
        },
        "offers": {
            "scope": "offer_based_estimate", "month": "2026-09",
            "avg_offered_mw": 80.0, "offered_mw_p95": 80.0,
            "price_band_min_avg": -20.0, "price_band_max_avg": 980.0,
            "negative_band_day_share": 1.0, "top2_band_volume_share": 0.0,
            "source_complete": True, "intervals_observed": 3,
        },
        "offer_curve": {
            "scope": "offer_based_estimate", "month": "2026-09",
            "bands": [{"band": i, "price": -20.0 if i == 1 else -20.0 + 100 * i, "cum_mw": 80.0}
                      for i in range(1, 11)],
        },
        "day_file": {
            "scope": "offer_based_estimate",
            "days": [
                {"date": "2026-09-01", "stack": [[-20.0, 80.0]]},
                {"date": "2026-09-30", "stack": [[-20.0, 80.0]]},
            ],
        },
    },
}


class TestBatteryHasBothSides:
    def test_offer_factors_carry_the_load_side(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch)
        bat = r["offers"].set_index("duid").loc["BAT1"]
        assert bat["offered_mw_avg"] == 60.0  # discharging: 50 + 10 MW
        assert bat["offered_mw_avg_load"] == 35.0  # charging: 30 + 5 MW
        assert bat["price_band_min_avg"] == BAT_GEN_P1
        assert bat["price_band_min_avg_load"] == BAT_LOAD_P1
        assert bat["vol_intervals_observed_load"] == len(TS)

    def test_offers_doc_has_a_load_block(self, tmp_path, monkeypatch):
        load = run_month(tmp_path, monkeypatch)["docs"]["BAT1"]["offers"]["load"]
        assert load == {
            "avg_offered_mw": 35.0, "offered_mw_p95": 35.0,
            "price_band_min_avg": -1000.0, "price_band_max_avg": 0.0,
            "negative_band_day_share": 1.0, "top2_band_volume_share": 0.0,
            "intervals_observed": len(TS),
        }

    def test_monthly_curve_has_load_bands(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch)
        bands = r["docs"]["BAT1"]["offer_curve"]["load_bands"]
        assert [b["band"] for b in bands] == list(range(1, 11))
        assert bands[0] == {"band": 1, "price": -1000.0, "cum_mw": 30.0}
        assert bands[2] == {"band": 3, "price": -700.0, "cum_mw": 35.0}
        # the generating side's bands are untouched in the same rows
        assert r["docs"]["BAT1"]["offer_curve"]["bands"][0] == {"band": 1, "price": -50.0, "cum_mw": 50.0}

    def test_day_file_has_load_days(self, tmp_path, monkeypatch):
        f = run_month(tmp_path, monkeypatch)["files"]["BAT1"]
        assert f["load_days"] == [
            {"date": "2026-09-01", "stack": [[-1000.0, 30.0], [-700.0, 35.0]]},
            {"date": "2026-09-30", "stack": [[-1000.0, 30.0], [-700.0, 35.0]]},
        ]
        assert f["days"][0]["stack"][0] == [-50.0, 50.0]

    def test_regulation_fcas_per_direction(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch)
        bat = r["fcas"].set_index("duid").loc["BAT1"]
        assert bat["fcas_avg_RAISEREG_mw"] == 20.0  # GEN, as #13 publishes it
        assert bat["fcas_avg_RAISEREG_load_mw"] == 60.0
        # LOWERREG LOAD offered at zero only: observed, but no positive average
        assert pd.isna(bat.get("fcas_avg_LOWERREG_load_mw"))
        assert bat["fcas_load_observed_intervals"] == len(TS)
        assert r["docs"]["BAT1"]["fcas_participation"]["regulation_by_direction"] == {
            "GEN": {"RAISEREG": 20.0, "LOWERREG": 15.0},
            "LOAD": {"RAISEREG": 60.0, "LOWERREG": None},
            "load_observed_intervals": len(TS),
        }

    def test_contingency_fcas_stays_one_combined_side(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch)
        bat = r["fcas"].set_index("duid").loc["BAT1"]
        assert bat["fcas_avg_RAISE6SEC_mw"] == 40.0
        assert not [c for c in r["fcas"].columns if "6SEC_load" in c]

    def test_one_biddayoffer_decode_serves_both_sides(self, tmp_path, monkeypatch):
        assert run_month(tmp_path, monkeypatch)["compiler_calls"] == ["BIDDAYOFFER_D"]

    def test_load_rows_need_the_load_direction(self):
        raw = raw_month()
        rows = db.load_offer_rows(raw, ["ENERGY"])
        assert set(raw.loc[rows, "DUID"]) == {"BAT1"}
        assert set(raw.loc[rows, "DIRECTION"]) == {"LOAD"}
        vols = oc.energy_load_volumes_from_raw(raw, 2026, 9)
        assert vols["BANDAVAIL1"].tolist() == [30] * len(TS)


class TestGeneratorHasNoLoadSide:
    def test_generator_gets_no_load_fields(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch)
        gen = r["docs"]["GEN1"]
        assert "load" not in gen["offers"]
        assert "load_bands" not in gen["offer_curve"]
        assert "regulation_by_direction" not in gen["fcas_participation"]
        assert "load_days" not in r["files"]["GEN1"]
        assert set(r["load_daily"]["duid"]) == {"BAT1"}
        g = r["offers"].set_index("duid").loc["GEN1"]
        assert pd.isna(g["offered_mw_avg_load"])

    def test_generators_only_month_adds_no_column(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch, battery=False)
        assert not [c for c in r["offers"].columns if c.endswith("_load")]
        assert not [c for c in r["curves"].columns if c.endswith("_load")]
        assert not [c for c in r["fcas"].columns if "load" in c]
        assert r["load_daily"] is None


class TestPreIessFrame:
    def test_no_direction_column_has_no_load_side(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch, with_direction=False, battery=False)
        assert not [c for c in r["offers"].columns if c.endswith("_load")]
        assert not [c for c in r["fcas"].columns if "load" in c]
        assert r["load_daily"] is None
        assert db.load_offer_rows(raw_month(with_direction=False), ["ENERGY"]).sum() == 0
        assert oc.energy_load_volumes_from_raw(raw_month(with_direction=False), 2026, 9).empty
        assert db.fcas_load_bids_from_raw(raw_month(with_direction=False), 2026, 9).empty

    def test_no_direction_column_publishes_the_generator_as_before(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch, with_direction=False, battery=False)
        assert gen_side(r["docs"], r["files"]) == {"GEN1": GEN_SIDE_PIN["GEN1"]}


class TestGenSideUnchanged:
    def test_published_gen_side_matches_the_pin(self, tmp_path, monkeypatch):
        r = run_month(tmp_path, monkeypatch)
        assert gen_side(r["docs"], r["files"]) == GEN_SIDE_PIN

    @pytest.mark.parametrize("frame", ["offers", "curves", "fcas"])
    def test_gen_columns_equal_a_run_without_the_battery_load_rows(self, tmp_path, monkeypatch, frame):
        """Dropping every LOAD row from the input leaves the existing columns
        identical: the load side only ever adds columns."""
        with_load = run_month(tmp_path / "a", monkeypatch)[frame]
        raw, prices = raw_month(), prices_month()
        monkeypatch.setattr("src.main.fetch_bidperoffer_union",
                            lambda *a, **k: raw[raw["DIRECTION"] != "LOAD"].copy())
        monkeypatch.setattr("nemosis.dynamic_data_compiler",
                            lambda **k: prices[prices["DIRECTION"] != "LOAD"].copy())
        out = _run_optional_factor_lanes(
            tmp_path / "b", [(2026, 9)], skip_fcas_factors=False,
            skip_offer_factors=False, full_refresh=False,
        )
        without = {"fcas": out[0], "offers": out[1], "curves": out[2]}[frame]
        pd.testing.assert_frame_equal(with_load[list(without.columns)], without)


class TestHistoryBeforeTheLoadSide:
    """Months cached before this change carry no load values: the doc shows no
    charging side for them, never an older month's figures."""

    def _offer_rows(self):
        base = {"offered_mw_avg": 60.0, "offered_mw_p95": 60.0, "price_band_min_avg": -50.0,
                "price_band_max_avg": 950.0, "negative_band_day_share": 1.0,
                "top2_band_volume_share": 0.0, "vol_source_complete": True,
                "n_days": 30, "vol_intervals_observed": 8640}
        aug = {"duid": "BAT1", "month": "2026-08", **base, "offered_mw_avg_load": 35.0,
               "offered_mw_p95_load": 35.0, "price_band_min_avg_load": -1000.0,
               "price_band_max_avg_load": 0.0, "negative_band_day_share_load": 1.0,
               "top2_band_volume_share_load": 0.0, "vol_intervals_observed_load": 8928}
        sep = {"duid": "BAT1", "month": "2026-09", **base}
        return pd.DataFrame([aug, sep])

    def test_headline_month_without_load_rows_has_no_load_block(self):
        doc: dict = {}
        oc.attach_offer_factor_doc(doc, self._offer_rows())
        assert doc["offers"]["month"] == "2026-09"
        assert "load" not in doc["offers"]

    def test_headline_month_with_load_rows_has_the_block(self):
        doc: dict = {}
        oc.attach_offer_factor_doc(doc, self._offer_rows().iloc[[0]])
        assert doc["offers"]["load"]["avg_offered_mw"] == 35.0
        assert doc["offers"]["load"]["intervals_observed"] == 8928

    def test_curve_rows_without_load_values_have_no_load_bands(self):
        rows = pd.DataFrame([{"duid": "BAT1", "month": "2026-09", "band": i, "price": 10.0 * i,
                              "cum_mw": 5.0, "source_complete": True,
                              "price_load": None, "cum_mw_load": None} for i in range(1, 11)])
        doc: dict = {}
        oc.attach_offer_curve_doc(doc, rows)
        assert "load_bands" not in doc["offer_curve"]
