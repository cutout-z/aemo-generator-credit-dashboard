"""Offer DIRECTION selection for bidirectional units (batteries).

Since IESS (June 2024) a battery offers ENERGY and regulation FCAS twice per
interval in BIDPEROFFER_D, once with DIRECTION "GEN" (discharge) and once
with "LOAD" (charge); BIDDAYOFFER_D price bands differ by DIRECTION too, and
contingency FCAS carries a single "BIDIRECTIONAL" row. The pipeline used to
drop DIRECTION and dedupe on (DUID, interval) keeping the LAST row, so a
battery's ENERGY volumes and prices were whichever direction came last in
file order, and its regulation FCAS offers averaged both directions.

These tests pin the rule in src.download_bids.offer_direction_mask: the
dashboard measures the generating side, so ENERGY and regulation FCAS keep
GEN only, contingency FCAS keeps GEN or BIDIRECTIONAL, LOAD is never kept,
and a frame without DIRECTION (pre-IESS archives) keeps every row as GEN.

Hermetic: synthetic frames and parquets; nemosis is stubbed.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src import download_bids as db
from src import offer_curves as oc
from src.fcas_factor import compute_fcas_factors

BANDS = [f"BANDAVAIL{i}" for i in range(1, 11)]
PRICES = [f"PRICEBAND{i}" for i in range(1, 11)]
# Two intervals, end-stamped inside 2026-09-01's trading day.
TS = pd.to_datetime(["2026-09-01 04:05:00", "2026-09-01 04:10:00"])


def _offer(duid, bidtype, direction, t, band1, maxavail=0):
    row = {
        "INTERVAL_DATETIME": t, "DUID": duid, "BIDTYPE": bidtype,
        "MAXAVAIL": maxavail, "ENABLEMENTMIN": 0.0, "ENABLEMENTMAX": 100.0,
    }
    if direction is not None:
        row["DIRECTION"] = direction
    for i, band in enumerate(BANDS, 1):
        row[band] = band1 if i == 1 else 0
    return row


def _battery_raw(load_last: bool = True, with_direction: bool = True) -> pd.DataFrame:
    """A raw BIDPEROFFER_D union frame: battery BAT1 (GEN 50 MW, LOAD 30 MW
    per interval) plus an ordinary generator GEN1 (GEN 80 MW)."""
    rows = []
    for t in TS:
        energy = [("GEN", 50), ("LOAD", 30)]
        if not load_last:
            energy.reverse()
        for direction, mw in energy:
            rows.append(_offer("BAT1", "ENERGY", direction if with_direction else None, t, mw))
        rows.append(_offer("GEN1", "ENERGY", "GEN" if with_direction else None, t, 80))
    frame = pd.DataFrame(rows)
    return frame


def _fcas_raw() -> pd.DataFrame:
    rows = []
    for t in TS:
        rows.append(_offer("BAT1", "RAISEREG", "GEN", t, 0, maxavail=20))
        rows.append(_offer("BAT1", "RAISEREG", "LOAD", t, 0, maxavail=60))
        rows.append(_offer("BAT1", "RAISE6SEC", "BIDIRECTIONAL", t, 0, maxavail=40))
        rows.append(_offer("GEN1", "RAISE6SEC", "GEN", t, 0, maxavail=10))
        rows.append(_offer("LOAD1", "RAISE6SEC", "LOAD", t, 0, maxavail=5))
    return pd.DataFrame(rows)


def _battery_prices(load_last: bool = True, with_direction: bool = True) -> pd.DataFrame:
    """BIDDAYOFFER_D as nemosis returns it (strings): BAT1 GEN bands start at
    $-50, LOAD bands at $-1000; same VERSIONNO, so only order separates them."""
    rows = []
    sides = [("GEN", -50.0), ("LOAD", -1000.0)]
    if not load_last:
        sides.reverse()
    for direction, band1 in sides:
        row = {
            "SETTLEMENTDATE": "2026/09/01 00:00:00", "DUID": "BAT1", "BIDTYPE": "ENERGY",
            "OFFERDATE": "2026/08/31 12:00:00", "VERSIONNO": "1",
        }
        if with_direction:
            row["DIRECTION"] = direction
        for i, col in enumerate(PRICES, 1):
            row[col] = str(band1 if i == 1 else band1 + 100 * i)
        rows.append(row)
    return pd.DataFrame(rows)


def _stub_compiler(monkeypatch, frame):
    seen = {}

    def fake(**kwargs):
        seen.update(kwargs)
        return frame.copy()

    monkeypatch.setattr("nemosis.dynamic_data_compiler", fake)
    return seen


class TestEnergyVolumes:
    @pytest.mark.parametrize("load_last", [True, False])
    def test_battery_keeps_gen_whatever_the_file_order(self, load_last):
        vols = oc.energy_volumes_from_raw(_battery_raw(load_last=load_last), 2026, 9)
        bat = vols[vols["DUID"] == "BAT1"]
        assert bat["BANDAVAIL1"].tolist() == [50] * len(TS)  # GEN 50 MW, not LOAD 30 MW
        assert set(bat["DIRECTION"]) == {"GEN"}
        assert vols.loc[vols["DUID"] == "GEN1", "BANDAVAIL1"].tolist() == [80] * len(TS)

    def test_no_direction_column_keeps_old_single_direction_rows(self):
        """Pre-IESS archives carry no DIRECTION: every row is the unit's one
        (generating) direction, and the (DUID, interval) dedupe is as before."""
        raw = _battery_raw(with_direction=False)
        assert "DIRECTION" not in raw.columns
        vols = oc.energy_volumes_from_raw(raw, 2026, 9)
        assert "DIRECTION" not in vols.columns
        assert vols.groupby("DUID").size().to_dict() == {"BAT1": len(TS), "GEN1": len(TS)}
        assert vols.loc[vols["DUID"] == "GEN1", "BANDAVAIL1"].tolist() == [80] * len(TS)

    def test_null_direction_in_mixed_vintage_frame_counts_as_gen(self):
        raw = _battery_raw()
        raw.loc[raw["DUID"] == "GEN1", "DIRECTION"] = None
        vols = oc.energy_volumes_from_raw(raw, 2026, 9)
        assert vols.loc[vols["DUID"] == "GEN1", "BANDAVAIL1"].tolist() == [80] * len(TS)

    def test_rule_dedupes_with_direction_in_the_key(self):
        raw = _battery_raw()
        assert db.offer_dedupe_keys(raw, ["DUID", "INTERVAL_DATETIME"]) == [
            "DUID", "INTERVAL_DATETIME", "DIRECTION",
        ]
        assert db.offer_dedupe_keys(raw.drop(columns="DIRECTION"), ["DUID"]) == ["DUID"]


class TestEnergyPrices:
    def test_price_select_requests_direction(self):
        assert "DIRECTION" in oc.PRICE_SELECT

    @pytest.mark.parametrize("load_last", [True, False])
    def test_battery_prices_are_the_gen_bands(self, monkeypatch, tmp_path, load_last):
        seen = _stub_compiler(monkeypatch, _battery_prices(load_last=load_last))
        prices = oc.fetch_energy_prices(2026, 9, str(tmp_path))
        assert prices["PRICEBAND1"].tolist() == [-50.0]  # GEN bands, not LOAD's -1000
        assert prices["DIRECTION"].tolist() == ["GEN"]
        assert "DIRECTION" in seen["select_columns"]

    def test_prices_without_direction_column_still_load(self, monkeypatch, tmp_path):
        _stub_compiler(monkeypatch, _battery_prices(with_direction=False).iloc[[0]])
        prices = oc.fetch_energy_prices(2026, 9, str(tmp_path))
        assert len(prices) == 1 and prices["PRICEBAND1"].iloc[0] == -50.0

    def test_offer_curve_pairs_gen_prices_with_gen_volumes(self, monkeypatch, tmp_path):
        _stub_compiler(monkeypatch, _battery_prices(load_last=True))
        prices = oc.fetch_energy_prices(2026, 9, str(tmp_path))
        vols = oc.energy_volumes_from_raw(_battery_raw(load_last=True), 2026, 9)
        daily = oc.compute_offer_curves_daily(prices, vols, "2026-09")
        bat = daily[daily["duid"] == "BAT1"].sort_values("band")
        assert bat.iloc[0][["band", "price", "cum_mw"]].tolist() == [1, -50.0, 50.0]


class TestFcasBids:
    def test_regulation_gen_only_contingency_bidirectional_never_load(self):
        bids = db.fcas_bids_from_raw(_fcas_raw(), 2026, 9)
        got = bids.groupby(["DUID", "BIDTYPE"])["DIRECTION"].agg(lambda s: sorted(set(s))).to_dict()
        assert got == {
            ("BAT1", "RAISEREG"): ["GEN"],
            ("BAT1", "RAISE6SEC"): ["BIDIRECTIONAL"],
            ("GEN1", "RAISE6SEC"): ["GEN"],
        }

    def test_battery_regulation_offer_is_the_gen_side(self):
        facts = compute_fcas_factors(db.fcas_bids_from_raw(_fcas_raw(), 2026, 9), 2026, 9)
        bat = facts.set_index("duid").loc["BAT1"]
        assert bat["fcas_avg_RAISEREG_mw"] == 20.0  # GEN side; both sides averaged to 40
        assert bat["fcas_avg_RAISE6SEC_mw"] == 40.0

    def test_no_direction_column_keeps_every_fcas_row(self):
        raw = _fcas_raw().drop(columns="DIRECTION")
        raw = raw[raw["DUID"] != "BAT1"]  # pre-IESS: one row per unit/service/interval
        bids = db.fcas_bids_from_raw(raw, 2026, 9)
        assert sorted(bids["DUID"].unique()) == ["GEN1", "LOAD1"]
        assert len(bids) == len(raw)


class TestLowMemoryRead:
    def test_union_requests_direction_but_does_not_require_it(self):
        assert "DIRECTION" in db.BIDPEROFFER_UNION_COLS
        assert "DIRECTION" not in db.BIDPEROFFER_REQUIRED_COLS

    def _write(self, tmp_path, year, month, with_direction):
        cols = (
            ["SETTLEMENTDATE", "DUID", "BIDTYPE"]
            + (["DIRECTION"] if with_direction else [])
            + ["INTERVAL_DATETIME", "OFFERDATE", "MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX"]
            + BANDS
        )
        rows = []
        for t in pd.date_range(f"{year}-{month:02d}-01 04:05", periods=4, freq="5min"):
            for direction, mw in (("GEN", 50), ("LOAD", 30)):
                row = {
                    "SETTLEMENTDATE": f"{year}/{month:02d}/01 00:00:00", "DUID": "BAT1",
                    "BIDTYPE": "ENERGY", "DIRECTION": direction,
                    "INTERVAL_DATETIME": t.strftime("%Y/%m/%d %H:%M:%S"),
                    "OFFERDATE": "2026/01/01 00:00:00", "MAXAVAIL": str(mw),
                    "ENABLEMENTMIN": "0", "ENABLEMENTMAX": "0",
                }
                for i, band in enumerate(BANDS, 1):
                    row[band] = str(mw if i == 1 else 0)
                rows.append(row)
        path = db._nemosis_cache_path(str(tmp_path), "BIDPEROFFER_D", year, month)
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows)[cols].astype("str").to_parquet(path)

    def _read_warm(self, tmp_path, monkeypatch):
        def boom(**kwargs):
            raise AssertionError("nemosis compiler called on a warm cache")

        monkeypatch.setattr("nemosis.dynamic_data_compiler", boom)
        return db.fetch_bidperoffer_union(2026, 9, str(tmp_path), rebuild=None)

    def test_cached_read_carries_direction_as_text(self, tmp_path, monkeypatch):
        for m in (8, 9):
            self._write(tmp_path, 2026, m, with_direction=True)
        raw = self._read_warm(tmp_path, monkeypatch)
        vols = oc.energy_volumes_from_raw(raw, 2026, 9)
        assert vols["BANDAVAIL1"].tolist() == [50] * 4  # GEN, though LOAD is last in the file
        assert raw["DIRECTION"].tolist().count("LOAD") == 4

    def test_cached_read_without_direction_column(self, tmp_path, monkeypatch):
        for m in (8, 9):
            self._write(tmp_path, 2026, m, with_direction=False)
        raw = self._read_warm(tmp_path, monkeypatch)
        assert "DIRECTION" not in raw.columns
        assert len(oc.energy_volumes_from_raw(raw, 2026, 9)) == 4


def test_post_iess_frame_without_direction_warns(caplog):
    import logging
    from src.download_bids import offer_direction_mask
    frame = pd.DataFrame({"DUID": ["B1"], "BIDTYPE": ["ENERGY"],
                          "INTERVAL_DATETIME": pd.to_datetime(["2026-09-01 00:05"])})
    with caplog.at_level(logging.WARNING):
        assert offer_direction_mask(frame) is None
    assert "lack DIRECTION" in caplog.text


def test_pre_iess_frame_without_direction_is_silent(caplog):
    import logging
    from src.download_bids import offer_direction_mask
    frame = pd.DataFrame({"DUID": ["B1"], "BIDTYPE": ["ENERGY"],
                          "INTERVAL_DATETIME": pd.to_datetime(["2023-09-01 00:05"])})
    with caplog.at_level(logging.WARNING):
        assert offer_direction_mask(frame) is None
    assert "lack DIRECTION" not in caplog.text
