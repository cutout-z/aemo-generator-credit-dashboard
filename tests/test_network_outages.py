"""Tests for the MMSDM NETWORK_OUTAGEDETAIL monthly outage lane.

Hermetic: synthetic MMS flat-file CSVs (and real zips built from them in tmp
dirs) + tmp dirs only — no network, no repo writes. The HTTP layer is
monkeypatched at the module-function boundary (``network_outages.probe_month`` /
``_request`` / ``download_zip``) so the real discovery, extraction, parse, join,
slice, storage and publish code paths run.
"""

from __future__ import annotations

import json
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src import network_outages as no
from src.network_outages import (
    OUTAGE_TABLE,
    EQUIPMENT_TABLE,
    RATING_TABLE,
    SUBSTATION_TABLE,
    attach_context,
    build_network_outages_payload,
    discover_month,
    load_region_map,
    load_substation_regions,
    load_snapshot,
    load_voltage_map,
    month_url,
    outage_days_in_month,
    parse_mms_timestamp,
    parse_outage_windows,
    prune_raw,
    publish_network_outages_json,
    run_network_outages_lane,
    slice_month,
    support_url,
    voltage_class,
    window_open,
)
from src.run_status import STATUS_DEGRADED, STATUS_ERROR, STATUS_OK

OUTAGE_COLS = [
    "OUTAGEID", "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "STARTTIME",
    "ENDTIME", "SUBMITTEDDATE", "OUTAGESTATUSCODE", "RESUBMITREASON",
    "RESUBMITOUTAGEID", "RECALLTIMEDAY", "RECALLTIMENIGHT", "LASTCHANGED",
    "REASON", "ISSECONDARY", "ACTUAL_STARTTIME", "ACTUAL_ENDTIME",
    "COMPANYREFCODE", "ELEMENTID",
]
EQUIP_COLS = [
    "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "VALIDFROM", "VALIDTO",
    "VOLTAGE", "DESCRIPTION", "LASTCHANGED", "ELEMENTID",
]
RATING_COLS = [
    "SPD_ID", "VALIDFROM", "VALIDTO", "REGIONID", "SUBSTATIONID",
    "EQUIPMENTTYPE", "EQUIPMENTID", "RATINGLEVEL", "ISDYNAMIC", "LASTCHANGED",
]
SUBSTATION_COLS = [
    "SUBSTATIONID", "VALIDFROM", "VALIDTO", "DESCRIPTION", "REGIONID",
    "OWNERID", "LASTCHANGED",
]
GROUP = {"OUTAGEDETAIL": "NETWORK", "EQUIPMENTDETAIL": "NETWORK",
         "RATING": "NETWORK", "SUBSTATIONDETAIL": "NETWORK"}


# ─── Synthetic MMS fixtures ────────────────────────────────────────────────

def _write_mms(path: Path, table: str, columns: list[str], rows: list[dict]) -> Path:
    """Write a real-format MMS flat file: C comment, I header, D data rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    group = GROUP.get(table, "NETWORK")
    lines = [
        f"C,SETP.WORLD,DVD_{group}_{table},AEMO,PUBLIC,2026/09/08,14:14:53,001,MONTHLY_ARCHIVE,001",
        ",".join(["I", group, table, "4", *columns]),
    ]
    for row in rows:
        values = []
        for col in columns:
            value = row.get(col, "")
            value = "" if value is None else str(value)
            values.append(f'"{value}"' if "," in value else value)
        lines.append(",".join(["D", group, table, "4", *values]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _outage_row(outage_id: str, sub: str, eq_type: str, eq_id: str, start: str,
                end: str, status: str = "COMPLETE", **overrides) -> dict:
    row = {
        "OUTAGEID": outage_id,
        "SUBSTATIONID": sub,
        "EQUIPMENTTYPE": eq_type,
        "EQUIPMENTID": eq_id,
        "STARTTIME": start,
        "ENDTIME": end,
        "SUBMITTEDDATE": "2026/07/01 09:00:00",
        "OUTAGESTATUSCODE": status,
        "LASTCHANGED": "2026/07/02 09:00:00",
        "REASON": "Line maintenance, planned",
        "ISSECONDARY": "0",
        "ELEMENTID": "1",
    }
    row.update(overrides)
    return row


def _equipment_row(sub, eq_type, eq_id, voltage, validfrom="2003/04/28 16:54:02.000",
                   description=None, validto="9999/12/31 00:00:00.000") -> dict:
    return {
        "SUBSTATIONID": sub, "EQUIPMENTTYPE": eq_type, "EQUIPMENTID": eq_id,
        "VALIDFROM": validfrom, "VALIDTO": validto, "VOLTAGE": voltage,
        "DESCRIPTION": description or f"{sub} {voltage}kV {eq_type}",
        "LASTCHANGED": validfrom, "ELEMENTID": "1",
    }


def _rating_row(sub, eq_type, eq_id, region, validfrom="2013/05/14 14:15:00") -> dict:
    return {
        "SPD_ID": f"X_{sub}", "VALIDFROM": validfrom, "VALIDTO": "9999/12/31 00:00:00",
        "REGIONID": region, "SUBSTATIONID": sub, "EQUIPMENTTYPE": eq_type,
        "EQUIPMENTID": eq_id, "RATINGLEVEL": "LDSH", "ISDYNAMIC": "1",
        "LASTCHANGED": validfrom,
    }


def _substation_row(sub, region) -> dict:
    return {
        "SUBSTATIONID": sub, "VALIDFROM": "2003/04/28 16:51:08.000",
        "VALIDTO": "9999/12/31 00:00:00.000", "DESCRIPTION": f"{sub} 132kV",
        "REGIONID": region, "OWNERID": "TransGrid", "LASTCHANGED": "2003/04/28 16:51:18.000",
    }


@pytest.fixture
def context_files(tmp_path) -> dict[str, Path]:
    """Equipment / rating / substation context CSVs for a small synthetic NEM."""
    equip = _write_mms(tmp_path / "EQUIPMENTDETAIL.CSV", "EQUIPMENTDETAIL", EQUIP_COLS, [
        _equipment_row("SYD_STH", "TRANS", "5H", 500),
        _equipment_row("SYD_WEST", "CAP", "C2", 220),
        _equipment_row("BALRANLD", "LINE", "X3", 330),
        # Two versions of the same key: the newest VALIDFROM must win.
        _equipment_row("TREVALYN", "LINE", "HA_TR1", 110, validfrom="2010/01/01 00:00:00"),
        _equipment_row("TREVALYN", "LINE", "HA_TR1", 220, validfrom="2020/01/01 00:00:00"),
        # A nonsense/zero voltage is never zero-filled.
        _equipment_row("ZERO_SUB", "LINE", "Z1", 0),
    ])
    rating = _write_mms(tmp_path / "RATING.CSV", "RATING", RATING_COLS, [
        _rating_row("SYD_STH", "TRANS", "5H", "NSW1"),
        # Substation-majority fallback: this equipment key is not rated, but the
        # substation's other equipment is.
        _rating_row("SYD_WEST", "CAP", "OTHER", "NSW1"),
        _rating_row("BALRANLD", "LINE", "X9", "NSW1"),
        _rating_row("TREVALYN", "LINE", "HA_TR1", "TAS1"),
    ])
    substations = _write_mms(tmp_path / "SUBSTATIONDETAIL.CSV", "SUBSTATIONDETAIL",
                             SUBSTATION_COLS, [
        _substation_row("ZERO_SUB", "QLD1"),
        _substation_row("LONELY_SUB", "SA1"),
    ])
    return {"equipment": equip, "rating": rating, "substation": substations}


def _windows_frame(rows: list[dict]) -> pd.DataFrame:
    """Build a parsed-window frame from synthetic rows (bypasses CSV parsing)."""
    return pd.DataFrame([
        {
            "outage_id": r["OUTAGEID"],
            "substation_id": r["SUBSTATIONID"],
            "equipment_type": r["EQUIPMENTTYPE"],
            "equipment_id": r["EQUIPMENTID"],
            "element_id": r.get("ELEMENTID"),
            "status_code": r.get("OUTAGESTATUSCODE", "COMPLETE"),
            "reason": r.get("REASON"),
            "is_secondary": r.get("ISSECONDARY", "0"),
            "start_time": parse_mms_timestamp(r["STARTTIME"]),
            "end_time": parse_mms_timestamp(r.get("ENDTIME")),
            "actual_start_time": None,
            "actual_end_time": None,
            "submitted_date": parse_mms_timestamp(r.get("SUBMITTEDDATE")),
            "last_changed": parse_mms_timestamp(r.get("LASTCHANGED")),
            "standing_window": no._row_standing(parse_mms_timestamp(r["STARTTIME"])),
        }
        for r in rows
    ])


def _context(windows: pd.DataFrame, context_files: dict) -> pd.DataFrame:
    return attach_context(
        windows,
        load_voltage_map(context_files["equipment"]),
        load_region_map(context_files["rating"]),
        load_substation_regions(context_files["substation"]),
    )


# ─── URL + discovery ───────────────────────────────────────────────────────

class TestSourceCoordinates:
    def test_month_url_shape(self):
        url = month_url(2026, 8)
        assert url.endswith("PUBLIC_ARCHIVE%23NETWORK_OUTAGEDETAIL%23FILE01%23202608010000.zip")
        assert "/2026/MMSDM_2026_08/MMSDM_Historical_Data_SQLLoader/DATA/" in url

    def test_support_url_shape(self):
        url = support_url("NETWORK_RATING", 2026, 7)
        assert url.endswith("PUBLIC_ARCHIVE%23NETWORK_RATING%23FILE01%23202607010000.zip")

    def test_discover_month_probes_back_to_latest(self):
        calls = []

        def fake_probe(year, month):
            calls.append((year, month))
            if (year, month) == (2026, 8):
                return {"year": year, "month": month, "url": "u", "content_length": 1,
                        "last_modified": "x"}
            return None

        found = discover_month(date(2026, 9, 19), probe=fake_probe)
        assert (found["year"], found["month"]) == (2026, 8)
        assert calls[0] == (2026, 9)

    def test_discover_month_none_when_nothing_served(self):
        assert discover_month(date(2026, 9, 19), probe=lambda y, m: None) is None

    def test_discover_month_wraps_year_boundary(self):
        calls = []

        def fake_probe(year, month):
            calls.append((year, month))
            return None

        discover_month(date(2026, 2, 5), probe=fake_probe, months_back=3)
        assert calls == [(2026, 2), (2026, 1), (2025, 12), (2025, 11)]


# ─── Parsing ───────────────────────────────────────────────────────────────

class TestParse:
    def test_parses_header_and_typed_values(self, tmp_path):
        path = _write_mms(tmp_path / "OUT.CSV", "OUTAGEDETAIL", OUTAGE_COLS, [
            _outage_row("1", "SYD_STH", "TRANS", "5H",
                        "2026/08/01 07:00:00", "2026/08/05 17:00:00"),
        ])
        df = parse_outage_windows(path)
        assert len(df) == 1
        assert df.loc[0, "reason"] == "Line maintenance, planned"
        assert df.loc[0, "start_time"] == pd.Timestamp("2026-08-01 07:00:00")
        assert df.loc[0, "standing_window"] is False or not df.loc[0, "standing_window"]

    def test_non_outage_tables_ignored(self, tmp_path):
        path = tmp_path / "MIXED.CSV"
        lines = [
            "C,SETP.WORLD,DVD_NETWORK_MIXED,AEMO,PUBLIC,2026/09/08,14:14:53,001,MONTHLY_ARCHIVE,001",
            ",".join(["I", "NETWORK", "OUTAGEDETAIL", "4", *OUTAGE_COLS]),
            ",".join(["I", "NETWORK", "RATING", "1", *RATING_COLS]),
            ",".join(["D", "NETWORK", "OUTAGEDETAIL", "4",
                      *_values(OUTAGE_COLS, _outage_row(
                          "1", "SYD_STH", "TRANS", "5H",
                          "2026/08/01 07:00:00", "2026/08/05 17:00:00"))]),
            ",".join(["D", "NETWORK", "RATING", "1",
                      *_values(RATING_COLS, _rating_row("SYD_STH", "TRANS", "5H", "NSW1"))]),
        ]
        path.write_text("\n".join(lines) + "\n")
        df = parse_outage_windows(path)
        assert list(df["outage_id"]) == ["1"]

    def test_chunking_is_equivalent(self, tmp_path):
        rows = [
            _outage_row(str(i), "SYD_STH", "TRANS", "5H",
                        "2026/08/01 07:00:00", "2026/08/02 07:00:00")
            for i in range(5)
        ]
        path = _write_mms(tmp_path / "OUT.CSV", "OUTAGEDETAIL", OUTAGE_COLS, rows)
        one = parse_outage_windows(path, chunk_rows=50_000)
        two = parse_outage_windows(path, chunk_rows=1)
        assert len(one) == len(two) == 5
        assert list(one["outage_id"]) == list(two["outage_id"])

    def test_keep_months_filters_non_overlapping(self, tmp_path):
        path = _write_mms(tmp_path / "OUT.CSV", "OUTAGEDETAIL", OUTAGE_COLS, [
            _outage_row("in", "SYD_STH", "TRANS", "5H",
                        "2026/08/01 07:00:00", "2026/08/05 17:00:00"),
            _outage_row("out", "SYD_STH", "TRANS", "5H",
                        "2020/01/01 07:00:00", "2020/01/05 17:00:00"),
        ])
        df = parse_outage_windows(path, keep_months=[(2026, 8)])
        assert list(df["outage_id"]) == ["in"]

    def test_standing_windows_kept_without_overlap(self, tmp_path):
        path = _write_mms(tmp_path / "OUT.CSV", "OUTAGEDETAIL", OUTAGE_COLS, [
            _outage_row("standing", "SYD_STH", "TRANS", "5H",
                        "2099/01/01 07:00:00", "2099/01/05 17:00:00"),
            _outage_row("old", "SYD_STH", "TRANS", "5H",
                        "2020/01/01 07:00:00", "2020/01/05 17:00:00"),
        ])
        df = parse_outage_windows(path, keep_months=[(2026, 8)])
        assert list(df["outage_id"]) == ["standing"]
        assert bool(df.loc[0, "standing_window"])

    def test_far_future_end_is_open(self):
        assert window_open(None) is True
        assert window_open(pd.Timestamp("2202-01-01")) is True
        assert window_open(pd.Timestamp("2026-09-01")) is False

    def test_bad_timestamp_is_none(self):
        assert parse_mms_timestamp("not a date") is None
        assert parse_mms_timestamp("") is None


def _values(columns: list[str], row: dict) -> list[str]:
    values = []
    for col in columns:
        value = row.get(col, "")
        value = "" if value is None else str(value)
        values.append(f'"{value}"' if "," in value else value)
    return values


# ─── Context join ──────────────────────────────────────────────────────────

class TestContextJoin:
    def test_voltage_latest_validfrom_wins(self, context_files):
        vmap = load_voltage_map(context_files["equipment"])
        row = vmap[vmap["substation_id"] == "TREVALYN"].iloc[0]
        assert row["voltage"] == 220

    def test_zero_voltage_never_kept(self, context_files):
        vmap = load_voltage_map(context_files["equipment"])
        assert "ZERO_SUB" not in set(vmap["substation_id"])

    def test_region_sources_in_priority_order(self, context_files):
        windows = _windows_frame([
            _outage_row("exact", "SYD_STH", "TRANS", "5H",
                        "2026/08/01 07:00:00", "2026/08/02 07:00:00"),
            _outage_row("majority", "SYD_WEST", "CAP", "C2",
                        "2026/08/01 07:00:00", "2026/08/02 07:00:00"),
            _outage_row("detail", "ZERO_SUB", "LINE", "Z1",
                        "2026/08/01 07:00:00", "2026/08/02 07:00:00"),
            _outage_row("unjoined", "NOWHERE", "LINE", "X",
                        "2026/08/01 07:00:00", "2026/08/02 07:00:00"),
        ])
        df = _context(windows, context_files).set_index("outage_id")
        assert df.loc["exact", "region"] == "NSW1"
        assert df.loc["exact", "region_source"] == "equipment_match"
        assert df.loc["majority", "region"] == "NSW1"
        assert df.loc["majority", "region_source"] == "substation_majority_rating"
        assert df.loc["detail", "region"] == "QLD1"
        assert df.loc["detail", "region_source"] == "substation_detail"
        # No join → null region, never a guessed/zero-filled one.
        assert pd.isna(df.loc["unjoined", "region"])

    def test_unknown_voltage_kept_not_zeroed(self, context_files):
        windows = _windows_frame([
            _outage_row("nolookup", "SYD_STH", "BUS", "B1",
                        "2026/08/01 07:00:00", "2026/08/02 07:00:00"),
        ])
        df = _context(windows, context_files)
        assert pd.isna(df.loc[0, "voltage"])
        assert df.loc[0, "voltage_class"] == no.VOLTAGE_UNKNOWN
        assert df.loc[0, "region"] == "NSW1"  # region still resolved

    @pytest.mark.parametrize("voltage,expected", [
        (500, "500kV"), (330, "330kV"), (275, "275kV"), (220, "220kV"),
        (132, "110-132kV"), (110, "110-132kV"), (66, "33-66kV"), (33, "33-66kV"),
        (11, "<33kV"), (None, "unknown"), (0, "unknown"), ("junk", "unknown"),
    ])
    def test_voltage_classes(self, voltage, expected):
        assert voltage_class(voltage) == expected


# ─── Windowing + aggregation ───────────────────────────────────────────────

class TestWindowing:
    def test_outage_days_clipped_to_month(self):
        # 20 Jul → 10 Aug overlaps August on 1–10 Aug inclusive = 9 days.
        assert outage_days_in_month(
            pd.Timestamp("2026-07-20"), pd.Timestamp("2026-08-10"), 2026, 8) == 9.0
        assert outage_days_in_month(
            pd.Timestamp("2026-07-20"), pd.Timestamp("2026-08-10"), 2026, 7) == 12.0

    def test_open_window_runs_to_month_end(self):
        days = outage_days_in_month(pd.Timestamp("2026-08-16"), None, 2026, 8)
        assert days == pytest.approx(16.0)

    def test_non_overlapping_month_is_zero(self):
        assert outage_days_in_month(
            pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-05"), 2026, 8) == 0.0

    def test_slice_month_drops_zero_day_windows(self, context_files):
        windows = _context(_windows_frame([
            _outage_row("aug", "SYD_STH", "TRANS", "5H",
                        "2026/08/01 07:00:00", "2026/08/03 07:00:00"),
            _outage_row("jul", "SYD_STH", "TRANS", "5H",
                        "2026/07/01 07:00:00", "2026/07/03 07:00:00"),
        ]), context_files)
        facts = slice_month(windows, 2026, 8)
        assert list(facts["outage_id"]) == ["aug"]
        assert facts.loc[0, "month"] == "2026-08"
        assert facts.loc[0, "outage_days"] == pytest.approx(2.0)

    def test_aggregate_excludes_withdrawn_and_unjoined(self, context_files):
        windows = _context(_windows_frame([
            _outage_row("keep", "SYD_STH", "TRANS", "5H",
                        "2026/08/01 00:00:00", "2026/08/11 00:00:00", status="PTP"),
            _outage_row("withdrawn", "SYD_STH", "TRANS", "5H",
                        "2026/08/01 00:00:00", "2026/08/21 00:00:00", status="WDRAWN"),
            _outage_row("unjoined", "NOWHERE", "LINE", "X",
                        "2026/08/01 00:00:00", "2026/08/21 00:00:00"),
        ]), context_files)
        facts = slice_month(windows, 2026, 8)
        agg = no.aggregate_by_region_voltage(facts)
        assert set(agg) == {"NSW1"}
        assert agg["NSW1"]["outage_days"] == pytest.approx(10.0)
        assert agg["NSW1"]["windows"] == 1
        assert agg["NSW1"]["open_windows"] == 0
        assert agg["NSW1"]["by_voltage"]["500kV"]["outage_days"] == pytest.approx(10.0)

    def test_aggregate_counts_open_windows(self, context_files):
        windows = _context(_windows_frame([
            _outage_row("open", "SYD_STH", "TRANS", "5H",
                        "2026/08/16 00:00:00", "2099/01/01 00:00:00", status="UTP"),
        ]), context_files)
        facts = slice_month(windows, 2026, 8)
        agg = no.aggregate_by_region_voltage(facts)
        assert agg["NSW1"]["open_windows"] == 1
        assert agg["NSW1"]["outage_days"] == pytest.approx(16.0)


# ─── Payload + publish ─────────────────────────────────────────────────────

class TestPayload:
    def _facts(self, context_files):
        windows = _context(_windows_frame([
            _outage_row("a", "SYD_STH", "TRANS", "5H",
                        "2026/08/01 00:00:00", "2026/08/11 00:00:00", status="PTP"),
            _outage_row("b", "TREVALYN", "LINE", "HA_TR1",
                        "2026/08/05 00:00:00", "2026/08/06 00:00:00"),
            _outage_row("standing", "SYD_STH", "TRANS", "5H",
                        "2099/01/01 00:00:00", "2099/01/05 00:00:00"),
        ]), context_files)
        return slice_month(windows, 2026, 8), windows

    def test_payload_shape_and_summary(self, context_files):
        facts, windows = self._facts(context_files)
        payload = build_network_outages_payload(
            facts, {"month": "2026-08", "fetched_months": ["2026-08"]}, windows,
        )
        assert payload["asof_month"] == "2026-08"
        assert payload["months"] == ["2026-08"]
        assert payload["summary"]["regions"] == 2
        assert payload["summary"]["standing_windows"] == 1
        assert payload["source"]["table"] == OUTAGE_TABLE
        assert payload["source_status"]["status"] == STATUS_OK
        assert {r["region"] for r in payload["active_windows"]} == {"NSW1", "TAS1"}
        # Standing windows are published, never dropped, and carry no outage-days.
        assert [r["outage_id"] for r in payload["standing_windows"]] == ["standing"]
        assert "outage_days" not in payload["standing_windows"][0]

    def test_payload_is_json_safe(self, context_files):
        facts, windows = self._facts(context_files)
        payload = build_network_outages_payload(facts, {"month": "2026-08"}, windows)
        json.dumps(payload)  # must not raise (no NaN/NaT leaks)

    def test_publish_semantic_gate(self, tmp_path, context_files):
        facts, windows = self._facts(context_files)
        payload = build_network_outages_payload(facts, {"month": "2026-08"}, windows)
        first = publish_network_outages_json(payload, tmp_path)
        assert first is not None
        stamped = json.loads((tmp_path / no.ARTIFACT_FILENAME).read_text())
        # Same facts, later clock → the gate leaves the file (and stamp) alone.
        again = publish_network_outages_json(
            {**payload, "updated_utc": "2030-01-01T00:00:00Z"}, tmp_path,
        )
        assert again is None
        assert json.loads((tmp_path / no.ARTIFACT_FILENAME).read_text())["updated_utc"] == \
            stamped["updated_utc"]
        # Changed facts → rewritten.
        assert publish_network_outages_json(
            {**payload, "asof_month": "2026-09"}, tmp_path,
        ) is not None


# ─── Lane orchestration ────────────────────────────────────────────────────

def _zip_from_csv(csv_path: Path, table: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest, "w") as zf:
        zf.write(csv_path, arcname=f"PUBLIC_ARCHIVE#{table}#FILE01#202608010000.CSV")
    return dest


def _install_fake_source(monkeypatch, tmp_path, *, month=(2026, 8), windows=None,
                         context_files=None, content_length=12345,
                         last_modified="Fri, 11 Sep 2026 06:18:31 GMT"):
    """Monkeypatch the HTTP boundary to serve synthetic MMS zips."""
    year, month_no = month
    windows = windows if windows is not None else [
        _outage_row("1", "SYD_STH", "TRANS", "5H",
                    f"{year}/{month_no:02d}/01 07:00:00", f"{year}/{month_no:02d}/11 17:00:00"),
    ]
    outage_csv = _write_mms(tmp_path / "src" / "OUTAGE.CSV", "OUTAGEDETAIL",
                            OUTAGE_COLS, windows)
    sources = {OUTAGE_TABLE: outage_csv}
    if context_files is not None:
        sources[EQUIPMENT_TABLE] = context_files["equipment"]
        sources[RATING_TABLE] = context_files["rating"]
        sources[SUBSTATION_TABLE] = context_files["substation"]

    probe_calls = []

    def fake_probe(y, m):
        probe_calls.append((y, m))
        if (y, m) == (year, month_no):
            return {"year": y, "month": m, "url": month_url(y, m),
                    "content_length": content_length, "last_modified": last_modified}
        return None

    download_calls = []

    def fake_download_zip(url, dest):
        download_calls.append(url)
        table = next((t for t in sources if t in url), None)
        if table is None:
            raise no.NetworkOutageError(f"unexpected url {url}")
        _zip_from_csv(sources[table], table, Path(dest))
        return {"url": url, "content_length": content_length,
                "last_modified": last_modified}

    monkeypatch.setattr(no, "probe_month", fake_probe)
    monkeypatch.setattr(no, "download_zip", fake_download_zip)
    return {"probe_calls": probe_calls, "download_calls": download_calls}


class TestLane:
    def test_success_writes_snapshot_slices_and_artifact(
        self, tmp_path, monkeypatch, context_files,
    ):
        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        docs_dir = tmp_path / "docs" / "data"
        result = run_network_outages_lane(
            data_dir, docs_dir, months_back=1, today=date(2026, 9, 19),
        )
        assert result.status == STATUS_OK
        assert result.fetched is True
        assert result.month == "2026-08"
        assert result.months == ["2026-08"]
        assert result.window_count == 1

        base = no.snapshot_dir(data_dir)
        assert (base / no.SNAPSHOT_FILENAME).exists()
        assert (base / no.MONTH_SLICE_TEMPLATE.format(ym="202608")).exists()
        meta = json.loads((base / no.META_FILENAME).read_text())
        assert meta["month"] == "2026-08"

        artifact = json.loads((docs_dir / no.ARTIFACT_FILENAME).read_text())
        assert artifact["asof_month"] == "2026-08"
        # 1 Aug 07:00 → 11 Aug 17:00 = 10 days + 10 hours.
        assert artifact["by_region"]["NSW1"]["outage_days"] == pytest.approx(10.42)
        # No raw archive survives the run (parse-and-slice).
        assert list(base.glob("*.zip")) == []
        assert list(base.glob("*.CSV")) == []

    def test_two_month_window_slices_both(self, tmp_path, monkeypatch, context_files):
        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        result = run_network_outages_lane(
            data_dir, tmp_path / "docs" / "data", months_back=2, today=date(2026, 9, 19),
        )
        assert result.status == STATUS_OK
        # Only August has windows, but both months are attempted and sliced.
        facts, meta = load_snapshot(data_dir)
        assert sorted(facts["month"].unique()) == ["2026-08"]
        assert meta["fetched_months"] == ["2026-08", "2026-07"]

    def test_backfill_extends_the_window(self, tmp_path, monkeypatch, context_files):
        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        result = run_network_outages_lane(
            tmp_path / "data", tmp_path / "docs" / "data",
            months_back=1, backfill=6, today=date(2026, 9, 19),
        )
        facts, meta = load_snapshot(tmp_path / "data")
        assert meta["fetched_months"] == [
            "2026-08", "2026-07", "2026-06", "2026-05", "2026-04", "2026-03",
        ]

    def test_unchanged_source_skips_download(self, tmp_path, monkeypatch, context_files):
        fake = _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        docs_dir = tmp_path / "docs" / "data"
        run_network_outages_lane(data_dir, docs_dir, months_back=1, today=date(2026, 9, 19))
        downloads_after_first = len(fake["download_calls"])
        assert downloads_after_first > 0

        second = run_network_outages_lane(
            data_dir, docs_dir, months_back=1, today=date(2026, 9, 19),
        )
        assert second.status == STATUS_OK
        assert second.fetched is False
        assert "unchanged" in (second.note or "")
        assert len(fake["download_calls"]) == downloads_after_first

    def test_force_refetches(self, tmp_path, monkeypatch, context_files):
        fake = _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        run_network_outages_lane(data_dir, tmp_path / "docs" / "data",
                                 months_back=1, today=date(2026, 9, 19))
        before = len(fake["download_calls"])
        run_network_outages_lane(data_dir, tmp_path / "docs" / "data",
                                 months_back=1, force=True, today=date(2026, 9, 19))
        assert len(fake["download_calls"]) > before

    def test_discovery_failure_degrades_and_retains(
        self, tmp_path, monkeypatch, context_files,
    ):
        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        docs_dir = tmp_path / "docs" / "data"
        run_network_outages_lane(data_dir, docs_dir, months_back=1, today=date(2026, 9, 19))
        published = (docs_dir / no.ARTIFACT_FILENAME).read_text()

        monkeypatch.setattr(no, "probe_month", lambda y, m: None)
        result = run_network_outages_lane(
            data_dir, docs_dir, months_back=1, today=date(2026, 9, 19),
        )
        assert result.status == STATUS_DEGRADED
        assert result.retained is True
        assert result.error
        assert result.month == "2026-08"
        # Last-known-good artifact untouched.
        assert (docs_dir / no.ARTIFACT_FILENAME).read_text() == published

    def test_discovery_failure_cold_start_is_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(no, "probe_month", lambda y, m: None)
        result = run_network_outages_lane(
            tmp_path / "data", tmp_path / "docs" / "data",
            months_back=1, today=date(2026, 9, 19),
        )
        assert result.status == STATUS_ERROR
        assert result.retained is False

    def test_parse_failure_retains_previous_snapshot(
        self, tmp_path, monkeypatch, context_files,
    ):
        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        docs_dir = tmp_path / "docs" / "data"
        run_network_outages_lane(data_dir, docs_dir, months_back=1, today=date(2026, 9, 19))

        def failing_download(url, dest):
            raise no.NetworkOutageError("boom")

        monkeypatch.setattr(no, "download_zip", failing_download)
        result = run_network_outages_lane(
            data_dir, docs_dir, months_back=1, force=True, today=date(2026, 9, 19),
        )
        assert result.status == STATUS_DEGRADED
        assert result.retained is True
        assert "boom" in (result.error or "")
        facts, _ = load_snapshot(data_dir)
        assert not facts.empty

    def test_no_windows_is_degraded_after_a_good_run(
        self, tmp_path, monkeypatch, context_files,
    ):
        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        docs_dir = tmp_path / "docs" / "data"
        run_network_outages_lane(data_dir, docs_dir, months_back=1, today=date(2026, 9, 19))

        _install_fake_source(
            monkeypatch, tmp_path, month=(2026, 8),
            windows=[_outage_row("old", "SYD_STH", "TRANS", "5H",
                                 "2020/01/01 07:00:00", "2020/01/02 07:00:00")],
            context_files=context_files, content_length=999,
        )
        result = run_network_outages_lane(
            data_dir, docs_dir, months_back=1, today=date(2026, 9, 19),
        )
        assert result.status == STATUS_DEGRADED
        assert result.retained is True

    def test_snapshot_is_retained_across_runs(self, tmp_path, monkeypatch, context_files):
        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        data_dir = tmp_path / "data"
        run_network_outages_lane(data_dir, tmp_path / "docs" / "data",
                                 months_back=1, today=date(2026, 9, 19))
        facts, meta = load_snapshot(data_dir)
        assert len(facts) == 1
        assert meta["join_warning"] is None

    def test_lane_frame_supports_asof_month(self, tmp_path, monkeypatch, context_files):
        from src.run_status import LaneRun

        _install_fake_source(monkeypatch, tmp_path, context_files=context_files)
        result = run_network_outages_lane(
            tmp_path / "data", tmp_path / "docs" / "data",
            months_back=1, today=date(2026, 9, 19),
        )
        lane = LaneRun(source="network_outages", frame=result.frame)
        assert lane.asof_month() == "2026-08"

    def test_missing_context_tables_still_publishes_with_warning(
        self, tmp_path, monkeypatch,
    ):
        # No context_files → only the outage zip resolves; region join fails for
        # every window, so the rollup is empty but the lane still publishes and
        # records the join warning rather than zero-filling regions.
        _install_fake_source(monkeypatch, tmp_path, context_files=None)
        docs_dir = tmp_path / "docs" / "data"
        result = run_network_outages_lane(
            tmp_path / "data", docs_dir, months_back=1, today=date(2026, 9, 19),
        )
        assert result.status == STATUS_OK
        artifact = json.loads((docs_dir / no.ARTIFACT_FILENAME).read_text())
        assert artifact["by_region"] == {}
        assert artifact["summary"]["unjoined_region_windows"] == 1
        assert artifact["source_status"]["join_warning"]


# ─── Storage hygiene ───────────────────────────────────────────────────────

class TestPrune:
    def test_prune_raw_removes_archives(self, tmp_path):
        base = tmp_path / "network_outages"
        base.mkdir()
        (base / "a.zip").write_text("x")
        (base / "b.CSV").write_text("x")
        (base / "c.csv").write_text("x")
        (base / "keep.feather").write_text("x")
        assert prune_raw(base) == 3
        assert not (base / "a.zip").exists()
        assert (base / "keep.feather").exists()

    def test_prune_raw_keeps_named_files(self, tmp_path):
        base = tmp_path / "network_outages"
        base.mkdir()
        keep = base / "current.zip"
        keep.write_text("x")
        (base / "old.zip").write_text("x")
        assert prune_raw(base, keep=(keep,)) == 1
        assert keep.exists()

    def test_prune_raw_on_missing_dir_is_zero(self, tmp_path):
        assert prune_raw(tmp_path / "nope") == 0
