"""DISPATCHLOAD intervals missing from the MMSDM archive are filled from NEXT_DAY_DISPATCHLOAD.

AEMO's September 2025 DISPATCHLOAD archive ends at 2025/09/30 04:00, so 240
intervals had no AVAILABILITY and that month's curtailment covered 29 of 30
days for every wind and solar unit.
"""

import io
import zipfile
from datetime import date

import pandas as pd
import pytest

import src.download_scada as ds

AFTER_MONTH = pd.Timestamp("2026-10-28")


def _frame(start, end, duids=("WF1", "SF1"), avail=10.0, intervention=0):
    ts = pd.date_range(start, end, freq="5min")
    return pd.DataFrame({
        "SETTLEMENTDATE": ts.repeat(len(duids)),
        "DUID": list(duids) * len(ts),
        "AVAILABILITY": avail,
        "INTERVENTION": intervention,
    })


def _archive_sep_2025():
    """What fetch_dispatchload_month returns for the short September 2025 archive."""
    df = _frame("2025-09-01 00:05", "2025-09-30 04:00").drop(columns=["INTERVENTION"])
    df["UIGF"] = float("nan")
    return df


def test_tail_gap_is_filled_from_next_day_dispatch(monkeypatch):
    calls = []
    # The Next_Day_Dispatch market day 2025-09-30 runs 04:05 to 2025-10-01 04:00.
    next_day = pd.concat([
        _frame("2025-09-30 04:05", "2025-10-01 04:00", avail=7.0),
        _frame("2025-09-30 04:05", "2025-09-30 05:00", avail=99.0, intervention=1),
    ])
    next_day["SETTLEMENTDATE"] = next_day["SETTLEMENTDATE"].dt.strftime('"%Y/%m/%d %H:%M:%S"').str.strip('"')
    next_day = next_day.astype(str)

    def fake_fetch(days, cache_dir, rebuild=False):
        calls.append(list(days))
        return next_day.copy()

    monkeypatch.setattr(ds, "fetch_next_day_unit_solution", fake_fetch)
    out = ds.heal_dispatchload_gaps(_archive_sep_2025(), 2025, 9, "/tmp/never-used", now=AFTER_MONTH)

    assert calls == [[date(2025, 9, 30)]]

    expected = pd.date_range("2025-09-01 00:05", "2025-10-01 00:00", freq="5min")
    assert out["SETTLEMENTDATE"].nunique() == len(expected) == 8640
    assert out["SETTLEMENTDATE"].max() == pd.Timestamp("2025-10-01 00:00")
    assert not out.duplicated(["SETTLEMENTDATE", "DUID"]).any()
    filled = out[out["SETTLEMENTDATE"] > pd.Timestamp("2025-09-30 04:00")]
    assert len(filled) == 240 * 2
    assert (filled["AVAILABILITY"] == 7.0).all()  # INTERVENTION=1 rows dropped
    assert filled["UIGF"].isna().all()
    assert list(out.columns) == ["SETTLEMENTDATE", "DUID", "AVAILABILITY", "UIGF"]


def test_gap_spanning_market_days_asks_for_each_day(monkeypatch):
    calls = []
    monkeypatch.setattr(
        ds, "fetch_next_day_unit_solution",
        lambda days, cache_dir, rebuild=False: calls.append(list(days)) or pd.DataFrame(
            columns=["SETTLEMENTDATE", "DUID", "INTERVENTION", "AVAILABILITY"]),
    )
    # Archive missing 2025-09-29 03:00 .. 2025-09-29 05:00: crosses the 04:00/04:05 market-day boundary.
    archive = _archive_sep_2025()
    hole = (archive["SETTLEMENTDATE"] > "2025-09-29 02:55") & (archive["SETTLEMENTDATE"] <= "2025-09-29 05:00")
    out = ds.heal_dispatchload_gaps(archive[~hole], 2025, 9, "/tmp/never-used", now=AFTER_MONTH)
    assert calls == [[date(2025, 9, 28), date(2025, 9, 29), date(2025, 9, 30)]]
    assert len(out) == int((~hole).sum())  # nothing to fill with, nothing dropped


def test_complete_month_is_not_refetched(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("no fill expected")

    monkeypatch.setattr(ds, "fetch_next_day_unit_solution", fail)
    full = _frame("2025-08-01 00:05", "2025-09-01 00:00").drop(columns=["INTERVENTION"])
    out = ds.heal_dispatchload_gaps(full, 2025, 8, "/tmp/never-used", now=AFTER_MONTH)
    assert out is full


@pytest.mark.parametrize("frame", [pd.DataFrame(), None])
def test_empty_month_is_publication_lag_not_a_gap(monkeypatch, frame):
    monkeypatch.setattr(ds, "fetch_next_day_unit_solution", lambda *a, **k: pytest.fail("no fill expected"))
    assert ds.heal_dispatchload_gaps(frame, 2025, 9, "/tmp/never-used", now=AFTER_MONTH) is frame


def test_month_too_recent_for_next_day_dispatch_is_left_alone(monkeypatch):
    monkeypatch.setattr(ds, "fetch_next_day_unit_solution", lambda *a, **k: pytest.fail("no fill expected"))
    archive = _archive_sep_2025()
    out = ds.heal_dispatchload_gaps(
        archive, 2025, 9, "/tmp/never-used", now=pd.Timestamp("2025-10-02 12:00"),
    )
    assert out is archive


def test_failed_fill_keeps_the_archive_rows(monkeypatch, caplog):
    def boom(*args, **kwargs):
        raise FileNotFoundError("Next_Day_Dispatch 2025-09-30 not on NEMWEB")

    monkeypatch.setattr(ds, "fetch_next_day_unit_solution", boom)
    archive = _archive_sep_2025()
    out = ds.heal_dispatchload_gaps(archive, 2025, 9, "/tmp/never-used", now=AFTER_MONTH)
    assert out is archive
    assert "stay without AVAILABILITY" in caplog.text


def test_main_heals_after_fetching():
    """The month loop passes every DISPATCHLOAD fetch through the heal."""
    import inspect

    import src.main as main

    src = inspect.getsource(main)
    fetch_at = src.index("dispatchload = fetch_dispatchload_month(")
    heal_at = src.index("dispatchload = heal_dispatchload_gaps(")
    assert fetch_at < heal_at < src.index("aggregate_month", fetch_at)


def _day_zip(csv_text):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("PUBLIC_NEXT_DAY_DISPATCH_20250930_0000000482922386.CSV", csv_text)
    return buf.getvalue()


# Shape of the real file: several MMS tables, each with its own I and D lines.
_DAY_CSV = """C,NEMP.WORLD,NEXT_DAY_DISPATCH,AEMO,PUBLIC,2025/10/01,04:10:00,0000000482922386,NEXT_DAY_DISPATCH,0000000482922385
I,DISPATCH,UNIT_SOLUTION,5,SETTLEMENTDATE,RUNNO,DUID,TRADETYPE,DISPATCHINTERVAL,INTERVENTION,CONNECTIONPOINTID,AVAILABILITY,UIGF
D,DISPATCH,UNIT_SOLUTION,5,"2025/09/30 04:05:00",1,WF1,0,20250930001,0,CP1,55.5,60
D,DISPATCH,UNIT_SOLUTION,5,"2025/09/30 04:05:00",1,SF1,0,20250930001,0,CP2,0,
I,DISPATCH,LOCAL_PRICE,1,SETTLEMENTDATE,DUID,LOCAL_PRICE_ADJUSTMENT,LOCALLY_CONSTRAINED
D,DISPATCH,LOCAL_PRICE,1,"2025/09/30 04:05:00",WF1,0,0
C,"END OF REPORT",5
"""


def test_parse_unit_solution_reads_only_that_table():
    df = ds._parse_unit_solution(_day_zip(_DAY_CSV))
    assert list(df.columns) == ["SETTLEMENTDATE", "DUID", "INTERVENTION", "AVAILABILITY"]
    assert df.values.tolist() == [
        ["2025/09/30 04:05:00", "WF1", "0", "55.5"],
        ["2025/09/30 04:05:00", "SF1", "0", "0"],
    ]


class _Resp:
    def __init__(self, content=b"", text=""):
        self.content, self.text = content, text

    def iter_content(self, chunk_size):
        yield self.content

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_fetch_reads_the_day_from_the_monthly_archive_bundle_and_caches_it(monkeypatch, tmp_path):
    day_zip = _day_zip(_DAY_CSV)
    bundle = io.BytesIO()
    with zipfile.ZipFile(bundle, "w") as zf:
        zf.writestr("PUBLIC_NEXT_DAY_DISPATCH_20250929_0000000482778442.zip", b"not this one")
        zf.writestr("PUBLIC_NEXT_DAY_DISPATCH_20250930_0000000482922386.zip", day_zip)
    urls = []

    def fake_get(url, stream=False):
        urls.append(url)
        if url.endswith("PUBLIC_NEXT_DAY_DISPATCH_20250901.zip"):
            return _Resp(content=bundle.getvalue())
        return None

    monkeypatch.setattr(ds, "_get", fake_get)
    df = ds.fetch_next_day_unit_solution([date(2025, 9, 30)], str(tmp_path))
    assert urls == ["https://nemweb.com.au/Reports/ARCHIVE/Next_Day_Dispatch/PUBLIC_NEXT_DAY_DISPATCH_20250901.zip"]
    assert df["DUID"].tolist() == ["WF1", "SF1"]

    # Second call is served from the per-day cache: no network.
    monkeypatch.setattr(ds, "_get", lambda *a, **k: pytest.fail("cache expected"))
    again = ds.fetch_next_day_unit_solution([date(2025, 9, 30)], str(tmp_path))
    assert again.equals(df)


def test_fetch_falls_back_to_current_page_then_raises(monkeypatch, tmp_path):
    day_zip = _day_zip(_DAY_CSV)
    listing = '<a href="/Reports/Current/Next_Day_Dispatch/PUBLIC_NEXT_DAY_DISPATCH_20250930_0000000482922386.zip">x</a>'

    def fake_get(url, stream=False):
        if "ARCHIVE" in url:
            return None
        if url.endswith("Next_Day_Dispatch/"):
            return _Resp(text=listing)
        if url.endswith("PUBLIC_NEXT_DAY_DISPATCH_20250930_0000000482922386.zip"):
            return _Resp(content=day_zip)
        return None

    monkeypatch.setattr(ds, "_get", fake_get)
    assert len(ds.fetch_next_day_unit_solution([date(2025, 9, 30)], str(tmp_path))) == 2
    with pytest.raises(FileNotFoundError):
        ds.fetch_next_day_unit_solution([date(2025, 9, 28)], str(tmp_path))
