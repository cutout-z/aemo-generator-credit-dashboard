"""Low-memory BIDPEROFFER_D cache read (NAS incident 2026-10-12).

nemosis's compiler decoded every cached BIDPEROFFER_D month whole (M-1, M
and M+1, ~20M rows x 21 string columns each) before date-filtering, peaking
at ~14 GiB per fetch; two overlapping lanes drove the NAS into swap. A warm
cache is now read row group by row group (src.download_bids
._read_cached_window). These tests pin that:

1. The frame is IDENTICAL to the real nemosis compiler's (rows, order, index,
   columns, dtypes, values), for a full month and for an unpublished month.
2. A warm cache never calls the compiler; an uncached month is cached in
   nemosis's caching mode (no decode); a cold M-1 falls back to the compiler.

Hermetic: synthetic parquets in tmp dirs; nemosis downloads are stubbed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from src import download_bids as db

# nemosis's default BIDPEROFFER_D cache columns, in its cached-file order.
_FILE_COLS = (
    ["SETTLEMENTDATE", "DUID", "BIDTYPE", "DIRECTION", "INTERVAL_DATETIME", "OFFERDATE",
     "MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX", "LOWBREAKPOINT", "HIGHBREAKPOINT"]
    + [f"BANDAVAIL{i}" for i in range(1, 11)]
)


def _next_month(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 1) if month == 12 else (year, month + 1)


def _archive_month(year: int, month: int) -> pd.DataFrame:
    """A month's archive as nemosis caches it: every column a string.

    Trading days run 04:05 -> 04:00 next day, so the file for M also holds
    M+1's first hours (00:05-04:00), exactly like the real archive. Hourly
    intervals keep it small; ENABLEMENTMIN carries decimals (-> float64),
    the rest are integer strings (-> int64).
    """
    start = datetime(year, month, 1, 4, 5)
    ny, nm = _next_month(year, month)
    end = datetime(ny, nm, 1, 4, 0)
    rows = []
    t = start
    k = 0
    while t <= end:
        for duid, bidtype in (("GEN1", "ENERGY"), ("GEN1", "RAISE6SEC"), ("BAT2", "LOWERREG")):
            k += 1
            row = {
                "SETTLEMENTDATE": (t - timedelta(hours=4, minutes=5)).strftime("%Y/%m/%d 00:00:00"),
                "DUID": duid, "BIDTYPE": bidtype, "DIRECTION": "GEN",
                "INTERVAL_DATETIME": t.strftime("%Y/%m/%d %H:%M:%S"),
                "OFFERDATE": "2026/01/01 00:00:00",
                "MAXAVAIL": str(k % 50), "ENABLEMENTMIN": f"{k % 7}.5",
                "ENABLEMENTMAX": str(k % 90), "LOWBREAKPOINT": "0", "HIGHBREAKPOINT": "0",
            }
            for i in range(1, 11):
                row[f"BANDAVAIL{i}"] = str((k * i) % 13)
            rows.append(row)
        t += timedelta(hours=1)
    return pd.DataFrame(rows, columns=_FILE_COLS).astype("str")


def _write_month(tmp_path, year: int, month: int) -> None:
    path = db._nemosis_cache_path(str(tmp_path), "BIDPEROFFER_D", year, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Small row groups so the footer-statistics skip is exercised.
    _archive_month(year, month).to_parquet(path, row_group_size=97)


def _window(year: int, month: int) -> tuple[str, str]:
    ny, nm = _next_month(year, month)
    return f"{year}/{month:02d}/01 00:00:00", f"{ny}/{nm:02d}/01 00:00:00"


@pytest.fixture
def no_downloads(monkeypatch):
    import nemosis.data_fetch_methods as dfm

    monkeypatch.setattr(dfm, "_download_data", lambda *a, **k: None)


def _compiler_frame(tmp_path, year: int, month: int) -> pd.DataFrame:
    """What the real nemosis compiler returns for the month (the old path)."""
    from nemosis import dynamic_data_compiler

    start, end = _window(year, month)
    return dynamic_data_compiler(
        start_time=start, end_time=end, table_name="BIDPEROFFER_D",
        raw_data_location=str(tmp_path / "nemosis_cache"),
        select_columns=db.BIDPEROFFER_UNION_COLS, fformat="parquet", rebuild=False,
    )


def _block_compiler(monkeypatch):
    def boom(**kwargs):
        raise AssertionError("nemosis compiler called on a warm cache")

    monkeypatch.setattr("nemosis.dynamic_data_compiler", boom)


class TestMatchesCompiler:
    def test_full_month_identical(self, tmp_path, monkeypatch, no_downloads):
        for m in (6, 7, 8):
            _write_month(tmp_path, 2026, m)
        expected = _compiler_frame(tmp_path, 2026, 7)

        _block_compiler(monkeypatch)
        got = db.fetch_bidperoffer_union(2026, 7, str(tmp_path), rebuild=None)

        pd.testing.assert_frame_equal(got, expected, check_exact=True)
        assert str(got["MAXAVAIL"].dtype) == "int64"
        assert str(got["ENABLEMENTMIN"].dtype) == "float64"

    def test_unpublished_month_identical(self, tmp_path, monkeypatch, no_downloads):
        """M not published: only M-1's boundary hours fall in the window."""
        _write_month(tmp_path, 2026, 8)
        expected = _compiler_frame(tmp_path, 2026, 9)
        assert 0 < len(expected) < 20  # 00:05-04:00 boundary rows only

        cached = []
        monkeypatch.setattr(db, "_cache_month", lambda *a, **k: cached.append(a[:2]), raising=False)
        _block_compiler(monkeypatch)
        got = db.fetch_bidperoffer_union(2026, 9, str(tmp_path), rebuild=None)

        pd.testing.assert_frame_equal(got, expected, check_exact=True)
        assert cached == [(2026, 9)]  # tried to cache M, decoded nothing


class TestRouting:
    def test_cold_previous_month_uses_compiler(self, tmp_path, monkeypatch):
        _write_month(tmp_path, 2026, 7)  # M cached, M-1 not
        seen = {}

        def fake_compiler(**kwargs):
            seen["called"] = True
            return _archive_month(2026, 7)

        monkeypatch.setattr("nemosis.dynamic_data_compiler", fake_compiler)
        db.fetch_bidperoffer_union(2026, 7, str(tmp_path), rebuild=None)
        assert seen.get("called")

    def test_full_refresh_uses_compiler(self, tmp_path, monkeypatch):
        for m in (6, 7):
            _write_month(tmp_path, 2026, m)
        seen = {}

        def fake_compiler(**kwargs):
            seen["rebuild"] = kwargs.get("rebuild")
            return _archive_month(2026, 7)

        monkeypatch.setattr("nemosis.dynamic_data_compiler", fake_compiler)
        db.fetch_bidperoffer_union(2026, 7, str(tmp_path), rebuild=True)
        assert seen.get("rebuild") is True

    def test_unreadable_cache_falls_back(self, tmp_path, monkeypatch):
        for m in (6, 7):
            _write_month(tmp_path, 2026, m)
        monkeypatch.setattr(db, "_window_parts", lambda *a, **k: 1 / 0, raising=False)
        seen = {}

        def fake_compiler(**kwargs):
            seen["called"] = True
            return _archive_month(2026, 7)

        monkeypatch.setattr("nemosis.dynamic_data_compiler", fake_compiler)
        db.fetch_bidperoffer_union(2026, 7, str(tmp_path), rebuild=None)
        assert seen.get("called")
