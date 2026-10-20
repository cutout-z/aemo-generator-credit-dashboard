"""Peak memory of the BIDPEROFFER_D factor step (NAS 2026-10-19).

The daily lane peaked at VmHWM 13 GB, enough to swap the NAS off Tailscale.
Profiled on three cached months (2026-07..09), the factor step's peak
physical footprint was 14.3 GiB, almost all of it frames kept alive longer
than needed. These tests pin the fixes that brought it to ~4.9 GiB:

1. The factor driver releases a month's frames before reading the next one.
2. The cached read frees each Arrow row group as it converts it, so a month
   is never resident twice (as Arrow and as pandas).
3. The FCAS and ENERGY slices copy only the columns their lanes read.
4. ``import src`` sets mimalloc's purge delay to 0 before pyarrow loads.

Hermetic: synthetic frames and parquets; nemosis is stubbed.
"""

from __future__ import annotations

import gc
import os
import subprocess
import sys
import weakref
from pathlib import Path

import pandas as pd
import pyarrow as pa

from src import download_bids as db
from src.main import _run_optional_factor_lanes
from src.offer_curves import energy_volumes_from_raw

REPO = Path(__file__).resolve().parents[1]
BANDS = [f"BANDAVAIL{i}" for i in range(1, 11)]


def _raw_month(year: int, month: int) -> pd.DataFrame:
    """A small raw union frame: one FCAS and one ENERGY unit, a full month."""
    last = pd.Period(f"{year}-{month:02d}", freq="M").days_in_month
    ts = pd.to_datetime([f"{year}-{month:02d}-01 04:05:00", f"{year}-{month:02d}-{last} 12:00:00"])
    rows = []
    for t in ts:
        for bidtype, maxavail in (("RAISE6SEC", 5), ("ENERGY", 0)):
            row = {
                "INTERVAL_DATETIME": t, "DUID": "GEN1", "BIDTYPE": bidtype,
                "MAXAVAIL": maxavail, "ENABLEMENTMIN": 0.0, "ENABLEMENTMAX": 5.0,
            }
            for i, band in enumerate(BANDS, 1):
                row[band] = 10 if i == 1 else 0
            rows.append(row)
    return pd.DataFrame(rows)


def _prices(year: int, month: int) -> pd.DataFrame:
    row = {"DUID": ["GEN1"], "SETTLEMENTDATE": pd.to_datetime([f"{year}-{month:02d}-01"])}
    for i in range(1, 11):
        row[f"PRICEBAND{i}"] = [float(i * 10)]
    return pd.DataFrame(row)


class TestFactorDriverReleasesMonths:
    def test_previous_month_is_freed_before_the_next_read(self, tmp_path, monkeypatch):
        served: list[weakref.ref] = []
        alive_at_read: list[int] = []

        def fake_fetch(year, month, cache_dir, rebuild=None):
            gc.collect()  # only a live reference can keep a frame now
            alive_at_read.append(sum(ref() is not None for ref in served))
            raw = _raw_month(year, month)
            served.append(weakref.ref(raw))
            return raw

        monkeypatch.setattr("src.main.fetch_bidperoffer_union", fake_fetch)
        monkeypatch.setattr("src.main.fetch_energy_price_sides", lambda y, m, d: (_prices(y, m), pd.DataFrame()))
        fcas, offer, curves, *_ = _run_optional_factor_lanes(
            tmp_path, [(2026, 7), (2026, 8), (2026, 9)],
            skip_fcas_factors=False, skip_offer_factors=False, full_refresh=False,
        )

        assert alive_at_read == [0, 0, 0]  # no earlier month resident at any read
        assert sorted(fcas["month"].unique()) == ["2026-07", "2026-08", "2026-09"]
        assert sorted(offer["month"].unique()) == ["2026-07", "2026-08", "2026-09"]
        assert not curves.empty


class TestSlicesCarryOnlyTheirColumns:
    def test_fcas_slice_drops_energy_bands(self):
        bids = db.fcas_bids_from_raw(_raw_month(2026, 7), 2026, 7)
        assert not bids.empty and set(bids["BIDTYPE"]) == {"RAISE6SEC"}
        assert list(bids.columns) == [
            "INTERVAL_DATETIME", "DUID", "BIDTYPE", "MAXAVAIL", "ENABLEMENTMIN", "ENABLEMENTMAX",
        ]

    def test_energy_slice_drops_fcas_capacity(self):
        vols = energy_volumes_from_raw(_raw_month(2026, 7), 2026, 7)
        assert not vols.empty and set(vols["BIDTYPE"]) == {"ENERGY"}
        assert list(vols.columns) == ["INTERVAL_DATETIME", "DUID", "BIDTYPE", *BANDS]


def _write_archive(tmp_path, year: int, month: int, n_units: int = 30) -> None:
    """A cached month as nemosis writes it (all strings): hourly intervals
    04:05 on day 1 to 04:00 next month, ``n_units`` units per interval."""
    start = pd.Timestamp(year, month, 1, 4, 5)
    end = (start + pd.offsets.MonthBegin(1)).replace(hour=4, minute=0)
    ts = pd.date_range(start, end, freq="h").repeat(n_units)
    k = pd.RangeIndex(len(ts))
    frame = pd.DataFrame({
        "DUID": [f"GEN{i % n_units}" for i in k], "BIDTYPE": "ENERGY",
        "INTERVAL_DATETIME": ts.strftime("%Y/%m/%d %H:%M:%S"),
        "MAXAVAIL": (k % 50).astype(str), "ENABLEMENTMIN": "0.5", "ENABLEMENTMAX": "5",
        **{band: (k % 13).astype(str) for band in BANDS},
    }).astype("str")
    path = db._nemosis_cache_path(str(tmp_path), "BIDPEROFFER_D", year, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, row_group_size=2000)


class TestCachedReadFreesArrowTables:
    def test_month_is_not_resident_twice(self, tmp_path, monkeypatch):
        # M-1 boundary hours plus a whole month M, many row groups.
        _write_archive(tmp_path, 2026, 6)
        _write_archive(tmp_path, 2026, 7)
        monkeypatch.setattr("nemosis.dynamic_data_compiler", lambda **k: 1 / 0)

        previous = pa.default_memory_pool()
        pool = pa.proxy_memory_pool(previous)
        pa.set_memory_pool(pool)
        try:
            frame = db.fetch_bidperoffer_union(2026, 7, str(tmp_path), rebuild=None)
            rows, held = len(frame), pool.bytes_allocated()  # the frame lives in the pool
            del frame  # its buffers must not outlive the proxy pool
            gc.collect()
        finally:
            pa.set_memory_pool(previous)

        assert rows > 20_000 and held > 0
        # Holding the row-group tables through to_pandas doubled the peak (2.0x
        # what the frame holds); freeing them as they convert keeps it near 1x.
        assert pool.max_memory() < 1.5 * held, (pool.max_memory(), held)


class TestMimallocPurgeDelay:
    def _import_src(self, env):
        out = subprocess.run(
            [sys.executable, "-c",
             "import os, sys; import src; "
             "print(os.environ.get('MIMALLOC_PURGE_DELAY'), 'pyarrow' in sys.modules)"],
            cwd=REPO, env=env, capture_output=True, text=True, check=True,
        )
        return out.stdout.split()

    def test_import_src_sets_it_before_pyarrow_loads(self):
        env = {k: v for k, v in os.environ.items() if k != "MIMALLOC_PURGE_DELAY"}
        assert self._import_src(env) == ["0", "False"]

    def test_an_explicit_setting_wins(self):
        assert self._import_src({**os.environ, "MIMALLOC_PURGE_DELAY": "10"})[0] == "10"


class TestPeakRssIsLogged:
    """The lane log records each run's peak, so a run can be checked after it ends."""

    def test_logs_peak_in_gib(self, caplog):
        import logging
        import re

        from src.main import _log_peak_rss

        with caplog.at_level(logging.INFO, logger="src.main"):
            _log_peak_rss("a test")
        msg = caplog.records[-1].getMessage()
        m = re.fullmatch(r"Peak RSS after a test: (\d+\.\d\d) GiB", msg)
        assert m and 0 < float(m.group(1)) < 1024, msg

    def test_main_logs_after_the_heavy_steps(self):
        import inspect

        from src import main as m

        src = inspect.getsource(m.main)
        for stage in ("step 3 (SCADA", "steps 3e/3e2/3e3", "the whole run"):
            assert f'_log_peak_rss("{stage}' in src, stage
