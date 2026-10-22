"""BIDPEROFFER_D asks nemosis only for columns it can bring (code pass 2026-10-20, R3 L).

nemosis keeps only its default BIDPEROFFER_D columns, in compiler and caching
mode alike. VERSIONNO is not one of them, so requesting it never brought it:
every month logged "{'VERSIONNO'} not in ..." and an "absent, delete the
cached parquet to rebuild" warning whose advice cost a ~14 GiB rebuild that
changed nothing. Nor is it needed: BIDPEROFFER_D holds one row per (DUID,
BIDTYPE, DIRECTION, interval) (NAS check 2026-10-22: Sep 2026, 19,607,712 rows,
every key once).
"""

from __future__ import annotations

import logging

import pandas as pd
from nemosis import defaults

from src import download_bids as db
from src import offer_curves as oc


def test_union_columns_are_all_nemosis_defaults():
    assert set(db.BIDPEROFFER_UNION_COLS) <= set(defaults.table_columns["BIDPEROFFER_D"])


def test_no_versionno_warning_for_a_bidperoffer_month(caplog):
    t = pd.Timestamp("2026-09-01 04:05")
    raw = pd.DataFrame({
        "INTERVAL_DATETIME": [t, t], "DUID": ["GEN1", "GEN1"],
        "BIDTYPE": ["ENERGY", "RAISE6SEC"], "DIRECTION": ["GEN", "GEN"],
        "MAXAVAIL": [10, 5], "ENABLEMENTMIN": [0.0, 0.0], "ENABLEMENTMAX": [0, 5],
        **{f"BANDAVAIL{i}": [1, 0] for i in range(1, 11)},
    })
    with caplog.at_level(logging.WARNING):
        volumes = oc.energy_volumes_from_raw(raw, 2026, 9)
        fcas = db.fcas_bids_from_raw(raw, 2026, 9)
    assert len(volumes) == 1 and len(fcas) == 1
    assert "VERSIONNO" not in caplog.text
