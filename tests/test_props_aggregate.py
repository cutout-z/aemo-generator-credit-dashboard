"""Property tests for the duplicate-interval protection (June 2022 double count, fa659afd8)."""

import pandas as pd
import pytest

pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st  # noqa: E402

from src.aggregate import aggregate_month, dedupe_interval_keys  # noqa: E402
from tests.hypothesis_profile import load_gate_profile  # noqa: E402

load_gate_profile()

KEYS = ["SETTLEMENTDATE", "DUID"]
_gens = pd.DataFrame({"DUID": ["A", "B"], "REGION": ["NSW1", "NSW1"],
                      "CAPACITY_MW": [10.0, 20.0], "FUEL_CATEGORY": ["Solar", "Wind"]})


@st.composite
def frames(draw):
    n = draw(st.integers(1, 6))
    ts = pd.date_range("2022-06-10 10:05", periods=n, freq="5min")
    duids = draw(st.lists(st.sampled_from(["A", "B"]), min_size=1, max_size=2, unique=True))
    scada = pd.DataFrame(
        [(t, d, float(draw(st.integers(0, 10)))) for t in ts for d in duids],
        columns=KEYS + ["SCADAVALUE"])
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": "NSW1",
                           "RRP": [float(draw(st.integers(-50, 300))) for _ in ts]})
    load = scada[KEYS].assign(AVAILABILITY=[float(draw(st.integers(0, 12))) for _ in range(len(scada))])
    return scada, prices, load


def _dup(draw_idx, df):
    """Repeat the rows at the given positions (identical repeats), appended at the end."""
    idx = [i % len(df) for i in draw_idx]
    return pd.concat([df, df.iloc[idx]], ignore_index=True)


_dups = st.lists(st.integers(0, 100), max_size=8)


@given(frames(), _dups, _dups, _dups)
def test_totals_unchanged_when_intervals_are_duplicated(fr, ds, dp, dl):
    scada, prices, load = fr
    clean = aggregate_month(scada, prices, load, _gens, {}, 2022, 6)
    dirty = aggregate_month(_dup(ds, scada), _dup(dp, prices), _dup(dl, load),
                            _gens, {}, 2022, 6)
    key = lambda f: f.sort_values("duid").reset_index(drop=True)  # noqa: E731
    pd.testing.assert_frame_equal(key(dirty), key(clean), check_exact=False, rtol=1e-9)


@given(frames(), _dups, _dups)
def test_dedupe_is_idempotent_and_one_row_per_key(fr, ds, extra):
    scada = _dup(ds, fr[0])
    # conflicting repeats too: later row with a different value
    conflict = scada.iloc[[i % len(scada) for i in extra]].assign(SCADAVALUE=99.0)
    df = pd.concat([scada, conflict], ignore_index=True)
    once = dedupe_interval_keys(df, KEYS, label="t", value_cols=["SCADAVALUE"])
    twice = dedupe_interval_keys(once, KEYS, label="t", value_cols=["SCADAVALUE"])
    assert not once.duplicated(subset=KEYS).any()
    # keep-last: every key that got a conflicting later row carries the later value
    conflicted = once.merge(conflict[KEYS].drop_duplicates(), on=KEYS)
    assert (conflicted["SCADAVALUE"] == 99.0).all()
    assert len(conflicted) == len(conflict[KEYS].drop_duplicates())
    pd.testing.assert_frame_equal(once, twice)
    assert set(map(tuple, once[KEYS].values)) == set(map(tuple, df[KEYS].values))
