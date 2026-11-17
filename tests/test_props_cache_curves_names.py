"""Property tests: processed_cache round-trip, daily offer-curve cum_mw, _safe_filename."""
import tempfile
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("hypothesis")
from hypothesis import assume, example, given, strategies as st  # noqa: E402

from src.generate_json import _safe_filename  # noqa: E402
from src.offer_curves import BAND_AVAIL_COLS, BAND_PRICE_COLS, compute_offer_curves_daily  # noqa: E402
from src.processed_cache import (  # noqa: E402
    MANIFEST_NAME, SNAPSHOT_FILES, publish_processed_cache, restore_processed_cache, snapshot_dir,
)
from tests.hypothesis_profile import load_gate_profile  # noqa: E402

load_gate_profile()


# ---- E8: processed_cache publish/restore -----------------------------------
_subset = st.sets(st.sampled_from(SNAPSHOT_FILES))
_blob = st.binary(min_size=0, max_size=64)


@given(_subset, st.data())
def test_publish_restore_round_trip_is_byte_identical(names, data):
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        src, docs, cold = tmp / "data", tmp / "docs" / "data", tmp / "cold"
        src.mkdir()
        content = {n: data.draw(_blob) for n in sorted(names)}
        for n, b in content.items():
            (src / n).write_bytes(b)
        assert sorted(publish_processed_cache(src, docs)) == sorted(names)
        manifest = (snapshot_dir(docs) / MANIFEST_NAME).read_bytes()
        publish_processed_cache(src, docs)
        assert (snapshot_dir(docs) / MANIFEST_NAME).read_bytes() == manifest  # idempotent
        assert sorted(restore_processed_cache(cold, docs)) == sorted(names)
        for n, b in content.items():
            assert (cold / n).read_bytes() == b
        assert sorted(p.name for p in cold.iterdir()) == sorted(names)  # nothing extra
        assert restore_processed_cache(cold, docs) == []  # second restore is a no-op


@given(_subset, _subset, st.data())
def test_restore_never_clobbers_and_fills_only_gaps(published, local, data):
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        src, docs, work = tmp / "src", tmp / "docs" / "data", tmp / "work"
        src.mkdir()
        work.mkdir()
        for n in published:
            (src / n).write_bytes(b"published-" + n.encode())
        publish_processed_cache(src, docs)
        for n in local:
            (work / n).write_bytes(b"local-" + n.encode())
        restored = restore_processed_cache(work, docs)
        assert sorted(restored) == sorted(published - local)
        for n in local:
            assert (work / n).read_bytes() == b"local-" + n.encode()


def test_restore_without_snapshot_returns_empty_and_creates_nothing(tmp_path):
    cold = tmp_path / "cold"
    assert restore_processed_cache(cold, tmp_path / "docs" / "data") == []
    assert not cold.exists()


def test_publish_with_empty_data_dir_writes_empty_manifest(tmp_path):
    (tmp_path / "data").mkdir()
    assert publish_processed_cache(tmp_path / "data", tmp_path / "docs") == []
    import json
    assert json.loads((snapshot_dir(tmp_path / "docs") / MANIFEST_NAME).read_text())["files"] == []


# ---- E9: compute_offer_curves_daily cum_mw ----------------------------------
_vol = st.one_of(st.just(0.0), st.floats(0, 500, allow_nan=False), st.floats(-50, 0, allow_nan=False))
_price = st.floats(-1000, 15500, allow_nan=False, allow_infinity=False)


@st.composite
def _bids(draw):
    duids = draw(st.lists(st.sampled_from(["A1", "B2", "C3"]), min_size=1, max_size=3, unique=True))
    days = draw(st.lists(st.integers(1, 28), min_size=1, max_size=3, unique=True))
    prices, vols = [], []
    for duid in duids:
        for d in days:
            row = {"SETTLEMENTDATE": pd.Timestamp(2026, 3, d), "DUID": duid}
            row.update({c: draw(_price) for c in BAND_PRICE_COLS})
            prices.append(row)
            for hour in draw(st.lists(st.integers(5, 23), min_size=1, max_size=3, unique=True)):
                v = {"INTERVAL_DATETIME": pd.Timestamp(2026, 3, d, hour), "DUID": duid}
                v.update({c: draw(_vol) for c in BAND_AVAIL_COLS})
                vols.append(v)
    return pd.DataFrame(prices), pd.DataFrame(vols)


@given(_bids())
def test_daily_cum_mw_is_non_decreasing_and_ends_at_tranche_sum(bids):
    prices, volumes = bids
    out = compute_offer_curves_daily(prices, volumes, "2026-03")
    if out.empty:  # every tranche non-positive: no stack is emitted
        return
    for (duid, date), g in out.groupby(["duid", "date"]):
        g = g.sort_values("band")
        cum = g["cum_mw"].tolist()
        assert all(b >= a for a, b in zip(cum, cum[1:])), cum
        assert cum[0] >= 0  # a sub-0.0005 MW first tranche rounds to 0.0 (cosmetic, see report)
        day_vol = volumes[
            (volumes["DUID"] == duid)
            & (volumes["INTERVAL_DATETIME"].dt.strftime("%Y-%m-%d") == date)
        ][BAND_AVAIL_COLS].mean().clip(lower=0)
        assert cum[-1] == pytest.approx(day_vol[day_vol > 1e-9].sum(), abs=0.002)


def test_daily_cum_mw_skips_non_positive_tranches():
    p = pd.DataFrame([{"SETTLEMENTDATE": pd.Timestamp(2026, 3, 5), "DUID": "A1",
                       **{c: float(i) for i, c in enumerate(BAND_PRICE_COLS)}}])
    vol = {c: 0.0 for c in BAND_AVAIL_COLS}
    vol.update(BANDAVAIL2=10.0, BANDAVAIL5=-3.0, BANDAVAIL7=5.0)
    v = pd.DataFrame([{"INTERVAL_DATETIME": pd.Timestamp(2026, 3, 5, 12), "DUID": "A1", **vol}])
    out = compute_offer_curves_daily(p, v, "2026-03")
    assert out["band"].tolist() == [2, 7]
    assert out["cum_mw"].tolist() == [10.0, 15.0]


# ---- E10: _safe_filename ----------------------------------------------------
_REPLACED = "/#\\_"


def _same_modulo_replacement(a: str, b: str) -> bool:
    return len(a) == len(b) and all(
        x == y or (x in _REPLACED and y in _REPLACED) for x, y in zip(a, b)
    )


@given(st.text(max_size=12))
def test_safe_filename_output_has_no_path_hostile_chars(s):
    out = _safe_filename(s)
    assert not set(out) & set("/#\\")
    assert len(out) == len(s)


_ALPHA = "AB19/#\\_-"


@given(st.text(alphabet=_ALPHA, max_size=6), st.data())
def test_safe_filename_collides_iff_inputs_differ_only_in_replaced_chars(a, data):
    """Honest part of injectivity: equal outputs exactly when inputs differ solely in replaced chars."""
    mutated = "".join(data.draw(st.sampled_from(_REPLACED)) if c in _REPLACED else c for c in a)
    b = data.draw(st.one_of(st.text(alphabet=_ALPHA, max_size=6), st.just(mutated)))
    assert (_safe_filename(a) == _safe_filename(b)) == _same_modulo_replacement(a, b)


@given(st.text(alphabet="AB19/#\\_-", max_size=6), st.text(alphabet="AB19/#\\_-", max_size=6))
@example("A#1", "A_1")
@pytest.mark.xfail(
    strict=True,
    reason="DUIDs 'A#1' and 'A_1' both map to 'A_1': _safe_filename is not injective, so two units "
    "would overwrite one JSON file. Proposed fix: percent-encode '/', '#', '\\\\' and '_' "
    "(or append a short hash of the raw DUID when the name was changed).",
)
def test_safe_filename_is_injective(a, b):
    assume(a != b)
    assert _safe_filename(a) != _safe_filename(b)
