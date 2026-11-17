"""Property tests for semantic_publish (stamp-only changes never republish)."""
import copy
import json
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("hypothesis")
from hypothesis import assume, given, strategies as st  # noqa: E402

from src.semantic_publish import facts_equal, write_json_if_facts_changed  # noqa: E402
from tests.hypothesis_profile import load_gate_profile  # noqa: E402

load_gate_profile()

STAMP = "updated_utc"
_leaf = st.one_of(
    st.none(), st.booleans(), st.integers(-10**6, 10**6),
    st.floats(allow_nan=False, allow_infinity=False), st.text(max_size=8),
)
_key = st.text(alphabet="abcxyz_", min_size=1, max_size=4)
_json = st.recursive(
    _leaf,
    lambda c: st.one_of(st.lists(c, max_size=3), st.dictionaries(_key, c, max_size=3)),
    max_leaves=12,
)
payloads = st.dictionaries(st.one_of(_key, st.just(STAMP)), _json, max_size=4)


def _leaf_paths(node, path=()):
    if isinstance(node, dict) and node:
        for k, v in node.items():
            yield from _leaf_paths(v, path + (k,))
    elif isinstance(node, list) and node:
        for i, v in enumerate(node):
            yield from _leaf_paths(v, path + (i,))
    else:
        yield path


@given(payloads, _json, st.permutations(range(8)))
def test_stamp_only_change_is_equal(payload, new_stamp, perm):
    other = copy.deepcopy(payload)
    other[STAMP] = new_stamp
    assert facts_equal(payload, other, (STAMP,))
    assert facts_equal(payload, other, STAMP)  # bare-string form
    # key order is irrelevant
    shuffled = dict(reversed(list(other.items())))
    assert facts_equal(payload, shuffled, (STAMP,))


@given(payloads, st.data())
def test_any_non_stamp_leaf_change_is_unequal(payload, data):
    paths = [p for p in _leaf_paths(payload) if p and p[0] != STAMP]
    assume(paths)
    path = data.draw(st.sampled_from(paths))
    changed = copy.deepcopy(payload)
    parent = changed
    for step in path[:-1]:
        parent = parent[step]
    parent[path[-1]] = [parent[path[-1]]]  # wrapping always changes the leaf
    assert not facts_equal(changed, payload, (STAMP,))


@given(payloads, _json, _json)
def test_second_write_is_byte_identical_noop(payload, stamp_a, stamp_b):
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "sub" / "f.json"
        assert write_json_if_facts_changed(path, payload, stamp_keys=(STAMP,)) is True
        first = path.read_bytes()
        again = copy.deepcopy(payload)
        again[STAMP] = stamp_a
        assert write_json_if_facts_changed(path, again, stamp_keys=(STAMP,)) is False
        assert path.read_bytes() == first
        again[STAMP] = stamp_b
        assert write_json_if_facts_changed(path, again, stamp_keys=(STAMP,)) is False
        assert path.read_bytes() == first
        assert json.loads(first) == json.loads(json.dumps(payload))


@given(payloads, _json)
def test_changed_fact_rewrites(payload, newval):
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "f.json"
        write_json_if_facts_changed(path, payload, stamp_keys=(STAMP,))
        changed = {**payload, "zz_new": [newval]}
        assert write_json_if_facts_changed(path, changed, stamp_keys=(STAMP,)) is True
        assert json.loads(path.read_text())["zz_new"] == json.loads(json.dumps([newval]))
