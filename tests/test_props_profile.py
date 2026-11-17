import pytest

pytest.importorskip("hypothesis")
from tests.hypothesis_profile import load_gate_profile  # noqa: E402

load_gate_profile()


def test_gate_profile_is_loaded():
    from hypothesis import settings

    assert settings().derandomize is True
    assert settings().database is None
