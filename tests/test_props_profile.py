import re
from pathlib import Path

import pytest

pytest.importorskip("hypothesis")


def test_every_props_module_loads_gate_profile():
    """Static scan: a test_props_*.py that forgets load_gate_profile() at import is caught."""
    mods = sorted(Path(__file__).parent.glob("test_props_*.py"))
    assert mods
    missing = [
        m.name for m in mods
        if m.name != Path(__file__).name
        and not re.search(r"^load_gate_profile\(\)", m.read_text(), re.M)
    ]
    assert not missing, f"modules not loading the gate profile at import: {missing}"


def test_gate_profile_is_loaded():
    """The profile the sibling modules load at import is the gate one (this module never loads it)."""
    import importlib

    for m in sorted(Path(__file__).parent.glob("test_props_*.py")):
        if m.name != Path(__file__).name:
            importlib.import_module(f"tests.{m.stem}")
    from hypothesis import settings

    assert settings().derandomize is True
    assert settings().database is None
