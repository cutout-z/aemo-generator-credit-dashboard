"""Fixed Hypothesis profile for the property tests.

Hypothesis is a dev-only dependency (requirements-dev.txt): the NAS lane runs
`pytest tests/` on a venv built from requirements.txt without it. This helper
imports hypothesis lazily, so only modules that already did
`pytest.importorskip("hypothesis")` call it; everything else still collects.
"""

import atexit
import shutil
import tempfile

_home = None


def load_gate_profile():
    from hypothesis import HealthCheck, configuration, settings

    global _home
    if _home is None:  # one dir per process, however many modules import this
        _home = tempfile.mkdtemp(prefix="hypothesis-")
        atexit.register(shutil.rmtree, _home, ignore_errors=True)
        configuration.set_hypothesis_home_dir(_home)
    settings.register_profile(
        "gate", derandomize=True, max_examples=100, deadline=None, database=None,
        print_blob=True, suppress_health_check=[HealthCheck.too_slow],
    )
    settings.load_profile("gate")
