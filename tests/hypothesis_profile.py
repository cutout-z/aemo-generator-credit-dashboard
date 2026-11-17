"""Fixed Hypothesis profile for the property tests.

Hypothesis is a dev-only dependency (requirements-dev.txt): the NAS lane runs
`pytest tests/` on a venv built from requirements.txt without it. This helper
imports hypothesis lazily, so only modules that already did
`pytest.importorskip("hypothesis")` call it; everything else still collects.
"""

import tempfile


def load_gate_profile():
    from hypothesis import HealthCheck, configuration, settings

    configuration.set_hypothesis_home_dir(tempfile.mkdtemp(prefix="hypothesis-"))
    settings.register_profile(
        "gate", derandomize=True, max_examples=100, deadline=None, database=None,
        print_blob=True, suppress_health_check=[HealthCheck.too_slow],
    )
    settings.load_profile("gate")
