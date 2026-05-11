import pandas as pd
import pytest

from src.main import _assert_protected_months_unchanged


def test_settled_history_guard_allows_mutable_window_changes():
    before = pd.DataFrame(
        {
            "duid": ["A", "A"],
            "month": ["2026-01", "2026-02"],
            "value": [1.0, 2.0],
        }
    )
    after = pd.DataFrame(
        {
            "duid": ["A", "A"],
            "month": ["2026-01", "2026-02"],
            "value": [1.0, 3.0],
        }
    )

    _assert_protected_months_unchanged(
        before,
        after,
        {"2026-02"},
        month_col="month",
        label="test",
    )


def test_settled_history_guard_rejects_historical_rewrite():
    before = pd.DataFrame(
        {
            "duid": ["A", "A"],
            "month": ["2026-01", "2026-02"],
            "value": [1.0, 2.0],
        }
    )
    after = pd.DataFrame(
        {
            "duid": ["A", "A"],
            "month": ["2026-01", "2026-02"],
            "value": [9.0, 2.0],
        }
    )

    with pytest.raises(RuntimeError, match="settled months"):
        _assert_protected_months_unchanged(
            before,
            after,
            {"2026-02"},
            month_col="month",
            label="test",
        )
