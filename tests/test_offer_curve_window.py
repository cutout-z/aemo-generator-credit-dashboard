"""Published daily offer stacks stay bounded whatever window a run processed (no network)."""

import pandas as pd

from src import config
from src.offer_curves import bound_published_curves


def _curves(rows):
    return pd.DataFrame(rows, columns=["duid", "date", "price", "cum_mw"])


def test_full_refresh_window_is_cut_to_the_last_two_months():
    days = pd.date_range("2024-08-01", "2026-08-31", freq="D").strftime("%Y-%m-%d")
    curves = _curves([("BW02", d, 10.0, 100.0) for d in days])
    out = bound_published_curves(curves, ["BW02"], 2)
    assert out["date"].min() == "2026-07-01" and out["date"].max() == "2026-08-31"
    assert len(out) == 62


def test_unlisted_duids_get_no_file():
    curves = _curves([("ADPBA1", "2026-08-31", 1.0, 5.0), ("ADPBA1G", "2026-08-31", 1.0, 5.0),
                      ("CAPBES1L", "2026-08-31", 1.0, 5.0)])
    out = bound_published_curves(curves, ["ADPBA1", "BW02"], 2)
    assert sorted(out["duid"].unique()) == ["ADPBA1"]


def test_daily_lane_window_is_unchanged():
    days = pd.date_range("2026-07-01", "2026-08-31", freq="D").strftime("%Y-%m-%d")
    curves = _curves([("BW02", d, 10.0, 100.0) for d in days])
    assert len(bound_published_curves(curves, ["BW02"], config.OFFER_CURVE_WINDOW_MONTHS)) == len(curves)


def test_empty_input_passes_through():
    assert bound_published_curves(_curves([]), ["BW02"], 2).empty
