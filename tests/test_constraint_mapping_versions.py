"""Constraint-to-connection-point mapping follows constraint versions (L8).

SPDCONNECTIONPOINTCONSTRAINT was reduced to the latest row per (connection
point, constraint) pair, ignoring EFFECTIVEDATE/VERSIONNO, so a connection
point that a later version of a constraint dropped stayed mapped to it and
kept collecting its binding hours.
"""

import pandas as pd

from src.aggregate import aggregate_constraints_month
from src.download_constraints import spdcp_mapping_asof


def _spdcp():
    return pd.DataFrame({
        "CONNECTIONPOINTID": ["CP_A", "CP_B", "CP_A"],
        "GENCONID": ["X", "X", "X"],
        "EFFECTIVEDATE": pd.to_datetime(["2024-01-01", "2024-01-01", "2025-01-01"]),
        "VERSIONNO": [1, 1, 1],
        "FACTOR": [1.0, 1.0, 1.0], "BIDTYPE": ["ENERGY"] * 3,
    })


def _binding(month):
    ts = pd.date_range(f"{month}-10 10:05", periods=12, freq="5min")
    return pd.DataFrame({"SETTLEMENTDATE": ts, "CONSTRAINTID": "X", "MARGINALVALUE": 5.0})


def test_mapping_is_the_version_in_force():
    assert spdcp_mapping_asof(_spdcp(), "2024-06-30") == {"CP_A": {"X"}, "CP_B": {"X"}}
    assert spdcp_mapping_asof(_spdcp(), "2025-06-30") == {"CP_A": {"X"}}
    assert spdcp_mapping_asof(_spdcp(), "2023-06-30") == {}


def test_dropped_connection_point_stops_collecting_binding_hours():
    cp_map = {"GEN_A": "CP_A", "GEN_B": "CP_B"}
    before = aggregate_constraints_month(_binding("2024-06"), _spdcp(), pd.DataFrame(), cp_map, 2024, 6)
    after = aggregate_constraints_month(_binding("2025-06"), _spdcp(), pd.DataFrame(), cp_map, 2025, 6)
    assert set(before["duid"]) == {"GEN_A", "GEN_B"}
    assert set(after["duid"]) == {"GEN_A"}
    assert after["hours_bound"].tolist() == [1.0]
