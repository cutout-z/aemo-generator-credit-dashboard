import pandas as pd

from src.aggregate import aggregate_month
from src.download_intermittent import _summarise_quality


def test_summarise_quality_counts_elav_good_intervals_only():
    raw = pd.DataFrame(
        {
            "DUID": ["GEN1", "GEN1", "GEN1", "GEN2"],
            "SCADA_TYPE": ["ELAV", "ELAV", "MW", "ELAV"],
            "SCADA_QUALITY": ["Good", "Bad", "Good", "Good"],
        }
    )

    summary = _summarise_quality(raw, 2026, 3, "test")

    gen1 = summary.set_index("DUID").loc["GEN1"]
    assert gen1["total_intervals"] == 2
    assert gen1["good_intervals"] == 1
    assert "GEN2" in set(summary["DUID"])


def test_aggregate_month_publishes_proxy_without_causal_split():
    ts = pd.to_datetime(["2026-03-01 00:05:00", "2026-03-01 00:10:00"])
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": ["GEN1", "GEN1"], "SCADAVALUE": [5.0, 5.0]})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": ["NSW1", "NSW1"], "RRP": [100.0, 100.0]})
    dispatchload = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": ["GEN1", "GEN1"], "AVAILABILITY": [10.0, 10.0]})
    generators = pd.DataFrame(
        {
            "DUID": ["GEN1"],
            "REGION": ["NSW1"],
            "CAPACITY_MW": [10.0],
            "FUEL_CATEGORY": ["Solar"],
        }
    )
    intermittent = pd.DataFrame({"DUID": ["GEN1"], "total_intervals": [4], "good_intervals": [3]})

    result = aggregate_month(scada, prices, dispatchload, generators, {}, 2026, 3, intermittent)
    row = result.iloc[0]

    # S3-01: total shortfall proxy still published; the quality-flag
    # proportional split (which locked in a causal grid/mechanical
    # inference) must not appear even when quality summaries are supplied.
    assert row["curtailment_pct"] == 0.5
    assert "grid_curtailment_pct" not in result.columns
    assert "mechanical_curtailment_pct" not in result.columns


def test_dispatchload_fetch_tolerates_missing_uigf(monkeypatch):
    """S3-01 schema boundary: nemosis versions / archive vintages without the
    UIGF column must degrade to a NaN-filled column, not fail the month."""
    import src.download_scada as ds

    ts = pd.to_datetime(["2026-03-01 00:05:00"])
    frame = pd.DataFrame({
        "SETTLEMENTDATE": ts, "DUID": ["GEN1"],
        "AVAILABILITY": [10.0], "INTERVENTION": [0],
    })

    calls = {"n": 0}

    def fake_compiler(**kwargs):
        calls["n"] += 1
        if "UIGF" in kwargs["select_columns"]:
            raise RuntimeError("column not known to nemosis")
        return frame.copy()

    monkeypatch.setattr(ds, "dynamic_data_compiler", fake_compiler)
    out = ds.fetch_dispatchload_month(2026, 3, "/tmp/never-used")

    assert calls["n"] == 2  # first attempt with UIGF, retry without
    assert "UIGF" in out.columns
    assert out["UIGF"].isna().all()
    assert out["AVAILABILITY"].iloc[0] == 10.0


def test_dispatchload_fetch_passes_uigf_through(monkeypatch):
    """When nemosis knows UIGF, values survive the INTERVENTION filter."""
    import src.download_scada as ds

    ts = pd.to_datetime(["2026-03-01 00:05:00", "2026-03-01 00:10:00", "2026-03-01 00:15:00"])
    frame = pd.DataFrame({
        "SETTLEMENTDATE": ts, "DUID": ["GEN1", "GEN1", "OTHER"],
        "AVAILABILITY": [10.0, 12.0, 50.0],
        "INTERVENTION": [0, 0, 0],
        "UIGF": [9.5, 11.0, float("nan")],
    })

    monkeypatch.setattr(ds, "dynamic_data_compiler", lambda **k: frame.copy())
    out = ds.fetch_dispatchload_month(2026, 3, "/tmp/never-used")

    assert out["UIGF"].isna().tolist() == [False, False, True]
    assert out["UIGF"].iloc[0] == 9.5
    assert out["UIGF"].iloc[1] == 11.0
    assert len(out) == 3
