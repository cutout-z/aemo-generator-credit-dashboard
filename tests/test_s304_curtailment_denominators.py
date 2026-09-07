"""S3-04: aggregated curtailment and price-bin denominators are ratio-of-sums.

Covers the three S3-04 defects:
1. FY curtailment weighted monthly percentages by actual generation — a fully
   curtailed month has zero generation and therefore zero weight, hiding the
   worst period. Fixed: 1 − Σ eligible actual / Σ potential over the FY.
2. Station curtailment weighted unit percentages by nameplate, not each unit's
   monthly potential energy. Fixed: same ratio-of-sums over member units.
3. Spot-price exposure averaged monthly histograms equally, including
   zero-generation months. Fixed: generation-weighted, zero-gen months
   excluded, unknown when the whole window has no (priced) generation.
"""

import json

import pandas as pd
import pytest

from src import config
from src.generate_json import generate_all, write_curtailment_by_fy

# config.PRICE_BIN_LABELS: "< -100", "-100 to -90", ..., "-10 to 0", "0 to 10",
# ..., "> 100". Negative-price bins are "< -100" plus every range whose upper
# edge is ≤ 0 (a 0$ price is not negative — digitize puts RRP 0 in "0 to 10").
NEGATIVE = {"< -100"} | {f"{lo} to {lo + 10}" for lo in range(-100, 0, 10)}


def negative_share(pd_):
    return sum(
        s for lab, s in zip(pd_["bins"], pd_["generation_share"])
        if lab in NEGATIVE
    )


def _month_row(duid, month, gen, curt_pct=None, pot=None, act=None):
    """One monthly aggregate row. pot/act present ⇒ v3 schema columns.
    gen=None emits NaN (a month with no SCADA rows at all)."""
    g = gen if gen is not None else float("nan")
    row = {
        "duid": duid,
        "month": month,
        "generation_mwh": g,
        "revenue_aud": g * 50.0,
        "capacity_factor": 0.3,
        "curtailment_pct": curt_pct,
        "econ_curtailment_pct": None,
        "captured_price": 50.0,
        "avg_rrp": 55.0,
        "price_capture_ratio": 0.91,
    }
    if pot is not None:
        row["curtailment_potential_mwh"] = pot
        row["curtailment_actual_mwh"] = (
            act if act is not None else round(float(g), 1)
        )
    return row


def _coerce_num(df):
    """Real monthly frames come from feather (float64, NaN for missing); build
    fixture frames to match instead of object dtype from None records."""
    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c])
        except (TypeError, ValueError):
            pass
    return df


def _generator(duid, cap, fuel="Wind"):
    return {
        "DUID": [duid],
        "STATION_NAME": [f"{duid} Farm"],
        "REGION": ["NSW1"],
        "FUEL_CATEGORY": [fuel],
        "CAPACITY_MW": [cap],
        "TECHNOLOGY": [fuel],
        "CONNECTION_POINT": [f"CP_{duid}"],
    }


# ─── FY curtailment ──────────────────────────────────────────────────────────


def test_fy_curtailment_is_ratio_of_energy_sums(tmp_path):
    """Review's synthetic case: equal 100 MWh potential months, one generating
    fully and one fully curtailed. Old generation-weighted mean published 0%
    (curtailed month had no generation to carry its weight); ratio of sums
    gives 1 − 100/200 = 0.5."""
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND1", "2026-01", 100.0, curt_pct=0.0, pot=100.0),
        _month_row("WIND1", "2026-02", 0.0, curt_pct=1.0, pot=100.0, act=0.0),
    ]))
    path = write_curtailment_by_fy(monthly, str(tmp_path))
    df = pd.read_csv(path)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["fy_start"] == 2025  # Jul-25..Jun-26 → FY25-26
    assert row["fy_label"] == "FY25-26"
    assert row["curtailment_pct"] == pytest.approx(0.5)
    assert row["metric_version"] == config.CURTAILMENT_METRIC_VERSION
    assert row["months_covered"] == 2
    assert row["generation_mwh"] == 100
    # Downstream consumer schema (AEMO Renewable Generator Dashboard) intact
    assert set(df.columns) == {
        "duid", "fy_start", "fy_label", "curtailment_pct",
        "metric_version", "generation_mwh", "months_covered",
    }


def test_fy_curtailment_legacy_schema_derives_potential(tmp_path):
    """Months cached before the v3 columns carry only generation + pct.
    Potential is recovered as generation/(1 − pct): 1000/0.8=1250 and
    3000/0.6=5000 → 1 − 4000/6250 = 0.36 (old generation-weighted mean: 0.35)."""
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND1", "2026-01", 1000.0, curt_pct=0.2),
        _month_row("WIND1", "2026-02", 3000.0, curt_pct=0.4),
    ]))
    assert "curtailment_potential_mwh" not in monthly.columns
    df = pd.read_csv(write_curtailment_by_fy(monthly, str(tmp_path)))
    assert df.iloc[0]["curtailment_pct"] == pytest.approx(0.36)
    assert df.iloc[0]["months_covered"] == 2


def test_fy_curtailment_skips_fy_without_derivable_energy(tmp_path):
    """A legacy fully-curtailed month (pct == 1, no generation) has no
    recoverable potential — it cannot distort the ratio and the FY is not
    published when nothing is derivable (the monthly proxy itself is unknown)."""
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND1", "2026-01", 0.0, curt_pct=1.0),
        _month_row("WIND1", "2026-02", None, curt_pct=None),
    ]))
    df = pd.read_csv(write_curtailment_by_fy(monthly, str(tmp_path)))
    assert df.empty


def test_fy_curtailment_fully_curtailed_month_counts_with_v3_columns(tmp_path):
    """With the v3 columns a fully-curtailed month contributes actual 0 /
    potential 1000 to the sums instead of vanishing — 1 − 1000/3000 ≈ 0.6667."""
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND1", "2026-01", 1000.0, curt_pct=0.0, pot=1000.0),
        _month_row("WIND1", "2026-02", 0.0, curt_pct=1.0, pot=1000.0, act=0.0),
        _month_row("WIND1", "2026-03", 1000.0, curt_pct=0.0, pot=1000.0),
    ]))
    df = pd.read_csv(write_curtailment_by_fy(monthly, str(tmp_path)))
    assert df.iloc[0]["curtailment_pct"] == pytest.approx(1 / 3, abs=1e-4)
    assert df.iloc[0]["months_covered"] == 3


# ─── Station curtailment ─────────────────────────────────────────────────────


def test_station_curtailment_uses_potential_energy_not_nameplate(tmp_path):
    """Review's station case: equal potential (1000 MWh) but 100/300 MW
    nameplates; unit A uncurtailed, unit B fully curtailed. Nameplate-weighted
    mean of unit percentages publishes 0.75; summed-energy ratio is 0.5."""
    generators = pd.DataFrame({
        "DUID": ["WIND_A", "WIND_B"],
        "STATION_NAME": ["Equal Potential Wind Farm", "Equal Potential Wind Farm"],
        "REGION": ["NSW1", "NSW1"],
        "FUEL_CATEGORY": ["Wind", "Wind"],
        "CAPACITY_MW": [100.0, 300.0],
        "TECHNOLOGY": ["Wind", "Wind"],
        "CONNECTION_POINT": ["CP_A", "CP_B"],
    })
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND_A", "2026-04", 1000.0, curt_pct=0.0, pot=1000.0),
        _month_row("WIND_B", "2026-04", 0.0, curt_pct=1.0, pot=1000.0, act=0.0),
    ]))
    generate_all(generators, monthly_aggregates=monthly,
                 output_dir=str(tmp_path / "generators"))
    with open(tmp_path / "generators" / "station_Equal_Potential_Wind_Farm.json") as f:
        station = json.load(f)
    assert station["monthly"]["curtailment_pct"] == [0.5]
    assert station["monthly"]["curtailment_metric_version"] == config.CURTAILMENT_METRIC_VERSION


def test_station_curtailment_mixed_schema_rows(tmp_path):
    """One member on v3 columns, one legacy (NaN potential) — each row resolves
    through its own path and the station month is still ratio-of-sums."""
    generators = pd.DataFrame({
        "DUID": ["WIND_A", "WIND_B"],
        "STATION_NAME": ["Mixed Schema Wind", "Mixed Schema Wind"],
        "REGION": ["NSW1", "NSW1"],
        "FUEL_CATEGORY": ["Wind", "Wind"],
        "CAPACITY_MW": [100.0, 100.0],
        "TECHNOLOGY": ["Wind", "Wind"],
        "CONNECTION_POINT": ["CP_A", "CP_B"],
    })
    legacy_b = _month_row("WIND_B", "2026-04", 250.0, curt_pct=0.5)
    monthly = _coerce_num(pd.DataFrame([
        _month_row("WIND_A", "2026-04", 250.0, curt_pct=0.5, pot=500.0),
        legacy_b,
    ]))
    # B is a legacy row inside a v3 frame → potential column exists but is NaN
    assert monthly.loc[1, "curtailment_potential_mwh"] != monthly.loc[1, "curtailment_potential_mwh"]  # NaN
    generate_all(generators, monthly_aggregates=monthly,
                 output_dir=str(tmp_path / "generators"))
    with open(tmp_path / "generators" / "station_Mixed_Schema_Wind.json") as f:
        station = json.load(f)
    # A: 250/500; B legacy: potential 250/0.5 = 500 → 1 − 500/1000 = 0.5
    assert station["monthly"]["curtailment_pct"] == [0.5]


# ─── Price distribution ──────────────────────────────────────────────────────


def _price_monthly(dist_by_month):
    """Build a unit's monthly frame with price bins from per-month dicts
    {month: {gen_mwh: float, bins: {label: share}}} (v3: mwh cols included)."""
    rows = []
    for month, spec in dist_by_month.items():
        rows.append(_month_row("SOLAR1", month, spec["gen_mwh"]))
    frame = pd.DataFrame(rows)
    for i, (month, spec) in enumerate(dist_by_month.items()):
        for lab in config.PRICE_BIN_LABELS:
            share = spec["bins"].get(lab, 0.0)
            frame.loc[i, f"price_dist_{lab}"] = share
            frame.loc[i, f"price_mwh_{lab}"] = round(share * spec["gen_mwh"], 1)
    return _coerce_num(frame)


def _unit_json(tmp_path, monthly, duid="SOLAR1", fuel="Solar"):
    generators = pd.DataFrame(_generator(duid, 100.0, fuel=fuel))
    generate_all(generators, monthly_aggregates=monthly,
                 output_dir=str(tmp_path / "generators"))
    with open(tmp_path / "generators" / f"{duid}.json") as f:
        return json.load(f)


def test_price_distribution_generation_weighted(tmp_path):
    """100 MWh at 50% negative in month 1, 300 MWh at 0% negative in month 2,
    a zero-generation month 3. Equal monthly averaging gives negative share
    0.5·(1/2 included months)=… the old mean incl. zero month diluted to
    (0.5+0+0)/3; generation-weighted = 50/400 = 0.125 and shares sum to 1."""
    monthly = _price_monthly({
        "2026-01": {"gen_mwh": 100.0, "bins": {"-10 to 0": 0.5, "0 to 10": 0.5}},
        "2026-02": {"gen_mwh": 300.0, "bins": {"0 to 10": 1.0}},
        "2026-03": {"gen_mwh": 0.0, "bins": {"0 to 10": 1.0}},
    })
    doc = _unit_json(tmp_path, monthly)
    pd_ = doc["price_distribution"]
    assert negative_share(pd_) == pytest.approx(0.125, abs=1e-3)
    assert sum(pd_["generation_share"]) == pytest.approx(1.0, abs=2e-3)
    # Per-bin MWh preserved: negative bins hold the 50 MWh of month 1
    neg_mwh = sum(m for lab, m in zip(pd_["bins"], pd_["mwh"]) if lab in NEGATIVE)
    assert neg_mwh == pytest.approx(50.0, abs=0.6)
    assert sum(pd_["mwh"]) == pytest.approx(400.0, abs=0.6)


def test_price_distribution_legacy_schema_equivalent(tmp_path):
    """Same months on the legacy schema (shares only, no price_mwh_* columns):
    rows contribute generation × share — identical 0.125 for full coverage."""
    monthly = _price_monthly({
        "2026-01": {"gen_mwh": 100.0, "bins": {"-10 to 0": 0.5, "0 to 10": 0.5}},
        "2026-02": {"gen_mwh": 300.0, "bins": {"0 to 10": 1.0}},
        "2026-03": {"gen_mwh": 0.0, "bins": {"0 to 10": 1.0}},
    })
    legacy = monthly.drop(columns=[c for c in monthly.columns if c.startswith("price_mwh_")])
    assert not any(c.startswith("price_mwh_") for c in legacy.columns)
    doc = _unit_json(tmp_path, legacy)
    pd_ = doc["price_distribution"]
    assert negative_share(pd_) == pytest.approx(0.125, abs=1e-3)
    assert sum(pd_["generation_share"]) == pytest.approx(1.0, abs=2e-3)
    # mwh recovered from share × generation
    assert sum(pd_["mwh"]) == pytest.approx(400.0, abs=1.0)


def test_price_distribution_mixed_schema_rows(tmp_path):
    """v3 row + legacy rows in one frame: per-row resolution, same result."""
    monthly = _price_monthly({
        "2026-01": {"gen_mwh": 100.0, "bins": {"-10 to 0": 0.5, "0 to 10": 0.5}},
        "2026-02": {"gen_mwh": 300.0, "bins": {"0 to 10": 1.0}},
        "2026-03": {"gen_mwh": 0.0, "bins": {"0 to 10": 1.0}},
    })
    mixed = monthly.copy()
    # month 2 + 3 become legacy rows (v3 columns exist but rows are NaN)
    for c in [c for c in mixed.columns if c.startswith("price_mwh_")]:
        mixed.loc[1:2, c] = float("nan")
    doc = _unit_json(tmp_path, mixed)
    assert negative_share(doc["price_distribution"]) == pytest.approx(0.125, abs=1e-3)


def test_price_distribution_unknown_when_whole_window_empty(tmp_path):
    """Zero generation across the whole window → block omitted (unknown),
    not a zeroed '% of Generation' histogram."""
    monthly = _price_monthly({
        "2026-01": {"gen_mwh": 0.0, "bins": {"0 to 10": 1.0}},
        "2026-02": {"gen_mwh": 0.0, "bins": {"0 to 10": 1.0}},
    })
    doc = _unit_json(tmp_path, monthly)
    assert "price_distribution" not in doc


# ─── Aggregate layer emits the first-class energy fields ─────────────────────


def test_aggregate_month_emits_energy_denominators():
    from src.aggregate import aggregate_month

    ts = pd.to_datetime(["2026-03-01 00:05:00", "2026-03-01 00:10:00"])
    scada = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": ["GEN1", "GEN1"],
                          "SCADAVALUE": [5.0, 5.0]})
    prices = pd.DataFrame({"SETTLEMENTDATE": ts, "REGIONID": ["NSW1", "NSW1"],
                           "RRP": [100.0, 100.0]})
    dispatchload = pd.DataFrame({"SETTLEMENTDATE": ts, "DUID": ["GEN1", "GEN1"],
                                 "AVAILABILITY": [10.0, 10.0]})
    generators = pd.DataFrame({
        "DUID": ["GEN1"], "REGION": ["NSW1"], "CAPACITY_MW": [10.0],
        "FUEL_CATEGORY": ["Solar"],
    })
    result = aggregate_month(scada, prices, dispatchload, generators, {}, 2026, 3)
    row = result.iloc[0]
    assert row["curtailment_pct"] == 0.5
    # 10 MW × 2 intervals / 12 → potential 1.7 MWh; 5 MW × 2 / 12 → actual 0.8
    assert row["curtailment_potential_mwh"] == pytest.approx(1.7)
    assert row["curtailment_actual_mwh"] == pytest.approx(0.8)
    # 5-minute SCADA at $100 → all priced generation in the '> 100' bin
    assert row["price_dist_> 100"] == pytest.approx(1.0)
    assert row["price_mwh_> 100"] == pytest.approx(0.8)
    # Non-curtailment fuel: no energy fields, no proxy
    result2 = aggregate_month(scada, prices, dispatchload,
                              generators.assign(FUEL_CATEGORY="Battery"), {}, 2026, 3)
    row2 = result2.iloc[0]
    assert pd.isna(row2["curtailment_pct"])
    assert pd.isna(row2["curtailment_potential_mwh"])
    assert pd.isna(row2["curtailment_actual_mwh"])
    assert row2["price_mwh_> 100"] == pytest.approx(0.8)
