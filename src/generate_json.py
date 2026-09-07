"""Generate per-generator JSON files and search index for the frontend."""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path

import numpy as np
import pandas as pd

from . import config
from .aggregate import (
    REVENUE_MLF_SOURCE_FY_COL,
    REVENUE_MLF_STATUS_COL,
    REVENUE_MLF_VALUE_COL,
    MLF_STATUS_PRIOR_CARRY,
    MLF_STATUS_UNKNOWN,
)

logger = logging.getLogger(__name__)


def _fy_label_or_none(src_fy_start) -> str | None:
    """Format a fy_start_year as 'FY25-26'; None for missing/NaN."""
    if src_fy_start is None:
        return None
    try:
        if isinstance(src_fy_start, float) and math.isnan(src_fy_start):
            return None
        start = int(src_fy_start)
    except (TypeError, ValueError):
        return None
    return config.fy_label(start)


def _add_revenue_mlf_provenance(monthly_doc: dict, monthly_data: pd.DataFrame) -> None:
    """Attach per-month revenue-MLF provenance arrays to a doc['monthly'] block.

    S3-11: every revenue_aud value is either MLF-adjusted (status ``exact`` or
    ``prior-carry`` — the applied factor and its source FY are recorded) or
    UNADJUSTED spot revenue when the DUID had no factor at or before that
    month's FY (status ``unknown``, value None) — which is PROVISIONAL by
    contract and must never be presented as an MLF-adjusted figure.

    The arrays parallel ``months``. Rows aggregated before the S3-11
    provenance columns existed carry None entries; a frame with no recorded
    provenance at all simply omits the keys (absence is not 'all exact').
    """
    if REVENUE_MLF_STATUS_COL not in monthly_data.columns:
        return
    statuses = monthly_data[REVENUE_MLF_STATUS_COL]
    if statuses.dropna().empty:
        return  # legacy frame — no month has recorded provenance
    monthly_doc["revenue_mlf_status"] = [
        None if pd.isna(v) else str(v) for v in statuses
    ]
    if REVENUE_MLF_SOURCE_FY_COL in monthly_data.columns:
        monthly_doc["revenue_mlf_source_fy"] = [
            _fy_label_or_none(v) for v in monthly_data[REVENUE_MLF_SOURCE_FY_COL]
        ]
    if REVENUE_MLF_VALUE_COL in monthly_data.columns:
        monthly_doc["revenue_mlf_value"] = [
            None if pd.isna(v) else float(v)
            for v in monthly_data[REVENUE_MLF_VALUE_COL]
        ]


def _safe_filename(duid: str) -> str:
    """Sanitize DUID for use as a filename (replace / # etc.)."""
    return duid.replace("/", "_").replace("#", "_").replace("\\", "_")


def _text(value, default: str = "") -> str:
    """Stringify a metadata cell for JSON — never emit the literal 'nan'.

    NaN/None/empty become ``default`` ('' unless overridden), so missing
    metadata serializes as null/empty rather than the string "nan"
    (S3-06: GENSETID-era appends leaked 'nan' region/station/technology
    values into index.json and the dashboard region filter).
    """
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    text = str(value)
    if text.strip().lower() == "nan":
        return default
    return text


def _add_daily_coverage(daily_doc: dict, daily_rows: pd.DataFrame) -> None:
    """Publish per-day observed/expected interval counts into a doc['daily'] block.

    S3-05: daily rows carry intervals_observed/intervals_expected (288 = full
    day). Rows cached before S3-05 have no counts (NaN = coverage unknown).
    The parallel arrays are emitted only when at least one row in the window
    carries counts; null entries mark individual legacy days. Absent keys
    mean the whole window predates the coverage columns — consumers must not
    read absence as "all days complete".
    """
    col = "intervals_observed"
    if col not in daily_rows.columns or daily_rows[col].dropna().empty:
        return
    exp_col = "intervals_expected" if "intervals_expected" in daily_rows.columns else None
    daily_doc["intervals_observed"] = [
        None if pd.isna(v) else int(v) for v in daily_rows[col]
    ]
    if exp_col is not None:
        daily_doc["intervals_expected"] = [
            None if pd.isna(v) else int(v) for v in daily_rows[exp_col]
        ]


def generate_index(
    generators: pd.DataFrame,
    output_dir: str | None = None,
    market: str = "NEM",
) -> Path:
    """Write index.json with searchable generator list.

    Each entry contains metadata for the search/autocomplete UI.
    Merges additively with existing entries — running the pipeline
    preserves any manually added entries from other sources.
    """
    out_dir = Path(output_dir or config.DOCS_DATA_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    index_path = out_dir / "index.json"

    new_entries = []
    for _, row in generators.iterrows():
        duid = _text(row.get("DUID", ""))
        entry = {
            "duid": duid,
            "file": _safe_filename(duid),
            "station_name": _text(row.get("STATION_NAME", "")),
            "region": _text(row.get("REGION", "")),
            "fuel_category": _text(row.get("FUEL_CATEGORY", "")),
            "capacity_mw": round(float(row["CAPACITY_MW"]), 1) if pd.notna(row.get("CAPACITY_MW")) else None,
            "technology": _text(row.get("TECHNOLOGY", "")),
            "connection_point": _text(row.get("CONNECTION_POINT", "")),
            "market": _text(row.get("MARKET", market)),
        }
        new_entries.append(entry)

    # Additive merge: load existing, remove this market's individual entries
    # (station entries are handled separately in _generate_station_files)
    if index_path.exists():
        existing = json.loads(index_path.read_text())
        preserved = [
            e for e in existing
            if e.get("market", "NEM") != market or e.get("type") == "station"
        ]
    else:
        preserved = []

    all_entries = preserved + new_entries
    all_entries.sort(key=lambda e: (e.get("station_name", ""), e.get("duid", "")))

    index_path.write_text(json.dumps(_sanitize(all_entries), indent=None, separators=(",", ":")))
    logger.info(f"Wrote index.json: {len(new_entries)} {market} generators "
                f"+ {len(preserved)} preserved ({index_path.stat().st_size / 1024:.1f} KB)")
    return index_path


def generate_generator_json(
    duid: str,
    metadata: dict,
    monthly_data: pd.DataFrame | None = None,
    mlf_data: dict | None = None,
    price_distribution: dict | None = None,
    draft_mlf: float | None = None,
    draft_fy_label: str | None = None,
    fcas_monthly: dict | None = None,
    daily_data: pd.DataFrame | None = None,
    constraint_data: pd.DataFrame | None = None,
    fcas_factor_rows: pd.DataFrame | None = None,
    output_dir: str | None = None,

    offer_factor_rows=None,
    offer_curve_rows=None,
    factor_source_status: dict | None = None,
) -> Path:
    """Write a single generator's JSON file with all dashboard data.

    Args:
        duid: Generator DUID
        metadata: Dict with station_name, region, fuel_category, etc.
        monthly_data: DataFrame with monthly time-series (optional, Phase 2+)
        mlf_data: Dict with years/values arrays (optional, Phase 3+)
        price_distribution: Dict with bins/generation_share (optional, Phase 4+)
    """
    out_dir = Path(output_dir or config.GENERATORS_JSON_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{_safe_filename(duid)}.json"

    doc = {
        "duid": duid,
        "station_name": metadata.get("station_name", ""),
        "region": metadata.get("region", ""),
        "fuel_category": metadata.get("fuel_category", ""),
        "capacity_mw": metadata.get("capacity_mw"),
        "technology": metadata.get("technology", ""),
        "connection_point": metadata.get("connection_point", ""),
        "market": metadata.get("market", "NEM"),
        "lgc_eligible": metadata.get("fuel_category", "") in config.LGC_ELIGIBLE_FUEL_TYPES,
    }

    # Monthly time-series (Phase 2+)
    if monthly_data is not None and not monthly_data.empty:
        doc["monthly"] = {
            "months": monthly_data["month"].tolist(),
            "generation_mwh": monthly_data["generation_mwh"].round(0).tolist(),
            "revenue_aud": monthly_data["revenue_aud"].round(0).tolist(),
            "capacity_factor": monthly_data["capacity_factor"].round(4).tolist(),
        }
        # S3-11: per-month revenue-MLF provenance. Every month's revenue_aud
        # carries which factor was applied and where it came from; a month
        # whose DUID had NO factor at or before that month's FY is UNADJUSTED
        # spot revenue (status "unknown", value None) and must be read as
        # provisional. Months without provenance columns (legacy aggregates
        # produced before S3-11) simply omit the arrays.
        _add_revenue_mlf_provenance(doc["monthly"], monthly_data)
        # Curtailment only for solar/wind
        # S3-01: curtailment_pct is a forecast-to-output shortfall proxy
        # (1 − SCADA/AVAILABILITY). The former grid/mechanical causal split
        # is removed; metric_version marks the schema+methodology generation.
        if "curtailment_pct" in monthly_data.columns:
            doc["monthly"]["curtailment_pct"] = monthly_data["curtailment_pct"].round(4).tolist()
            doc["monthly"]["curtailment_metric_version"] = config.CURTAILMENT_METRIC_VERSION
        if "econ_curtailment_pct" in monthly_data.columns:
            doc["monthly"]["econ_curtailment_pct"] = monthly_data["econ_curtailment_pct"].round(4).tolist()
        # Price capture
        if "captured_price" in monthly_data.columns:
            doc["monthly"]["captured_price"] = monthly_data["captured_price"].round(2).tolist()
            doc["monthly"]["avg_rrp"] = monthly_data["avg_rrp"].round(2).tolist()
            doc["monthly"]["price_capture_ratio"] = monthly_data["price_capture_ratio"].round(4).tolist()

    # MLF history (Phase 3+)
    if mlf_data:
        doc["mlf"] = mlf_data
        # Append draft MLF if available
        if draft_mlf is not None and draft_fy_label:
            doc["mlf"]["draft_year"] = draft_fy_label
            doc["mlf"]["draft_value"] = round(draft_mlf, 6)

    # Price distribution (Phase 4+)
    if price_distribution:
        doc["price_distribution"] = price_distribution

    # FCAS regional price context. IMPORTANT: these are REGIONAL market
    # averages (DISPATCHPRICE) — every generator in the same region carries
    # identical values by construction. They describe the market the unit
    # sells into, NOT that unit's FCAS revenue. (The Apr-2026 audit's
    # "WANDSF1/EMERASF1 100% duplication" P1 was this design, unlabelled.)
    # Generator-specific FCAS behaviour lives in doc["fcas_participation"].
    if fcas_monthly:
        doc["fcas"] = {
            "scope": "regional_average",
            "note": (
                "Regional FCAS market price averages — identical for all "
                "generators in this region. Not unit-level revenue."
            ),
            **fcas_monthly,
        }

    # Per-DUID FCAS participation factors (BIDPEROFFER_D offers)
    from .fcas_factor import attach_fcas_factor_doc
    attach_fcas_factor_doc(doc, fcas_factor_rows)
    from .offer_curves import attach_offer_factor_doc
    attach_offer_factor_doc(doc, offer_factor_rows)
    from .offer_curves import attach_offer_curve_doc
    attach_offer_curve_doc(doc, offer_curve_rows)

    # S3-08: when an optional source failed and its blocks were carried over
    # from the last-known-good cache (retained=True), stamp them so consumers
    # can tell retained-stale values from freshly computed ones. The block's
    # own 'month' is the as-of of the retained data — no duplicated field.
    # Absence of the stamp means the block was computed from this run's data.
    if factor_source_status:
        for key, src in (
            ("fcas_participation", "fcas_factors"),
            ("offers", "offer_factors"),
            ("offer_curve", "offer_curves"),
        ):
            lane = factor_source_status.get(src)
            if lane and lane.get("retained") and key in doc and doc[key]:
                doc[key]["source_status"] = "retained_stale"

    # Daily capacity factor (last 12 months)
    if daily_data is not None and not daily_data.empty:
        doc["daily"] = {
            "dates": daily_data["date"].tolist(),
            "capacity_factor": daily_data["daily_capacity_factor"].tolist(),
            "generation_mwh": daily_data["daily_generation_mwh"].tolist(),
        }
        _add_daily_coverage(doc["daily"], daily_data)

    _add_constraints_doc(doc, constraint_data)

    json_path.write_text(json.dumps(_sanitize(doc), separators=(",", ":")))
    return json_path


def _add_constraints_doc(doc: dict, constraint_data: pd.DataFrame | None) -> None:
    """Attach top binding-constraint summary to a generator/station document."""
    if constraint_data is not None and not constraint_data.empty:
        top = (
            constraint_data.groupby(["constraint_id", "description"])["hours_bound"]
            .sum()
            .nlargest(15)
            .reset_index()
        )
        if not top.empty:
            months = sorted(constraint_data["month"].unique())
            heatmap = {}
            for _, row in top.iterrows():
                cid = row["constraint_id"]
                monthly_hours = []
                for m in months:
                    match = constraint_data[
                        (constraint_data["constraint_id"] == cid)
                        & (constraint_data["month"] == m)
                    ]
                    monthly_hours.append(
                        round(float(match["hours_bound"].sum()), 1) if not match.empty else 0
                    )
                heatmap[cid] = monthly_hours
            doc["constraints"] = {
                "top_constraints": [
                    {
                        "id": row["constraint_id"],
                        "description": row["description"],
                        "total_hours": round(float(row["hours_bound"]), 1),
                    }
                    for _, row in top.iterrows()
                ],
                "heatmap": {"months": months, "constraints": heatmap},
            }


def _sanitize(obj):
    """Replace NaN/Inf floats with None for valid JSON."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def _curtailment_energy_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Resolve eligible (actual, potential) MWh for monthly aggregate rows.

    S3-04: station/FY curtailment rollups must sum numerators and denominators
    (eligible actual / availability potential energy), not average monthly
    percentages — a fully-curtailed month has zero actual generation, so
    generation-weighting gave it no weight, and nameplate capacity is not the
    available resource in a given month.

    Rows carrying the v3 aggregate columns (curtailment_actual_mwh /
    curtailment_potential_mwh) use them directly. Legacy rows (cached before
    S3-04 columns exist — normal while NAS runs are incremental) recover
    potential as generation/(1 − pct), exact when availability coverage is
    complete. A fully-curtailed legacy month (pct == 1, no generation) has no
    derivable potential and contributes nothing to either sum; it is still
    counted in months_covered by callers.

    Returns a DataFrame indexed like df with two float columns:
    _potential_mwh and _eligible_actual_mwh (NaN where not derivable).
    """
    out = pd.DataFrame(index=df.index)
    out["_potential_mwh"] = np.nan
    out["_eligible_actual_mwh"] = np.nan
    pct = df["curtailment_pct"]

    has_new = "curtailment_potential_mwh" in df.columns
    if has_new:
        pot = df["curtailment_potential_mwh"]
        ok = pot.notna() & (pot > 0) & pct.notna()
        out.loc[ok, "_potential_mwh"] = pot[ok]
        if "curtailment_actual_mwh" in df.columns:
            act = df["curtailment_actual_mwh"]
            out.loc[ok, "_eligible_actual_mwh"] = act[ok].fillna(0.0)
        else:
            out.loc[ok, "_eligible_actual_mwh"] = pot[ok] * (1.0 - pct[ok])

    # Legacy rows: recover potential from generation & the proxy.
    gen = df["generation_mwh"]
    legacy_ok = (
        out["_potential_mwh"].isna()
        & pct.notna()
        & (pct < 1.0)
        & gen.notna()
        & (gen > 0)
    )
    if legacy_ok.any():
        out.loc[legacy_ok, "_potential_mwh"] = gen[legacy_ok] / (1.0 - pct[legacy_ok])
        out.loc[legacy_ok, "_eligible_actual_mwh"] = gen[legacy_ok]
    return out


def _aggregate_price_distribution(monthly: pd.DataFrame) -> dict | None:
    """Generation-weighted spot-price distribution across a unit's months.

    S3-04: the previous equal mean over monthly histograms let zero-generation
    months dilute every bin (RUBICON's published histogram summed to 41.55%)
    and gave low-output months the same weight as high-output ones (CGBESS01's
    negative-price exposure was 29.94% vs 7.52% once correctly weighted).

    Per-bin energy is the first-class field: when the monthly rows carry the
    v3 price_mwh_* columns the totals are exact sums of those; legacy rows
    contribute generation × share (exact when price coverage is complete).
    Zero-generation months contribute nothing automatically. Returns None
    (unknown) when the whole window has no priced generation.
    """
    if monthly is None or monthly.empty or "generation_mwh" not in monthly.columns:
        return None
    pairs = [
        (lab, f"price_dist_{lab}", f"price_mwh_{lab}")
        for lab in config.PRICE_BIN_LABELS
        if f"price_dist_{lab}" in monthly.columns
    ]
    if not pairs:
        return None

    gen = monthly["generation_mwh"].fillna(0.0)
    has_mwh = all(mwh_col in monthly.columns for _, _, mwh_col in pairs)
    share_frame = monthly[[d for _, d, _ in pairs]].fillna(0.0)

    if has_mwh:
        mwh_frame = monthly[[m for _, _, m in pairs]].fillna(0.0)
        bin_tot = mwh_frame.sum(axis=0)
        # Legacy rows within a mixed frame (columns exist but row is NaN
        # pre-S3-04) fall back to share × generation.
        row_new = monthly[[m for _, _, m in pairs]].notna().any(axis=1)
        leg = ~row_new
        if leg.any():
            eligible = leg & (gen > 0) & (share_frame.sum(axis=1) > 0)
            leg_energy = share_frame.mul(gen.where(eligible, 0.0), axis=0)
            leg_energy.columns = [m for _, _, m in pairs]  # re-label to mwh names
            bin_tot = bin_tot + leg_energy.sum(axis=0)
    else:
        eligible = (gen > 0) & (share_frame.sum(axis=1) > 0)
        bin_tot = share_frame.mul(gen.where(eligible, 0.0), axis=0).sum(axis=0)

    total = float(bin_tot.sum())
    if not math.isfinite(total) or total <= 0:
        return None
    return {
        "bins": [lab for lab, _, _ in pairs],
        "generation_share": [round(float(v) / total, 4) for v in bin_tot],
        "mwh": [round(float(v), 1) for v in bin_tot],
    }


def write_curtailment_by_fy(
    monthly_aggregates: pd.DataFrame,
    output_dir: str | None = None,
) -> Path:
    """Publish consolidated per-DUID per-FY curtailment to docs/data/curtailment_by_fy.csv.

    Consumed by the renewable generator dashboard, which needs FY-aggregated
    curtailment for its cross-sectional table. S3-04: each FY row is the ratio
    of summed eligible energy — 1 − Σ eligible actual MWh / Σ potential MWh
    across that FY's months (the same actual/potential pair behind each
    monthly proxy, see _curtailment_energy_columns). Averaging monthly
    percentages by generation hid exactly the worst months (a fully curtailed
    month has no generation to carry its weight).
    """
    out_dir = Path(output_dir or config.DOCS_DATA_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "curtailment_by_fy.csv"

    if monthly_aggregates is None or monthly_aggregates.empty:
        logger.warning("No monthly aggregates — skipping curtailment_by_fy.csv")
        return out_path

    if "curtailment_pct" not in monthly_aggregates.columns:
        logger.debug("No curtailment_pct column — skipping curtailment_by_fy.csv")
        return out_path
    df = monthly_aggregates[monthly_aggregates["curtailment_pct"].notna()].copy()
    if df.empty:
        logger.warning("No curtailment rows in monthly aggregates")
        return out_path

    months = df["month"].str.split("-", expand=True).astype(int)
    df["fy_start"] = months[0].where(months[1] >= 7, months[0] - 1)

    energy = _curtailment_energy_columns(df)
    df["_potential_mwh"] = energy["_potential_mwh"]
    df["_eligible_actual_mwh"] = energy["_eligible_actual_mwh"]

    rows = []
    for (duid, fy_start), group in df.groupby(["duid", "fy_start"]):
        pot_sum = float(group["_potential_mwh"].sum())
        if not math.isfinite(pot_sum) or pot_sum <= 0:
            continue
        act_sum = float(group["_eligible_actual_mwh"].sum())
        curt = max(0.0, 1.0 - act_sum / pot_sum)
        # S3-01: metric_version travels with the CSV so the downstream
        # consumer (AEMO Renewable Generator Dashboard) can tell which
        # methodology produced each row. grid_curtailment_pct is no longer
        # published — the causal split was unsupported.
        rows.append({
            "duid": duid,
            "fy_start": int(fy_start),
            "fy_label": f"FY{fy_start % 100:02d}-{(fy_start + 1) % 100:02d}",
            "curtailment_pct": round(curt, 4),
            "metric_version": config.CURTAILMENT_METRIC_VERSION,
            "generation_mwh": round(float(group["generation_mwh"].sum()), 0),
            "months_covered": int(len(group)),
        })

    if not rows:
        # Nothing derivable in this window (e.g. only legacy fully-curtailed
        # months with no recoverable potential) → publish an empty file, not a
        # crash and not a stale last-good one (unknown ≠ previous value).
        logger.warning("No derivable curtailment energy — writing empty curtailment_by_fy.csv")
        pd.DataFrame(columns=[
            "duid", "fy_start", "fy_label", "curtailment_pct",
            "metric_version", "generation_mwh", "months_covered",
        ]).to_csv(out_path, index=False)
        return out_path
    result = pd.DataFrame(rows).sort_values(["duid", "fy_start"])
    result.to_csv(out_path, index=False)
    logger.info(f"Wrote curtailment_by_fy.csv: {len(result)} DUID×FY rows")
    return out_path


def generate_all(
    generators: pd.DataFrame,
    monthly_aggregates: pd.DataFrame | None = None,
    mlf_history: pd.DataFrame | None = None,
    output_dir: str | None = None,
    draft_mlfs: dict[str, float] | None = None,
    draft_fy_label: str | None = None,
    fcas_data: dict | None = None,
    daily_aggregates: pd.DataFrame | None = None,
    constraint_data: pd.DataFrame | None = None,
    fcas_factors: pd.DataFrame | None = None,
    offer_factors: pd.DataFrame | None = None,
    offer_curves: pd.DataFrame | None = None,
    factor_source_status: dict | None = None,
    market: str = "NEM",
) -> int:
    """Generate all per-generator JSON files and the index.

    Returns count of generator files written.
    """
    gen_dir = output_dir or config.GENERATORS_JSON_DIR
    docs_dir = str(Path(gen_dir).parent)

    # Write index (additive — preserves other markets)
    generate_index(generators, docs_dir, market=market)

    # Publish FY curtailment rollup (consumed by renewable dashboard)
    if monthly_aggregates is not None and not monthly_aggregates.empty:
        write_curtailment_by_fy(monthly_aggregates, docs_dir)

    # Write per-generator files
    count = 0
    for _, row in generators.iterrows():
        duid = str(row["DUID"])
        metadata = {
            "station_name": _text(row.get("STATION_NAME", "")),
            "region": _text(row.get("REGION", "")),
            "fuel_category": _text(row.get("FUEL_CATEGORY", "")),
            "capacity_mw": round(float(row["CAPACITY_MW"]), 1) if pd.notna(row.get("CAPACITY_MW")) else None,
            "technology": _text(row.get("TECHNOLOGY", "")),
            "connection_point": _text(row.get("CONNECTION_POINT", "")),
            "market": _text(row.get("MARKET", market)),
        }

        # Extract this generator's monthly data if available
        monthly = None
        if monthly_aggregates is not None and not monthly_aggregates.empty:
            monthly = monthly_aggregates[monthly_aggregates["duid"] == duid].copy()
            if monthly.empty:
                monthly = None

        # Extract MLF data if available
        mlf = None
        if mlf_history is not None and not mlf_history.empty:
            mlf_rows = mlf_history[mlf_history["DUID"] == duid]
            if not mlf_rows.empty:
                mlf = {
                    "years": mlf_rows["fy_label"].tolist(),
                    "values": mlf_rows["mlf"].round(6).tolist(),
                }

        # Compute aggregate price distribution from monthly data.
        # S3-04: generation-weighted across months with output; zero-generation
        # months cannot dilute the histogram, and the whole window being empty
        # yields unknown (block omitted). Per-bin MWh travels alongside.
        price_dist = _aggregate_price_distribution(monthly) if monthly is not None else None

        # Draft MLF for this DUID
        d_mlf = draft_mlfs.get(duid) if draft_mlfs else None

        # Build FCAS monthly data for this generator's region
        fcas_monthly = None
        region = _text(row.get("REGION", ""))
        if fcas_data and region and monthly is not None and not monthly.empty:
            months_list = monthly["month"].tolist()
            fcas_months = []
            fcas_services = {}
            for m in months_list:
                key = (region, m)
                if key not in fcas_data:
                    continue  # skip — don't create misaligned None entries
                fcas_months.append(m)
                for service, price in fcas_data[key].items():
                    if service not in fcas_services:
                        fcas_services[service] = []
                    fcas_services[service].append(price)
            if fcas_services:
                fcas_monthly = {"months": fcas_months, "services": fcas_services}

        # Extract daily data for this DUID
        daily = None
        if daily_aggregates is not None and not daily_aggregates.empty:
            daily = daily_aggregates[daily_aggregates["duid"] == duid].copy()
            if daily.empty:
                daily = None

        # Extract constraint data for this DUID
        duid_constraints = None
        if constraint_data is not None and not constraint_data.empty:
            duid_constraints = constraint_data[constraint_data["duid"] == duid].copy()
            if duid_constraints.empty:
                duid_constraints = None

        # Per-DUID FCAS participation (offer behaviour), if computed
        duid_fcas_factors = None
        if fcas_factors is not None and not fcas_factors.empty:
            duid_fcas_factors = fcas_factors[fcas_factors["duid"] == duid].copy()
            if duid_fcas_factors.empty:
                duid_fcas_factors = None

        duid_curves = None
        if offer_curves is not None and not offer_curves.empty:
            duid_curves = offer_curves[offer_curves["duid"] == duid].copy()
            if duid_curves.empty:
                duid_curves = None

        duid_offers = None
        if offer_factors is not None and not offer_factors.empty:
            duid_offers = offer_factors[offer_factors["duid"] == duid].copy()
            if duid_offers.empty:
                duid_offers = None

        generate_generator_json(
            duid, metadata, monthly, mlf, price_dist,
            draft_mlf=d_mlf, draft_fy_label=draft_fy_label,
            fcas_monthly=fcas_monthly, daily_data=daily,
            constraint_data=duid_constraints,
            fcas_factor_rows=duid_fcas_factors,
            offer_factor_rows=duid_offers,
            offer_curve_rows=duid_curves,
            factor_source_status=factor_source_status,
            output_dir=gen_dir,
        )
        count += 1

    logger.info(f"Wrote {count} generator JSON files")

    # Generate station-level aggregations for multi-DUID stations
    station_count = _generate_station_files(
        generators, monthly_aggregates, mlf_history,
        draft_mlfs, draft_fy_label, fcas_data, gen_dir, docs_dir,
        daily_aggregates=daily_aggregates,
        constraint_data=constraint_data,
        market=market,
    )
    logger.info(f"Wrote {station_count} station aggregate files")

    return count


def _safe_station_filename(name: str) -> str:
    """Convert station name to safe filename."""
    import re
    safe = re.sub(r'[^a-zA-Z0-9]', '_', name)
    safe = re.sub(r'_+', '_', safe).strip('_')
    return f"station_{safe}"


def _generate_station_files(
    generators: pd.DataFrame,
    monthly_aggregates: pd.DataFrame | None,
    mlf_history: pd.DataFrame | None,
    draft_mlfs: dict[str, float] | None,
    draft_fy_label: str | None,
    fcas_data: dict | None,
    gen_dir: str,
    docs_dir: str,
    daily_aggregates: pd.DataFrame | None = None,
    constraint_data: pd.DataFrame | None = None,
    market: str = "NEM",
) -> int:
    """Generate station-level aggregation files for multi-DUID stations."""
    # Group generators by station name
    station_groups = generators.groupby("STATION_NAME")
    multi_duid = {name: group for name, group in station_groups if len(group) > 1}

    if not multi_duid:
        return 0

    out_dir = Path(gen_dir)
    station_entries = []
    count = 0

    for station_name, group in multi_duid.items():
        duids = group["DUID"].tolist()
        total_capacity = group["CAPACITY_MW"].sum() if "CAPACITY_MW" in group.columns else 0
        region = _text(group["REGION"].iloc[0])
        fuel = _text(group["FUEL_CATEGORY"].iloc[0])
        technology = _text(group["TECHNOLOGY"].iloc[0])
        connection_points = group.get("CONNECTION_POINT", pd.Series()).tolist()
        capacity_by_duid = group.set_index("DUID")["CAPACITY_MW"].to_dict()

        file_key = _safe_station_filename(station_name)

        doc = {
            "type": "station",
            "station_name": station_name,
            "duids": duids,
            "region": region,
            "fuel_category": fuel,
            "capacity_mw": round(float(total_capacity), 1) if total_capacity else None,
            "technology": technology,
            "connection_points": [_text(cp) for cp in connection_points if _text(cp)],
            "lgc_eligible": fuel in config.LGC_ELIGIBLE_FUEL_TYPES,
        }

        # Aggregate monthly data across DUIDs
        if monthly_aggregates is not None and not monthly_aggregates.empty:
            station_monthly = monthly_aggregates[monthly_aggregates["duid"].isin(duids)]
            if not station_monthly.empty:
                doc["monthly"] = _aggregate_station_monthly(
                    station_monthly, total_capacity, fuel, capacity_by_duid,
                )

        # Per-DUID MLFs
        if mlf_history is not None and not mlf_history.empty:
            mlf_by_duid = {}
            for duid in duids:
                mlf_rows = mlf_history[mlf_history["DUID"] == duid]
                if not mlf_rows.empty:
                    entry = {
                        "years": mlf_rows["fy_label"].tolist(),
                        "values": mlf_rows["mlf"].round(6).tolist(),
                    }
                    if draft_mlfs and duid in draft_mlfs and draft_fy_label:
                        entry["draft_year"] = draft_fy_label
                        entry["draft_value"] = round(draft_mlfs[duid], 6)
                    mlf_by_duid[duid] = entry
            if mlf_by_duid:
                doc["mlf_by_duid"] = mlf_by_duid

        # FCAS (same region, so same data as any single DUID)
        if fcas_data and region and "monthly" in doc:
            months_list = doc["monthly"]["months"]
            fcas_months = []
            fcas_services = {}
            for m in months_list:
                key = (region, m)
                if key not in fcas_data:
                    continue  # skip — don't create misaligned None entries
                fcas_months.append(m)
                for service, price in fcas_data[key].items():
                    if service not in fcas_services:
                        fcas_services[service] = []
                    fcas_services[service].append(price)
            if fcas_services:
                doc["fcas"] = {"months": fcas_months, "services": fcas_services}

        # Daily data: sum across station DUIDs
        if daily_aggregates is not None and not daily_aggregates.empty:
            station_daily = daily_aggregates[daily_aggregates["duid"].isin(duids)]
            if not station_daily.empty:
                agg_spec = {"daily_generation_mwh": ("daily_generation_mwh", "sum")}
                has_daily_cov = (
                    "intervals_observed" in station_daily.columns
                    and "intervals_expected" in station_daily.columns
                )
                if has_daily_cov:
                    # Station coverage = union of member-unit intervals
                    # (sum of per-unit observed/expected counts).
                    agg_spec["intervals_observed"] = ("intervals_observed", "sum")
                    agg_spec["intervals_expected"] = ("intervals_expected", "sum")
                grouped_daily = station_daily.groupby("date").agg(**agg_spec).reset_index()
                # Recompute CF from total generation and total capacity
                if total_capacity and total_capacity > 0:
                    grouped_daily["daily_capacity_factor"] = (
                        grouped_daily["daily_generation_mwh"] / (total_capacity * 24)
                    ).round(4)
                else:
                    grouped_daily["daily_capacity_factor"] = None
                grouped_daily = grouped_daily.sort_values("date")
                doc["daily"] = {
                    "dates": grouped_daily["date"].tolist(),
                    "capacity_factor": grouped_daily["daily_capacity_factor"].tolist(),
                    "generation_mwh": grouped_daily["daily_generation_mwh"].round(1).tolist(),
                }
                _add_daily_coverage(doc["daily"], grouped_daily)

        # Binding constraints: union the DUID-level rows for the station.
        # A single binding constraint can map to multiple station DUIDs; keep
        # one station-hour observation per month/constraint rather than
        # double-counting the same binding interval as DUID-hours.
        if constraint_data is not None and not constraint_data.empty:
            station_constraints = constraint_data[constraint_data["duid"].isin(duids)].copy()
            if not station_constraints.empty:
                station_constraints = (
                    station_constraints
                    .groupby(["month", "constraint_id", "description"], as_index=False)
                    .agg(hours_bound=("hours_bound", "max"))
                )
                _add_constraints_doc(doc, station_constraints)

        json_path = out_dir / f"{file_key}.json"
        json_path.write_text(json.dumps(_sanitize(doc), separators=(",", ":")))
        count += 1

        # Add to station index
        station_entries.append({
            "duid": file_key,
            "file": file_key,
            "station_name": station_name,
            "region": region,
            "fuel_category": fuel,
            "capacity_mw": round(float(total_capacity), 1) if total_capacity else None,
            "technology": technology,
            "connection_point": ", ".join(_text(cp) for cp in connection_points if _text(cp)),
            "type": "station",
            "duid_count": len(duids),
            "market": market,
        })

    # Append station entries to index.json (additive — only replace same-market stations)
    if station_entries:
        index_path = Path(docs_dir) / "index.json"
        existing = json.loads(index_path.read_text())
        # Remove old station entries for this market only
        existing = [
            e for e in existing
            if not (e.get("type") == "station" and e.get("market", "NEM") == market)
        ]
        existing.extend(station_entries)
        existing.sort(key=lambda e: (e.get("station_name", ""), e.get("duid", "")))
        index_path.write_text(json.dumps(_sanitize(existing), indent=None, separators=(",", ":")))
        logger.info(f"Added {len(station_entries)} {market} station entries to index.json")

    return count


def _aggregate_station_monthly(
    station_monthly: pd.DataFrame,
    total_capacity: float,
    fuel: str,
    capacity_by_duid: dict[str, float] | None = None,
) -> dict:
    """Aggregate monthly metrics across multiple DUIDs for a station."""
    from calendar import monthrange

    if capacity_by_duid:
        station_monthly = station_monthly.copy()
        station_monthly["_capacity_weight"] = station_monthly["duid"].map(capacity_by_duid)
    else:
        station_monthly = station_monthly.copy()
        station_monthly["_capacity_weight"] = None

    grouped = station_monthly.groupby("month")

    months = sorted(station_monthly["month"].unique())
    gen_mwh = []
    revenue = []
    cap_factor = []
    curtailment = []
    econ_curtailment = []
    captured_price = []
    avg_rrp = []
    pcr = []

    has_curtailment = "curtailment_pct" in station_monthly.columns and fuel in config.CURTAILMENT_FUEL_TYPES
    has_econ_curt = "econ_curtailment_pct" in station_monthly.columns and fuel in config.CURTAILMENT_FUEL_TYPES
    has_price = "captured_price" in station_monthly.columns

    def weighted_pct(month_data: pd.DataFrame, col: str) -> float | None:
        """Capacity-weighted mean of a per-unit percentage (nameplate weights).

        S3-04: no longer used for curtailment — station curtailment sums the
        member units' eligible actual/potential energy instead (nameplate is
        not the available resource in a given month). Kept for the economic
        curtailment estimate, which has no first-class energy fields.
        """
        valid = month_data.dropna(subset=[col])
        if valid.empty:
            return None

        valid_weighted = valid.dropna(subset=["_capacity_weight"])
        weight_sum = valid_weighted["_capacity_weight"].sum()
        if not valid_weighted.empty and weight_sum > 0:
            value = (valid_weighted[col] * valid_weighted["_capacity_weight"]).sum() / weight_sum
        else:
            value = valid[col].mean()
        return round(float(value), 4)

    def station_curtailment(month_data: pd.DataFrame) -> float | None:
        """S3-04 station-month curtailment = 1 − Σ eligible actual / Σ potential
        across the member units present that month (per-unit availability
        energy denominators, never nameplate or actual-generation weights)."""
        energy = _curtailment_energy_columns(month_data)
        pot_sum = float(energy["_potential_mwh"].sum())
        if not math.isfinite(pot_sum) or pot_sum <= 0:
            return None
        act_sum = float(energy["_eligible_actual_mwh"].sum())
        return round(max(0.0, 1.0 - act_sum / pot_sum), 4)

    for m in months:
        month_data = grouped.get_group(m)
        total_gen = month_data["generation_mwh"].sum()
        gen_mwh.append(round(total_gen, 0))
        revenue.append(round(month_data["revenue_aud"].sum(), 0))

        # Capacity factor from total generation
        year, mon = int(m[:4]), int(m[5:])
        hours = monthrange(year, mon)[1] * 24
        if total_capacity and total_capacity > 0:
            cap_factor.append(round(total_gen / (total_capacity * hours), 4))
        else:
            cap_factor.append(None)

        if has_curtailment:
            curtailment.append(station_curtailment(month_data))

        if has_econ_curt:
            econ_curtailment.append(weighted_pct(month_data, "econ_curtailment_pct"))

        # Price capture: generation-weighted
        if has_price:
            valid = month_data.dropna(subset=["captured_price"])
            if not valid.empty and total_gen > 0:
                weighted = (valid["captured_price"] * valid["generation_mwh"]).sum() / valid["generation_mwh"].sum()
                captured_price.append(round(float(weighted), 2))
            else:
                captured_price.append(None)
            rrp_vals = month_data["avg_rrp"].dropna()
            avg_rrp.append(round(float(rrp_vals.mean()), 2) if not rrp_vals.empty else None)
            cp_val = captured_price[-1]
            ar_val = avg_rrp[-1]
            if cp_val and ar_val and ar_val != 0:
                pcr.append(round(cp_val / ar_val, 4))
            else:
                pcr.append(None)

    result = {
        "months": months,
        "generation_mwh": gen_mwh,
        "revenue_aud": revenue,
        "capacity_factor": cap_factor,
    }
    if has_curtailment:
        result["curtailment_pct"] = curtailment
        result["curtailment_metric_version"] = config.CURTAILMENT_METRIC_VERSION
    if has_econ_curt:
        result["econ_curtailment_pct"] = econ_curtailment
    if has_price:
        result["captured_price"] = captured_price
        result["avg_rrp"] = avg_rrp
        result["price_capture_ratio"] = pcr

    # S3-11: station monthly revenue is the SUM of member-unit revenues, each
    # adjusted by its own MLF factor (or unadjusted+provisional when unknown).
    # A single station factor is not meaningful (members may carry different
    # factors/FYs), so expose the provenance contract at the station level as
    # the most conservative member status per month — a station month whose
    # revenue contains ANY unadjusted member revenue is itself provisional.
    # Per-member factors remain available in doc["mlf_by_duid"].
    if REVENUE_MLF_STATUS_COL in station_monthly.columns:
        has_mlf_status = station_monthly[REVENUE_MLF_STATUS_COL].dropna()
        if not has_mlf_status.empty:
            statuses = []
            for m in months:
                month_statuses = (
                    grouped.get_group(m)[REVENUE_MLF_STATUS_COL].dropna().astype(str)
                )
                if month_statuses.empty:
                    statuses.append(None)
                elif (month_statuses == MLF_STATUS_UNKNOWN).any():
                    statuses.append(MLF_STATUS_UNKNOWN)
                elif (month_statuses == MLF_STATUS_PRIOR_CARRY).any():
                    statuses.append(MLF_STATUS_PRIOR_CARRY)
                else:
                    statuses.append("exact")
            result["revenue_mlf_status"] = statuses

    return result
