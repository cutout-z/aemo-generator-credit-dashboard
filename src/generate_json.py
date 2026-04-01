"""Generate per-generator JSON files and search index for the frontend."""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path

import pandas as pd

from . import config

logger = logging.getLogger(__name__)


def _safe_filename(duid: str) -> str:
    """Sanitize DUID for use as a filename (replace / # etc.)."""
    return duid.replace("/", "_").replace("#", "_").replace("\\", "_")


def generate_index(generators: pd.DataFrame, output_dir: str | None = None) -> Path:
    """Write index.json with searchable generator list.

    Each entry contains metadata for the search/autocomplete UI.
    """
    out_dir = Path(output_dir or config.DOCS_DATA_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    index_path = out_dir / "index.json"

    entries = []
    for _, row in generators.iterrows():
        duid = str(row.get("DUID", ""))
        entry = {
            "duid": duid,
            "file": _safe_filename(duid),
            "station_name": str(row.get("STATION_NAME", "")),
            "region": str(row.get("REGION", "")),
            "fuel_category": str(row.get("FUEL_CATEGORY", "")),
            "capacity_mw": round(float(row["CAPACITY_MW"]), 1) if pd.notna(row.get("CAPACITY_MW")) else None,
            "technology": str(row.get("TECHNOLOGY", "")),
            "connection_point": str(row.get("CONNECTION_POINT", "")),
        }
        entries.append(entry)

    # Sort by station name for consistent output
    entries.sort(key=lambda e: (e["station_name"], e["duid"]))

    index_path.write_text(json.dumps(_sanitize(entries), indent=None, separators=(",", ":")))
    logger.info(f"Wrote index.json with {len(entries)} generators ({index_path.stat().st_size / 1024:.1f} KB)")
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
    output_dir: str | None = None,
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
        # Curtailment only for solar/wind
        if "curtailment_pct" in monthly_data.columns:
            doc["monthly"]["curtailment_pct"] = monthly_data["curtailment_pct"].round(4).tolist()
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

    # FCAS regional price context
    if fcas_monthly:
        doc["fcas"] = fcas_monthly

    json_path.write_text(json.dumps(_sanitize(doc), separators=(",", ":")))
    return json_path


def _sanitize(obj):
    """Replace NaN/Inf floats with None for valid JSON."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def generate_all(
    generators: pd.DataFrame,
    monthly_aggregates: pd.DataFrame | None = None,
    mlf_history: pd.DataFrame | None = None,
    output_dir: str | None = None,
    draft_mlfs: dict[str, float] | None = None,
    draft_fy_label: str | None = None,
    fcas_data: dict | None = None,
) -> int:
    """Generate all per-generator JSON files and the index.

    Returns count of generator files written.
    """
    gen_dir = output_dir or config.GENERATORS_JSON_DIR
    docs_dir = str(Path(gen_dir).parent)

    # Write index
    generate_index(generators, docs_dir)

    # Write per-generator files
    count = 0
    for _, row in generators.iterrows():
        duid = str(row["DUID"])
        metadata = {
            "station_name": str(row.get("STATION_NAME", "")),
            "region": str(row.get("REGION", "")),
            "fuel_category": str(row.get("FUEL_CATEGORY", "")),
            "capacity_mw": round(float(row["CAPACITY_MW"]), 1) if pd.notna(row.get("CAPACITY_MW")) else None,
            "technology": str(row.get("TECHNOLOGY", "")),
            "connection_point": str(row.get("CONNECTION_POINT", "")),
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

        # Compute aggregate price distribution from monthly data
        price_dist = None
        if monthly is not None:
            dist_cols = [c for c in monthly.columns if c.startswith("price_dist_")]
            if dist_cols:
                # Average across months (weighted by generation would be better but this is simpler)
                avg_dist = monthly[dist_cols].mean()
                total = avg_dist.sum()
                if total > 0:
                    price_dist = {
                        "bins": config.PRICE_BIN_LABELS,
                        "generation_share": [round(float(avg_dist[c]), 4) for c in dist_cols],
                    }

        # Draft MLF for this DUID
        d_mlf = draft_mlfs.get(duid) if draft_mlfs else None

        # Build FCAS monthly data for this generator's region
        fcas_monthly = None
        region = str(row.get("REGION", ""))
        if fcas_data and region and monthly is not None and not monthly.empty:
            months_list = monthly["month"].tolist()
            fcas_months = []
            fcas_services = {}
            for m in months_list:
                key = (region, m)
                if key in fcas_data:
                    fcas_months.append(m)
                    for service, price in fcas_data[key].items():
                        if service not in fcas_services:
                            fcas_services[service] = []
                        fcas_services[service].append(price)
                else:
                    fcas_months.append(m)
                    for service in fcas_services:
                        fcas_services[service].append(None)
            if fcas_services:
                fcas_monthly = {"months": fcas_months, "services": fcas_services}

        generate_generator_json(
            duid, metadata, monthly, mlf, price_dist,
            draft_mlf=d_mlf, draft_fy_label=draft_fy_label,
            fcas_monthly=fcas_monthly, output_dir=gen_dir,
        )
        count += 1

    logger.info(f"Wrote {count} generator JSON files")

    # Generate station-level aggregations for multi-DUID stations
    station_count = _generate_station_files(
        generators, monthly_aggregates, mlf_history,
        draft_mlfs, draft_fy_label, fcas_data, gen_dir, docs_dir,
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
        region = group["REGION"].iloc[0]
        fuel = group["FUEL_CATEGORY"].iloc[0]
        technology = group["TECHNOLOGY"].iloc[0]
        connection_points = group.get("CONNECTION_POINT", pd.Series()).tolist()

        file_key = _safe_station_filename(station_name)

        doc = {
            "type": "station",
            "station_name": station_name,
            "duids": duids,
            "region": region,
            "fuel_category": fuel,
            "capacity_mw": round(float(total_capacity), 1) if total_capacity else None,
            "technology": technology,
            "connection_points": [cp for cp in connection_points if cp],
            "lgc_eligible": fuel in config.LGC_ELIGIBLE_FUEL_TYPES,
        }

        # Aggregate monthly data across DUIDs
        if monthly_aggregates is not None and not monthly_aggregates.empty:
            station_monthly = monthly_aggregates[monthly_aggregates["duid"].isin(duids)]
            if not station_monthly.empty:
                doc["monthly"] = _aggregate_station_monthly(
                    station_monthly, total_capacity, fuel,
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
            fcas_services = {}
            for m in months_list:
                key = (region, m)
                if key in fcas_data:
                    for service, price in fcas_data[key].items():
                        if service not in fcas_services:
                            fcas_services[service] = []
                        fcas_services[service].append(price)
                else:
                    for service in fcas_services:
                        fcas_services[service].append(None)
            if fcas_services:
                doc["fcas"] = {"months": months_list, "services": fcas_services}

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
            "connection_point": ", ".join(cp for cp in connection_points if cp),
            "type": "station",
            "duid_count": len(duids),
        })

    # Append station entries to index.json
    if station_entries:
        index_path = Path(docs_dir) / "index.json"
        existing = json.loads(index_path.read_text())
        # Remove old station entries before adding new ones
        existing = [e for e in existing if e.get("type") != "station"]
        existing.extend(station_entries)
        existing.sort(key=lambda e: (e["station_name"], e.get("duid", "")))
        index_path.write_text(json.dumps(_sanitize(existing), indent=None, separators=(",", ":")))
        logger.info(f"Added {len(station_entries)} station entries to index.json")

    return count


def _aggregate_station_monthly(
    station_monthly: pd.DataFrame,
    total_capacity: float,
    fuel: str,
) -> dict:
    """Aggregate monthly metrics across multiple DUIDs for a station."""
    from calendar import monthrange

    # Group by month and sum/average
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

        # Curtailment: capacity-weighted average
        if has_curtailment:
            valid = month_data.dropna(subset=["curtailment_pct"])
            if not valid.empty:
                curtailment.append(round(float(valid["curtailment_pct"].mean()), 4))
            else:
                curtailment.append(None)

        if has_econ_curt:
            valid = month_data.dropna(subset=["econ_curtailment_pct"])
            if not valid.empty:
                econ_curtailment.append(round(float(valid["econ_curtailment_pct"].mean()), 4))
            else:
                econ_curtailment.append(None)

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
    if has_econ_curt:
        result["econ_curtailment_pct"] = econ_curtailment
    if has_price:
        result["captured_price"] = captured_price
        result["avg_rrp"] = avg_rrp
        result["price_capture_ratio"] = pcr

    return result
