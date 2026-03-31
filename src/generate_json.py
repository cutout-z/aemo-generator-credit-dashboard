"""Generate per-generator JSON files and search index for the frontend."""

from __future__ import annotations

import json
import logging
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

    index_path.write_text(json.dumps(entries, indent=None, separators=(",", ":")))
    logger.info(f"Wrote index.json with {len(entries)} generators ({index_path.stat().st_size / 1024:.1f} KB)")
    return index_path


def generate_generator_json(
    duid: str,
    metadata: dict,
    monthly_data: pd.DataFrame | None = None,
    mlf_data: dict | None = None,
    price_distribution: dict | None = None,
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
        # Price capture
        if "captured_price" in monthly_data.columns:
            doc["monthly"]["captured_price"] = monthly_data["captured_price"].round(2).tolist()
            doc["monthly"]["avg_rrp"] = monthly_data["avg_rrp"].round(2).tolist()
            doc["monthly"]["price_capture_ratio"] = monthly_data["price_capture_ratio"].round(4).tolist()

    # MLF history (Phase 3+)
    if mlf_data:
        doc["mlf"] = mlf_data

    # Price distribution (Phase 4+)
    if price_distribution:
        doc["price_distribution"] = price_distribution

    json_path.write_text(json.dumps(doc, separators=(",", ":")))
    return json_path


def generate_all(
    generators: pd.DataFrame,
    monthly_aggregates: pd.DataFrame | None = None,
    mlf_history: pd.DataFrame | None = None,
    output_dir: str | None = None,
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

        generate_generator_json(duid, metadata, monthly, mlf, price_dist, output_dir=gen_dir)
        count += 1

    logger.info(f"Wrote {count} generator JSON files")
    return count
