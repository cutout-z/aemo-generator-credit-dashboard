"""Unit lineage: history of DUIDs that were renamed, converted or merged.

Aggregation keys every interval to a NEM region through the CURRENT
Registration List, so a DUID that has left the list drops out of the history
entirely. When the plant still runs under another DUID that history belongs to
the successor (audit 2026-10, M8):

- HPRG1 -> HPR1: Hornsdale Power Reserve's generator DUID (with load HPRL1)
  became the bidirectional HPR1 on 12 Sep 2024 (DUDETAILSUMMARY periods: HPR1
  from 2024-09-12, HPRG1/HPRL1 end 2024-10-02). HPR1's published history began
  2024-09; HPRG1's 2021-09..2024-09 discharge was gone.
- WKIEWA2 -> WKIEWA1: the October 2026 Registration List shows WKIEWA1 as an
  aggregated unit (units 1-4, 80 MW) and no WKIEWA2; WKIEWA2's SCADA ends in
  June 2026 as WKIEWA1's output roughly triples.

The lineage is applied in two places only. Aggregation sees each predecessor
as a unit of its successor's region and fuel (capacity unknown, so its own
rows carry no capacity factor), which keeps its rows in the processed cache
under its own DUID and with its own loss factors. Publication then folds the
predecessor rows into the successor's series: per month (or day), energies and
revenues are summed and every ratio is recomputed against the successor's
registered capacity. A month where both DUIDs ran (HPR1/HPRG1 in Sep 2024)
becomes one month. The successor's JSON lists which DUIDs each month came from.
"""

from __future__ import annotations

import calendar
import logging

import numpy as np
import pandas as pd

from . import config

logger = logging.getLogger(__name__)

# Most conservative first (aggregate.MLF_STATUS_*).
_MLF_STATUS_ORDER = ("unknown", "prior-carry", "exact")


def successors(generators: pd.DataFrame) -> dict[str, str]:
    """{predecessor: successor} for successors present in ``generators``."""
    listed = set(generators["DUID"].astype(str))
    return {
        old: new for old, new in config.DUID_SUCCESSORS.items()
        if new in listed and old not in listed
    }


def with_predecessor_units(generators: pd.DataFrame) -> pd.DataFrame:
    """``generators`` plus one aggregation-only row per predecessor DUID.

    The row borrows the successor's region, station, fuel and technology so the
    predecessor's SCADA is priced in the right region and keeps its curtailment
    proxy; CAPACITY_MW is NaN (its own registered capacity is not known here).
    Never pass the result to the JSON writers: predecessors are published only
    through their successor.
    """
    links = successors(generators)
    if not links:
        return generators
    base = generators.set_index("DUID", drop=False)
    rows = []
    for old, new in links.items():
        row = base.loc[new].copy()
        row["DUID"] = old
        if "CAPACITY_MW" in row.index:
            row["CAPACITY_MW"] = np.nan
        if "CONNECTION_POINT" in row.index:
            row["CONNECTION_POINT"] = ""
        rows.append(row)
    return pd.concat([generators, pd.DataFrame(rows)], ignore_index=True)


def _hours(month: str) -> int:
    return calendar.monthrange(int(month[:4]), int(month[5:7]))[1] * 24


def _wmean(values: pd.Series, weights: pd.Series) -> float | None:
    ok = values.notna() & weights.notna()
    if not ok.any():
        return None
    w = weights[ok].clip(lower=0)
    if w.sum() <= 0:
        return float(values[ok].mean())
    return float((values[ok] * w).sum() / w.sum())


def _sum_or_none(values: pd.Series) -> float | None:
    """Sum when every member has a value; None when any is missing."""
    if values.isna().any():
        return None
    return float(values.sum())


def _combine_month(group: pd.DataFrame, capacity: float | None) -> dict:
    """One successor-month row from its member rows (successor + predecessors)."""
    month = str(group["month"].iloc[0])
    gen = group["generation_mwh"].fillna(0.0)
    out: dict = {"duid": group["duid"].iloc[0], "month": month}
    out["generation_mwh"] = round(float(gen.sum()), 1)
    out["revenue_aud"] = round(float(group["revenue_aud"].fillna(0.0).sum()), 0)
    if capacity and capacity > 0:
        out["capacity_factor"] = round(out["generation_mwh"] / (capacity * _hours(month)), 4)
    else:
        out["capacity_factor"] = None

    cols = set(group.columns)
    if "revenue_loss_adjusted_aud" in cols:
        v = _sum_or_none(group["revenue_loss_adjusted_aud"])
        out["revenue_loss_adjusted_aud"] = None if v is None else round(v, 0)
    for col in ("curtailment_actual_mwh", "curtailment_potential_mwh"):
        if col in cols:
            vals = group[col]
            out[col] = round(float(vals.sum()), 1) if vals.notna().any() else None
    if "curtailment_pct" in cols:
        pot = out.get("curtailment_potential_mwh")
        act = out.get("curtailment_actual_mwh")
        if pot and pot > 0 and act is not None:
            out["curtailment_pct"] = round(max(0.0, 1.0 - act / pot), 4)
        else:
            out["curtailment_pct"] = _wmean(group["curtailment_pct"], gen)
    if "econ_curtailment_pct" in cols:
        weights = group["curtailment_potential_mwh"] if "curtailment_potential_mwh" in cols else gen
        econ = _wmean(group["econ_curtailment_pct"], weights)
        total = out.get("curtailment_pct")
        if econ is not None and total is not None:
            econ = min(econ, total)
        out["econ_curtailment_pct"] = None if econ is None else round(econ, 4)
    if "captured_price" in cols:
        cp = _wmean(group["captured_price"], gen)
        out["captured_price"] = None if cp is None else round(cp, 2)
    if "avg_rrp" in cols:
        ar = group["avg_rrp"].dropna()
        out["avg_rrp"] = round(float(ar.mean()), 2) if not ar.empty else None
    if "price_capture_ratio" in cols:
        cp, ar = out.get("captured_price"), out.get("avg_rrp")
        out["price_capture_ratio"] = round(cp / ar, 4) if cp is not None and ar else None

    labels = [c[len("price_mwh_"):] for c in group.columns if c.startswith("price_mwh_")]
    if labels:
        mwh = {lab: float(group[f"price_mwh_{lab}"].fillna(0.0).sum()) for lab in labels}
        total = sum(mwh.values())
        for lab in labels:
            out[f"price_mwh_{lab}"] = round(mwh[lab], 1)
            if f"price_dist_{lab}" in cols:
                out[f"price_dist_{lab}"] = round(mwh[lab] / total, 4) if total > 0 else 0.0

    if "revenue_mlf_status" in cols:
        st = [s for s in group["revenue_mlf_status"].dropna().astype(str)]
        out["revenue_mlf_status"] = min(
            st, key=lambda s: _MLF_STATUS_ORDER.index(s) if s in _MLF_STATUS_ORDER else -1,
        ) if st else None
    if "revenue_mlf_value" in cols:
        v = _wmean(group["revenue_mlf_value"], gen)
        out["revenue_mlf_value"] = None if v is None else round(v, 6)
    if "revenue_mlf_source" in cols:
        src = set(group["revenue_mlf_source"].dropna().astype(str))
        out["revenue_mlf_source"] = (src.pop() if len(src) == 1 else "mixed") if src else None
    if "revenue_mlf_source_fy_start" in cols:
        fy = group["revenue_mlf_source_fy_start"].dropna()
        out["revenue_mlf_source_fy_start"] = int(fy.min()) if not fy.empty else None
    if "revenue_dlf_value" in cols:
        v = _wmean(group["revenue_dlf_value"], gen)
        out["revenue_dlf_value"] = None if v is None else round(v, 6)
    if "revenue_dlf_status" in cols:
        st = set(group["revenue_dlf_status"].dropna().astype(str))
        out["revenue_dlf_status"] = (st.pop() if len(st) == 1 else "partial") if st else None
    return out


def merge_lineage_monthly(monthly: pd.DataFrame | None, generators: pd.DataFrame) -> pd.DataFrame | None:
    """Fold predecessor rows into their successor's monthly series.

    Rows of DUIDs that are not part of a lineage pass through untouched. Each
    successor month gains ``source_duids`` ("HPRG1", "HPRG1+HPR1", "HPR1"),
    so a merged history is never mistaken for one unit's own record.
    """
    if monthly is None or monthly.empty:
        return monthly
    links = successors(generators)
    if not links:
        return monthly
    family = set(links) | set(links.values())
    inside = monthly["duid"].isin(family)
    if not inside.any():
        return monthly
    capacity = generators.set_index("DUID")["CAPACITY_MW"].to_dict()
    rows = []
    part = monthly[inside].copy()
    part["_source"] = part["duid"]
    part["duid"] = part["duid"].map(lambda d: links.get(d, d))
    for (duid, _month), group in part.groupby(["duid", "month"], sort=True):
        sources = sorted(group["_source"].astype(str))
        if sources == [duid]:
            # The successor's own month: published exactly as aggregated.
            row = group.drop(columns="_source").iloc[0].to_dict()
        else:
            row = _combine_month(group, capacity.get(duid))
        row["source_duids"] = "+".join(sources)
        rows.append(row)
    merged = pd.DataFrame(rows)
    rest = monthly[~inside]
    out = pd.concat([rest, merged], ignore_index=True, sort=False) if not rest.empty else merged
    out = out.sort_values(["duid", "month"]).reset_index(drop=True)
    n_pred = int(monthly["duid"].isin(set(links)).sum())
    if n_pred:
        logger.info("Lineage: folded %d predecessor month rows into %s", n_pred,
                    ", ".join(sorted(set(links.values()))))
    return out


def merge_lineage_daily(daily: pd.DataFrame | None, generators: pd.DataFrame) -> pd.DataFrame | None:
    """Fold predecessor daily rows into the successor (sum MWh and intervals)."""
    if daily is None or daily.empty:
        return daily
    links = successors(generators)
    if not links or not daily["duid"].isin(set(links)).any():
        return daily
    family = set(links) | set(links.values())
    inside = daily["duid"].isin(family)
    part = daily[inside].copy()
    part["duid"] = part["duid"].map(lambda d: links.get(d, d))
    spec = {"daily_generation_mwh": ("daily_generation_mwh", "sum")}
    for col in ("intervals_observed", "intervals_expected"):
        if col in part.columns:
            spec[col] = (col, "sum")
    merged = part.groupby(["duid", "date"], as_index=False).agg(**spec)
    capacity = merged["duid"].map(generators.set_index("DUID")["CAPACITY_MW"]).astype(float)
    merged["daily_generation_mwh"] = merged["daily_generation_mwh"].round(1)
    merged["daily_capacity_factor"] = (
        merged["daily_generation_mwh"] / (capacity * 24)
    ).where(capacity > 0).round(4)
    rest = daily[~inside]
    out = pd.concat([rest, merged], ignore_index=True, sort=False) if not rest.empty else merged
    return out.sort_values(["duid", "date"]).reset_index(drop=True)
