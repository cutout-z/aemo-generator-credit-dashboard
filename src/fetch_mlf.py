"""Fetch MLF data from the AEMO MLF Tracker's published summary CSV.

Single source of truth for all MLF data in the credit dashboard.
Replaces the independent DUDETAILSUMMARY download (download_mlf.py) and
draft Excel download (download_draft_mlf.py).

The MLF Tracker (https://github.com/cutout-z/aemo-mlf-tracker) handles:
  - Final MLF Excel ingestion each April (genuine values before DUDETAILSUMMARY updates July 1)
  - Draft MLF Excel ingestion each October
  - Sentinel date handling, FY extraction logic, annual automation

This module just reads the tracker's published output.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
import requests

from . import config

logger = logging.getLogger(__name__)

MLF_TRACKER_URL = "https://cutout-z.github.io/aemo-mlf-tracker/outputs/summary.csv"
_CACHE_FILE = "mlf_tracker_summary.csv"

_FY_COL = re.compile(r"^FY(\d{2})-(\d{2})$")
_DRAFT_COL = re.compile(r"^FY(\d{2})-(\d{2}) \(Draft\)$")

# Contract for the long-form MLF history frame every consumer relies on
# (aggregate.build_mlf_lookup, generate_json mlf blocks).
MLF_HISTORY_COLUMNS = ("DUID", "fy_label", "fy_start_year", "mlf")
# Documented plausible MLF envelope. Genuine AEMO MLFs sit ~0.75-1.30; values
# outside 0.5-1.5 are sentinels/units errors (e.g. -999, 9.99) that must never
# reach a revenue lookup. This is the same band the output tests enforce.
MLF_PLAUSIBLE_RANGE = (0.5, 1.5)


def validate_mlf_history(mlf_history: pd.DataFrame) -> pd.DataFrame:
    """Validate the long-form MLF history frame; raise ValueError when unusable.

    The frame is the module's own melt output, so the schema contract is exact
    (the four long columns, nothing else). Checks:
      - exact long schema ``MLF_HISTORY_COLUMNS`` (a wide FY-per-column frame —
        the legacy download_mlf-era shape — fails here),
      - non-empty with at least one DUID and one FY,
      - ``mlf`` coercible to float, finite, and inside ``MLF_PLAUSIBLE_RANGE``
        (catches -999-style sentinels before they reach aggregation),
      - ``fy_start_year``/``DUID``/``fy_label`` fully populated.

    Returns the frame unchanged on success.
    """
    if mlf_history is None or not isinstance(mlf_history, pd.DataFrame):
        raise ValueError("MLF history is not a DataFrame")
    cols = set(mlf_history.columns)
    if cols != set(MLF_HISTORY_COLUMNS):
        raise ValueError(
            "MLF history schema mismatch: expected columns "
            f"{list(MLF_HISTORY_COLUMNS)}, got {list(mlf_history.columns)}"
        )
    if mlf_history.empty:
        raise ValueError("MLF history frame is empty")
    if mlf_history["DUID"].isna().any() or (mlf_history["DUID"].astype(str).str.strip() == "").any():
        raise ValueError("MLF history contains empty DUIDs")
    if mlf_history["fy_label"].isna().any():
        raise ValueError("MLF history contains empty fy_label values")
    if mlf_history["fy_start_year"].isna().any():
        raise ValueError("MLF history contains empty fy_start_year values")

    lo, hi = MLF_PLAUSIBLE_RANGE
    coerced = pd.to_numeric(mlf_history["mlf"], errors="coerce")
    if coerced.isna().any():
        bad = mlf_history.loc[coerced.isna(), ["DUID", "fy_label", "mlf"]].head(5)
        raise ValueError(
            f"MLF history contains non-numeric mlf values (sentinel?): "
            f"{bad.to_dict('records')}"
        )
    out_of_range = coerced[(coerced < lo) | (coerced > hi)]
    if not out_of_range.empty:
        bad = mlf_history.loc[out_of_range.index, ["DUID", "fy_label", "mlf"]].head(5)
        raise ValueError(
            f"MLF values outside the plausible range {lo}-{hi} "
            f"(sentinel/units error — never use as a factor): "
            f"{bad.to_dict('records')}"
        )
    return mlf_history


def fetch_mlf_data(
    cache_dir: str,
    force: bool = False,
) -> tuple[pd.DataFrame, dict[str, float] | None, str | None, dict[str, str]]:
    """Download MLF Tracker summary CSV and extract all MLF data needed by the pipeline.

    Returns:
        mlf_history   — DataFrame [DUID, fy_label, fy_start_year, mlf], long format.
                        One row per DUID per FY, covering FY15-16 to current final FY.
                        Validated by :func:`validate_mlf_history` (schema, sentinels,
                        plausible range) before being returned.
        draft_mlfs    — dict {DUID: float} for the next FY's draft, or None if not yet published.
        draft_fy_label — e.g. "FY27-28", or None.
        cp_map        — dict {DUID: CONNECTIONPOINTID} for constraint aggregation.

    Raises:
        ValueError: the melted history frame fails schema/sentinel/range checks
            (e.g. a -999 sentinel in an FY column).
        RuntimeError: the tracker summary carries no final FY columns.
    """
    cache_path = Path(cache_dir) / _CACHE_FILE

    if cache_path.exists() and not force:
        logger.info("Loading cached MLF Tracker summary")
        summary = pd.read_csv(cache_path)
    else:
        logger.info(f"Downloading MLF Tracker summary...")
        for attempt in range(config.MAX_RETRIES):
            try:
                resp = requests.get(
                    MLF_TRACKER_URL, timeout=30,
                    headers={"User-Agent": config.USER_AGENT},
                )
                resp.raise_for_status()
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(resp.text, encoding="utf-8")
                logger.info(f"Downloaded MLF Tracker summary ({len(resp.content) / 1024:.0f} KB)")
                break
            except requests.RequestException as e:
                if attempt < config.MAX_RETRIES - 1:
                    import time
                    wait = config.RETRY_BACKOFF * (attempt + 1)
                    logger.warning(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    if cache_path.exists():
                        logger.warning(f"Download failed, using stale cache: {e}")
                    else:
                        raise RuntimeError(f"Cannot fetch MLF Tracker summary and no cache exists: {e}")
        summary = pd.read_csv(cache_path)

    logger.info(f"MLF Tracker summary: {len(summary)} generators")

    # ── Connection point map ──────────────────────────────────────────────────
    cp_map: dict[str, str] = {}
    if "CONNECTIONPOINTID" in summary.columns:
        cp_map = {
            row["DUID"]: row["CONNECTIONPOINTID"]
            for _, row in summary[["DUID", "CONNECTIONPOINTID"]].dropna().iterrows()
        }

    # ── Identify FY columns ───────────────────────────────────────────────────
    final_fy_cols = [c for c in summary.columns if _FY_COL.match(c)]
    draft_fy_cols = [c for c in summary.columns if _DRAFT_COL.match(c)]

    if not final_fy_cols:
        raise RuntimeError(
            "MLF Tracker summary has no final FY columns — refusing to return "
            "an empty MLF frame. Columns: " + ", ".join(map(str, summary.columns))
        )

    # ── MLF history: melt wide → long ────────────────────────────────────────
    melted = (
        summary[["DUID"] + final_fy_cols]
        .melt(id_vars="DUID", var_name="fy_label", value_name="mlf")
        .dropna(subset=["mlf"])
    )
    # FY label "FY26-27" → fy_start_year 2026
    melted["fy_start_year"] = (
        melted["fy_label"].str.extract(r"FY(\d{2})-").astype(int) + 2000
    )
    melted = melted.sort_values(["DUID", "fy_start_year"]).reset_index(drop=True)

    logger.info(
        f"MLF history: {len(melted)} DUID×FY records across "
        f"{melted['DUID'].nunique()} DUIDs, {melted['fy_label'].nunique()} FYs"
    )

    # ── History-frame validation (schema / sentinels / plausible range) ─────
    validate_mlf_history(melted)

    # ── Draft MLFs ────────────────────────────────────────────────────────────
    draft_mlfs: dict[str, float] | None = None
    draft_fy_label: str | None = None

    if draft_fy_cols:
        draft_col = draft_fy_cols[0]  # at most one draft column
        m = _DRAFT_COL.match(draft_col)
        if m:
            draft_fy_label = f"FY{m.group(1)}-{m.group(2)}"
        draft_df = summary[["DUID", draft_col]].dropna(subset=[draft_col])
        draft_mlfs = dict(zip(draft_df["DUID"], draft_df[draft_col].astype(float)))
        logger.info(f"Draft MLFs: {len(draft_mlfs)} DUIDs for {draft_fy_label}")
    else:
        logger.info("No draft MLF column in tracker summary")

    return melted, draft_mlfs, draft_fy_label, cp_map
