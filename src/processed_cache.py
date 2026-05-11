"""Durable processed-cache snapshots for the AEMO pipeline.

The raw NEMOSIS/AEMO cache is large and machine-local. The processed cache is
the small validated project history that the dashboard actually needs. These
helpers publish that compact layer under docs/data so a cold runner can restore
settled history before reprocessing only the recent overlap window.
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

SNAPSHOT_DIR_NAME = "processed-cache"
MANIFEST_NAME = "manifest.json"

SNAPSHOT_FILES = (
    "monthly_aggregates.feather",
    "daily_aggregates.feather",
    "fcas_aggregates.feather",
    "constraint_aggregates.feather",
    "generators.feather",
    "mlf_history.feather",
    "mlf_tracker_summary.csv",
)

SNAPSHOT_GLOBS = (
    "intermittent_quality_*.feather",
)


def snapshot_dir(docs_data_dir: Path) -> Path:
    return docs_data_dir / SNAPSHOT_DIR_NAME


def restore_processed_cache(data_dir: Path, docs_data_dir: Path) -> list[str]:
    """Restore missing processed cache files from the published snapshot."""
    source_dir = snapshot_dir(docs_data_dir)
    if not source_dir.exists():
        logger.info("No processed-cache snapshot found at %s", source_dir)
        return []

    data_dir.mkdir(parents=True, exist_ok=True)
    restored: list[str] = []

    for name in SNAPSHOT_FILES:
        src = source_dir / name
        dest = data_dir / name
        if src.exists() and not dest.exists():
            shutil.copy2(src, dest)
            restored.append(name)

    for pattern in SNAPSHOT_GLOBS:
        for src in source_dir.glob(pattern):
            dest = data_dir / src.name
            if not dest.exists():
                shutil.copy2(src, dest)
                restored.append(src.name)

    if restored:
        logger.info("Restored %d processed cache files: %s", len(restored), ", ".join(restored))
    else:
        logger.info("Processed cache snapshot present; no missing files needed restore")

    return restored


def publish_processed_cache(data_dir: Path, docs_data_dir: Path) -> list[str]:
    """Publish compact processed cache files into docs/data/processed-cache."""
    target_dir = snapshot_dir(docs_data_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    published: list[str] = []
    for name in SNAPSHOT_FILES:
        src = data_dir / name
        if src.exists():
            shutil.copy2(src, target_dir / name)
            published.append(name)

    for pattern in SNAPSHOT_GLOBS:
        for src in sorted(data_dir.glob(pattern)):
            shutil.copy2(src, target_dir / src.name)
            published.append(src.name)

    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": published,
        "note": "Compact processed project history. Raw AEMO/NEMOSIS cache is intentionally excluded.",
    }
    (target_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    logger.info("Published %d processed cache files to %s", len(published), target_dir)
    return published
