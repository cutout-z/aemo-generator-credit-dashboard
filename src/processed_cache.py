"""Durable processed-cache snapshots for the AEMO pipeline.

The raw NEMOSIS/AEMO cache is large and machine-local. The processed cache is
the small validated project history that the dashboard actually needs. These
helpers publish that compact layer under docs/data so a cold runner can restore
settled history before reprocessing only the recent overlap window.
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
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
    manifest_files: list[dict[str, object]] = []
    for name in SNAPSHOT_FILES:
        src = data_dir / name
        if src.exists():
            dest = target_dir / name
            shutil.copy2(src, dest)
            published.append(name)
            manifest_files.append(_manifest_entry(dest))

    for pattern in SNAPSHOT_GLOBS:
        for src in sorted(data_dir.glob(pattern)):
            dest = target_dir / src.name
            shutil.copy2(src, dest)
            published.append(src.name)
            manifest_files.append(_manifest_entry(dest))

    manifest = {
        "schema_version": 1,
        "files": sorted(manifest_files, key=lambda item: str(item["name"])),
        "note": "Compact processed project history. Raw AEMO/NEMOSIS cache is intentionally excluded.",
    }
    (target_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    logger.info("Published %d processed cache files to %s", len(published), target_dir)
    return published



def _manifest_entry(path: Path) -> dict[str, object]:
    return {
        "name": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
