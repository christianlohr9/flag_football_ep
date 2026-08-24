"""Tracking-Parquet -> CSV export for downstream/human consumption.

A thin transform-and-write step over `schema.conform_tracking`'s canonical frame,
mirroring `model/score.py`'s scored-output writer shape: read the tracking Parquet,
conform it to the canonical schema, write a plain CSV alongside it.

Implemented by plan 02.1-05, alongside `cv/schema.py`.
"""

from __future__ import annotations

from pathlib import Path


def export_tracking_csv(parquet_path: Path, csv_path: Path) -> Path:
    """Read the tracking Parquet at `parquet_path`, conform it to the canonical
    schema, and write it as a plain CSV to `csv_path`.
    """
    raise NotImplementedError("cv.export.export_tracking_csv is implemented by plan 02.1-05")
