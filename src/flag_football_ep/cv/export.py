"""Tracking-Parquet -> CSV export for downstream/human consumption.

A thin transform-and-write step over `schema.conform_tracking`'s canonical frame,
mirroring `model/score.py`'s scored-output writer shape: read the tracking Parquet,
conform it to the canonical schema, write a plain CSV alongside it.

D-14: the Parquet is the only canonical tracking artifact. This export is one-way --
the CSV produced here is written for eyeballing only and is never read back into any
pipeline stage, and never becomes a second join surface: by default it is written
under the tracking directory next to its source Parquet, same stem plus `.csv`.

Implemented by plan 02.1-05, alongside `cv/schema.py`.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from flag_football_ep.cv.schema import TRACKING_COLUMNS, conform_tracking

_CSV_FLOAT_PRECISION = 4


class TrackingParquetNotFound(Exception):
    """Raised when `export_tracking_csv`'s input Parquet path does not exist."""


def export_tracking_csv(parquet_path: Path, csv_path: Path) -> Path:
    """Read the tracking Parquet at `parquet_path`, conform it to the canonical
    schema, and write it as a plain CSV to `csv_path`.

    Columns are written in `TRACKING_COLUMNS` order, floats formatted to
    `_CSV_FLOAT_PRECISION` decimal places (pixel/yard precision beyond that is noise
    and makes the file unreadable), and nulls render as an empty field, never the
    string `"null"`. Raises `TrackingParquetNotFound` naming `parquet_path` when it is
    missing.
    """
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        raise TrackingParquetNotFound(f"Tracking Parquet not found: {parquet_path}")

    df = pl.read_parquet(parquet_path)
    df = conform_tracking(df)

    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(csv_path, float_precision=_CSV_FLOAT_PRECISION)

    assert df.columns == list(TRACKING_COLUMNS)
    return csv_path
