"""Round-trip and format guards for the one-way tracking Parquet -> CSV export
(D-14: the Parquet stays canonical, the CSV is never a pipeline input).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from flag_football_ep.cv.export import TrackingParquetNotFound, export_tracking_csv
from flag_football_ep.cv.schema import TRACKING_COLUMNS, write_tracking_parquet
from flag_football_ep.testing import synthetic_tracks


def _write_tracks(tmp_path: Path) -> Path:
    tracks = synthetic_tracks(with_teams=True, with_field_coords=True)
    parquet_path = tmp_path / "tracks.parquet"
    write_tracking_parquet(tracks, parquet_path)
    return parquet_path


def test_export_round_trip_header_matches_tracking_columns(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"

    written = export_tracking_csv(parquet_path, csv_path)

    assert written == csv_path
    first_line = csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert first_line == ",".join(TRACKING_COLUMNS)


def test_export_round_trip_row_count_matches(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"
    export_tracking_csv(parquet_path, csv_path)

    lines = csv_path.read_text(encoding="utf-8").splitlines()
    tracks = synthetic_tracks(with_teams=True, with_field_coords=True)
    assert len(lines) - 1 == tracks.height


def test_export_null_team_id_renders_as_empty_field(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"
    export_tracking_csv(parquet_path, csv_path)

    header = TRACKING_COLUMNS
    team_id_idx = header.index("team_id")
    lines = csv_path.read_text(encoding="utf-8").splitlines()[1:]

    referee_lines = [
        line for line in lines if line.split(",")[header.index("class_name")] == "referee"
    ]
    assert referee_lines, "expected at least one referee row (null team_id) in the fixture"
    for line in referee_lines:
        field = line.split(",")[team_id_idx]
        assert field == ""
        assert field != "null"


def test_export_floats_carry_at_most_four_decimals(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"
    export_tracking_csv(parquet_path, csv_path)

    header = TRACKING_COLUMNS
    float_columns = {"confidence", "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2",
                      "foot_x_px", "foot_y_px", "x_yards", "y_yards"}
    float_indices = [header.index(name) for name in float_columns]

    lines = csv_path.read_text(encoding="utf-8").splitlines()[1:]
    for line in lines:
        fields = line.split(",")
        for idx in float_indices:
            value = fields[idx]
            if value == "":
                continue
            if "." in value:
                decimals = value.split(".")[1]
                assert len(decimals) <= 4, f"{value!r} has more than 4 decimals"


def test_export_missing_input_parquet_raises_named_exception(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.parquet"
    csv_path = tmp_path / "out.csv"

    with pytest.raises(TrackingParquetNotFound, match=str(missing)):
        export_tracking_csv(missing, csv_path)
