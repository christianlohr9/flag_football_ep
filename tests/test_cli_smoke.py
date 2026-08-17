"""CLI help smoke test for every ffep subcommand, plus a fixture round-trip check."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from typer.testing import CliRunner

from flag_football_ep.cli import app

runner = CliRunner()

SUBCOMMANDS = ["ingest", "fetch-sportapp", "fetch-ifaf", "train", "score", "run"]


def test_top_level_help_lists_all_subcommands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for name in SUBCOMMANDS:
        assert name in result.output


def test_ingest_help() -> None:
    result = runner.invoke(app, ["ingest", "--help"])
    assert result.exit_code == 0


def test_fetch_sportapp_help() -> None:
    result = runner.invoke(app, ["fetch-sportapp", "--help"])
    assert result.exit_code == 0


def test_fetch_ifaf_help() -> None:
    result = runner.invoke(app, ["fetch-ifaf", "--help"])
    assert result.exit_code == 0


def test_train_help() -> None:
    result = runner.invoke(app, ["train", "--help"])
    assert result.exit_code == 0


def test_score_help() -> None:
    result = runner.invoke(app, ["score", "--help"])
    assert result.exit_code == 0


def test_run_help() -> None:
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0


def test_make_hudl_csv_round_trips_through_polars(tmp_path: Path, make_hudl_csv) -> None:
    path = make_hudl_csv(
        tmp_path,
        "2026-06-14_GER-vs-AUT_EM-QUALI.csv",
        rows=[
            {
                "PLAY #": "1",
                "ODK": "O",
                "DN": "1",
                "DIST": "10",
                "YARD LN": "25",
                "PLAY TYPE": "Rush",
                "RESULT": "Complete",
                "GN/LS": "5",
            }
        ],
    )

    df = pl.read_csv(path, separator=";", infer_schema_length=0)

    expected_columns = ["PLAY #", "ODK", "DN", "DIST", "YARD LN", "PLAY TYPE", "RESULT", "GN/LS"]
    assert df.columns == expected_columns
    # utf-8-sig BOM must not leak into the first column name.
    assert not df.columns[0].startswith("﻿")
    assert df.columns[0] == "PLAY #"
