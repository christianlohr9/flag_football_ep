"""CLI help smoke test for every ffep subcommand, plus a fixture round-trip check."""

from __future__ import annotations

import re
from pathlib import Path

import mlflow
import polars as pl
from typer.testing import CliRunner

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    """Strip ANSI escape codes -- rich's `--help` rendering splits an option's leading
    `--` from the rest of its name with a color-reset escape sequence (e.g. `--model`
    becomes `-` + reset + `-model`), which breaks a naive substring check against the raw
    CliRunner output."""
    return _ANSI_RE.sub("", output)

import test_pipeline_ingest as tpi
from flag_football_ep.cli import app
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.model.score import load_model
from flag_football_ep.model.train import train_ep
from flag_football_ep.testing import canonical_plays_with_scores

runner = CliRunner()

SUBCOMMANDS = ["ingest", "fetch-sportapp", "fetch-ifaf", "train", "score", "promote", "run"]


def _training_corpus(n_games: int = 4, plays_per_game: int = 16) -> pl.DataFrame:
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # mid-half, second drive of the half
    overrides = {"touchdown": touchdown * n_games}
    return canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )


def _register_run(config, run_id: str, model_prefix: str = "ep") -> None:
    """Register `run_id`'s already-logged model under `model_prefix`'s registered model name
    -- `train_ep`/`train_wp` don't call `registered_model_name=` themselves until plan
    01.3-05, so `ffep promote` tests against a real training run must register a version
    explicitly first.
    """
    name = registry.registered_model_name(model_prefix)
    mlflow_store.configure(config)
    with mlflow.start_run(run_id=run_id):
        model = load_model(run_id, config)
        registry.register_production_model(model, name, config)


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


def test_promote_help() -> None:
    result = runner.invoke(app, ["promote", "--help"])
    assert result.exit_code == 0
    output = _plain(result.output)
    assert "--model" in output
    assert "--run" in output


def test_promote_with_explicit_run_id_makes_it_champion(tmp_path: Path, repo_root: Path) -> None:
    config = tpi._make_config(tmp_path, repo_root)
    toml_path = tpi._write_toml_config(tmp_path, repo_root)
    plays = _training_corpus()
    run_id = train_ep(plays, config)
    _register_run(config, run_id, "ep")

    result = runner.invoke(
        app, ["promote", "--config", str(toml_path), "--model", "ep", "--run", run_id]
    )

    assert result.exit_code == 0, result.output
    assert registry.resolve_champion("ep_model", config) == run_id


def test_promote_without_run_promotes_most_recent_finished_run(
    tmp_path: Path, repo_root: Path
) -> None:
    config = tpi._make_config(tmp_path, repo_root)
    toml_path = tpi._write_toml_config(tmp_path, repo_root)
    plays = _training_corpus()
    first_run_id = train_ep(plays, config)
    _register_run(config, first_run_id, "ep")
    latest_run_id = train_ep(plays, config)
    _register_run(config, latest_run_id, "ep")

    result = runner.invoke(app, ["promote", "--config", str(toml_path), "--model", "ep"])

    assert result.exit_code == 0, result.output
    assert latest_run_id in result.output
    assert registry.resolve_champion("ep_model", config) == latest_run_id


def test_promote_bogus_model_exits_nonzero_naming_allowed_values(
    tmp_path: Path, repo_root: Path
) -> None:
    toml_path = tpi._write_toml_config(tmp_path, repo_root)

    result = runner.invoke(app, ["promote", "--config", str(toml_path), "--model", "bogus"])

    assert result.exit_code != 0
    assert "ep" in result.output
    assert "wp" in result.output
    assert "both" in result.output


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
