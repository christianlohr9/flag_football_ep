"""Coverage for `flag_football_ep.reports.build` (REQ-S1-16) and the `ffep report` CLI
command.

Module-level fixtures/helpers land here in task 1; the `ffep report` CLI tests (task 2) and
the full raw-tree-to-written-HTML end-to-end test (task 3) extend this same file.

Config/corpus construction reuses `tests/test_model_score.py`'s `_make_config`/
`_trained_config`/`_register_run` helpers (imported as `tms`) rather than duplicating them --
those already build a fully-populated `Config` (including `competition_tier.csv`, which
`score_plays` requires) and a real train-then-register round trip against a temporary MLflow
store.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import test_model_score as tms
from flag_football_ep.config import Config
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.reports.build import (
    PRODUCTS,
    ReportRunResult,
    build_reports,
    run_report_pipeline,
)
from flag_football_ep.reports.decisions import DECISIONS_FILENAME
from flag_football_ep.reports.opponent import opponent_filename
from flag_football_ep.reports.own_team import OWN_TEAM_FILENAME
from flag_football_ep.reports.wp_review import wp_review_filename
from flag_football_ep.testing import canonical_plays_with_scores

# --- shared fixtures/helpers ----------------------------------------------------------------


def _write_group_opponents(reference_dir: Path, rows: list[tuple[str, str]]) -> None:
    reference_dir.mkdir(parents=True, exist_ok=True)
    lines = ["canonical_team,team_name"] + [f"{code},{name}" for code, name in rows]
    (reference_dir / "group_opponents.csv").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _write_player_mapping(
    reference_dir: Path, rows: list[tuple[str, str, str]] | None = None
) -> None:
    reference_dir.mkdir(parents=True, exist_ok=True)
    lines = ["source,source_player,canonical_player"]
    for row in rows or []:
        lines.append(",".join(row))
    (reference_dir / "player_mapping.csv").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _plays_and_fake_scored(
    n_games: int = 2, plays_per_game: int = 8
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """A full canonical corpus plus a hand-built `scored` frame (no real model involved) --
    enough for `build_reports`'s own dispatch/validation/isolation behaviour, which never
    inspects the numeric EP/WP values themselves.

    Play 7 (index 6) of every game is overridden to a 1-pt PAT attempt (`down=0`,
    `yards_to_go=3`) and play 8 (index 7) to a 2-pt PAT attempt (`down=0`, `yards_to_go=8`) --
    `estimate_pat_baselines` (used by `own_team.attach_epa`/`reports.decisions`) raises on a
    corpus with zero attempts of either type, matching `test_model_score.py::_training_corpus`.
    """
    height = n_games * plays_per_game
    down = [((i % plays_per_game) - 1) % 4 + 1 for i in range(height)]
    yards_to_go = [5 + ((i % plays_per_game) % 16) for i in range(height)]
    for game_idx in range(n_games):
        base = game_idx * plays_per_game
        down[base + 6] = 0
        yards_to_go[base + 6] = 3
        down[base + 7] = 0
        yards_to_go[base + 7] = 8

    plays = canonical_plays_with_scores(
        n_games=n_games,
        plays_per_game=plays_per_game,
        overrides={
            "down": down,
            "yards_to_go": yards_to_go,
            "one_point_conv_success": [
                1 if (i % plays_per_game) == 6 else 0 for i in range(height)
            ],
            "two_point_conv_success": [
                1 if (i % plays_per_game) == 7 else 0 for i in range(height)
            ],
        },
    )
    scored = plays.with_columns(
        ep=pl.lit(0.5, dtype=pl.Float64),
        epa=pl.lit(0.05, dtype=pl.Float64),
        wp=pl.lit(0.5, dtype=pl.Float64),
        home_wp=pl.lit(0.5, dtype=pl.Float64),
        away_wp=pl.lit(0.5, dtype=pl.Float64),
        wpa=pl.lit(0.01, dtype=pl.Float64),
    )
    return plays, scored


def _wp_only_plays(game_id: str = "G1", n_plays: int = 4) -> pl.DataFrame:
    """The minimal column set `attach_wp_provenance`'s pipeline (`prepare_wp_data` +
    `add_wp_variables`) and `build_wp_review_page` both need -- mirrors
    `tests/test_reports_wp_review.py::_provenance_plays`/`_game_plays`."""
    rows = []
    for play_id in range(1, n_plays + 1):
        rows.append(
            {
                "game_id": game_id,
                "play_id": play_id,
                "half": 1,
                "score_differential": 0,
                "posteam": "HOME",
                "home_team": "HOME",
                "away_team": "AWAY",
                "home_team_score": 0,
                "away_team_score": 0,
                "touchdown": 0,
                "interception": 0,
                "one_point_conv_success": 0,
                "two_point_conv_success": 0,
                "safety": 0,
            }
        )
    return pl.DataFrame(rows)


def _write_oof_wp_parquet(processed_dir: Path, rows: dict) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(processed_dir / "oof_predictions_wp.parquet")


def _promoted_config(tmp_path: Path) -> Config:
    """A config with both models trained and promoted to `champion` -- the shape
    `run_report_pipeline`'s own tests need to reach the scoring stage at all."""
    config, ep_run, wp_run = tms._trained_config(tmp_path)

    # `train_ep`/`train_wp` each leave the MLflow fluent API's active experiment pointed at
    # whichever one ran last -- `mlflow.start_run(run_id=...)` inside `_register_run` rejects a
    # run id whose own experiment doesn't match that active experiment, so it must be reset to
    # each run's own experiment right before registering it.
    mlflow_store.ensure_experiment(config.train.ep_experiment, config)
    tms._register_run(config, ep_run, "ep")
    registry.promote(registry.registered_model_name("ep"), ep_run, config)

    mlflow_store.ensure_experiment(config.train.wp_experiment, config)
    tms._register_run(config, wp_run, "wp")
    registry.promote(registry.registered_model_name("wp"), wp_run, config)

    return config


# --- build_reports: allow-list validation ---------------------------------------------------


def test_build_reports_rejects_opponent_outside_reference_file(tmp_path: Path) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    with pytest.raises(ValueError, match="NOPE"):
        build_reports(
            plays, scored, config=config, opponents=["NOPE"], products=["opponents"]
        )


def test_build_reports_valid_opponent_subset_produces_exactly_that_subset(
    tmp_path: Path,
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    rendered, notices = build_reports(
        plays, scored, config=config, opponents=["AWAY"], products=["opponents"]
    )

    assert notices == ()
    assert set(rendered) == {opponent_filename("AWAY")}


def test_build_reports_unknown_product_raises(tmp_path: Path) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    with pytest.raises(ValueError, match="bogus"):
        build_reports(plays, scored, config=config, products=["bogus"])


def test_products_tuple_is_the_four_locked_families(tmp_path: Path) -> None:
    assert PRODUCTS == ("opponents", "own-team", "decisions", "wp-review")


def test_build_reports_own_team_product_uses_own_team_filename(tmp_path: Path) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    _write_player_mapping(config.reference.player_mapping.parent)
    plays, scored = _plays_and_fake_scored()

    rendered, notices = build_reports(
        plays, scored, config=config, opponents=[], products=["own-team"]
    )

    assert notices == ()
    assert OWN_TEAM_FILENAME in rendered


def test_build_reports_none_opponents_builds_every_group_opponent(
    tmp_path: Path,
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent,
        [("AWAY", "Away Team"), ("HOME", "Home Team")],
    )
    plays, scored = _plays_and_fake_scored()

    rendered, notices = build_reports(
        plays, scored, config=config, products=["opponents"]
    )

    assert notices == ()
    assert set(rendered) == {opponent_filename("AWAY"), opponent_filename("HOME")}


# --- build_reports: per-product failure isolation (T-1.4-59) -------------------------------


def test_build_reports_product_failure_yields_notice_and_other_products_still_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    import flag_football_ep.reports.build as build_module

    def _boom(*args: object, **kwargs: object) -> str:
        raise RuntimeError("synthetic decisions failure")

    monkeypatch.setattr(build_module, "build_decisions_page", _boom)

    rendered, notices = build_reports(
        plays,
        scored,
        config=config,
        opponents=["AWAY"],
        products=["opponents", "decisions"],
    )

    assert DECISIONS_FILENAME not in rendered
    assert opponent_filename("AWAY") in rendered
    assert any("decisions" in n.lower() or "entscheidung" in n.lower() for n in notices)
    assert any("synthetic decisions failure" in n for n in notices)


# --- build_reports: WP review pages come from the provenance-resolved frame (T-1.4-67) ------


def test_build_reports_wp_review_page_states_out_of_fold_provenance(
    tmp_path: Path,
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    plays = _wp_only_plays(game_id="G1", n_plays=4)
    _write_oof_wp_parquet(
        config.paths.processed,
        {"game_id": ["G1"] * 4, "play_id": [1, 2, 3, 4], "wp": [0.5, 0.55, 0.6, 0.65]},
    )

    scored = plays.with_columns(
        wp=pl.lit(0.5, dtype=pl.Float64),
        home_wp=pl.lit(0.5, dtype=pl.Float64),
        away_wp=pl.lit(0.5, dtype=pl.Float64),
        wpa=pl.lit(0.0, dtype=pl.Float64),
    )
    rendered, notices = build_reports(
        plays, scored, config=config, opponents=[], products=["wp-review"]
    )

    assert notices == ()
    page = rendered[wp_review_filename("G1")]
    assert "out-of-fold" in page
    assert "WP-Provenienz unbekannt" not in page


# --- run_report_pipeline: stage durations and the Pitfall-1 regression guard ----------------


def test_run_report_pipeline_never_trains_and_durations_has_exactly_three_stage_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _promoted_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    _write_player_mapping(config.reference.player_mapping.parent)

    plays = tms._training_corpus()
    config.paths.processed.mkdir(parents=True, exist_ok=True)
    plays.write_parquet(config.paths.processed / "plays.parquet")

    import flag_football_ep.model.train as train_module

    def _fail_if_called(*args: object, **kwargs: object) -> str:
        raise AssertionError(
            "run_report_pipeline must never call train_ep/train_wp (RESEARCH Pitfall 1)"
        )

    monkeypatch.setattr(train_module, "train_ep", _fail_if_called)
    monkeypatch.setattr(train_module, "train_wp", _fail_if_called)

    result = run_report_pipeline(
        config, opponents=[], products=["decisions"], skip_ingest=True
    )

    assert isinstance(result, ReportRunResult)
    assert set(result.durations) == {"ingest", "score", "report"}
    assert DECISIONS_FILENAME in result.filenames


def test_run_report_pipeline_skip_ingest_records_zero_duration_and_notice(
    tmp_path: Path,
) -> None:
    config = _promoted_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    _write_player_mapping(config.reference.player_mapping.parent)

    plays = tms._training_corpus()
    config.paths.processed.mkdir(parents=True, exist_ok=True)
    plays.write_parquet(config.paths.processed / "plays.parquet")

    result = run_report_pipeline(
        config, opponents=[], products=["decisions"], skip_ingest=True
    )

    assert result.durations["ingest"] == 0.0
    assert any("skip-ingest" in n or "übersprungen" in n for n in result.notices)
