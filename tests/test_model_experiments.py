"""Coverage for `flag_football_ep.model.experiments`: the REQ-S1-09 feature-candidate
harness (control-vs-candidate LOGO comparison, MLflow-logged verdict).

Every test builds a config pointing `mlruns`/`models` at `tmp_path` (never the real repo
`mlruns/`) and a synthetic multi-game canonical corpus, matching `tests/test_model_train.py`'s
`_make_config`/`_ep_training_corpus` pattern. Tests assert on verdict/params/metrics and on
the equal-row-set property -- never that a particular candidate wins, since the synthetic
corpus cannot support that claim (the real verdict comes from the CLI run in plan 01.3-07
Task 3).
"""

from __future__ import annotations

import re
from pathlib import Path

import mlflow
import polars as pl
import pytest
from typer.testing import CliRunner

from flag_football_ep.cli import app
from flag_football_ep.config import (
    Config,
    IfafSource,
    Paths,
    ReferenceFiles,
    Sources,
    SportappSource,
    TrainSettings,
)
from flag_football_ep.model import mlflow_store
from flag_football_ep.model.experiments import (
    CANDIDATES,
    CandidateResult,
    CandidateSpec,
    ExperimentError,
    candidate_experiment_name,
    run_candidate,
    run_recency_candidate,
)
from flag_football_ep.model.hyperparams import (
    EP_FEATURES,
    RECENCY_HALF_LIFE_DAYS_GRID,
    WP_FEATURES,
)
from flag_football_ep.testing import canonical_plays_with_scores

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    """Strip ANSI escape codes -- matches `tests/test_cli_smoke.py`'s `_plain` helper, since
    rich's `--help` rendering can split an option's leading `--` from its name."""
    return _ANSI_RE.sub("", output)


runner = CliRunner()

# --- shared test config/corpus helpers (mirrors tests/test_model_train.py) ---------------


def _make_config(
    tmp_path: Path,
    exclude_games_ep: list[str] | None = None,
    exclude_games_wp: list[str] | None = None,
) -> Config:
    paths = Paths(
        data_root=tmp_path / "data",
        raw_hudl=tmp_path / "data" / "raw" / "hudl",
        raw_sportapp=tmp_path / "data" / "raw" / "sportapp",
        raw_ifaf=tmp_path / "data" / "raw" / "ifaf",
        raw_legacy=tmp_path / "data" / "raw" / "legacy",
        processed=tmp_path / "data" / "processed",
        reference=tmp_path / "data" / "reference",
        models=tmp_path / "models",
        mlruns=tmp_path / "mlruns",
        contract=tmp_path / "docs" / "data-contract.schema.json",
    )
    reference = ReferenceFiles(
        half_boundaries=tmp_path / "data" / "reference" / "half_boundaries.csv",
        final_scores=tmp_path / "data" / "reference" / "final_scores.csv",
        team_mapping=tmp_path / "data" / "reference" / "team_mapping.csv",
        sportapp_games=tmp_path / "data" / "reference" / "sportapp_games.csv",
        competition_tier=tmp_path / "data" / "reference" / "competition_tier.csv",
    )
    sources = Sources(
        sportapp=SportappSource(
            base_url="https://example.invalid/api/v1/public", api_key_env="SPORTAPP_API_KEY"
        ),
        ifaf=IfafSource(
            base_url="https://example.invalid/v1",
            tournament="test-tournament",
            api_key_env="CPX_API_KEY",
        ),
    )
    train = TrainSettings(
        ep_experiment="ep_model_test",
        wp_experiment="wp_model_test",
        exclude_games_ep=exclude_games_ep or [],
        exclude_games_wp=exclude_games_wp or [],
    )
    return Config(paths=paths, reference=reference, sources=sources, train=train)


def _ep_training_corpus(n_games: int = 12, plays_per_game: int = 16) -> pl.DataFrame:
    """Mirrors `tests/test_model_train.py::_ep_training_corpus` -- enough scoring variation
    for `make_ep_model_mutations`'s sample-weight computation and a real (if tiny) LOGO
    pass (12 games -> 12 folds).
    """
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # mid-half, second drive of the half
    overrides = {"touchdown": touchdown * n_games}
    return canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )


def _ep_training_corpus_with_dates(n_games: int = 12, plays_per_game: int = 16) -> pl.DataFrame:
    """`_ep_training_corpus` with `game_date` populated one distinct date per game, 90 days
    apart -- spans more than one entry of `RECENCY_HALF_LIFE_DAYS_GRID` (min 60 days), so
    the recency candidate's arms actually differ from the control and from each other.
    """
    from datetime import date, timedelta

    touchdown = [0] * plays_per_game
    touchdown[5] = 1
    overrides = {"touchdown": touchdown * n_games}
    base_date = date(2024, 1, 1)
    game_dates: list[str] = []
    for game_idx in range(n_games):
        game_date = (base_date + timedelta(days=game_idx * 90)).isoformat()
        game_dates.extend([game_date] * plays_per_game)
    return canonical_plays_with_scores(
        n_games=n_games,
        plays_per_game=plays_per_game,
        overrides=overrides,
        extras={"game_date": game_dates},
    )


def _passthrough_spec(name: str, extra_features: list[str], applies_to=("ep", "wp")) -> CandidateSpec:
    """A candidate spec whose build adds no columns and reports the given (already-present
    or deliberately-bogus) feature names -- used to drive the error-path tests."""

    def _build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
        return df, extra_features

    return CandidateSpec(name=name, build=_build, applies_to=applies_to)


def _partial_null_feature_spec(name: str = "flaky") -> CandidateSpec:
    """A candidate spec that adds a feature column with a null on exactly one row -- drives
    the row-count-mismatch error path (the null row survives the control arm's
    `drop_nulls()` but is dropped from the candidate arm's).
    """

    def _build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
        augmented = df.with_columns(
            pl.when(pl.col("play_id") == 1)
            .then(None)
            .otherwise(pl.lit(1.0))
            .alias("_flaky_feature")
        )
        return augmented, ["_flaky_feature"]

    return CandidateSpec(name=name, build=_build, applies_to=("ep", "wp"))


# --- candidate_experiment_name -------------------------------------------------------------


def test_candidate_experiment_name_appends_suffix_for_ep(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    assert candidate_experiment_name("ep", config) == "ep_model_test_candidates"


def test_candidate_experiment_name_appends_suffix_for_wp(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    assert candidate_experiment_name("wp", config) == "wp_model_test_candidates"


def test_candidate_experiment_name_rejects_unknown_prefix(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    with pytest.raises(ExperimentError):
        candidate_experiment_name("bogus", config)


def test_candidate_experiment_name_is_never_the_production_experiment(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    assert candidate_experiment_name("ep", config) != config.train.ep_experiment
    assert candidate_experiment_name("wp", config) != config.train.wp_experiment


# --- run_candidate: happy path / CandidateResult fields -----------------------------------


def test_run_candidate_returns_candidate_result(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"]
    )

    assert isinstance(result, CandidateResult)


def test_candidate_result_exposes_every_documented_field(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"]
    )

    assert result.name == "half"
    assert result.model_prefix == "ep"
    assert isinstance(result.control_logloss, float)
    assert isinstance(result.candidate_logloss, float)
    assert isinstance(result.delta, float)
    assert isinstance(result.adopted, bool)
    assert isinstance(result.n_folds, int)
    assert isinstance(result.n_plays, int)
    assert result.control_features == list(EP_FEATURES)
    assert result.candidate_features == [*EP_FEATURES, "half"]


def test_run_candidate_wp_uses_wp_feature_list(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="wp", spec=CANDIDATES["half"]
    )

    assert result.model_prefix == "wp"
    assert result.control_features == list(WP_FEATURES)
    assert result.candidate_features == [*WP_FEATURES, "half"]


def test_run_candidate_both_arms_share_fold_count(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus(n_games=6, plays_per_game=16)

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"]
    )

    assert result.n_folds == 6


# --- verdict / delta arithmetic ------------------------------------------------------------


def test_run_candidate_verdict_is_adopted_or_rejected(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"]
    )

    assert result.delta == pytest.approx(result.control_logloss - result.candidate_logloss)
    assert result.adopted == (result.delta > 0)


def test_run_candidate_verdict_matches_mlflow_logged_param(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"]
    )

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    experiment_name = candidate_experiment_name("ep", config)
    experiment = client.get_experiment_by_name(experiment_name)
    runs = client.search_runs(experiment_ids=[experiment.experiment_id])
    assert len(runs) == 1
    expected_verdict = "adopted" if result.adopted else "rejected"
    assert runs[0].data.params["verdict"] == expected_verdict


# --- equal-row-set / offending-column error paths ------------------------------------------


def test_run_candidate_raises_when_arms_do_not_share_same_row_set(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    with pytest.raises(ExperimentError, match="row"):
        run_candidate(
            plays=plays,
            config=config,
            model_prefix="ep",
            spec=_partial_null_feature_spec(),
        )


def test_run_candidate_raises_naming_feature_already_in_control_list(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()
    spec = _passthrough_spec("dupe", ["yardline_50"])

    with pytest.raises(ExperimentError, match="yardline_50"):
        run_candidate(plays=plays, config=config, model_prefix="ep", spec=spec)


def test_run_candidate_raises_naming_feature_absent_from_frame(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()
    spec = _passthrough_spec("bogus_feature", ["does_not_exist_anywhere"])

    with pytest.raises(ExperimentError, match="does_not_exist_anywhere"):
        run_candidate(plays=plays, config=config, model_prefix="ep", spec=spec)


def test_run_candidate_raises_when_candidate_does_not_apply_to_model_prefix(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()
    spec = _passthrough_spec("ep_only", ["yards_to_go"], applies_to=("ep",))
    # yards_to_go overlaps WP_FEATURES too, but applies_to should fail first.

    with pytest.raises(ExperimentError):
        run_candidate(plays=plays, config=config, model_prefix="wp", spec=spec)


def test_run_candidate_rejects_unknown_model_prefix(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    with pytest.raises(ExperimentError):
        run_candidate(
            plays=plays, config=config, model_prefix="bogus", spec=CANDIDATES["half"]
        )


# --- MLflow logging: dedicated experiment, no registration/promotion ----------------------


def test_run_candidate_logs_to_dedicated_candidate_experiment_not_production(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_candidate(plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"])

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    candidate_experiment = client.get_experiment_by_name(candidate_experiment_name("ep", config))
    production_experiment = client.get_experiment_by_name(config.train.ep_experiment)
    assert candidate_experiment is not None
    assert production_experiment is None  # run_candidate never touches the production experiment


def test_run_candidate_logs_candidate_tag_and_metrics(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"])

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(candidate_experiment_name("ep", config))
    runs = client.search_runs(experiment_ids=[experiment.experiment_id])
    run = runs[0]

    assert run.data.tags["candidate"] == "half"
    assert run.data.metrics["control_logloss"] == pytest.approx(result.control_logloss)
    assert run.data.metrics["candidate_logloss"] == pytest.approx(result.candidate_logloss)
    assert run.data.metrics["delta"] == pytest.approx(result.delta)
    assert "control_features" in run.data.params
    assert "candidate_features" in run.data.params


def test_run_candidate_never_logs_a_model_artifact(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_candidate(plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["half"])

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(candidate_experiment_name("ep", config))
    runs = client.search_runs(experiment_ids=[experiment.experiment_id])
    assert not runs[0].outputs.model_outputs


# --- CANDIDATES registry --------------------------------------------------------------------


def test_candidates_registry_contains_half() -> None:
    assert "half" in CANDIDATES
    assert CANDIDATES["half"].applies_to == ("ep", "wp")


# --- competition_tier candidate (plan 01.3-07 Task 2) ---------------------------------------


def _write_competition_tier_csv(config: Config, rows: list[tuple[str, str, str]]) -> None:
    """Write a minimal `source,competition,tier` CSV at `config.reference.competition_tier`
    -- the real reference file's schema, built inline per test rather than depending on the
    checked-in `data/reference/competition_tier.csv`."""
    config.reference.competition_tier.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "source": [r[0] for r in rows],
            "competition": [r[1] for r in rows],
            "tier": [r[2] for r in rows],
        }
    ).write_csv(config.reference.competition_tier)


def test_competition_tier_candidate_is_registered_in_candidates(tmp_path: Path) -> None:
    assert "competition_tier" in CANDIDATES
    assert CANDIDATES["competition_tier"].applies_to == ("ep", "wp")


def test_run_candidate_competition_tier_control_and_candidate_arms_share_row_count(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()
    # canonical_plays_with_scores defaults every row to source="hudl", competition="TEST".
    _write_competition_tier_csv(config, [("hudl", "TEST", "womens-international")])

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["competition_tier"]
    )

    assert result.n_plays > 0
    assert result.candidate_features == [
        *EP_FEATURES,
        "tier_womens_international",
        "tier_womens_national",
        "tier_mixed_other",
    ]


def test_run_candidate_competition_tier_wp_also_measures_cleanly(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()
    _write_competition_tier_csv(config, [("hudl", "TEST", "mixed-other")])

    result = run_candidate(
        plays=plays, config=config, model_prefix="wp", spec=CANDIDATES["competition_tier"]
    )

    assert result.model_prefix == "wp"
    assert result.candidate_features == [
        *WP_FEATURES,
        "tier_womens_international",
        "tier_womens_national",
        "tier_mixed_other",
    ]


# --- recency candidate (plan 01.3-08 Task 2) -------------------------------------------------


def test_candidates_registry_contains_recency_ep_only() -> None:
    assert "recency" in CANDIDATES
    assert CANDIDATES["recency"].applies_to == ("ep",)


def test_recency_candidate_has_weight_build_but_half_and_competition_tier_do_not() -> None:
    assert CANDIDATES["recency"].weight_build is not None
    assert CANDIDATES["half"].weight_build is None
    assert CANDIDATES["competition_tier"].weight_build is None


def test_run_candidate_rejects_recency_spec_directing_to_run_recency_candidate(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    with pytest.raises(ExperimentError, match="run_recency_candidate"):
        run_candidate(plays=plays, config=config, model_prefix="ep", spec=CANDIDATES["recency"])


def test_run_recency_candidate_rejects_wp_naming_prefix(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    with pytest.raises(ExperimentError, match="wp"):
        run_recency_candidate(plays=plays, config=config, model_prefix="wp")


def test_run_recency_candidate_returns_one_result_per_grid_entry(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    _, half_life_results = run_recency_candidate(plays=plays, config=config, model_prefix="ep")

    assert len(half_life_results) == len(RECENCY_HALF_LIFE_DAYS_GRID)


def test_run_recency_candidate_best_result_is_min_candidate_logloss(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    best, half_life_results = run_recency_candidate(
        plays=plays, config=config, model_prefix="ep"
    )

    assert best.candidate_logloss == min(r.candidate_logloss for r in half_life_results)
    assert best in half_life_results


def test_run_recency_candidate_best_name_identifies_winning_half_life(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    best, _ = run_recency_candidate(plays=plays, config=config, model_prefix="ep")

    assert best.name.startswith("recency_half_life_")
    winning_half_life = float(best.name.removeprefix("recency_half_life_"))
    assert winning_half_life in RECENCY_HALF_LIFE_DAYS_GRID


def test_run_recency_candidate_adopted_matches_delta_arithmetic(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    _, half_life_results = run_recency_candidate(plays=plays, config=config, model_prefix="ep")

    for result in half_life_results:
        assert result.delta == pytest.approx(result.control_logloss - result.candidate_logloss)
        assert result.adopted == (result.delta > 0)


def test_run_recency_candidate_feature_lists_identical_to_control_across_arms(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    _, half_life_results = run_recency_candidate(plays=plays, config=config, model_prefix="ep")

    for result in half_life_results:
        assert result.model_prefix == "ep"
        assert result.control_features == list(EP_FEATURES)
        assert result.candidate_features == list(EP_FEATURES)


def test_run_recency_candidate_logs_one_mlflow_run_per_arm_tagged_recency(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    run_recency_candidate(plays=plays, config=config, model_prefix="ep")

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(candidate_experiment_name("ep", config))
    runs = client.search_runs(experiment_ids=[experiment.experiment_id])

    # One control run plus one run per half-life grid entry.
    assert len(runs) == len(RECENCY_HALF_LIFE_DAYS_GRID) + 1
    for run in runs:
        assert run.data.tags["candidate"] == "recency"
        assert "half_life_days" in run.data.params

    half_life_params = {run.data.params["half_life_days"] for run in runs}
    assert "control" in half_life_params
    for half_life in RECENCY_HALF_LIFE_DAYS_GRID:
        assert str(half_life) in half_life_params


def test_run_recency_candidate_never_logs_to_production_experiment(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus_with_dates()

    run_recency_candidate(plays=plays, config=config, model_prefix="ep")

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    production_experiment = client.get_experiment_by_name(config.train.ep_experiment)

    assert production_experiment is None


# --- `ffep experiment` CLI (plan 01.3-07 Task 3) --------------------------------------------


def test_experiment_cli_help_exits_zero_and_lists_options() -> None:
    result = runner.invoke(app, ["experiment", "--help"])

    assert result.exit_code == 0
    output = _plain(result.output)
    assert "--candidate" in output
    assert "--model" in output


def test_experiment_cli_unknown_candidate_exits_nonzero_naming_valid_candidates() -> None:
    result = runner.invoke(app, ["experiment", "--candidate", "bogus"])

    assert result.exit_code != 0
    assert "half" in result.output
    assert "competition_tier" in result.output


def test_experiment_cli_invalid_model_exits_nonzero_naming_allowed_values() -> None:
    result = runner.invoke(app, ["experiment", "--model", "bogus"])

    assert result.exit_code != 0
    assert "ep" in result.output
    assert "wp" in result.output
    assert "both" in result.output
