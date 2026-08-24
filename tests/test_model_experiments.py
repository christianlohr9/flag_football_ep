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
    CvSettings,
    IfafSource,
    Paths,
    ReferenceFiles,
    ReportSettings,
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
    run_real_clock_experiment,
    run_recency_candidate,
)
from flag_football_ep.model.hyperparams import (
    EP_FEATURES,
    RECENCY_HALF_LIFE_DAYS_GRID,
    TIER_FEATURE_COLUMNS,
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
    """Mirrors `tests/test_model_train.py::_make_config`. Also writes a default
    `competition_tier.csv`: `EP_FEATURES`/`WP_FEATURES` adopted the tier one-hot columns
    (plan 01.3-09), so `run_candidate`/`run_recency_candidate`/`run_real_clock_experiment`
    now call `_build_current_production_base_features` unconditionally, which reads this
    file. `canonical_plays_with_scores` always sets `competition="TEST"` regardless of
    `source`; this module's corpora use `source="hudl"` (default) and `source="ifaf"`
    (`_ifaf_wp_training_corpus`), so both need a mapping row. Tests that care about a
    specific tier value can overwrite this file after calling `_make_config`.
    """
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
        reports=tmp_path / "reports",
        video=tmp_path / "data" / "video",
        labels=tmp_path / "data" / "labels",
        tracking=tmp_path / "data" / "processed" / "tracking",
    )
    reference = ReferenceFiles(
        half_boundaries=tmp_path / "data" / "reference" / "half_boundaries.csv",
        final_scores=tmp_path / "data" / "reference" / "final_scores.csv",
        team_mapping=tmp_path / "data" / "reference" / "team_mapping.csv",
        sportapp_games=tmp_path / "data" / "reference" / "sportapp_games.csv",
        competition_tier=tmp_path / "data" / "reference" / "competition_tier.csv",
        player_mapping=tmp_path / "data" / "reference" / "player_mapping.csv",
        group_opponents=tmp_path / "data" / "reference" / "group_opponents.csv",
        hover_positions=tmp_path / "data" / "reference" / "hover_positions.csv",
        homography_calibration=tmp_path / "data" / "reference" / "homography_calibration.csv",
        gt_positions=tmp_path / "data" / "reference" / "gt_positions.csv",
        continuity_review=tmp_path / "data" / "reference" / "continuity_review.csv",
    )
    reference.competition_tier.parent.mkdir(parents=True, exist_ok=True)
    reference.competition_tier.write_text(
        "source,competition,tier\n"
        "hudl,TEST,womens-international\n"
        "ifaf,TEST,mixed-other\n"
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
    report = ReportSettings(own_team="HOME", cycle_start_season=2026)
    cv = CvSettings(
        pilot_session_id="test-session",
        detector_model="cv_detector_model_test",
        detector_experiment="cv_detector_test",
        resolution=672,
        sahi=False,
        sahi_slice=640,
        sahi_overlap=0.2,
        train_epochs=1,
        train_batch_size=4,
        train_grad_accum=4,
        device="cpu",
        label_frame_target=10,
        cvat_host="http://localhost:8080",
        cvat_username_env="CVAT_USERNAME",
        cvat_password_env="CVAT_PASSWORD",
        field_length_yards=50.0,
        field_width_yards=25.0,
        endzone_yards=10.0,
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


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


def _ifaf_wp_training_corpus(n_games: int = 3, plays_per_game: int = 16) -> pl.DataFrame:
    """IFAF-sourced synthetic corpus with a monotonically-decreasing `game_clock_ms` per
    half, plus enough games for a real (if tiny) LOGO pass -- drives the `real_clock`
    candidate tests (plan 01.3-08 Task 3).
    """
    touchdown = [0] * plays_per_game
    touchdown[-1] = 1
    half_boundary = (plays_per_game + 1) // 2
    half1_clock = [
        1200000 - i * (1200000 // half_boundary) for i in range(half_boundary)
    ]
    half2_clock = [
        1200000 - i * (1200000 // (plays_per_game - half_boundary))
        for i in range(plays_per_game - half_boundary)
    ]
    game_clock_ms = (half1_clock + half2_clock) * n_games
    overrides = {"touchdown": touchdown * n_games}
    return canonical_plays_with_scores(
        n_games=n_games,
        plays_per_game=plays_per_game,
        source="ifaf",
        overrides=overrides,
        extras={"game_clock_ms": game_clock_ms},
    )


def _passthrough_spec(name: str, extra_features: list[str], applies_to=("ep", "wp")) -> CandidateSpec:
    """A candidate spec whose build adds no columns and reports the given (already-present
    or deliberately-bogus) feature names -- used to drive the error-path tests."""

    def _build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
        return df, extra_features

    return CandidateSpec(name=name, build=_build, applies_to=applies_to)


def _season_spec() -> CandidateSpec:
    """A generic, still-testable "candidate" for the `run_candidate` happy-path tests below:
    `season` is a real `CORE_COLUMNS` Int32 column (`canonical_plays_with_scores` always
    sets it), present in every prepared frame, but not in `EP_FEATURES`/`WP_FEATURES` --
    `half` filled this role before plan 01.3-09 adopted it into production (making it
    overlap `base_features` and always raise). A `_passthrough_spec` over an
    already-present column, same shape as the old `half`-based tests."""
    return _passthrough_spec("season", ["season"])


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
        plays=plays, config=config, model_prefix="ep", spec=_season_spec()
    )

    assert isinstance(result, CandidateResult)


def test_candidate_result_exposes_every_documented_field(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=_season_spec()
    )

    assert result.name == "season"
    assert result.model_prefix == "ep"
    assert isinstance(result.control_logloss, float)
    assert isinstance(result.candidate_logloss, float)
    assert isinstance(result.delta, float)
    assert isinstance(result.adopted, bool)
    assert isinstance(result.n_folds, int)
    assert isinstance(result.n_plays, int)
    assert result.control_features == list(EP_FEATURES)
    assert result.candidate_features == [*EP_FEATURES, "season"]


def test_run_candidate_wp_uses_wp_feature_list(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="wp", spec=_season_spec()
    )

    assert result.model_prefix == "wp"
    assert result.control_features == list(WP_FEATURES)
    assert result.candidate_features == [*WP_FEATURES, "season"]


def test_run_candidate_both_arms_share_fold_count(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus(n_games=6, plays_per_game=16)

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=_season_spec()
    )

    assert result.n_folds == 6


# --- verdict / delta arithmetic ------------------------------------------------------------


def test_run_candidate_verdict_is_adopted_or_rejected(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=_season_spec()
    )

    assert result.delta == pytest.approx(result.control_logloss - result.candidate_logloss)
    assert result.adopted == (result.delta > 0)


def test_run_candidate_verdict_matches_mlflow_logged_param(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(
        plays=plays, config=config, model_prefix="ep", spec=_season_spec()
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
            plays=plays, config=config, model_prefix="bogus", spec=_season_spec()
        )


# --- MLflow logging: dedicated experiment, no registration/promotion ----------------------


def test_run_candidate_logs_to_dedicated_candidate_experiment_not_production(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_candidate(plays=plays, config=config, model_prefix="ep", spec=_season_spec())

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    candidate_experiment = client.get_experiment_by_name(candidate_experiment_name("ep", config))
    production_experiment = client.get_experiment_by_name(config.train.ep_experiment)
    assert candidate_experiment is not None
    assert production_experiment is None  # run_candidate never touches the production experiment


def test_run_candidate_logs_candidate_tag_and_metrics(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    result = run_candidate(plays=plays, config=config, model_prefix="ep", spec=_season_spec())

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(candidate_experiment_name("ep", config))
    runs = client.search_runs(experiment_ids=[experiment.experiment_id])
    run = runs[0]

    assert run.data.tags["candidate"] == "season"
    assert run.data.metrics["control_logloss"] == pytest.approx(result.control_logloss)
    assert run.data.metrics["candidate_logloss"] == pytest.approx(result.candidate_logloss)
    assert run.data.metrics["delta"] == pytest.approx(result.delta)
    assert "control_features" in run.data.params
    assert "candidate_features" in run.data.params


def test_run_candidate_never_logs_a_model_artifact(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_candidate(plays=plays, config=config, model_prefix="ep", spec=_season_spec())

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(candidate_experiment_name("ep", config))
    runs = client.search_runs(experiment_ids=[experiment.experiment_id])
    assert not runs[0].outputs.model_outputs


# --- CANDIDATES registry --------------------------------------------------------------------


def test_candidates_registry_no_longer_contains_adopted_candidates() -> None:
    """`half` and `competition_tier` were adopted into `EP_FEATURES`/`WP_FEATURES` (plan
    01.3-09, REQ-S1-09's combined-confirmation decision) -- nothing left to test via the
    generic harness, so they are no longer registered."""
    assert "half" not in CANDIDATES
    assert "competition_tier" not in CANDIDATES


def test_run_candidate_rejects_already_adopted_tier_columns_as_overlap(
    tmp_path: Path,
) -> None:
    """Re-testing `competition_tier` (now part of `base_features`) via the generic harness
    must fail with the same already-in-control-list error a duplicate feature does, not
    silently no-op -- this is what "no longer registered" (above) protects in practice: even
    a hand-built spec that reintroduces the tier columns is rejected."""
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()
    spec = _passthrough_spec("competition_tier_retest", list(TIER_FEATURE_COLUMNS))

    with pytest.raises(ExperimentError, match="tier_"):
        run_candidate(plays=plays, config=config, model_prefix="ep", spec=spec)


# --- recency candidate (plan 01.3-08 Task 2) -------------------------------------------------


def test_candidates_registry_contains_recency_ep_only() -> None:
    assert "recency" in CANDIDATES
    assert CANDIDATES["recency"].applies_to == ("ep",)


def test_recency_candidate_has_weight_build_but_real_clock_does_not() -> None:
    assert CANDIDATES["recency"].weight_build is not None
    assert CANDIDATES["real_clock"].weight_build is None


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


# --- real_clock candidate (plan 01.3-08 Task 3) -----------------------------------------------


def test_candidates_registry_contains_real_clock_wp_only() -> None:
    assert "real_clock" in CANDIDATES
    assert CANDIDATES["real_clock"].applies_to == ("wp",)


def test_run_candidate_rejects_real_clock_spec_directing_to_run_real_clock_experiment(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus()

    with pytest.raises(ExperimentError, match="run_real_clock_experiment"):
        run_candidate(
            plays=plays, config=config, model_prefix="wp", spec=CANDIDATES["real_clock"]
        )


def test_run_real_clock_experiment_raises_when_no_ifaf_rows(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()  # defaults to source="hudl", no "ifaf" rows at all

    with pytest.raises(ExperimentError, match="ifaf"):
        run_real_clock_experiment(plays=plays, config=config)


def test_run_real_clock_experiment_raises_when_fewer_than_two_ifaf_games(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus(n_games=1)

    with pytest.raises(ExperimentError, match="game"):
        run_real_clock_experiment(plays=plays, config=config)


def test_run_real_clock_experiment_raises_named_when_a_wp_column_is_entirely_null(
    tmp_path: Path,
) -> None:
    # Mirrors the real corpus's IFAF source, where `yards_to_go` is null for every row
    # (01.3-08-SUMMARY.md finding) -- drop_nulls() would otherwise silently produce a 0-row
    # frame that fails deep inside run_logo with an unnamed sklearn error.
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus().with_columns(
        pl.lit(None, dtype=pl.Int32).alias("yards_to_go")
    )

    with pytest.raises(ExperimentError, match="yards_to_go"):
        run_real_clock_experiment(plays=plays, config=config)


def test_run_real_clock_experiment_returns_synthetic_and_real_clock_results(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus()

    synthetic_result, real_clock_result = run_real_clock_experiment(
        plays=plays, config=config
    )

    assert isinstance(synthetic_result, CandidateResult)
    assert isinstance(real_clock_result, CandidateResult)
    assert synthetic_result.model_prefix == "wp"
    assert real_clock_result.model_prefix == "wp"


def test_run_real_clock_experiment_delta_and_adopted_consistent(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus()

    synthetic_result, real_clock_result = run_real_clock_experiment(
        plays=plays, config=config
    )

    assert synthetic_result.delta == pytest.approx(
        synthetic_result.control_logloss - synthetic_result.candidate_logloss
    )
    assert real_clock_result.delta == real_clock_result.delta
    assert synthetic_result.adopted == real_clock_result.adopted
    assert synthetic_result.adopted == (synthetic_result.delta > 0)


def test_run_real_clock_experiment_feature_lists_are_wp_features(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus()

    synthetic_result, real_clock_result = run_real_clock_experiment(
        plays=plays, config=config
    )

    assert synthetic_result.control_features == list(WP_FEATURES)
    assert synthetic_result.candidate_features == list(WP_FEATURES)
    assert real_clock_result.control_features == list(WP_FEATURES)
    assert real_clock_result.candidate_features == list(WP_FEATURES)


def test_run_real_clock_experiment_logs_two_mlflow_runs_tagged_real_clock(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus()

    run_real_clock_experiment(plays=plays, config=config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(candidate_experiment_name("wp", config))
    runs = client.search_runs(experiment_ids=[experiment.experiment_id])

    assert len(runs) == 2
    arms = {run.data.params["arm"] for run in runs}
    assert arms == {"synthetic", "real_clock"}
    for run in runs:
        assert run.data.tags["candidate"] == "real_clock"


def test_run_real_clock_experiment_never_touches_production_wp_experiment(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _ifaf_wp_training_corpus()

    run_real_clock_experiment(plays=plays, config=config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    production_experiment = client.get_experiment_by_name(config.train.wp_experiment)

    assert production_experiment is None


def test_wp_features_never_includes_game_clock_ms() -> None:
    assert "game_clock_ms" not in WP_FEATURES


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
    # `half`/`competition_tier` were adopted into production (plan 01.3-09) and are no
    # longer valid `--candidate` choices -- `recency`/`real_clock` are what remain.
    assert "recency" in result.output
    assert "real_clock" in result.output


def test_experiment_cli_invalid_model_exits_nonzero_naming_allowed_values() -> None:
    result = runner.invoke(app, ["experiment", "--model", "bogus"])

    assert result.exit_code != 0
    assert "ep" in result.output
    assert "wp" in result.output
    assert "both" in result.output
