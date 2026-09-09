"""Coverage for `flag_football_ep.model.gate`: the mechanical promotion-gate checks a
candidate MLflow run must pass before `ffep promote` will move the `champion` alias to it.

Every test builds a config pointing `mlruns`/`models` at `tmp_path` (never the real repo
`mlruns/`), matching the pattern in `tests/test_model_registry.py`. Runs are produced by
starting an `mlflow.start_run()` and logging synthetic metrics directly -- these tests never
train a real model, they exercise the gate's metric-reading/threshold logic in isolation.
"""

from __future__ import annotations

from pathlib import Path

import mlflow
import numpy as np
import pytest
import xgboost as xgb

from flag_football_ep.config import (
    Config,
    CvSettings,
    GateThresholds,
    IfafSource,
    Paths,
    ReferenceFiles,
    ReportSettings,
    Sources,
    SportappSource,
    TrainSettings,
)
from flag_football_ep.model import gate, mlflow_store, registry
from flag_football_ep.model.gate import GateError, GateResult, evaluate_gate

# --- shared test config helper (matches tests/test_model_registry.py) ----------------------


def _make_config(tmp_path: Path) -> Config:
    """A fully-populated Config pointing every path at `tmp_path` -- never the real repo."""
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
        exclude_games_ep=[],
        exclude_games_wp=[],
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
        dvc_remote_name="otc-obs",
        dvc_remote_url="s3://test-bucket/flag-football-datasets",
        dvc_remote_endpoint="https://obs.eu-de.otc.t-systems.com",
        otc_obs_access_key_env="OTC_OBS_ACCESS_KEY_ID",
        otc_obs_secret_key_env="OTC_OBS_SECRET_ACCESS_KEY",
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


# --- fixture-store helpers -----------------------------------------------------------------


def _log_run(config: Config, experiment: str, metrics: dict[str, float]) -> str:
    """Start a run against `experiment`, log `metrics`, return the run id. No model version
    registered -- used for candidate runs and for a champion's underlying run when the
    champion is produced separately via `_register_and_promote`.
    """
    mlflow_store.ensure_experiment(experiment, config)
    with mlflow.start_run() as run:
        for key, value in metrics.items():
            mlflow.log_metric(key, value)
    return run.info.run_id


def _register_and_promote(
    config: Config, name: str, experiment: str, metrics: dict[str, float]
) -> str:
    """Start a run, log `metrics`, register a trivially-fit model version under `name`, and
    promote it to `champion`. Returns the run id -- the gate's `resolve_champion` call will
    find this run.
    """
    mlflow_store.ensure_experiment(experiment, config)
    with mlflow.start_run() as run:
        for key, value in metrics.items():
            mlflow.log_metric(key, value)
        model = xgb.XGBRegressor(n_estimators=2, max_depth=2)
        model.fit(np.array([[0.0], [1.0], [2.0]]), np.array([0.0, 1.0, 2.0]))
        registry.register_production_model(model, name, config)
    registry.promote(name, run.info.run_id, config)
    return run.info.run_id


_PASSING_EP_METRICS = {
    "logo_mlogloss": 0.60,
    "naive_mlogloss": 0.70,
    "logloss_improvement": 0.10,
    "calibration_max_deviation_touchdown": 0.05,
    "calibration_max_deviation_no_score": 0.03,
    "no_play_share": 0.0,
}


# --- evaluate_gate: overall pass/fail ------------------------------------------------------


def test_evaluate_gate_passes_when_no_champion_and_all_metrics_within_tolerance(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    run_id = _log_run(config, "ep_model_test", _PASSING_EP_METRICS)

    result = evaluate_gate("ep", run_id, config)

    assert isinstance(result, GateResult)
    assert result.passed is True
    assert result.checks["beats_naive"][0] is True
    assert result.checks["beats_champion"][0] is True
    assert "no" in result.checks["beats_champion"][1] and "champion" in result.checks["beats_champion"][1]
    assert result.checks["calibration"][0] is True
    assert result.checks["no_play_share"][0] is True


def test_evaluate_gate_uses_default_thresholds_from_config_when_none_given(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    assert config.promotion_gate == GateThresholds()
    run_id = _log_run(config, "ep_model_test", _PASSING_EP_METRICS)

    result = evaluate_gate("ep", run_id, config, thresholds=None)

    assert result.passed is True


# --- check 1: beats naive baseline ----------------------------------------------------------


def test_evaluate_gate_fails_beats_naive_when_improvement_not_positive(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {**_PASSING_EP_METRICS, "logloss_improvement": -0.01}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    assert result.checks["beats_naive"][0] is False
    assert result.passed is False


def test_evaluate_gate_fails_beats_naive_when_improvement_exactly_zero(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {**_PASSING_EP_METRICS, "logloss_improvement": 0.0}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    assert result.checks["beats_naive"][0] is False


def test_evaluate_gate_skips_beats_naive_when_metric_absent(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {k: v for k, v in _PASSING_EP_METRICS.items() if k != "logloss_improvement"}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["beats_naive"]
    assert status is None
    assert "skipped" in reason
    # A skipped check never blocks a promotion by itself -- every other check still passes.
    assert result.passed is True


# --- check 2: beats champion -----------------------------------------------------------------


def test_evaluate_gate_beats_champion_passes_automatically_when_none_set(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    run_id = _log_run(config, "ep_model_test", _PASSING_EP_METRICS)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["beats_champion"]
    assert status is True
    assert "champion" in reason


def test_evaluate_gate_beats_champion_fails_when_candidate_worse(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _register_and_promote(
        config, "ep_model", "ep_model_test", {"logo_mlogloss": 0.50}
    )
    candidate_metrics = {**_PASSING_EP_METRICS, "logo_mlogloss": 0.90}
    run_id = _log_run(config, "ep_model_test", candidate_metrics)

    result = evaluate_gate("ep", run_id, config)

    assert result.checks["beats_champion"][0] is False
    assert result.passed is False


def test_evaluate_gate_beats_champion_passes_when_candidate_strictly_better(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _register_and_promote(
        config, "ep_model", "ep_model_test", {"logo_mlogloss": 0.90}
    )
    candidate_metrics = {**_PASSING_EP_METRICS, "logo_mlogloss": 0.50}
    run_id = _log_run(config, "ep_model_test", candidate_metrics)

    result = evaluate_gate("ep", run_id, config)

    assert result.checks["beats_champion"][0] is True


def test_evaluate_gate_beats_champion_ties_pass_with_default_zero_epsilon(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _register_and_promote(
        config, "ep_model", "ep_model_test", {"logo_mlogloss": 0.60}
    )
    candidate_metrics = {**_PASSING_EP_METRICS, "logo_mlogloss": 0.60}
    run_id = _log_run(config, "ep_model_test", candidate_metrics)

    result = evaluate_gate("ep", run_id, config)

    assert result.checks["beats_champion"][0] is True


def test_evaluate_gate_beats_champion_epsilon_widens_the_tolerance(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _register_and_promote(
        config, "ep_model", "ep_model_test", {"logo_mlogloss": 0.60}
    )
    # Slightly worse than champion, but within a widened epsilon.
    candidate_metrics = {**_PASSING_EP_METRICS, "logo_mlogloss": 0.62}
    run_id = _log_run(config, "ep_model_test", candidate_metrics)

    default_result = evaluate_gate("ep", run_id, config)
    assert default_result.checks["beats_champion"][0] is False

    widened = GateThresholds(champion_epsilon=0.05)
    widened_result = evaluate_gate("ep", run_id, config, thresholds=widened)
    assert widened_result.checks["beats_champion"][0] is True


def test_evaluate_gate_beats_champion_skips_when_champion_run_missing_metric(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    # Champion run logs no logo_mlogloss at all (e.g. predates this metric convention).
    _register_and_promote(config, "ep_model", "ep_model_test", {})
    run_id = _log_run(config, "ep_model_test", _PASSING_EP_METRICS)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["beats_champion"]
    assert status is None
    assert "skipped" in reason


# --- check 3: calibration --------------------------------------------------------------------


def test_evaluate_gate_calibration_fails_when_any_class_exceeds_tolerance(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    metrics = {**_PASSING_EP_METRICS, "calibration_max_deviation_touchdown": 0.40}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["calibration"]
    assert status is False
    assert "calibration_max_deviation_touchdown" in reason
    assert result.passed is False


def test_evaluate_gate_calibration_passes_at_exactly_the_threshold(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {**_PASSING_EP_METRICS, "calibration_max_deviation_touchdown": 0.15}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    assert result.checks["calibration"][0] is True


def test_evaluate_gate_calibration_skips_when_no_metrics_present(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {
        k: v for k, v in _PASSING_EP_METRICS.items() if not k.startswith("calibration_max_deviation_")
    }
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["calibration"]
    assert status is None
    assert "predates" in reason
    assert result.passed is True


def test_evaluate_gate_custom_calibration_threshold_is_honored(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {**_PASSING_EP_METRICS, "calibration_max_deviation_touchdown": 0.20}
    run_id = _log_run(config, "ep_model_test", metrics)

    default_result = evaluate_gate("ep", run_id, config)
    assert default_result.checks["calibration"][0] is False

    looser = GateThresholds(max_calibration_deviation=0.25)
    looser_result = evaluate_gate("ep", run_id, config, thresholds=looser)
    assert looser_result.checks["calibration"][0] is True


# --- check 4: no-play share ------------------------------------------------------------------


def test_evaluate_gate_no_play_share_fails_when_over_threshold(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {**_PASSING_EP_METRICS, "no_play_share": 0.05}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["no_play_share"]
    assert status is False
    assert result.passed is False


def test_evaluate_gate_no_play_share_passes_at_exactly_the_threshold(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {**_PASSING_EP_METRICS, "no_play_share": 0.02}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    assert result.checks["no_play_share"][0] is True


def test_evaluate_gate_no_play_share_skips_when_metric_absent(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    metrics = {k: v for k, v in _PASSING_EP_METRICS.items() if k != "no_play_share"}
    run_id = _log_run(config, "ep_model_test", metrics)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["no_play_share"]
    assert status is None
    assert "predates" in reason
    assert result.passed is True


# --- per-model metric-key resolution (T-M3-05-14 / RESEARCH Pitfall 2) ----------------------


def test_evaluate_gate_reads_logo_mlogloss_for_ep_never_logo_logloss(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    # A champion whose logo_mlogloss is worse than the candidate, but whose logo_logloss
    # (the WRONG key for "ep") is better -- if the gate ever reads the wrong key, the
    # beats_champion check flips outcome.
    _register_and_promote(
        config,
        "ep_model",
        "ep_model_test",
        {"logo_mlogloss": 0.90, "logo_logloss": 0.10},
    )
    candidate_metrics = {
        **_PASSING_EP_METRICS,
        "logo_mlogloss": 0.50,  # beats champion's logo_mlogloss (0.90)
        "logo_logloss": 0.99,  # would LOSE to champion's logo_logloss (0.10) if misread
    }
    run_id = _log_run(config, "ep_model_test", candidate_metrics)

    result = evaluate_gate("ep", run_id, config)

    status, reason = result.checks["beats_champion"]
    assert status is True
    assert "logo_mlogloss" in reason
    assert "logo_logloss" not in reason


def test_evaluate_gate_reads_logo_logloss_for_wp_never_logo_mlogloss(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _register_and_promote(
        config,
        "wp_model",
        "wp_model_test",
        {"logo_logloss": 0.90, "logo_mlogloss": 0.10},
    )
    candidate_metrics = {
        "logo_logloss": 0.50,  # beats champion's logo_logloss (0.90)
        "logo_mlogloss": 0.99,  # would LOSE to champion's logo_mlogloss (0.10) if misread
        "naive_logloss": 0.70,
        "logloss_improvement": 0.10,
        "no_play_share": 0.0,
    }
    run_id = _log_run(config, "wp_model_test", candidate_metrics)

    result = evaluate_gate("wp", run_id, config)

    status, reason = result.checks["beats_champion"]
    assert status is True
    assert "logo_logloss" in reason
    assert "logo_mlogloss" not in reason


def test_evaluate_gate_rejects_unknown_model_prefix(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    run_id = _log_run(config, "ep_model_test", _PASSING_EP_METRICS)

    with pytest.raises(ValueError):
        evaluate_gate("bogus", run_id, config)


# --- run resolution errors -------------------------------------------------------------------


def test_evaluate_gate_raises_gate_error_for_nonexistent_run(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    mlflow_store.configure(config)

    with pytest.raises(GateError):
        evaluate_gate("ep", "0" * 32, config)


def test_evaluate_gate_rejects_non_hex_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    mlflow_store.configure(config)

    with pytest.raises(ValueError):
        evaluate_gate("ep", "../../etc", config)
