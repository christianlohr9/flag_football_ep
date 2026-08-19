"""Coverage for `flag_football_ep.model.train`: EP/WP training against a temporary local
MLflow store.

Every test builds a config pointing `mlruns`/`models` at `tmp_path` (never the real repo
`mlruns/`) and a synthetic canonical corpus via
`flag_football_ep.testing.canonical_plays_with_scores`, matching the pattern in
`tests/test_features_mutations.py`. Tests assert on artifacts/params/metrics, never on
model quality -- the corpus is synthetic and small on purpose so the suite stays fast.
"""

from __future__ import annotations

import re
from pathlib import Path

import mlflow
import polars as pl
import pytest

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
from flag_football_ep.model.hyperparams import EP_FEATURES, EP_PARAMS, WP_FEATURES, WP_PARAMS
from flag_football_ep.model.train import MissingTrainingColumns, train_ep, train_wp
from flag_football_ep.testing import canonical_plays_with_scores

# --- shared test config/corpus helpers ---------------------------------------------------


def _make_config(
    tmp_path: Path,
    exclude_games_ep: list[str] | None = None,
    exclude_games_wp: list[str] | None = None,
) -> Config:
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
    )
    reference = ReferenceFiles(
        half_boundaries=tmp_path / "data" / "reference" / "half_boundaries.csv",
        final_scores=tmp_path / "data" / "reference" / "final_scores.csv",
        team_mapping=tmp_path / "data" / "reference" / "team_mapping.csv",
        sportapp_games=tmp_path / "data" / "reference" / "sportapp_games.csv",
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
    """A multi-game canonical frame with enough scoring variation for EP training.

    Mirrors `tests/test_features_mutations.py`'s `_multi_drive_ep_frame` shape (a mid-half
    touchdown) but repeated across many games so `Drive_Score_Dist`/`score_differential`
    vary enough for `make_ep_model_mutations`'s sample-weight computation and a real
    (if tiny) XGBoost fit with an 80/20 split.
    """
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # mid-half, second drive of the half
    overrides = {"touchdown": touchdown * n_games}
    return canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )


def _wp_training_corpus(n_games: int = 12, plays_per_game: int = 16) -> pl.DataFrame:
    """A multi-game canonical frame with a real, varied home/away winner per game.

    Reuses `_ep_training_corpus`'s mid-half-touchdown shape: the touchdown always lands on
    an AWAY-possession play (even `play_id`), so every game has a decisive, non-tied score
    (AWAY wins every game here, but `posteam` alternates play-to-play, so `label`
    (`posteam == Winner`) still varies row-to-row within and across games) -- enough
    variety for a real WP fit without a degenerate single-class target.
    """
    return _ep_training_corpus(n_games=n_games, plays_per_game=plays_per_game)


# --- Task 1: EP training ------------------------------------------------------------------


def test_train_ep_returns_non_empty_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_id = train_ep(plays, config)

    assert isinstance(run_id, str)
    assert run_id


def test_train_ep_run_directory_has_params_metric_and_model_artifact(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_id = train_ep(plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)

    assert len(run.data.params) > 0
    assert len(run.data.metrics) > 0

    retrievable = client.search_runs(experiment_ids=[run.info.experiment_id])
    assert any(r.info.run_id == run_id for r in retrievable), (
        "expected the run to be retrievable through the MlflowClient search API"
    )
    assert (config.paths.mlruns / "mlflow.db").is_file()

    # MLflow >=3.x logs the model as a first-class "Logged Model" (mlflow.xgboost.log_model
    # with `name=`) rather than a plain artifacts/model/ path under the run directory --
    # resolve it through the run's model outputs instead of assuming that layout.
    assert run.outputs.model_outputs, "expected a logged model output on the run"
    model_id = run.outputs.model_outputs[0].model_id
    logged_model = client.get_logged_model(model_id)
    artifact_dir = Path(logged_model.artifact_location.removeprefix("file://"))
    assert artifact_dir.exists() and any(artifact_dir.iterdir())


def test_train_ep_logs_every_hyperparam_plus_provenance_params(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_id = train_ep(plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)
    params = run.data.params

    for key in EP_PARAMS:
        assert key in params, f"missing logged EP hyperparam: {key}"
    for key in ("n_plays", "n_features", "test_size", "random_state", "training_data_sha256"):
        assert key in params, f"missing logged provenance param: {key}"


def test_train_ep_logs_test_mlogloss_metric(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_id = train_ep(plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)

    assert "test_mlogloss" in run.data.metrics


def test_train_ep_drops_excluded_games_and_logs_them(tmp_path: Path) -> None:
    plays = _ep_training_corpus(n_games=12, plays_per_game=16)
    excluded_game_id = plays["game_id"].unique().sort()[0]
    config = _make_config(tmp_path, exclude_games_ep=[excluded_game_id])

    run_id = train_ep(plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)

    assert excluded_game_id in run.data.params["excluded_game_ids"]

    # n_plays reflects the exclusion: fewer rows than an unexcluded run on the same corpus.
    config_all = _make_config(tmp_path)
    run_id_all = train_ep(plays, config_all)
    run_all = client.get_run(run_id_all)
    assert int(run.data.params["n_plays"]) < int(run_all.data.params["n_plays"])


def test_train_ep_drops_null_rows_before_split(tmp_path: Path) -> None:
    plays = _ep_training_corpus(n_games=12, plays_per_game=16)
    target_game_id = plays["game_id"].unique().sort()[0]
    # Null a single row's yardline_50 -- make_ep_model_mutations/drop_nulls() must drop it.
    plays = plays.with_columns(
        pl.when((pl.col("game_id") == target_game_id) & (pl.col("play_id") == 3))
        .then(pl.lit(None))
        .otherwise(pl.col("yardline_50"))
        .alias("yardline_50")
    )
    config = _make_config(tmp_path)
    config_baseline = _make_config(tmp_path / "baseline")
    baseline_plays = _ep_training_corpus(n_games=12, plays_per_game=16)

    run_id = train_ep(plays, config)
    run_id_baseline = train_ep(baseline_plays, config_baseline)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)

    mlflow_store.configure(config_baseline)
    client_baseline = mlflow.tracking.MlflowClient()
    run_baseline = client_baseline.get_run(run_id_baseline)

    assert int(run.data.params["n_plays"]) == int(run_baseline.data.params["n_plays"]) - 1


def test_train_ep_booster_feature_names_match_ep_features(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    run_id = train_ep(plays, config)

    mlflow_store.configure(config)
    loaded = mlflow.xgboost.load_model(f"runs:/{run_id}/model")

    assert loaded.get_booster().feature_names == list(EP_FEATURES)


def test_train_ep_raises_on_missing_feature_column(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus().drop("yardline_50")

    with pytest.raises(MissingTrainingColumns):
        train_ep(plays, config)


def test_train_ep_never_writes_fixed_ep_model_pkl(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    train_ep(plays, config)

    matches = list(tmp_path.rglob("ep_model.pkl"))
    assert matches == []


def test_train_ep_writes_sqlite_backed_store_and_no_filestore_layout(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _ep_training_corpus()

    train_ep(plays, config)

    assert (config.paths.mlruns / "mlflow.db").is_file()

    # FileStore's on-disk layout put a `params/` subdirectory directly under each run
    # directory; the sqlite-backed store keeps no such layout under config.paths.mlruns.
    params_dirs = list(config.paths.mlruns.rglob("params"))
    assert params_dirs == []


# --- Task 2: WP training on the shared run-logging helper --------------------------------


def test_train_wp_returns_non_empty_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _wp_training_corpus()

    run_id = train_wp(plays, config)

    assert isinstance(run_id, str)
    assert run_id


def test_train_wp_logs_into_its_own_experiment(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _wp_training_corpus()

    run_id = train_wp(plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)
    experiment = client.get_experiment(run.info.experiment_id)

    assert experiment.name == config.train.wp_experiment
    assert experiment.name != config.train.ep_experiment


def test_train_wp_logs_test_logloss_metric(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _wp_training_corpus()

    run_id = train_wp(plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)

    assert "test_logloss" in run.data.metrics


def test_train_wp_drops_excluded_games_and_logs_them(tmp_path: Path) -> None:
    plays = _wp_training_corpus(n_games=12, plays_per_game=16)
    excluded_game_id = plays["game_id"].unique().sort()[0]
    config = _make_config(tmp_path, exclude_games_wp=[excluded_game_id])

    run_id = train_wp(plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)

    assert excluded_game_id in run.data.params["excluded_game_ids"]

    config_all = _make_config(tmp_path)
    run_id_all = train_wp(plays, config_all)
    run_all = client.get_run(run_id_all)
    assert int(run.data.params["n_plays"]) < int(run_all.data.params["n_plays"])


def test_train_wp_uses_no_sample_weights(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    plays = _wp_training_corpus()

    captured: dict = {}
    original_fit = __import__("xgboost").XGBRegressor.fit

    def _spy_fit(self, X, y, *args, **kwargs):
        captured["sample_weight"] = kwargs.get("sample_weight")
        return original_fit(self, X, y, *args, **kwargs)

    monkeypatch.setattr("xgboost.XGBRegressor.fit", _spy_fit)

    train_wp(plays, config)

    assert captured["sample_weight"] is None


def test_train_wp_booster_feature_names_match_wp_features(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _wp_training_corpus()

    run_id = train_wp(plays, config)

    mlflow_store.configure(config)
    loaded = mlflow.xgboost.load_model(f"runs:/{run_id}/model")

    assert loaded.get_booster().feature_names == list(WP_FEATURES)


def test_train_ep_and_train_wp_are_independent_runs(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    ep_plays = _ep_training_corpus()
    wp_plays = _wp_training_corpus()

    ep_run_id = train_ep(ep_plays, config)
    wp_run_id = train_wp(wp_plays, config)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()

    ep_experiment = client.get_experiment_by_name(config.train.ep_experiment)
    wp_experiment = client.get_experiment_by_name(config.train.wp_experiment)
    assert ep_experiment.experiment_id != wp_experiment.experiment_id

    ep_runs = client.search_runs([ep_experiment.experiment_id])
    wp_runs = client.search_runs([wp_experiment.experiment_id])
    assert len(ep_runs) == 1
    assert len(wp_runs) == 1
    assert ep_runs[0].info.run_id == ep_run_id
    assert wp_runs[0].info.run_id == wp_run_id


# --- Task 3: optional hyperopt tuning and dated artifact export --------------------------

_TUNE_CORPUS_GAMES = 20
_TUNE_CORPUS_PLAYS_PER_GAME = 16


def _tune_corpus() -> pl.DataFrame:
    """A corpus with both touchdown and safety events injected per game.

    `XGBClassifier.fit(..., eval_set=[...])` (used by the ported tuning objective) requires
    every eval_set split to see the same label set the training split does -- a corpus with
    only touchdown events never produces the Safety/Opp_Safety classes, so an 80/20 split
    can easily strand a class in only one side. Injecting both keeps all five EP classes
    present in both the train and test partitions of a `random_state=42` 80/20 split.
    """
    plays_per_game = _TUNE_CORPUS_PLAYS_PER_GAME
    touchdown = [0] * plays_per_game
    touchdown[5] = 1
    safety = [0] * plays_per_game
    safety[11] = 1
    overrides = {
        "touchdown": touchdown * _TUNE_CORPUS_GAMES,
        "safety": safety * _TUNE_CORPUS_GAMES,
    }
    return canonical_plays_with_scores(
        n_games=_TUNE_CORPUS_GAMES, plays_per_game=plays_per_game, overrides=overrides
    )


def test_train_ep_tune_false_never_calls_fmin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    plays = _tune_corpus()

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("fmin must not be called when tune=False")

    monkeypatch.setattr("flag_football_ep.model.train.fmin", _fail_if_called)

    run_id = train_ep(plays, config, tune=False)

    assert run_id


def test_train_ep_tune_true_max_evals_2_logs_best_params(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _tune_corpus()

    run_id = train_ep(plays, config, tune=True, max_evals=2)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(run_id)
    params = run.data.params

    best_keys = [k for k in params if k.startswith("best_")]
    assert best_keys, "expected best_-prefixed params from the hyperopt search"
    assert any("eta" in k for k in best_keys)


def test_train_ep_tune_true_logs_stepped_trial_losses(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _tune_corpus()

    run_id = train_ep(plays, config, tune=True, max_evals=2)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    history = client.get_metric_history(run_id, "tune_logloss")

    # MLflow's Logged Model API re-appends a duplicate entry for whichever step was most
    # recently logged when mlflow.xgboost.log_model() runs, so history length itself is not
    # a stable count -- the number of *distinct steps* is: one per hyperopt trial.
    assert {m.step for m in history} == {0, 1}
    assert len(history) >= 2


def test_train_ep_records_tuned_and_max_evals_params(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _tune_corpus()

    run_id_fixed = train_ep(plays, config, tune=False)
    run_id_tuned = train_ep(plays, config, tune=True, max_evals=2)

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()

    fixed_params = client.get_run(run_id_fixed).data.params
    tuned_params = client.get_run(run_id_tuned).data.params

    assert fixed_params["tuned"] == "False"
    assert fixed_params["max_evals"] == "100"
    assert tuned_params["tuned"] == "True"
    assert tuned_params["max_evals"] == "2"


def test_train_ep_export_pkl_writes_dated_hash_named_file(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _tune_corpus()

    train_ep(plays, config, export_pkl=True)

    files = list(config.paths.models.glob("*.pkl"))
    assert len(files) == 1
    assert re.match(r"^ep_model_\d{8}_[0-9a-f]{8}\.pkl$", files[0].name)


def test_train_ep_export_pkl_raises_on_second_call_same_day(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _tune_corpus()

    train_ep(plays, config, export_pkl=True)

    with pytest.raises(FileExistsError):
        train_ep(plays, config, export_pkl=True)
