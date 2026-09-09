"""Coverage for `scripts.migrate_mlflow_store` (M3-05-09): a real, temp-sqlite-backed source
and target store, no Docker/network required. Proves the migration is faithful (run/param/
metric/artifact fidelity, champion-alias resolution) and read-only against the source, not
just "ran without error".
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from mlflow import MlflowClient
from mlflow.entities import Metric, Param

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import migrate_mlflow_store as mms  # noqa: E402


def _sqlite_uri(path: Path) -> str:
    return "sqlite:///" + str(path)


def _seed_source_run(
    client: MlflowClient,
    experiment_id: str,
    tmp_path: Path,
    *,
    params: dict[str, str],
    metrics: dict[str, float],
    tags: dict[str, str],
    artifact_text: str = "dummy xgboost model bytes",
) -> str:
    """Create one FINISHED run with params/metrics/tags and a `model/` artifact subdir,
    matching the shape `mlflow.xgboost.log_model(model, name="model", ...)` produces.
    """
    run = client.create_run(experiment_id=experiment_id, tags=tags)
    run_id = run.info.run_id
    client.log_batch(
        run_id,
        metrics=[Metric(k, v, 0, 0) for k, v in metrics.items()],
        params=[Param(k, v) for k, v in params.items()],
    )
    artifact_file = tmp_path / "MLmodel"
    artifact_file.write_text(artifact_text)
    client.log_artifact(run_id, str(artifact_file), artifact_path="model")
    client.set_terminated(run_id, status="FINISHED")
    return run_id


@pytest.fixture
def source_store(tmp_path: Path) -> tuple[str, MlflowClient]:
    uri = _sqlite_uri(tmp_path / "source" / "mlflow.db")
    (tmp_path / "source").mkdir(parents=True, exist_ok=True)
    return uri, MlflowClient(tracking_uri=uri)


@pytest.fixture
def target_store(tmp_path: Path) -> tuple[str, MlflowClient]:
    uri = _sqlite_uri(tmp_path / "target" / "mlflow.db")
    (tmp_path / "target").mkdir(parents=True, exist_ok=True)
    return uri, MlflowClient(tracking_uri=uri)


def test_migrate_copies_runs_params_metrics_tags_and_artifacts(
    source_store: tuple[str, MlflowClient],
    target_store: tuple[str, MlflowClient],
    tmp_path: Path,
) -> None:
    source_uri, source_client = source_store
    target_uri, target_client = target_store

    exp_id = source_client.create_experiment("ep_model")
    run_id = _seed_source_run(
        source_client,
        exp_id,
        tmp_path,
        params={"cv_scheme": "leave-one-game-out", "n_folds": "12"},
        metrics={"logo_mlogloss": 0.87, "no_play_share": 0.0},
        tags={"model_prefix": "ep"},
    )
    source_client.create_registered_model("ep_model")
    source_client.create_model_version(
        name="ep_model", source=f"runs:/{run_id}/model", run_id=run_id
    )
    source_client.set_registered_model_alias("ep_model", "champion", "1")

    report = mms.migrate(source_uri, target_uri, experiment_names=("ep_model",))

    assert report.experiment_run_counts["ep_model"] == 1
    assert report.skipped_experiments == []
    assert report.model_version_counts["ep_model"] == 1

    target_exp = target_client.get_experiment_by_name("ep_model")
    assert target_exp is not None
    target_runs = target_client.search_runs([target_exp.experiment_id])
    assert len(target_runs) == 1
    target_run = target_runs[0]

    assert target_run.data.params == {"cv_scheme": "leave-one-game-out", "n_folds": "12"}
    assert target_run.data.metrics == {"logo_mlogloss": 0.87, "no_play_share": 0.0}
    assert target_run.data.tags.get("model_prefix") == "ep"

    artifacts = target_client.list_artifacts(target_run.info.run_id, path="model")
    assert any(f.path.endswith("MLmodel") for f in artifacts)


def test_migrate_resolves_champion_alias_to_correct_target_run(
    source_store: tuple[str, MlflowClient],
    target_store: tuple[str, MlflowClient],
    tmp_path: Path,
) -> None:
    source_uri, source_client = source_store
    target_uri, target_client = target_store

    exp_id = source_client.create_experiment("wp_model")
    v1_dir = tmp_path / "v1"
    v2_dir = tmp_path / "v2"
    v1_dir.mkdir()
    v2_dir.mkdir()
    run_id_v1 = _seed_source_run(
        source_client,
        exp_id,
        v1_dir,
        params={"n_folds": "10"},
        metrics={"logo_logloss": 0.9},
        tags={},
    )
    run_id_v2 = _seed_source_run(
        source_client,
        exp_id,
        v2_dir,
        params={"n_folds": "11"},
        metrics={"logo_logloss": 0.8},
        tags={},
    )
    source_client.create_registered_model("wp_model")
    source_client.create_model_version(
        name="wp_model", source=f"runs:/{run_id_v1}/model", run_id=run_id_v1
    )
    source_client.create_model_version(
        name="wp_model", source=f"runs:/{run_id_v2}/model", run_id=run_id_v2
    )
    # Champion points at v2 (the second, better run) -- not simply "the first version".
    source_client.set_registered_model_alias("wp_model", "champion", "2")

    report = mms.migrate(
        source_uri, target_uri, experiment_names=("wp_model",), model_names=("wp_model",)
    )

    target_champion_mv = target_client.get_model_version_by_alias("wp_model", "champion")
    target_champion_run = target_client.get_run(target_champion_mv.run_id)
    assert target_champion_run.data.params["n_folds"] == "11"
    assert report.champion_run_ids["wp_model"] == target_champion_mv.run_id


def test_migrate_skips_experiment_absent_from_source(
    source_store: tuple[str, MlflowClient],
    target_store: tuple[str, MlflowClient],
) -> None:
    source_uri, _source_client = source_store
    target_uri, _target_client = target_store

    report = mms.migrate(
        source_uri,
        target_uri,
        experiment_names=("ep_model", "wp_model", "ep_model_candidates", "wp_model_candidates"),
        model_names=(),
    )

    assert set(report.skipped_experiments) == {
        "ep_model",
        "wp_model",
        "ep_model_candidates",
        "wp_model_candidates",
    }
    assert report.experiment_run_counts == {}


def test_migrate_skips_model_absent_from_source(
    source_store: tuple[str, MlflowClient],
    target_store: tuple[str, MlflowClient],
) -> None:
    source_uri, _source_client = source_store
    target_uri, _target_client = target_store

    report = mms.migrate(
        source_uri, target_uri, experiment_names=(), model_names=("ep_model", "wp_model")
    )

    assert set(report.skipped_models) == {"ep_model", "wp_model"}
    assert report.champion_run_ids == {}


def test_migrate_is_read_only_against_source(
    source_store: tuple[str, MlflowClient],
    target_store: tuple[str, MlflowClient],
    tmp_path: Path,
) -> None:
    source_uri, source_client = source_store
    target_uri, _target_client = target_store

    exp_id = source_client.create_experiment("ep_model")
    _seed_source_run(
        source_client,
        exp_id,
        tmp_path,
        params={"cv_scheme": "leave-one-game-out"},
        metrics={"logo_mlogloss": 0.87},
        tags={},
    )

    before_runs = source_client.search_runs([exp_id])
    before_count = len(before_runs)
    before_params = before_runs[0].data.params
    before_metrics = before_runs[0].data.metrics

    mms.migrate(source_uri, target_uri, experiment_names=("ep_model",), model_names=())

    after_runs = source_client.search_runs([exp_id])
    assert len(after_runs) == before_count
    assert after_runs[0].data.params == before_params
    assert after_runs[0].data.metrics == before_metrics


def test_migrate_raises_on_missing_artifact_download(
    source_store: tuple[str, MlflowClient],
    target_store: tuple[str, MlflowClient],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_uri, source_client = source_store
    target_uri, _target_client = target_store

    exp_id = source_client.create_experiment("ep_model")
    _seed_source_run(
        source_client,
        exp_id,
        tmp_path,
        params={},
        metrics={},
        tags={},
    )

    def _empty_download(*, run_id, dst_path, tracking_uri):  # noqa: ANN001
        Path(dst_path).mkdir(parents=True, exist_ok=True)
        return dst_path

    monkeypatch.setattr(mms.mlflow.artifacts, "download_artifacts", _empty_download)

    with pytest.raises(mms.MigrationError, match="missing artifact tree"):
        mms.migrate(source_uri, target_uri, experiment_names=("ep_model",), model_names=())


def test_migration_report_json_round_trips(
    source_store: tuple[str, MlflowClient],
    target_store: tuple[str, MlflowClient],
    tmp_path: Path,
) -> None:
    import json

    source_uri, source_client = source_store
    target_uri, _target_client = target_store
    exp_id = source_client.create_experiment("ep_model")
    _seed_source_run(source_client, exp_id, tmp_path, params={}, metrics={}, tags={})

    report = mms.migrate(source_uri, target_uri, experiment_names=("ep_model",), model_names=())
    # Must be plain-JSON-serializable (the CLI writes report.to_dict() straight to a file).
    json.dumps(report.to_dict())
    assert report.to_dict()["experiment_run_counts"]["ep_model"] == 1
