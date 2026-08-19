"""Coverage for `flag_football_ep.model.registry`: identifier validation, version
registration, champion-alias promotion and resolution against a temporary local MLflow
store.

Every test builds a config pointing `mlruns`/`models` at `tmp_path` (never the real repo
`mlruns/`), matching the pattern in `tests/test_model_train.py`. Model versions are produced
either by training a real EP model via `train_ep` (when a run backed by a real logged model
is useful) or by starting an `mlflow.start_run()` and calling `register_production_model`
with a trivially-fitted `xgb.XGBRegressor` -- these tests never assert on model quality.
"""

from __future__ import annotations

from pathlib import Path

import mlflow
import numpy as np
import pytest
import xgboost as xgb

from flag_football_ep.config import (
    Config,
    IfafSource,
    Paths,
    ReferenceFiles,
    Sources,
    SportappSource,
    TrainSettings,
)
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.model.registry import CHAMPION_ALIAS, RegistryError
from flag_football_ep.model.train import train_ep
from flag_football_ep.testing import canonical_plays_with_scores

# --- shared test config helper -------------------------------------------------------------


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


def _register_trivial_version(config: Config, name: str, experiment: str = "ep_model_test") -> tuple[str, str]:
    """Start a run against `experiment`, register a trivially-fit model under `name`.

    Returns `(run_id, version)`.
    """
    mlflow_store.ensure_experiment(experiment, config)
    with mlflow.start_run() as run:
        model = xgb.XGBRegressor(n_estimators=2, max_depth=2)
        model.fit(np.array([[0.0], [1.0], [2.0]]), np.array([0.0, 1.0, 2.0]))
        version = registry.register_production_model(model, name, config)
    return run.info.run_id, version


def _training_corpus(n_games: int = 4, plays_per_game: int = 16):
    touchdown = [0] * plays_per_game
    touchdown[5] = 1
    overrides = {"touchdown": touchdown * n_games}
    return canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )


# --- registered_model_name -------------------------------------------------------------


def test_registered_model_name_ep() -> None:
    assert registry.registered_model_name("ep") == "ep_model"


def test_registered_model_name_wp() -> None:
    assert registry.registered_model_name("wp") == "wp_model"


# --- identifier validation ---------------------------------------------------------------


@pytest.mark.parametrize("value", ["ep_model", "wp_model", "champion", "abc-123_XYZ"])
def test_validate_model_name_accepts_plain_identifiers(value: str) -> None:
    registry._validate_model_name(value)  # must not raise


@pytest.mark.parametrize("value", ["champion", "ep_model", "abc-123_XYZ"])
def test_validate_alias_accepts_plain_identifiers(value: str) -> None:
    registry._validate_alias(value)  # must not raise


@pytest.mark.parametrize("value", ["../etc", "a/b", "models:/x", "ep model", ""])
def test_validate_model_name_rejects_non_plain_identifiers(value: str) -> None:
    with pytest.raises(ValueError):
        registry._validate_model_name(value)


@pytest.mark.parametrize("value", ["../etc", "a/b", "models:/x", "ep model", ""])
def test_validate_alias_rejects_non_plain_identifiers(value: str) -> None:
    with pytest.raises(ValueError):
        registry._validate_alias(value)


# --- register_production_model ------------------------------------------------------------


def test_register_production_model_returns_parseable_version(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    _run_id, version = _register_trivial_version(config, "ep_model")

    assert int(version) >= 1


def test_register_production_model_second_call_yields_strictly_greater_version(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)

    _first_run_id, first_version = _register_trivial_version(config, "ep_model")
    _second_run_id, second_version = _register_trivial_version(config, "ep_model")

    assert int(second_version) > int(first_version)


def test_register_production_model_from_real_train_ep_run(tmp_path: Path) -> None:
    """A real train_ep run's model can also be registered -- register_production_model does
    not assume its caller is this test module's trivial-fit path."""
    config = _make_config(tmp_path)
    plays = _training_corpus()
    ep_run_id = train_ep(plays, config)

    mlflow_store.configure(config)
    with mlflow.start_run(run_id=ep_run_id):
        from flag_football_ep.model.score import load_model

        model = load_model(ep_run_id, config)
        version = registry.register_production_model(model, "ep_model", config)

    assert int(version) >= 1


# --- promote ---------------------------------------------------------------------------


def test_promote_sets_champion_alias_to_run_version(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    run_id, version = _register_trivial_version(config, "ep_model")

    returned_version = registry.promote("ep_model", run_id, config)

    assert returned_version == version
    assert registry.resolve_champion("ep_model", config) == run_id


def test_promote_second_call_moves_alias_and_leaves_previous_version_present(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    first_run_id, first_version = _register_trivial_version(config, "ep_model")
    second_run_id, _second_version = _register_trivial_version(config, "ep_model")

    registry.promote("ep_model", first_run_id, config)
    assert registry.resolve_champion("ep_model", config) == first_run_id

    registry.promote("ep_model", second_run_id, config)
    assert registry.resolve_champion("ep_model", config) == second_run_id

    # The previous version is still present in the registry (not deleted by re-promotion).
    from mlflow import MlflowClient

    versions = MlflowClient().search_model_versions("name='ep_model'")
    version_numbers = {mv.version for mv in versions}
    assert first_version in version_numbers


def test_promote_with_run_id_lacking_registered_version_raises_registry_error(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    mlflow_store.ensure_experiment("ep_model_test", config)
    with mlflow.start_run() as run:
        pass  # a run with no model version registered under "ep_model"

    with pytest.raises(RegistryError) as exc_info:
        registry.promote("ep_model", run.info.run_id, config)

    message = str(exc_info.value)
    assert run.info.run_id in message
    assert "ep_model" in message


def test_promote_rejects_non_hex_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _run_id, _version = _register_trivial_version(config, "ep_model")

    with pytest.raises(ValueError):
        registry.promote("ep_model", "../../etc", config)


# --- resolve_champion ------------------------------------------------------------------


def test_resolve_champion_returns_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    run_id, _version = _register_trivial_version(config, "ep_model")
    registry.promote("ep_model", run_id, config)

    assert registry.resolve_champion("ep_model", config) == run_id


def test_resolve_champion_no_alias_raises_registry_error_naming_model_and_ffep_promote(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _register_trivial_version(config, "ep_model")  # registered, but never promoted

    with pytest.raises(RegistryError) as exc_info:
        registry.resolve_champion("ep_model", config)

    message = str(exc_info.value)
    assert "ep_model" in message
    assert "ffep promote" in message


def test_resolve_champion_on_never_registered_name_raises_registry_error_naming_model(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    mlflow_store.configure(config)

    with pytest.raises(RegistryError) as exc_info:
        registry.resolve_champion("never_registered_model", config)

    assert "never_registered_model" in str(exc_info.value)


# --- store isolation: every public function reconfigures the tracking uri from config -------


def test_resolve_champion_ignores_a_differently_set_ambient_tracking_uri(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    run_id, _version = _register_trivial_version(config, "ep_model")
    registry.promote("ep_model", run_id, config)

    mlflow.set_tracking_uri("sqlite:///" + str(tmp_path / "somewhere-else" / "mlflow.db"))

    assert registry.resolve_champion("ep_model", config) == run_id
    assert mlflow.get_tracking_uri() == mlflow_store.tracking_uri(config)


def test_promote_ignores_a_differently_set_ambient_tracking_uri(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    run_id, version = _register_trivial_version(config, "ep_model")

    mlflow.set_tracking_uri("sqlite:///" + str(tmp_path / "somewhere-else" / "mlflow.db"))

    returned_version = registry.promote("ep_model", run_id, config)

    assert returned_version == version
    assert mlflow.get_tracking_uri() == mlflow_store.tracking_uri(config)


# --- import direction: registry -> score only inside promote (not module top level) ---------


def test_registry_module_does_not_import_score_at_top_level() -> None:
    import pathlib

    source = pathlib.Path("src/flag_football_ep/model/registry.py").read_text(encoding="utf-8")
    lines = source.splitlines()
    # Find the line defining `def promote(` and ensure no `from flag_football_ep.model.score`
    # import appears before it (i.e. not at module top level).
    promote_line = next(i for i, line in enumerate(lines) if line.startswith("def promote("))
    top_level_source = "\n".join(lines[:promote_line])
    assert "from flag_football_ep.model.score import" not in top_level_source
    assert "from flag_football_ep.model import score" not in top_level_source
    assert "from flag_football_ep.model.score import" in source
