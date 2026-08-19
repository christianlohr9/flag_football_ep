"""Coverage for `flag_football_ep.model.score`: run resolution, model loading and
scoring against a temporary local MLflow store.

Every test builds a config pointing `mlruns`/`models` at `tmp_path` (never the real repo
`mlruns/`) and a synthetic canonical corpus via
`flag_football_ep.testing.canonical_plays_with_scores`, matching the pattern in
`tests/test_model_train.py`. Models are produced by actually calling `train_ep`/`train_wp`
against the temporary store, then resolved/loaded/scored -- a real train-then-score round
trip, not a mocked one.
"""

from __future__ import annotations

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
from flag_football_ep.model.hyperparams import EP_PROB_LABELS
from flag_football_ep.model.score import RunNotFound, load_model, resolve_run, score_plays
from flag_football_ep.model.train import train_ep, train_wp
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


def _training_corpus(n_games: int = 12, plays_per_game: int = 16) -> pl.DataFrame:
    """A multi-game canonical frame with enough scoring variation for EP/WP training.

    Mirrors `tests/test_model_train.py`'s `_ep_training_corpus`: a mid-half touchdown
    repeated across many games so both EP's sample-weight computation and WP's win label
    have enough variety for a real (if tiny) XGBoost fit with an 80/20 split.
    """
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # mid-half, second drive of the half
    overrides = {"touchdown": touchdown * n_games}
    return canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )


def _trained_config(tmp_path: Path) -> tuple[Config, str, str]:
    """A config with one real EP run and one real WP run already logged."""
    config = _make_config(tmp_path)
    plays = _training_corpus()
    ep_run_id = train_ep(plays, config)
    wp_run_id = train_wp(plays, config)
    return config, ep_run_id, wp_run_id


# --- Task 1: run resolution and model loading ---------------------------------------------


def test_resolve_run_with_explicit_id_returns_it_unchanged(tmp_path: Path) -> None:
    config, ep_run_id, _wp_run_id = _trained_config(tmp_path)

    resolved = resolve_run(config.train.ep_experiment, config, run_id=ep_run_id)

    assert resolved == ep_run_id


def test_resolve_run_explicit_id_raises_run_not_found_when_missing(tmp_path: Path) -> None:
    config, _ep_run_id, _wp_run_id = _trained_config(tmp_path)

    with pytest.raises(RunNotFound):
        resolve_run(config.train.ep_experiment, config, run_id="0" * 32)


def test_resolve_run_without_id_returns_latest_finished_run(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _training_corpus()

    first_run_id = train_ep(plays, config)
    second_run_id = train_ep(plays, config)

    resolved = resolve_run(config.train.ep_experiment, config)

    assert resolved == second_run_id
    assert resolved != first_run_id


def test_resolve_run_on_experiment_with_no_runs_raises_run_not_found_naming_experiment(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    # Force the store to exist without ever training into it.
    mlflow.set_tracking_uri("file:" + str(config.paths.mlruns))
    mlflow.set_experiment("empty_experiment")

    with pytest.raises(RunNotFound) as exc_info:
        resolve_run("empty_experiment", config)

    message = str(exc_info.value)
    assert "empty_experiment" in message
    assert str(config.paths.mlruns) in message


def test_resolve_run_rejects_path_fragment_run_id(tmp_path: Path) -> None:
    config, _ep_run_id, _wp_run_id = _trained_config(tmp_path)

    with pytest.raises(ValueError):
        resolve_run(config.train.ep_experiment, config, run_id="../../etc")


def test_load_model_booster_feature_names_match_training_features(tmp_path: Path) -> None:
    config, ep_run_id, _wp_run_id = _trained_config(tmp_path)

    from flag_football_ep.model.hyperparams import EP_FEATURES

    model = load_model(ep_run_id, config)

    assert model.get_booster().feature_names == list(EP_FEATURES)


def test_load_model_sets_tracking_uri_from_config(tmp_path: Path) -> None:
    config, ep_run_id, _wp_run_id = _trained_config(tmp_path)

    # Point the ambient tracking uri somewhere else first -- load_model must still resolve
    # against config.paths.mlruns, not whatever uri happens to be set beforehand.
    mlflow.set_tracking_uri("file:" + str(tmp_path / "somewhere-else"))

    model = load_model(ep_run_id, config)

    assert model is not None
    assert mlflow.get_tracking_uri() == "file:" + str(config.paths.mlruns)


def test_load_model_rejects_non_hex_run_id(tmp_path: Path) -> None:
    config, _ep_run_id, _wp_run_id = _trained_config(tmp_path)

    with pytest.raises(ValueError):
        load_model("../../etc/passwd", config)


def test_score_module_never_uses_pickle_load() -> None:
    import pathlib

    source = pathlib.Path("src/flag_football_ep/model/score.py").read_text(encoding="utf-8")
    assert "pickle.load" not in source


# --- Task 2: score_plays producing EPA and WPA columns -------------------------------------


_EXTRA_SCORE_COLUMNS = ["ExpPts", "ep", "epa", "wp", "home_wp", "away_wp", "wpa"]


def test_score_plays_returns_probability_and_epa_wpa_columns(tmp_path: Path) -> None:
    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()

    scored = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    for column in [*EP_PROB_LABELS, *_EXTRA_SCORE_COLUMNS]:
        assert column in scored.columns, f"missing expected scored column: {column}"


def test_score_plays_probability_columns_match_ep_prob_labels_order(tmp_path: Path) -> None:
    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()

    scored = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    positions = [scored.columns.index(label) for label in EP_PROB_LABELS]
    assert positions == sorted(positions)


def test_score_plays_ep_probabilities_sum_to_one(tmp_path: Path) -> None:
    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()

    scored = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    complete = scored.drop_nulls(subset=list(EP_PROB_LABELS))
    totals = complete.select(pl.sum_horizontal(list(EP_PROB_LABELS)).alias("total"))["total"]
    assert (totals.to_numpy() - 1.0 < 1e-6).all()
    assert (1.0 - totals.to_numpy() < 1e-6).all()


def test_score_plays_exp_pts_within_bounds(tmp_path: Path) -> None:
    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()

    scored = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    exp_pts = scored.drop_nulls(subset=["ExpPts"])["ExpPts"].to_numpy()
    assert (exp_pts >= -6).all()
    assert (exp_pts <= 6).all()


def test_score_plays_preserves_row_count_with_null_feature_rows(tmp_path: Path) -> None:
    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()
    target_game_id = plays["game_id"].unique().sort()[0]
    plays = plays.with_columns(
        pl.when((pl.col("game_id") == target_game_id) & (pl.col("play_id") == 3))
        .then(pl.lit(None))
        .otherwise(pl.col("yardline_50"))
        .alias("yardline_50")
    )

    scored = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    assert scored.height == plays.height

    null_row = scored.filter(
        (pl.col("game_id") == target_game_id) & (pl.col("play_id") == 3)
    )
    assert null_row.height == 1
    for label in EP_PROB_LABELS:
        assert null_row[label][0] is None


def test_score_plays_raises_on_missing_feature_column(tmp_path: Path) -> None:
    from flag_football_ep.model.score import MissingScoreColumns

    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus().drop("yardline_50")

    with pytest.raises(MissingScoreColumns):
        score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)


def test_score_plays_is_deterministic(tmp_path: Path) -> None:
    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()

    first = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)
    second = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    assert first.equals(second)


def test_score_module_never_imports_pandas() -> None:
    import pathlib

    source = pathlib.Path("src/flag_football_ep/model/score.py").read_text(encoding="utf-8")
    assert "import pandas" not in source


# --- CR-01 regression: _score_probabilities must preserve input row order ------------------
#
# These tests deliberately do NOT assert that a null-adjacent row's epa on the mixed frame
# equals its epa on a fully-complete frame -- that equality does not hold even with the fix.
# On the fully-complete frame the preceding row's home_ep_after is the null row's OWN
# prediction; on the mixed (fixed) frame it is the backward-filled value from the following
# play. Test 2/3 below (EP/WP backfill) are the correct, discriminating form of "the null
# row's neighbours are computed against the right play": they assert the null row itself
# inherits the NEXT play's ep/wp via backward_fill, which is only possible if
# _score_probabilities kept it in chronological position instead of relocating it to the
# frame's tail. Test 4 is the correct form of "unaffected rows are unchanged".


def test_score_probabilities_returns_rows_in_row_id_order(tmp_path: Path) -> None:
    from flag_football_ep.model.score import (
        _ROW_ID_COLUMN,
        _prepare_ep_features,
        _score_probabilities,
    )
    from flag_football_ep.model.hyperparams import EP_FEATURES

    config, ep_run_id, _wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()
    target_game_id = plays["game_id"].unique().sort()[0]
    plays = plays.with_columns(
        pl.when((pl.col("game_id") == target_game_id) & (pl.col("play_id") == 3))
        .then(pl.lit(None))
        .otherwise(pl.col("yardline_50"))
        .alias("yardline_50")
    )
    plays = plays.with_row_index(name=_ROW_ID_COLUMN, offset=0)
    ep_prepared = _prepare_ep_features(plays)
    ep_model = load_model(ep_run_id, config)

    scored = _score_probabilities(ep_prepared, ep_model, EP_FEATURES, EP_PROB_LABELS)

    row_ids = scored[_ROW_ID_COLUMN]
    assert row_ids.to_list() == row_ids.sort().to_list()


def _scored_with_null_yardline_50(tmp_path: Path) -> tuple[pl.DataFrame, pl.DataFrame, object]:
    """Score the untouched corpus (`baseline`) and the same corpus with `yardline_50` nulled
    on play 3 of the first game id (`scored`). Reused by Tests 2-4."""
    config, ep_run_id, wp_run_id = _trained_config(tmp_path)
    plays = _training_corpus()
    target_game_id = plays["game_id"].unique().sort()[0]

    baseline = score_plays(plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    nulled_plays = plays.with_columns(
        pl.when((pl.col("game_id") == target_game_id) & (pl.col("play_id") == 3))
        .then(pl.lit(None))
        .otherwise(pl.col("yardline_50"))
        .alias("yardline_50")
    )
    scored = score_plays(nulled_plays, config, ep_run=ep_run_id, wp_run=wp_run_id)

    return baseline, scored, target_game_id


def test_score_plays_backfills_ep_on_null_feature_row_from_next_play(tmp_path: Path) -> None:
    _baseline, scored, target_game_id = _scored_with_null_yardline_50(tmp_path)

    null_row = scored.filter(
        (pl.col("game_id") == target_game_id) & (pl.col("play_id") == 3)
    )
    next_row = scored.filter(
        (pl.col("game_id") == target_game_id) & (pl.col("play_id") == 4)
    )
    assert null_row.height == 1
    assert next_row.height == 1

    null_ep = null_row["ep"][0]
    next_exp_pts = next_row["ExpPts"][0]
    assert null_ep is not None
    assert null_ep == pytest.approx(next_exp_pts, abs=1e-9)


def test_score_plays_backfills_wp_on_null_feature_row_from_next_play(tmp_path: Path) -> None:
    _baseline, scored, target_game_id = _scored_with_null_yardline_50(tmp_path)

    null_row = scored.filter(
        (pl.col("game_id") == target_game_id) & (pl.col("play_id") == 3)
    )
    next_row = scored.filter(
        (pl.col("game_id") == target_game_id) & (pl.col("play_id") == 4)
    )
    assert null_row.height == 1
    assert next_row.height == 1

    null_wp = null_row["wp"][0]
    next_wp = next_row["wp"][0]
    assert null_wp is not None
    assert null_wp == pytest.approx(next_wp, abs=1e-9)


def test_score_plays_unaffected_games_match_fully_complete_scoring(tmp_path: Path) -> None:
    baseline, scored, target_game_id = _scored_with_null_yardline_50(tmp_path)

    baseline_other = baseline.filter(pl.col("game_id") != target_game_id).sort(
        ["game_id", "play_id"]
    )
    scored_other = scored.filter(pl.col("game_id") != target_game_id).sort(
        ["game_id", "play_id"]
    )

    assert baseline_other["epa"].eq_missing(scored_other["epa"]).all()
    assert baseline_other["wpa"].eq_missing(scored_other["wpa"]).all()
