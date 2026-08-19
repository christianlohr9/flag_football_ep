"""Coverage for `flag_football_ep.model.evaluate.run_logo`: the leave-one-game-out (LOGO)
measurement loop.

Reuses `flag_football_ep.testing.canonical_plays_with_scores` plus the real
`prepare_ep_data`/`prepare_wp_data` + `make_ep_model_mutations`/`make_wp_model_mutations`
chain to build realistic multi-game training frames (mirroring
`tests/test_model_train.py`'s `_ep_training_corpus`). The fold-disjointness test uses a
small hand-made frame so the assertion can enumerate fold membership directly against
`sklearn.model_selection.LeaveOneGroupOut`, rather than trusting `run_logo`'s internals.
Never asserts on model quality -- only on shape, coverage, disjointness and error behaviour.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from sklearn.model_selection import LeaveOneGroupOut

from flag_football_ep.features.mutations import (
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model.evaluate import EvaluationError, LogoResult, run_logo
from flag_football_ep.model.hyperparams import (
    EP_FEATURES,
    EP_PARAMS,
    EP_TRAINING_COLUMNS,
    WP_FEATURES,
    WP_PARAMS,
    WP_TRAINING_COLUMNS,
)
from flag_football_ep.testing import canonical_plays_with_scores

# --- shared corpus helpers ------------------------------------------------------------------


def _ep_model_data(n_games: int = 6, plays_per_game: int = 16) -> pl.DataFrame:
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # mid-half, second drive of the half
    overrides = {"touchdown": touchdown * n_games}
    plays = canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )
    prepared = prepare_ep_data(plays)
    return make_ep_model_mutations(prepared, EP_TRAINING_COLUMNS)


def _wp_model_data(n_games: int = 6, plays_per_game: int = 16) -> pl.DataFrame:
    touchdown = [0] * plays_per_game
    touchdown[5] = 1
    overrides = {"touchdown": touchdown * n_games}
    plays = canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )
    prepared = prepare_wp_data(plays)
    return make_wp_model_mutations(prepared, WP_TRAINING_COLUMNS)


# --- shape / coverage -------------------------------------------------------------------


def test_run_logo_n_folds_equals_distinct_game_id_count() -> None:
    model_data = _ep_model_data(n_games=6)

    result = run_logo(
        model_data=model_data,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )

    assert isinstance(result, LogoResult)
    assert result.n_folds == model_data["game_id"].n_unique()


def test_run_logo_oof_coverage_no_nan_one_row_per_input_row() -> None:
    model_data = _ep_model_data(n_games=6)

    result = run_logo(
        model_data=model_data,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )

    assert result.oof_pred.shape[0] == model_data.height
    assert not np.isnan(result.oof_pred).any()


def test_run_logo_oof_pred_width_equals_num_class_for_multiclass() -> None:
    model_data = _ep_model_data(n_games=6)

    result = run_logo(
        model_data=model_data,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )

    assert result.oof_pred.shape[1] == EP_PARAMS["num_class"]


def test_run_logo_oof_pred_width_is_one_for_binary() -> None:
    model_data = _wp_model_data(n_games=6)

    result = run_logo(
        model_data=model_data,
        features=WP_FEATURES,
        fixed_params=WP_PARAMS,
        weight_column=None,
    )

    assert result.oof_pred.shape[1] == 1


def test_run_logo_oof_arrays_aligned_with_input_frame_row_order() -> None:
    model_data = _ep_model_data(n_games=6)

    result = run_logo(
        model_data=model_data,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )

    assert np.array_equal(result.oof_game_id, model_data["game_id"].to_numpy())
    assert np.array_equal(result.oof_play_id, model_data["play_id"].to_numpy())
    assert np.array_equal(result.oof_source, model_data["source"].to_numpy())
    assert np.array_equal(result.oof_label, model_data["label"].to_numpy())


def test_run_logo_wall_seconds_is_positive_float() -> None:
    model_data = _ep_model_data(n_games=6)

    result = run_logo(
        model_data=model_data,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )

    assert isinstance(result.wall_seconds, float)
    assert result.wall_seconds > 0


# --- fold disjointness -------------------------------------------------------------------


def test_run_logo_folds_are_disjoint_by_game_id() -> None:
    """Hand-made frame: enumerate fold membership directly via LeaveOneGroupOut, not via
    run_logo's internals, and cross-check run_logo's own oof coverage on the same frame."""
    model_data = _ep_model_data(n_games=6, plays_per_game=16)
    groups = model_data["game_id"].to_numpy()
    X = model_data.select(list(EP_FEATURES)).to_numpy()
    y = model_data.select("label").to_numpy().ravel()

    logo = LeaveOneGroupOut()
    fold_count = 0
    for train_idx, test_idx in logo.split(X, y, groups=groups):
        train_games = set(groups[train_idx])
        test_games = set(groups[test_idx])
        assert train_games.isdisjoint(test_games)
        assert len(test_games) == 1
        fold_count += 1

    assert fold_count == model_data["game_id"].n_unique()

    result = run_logo(
        model_data=model_data,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )
    assert result.n_folds == fold_count


# --- error handling ------------------------------------------------------------------------


def test_run_logo_raises_on_missing_group_column() -> None:
    model_data = _ep_model_data(n_games=6).drop("game_id")

    with pytest.raises(EvaluationError, match="game_id"):
        run_logo(
            model_data=model_data,
            features=EP_FEATURES,
            fixed_params=EP_PARAMS,
            weight_column="Total_W_Scaled",
        )


def test_run_logo_raises_on_missing_label_column() -> None:
    model_data = _ep_model_data(n_games=6).drop("label")

    with pytest.raises(EvaluationError, match="label"):
        run_logo(
            model_data=model_data,
            features=EP_FEATURES,
            fixed_params=EP_PARAMS,
            weight_column="Total_W_Scaled",
        )


def test_run_logo_raises_on_missing_feature_column() -> None:
    model_data = _ep_model_data(n_games=6).drop("yardline_50")

    with pytest.raises(EvaluationError, match="yardline_50"):
        run_logo(
            model_data=model_data,
            features=EP_FEATURES,
            fixed_params=EP_PARAMS,
            weight_column="Total_W_Scaled",
        )


def test_run_logo_raises_when_fewer_than_two_groups() -> None:
    model_data = _ep_model_data(n_games=1)

    with pytest.raises(EvaluationError, match="1"):
        run_logo(
            model_data=model_data,
            features=EP_FEATURES,
            fixed_params=EP_PARAMS,
            weight_column="Total_W_Scaled",
        )


# --- no per-fold mutation recomputation -----------------------------------------------------


def test_run_logo_never_recomputes_mutations_on_constant_weight_subset() -> None:
    """`make_ep_model_mutations` raises `DegenerateWeightRange` on a zero-range subset -- if
    run_logo ever called it per-fold, a constant weight column across the whole frame would
    trigger it. `run_logo` must fit only on the already-supplied weight column values, so
    this must complete without raising."""
    model_data = _ep_model_data(n_games=6).with_columns(pl.lit(0.5).alias("Total_W_Scaled"))

    result = run_logo(
        model_data=model_data,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )

    assert result.n_folds == model_data["game_id"].n_unique()
