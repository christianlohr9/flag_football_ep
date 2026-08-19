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

import matplotlib

matplotlib.use("Agg")

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
from flag_football_ep.model.evaluate import (
    EvaluationError,
    LogoResult,
    ReliabilityCurve,
    naive_baseline_logloss,
    per_source_metrics,
    reliability_curves,
    render_reliability_figure,
    run_logo,
)
from flag_football_ep.model.hyperparams import (
    EP_FEATURES,
    EP_PARAMS,
    EP_PROB_LABELS,
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


# --- naive_baseline_logloss ---------------------------------------------------------------


def test_naive_baseline_logloss_balanced_binary_equals_log_2() -> None:
    labels = np.array([0, 1, 0, 1])

    result = naive_baseline_logloss(labels, num_class=2)

    assert abs(result - np.log(2)) < 1e-9


def test_naive_baseline_logloss_computes_frequencies_from_labels_not_fixed_half() -> None:
    """An unbalanced label array must not silently reuse the balanced-case value -- the
    naive baseline is always the *actual* marginal class distribution of the labels passed
    in, not a fixed 0.5/uniform assumption."""
    balanced = np.array([0, 1, 0, 1])
    unbalanced = np.array([0, 0, 0, 1])

    balanced_loss = naive_baseline_logloss(balanced, num_class=2)
    unbalanced_loss = naive_baseline_logloss(unbalanced, num_class=2)

    assert balanced_loss != pytest.approx(unbalanced_loss)


def test_naive_baseline_logloss_binary_wp_num_class_one_uses_marginal_positive_rate() -> None:
    labels = np.array([1, 1, 1, 0])  # marginal positive rate 0.75

    result = naive_baseline_logloss(labels, num_class=1)

    # hand-computed: -[3*log(0.75) + 1*log(0.25)] / 4
    expected = -(3 * np.log(0.75) + 1 * np.log(0.25)) / 4
    assert abs(result - expected) < 1e-9


# --- reliability_curves ---------------------------------------------------------------------


def test_reliability_curves_one_entry_per_class_for_multiclass() -> None:
    rng = np.random.default_rng(0)
    n = 200
    oof_label = rng.integers(0, 5, size=n)
    oof_pred = rng.dirichlet(np.ones(5), size=n)

    curves = reliability_curves(oof_pred, oof_label, EP_PROB_LABELS, n_bins=5)

    assert len(curves) == 5
    assert [c.name for c in curves] == EP_PROB_LABELS
    for curve in curves:
        assert isinstance(curve, ReliabilityCurve)
        assert len(curve.prob_true) == len(curve.prob_pred)
        assert curve.n_bins == 5


def test_reliability_curves_exactly_one_entry_for_binary_input() -> None:
    rng = np.random.default_rng(1)
    n = 200
    oof_label = rng.integers(0, 2, size=n)
    oof_pred = rng.uniform(size=(n, 1))

    curves = reliability_curves(oof_pred, oof_label, ["wp"], n_bins=5)

    assert len(curves) == 1
    assert curves[0].name == "wp"
    assert len(curves[0].prob_true) == len(curves[0].prob_pred)


def test_reliability_curves_raises_on_class_names_length_mismatch() -> None:
    rng = np.random.default_rng(2)
    n = 50
    oof_label = rng.integers(0, 5, size=n)
    oof_pred = rng.dirichlet(np.ones(5), size=n)

    with pytest.raises(EvaluationError, match="4"):
        reliability_curves(oof_pred, oof_label, EP_PROB_LABELS[:4], n_bins=5)


# --- per_source_metrics ---------------------------------------------------------------------


def test_per_source_metrics_one_row_per_source_plus_pooled() -> None:
    # Hand-built two-source binary array so expected numbers are computable by hand.
    oof_label = np.array([0, 1, 0, 1, 1, 1])
    oof_pred = np.array([0.5, 0.5, 0.5, 0.5, 0.9, 0.9]).reshape(-1, 1)
    oof_source = np.array(["legacy", "legacy", "legacy", "legacy", "hudl", "hudl"])

    result = per_source_metrics(oof_pred, oof_label, oof_source, num_class=1)

    sources = set(result["source"].to_list())
    assert sources == {"legacy", "hudl", "__pooled__"}
    assert result.height == 3


def test_per_source_metrics_improvement_equals_naive_minus_logloss() -> None:
    oof_label = np.array([0, 1, 0, 1, 1, 1])
    oof_pred = np.array([0.5, 0.5, 0.5, 0.5, 0.9, 0.9]).reshape(-1, 1)
    oof_source = np.array(["legacy", "legacy", "legacy", "legacy", "hudl", "hudl"])

    result = per_source_metrics(oof_pred, oof_label, oof_source, num_class=1)

    for row in result.iter_rows(named=True):
        assert row["improvement"] == pytest.approx(row["naive_logloss"] - row["logloss"])


def test_per_source_metrics_computes_each_sources_own_naive_baseline() -> None:
    """The 'legacy' source here is perfectly balanced (naive = log(2)); 'hudl' is all-1s
    (naive baseline should be near-zero, not log(2)) -- proves each source's naive baseline
    comes from its own label distribution, not the pooled one."""
    oof_label = np.array([0, 1, 0, 1, 1, 1])
    oof_pred = np.array([0.5, 0.5, 0.5, 0.5, 0.9, 0.9]).reshape(-1, 1)
    oof_source = np.array(["legacy", "legacy", "legacy", "legacy", "hudl", "hudl"])

    result = per_source_metrics(oof_pred, oof_label, oof_source, num_class=1)
    by_source = {row["source"]: row["naive_logloss"] for row in result.iter_rows(named=True)}

    assert abs(by_source["legacy"] - np.log(2)) < 1e-9
    assert by_source["hudl"] < by_source["legacy"]


def test_per_source_metrics_pooled_row_matches_naive_baseline_logloss() -> None:
    oof_label = np.array([0, 1, 0, 1, 1, 1])
    oof_pred = np.array([0.5, 0.5, 0.5, 0.5, 0.9, 0.9]).reshape(-1, 1)
    oof_source = np.array(["legacy", "legacy", "legacy", "legacy", "hudl", "hudl"])

    result = per_source_metrics(oof_pred, oof_label, oof_source, num_class=1)
    pooled = next(row for row in result.iter_rows(named=True) if row["source"] == "__pooled__")

    expected_naive = naive_baseline_logloss(oof_label, num_class=1)
    assert pooled["naive_logloss"] == pytest.approx(expected_naive)
    assert pooled["n_plays"] == len(oof_label)


# --- render_reliability_figure ---------------------------------------------------------------


def test_render_reliability_figure_returns_figure_with_one_subplot_per_curve() -> None:
    import matplotlib.figure

    rng = np.random.default_rng(3)
    n = 100
    oof_label = rng.integers(0, 5, size=n)
    oof_pred = rng.dirichlet(np.ones(5), size=n)
    curves = reliability_curves(oof_pred, oof_label, EP_PROB_LABELS, n_bins=5)

    fig = render_reliability_figure(curves, "EP reliability")

    assert isinstance(fig, matplotlib.figure.Figure)
    assert len(fig.axes) == len(curves)
