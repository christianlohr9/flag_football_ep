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
from flag_football_ep.features.mutations import (
    add_competition_tier_features,
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
    oof_frame,
    per_source_metrics,
    reliability_curves,
    render_reliability_figure,
    run_logo,
    write_oof_predictions,
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

# --- shared config helper (tmp_path-scoped, never the real repo data/) -----------------------


def _make_config(tmp_path) -> Config:
    """A fully-populated Config pointing every path at `tmp_path` -- mirrors
    `tests/test_model_train.py`'s `_make_config`."""
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


def _logo_result(n_games: int = 4, plays_per_game: int = 6, n_outputs: int = 5) -> LogoResult:
    """A hand-built LogoResult -- no real fit needed for oof_frame/write_oof_predictions
    tests, only the array shapes and alignment `run_logo` guarantees."""
    n_rows = n_games * plays_per_game
    rng = np.random.default_rng(42)
    game_ids = np.repeat([f"game-{i}" for i in range(n_games)], plays_per_game)
    play_ids = np.tile(np.arange(plays_per_game), n_games)
    sources = np.repeat(["hudl", "legacy"], n_rows // 2)
    oof_pred = rng.dirichlet(np.ones(n_outputs), size=n_rows) if n_outputs > 1 else rng.uniform(
        size=(n_rows, 1)
    )
    oof_label = rng.integers(0, max(n_outputs, 2), size=n_rows)
    return LogoResult(
        oof_pred=oof_pred,
        oof_label=oof_label,
        oof_game_id=game_ids,
        oof_play_id=play_ids,
        oof_source=sources,
        n_folds=n_games,
        wall_seconds=0.1,
    )


# --- shared corpus helpers ------------------------------------------------------------------

# `EP_FEATURES`/`WP_FEATURES` adopted the tier one-hot columns (plan 01.3-09), so
# `EP_TRAINING_COLUMNS`/`WP_TRAINING_COLUMNS` now require them present before
# `make_*_model_mutations` runs -- mirrors `model/train.py::_build_competition_tier`'s
# production build step. `canonical_plays_with_scores` always sets `source="hudl"` (default)
# and `competition="TEST"`; a minimal inline mapping (no config/reference file needed, since
# `add_competition_tier_features` takes the mapping frame directly) covers that one pair.
_TIER_MAPPING = pl.DataFrame(
    {"source": ["hudl"], "competition": ["TEST"], "tier": ["womens-international"]}
)


def _ep_model_data(n_games: int = 6, plays_per_game: int = 16) -> pl.DataFrame:
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # mid-half, second drive of the half
    overrides = {"touchdown": touchdown * n_games}
    plays = canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )
    plays, _tier_features = add_competition_tier_features(plays, _TIER_MAPPING)
    prepared = prepare_ep_data(plays)
    return make_ep_model_mutations(prepared, EP_TRAINING_COLUMNS)


def _wp_model_data(n_games: int = 6, plays_per_game: int = 16) -> pl.DataFrame:
    touchdown = [0] * plays_per_game
    touchdown[5] = 1
    overrides = {"touchdown": touchdown * n_games}
    plays = canonical_plays_with_scores(
        n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
    )
    plays, _tier_features = add_competition_tier_features(plays, _TIER_MAPPING)
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


# --- oof_frame -------------------------------------------------------------------------------


def test_oof_frame_has_expected_columns_and_row_count() -> None:
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=5)

    frame = oof_frame(logo, EP_PROB_LABELS)

    assert frame.height == len(logo.oof_label)
    assert set(frame.columns) == {"game_id", "play_id", "source", *EP_PROB_LABELS}


def test_oof_frame_wp_binary_has_single_wp_column() -> None:
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=1)

    frame = oof_frame(logo, ["wp"])

    assert frame.height == len(logo.oof_label)
    assert set(frame.columns) == {"game_id", "play_id", "source", "wp"}


def test_oof_frame_game_id_play_id_pair_is_unique() -> None:
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=5)

    frame = oof_frame(logo, EP_PROB_LABELS)

    assert frame.select("game_id", "play_id").n_unique() == frame.height


def test_oof_frame_raises_on_prob_labels_length_mismatch() -> None:
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=5)

    with pytest.raises(EvaluationError, match="4"):
        oof_frame(logo, EP_PROB_LABELS[:4])


# --- write_oof_predictions --------------------------------------------------------------------


def test_write_oof_predictions_writes_expected_path_and_returns_it(tmp_path) -> None:
    config = _make_config(tmp_path)
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=5)

    out_path = write_oof_predictions(logo, EP_PROB_LABELS, config, "ep")

    assert out_path == config.paths.processed / "oof_predictions_ep.parquet"
    assert out_path.is_file()


def test_write_oof_predictions_rerun_replaces_file_no_tmp_residue(tmp_path) -> None:
    config = _make_config(tmp_path)
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=5)

    write_oof_predictions(logo, EP_PROB_LABELS, config, "ep")
    write_oof_predictions(logo, EP_PROB_LABELS, config, "ep")

    tmp_residue = list(config.paths.processed.glob("*.tmp"))
    assert tmp_residue == []
    matches = list(config.paths.processed.glob("oof_predictions_ep.parquet"))
    assert len(matches) == 1


def test_write_oof_predictions_wp_writes_wp_named_file(tmp_path) -> None:
    config = _make_config(tmp_path)
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=1)

    out_path = write_oof_predictions(logo, ["wp"], config, "wp")

    assert out_path == config.paths.processed / "oof_predictions_wp.parquet"
    assert out_path.is_file()


def test_write_oof_predictions_roundtrips_and_joins_on_game_id_play_id(tmp_path) -> None:
    config = _make_config(tmp_path)
    logo = _logo_result(n_games=4, plays_per_game=6, n_outputs=5)

    out_path = write_oof_predictions(logo, EP_PROB_LABELS, config, "ep")
    read_back = pl.read_parquet(out_path)

    training_frame = pl.DataFrame(
        {
            "game_id": pl.Series("game_id", logo.oof_game_id, dtype=pl.Utf8),
            "play_id": pl.Series("play_id", logo.oof_play_id, dtype=pl.Int32),
        }
    )
    joined = training_frame.join(read_back, on=["game_id", "play_id"], how="inner")
    assert joined.height == training_frame.height
