"""Coverage for `scripts/hc_corpus_ablation.py`: wiring, tags and bookkeeping only -- never
model quality (the synthetic corpus is small and deterministic on purpose).

Every test builds a config pointing `mlruns`/`models` at `tmp_path` (never the real repo
`mlruns/`), mirroring `tests/test_model_train.py::_make_config`'s pattern (copied here, not
imported, per that module's own docstring: tests never import a private helper across test
modules).
"""

from __future__ import annotations

import sys
from pathlib import Path

import mlflow
import polars as pl
import pytest

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
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.testing import canonical_plays_with_scores

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import hc_corpus_ablation as driver  # noqa: E402
from hc_corpus_ablation import (  # noqa: E402
    ARMS,
    HC_SOURCE_PREFIX,
    NoHeadCoachRowsError,
    build_arms,
    main,
    report_no_play_rows,
    run_arm,
)

_HUDL_SOURCE = "hudl"
_HC_SOURCE = "hc_workbook:test-workbook:data"


def _make_config(tmp_path: Path) -> Config:
    """Copied from `tests/test_model_train.py::_make_config` -- every path under `tmp_path`,
    never the real repo. Also writes a `competition_tier.csv` covering both `hudl` and the
    synthetic `hc_workbook:` source this module's corpus uses."""
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
        f"{_HC_SOURCE},TEST,mixed-other\n"
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
        ep_experiment="ep_model_ablation_test",
        wp_experiment="wp_model_ablation_test",
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


def _touchdown_overrides(n_games: int, plays_per_game: int) -> dict:
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # mid-half, second drive
    return {"touchdown": touchdown * n_games}


def _mixed_corpus(n_hudl: int = 6, n_hc: int = 4, plays_per_game: int = 16) -> pl.DataFrame:
    """A two-source canonical corpus: `hudl` rows plus `hc_workbook:` rows, both with enough
    scoring variation for a real (tiny) EP/WP LOGO fit."""
    hudl = canonical_plays_with_scores(
        n_games=n_hudl,
        plays_per_game=plays_per_game,
        source=_HUDL_SOURCE,
        overrides={**_touchdown_overrides(n_hudl, plays_per_game), "competition": "TEST"},
    )
    hc = canonical_plays_with_scores(
        n_games=n_hc,
        plays_per_game=plays_per_game,
        source=_HC_SOURCE,
        overrides={
            **_touchdown_overrides(n_hc, plays_per_game),
            "competition": "TEST",
            "half": 2,
        },
    )
    return pl.concat([hudl, hc], how="vertical")


def _write_plays(config: Config, plays: pl.DataFrame) -> None:
    config.paths.processed.mkdir(parents=True, exist_ok=True)
    plays.write_parquet(config.paths.processed / "plays.parquet")


# --- build_arms ----------------------------------------------------------------------


def test_build_arms_without_hc_is_strictly_smaller() -> None:
    plays = _mixed_corpus()

    arms = build_arms(plays)

    assert set(arms) == {"without_hc", "with_hc"}
    assert arms["without_hc"].height < arms["with_hc"].height
    assert not arms["without_hc"]["source"].str.starts_with(HC_SOURCE_PREFIX).any()


def test_build_arms_no_hc_rows_refuses() -> None:
    plays = canonical_plays_with_scores(n_games=3, plays_per_game=8, source=_HUDL_SOURCE)

    with pytest.raises(NoHeadCoachRowsError):
        build_arms(plays)


# --- run_arm: tags, distinct hashes, registry untouched -------------------------------


def test_run_arm_tags_and_distinct_training_data_hash(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _mixed_corpus()
    arms = build_arms(plays)
    snapshot_dir = tmp_path / "snap"

    without_result = run_arm("ep", "without_hc", arms["without_hc"], config, snapshot_dir)
    with_result = run_arm("ep", "with_hc", arms["with_hc"], config, snapshot_dir)

    assert without_result.run_id != with_result.run_id
    assert without_result.n_plays < with_result.n_plays
    assert without_result.training_data_sha256 != with_result.training_data_sha256

    mlflow_store.configure(config)
    client = mlflow.tracking.MlflowClient()
    for result in (without_result, with_result):
        run = client.get_run(result.run_id)
        assert run.data.tags["corpus_arm"] == result.arm
        assert run.data.tags["gsd_phase"] == "M3-02"
        assert run.data.tags["plan"] == "M3-02-05"
        assert run.data.tags["phase"] == "01.3"  # frozen provenance tag, unchanged

    assert Path(without_result.per_source_metrics_path).is_file()
    assert Path(without_result.oof_snapshot_path).is_file()
    assert Path(with_result.oof_snapshot_path).is_file()
    assert without_result.oof_snapshot_path != with_result.oof_snapshot_path


def test_run_arm_never_sets_champion_alias(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _mixed_corpus()
    arms = build_arms(plays)
    snapshot_dir = tmp_path / "snap"

    for arm_name in ARMS:
        run_arm("wp", arm_name, arms[arm_name], config, snapshot_dir)

    name = registry.registered_model_name("wp")
    with pytest.raises(registry.RegistryError):
        registry.resolve_champion(name, config)


# --- main(): end-to-end wiring ----------------------------------------------------------


def test_main_dry_run_prints_counts_and_fits_nothing(tmp_path: Path, capsys) -> None:
    config = _make_config(tmp_path)
    plays = _mixed_corpus()
    _write_plays(config, plays)
    ffep_toml = tmp_path / "ffep.toml"
    ffep_toml.write_text("placeholder")  # main() never reads this in dry-run branch below

    argv = [
        "--model", "ep",
        "--dry-run",
        "--out-dir", str(tmp_path / "out"),
    ]
    # main() loads config via load_config(args.config); point it at a real, minimal
    # ffep.toml-equivalent by monkeypatching load_config through the module's own import.
    original_load_config = driver.load_config
    driver.load_config = lambda path: config  # noqa: ARG005
    try:
        exit_code = main(argv)
    finally:
        driver.load_config = original_load_config

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "without_hc:" in out
    assert "with_hc:" in out
    assert "[dry-run] nothing fitted" in out
    assert not (tmp_path / "out" / "ablation_summary.csv").exists()


def test_main_both_models_writes_seven_csvs(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _mixed_corpus()
    _write_plays(config, plays)
    out_dir = tmp_path / "out"

    original_load_config = driver.load_config
    driver.load_config = lambda path: config  # noqa: ARG005
    try:
        exit_code = main(["--model", "both", "--out-dir", str(out_dir)])
    finally:
        driver.load_config = original_load_config

    assert exit_code == 0

    ablation_summary = pl.read_csv(out_dir / "ablation_summary.csv")
    assert ablation_summary.height == 4
    assert set(ablation_summary["arm"].unique().to_list()) == {"with_hc", "without_hc"}
    for model in ("ep", "wp"):
        rows = ablation_summary.filter(pl.col("model") == model)
        with_row = rows.filter(pl.col("arm") == "with_hc").row(0, named=True)
        without_row = rows.filter(pl.col("arm") == "without_hc").row(0, named=True)
        assert with_row["n_folds"] > without_row["n_folds"]
        assert with_row["training_data_sha256"] != without_row["training_data_sha256"]

    corpus_arms = pl.read_csv(out_dir / "corpus_arms.csv")
    assert corpus_arms.height > 0
    assert set(corpus_arms.columns) == {"game_id", "with_hc", "without_hc"}
    assert corpus_arms["with_hc"].all()
    n_hc_games = corpus_arms.filter(~pl.col("without_hc")).height
    assert n_hc_games > 0

    for model in ("ep", "wp"):
        source_csv = pl.read_csv(out_dir / f"per_source_metrics_{model}.csv")
        assert "arm" in source_csv.columns
        assert "__pooled__" in source_csv["source"].to_list()
        assert HC_SOURCE_PREFIX + "test-workbook:data" in source_csv["source"].to_list()

        tier_csv = pl.read_csv(out_dir / f"per_tier_metrics_{model}.csv")
        assert {"arm", "competition_tier", "n", "logloss", "naive_logloss", "improvement"} <= set(
            tier_csv.columns
        )
        assert tier_csv.height > 0

    no_play = pl.read_csv(out_dir / "no_play_rows.csv")
    assert {"source", "token", "rows", "share_of_source_rows", "rows_surviving_to_ep_training"} <= set(
        no_play.columns
    )

    # No champion alias moved by the full run either.
    for model in ("ep", "wp"):
        name = registry.registered_model_name(model)
        with pytest.raises(registry.RegistryError):
            registry.resolve_champion(name, config)


# --- report_no_play_rows -----------------------------------------------------------------


def test_report_no_play_rows_counts_timeout_tokens(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _mixed_corpus(n_hudl=4, n_hc=4, plays_per_game=10)
    result_raw = plays["result_raw"].to_list()
    result_raw[0] = "Timeout"
    result_raw[1] = "Rush, Penalty"
    plays = plays.with_columns(pl.Series("result_raw", result_raw))

    table = report_no_play_rows(plays, config)

    assert table.height > 0
    timeout_rows = table.filter(pl.col("token") == "Timeout")
    assert timeout_rows["rows"].sum() >= 1
    any_rows = table.filter(pl.col("token") == "__any__")
    assert any_rows["rows"].sum() >= 2
