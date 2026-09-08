"""Coverage for `flag_football_ep.model.freeze`: corpus freeze manifests
(`build_freeze_manifest`/`write_freeze_manifest`/`load_freeze_manifest`/
`latest_freeze_manifest`) and the `ffep freeze-corpus` CLI command.

Every test builds a config pointing every path at `tmp_path` (never the real repo),
mirroring `tests/test_model_train.py::_make_config`'s pattern (copied here, not imported,
per that module's own docstring: tests never import a private helper across test modules).
`compute_corpus_fingerprint`/`git_commit_sha` are cross-checked against
`scripts/hc_corpus_ablation.py`'s copies of the same functions for byte-for-byte parity.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

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
from flag_football_ep.model import freeze
from flag_football_ep.testing import canonical_plays_with_scores

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import hc_corpus_ablation as driver  # noqa: E402

runner = CliRunner()


def _make_config(tmp_path: Path) -> Config:
    """Copied from `tests/test_model_train.py::_make_config` -- every path under
    `tmp_path`, never the real repo."""
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
        raw_hc_files=tmp_path / "data" / "raw" / "hc_files",
        corpus_freeze=tmp_path / "data" / "reference" / "corpus_freeze",
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


def _plays(n_games: int = 3, plays_per_game: int = 8, source: str = "hudl") -> pl.DataFrame:
    return canonical_plays_with_scores(n_games=n_games, plays_per_game=plays_per_game, source=source)


def _write_games_parquet(config: Config, statuses: list[str]) -> None:
    """A minimal `games.parquet` fixture -- one row per status in `statuses`, matching
    `pipeline._build_games_table`'s real column set closely enough for
    `build_freeze_manifest` to read (it only reads `status`)."""
    config.paths.processed.mkdir(parents=True, exist_ok=True)
    n = len(statuses)
    pl.DataFrame(
        {
            "game_id": [f"game-{i}" for i in range(n)],
            "source": ["hudl"] * n,
            "competition": ["TEST"] * n,
            "season": [2026] * n,
            "home_team": ["HOME"] * n,
            "away_team": ["AWAY"] * n,
            "n_plays": [8] * n,
            "n_drives": [2] * n,
            "status": statuses,
            "quarantine_reasons": [None] * n,
            "ingested_at": ["run-1"] * n,
        }
    ).write_parquet(config.paths.processed / "games.parquet")


# --- build_freeze_manifest ------------------------------------------------------------


def test_build_freeze_manifest_has_expected_top_level_keys(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    _write_games_parquet(config, ["accepted"])

    manifest = freeze.build_freeze_manifest(plays, config)

    for key in (
        "date",
        "corpus_fingerprint",
        "git_commit",
        "per_source_row_counts",
        "games",
        "per_source_snapshot_date",
    ):
        assert key in manifest, f"missing manifest key: {key}"


def test_build_freeze_manifest_per_source_row_counts_match_plays(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    hudl_plays = _plays(n_games=2, plays_per_game=8, source="hudl")
    legacy_plays = _plays(n_games=1, plays_per_game=8, source="legacy")
    plays = pl.concat([hudl_plays, legacy_plays], how="vertical")
    _write_games_parquet(config, ["accepted"])

    manifest = freeze.build_freeze_manifest(plays, config)

    assert manifest["per_source_row_counts"]["hudl"] == hudl_plays.height
    assert manifest["per_source_row_counts"]["legacy"] == legacy_plays.height


def test_build_freeze_manifest_games_summary_groups_accepted_with_warnings_as_accepted(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    _write_games_parquet(
        config, ["accepted", "accepted-with-warnings", "quarantined", "quarantined"]
    )

    manifest = freeze.build_freeze_manifest(plays, config)

    assert manifest["games"]["accepted"] == 2
    assert manifest["games"]["quarantined"] == 2
    assert manifest["games"]["total"] == 4


def test_build_freeze_manifest_games_summary_none_when_games_parquet_absent(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    # No games.parquet written -- must not raise.

    manifest = freeze.build_freeze_manifest(plays, config)

    assert manifest["games"]["accepted"] is None
    assert manifest["games"]["quarantined"] is None


def test_build_freeze_manifest_snapshot_dates_labelled_approximate(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _plays(source="hudl")
    _write_games_parquet(config, ["accepted"])
    config.paths.raw_hudl.mkdir(parents=True, exist_ok=True)
    (config.paths.raw_hudl / "one.csv").write_text("x")

    manifest = freeze.build_freeze_manifest(plays, config)

    snapshot = manifest["per_source_snapshot_date"]["hudl"]
    assert snapshot["approximate"] is True
    assert snapshot["date"] is not None


def test_build_freeze_manifest_snapshot_date_none_when_raw_dir_empty(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _plays(source="hudl")
    _write_games_parquet(config, ["accepted"])
    # raw_hudl directory never created/populated.

    manifest = freeze.build_freeze_manifest(plays, config)

    snapshot = manifest["per_source_snapshot_date"]["hudl"]
    assert snapshot["approximate"] is True
    assert snapshot["date"] is None


def test_build_freeze_manifest_fingerprint_deterministic_for_identical_plays(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    _write_games_parquet(config, ["accepted"])

    first = freeze.build_freeze_manifest(plays, config)
    second = freeze.build_freeze_manifest(plays, config)

    assert first["corpus_fingerprint"] == second["corpus_fingerprint"]
    assert first["git_commit"] == second["git_commit"]


def test_compute_corpus_fingerprint_matches_hc_corpus_ablation_byte_for_byte() -> None:
    plays = _plays()

    assert freeze.compute_corpus_fingerprint(plays) == driver.compute_corpus_fingerprint(plays)


def test_git_commit_sha_matches_hc_corpus_ablation() -> None:
    assert freeze.git_commit_sha() == driver.git_commit_sha()


# --- write_freeze_manifest / load_freeze_manifest -------------------------------------


def test_write_freeze_manifest_writes_dated_fingerprinted_json(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    _write_games_parquet(config, ["accepted"])
    manifest = freeze.build_freeze_manifest(plays, config)

    out_path = freeze.write_freeze_manifest(manifest, config)

    assert out_path.exists()
    assert out_path.parent == config.paths.corpus_freeze
    expected_name = f"{manifest['date']}_{manifest['corpus_fingerprint'][:8]}.json"
    assert out_path.name == expected_name


def test_write_freeze_manifest_creates_corpus_freeze_directory(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    _write_games_parquet(config, ["accepted"])
    manifest = freeze.build_freeze_manifest(plays, config)
    assert not config.paths.corpus_freeze.exists()

    freeze.write_freeze_manifest(manifest, config)

    assert config.paths.corpus_freeze.is_dir()


def test_write_freeze_manifest_idempotent_for_repeat_write(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    _write_games_parquet(config, ["accepted"])
    manifest = freeze.build_freeze_manifest(plays, config)

    first_path = freeze.write_freeze_manifest(manifest, config)
    second_path = freeze.write_freeze_manifest(manifest, config)

    assert first_path == second_path
    assert json.loads(first_path.read_text()) == manifest


def test_load_freeze_manifest_roundtrips_build_freeze_manifest(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    plays = _plays()
    _write_games_parquet(config, ["accepted"])
    manifest = freeze.build_freeze_manifest(plays, config)
    out_path = freeze.write_freeze_manifest(manifest, config)

    loaded = freeze.load_freeze_manifest(out_path)

    assert loaded == manifest


# --- latest_freeze_manifest -------------------------------------------------------------


def test_latest_freeze_manifest_returns_none_when_directory_absent(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    assert freeze.latest_freeze_manifest(config) is None


def test_latest_freeze_manifest_returns_none_when_directory_empty(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.paths.corpus_freeze.mkdir(parents=True, exist_ok=True)

    assert freeze.latest_freeze_manifest(config) is None


def test_latest_freeze_manifest_returns_most_recently_dated(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    config.paths.corpus_freeze.mkdir(parents=True, exist_ok=True)
    older = {"date": "2026-01-01", "corpus_fingerprint": "a" * 64, "git_commit": "unknown"}
    newer = {"date": "2026-06-01", "corpus_fingerprint": "b" * 64, "git_commit": "unknown"}
    (config.paths.corpus_freeze / "2026-01-01_aaaaaaaa.json").write_text(json.dumps(older))
    (config.paths.corpus_freeze / "2026-06-01_bbbbbbbb.json").write_text(json.dumps(newer))

    latest_path = freeze.latest_freeze_manifest(config)

    assert latest_path is not None
    assert freeze.load_freeze_manifest(latest_path)["date"] == "2026-06-01"


# --- `ffep freeze-corpus` CLI ------------------------------------------------------------


def _write_toml_config(root: Path, repo_root: Path) -> Path:
    """A complete `ffep.toml` at `root`, mirroring
    `tests/test_pipeline_ingest.py::_write_toml_config`'s shape (every required table)."""
    data_root = root / "data"
    toml_text = f"""
[paths]
data_root = "{data_root}"
raw_hudl = "{data_root / "raw" / "hudl"}"
raw_sportapp = "{data_root / "raw" / "sportapp"}"
raw_ifaf = "{data_root / "raw" / "ifaf"}"
raw_legacy = "{data_root / "raw" / "legacy"}"
processed = "{data_root / "processed"}"
reference = "{data_root / "reference"}"
models = "{root / "models"}"
mlruns = "{root / "mlruns"}"
contract = "{repo_root / "docs" / "data-contract.schema.json"}"
reports = "{root / "reports"}"
video = "{data_root / "video"}"
labels = "{data_root / "labels"}"
tracking = "{data_root / "processed" / "tracking"}"
raw_hc_files = "{data_root / "raw" / "hc_files"}"
corpus_freeze = "{data_root / "reference" / "corpus_freeze"}"

[reference]
half_boundaries = "{data_root / "reference" / "half_boundaries.csv"}"
final_scores = "{data_root / "reference" / "final_scores.csv"}"
team_mapping = "{data_root / "reference" / "team_mapping.csv"}"
sportapp_games = "{data_root / "reference" / "sportapp_games.csv"}"
competition_tier = "{data_root / "reference" / "competition_tier.csv"}"
player_mapping = "{data_root / "reference" / "player_mapping.csv"}"
group_opponents = "{data_root / "reference" / "group_opponents.csv"}"
hover_positions = "{data_root / "reference" / "hover_positions.csv"}"
homography_calibration = "{data_root / "reference" / "homography_calibration.csv"}"
gt_positions = "{data_root / "reference" / "gt_positions.csv"}"
continuity_review = "{data_root / "reference" / "continuity_review.csv"}"

[sources.sportapp]
base_url = "https://example.invalid"
api_key_env = "SPORTAPP_API_KEY"

[sources.ifaf]
base_url = "https://example.invalid"
tournament = "test"
api_key_env = "CPX_API_KEY"

[train]
ep_experiment = "ep_model"
wp_experiment = "wp_model"
exclude_games_ep = []
exclude_games_wp = []

[report]
own_team = "HOME"
cycle_start_season = 2026

[cv]
pilot_session_id = "test-session"
detector_model = "cv_detector_model_test"
detector_experiment = "cv_detector_test"
resolution = 672
sahi = false
sahi_slice = 640
sahi_overlap = 0.2
train_epochs = 1
train_batch_size = 4
train_grad_accum = 4
device = "cpu"
label_frame_target = 10
cvat_host = "http://localhost:8080"
cvat_username_env = "CVAT_USERNAME"
cvat_password_env = "CVAT_PASSWORD"
field_length_yards = 50.0
field_width_yards = 25.0
endzone_yards = 10.0
dvc_remote_name = "otc-obs"
dvc_remote_url = "s3://test-bucket/flag-football-datasets"
dvc_remote_endpoint = "https://obs.eu-de.otc.t-systems.com"
otc_obs_access_key_env = "OTC_OBS_ACCESS_KEY_ID"
otc_obs_secret_key_env = "OTC_OBS_SECRET_ACCESS_KEY"
"""
    config_path = root / "ffep.toml"
    config_path.write_text(toml_text, encoding="utf-8")
    return config_path


def test_freeze_corpus_cli_help() -> None:
    result = runner.invoke(app, ["freeze-corpus", "--help"])
    assert result.exit_code == 0


def test_freeze_corpus_cli_writes_real_manifest(tmp_path: Path) -> None:
    repo_root = REPO_ROOT
    config_path = _write_toml_config(tmp_path, repo_root)
    data_root = tmp_path / "data"
    (data_root / "processed").mkdir(parents=True, exist_ok=True)
    plays = _plays()
    plays.write_parquet(data_root / "processed" / "plays.parquet")

    result = runner.invoke(app, ["freeze-corpus", "--config", str(config_path)])

    assert result.exit_code == 0, result.output
    written = list((data_root / "reference" / "corpus_freeze").glob("*.json"))
    assert len(written) == 1
    manifest = json.loads(written[0].read_text())
    assert manifest["corpus_fingerprint"]
    assert "freeze:" in result.output
