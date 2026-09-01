"""Tests for flag_football_ep.config.

Uses tmp_path for TOML/.env fixtures per plan 03's read_first note, and
monkeypatch.setenv/delenv for the secret() cases. No test asserts a real
secret value anywhere in a message — only variable *names* are asserted.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from flag_football_ep.config import ConfigError, Config, load_config, load_dotenv, secret

MINIMAL_TOML = """
[paths]
data_root = "data"
raw_hudl = "data/raw/hudl"
raw_sportapp = "data/raw/sportapp"
raw_ifaf = "data/raw/ifaf"
raw_legacy = "data/raw/legacy"
processed = "data/processed"
reference = "data/reference"
models = "models"
mlruns = "mlruns"
contract = "docs/data-contract.schema.json"
reports = "reports"
video = "data/video"
labels = "data/labels"
tracking = "data/processed/tracking"

[reference]
half_boundaries = "data/reference/half_boundaries.csv"
final_scores = "data/reference/final_scores.csv"
team_mapping = "data/reference/team_mapping.csv"
sportapp_games = "data/reference/sportapp_games.csv"
competition_tier = "data/reference/competition_tier.csv"
player_mapping = "data/reference/player_mapping.csv"
group_opponents = "data/reference/group_opponents.csv"
hover_positions = "data/reference/hover_positions.csv"
homography_calibration = "data/reference/homography_calibration.csv"
gt_positions = "data/reference/gt_positions.csv"
continuity_review = "data/reference/continuity_review.csv"

[sources.sportapp]
base_url = "https://example.invalid/api/v1/public"
api_key_env = "SPORTAPP_API_KEY"

[sources.ifaf]
base_url = "https://example.invalid/v1"
tournament = "test-tournament"
api_key_env = "CPX_API_KEY"

[train]
ep_experiment = "ep_model"
wp_experiment = "wp_model"
exclude_games_ep = ["legacy-37"]
exclude_games_wp = ["legacy-35"]

[report]
own_team = "GER"
cycle_start_season = 2025

[cv]
pilot_session_id = "2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE"
detector_model = "cv_detector_model"
detector_experiment = "cv_detector"
resolution = 672
sahi = false
sahi_slice = 640
sahi_overlap = 0.2
train_epochs = 30
train_batch_size = 4
train_grad_accum = 4
device = "cpu"
label_frame_target = 400
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


def test_load_config_resolves_paths_relative_to_config_dir(tmp_path: Path) -> None:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")

    cfg = load_config(config_path)

    assert isinstance(cfg, Config)
    assert isinstance(cfg.paths.processed, Path)
    assert cfg.paths.processed == tmp_path / "data" / "processed"
    assert cfg.paths.raw_sportapp == tmp_path / "data" / "raw" / "sportapp"
    assert cfg.reference.final_scores == tmp_path / "data" / "reference" / "final_scores.csv"
    assert cfg.sources.sportapp.base_url == "https://example.invalid/api/v1/public"
    assert cfg.sources.ifaf.tournament == "test-tournament"


def test_load_config_missing_file_raises_filenotfounderror_with_path(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.toml"

    with pytest.raises(FileNotFoundError) as exc_info:
        load_config(missing)

    assert str(missing) in str(exc_info.value)


def test_load_config_missing_table_raises_configerror_naming_it(tmp_path: Path) -> None:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text('[paths]\ndata_root = "data"\n', encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_config(config_path)

    # missing [paths].raw_hudl is what should be surfaced, since [paths] itself exists
    assert "raw_hudl" in str(exc_info.value)


def test_load_config_missing_key_raises_configerror_naming_it(tmp_path: Path) -> None:
    incomplete = MINIMAL_TOML.replace('data_root = "data"\n', "")
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(incomplete, encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_config(config_path)

    assert "data_root" in str(exc_info.value)


def test_load_dotenv_sets_vars_ignores_blank_and_comments_strips_quotes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FFEP_TEST_PLAIN", raising=False)
    monkeypatch.delenv("FFEP_TEST_QUOTED", raising=False)
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "# a leading comment",
                "",
                "FFEP_TEST_PLAIN=plain-value",
                '  ',
                'FFEP_TEST_QUOTED="quoted-value"',
                "# trailing comment",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    load_dotenv(env_path)

    assert os.environ["FFEP_TEST_PLAIN"] == "plain-value"
    assert os.environ["FFEP_TEST_QUOTED"] == "quoted-value"


def test_load_dotenv_does_not_overwrite_preset_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FFEP_TEST_PRESET", "original-value")
    env_path = tmp_path / ".env"
    env_path.write_text("FFEP_TEST_PRESET=would-be-overwritten\n", encoding="utf-8")

    load_dotenv(env_path)

    assert os.environ["FFEP_TEST_PRESET"] == "original-value"


def test_load_dotenv_missing_file_is_a_noop(tmp_path: Path) -> None:
    load_dotenv(tmp_path / "no-such-file.env")  # must not raise


def test_secret_returns_env_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FFEP_TEST_SECRET", "some-resolved-value")

    assert secret("FFEP_TEST_SECRET") == "some-resolved-value"


def test_secret_raises_naming_variable_pointing_at_env_example_no_value_leaked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FFEP_TEST_SECRET_UNSET", raising=False)

    with pytest.raises(ConfigError) as exc_info:
        secret("FFEP_TEST_SECRET_UNSET")

    message = str(exc_info.value)
    assert "FFEP_TEST_SECRET_UNSET" in message
    assert ".env.example" in message


def test_secret_optional_returns_none_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FFEP_TEST_OPTIONAL_SECRET", raising=False)

    assert secret("FFEP_TEST_OPTIONAL_SECRET", required=False) is None


def test_checked_in_config_exclude_games(repo_root: Path) -> None:
    cfg = load_config(repo_root / "ffep.toml")

    assert cfg.train.exclude_games_ep == ["legacy-37"]
    assert cfg.train.exclude_games_wp == ["legacy-35"]


def test_load_config_exposes_competition_tier_path(tmp_path: Path) -> None:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")

    cfg = load_config(config_path)

    assert cfg.reference.competition_tier == (
        tmp_path / "data" / "reference" / "competition_tier.csv"
    )


def test_load_config_missing_competition_tier_key_raises_configerror_naming_it(
    tmp_path: Path,
) -> None:
    incomplete = MINIMAL_TOML.replace(
        'competition_tier = "data/reference/competition_tier.csv"\n', ""
    )
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(incomplete, encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_config(config_path)

    assert "competition_tier" in str(exc_info.value)


def test_load_config_exposes_reports_path_and_new_reference_files(tmp_path: Path) -> None:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")

    cfg = load_config(config_path)

    assert cfg.paths.reports == tmp_path / "reports"
    assert cfg.reference.player_mapping == tmp_path / "data" / "reference" / "player_mapping.csv"
    assert cfg.reference.group_opponents == (
        tmp_path / "data" / "reference" / "group_opponents.csv"
    )


def test_load_config_exposes_report_settings(tmp_path: Path) -> None:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")

    cfg = load_config(config_path)

    assert cfg.report.own_team == "GER"
    assert cfg.report.cycle_start_season == 2025


def test_load_config_missing_report_table_raises_configerror(tmp_path: Path) -> None:
    incomplete = MINIMAL_TOML.replace(
        '\n[report]\nown_team = "GER"\ncycle_start_season = 2025\n', ""
    )
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(incomplete, encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_config(config_path)

    assert "report" in str(exc_info.value)
