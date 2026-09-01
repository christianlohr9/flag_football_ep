"""Coverage for the Phase 2.1 CV config surface: `Paths.video/labels/tracking`,
`ReferenceFiles.hover_positions/homography_calibration/gt_positions/continuity_review`,
and the `[cv]` `CvSettings` table.

Mirrors `tests/test_config.py`'s tmp_path/TOML-fixture style. `MINIMAL_TOML` there
already carries a complete `[cv]` table, so these tests reuse the checked-in
`ffep.toml` for the "real config" assertions and a locally trimmed TOML for the
missing-key `ConfigError` case.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.config import ConfigError, load_config
from test_config import MINIMAL_TOML


def test_checked_in_config_pilot_session_id_matches_video_inventory(repo_root: Path) -> None:
    cfg = load_config(repo_root / "ffep.toml")

    inventory = pl.read_csv(repo_root / "data" / "reference" / "video_inventory.csv")
    session_ids = set(inventory["session_id"].unique().to_list())

    assert cfg.cv.pilot_session_id in session_ids, (
        f"cv.pilot_session_id {cfg.cv.pilot_session_id!r} is not a session_id present "
        "in data/reference/video_inventory.csv"
    )


def test_checked_in_config_resolution_is_multiple_of_224(repo_root: Path) -> None:
    cfg = load_config(repo_root / "ffep.toml")

    # 224 = lcm(32, 56) -- valid under either documented RF-DETR resolution-divisibility rule.
    assert cfg.cv.resolution % 224 == 0, (
        f"cv.resolution {cfg.cv.resolution} is not a multiple of 224 (= lcm(32, 56))"
    )


def test_new_paths_and_reference_files_are_absolute_under_repo_root(tmp_path: Path) -> None:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")

    cfg = load_config(config_path)

    for path in (
        cfg.paths.video,
        cfg.paths.labels,
        cfg.paths.tracking,
        cfg.reference.hover_positions,
        cfg.reference.homography_calibration,
        cfg.reference.gt_positions,
        cfg.reference.continuity_review,
    ):
        assert path.is_absolute(), f"{path} is not an absolute path"
        assert tmp_path in path.parents or path.parent == tmp_path, (
            f"{path} does not resolve under {tmp_path}"
        )

    assert cfg.paths.video == tmp_path / "data" / "video"
    assert cfg.paths.labels == tmp_path / "data" / "labels"
    assert cfg.paths.tracking == tmp_path / "data" / "processed" / "tracking"
    assert cfg.reference.homography_calibration == (
        tmp_path / "data" / "reference" / "homography_calibration.csv"
    )


def test_checked_in_config_has_dvc_versioning_fields(repo_root: Path) -> None:
    cfg = load_config(repo_root / "ffep.toml")

    assert cfg.cv.dvc_remote_name == "otc-obs"
    assert cfg.cv.dvc_remote_url.startswith("s3://")
    assert cfg.cv.dvc_remote_endpoint == "https://obs.eu-de.otc.t-systems.com"
    assert cfg.cv.otc_obs_access_key_env == "OTC_OBS_ACCESS_KEY_ID"
    assert cfg.cv.otc_obs_secret_key_env == "OTC_OBS_SECRET_ACCESS_KEY"


def test_checked_in_config_has_no_literal_key_material(repo_root: Path) -> None:
    text = (repo_root / "ffep.toml").read_text(encoding="utf-8")

    # AWS/OTC access keys are shaped `AKIA...` (20 uppercase alnum chars) or
    # appear via a literal `access_key`/`secret_key` assignment -- neither
    # should ever appear in the checked-in TOML, only env-var *names*.
    assert "AKIA" not in text, "ffep.toml appears to contain a literal AWS/OTC access key"
    assert not re.search(r"\baccess_key\s*=", text), (
        "ffep.toml assigns access_key directly instead of referencing an env-var name"
    )
    assert not re.search(r"\bsecret_key\s*=", text), (
        "ffep.toml assigns secret_key directly instead of referencing an env-var name"
    )


def test_missing_cv_table_raises_configerror_naming_it(tmp_path: Path) -> None:
    incomplete = MINIMAL_TOML[: MINIMAL_TOML.index("[cv]")]
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(incomplete, encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_config(config_path)

    assert "cv" in str(exc_info.value)


@pytest.mark.parametrize(
    "key",
    [
        "pilot_session_id",
        "detector_model",
        "detector_experiment",
        "resolution",
        "sahi",
        "sahi_slice",
        "sahi_overlap",
        "train_epochs",
        "train_batch_size",
        "train_grad_accum",
        "device",
        "label_frame_target",
        "cvat_host",
        "cvat_username_env",
        "cvat_password_env",
        "field_length_yards",
        "field_width_yards",
        "endzone_yards",
        "dvc_remote_name",
        "dvc_remote_url",
        "dvc_remote_endpoint",
        "otc_obs_access_key_env",
        "otc_obs_secret_key_env",
    ],
)
def test_missing_cv_key_raises_configerror_naming_it(tmp_path: Path, key: str) -> None:
    cv_start = MINIMAL_TOML.index("[cv]")
    header, cv_block = MINIMAL_TOML[:cv_start], MINIMAL_TOML[cv_start:]
    lines = cv_block.splitlines(keepends=True)
    trimmed_lines = [line for line in lines if not line.startswith(f"{key} = ")]
    assert len(trimmed_lines) == len(lines) - 1, f"expected exactly one line for key {key!r}"
    incomplete = header + "".join(trimmed_lines)

    config_path = tmp_path / "ffep.toml"
    config_path.write_text(incomplete, encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_config(config_path)

    assert key in str(exc_info.value)
