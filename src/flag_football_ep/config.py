"""Configuration loading for the flag-football-ep pipeline.

Every later plan resolves paths and secrets through this module rather than
hardcoding them. Paths and non-secret settings are checked in via `ffep.toml`;
secrets (API keys) are resolved only from the environment or a git-ignored
`.env` file — never from committed code or config.

Zero extra runtime dependencies for config parsing: `tomllib` (stdlib,
Python >=3.11) reads the TOML, and `load_dotenv()` below is a small
stdlib-only `.env` parser.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


class ConfigError(Exception):
    """Raised when the checked-in TOML config is missing a required table or key,
    or when a required secret is not resolvable from the environment.
    """


@dataclass(frozen=True)
class Paths:
    data_root: Path
    raw_hudl: Path
    raw_sportapp: Path
    raw_ifaf: Path
    raw_legacy: Path
    processed: Path
    reference: Path
    models: Path
    mlruns: Path
    contract: Path
    reports: Path
    video: Path
    labels: Path
    tracking: Path
    # M3-01: optional, resolved outside _PATH_KEYS/_key() below so that a
    # config written before M3 (missing this key entirely) keeps loading and
    # no pre-existing test fixture TOML needs an edit.
    raw_hc_files: Path = Path("data/raw/hc_files")


@dataclass(frozen=True)
class ReferenceFiles:
    half_boundaries: Path
    final_scores: Path
    team_mapping: Path
    sportapp_games: Path
    competition_tier: Path
    player_mapping: Path
    group_opponents: Path
    hover_positions: Path
    homography_calibration: Path
    gt_positions: Path
    continuity_review: Path
    # M3-01: optional, same rationale as Paths.raw_hc_files above. The file
    # this points at (data/reference/hc_games.csv) is not created until plan
    # M3-01-03; nothing loads it until the pipeline wiring in M3-01-04, which
    # guards the load.
    hc_games: Path = Path("data/reference/hc_games.csv")
    # M3-04-02: optional, same rationale as hc_games above. Points at the
    # maintained camp/competition row-window table; nothing loads it until
    # the player-analysis report plans (M3-04-03..05) wire it in.
    hc_splits: Path = Path("data/reference/hc_splits.csv")


@dataclass(frozen=True)
class SportappSource:
    base_url: str
    api_key_env: str


@dataclass(frozen=True)
class IfafSource:
    base_url: str
    tournament: str
    api_key_env: str


@dataclass(frozen=True)
class Sources:
    sportapp: SportappSource
    ifaf: IfafSource


@dataclass(frozen=True)
class TrainSettings:
    ep_experiment: str
    wp_experiment: str
    exclude_games_ep: list[str]
    exclude_games_wp: list[str]


@dataclass(frozen=True)
class ReportSettings:
    own_team: str
    cycle_start_season: int


@dataclass(frozen=True)
class CvSettings:
    pilot_session_id: str
    detector_model: str
    detector_experiment: str
    resolution: int
    sahi: bool
    sahi_slice: int
    sahi_overlap: float
    train_epochs: int
    train_batch_size: int
    train_grad_accum: int
    device: str
    label_frame_target: int
    cvat_host: str
    cvat_username_env: str
    cvat_password_env: str
    field_length_yards: float
    field_width_yards: float
    endzone_yards: float
    dvc_remote_name: str
    dvc_remote_url: str
    dvc_remote_endpoint: str
    otc_obs_access_key_env: str
    otc_obs_secret_key_env: str


@dataclass(frozen=True)
class Config:
    paths: Paths
    reference: ReferenceFiles
    sources: Sources
    train: TrainSettings
    report: ReportSettings
    cv: CvSettings


_PATH_KEYS = (
    "data_root",
    "raw_hudl",
    "raw_sportapp",
    "raw_ifaf",
    "raw_legacy",
    "processed",
    "reference",
    "models",
    "mlruns",
    "contract",
    "reports",
    "video",
    "labels",
    "tracking",
)
_REFERENCE_KEYS = (
    "half_boundaries",
    "final_scores",
    "team_mapping",
    "sportapp_games",
    "competition_tier",
    "player_mapping",
    "group_opponents",
    "hover_positions",
    "homography_calibration",
    "gt_positions",
    "continuity_review",
)
_SPORTAPP_KEYS = ("base_url", "api_key_env")
_IFAF_KEYS = ("base_url", "tournament", "api_key_env")
_TRAIN_KEYS = ("ep_experiment", "wp_experiment", "exclude_games_ep", "exclude_games_wp")
_REPORT_KEYS = ("own_team", "cycle_start_season")
_CV_KEYS = (
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
)


def _table(data: dict, dotted_name: str) -> dict:
    """Fetch a (possibly nested, dotted) table, raising ConfigError if absent."""
    node = data
    for part in dotted_name.split("."):
        if not isinstance(node, dict) or part not in node:
            raise ConfigError(f"missing required table [{dotted_name}] in config")
        node = node[part]
    return node


def _key(table: dict, dotted_table_name: str, key: str):
    if key not in table:
        raise ConfigError(f"missing required key '{key}' in [{dotted_table_name}]")
    return table[key]


def _resolve(base_dir: Path, raw: str) -> Path:
    """Resolve a TOML path string relative to the config file's directory."""
    candidate = Path(raw)
    return candidate if candidate.is_absolute() else base_dir / candidate


def load_config(path: Path = Path("ffep.toml")) -> Config:
    """Load and validate the checked-in TOML config.

    Path attributes are `Path` objects resolved relative to `path`'s parent
    directory (not the process working directory), so behavior is the same
    regardless of where `ffep` is invoked from.

    Calls `load_dotenv()` once so `.env` values are visible to `secret()` for
    the rest of the process.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")

    load_dotenv()

    with path.open("rb") as f:
        data = tomllib.load(f)

    base_dir = path.resolve().parent

    paths_table = _table(data, "paths")
    paths = Paths(
        **{key: _resolve(base_dir, _key(paths_table, "paths", key)) for key in _PATH_KEYS},
        # Not in _PATH_KEYS/_key() deliberately (see Paths.raw_hc_files docstring):
        # falls back to the same default the dataclass field carries when a
        # pre-M3 ffep.toml doesn't declare this key.
        raw_hc_files=_resolve(
            base_dir, paths_table.get("raw_hc_files", "data/raw/hc_files")
        ),
    )

    reference_table = _table(data, "reference")
    reference = ReferenceFiles(
        **{
            key: _resolve(base_dir, _key(reference_table, "reference", key))
            for key in _REFERENCE_KEYS
        },
        # Not in _REFERENCE_KEYS/_key() deliberately (see ReferenceFiles.hc_games
        # docstring): same pre-M3-compat fallback as raw_hc_files above.
        hc_games=_resolve(
            base_dir, reference_table.get("hc_games", "data/reference/hc_games.csv")
        ),
        # Not in _REFERENCE_KEYS/_key() deliberately (see ReferenceFiles.hc_splits
        # docstring): same pre-M3-04-compat fallback as hc_games above.
        hc_splits=_resolve(
            base_dir, reference_table.get("hc_splits", "data/reference/hc_splits.csv")
        ),
    )

    sportapp_table = _table(data, "sources.sportapp")
    sportapp = SportappSource(
        **{key: _key(sportapp_table, "sources.sportapp", key) for key in _SPORTAPP_KEYS}
    )

    ifaf_table = _table(data, "sources.ifaf")
    ifaf = IfafSource(
        **{key: _key(ifaf_table, "sources.ifaf", key) for key in _IFAF_KEYS}
    )

    train_table = _table(data, "train")
    train = TrainSettings(
        **{key: _key(train_table, "train", key) for key in _TRAIN_KEYS}
    )

    report_table = _table(data, "report")
    report = ReportSettings(
        **{key: _key(report_table, "report", key) for key in _REPORT_KEYS}
    )

    cv_table = _table(data, "cv")
    cv = CvSettings(**{key: _key(cv_table, "cv", key) for key in _CV_KEYS})

    return Config(
        paths=paths,
        reference=reference,
        sources=Sources(sportapp=sportapp, ifaf=ifaf),
        train=train,
        report=report,
        cv=cv,
    )


def load_dotenv(path: Path = Path(".env")) -> None:
    """Populate `os.environ` from a simple `KEY=VALUE` file.

    Blank lines and lines starting with `#` are ignored. Surrounding single
    or double quotes around a value are stripped. Never overwrites a
    variable already present in `os.environ` — an explicitly-set environment
    variable always wins over `.env`.
    """
    path = Path(path)
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def secret(env_name: str, required: bool = True) -> str | None:
    """Resolve a secret by environment variable name.

    Reads only from `os.environ` (populated directly, or via `load_dotenv()`
    from a git-ignored `.env`). Never logs or echoes the resolved value.
    """
    value = os.environ.get(env_name)
    if value:
        return value
    if required:
        raise ConfigError(
            f"environment variable '{env_name}' is not set — "
            "see .env.example for the expected secret names"
        )
    return None
