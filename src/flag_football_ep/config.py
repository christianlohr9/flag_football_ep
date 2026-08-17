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


@dataclass(frozen=True)
class ReferenceFiles:
    half_boundaries: Path
    final_scores: Path
    team_mapping: Path
    sportapp_games: Path


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
class Config:
    paths: Paths
    reference: ReferenceFiles
    sources: Sources
    train: TrainSettings


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
)
_REFERENCE_KEYS = ("half_boundaries", "final_scores", "team_mapping", "sportapp_games")
_SPORTAPP_KEYS = ("base_url", "api_key_env")
_IFAF_KEYS = ("base_url", "tournament", "api_key_env")
_TRAIN_KEYS = ("ep_experiment", "wp_experiment", "exclude_games_ep", "exclude_games_wp")


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

    base_dir = path.parent

    paths_table = _table(data, "paths")
    paths = Paths(
        **{key: _resolve(base_dir, _key(paths_table, "paths", key)) for key in _PATH_KEYS}
    )

    reference_table = _table(data, "reference")
    reference = ReferenceFiles(
        **{
            key: _resolve(base_dir, _key(reference_table, "reference", key))
            for key in _REFERENCE_KEYS
        }
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

    return Config(
        paths=paths,
        reference=reference,
        sources=Sources(sportapp=sportapp, ifaf=ifaf),
        train=train,
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
