"""Corpus freeze manifests: a dated, fingerprinted, committed statement of which rows
`plays.parquet` contained on a given date, and which git commit produced them.

M3-05-03 (RESEARCH Q3 / Pattern 1): `scripts/hc_corpus_ablation.py` already proved out the
hashing scheme (`compute_corpus_fingerprint`) and the git-lineage capture (`git_commit_sha`)
this module reuses verbatim -- copied in, not imported, so this module and `model/train.py`
each stay self-contained and neither has to import a `scripts/` driver as a library
dependency. `ffep freeze-corpus` (`cli.py`) calls `build_freeze_manifest` then
`write_freeze_manifest`; `ffep train` (`--freeze <path>` or the default latest-manifest
resolution) reads a manifest back via `load_freeze_manifest`/`latest_freeze_manifest`.

Per-source snapshot dates are a best-effort file-mtime approximation only (RESEARCH
Pitfall 3): no source in this project (`fetch/ifaf.py`, `ingest/hc_workbook.py`) writes an
in-band "fetched at" timestamp today, and mtime can be touched by `git checkout`/backup/
`rsync` without content changing. Every snapshot-date entry this module writes carries an
explicit `"approximate": true` flag -- never treated as ground truth.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from flag_football_ep.config import Config

# src/flag_football_ep/model/freeze.py -> model -> flag_football_ep -> src -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[3]

# Same "accepted" grouping `pipeline._build_games_table` documents: "accepted" and
# "accepted-with-warnings" both mean the game reached plays.parquet; only "quarantined"
# means it did not.
_ACCEPTED_STATUSES: frozenset[str] = frozenset({"accepted", "accepted-with-warnings"})


def compute_corpus_fingerprint(plays: pl.DataFrame) -> str:
    """SHA-256 over the sorted `(game_id, play_id, source)` key set of `plays`.

    Copied verbatim from `scripts/hc_corpus_ablation.py::compute_corpus_fingerprint` (same
    docstring rationale: identifies WHICH rows a corpus snapshot saw, independent of any
    model-specific, post-filter training-frame hash). Two calls on the identical `plays`
    frame produce the identical fingerprint.
    """
    keys = plays.select("game_id", "play_id", "source").sort(["game_id", "play_id", "source"])
    return hashlib.sha256(keys.write_csv().encode("utf-8")).hexdigest()


def git_commit_sha() -> str:
    """The current `HEAD` commit, `"unknown"` if `git` is unavailable.

    Copied verbatim from `scripts/hc_corpus_ablation.py::git_commit_sha` -- a missing
    provenance tag must never fail a freeze-manifest write.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _source_raw_dir(source: str, config: Config) -> Path | None:
    """Best-effort mapping from a `plays.parquet` `source` value to the raw directory it
    was fetched/ingested into -- used only for the approximate snapshot-date fallback.

    Prefix-matched, not exact: `source` values carry per-workbook/per-game suffixes (e.g.
    `hc_workbook:scoring-probability-by-situation-2023-2026:data`) that this function does
    not need to parse fully. `"legacy-sportapp"` maps to `raw_legacy` -- it is the pre-port
    CSV `ingest/sportapp.py::_load_legacy_sportapp_csv` reads from `data/raw/legacy`, not a
    fresh sportapp.fi API fetch. Returns `None` for an unrecognized prefix -- best-effort,
    never raises.
    """
    if source.startswith("hc_workbook"):
        return config.paths.raw_hc_files
    if source in ("legacy", "legacy-sportapp"):
        return config.paths.raw_legacy
    if source.startswith("hudl"):
        return config.paths.raw_hudl
    if source.startswith("ifaf"):
        return config.paths.raw_ifaf
    if source.startswith("sportapp"):
        return config.paths.raw_sportapp
    return None


def _latest_mtime_iso(directory: Path) -> str | None:
    """The most recent file mtime under `directory` (recursive), ISO-8601 UTC, or `None`
    when `directory` does not exist or contains no files."""
    if not directory.exists():
        return None
    latest: float | None = None
    for path in directory.rglob("*"):
        if path.is_file():
            mtime = path.stat().st_mtime
            if latest is None or mtime > latest:
                latest = mtime
    if latest is None:
        return None
    return datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()


def build_freeze_manifest(plays: pl.DataFrame, config: Config) -> dict:
    """Build (but do not write) a corpus freeze manifest for `plays`.

    Carries: `date` (today, UTC, ISO), `corpus_fingerprint`, `git_commit`, per-source row
    counts, accepted/quarantined game counts (read from `config.paths.processed /
    "games.parquet"`, not recomputed -- `pipeline._build_games_table`'s own artifact), and a
    best-effort per-source snapshot date (file mtime, explicitly labelled `"approximate":
    true`). Two calls on the identical `plays` frame and unchanged git HEAD produce
    identical `corpus_fingerprint`/`git_commit`.
    """
    corpus_fingerprint = compute_corpus_fingerprint(plays)
    git_commit = git_commit_sha()
    date = datetime.now(timezone.utc).date().isoformat()

    per_source_row_counts = {
        row["source"]: row["n"]
        for row in plays.group_by("source").agg(pl.len().alias("n")).iter_rows(named=True)
    }

    games_path = config.paths.processed / "games.parquet"
    if games_path.exists():
        games = pl.read_parquet(games_path)
        status_counts = {
            row["status"]: row["n"]
            for row in games.group_by("status").agg(pl.len().alias("n")).iter_rows(named=True)
        }
        n_accepted = sum(n for status, n in status_counts.items() if status in _ACCEPTED_STATUSES)
        n_quarantined = sum(
            n for status, n in status_counts.items() if status not in _ACCEPTED_STATUSES
        )
        games_summary = {
            "accepted": n_accepted,
            "quarantined": n_quarantined,
            "total": games.height,
        }
    else:
        # `games.parquet` is a `pipeline.run_ingest` artifact -- absent for a corpus built
        # some other way. Never block a freeze write on it; just say so.
        games_summary = {"accepted": None, "quarantined": None, "total": None}

    per_source_snapshot_date: dict[str, dict] = {}
    for source in sorted(per_source_row_counts):
        raw_dir = _source_raw_dir(source, config)
        snapshot_date = _latest_mtime_iso(raw_dir) if raw_dir is not None else None
        per_source_snapshot_date[source] = {"date": snapshot_date, "approximate": True}

    return {
        "date": date,
        "corpus_fingerprint": corpus_fingerprint,
        "git_commit": git_commit,
        "per_source_row_counts": per_source_row_counts,
        "games": games_summary,
        "per_source_snapshot_date": per_source_snapshot_date,
    }


def write_freeze_manifest(manifest: dict, config: Config) -> Path:
    """Write `manifest` to `data/reference/corpus_freeze/<date>_<fingerprint[:8]>.json`.

    Creates the directory if needed. Idempotent: writing the identical `(date,
    corpus_fingerprint)` manifest twice writes the same content to the same path both
    times -- never raises on a repeat write.
    """
    out_dir = config.paths.corpus_freeze
    out_dir.mkdir(parents=True, exist_ok=True)
    fingerprint_prefix = manifest["corpus_fingerprint"][:8]
    out_path = out_dir / f"{manifest['date']}_{fingerprint_prefix}.json"
    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def load_freeze_manifest(path: Path) -> dict:
    """Read a manifest written by `write_freeze_manifest` back into the same shape."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def latest_freeze_manifest(config: Config) -> Path | None:
    """The most recently dated manifest under `config.paths.corpus_freeze`, or `None` when
    the directory is empty or absent.

    Never raises for "no freeze yet" -- `ffep train`'s default `--freeze` resolution must
    fall back gracefully to training with no freeze citation. Manifest filenames sort
    chronologically (`<date>_<fingerprint8>.json`, ISO date prefix), so the lexicographically
    last filename is the most recent freeze.
    """
    freeze_dir = config.paths.corpus_freeze
    if not freeze_dir.exists():
        return None
    manifests = sorted(freeze_dir.glob("*.json"))
    if not manifests:
        return None
    return manifests[-1]
