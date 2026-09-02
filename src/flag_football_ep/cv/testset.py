"""Hackathon-role split and private-test-set ground-truth tooling (Phase 2.2 plan 02.2-21).

Deliberately separate from `cv.frames`'s `EvalSplit`/`freeze_eval_clips`/`read_eval_split`,
which govern the DETECTOR's own training/eval split (`role = pool` / `role = frozen_eval`
in `data/reference/frozen_eval_clips.csv`) and stay entirely unchanged by this module. This
module governs a different, higher-level split -- which GAME each hackathon bundle kind
ships (`hackathon_role = dev` / `private_test` in `data/reference/hackathon_split.csv`) --
so that the private hackathon test set can be the real second drone game
(`2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE`) instead of a same-game clip withholding
(D-07's now-superseded fallback), satisfying DATA-04's game-disjointness requirement.

`data/reference/al_excluded_sessions.csv` is the session-level (not clip-level) training-pool
exclusion the private test game needs: Puerto Rico is `drone` and reuses clip numbers 1..63,
the same numbering the pilot session uses, so a clip-level exclusion via
`frozen_eval_clips.csv` would either be refused by that file's file-wide seed invariant or,
if forced, silently empty the pilot session's own candidate pool. `select_al_frames`
(`cv.active_learning`) reads this file and refuses to open a single clip of an excluded
session, fail-closed, before any detector is loaded (DATA-04).

Both reference tables are written ONLY by `ffep cv hackathon-split` (`write_hackathon_split`/
`write_al_exclusion`) -- never hand-edited -- mirroring `cv.frames.freeze_eval_clips`'s
atomic-write, metadata-only-inventory-read discipline.

`write_continuity_skeleton`/`write_flag_pull_skeleton`/`validate_test_labels` (added by this
plan's Task 2) are the tooling the private test game's human labelling session needs: they
pre-fill the two ground-truth tables' automatic columns from the frozen tracks, then validate
the user's filled-in verdicts against the same vocabulary/dialect gates
`tests/test_cv_benchmark_labels.py` already enforces for the public pilot-session tables.

Implemented by plan 02.2-21.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class TestsetError(CvError, ValueError):
    """Raised when the hackathon-role split, the AL-exclusion table, or the private
    test-set label vault cannot be written or read: a malformed/missing CSV, a
    `hackathon_role` value outside {`dev`, `private_test`}, zero or more than one
    `private_test`/`dev` session named in the split, or (from `validate_test_labels`)
    an incomplete or malformed vault.
    """


@dataclass(frozen=True)
class HackathonSplit:
    """The frozen hackathon-role split: which session is the public dev set, which is
    the private test set, and each one's registered clip numbers (sorted, never a
    hard-coded list -- always resolved from `video_inventory.csv`).
    """

    dev_session_id: str
    test_session_id: str
    dev_clips: list[int]
    test_clips: list[int]
    frozen_at: str


_HACKATHON_SPLIT_COLUMNS: tuple[str, ...] = (
    "domain",
    "session_id",
    "clip_number",
    "hackathon_role",
    "frozen_at",
    "note",
)

_HACKATHON_SPLIT_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "clip_number": pl.Int64,
    "hackathon_role": pl.Utf8,
    "frozen_at": pl.Utf8,
    "note": pl.Utf8,
}

_HACKATHON_ROLES = frozenset({"dev", "private_test"})

_AL_EXCLUSION_COLUMNS: tuple[str, ...] = (
    "session_id",
    "domain",
    "reason",
    "requirement",
    "excluded_at",
)

_AL_EXCLUSION_SCHEMA: dict[str, pl.DataType] = {
    "session_id": pl.Utf8,
    "domain": pl.Utf8,
    "reason": pl.Utf8,
    "requirement": pl.Utf8,
    "excluded_at": pl.Utf8,
}

# Mirrors `cv.frames`'s own private `_INVENTORY_SCHEMA` -- kept as a separate constant
# rather than importing another module's private attribute across module boundaries
# (the established precedent: `cv.bundle`/`cv.continuity` both do the same for this
# exact schema).
_INVENTORY_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "game_id": pl.Utf8,
    "capture_date": pl.Utf8,
    "resolution": pl.Utf8,
    "fps": pl.Float64,
    "duration_seconds": pl.Float64,
    "local_path": pl.Utf8,
    "content_sha256": pl.Utf8,
    "notes": pl.Utf8,
}

_SPLIT_NOTE = "plan 02.2-21, decision 2026-09-02: hackathon dev/private_test role split"
_EXCLUSION_NOTE_TEMPLATE = "plan 02.2-21, decision 2026-09-02: {reason}"


def _read_session_domain_and_clips(config: Config, session_id: str) -> tuple[str, set[int]]:
    """The registered domain and clip-number set for `session_id`, read straight from
    `video_inventory.csv` -- a metadata-only read (mirrors
    `cv.frames._read_domain_clip_numbers`'s own technique) that never requires the
    video bytes to be present on disk, only the inventory row. Raises `TestsetError`
    naming `session_id` when it has no registered rows, or spans more than one domain
    (every session this module handles is single-domain today; a multi-domain session
    would be a data-entry error, not something to silently pick one domain for).
    """
    from flag_football_ep.cv.frames import clip_number as clip_number_of

    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        raise TestsetError(f"video inventory not found: {inventory_path}")

    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter(pl.col("session_id") == session_id)
    if rows.height == 0:
        raise TestsetError(f"no clips registered for session_id {session_id!r} in {inventory_path}")

    domains = sorted(rows.select("domain").unique().to_series().to_list())
    if len(domains) != 1:
        raise TestsetError(
            f"session_id {session_id!r} spans more than one domain in {inventory_path}: {domains}"
        )

    numbers: set[int] = set()
    for row in rows.iter_rows(named=True):
        local_path = row["local_path"]
        if local_path:
            numbers.add(clip_number_of(Path(local_path)))
    return domains[0], numbers


def _atomic_write_csv(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def write_hackathon_split(
    config: Config, dev_session_id: str, test_session_id: str, out_csv: Path
) -> HackathonSplit:
    """Write the hackathon-role split (`data/reference/hackathon_split.csv`): one row
    per registered clip of `dev_session_id` (`hackathon_role = dev`) and one row per
    registered clip of `test_session_id` (`hackathon_role = private_test`) -- clip
    numbers always resolved from `video_inventory.csv`, never hard-coded.

    Idempotent: if `out_csv` already exists and already encodes this exact
    `(dev_session_id, test_session_id, dev_clips, test_clips)` combination, the file
    is left untouched and the existing split is returned -- re-running with the same
    arguments produces a byte-identical file (nothing is written a second time, so
    there is nothing that could differ). A change to either session id or either
    session's registered clip set rewrites the file with a fresh `frozen_at`.
    """
    out_csv = Path(out_csv)
    dev_domain, dev_clip_set = _read_session_domain_and_clips(config, dev_session_id)
    test_domain, test_clip_set = _read_session_domain_and_clips(config, test_session_id)
    dev_clips = sorted(dev_clip_set)
    test_clips = sorted(test_clip_set)

    if out_csv.exists():
        try:
            existing = read_hackathon_split(out_csv)
        except TestsetError:
            existing = None
        if (
            existing is not None
            and existing.dev_session_id == dev_session_id
            and existing.test_session_id == test_session_id
            and existing.dev_clips == dev_clips
            and existing.test_clips == test_clips
        ):
            return existing

    frozen_at = datetime.now(timezone.utc).isoformat()

    rows: list[dict] = []
    for n in dev_clips:
        rows.append(
            {
                "domain": dev_domain,
                "session_id": dev_session_id,
                "clip_number": n,
                "hackathon_role": "dev",
                "frozen_at": frozen_at,
                "note": _SPLIT_NOTE,
            }
        )
    for n in test_clips:
        rows.append(
            {
                "domain": test_domain,
                "session_id": test_session_id,
                "clip_number": n,
                "hackathon_role": "private_test",
                "frozen_at": frozen_at,
                "note": _SPLIT_NOTE,
            }
        )

    rows_sorted = sorted(rows, key=lambda r: (r["hackathon_role"], r["clip_number"]))
    df = pl.DataFrame(
        {col: [row[col] for row in rows_sorted] for col in _HACKATHON_SPLIT_COLUMNS},
        schema=_HACKATHON_SPLIT_SCHEMA,
    )
    _atomic_write_csv(df, out_csv)

    return HackathonSplit(
        dev_session_id=dev_session_id,
        test_session_id=test_session_id,
        dev_clips=dev_clips,
        test_clips=test_clips,
        frozen_at=frozen_at,
    )


def read_hackathon_split(path: Path) -> HackathonSplit:
    """Load a `HackathonSplit` previously written by `write_hackathon_split`.

    Raises `TestsetError` naming `path` when the file is absent, is not readable as
    CSV, is missing a declared column, has zero rows, carries a `hackathon_role`
    value outside {`dev`, `private_test`}, or names zero or more than one distinct
    `private_test`/`dev` session id.
    """
    path = Path(path)
    if not path.exists():
        raise TestsetError(f"hackathon split not found: {path}")

    try:
        df = pl.read_csv(path, schema_overrides=_HACKATHON_SPLIT_SCHEMA)
    except Exception as exc:  # noqa: BLE001 -- surfaced as a named TestsetError
        raise TestsetError(f"hackathon split at {path} could not be read: {exc}") from exc

    missing_columns = set(_HACKATHON_SPLIT_COLUMNS) - set(df.columns)
    if missing_columns:
        raise TestsetError(
            f"hackathon split at {path} is missing column(s) {sorted(missing_columns)}"
        )
    if df.height == 0:
        raise TestsetError(f"hackathon split at {path} has no rows")

    roles = set(df["hackathon_role"].to_list())
    bad_roles = roles - _HACKATHON_ROLES
    if bad_roles:
        raise TestsetError(
            f"hackathon split at {path} has hackathon_role value(s) outside "
            f"{sorted(_HACKATHON_ROLES)}: {sorted(bad_roles)}"
        )

    test_rows = df.filter(pl.col("hackathon_role") == "private_test")
    test_session_ids = sorted(test_rows.select("session_id").unique().to_series().to_list())
    if len(test_session_ids) != 1:
        raise TestsetError(
            f"hackathon split at {path} must name exactly one private_test session_id, "
            f"found {len(test_session_ids)}: {test_session_ids}"
        )

    dev_rows = df.filter(pl.col("hackathon_role") == "dev")
    dev_session_ids = sorted(dev_rows.select("session_id").unique().to_series().to_list())
    if len(dev_session_ids) != 1:
        raise TestsetError(
            f"hackathon split at {path} must name exactly one dev session_id, "
            f"found {len(dev_session_ids)}: {dev_session_ids}"
        )

    frozen_at = max(df.select("frozen_at").unique().to_series().to_list())

    return HackathonSplit(
        dev_session_id=dev_session_ids[0],
        test_session_id=test_session_ids[0],
        dev_clips=sorted(dev_rows["clip_number"].to_list()),
        test_clips=sorted(test_rows["clip_number"].to_list()),
        frozen_at=frozen_at,
    )


def write_al_exclusion(
    config: Config, session_id: str, reason: str, requirement: str, out_csv: Path
) -> Path:
    """Write (or refresh) `session_id`'s row in the tracked training-pool exclusion
    table (`data/reference/al_excluded_sessions.csv`) -- session-level, never
    clip-level (a clip-level exclusion cannot work here: the private test game reuses
    the pilot session's own clip numbers, `docs/…`/this plan's `<interfaces>` block).
    Accumulates across calls: an existing row for a *different* session_id is
    preserved untouched; a call naming an already-excluded session_id replaces just
    that row (fresh `excluded_at`), matching `cv.frames.freeze_eval_clips`'s
    accumulate-across-domains precedent.
    """
    out_csv = Path(out_csv)
    domain, _clips = _read_session_domain_and_clips(config, session_id)
    excluded_at = datetime.now(timezone.utc).isoformat()

    existing_rows: list[dict] = []
    if out_csv.exists():
        existing_df = pl.read_csv(out_csv, schema_overrides=_AL_EXCLUSION_SCHEMA)
        existing_rows = [
            row for row in existing_df.to_dicts() if row["session_id"] != session_id
        ]

    existing_rows.append(
        {
            "session_id": session_id,
            "domain": domain,
            "reason": reason,
            "requirement": requirement,
            "excluded_at": excluded_at,
        }
    )
    rows_sorted = sorted(existing_rows, key=lambda r: r["session_id"])

    df = pl.DataFrame(
        {col: [row[col] for row in rows_sorted] for col in _AL_EXCLUSION_COLUMNS},
        schema=_AL_EXCLUSION_SCHEMA,
    )
    _atomic_write_csv(df, out_csv)
    return out_csv


def read_al_excluded_sessions(path: Path) -> dict[str, str]:
    """`session_id -> reason` for every session excluded from active-learning
    candidate pools. An absent file means no session is excluded (empty dict) --
    `select_al_frames` must not treat a missing exclusion table as an error, only a
    malformed one.
    """
    path = Path(path)
    if not path.exists():
        return {}

    df = pl.read_csv(path, schema_overrides=_AL_EXCLUSION_SCHEMA)
    return {row["session_id"]: row["reason"] for row in df.iter_rows(named=True)}
