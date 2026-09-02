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
import re
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


# --- Private test-set ground-truth vault tooling (Task 2) ---------------------------
#
# `write_continuity_skeleton`/`write_flag_pull_skeleton` pre-fill the two ground-truth
# tables' automatic columns from a session's own tracks -- exactly the shape the
# labelling session in `docs/hackathon-benchmark-labels.md` fills in by hand.
# `validate_test_labels` then gates the filled-in vault against the same
# vocabulary/dialect/completeness rules `tests/test_cv_benchmark_labels.py` already
# enforces for the public pilot-session tables (verdict vocabulary, outcome
# vocabulary, the pull-outcome/pull_time_s pairing, the puller_track_id
# single-int-or-slash pattern, comma/LF dialect), plus a clip-set match against the
# private test game's own registered clips.

_FLAG_PULL_SKELETON_COLUMNS: tuple[str, ...] = (
    "clip_number",
    "outcome",
    "pull_time_s",
    "carrier_track_id",
    "puller_track_id",
    "notes",
)

# Mirrors `cv.bundle`'s own private `_CONTINUITY_REVIEW_SCHEMA`/`_FLAG_PULL_EVENTS_SCHEMA`
# (in turn mirroring `tests/test_cv_benchmark_labels.py`'s authoritative dtypes) --
# kept as separate constants here rather than importing another module's private
# attributes across module boundaries (the established precedent throughout `cv.*`).
_VAULT_CONTINUITY_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "n_tracks": pl.Int32,
    "longest_track_frac": pl.Float64,
    "n_fragments": pl.Int32,
    "auto_flag": pl.Utf8,
    "verdict": pl.Utf8,
    "id_switches": pl.Int32,
    "reviewer_note": pl.Utf8,
}

_VAULT_FLAG_PULL_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "outcome": pl.Utf8,
    "pull_time_s": pl.Float64,
    "carrier_track_id": pl.Int32,
    # Utf8, not Int32: a pull can involve more than one puller ("13/8") -- see
    # `docs/hackathon-benchmark-labels.md`'s multi-puller convention.
    "puller_track_id": pl.Utf8,
    "notes": pl.Utf8,
}

_VALID_TEST_VERDICTS = frozenset({"pass", "fail"})
_TEST_OUTCOME_VOCABULARY = frozenset(
    {
        "pull",
        "incomplete",
        "out_of_bounds",
        "touchdown",
        "other",
        "completion",
        "interception",
        "unknown",
    }
)
_PULLER_TRACK_ID_PATTERN = re.compile(r"^\d+(/\d+)*$")


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_bytes(data)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def write_continuity_skeleton(
    config: Config, session_id: str, tracks: pl.DataFrame, out_path: Path
) -> Path:
    """Pre-fill `out_path` with one `REVIEW_COLUMNS` row per clip registered for
    `session_id` -- auto columns (`n_tracks`, `longest_track_frac`, `n_fragments`,
    `auto_flag`) computed from `tracks` exactly like `continuity.measure_continuity`
    (composing its private per-clip fragment logic directly, `continuity._measure_clip`/
    `_read_expected_frame_counts`, rather than reimplementing it or widening the
    frozen `measure_continuity` signature, which is session-locked to
    `config.cv.pilot_session_id` and exact-matched by `tests/test_cv_contracts.py`).
    Human columns (`verdict`/`id_switches`/`reviewer_note`) start empty, and any
    already-filled values from a prior run at `out_path` survive a re-run unchanged
    (`continuity._write_review_csv`'s own preservation logic, T-2.1-35 precedent) --
    the labelling session can be interrupted and resumed without losing work.
    """
    from flag_football_ep.cv import frames as frames_module
    from flag_football_ep.cv.continuity import _measure_clip, _read_expected_frame_counts, _write_review_csv

    session_clip_numbers = sorted(
        frames_module.clip_number(path) for path in frames_module.clip_paths(config, session_id)
    )
    expected_counts = _read_expected_frame_counts(config, session_id, set(session_clip_numbers))

    rows = []
    for clip_num in session_clip_numbers:
        clip_tracks = tracks.filter(pl.col("clip_number") == clip_num)
        expected = expected_counts.get(clip_num)
        rows.append(_measure_clip(clip_num, clip_tracks, expected_frame_count=expected))

    return _write_review_csv(rows, Path(out_path))


def _load_existing_flag_pull_columns(path: Path) -> dict[int, dict]:
    """Mirrors `continuity._load_existing_human_columns`'s re-run-preservation
    pattern for the flag-pull skeleton: reading `out_path`'s already-filled rows
    before regenerating it means a `write_flag_pull_skeleton` re-run never wipes
    labelling work already in progress.
    """
    if not path.exists():
        return {}

    df = pl.read_csv(path, schema_overrides=_VAULT_FLAG_PULL_SCHEMA)
    if df.height == 0:
        return {}

    preserved: dict[int, dict] = {}
    for row in df.iter_rows(named=True):
        preserved[int(row["clip_number"])] = {
            "outcome": row["outcome"],
            "pull_time_s": row["pull_time_s"],
            "carrier_track_id": row["carrier_track_id"],
            "puller_track_id": row["puller_track_id"],
            "notes": row["notes"],
        }
    return preserved


def write_flag_pull_skeleton(config: Config, session_id: str, out_path: Path) -> Path:
    """Pre-fill `out_path` with the exact public flag-pull header
    (`clip_number,outcome,pull_time_s,carrier_track_id,puller_track_id,notes`), one
    row per clip registered for `session_id`, human columns empty. Already-filled
    rows from a prior run survive a re-run unchanged (see
    `_load_existing_flag_pull_columns`) -- symmetric with `write_continuity_skeleton`'s
    own re-run safety.
    """
    from flag_football_ep.cv import frames as frames_module

    clip_numbers = sorted(
        frames_module.clip_number(path) for path in frames_module.clip_paths(config, session_id)
    )
    out_path = Path(out_path)
    existing = _load_existing_flag_pull_columns(out_path)

    columns: dict[str, list] = {name: [] for name in _FLAG_PULL_SKELETON_COLUMNS}
    for n in clip_numbers:
        human = existing.get(
            n,
            {
                "outcome": None,
                "pull_time_s": None,
                "carrier_track_id": None,
                "puller_track_id": None,
                "notes": None,
            },
        )
        columns["clip_number"].append(n)
        columns["outcome"].append(human["outcome"])
        columns["pull_time_s"].append(human["pull_time_s"])
        columns["carrier_track_id"].append(human["carrier_track_id"])
        columns["puller_track_id"].append(human["puller_track_id"])
        columns["notes"].append(human["notes"])

    df = pl.DataFrame(columns, schema=_VAULT_FLAG_PULL_SCHEMA)
    _atomic_write_bytes(out_path, df.write_csv().encode("utf-8"))
    return out_path


def _assert_comma_dialect(path: Path) -> None:
    raw = path.read_text(encoding="utf-8")
    if ";" in raw:
        raise TestsetError(
            f"{path} contains ';' -- must be comma/LF dialect, not the Hudl-export dialect"
        )


def validate_test_labels(
    continuity_csv: Path, flag_pull_csv: Path, expected_clips: list[int]
) -> dict:
    """Validate the private test game's filled-in ground-truth vault: both files
    exist, use the comma/LF dialect, cover exactly `expected_clips` (no missing clip,
    no extra clip, no wrong-game clip number), every `verdict` is a non-empty
    `pass`/`fail`, every `outcome` is a non-empty value from the documented
    vocabulary, every `outcome == "pull"` row carries `pull_time_s`, and every
    `puller_track_id` is a single int or `/`-separated ints. Raises `TestsetError`
    naming the offending clips on any gate failure. Returns a summary dict
    (`n_clips`, `n_pass`, `n_fail`, `pass_rate`, `n_outcomes`) so a caller (the
    `--validate` CLI path, a plan's SUMMARY) can quote the measured baseline.
    """
    continuity_csv = Path(continuity_csv)
    flag_pull_csv = Path(flag_pull_csv)
    expected_set = set(expected_clips)

    if not continuity_csv.exists():
        raise TestsetError(f"continuity review CSV not found: {continuity_csv}")
    if not flag_pull_csv.exists():
        raise TestsetError(f"flag-pull events CSV not found: {flag_pull_csv}")

    _assert_comma_dialect(continuity_csv)
    _assert_comma_dialect(flag_pull_csv)

    continuity_df = pl.read_csv(continuity_csv, schema_overrides=_VAULT_CONTINUITY_SCHEMA)
    flag_pull_df = pl.read_csv(flag_pull_csv, schema_overrides=_VAULT_FLAG_PULL_SCHEMA)

    continuity_clips = set(continuity_df["clip_number"].to_list())
    if continuity_clips != expected_set:
        raise TestsetError(
            f"{continuity_csv} clip set does not match the expected {len(expected_set)} "
            f"private-test clips: missing {sorted(expected_set - continuity_clips)}, "
            f"unexpected {sorted(continuity_clips - expected_set)}"
        )

    flag_pull_clips = set(flag_pull_df["clip_number"].to_list())
    if flag_pull_clips != expected_set:
        raise TestsetError(
            f"{flag_pull_csv} clip set does not match the expected {len(expected_set)} "
            f"private-test clips: missing {sorted(expected_set - flag_pull_clips)}, "
            f"unexpected {sorted(flag_pull_clips - expected_set)}"
        )

    missing_verdict = (
        continuity_df.filter(pl.col("verdict").fill_null("").str.strip_chars() == "")[
            "clip_number"
        ]
        .sort()
        .to_list()
    )
    if missing_verdict:
        raise TestsetError(f"{continuity_csv} missing verdict for clip(s): {missing_verdict}")

    bad_verdict = continuity_df.filter(~pl.col("verdict").is_in(sorted(_VALID_TEST_VERDICTS)))
    if bad_verdict.height:
        bad_pairs = [
            f"clip {row['clip_number']}: {row['verdict']!r}"
            for row in bad_verdict.iter_rows(named=True)
        ]
        raise TestsetError(f"invalid verdict(s) in {continuity_csv}: {'; '.join(bad_pairs)}")

    missing_outcome = (
        flag_pull_df.filter(pl.col("outcome").fill_null("").str.strip_chars() == "")[
            "clip_number"
        ]
        .sort()
        .to_list()
    )
    if missing_outcome:
        raise TestsetError(f"{flag_pull_csv} missing outcome for clip(s): {missing_outcome}")

    bad_outcome = flag_pull_df.filter(~pl.col("outcome").is_in(sorted(_TEST_OUTCOME_VOCABULARY)))
    if bad_outcome.height:
        bad_pairs = [
            f"clip {row['clip_number']}: {row['outcome']!r}"
            for row in bad_outcome.iter_rows(named=True)
        ]
        raise TestsetError(f"invalid outcome(s) in {flag_pull_csv}: {'; '.join(bad_pairs)}")

    missing_pull_time = (
        flag_pull_df.filter((pl.col("outcome") == "pull") & pl.col("pull_time_s").is_null())[
            "clip_number"
        ]
        .sort()
        .to_list()
    )
    if missing_pull_time:
        raise TestsetError(
            f"{flag_pull_csv}: outcome == 'pull' without pull_time_s for clip(s): "
            f"{missing_pull_time}"
        )

    puller_values = flag_pull_df["puller_track_id"].fill_null("").to_list()
    bad_puller = [
        v for v in puller_values if v.strip() and not _PULLER_TRACK_ID_PATTERN.match(v.strip())
    ]
    if bad_puller:
        raise TestsetError(
            f"{flag_pull_csv}: puller_track_id values not int or '/'-separated ints: {bad_puller}"
        )

    n_clips = continuity_df.height
    n_pass = int((continuity_df["verdict"] == "pass").sum())
    n_fail = int((continuity_df["verdict"] == "fail").sum())
    pass_rate = (n_pass / n_clips) if n_clips else None

    return {
        "n_clips": n_clips,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "pass_rate": pass_rate,
        "n_outcomes": flag_pull_df.height,
    }
