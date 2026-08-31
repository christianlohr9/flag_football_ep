"""Per-clip track continuity measurement: the C-09 "trackable without ID switch" gate metric.

`measure_continuity` computes, per clip, the fraction of the clip covered by its
longest single track (`longest_track_frac`) and the number of track fragments a player
was split into (`n_fragments`), auto-flagging clips that fall below the pilot gate's
"trackable without ID switch" threshold for human review in `review_csv`.
`summarise_review` rolls the (possibly human-annotated) review CSV up into the
whole-session summary dict the go/no-go gate doc reports -- explicitly over the full
`n=61`-clip denominator, never a cherry-picked subset (D-09).

`measure_continuity` uses `config.cv.pilot_session_id` (there is exactly one pilot
session, D-02) via `frames.clip_paths` to enumerate every clip registered for the
session, not just the clip numbers present in `tracks` -- a clip with zero confirmed
tracks still gets a row (T-2.1-31): the whole game is the denominator, not the subset
of clips the tracker happened to produce output for.

Automatic metrics (`auto_flag`) prioritise the human review; they never substitute for
it (D-09). `summarise_review` enforces this at the data level: as long as any clip's
`verdict` cell is empty, `pass_rate` is `None` and the offending clips are listed --
the summary refuses to manufacture a headline number from a shrunken denominator.

Implemented by plan 02.1-14, alongside `overlay.py`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from flag_football_ep.config import Config


@dataclass(frozen=True)
class ContinuityRow:
    """One clip's continuity measurement: track count, the longest track's coverage
    fraction, fragment count, and an auto-assigned review flag.
    """

    clip_number: int
    n_tracks: int
    longest_track_frac: float
    n_fragments: int
    auto_flag: str


@dataclass
class ContinuityResult:
    """The full continuity measurement: every clip's `ContinuityRow`, the review CSV
    path flagged clips were written to, the whole-session summary dict, and notices
    (one per clip whose coverage denominator had to fall back to the last tracked
    frame because the inventory carries no duration/fps for it).
    """

    rows: list[ContinuityRow] = field(default_factory=list)
    review_csv: Path = field(default_factory=Path)
    summary: dict = field(default_factory=dict)
    notices: list[str] = field(default_factory=list)


# Exact column order per this plan's <interfaces> block -- the committed review CSV's
# header, byte for byte.
REVIEW_COLUMNS: tuple[str, ...] = (
    "clip_number",
    "n_tracks",
    "longest_track_frac",
    "n_fragments",
    "auto_flag",
    "verdict",
    "id_switches",
    "reviewer_note",
)

_REVIEW_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "n_tracks": pl.Int32,
    "longest_track_frac": pl.Float64,
    "n_fragments": pl.Int32,
    "auto_flag": pl.Utf8,
    "verdict": pl.Utf8,
    "id_switches": pl.Int32,
    "reviewer_note": pl.Utf8,
}

# A track covering less than this fraction of its clip counts as a fragment (the
# <action> text's "under 50%").
_FRAGMENT_COVERAGE_THRESHOLD = 0.5

# `few-tracks` floor: below this many distinct confirmed tracks in a clip is almost
# certainly a detection failure rather than a sparsely-framed play (5v5 plus
# officials means a real play very rarely has fewer than a handful of trackable
# entities). Deliberately conservative/low so it never masks the `fragmented` signal
# on clips with a normal track count but poor continuity.
_EXPECTED_MIN_TRACKS = 3

_VALID_VERDICTS = frozenset({"pass", "fail", ""})

# Mirrors `track.py`'s own private `_INVENTORY_SCHEMA` -- kept as a separate constant
# rather than importing the private module attribute across module boundaries (same
# precedent as `detect.py`'s `_IMAGE_SUFFIXES`).
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


def _read_expected_frame_counts(
    config: Config, session_id: str, clip_numbers: set[int]
) -> dict[int, int]:
    """The expected whole-clip frame count per clip -- `round(duration_seconds * fps)`
    from `video_inventory.csv` (the same source `frames.clip_paths` reads, filtered the
    same way). This is the coverage denominator D-09 demands: the clip's REAL length,
    not the last frame the tracker happened to produce output for. A clip absent from
    the inventory (or with a null/non-positive duration or fps) is simply missing from
    the returned dict -- the caller falls back to the last tracked frame with a notice.
    """
    from flag_football_ep.cv.frames import clip_number as clip_number_of

    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        return {}

    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter((pl.col("domain") == "drone") & (pl.col("session_id") == session_id))

    expected: dict[int, int] = {}
    for row in rows.iter_rows(named=True):
        local_path = row["local_path"]
        if not local_path:
            continue
        n = clip_number_of(Path(local_path))
        duration = row["duration_seconds"]
        fps = row["fps"]
        if n in clip_numbers and duration is not None and fps is not None:
            if duration > 0 and fps > 0:
                expected[n] = round(duration * fps)
    return expected


def _clip_flag(n_tracks: int, n_fragments: int) -> str:
    if n_tracks == 0:
        return "no-tracks"
    if n_tracks < _EXPECTED_MIN_TRACKS:
        return "few-tracks"
    if n_fragments > n_tracks / 2:
        return "fragmented"
    return "ok"


def _measure_clip(
    clip_number: int, clip_tracks: pl.DataFrame, *, expected_frame_count: int | None = None
) -> ContinuityRow:
    if clip_tracks.height == 0:
        return ContinuityRow(
            clip_number=clip_number,
            n_tracks=0,
            longest_track_frac=0.0,
            n_fragments=0,
            auto_flag="no-tracks",
        )

    # The coverage denominator is the clip's REAL frame count from the inventory, not
    # the last frame that produced a confirmed track -- if tracking dies partway
    # through a clip (exactly the failure mode this metric exists to catch), a
    # tracked-rows-only denominator would overstate `longest_track_frac` (a track
    # covering frames 0-299 of a 900-frame clip would read as 1.0). The last tracked
    # frame still floors the denominator so an inventory duration that slightly
    # under-declares the clip never pushes a fraction above 1.0.
    last_tracked_frame_count = int(clip_tracks["frame_index"].max()) + 1
    if expected_frame_count is not None:
        clip_frame_count = max(expected_frame_count, last_tracked_frame_count)
    else:
        clip_frame_count = last_tracked_frame_count

    per_track = clip_tracks.group_by("track_id").agg(
        pl.col("frame_index").n_unique().alias("n_frames")
    )
    n_tracks = per_track.height
    longest = int(per_track["n_frames"].max())
    longest_track_frac = longest / clip_frame_count if clip_frame_count else 0.0
    n_fragments = int(
        (per_track["n_frames"] < clip_frame_count * _FRAGMENT_COVERAGE_THRESHOLD).sum()
    )

    return ContinuityRow(
        clip_number=clip_number,
        n_tracks=n_tracks,
        longest_track_frac=round(longest_track_frac, 4),
        n_fragments=n_fragments,
        auto_flag=_clip_flag(n_tracks, n_fragments),
    )


def _validate_verdicts(df: pl.DataFrame, path: Path) -> None:
    """Raise `ValueError` on any `verdict` outside `_VALID_VERDICTS` (`pass`/`fail`/
    empty), naming the CSV and the offending clip numbers. A typo'd verdict (`Pass`,
    `ok`, `yes`) would otherwise count as *reviewed* while landing in neither the pass
    nor the fail bucket -- silently deflating `pass_rate` with no error, the exact
    "quietly wrong headline number" failure mode this module exists to prevent.
    """
    bad = df.filter(~pl.col("verdict").fill_null("").is_in(sorted(_VALID_VERDICTS)))
    if bad.height:
        bad_pairs = [
            f"clip {row['clip_number']}: {row['verdict']!r}"
            for row in bad.iter_rows(named=True)
        ]
        raise ValueError(
            f"invalid verdict(s) in {path} (allowed: 'pass', 'fail', empty): "
            + "; ".join(bad_pairs)
        )


def _load_existing_human_columns(path: Path) -> dict[int, dict]:
    """Read `verdict`/`id_switches`/`reviewer_note` per clip from a previously written
    review CSV, so a re-run of `measure_continuity` never wipes a human's work
    (T-2.1-35). Returns an empty dict when the file does not exist yet or is
    header-only. Raises `ValueError` on a verdict outside `_VALID_VERDICTS` -- a typo
    must never be silently carried forward into the refreshed CSV.
    """
    if not path.exists():
        return {}

    df = pl.read_csv(path, schema_overrides=_REVIEW_SCHEMA)
    if df.height == 0:
        return {}

    _validate_verdicts(df, path)

    preserved: dict[int, dict] = {}
    for row in df.iter_rows(named=True):
        preserved[int(row["clip_number"])] = {
            "verdict": row["verdict"] or "",
            "id_switches": row["id_switches"],
            "reviewer_note": row["reviewer_note"] or "",
        }
    return preserved


def _write_review_csv(rows: list[ContinuityRow], path: Path) -> Path:
    """Write `rows`' auto columns to `path`, preserving any existing
    `verdict`/`id_switches`/`reviewer_note` per clip (T-2.1-35/T-2.1-36) and writing
    atomically (`.tmp` sibling + `os.replace`, matching `schema.write_tracking_parquet`).
    """
    existing = _load_existing_human_columns(path)

    columns: dict[str, list] = {name: [] for name in REVIEW_COLUMNS}
    for row in sorted(rows, key=lambda r: r.clip_number):
        human = existing.get(row.clip_number, {"verdict": "", "id_switches": None, "reviewer_note": ""})
        columns["clip_number"].append(row.clip_number)
        columns["n_tracks"].append(row.n_tracks)
        columns["longest_track_frac"].append(row.longest_track_frac)
        columns["n_fragments"].append(row.n_fragments)
        columns["auto_flag"].append(row.auto_flag)
        columns["verdict"].append(human["verdict"])
        columns["id_switches"].append(human["id_switches"])
        columns["reviewer_note"].append(human["reviewer_note"])

    df = pl.DataFrame(columns, schema=_REVIEW_SCHEMA).select(list(REVIEW_COLUMNS))

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return path


def measure_continuity(
    tracks: pl.DataFrame, config: Config, *, review_csv: Path | None = None
) -> ContinuityResult:
    """Measure per-clip track continuity over `tracks`, writing flagged clips to
    `review_csv` (defaulting to `config.reference.continuity_review`).

    Every clip registered for `config.cv.pilot_session_id` gets a row -- including
    clips with zero confirmed tracks -- because the C-09 continuity criterion's
    denominator is the whole game, not the subset of clips `tracks` happens to cover
    (D-09, T-2.1-31). `verdict`/`id_switches`/`reviewer_note` already present in
    `review_csv` for a clip survive the re-run unchanged; only the auto columns
    (`n_tracks`, `longest_track_frac`, `n_fragments`, `auto_flag`) are refreshed.
    """
    from flag_football_ep.cv import frames as frames_module

    session_id = config.cv.pilot_session_id
    session_clip_numbers = sorted(
        frames_module.clip_number(path)
        for path in frames_module.clip_paths(config, session_id)
    )

    expected_counts = _read_expected_frame_counts(
        config, session_id, set(session_clip_numbers)
    )

    rows: list[ContinuityRow] = []
    notices: list[str] = []
    for clip_num in session_clip_numbers:
        clip_tracks = tracks.filter(pl.col("clip_number") == clip_num)
        expected = expected_counts.get(clip_num)
        if expected is None and clip_tracks.height > 0:
            notices.append(
                f"clip {clip_num}: no inventory duration/fps -- coverage denominator "
                "falls back to the last tracked frame"
            )
        rows.append(_measure_clip(clip_num, clip_tracks, expected_frame_count=expected))

    review_path = Path(review_csv) if review_csv is not None else config.reference.continuity_review
    written = _write_review_csv(rows, review_path)
    summary = summarise_review(written)

    return ContinuityResult(rows=rows, review_csv=written, summary=summary, notices=notices)


def summarise_review(review_csv: Path) -> dict:
    """Roll a (possibly human-annotated) continuity review CSV up into the
    whole-session summary dict reported in the pilot gate doc.

    Returns `{"n_clips", "n_reviewed", "n_pass", "n_fail", "pass_rate",
    "unreviewed_clips"}`. As long as any clip has an empty `verdict` (or the file has
    zero rows), `pass_rate` is `None` and `unreviewed_clips` names the offenders --
    this function must never manufacture a headline pass rate from a partial review,
    because a shrinking denominator is exactly how a false 90%+ claim gets made
    (D-09, T-2.1-31).

    Raises `ValueError` on any verdict outside `_VALID_VERDICTS` (`pass`/`fail`/empty),
    naming the offending clips -- a typo'd verdict must fail loudly, never silently
    skew `pass_rate`.
    """
    review_csv = Path(review_csv)
    df = pl.read_csv(review_csv, schema_overrides=_REVIEW_SCHEMA)

    _validate_verdicts(df, review_csv)

    n_clips = df.height
    verdicts = df["verdict"].fill_null("")
    reviewed_mask = verdicts != ""

    n_reviewed = int(reviewed_mask.sum())
    n_pass = int((verdicts == "pass").sum())
    n_fail = int((verdicts == "fail").sum())
    unreviewed_clips = (
        df.filter(~reviewed_mask)["clip_number"].sort().to_list() if n_clips else []
    )

    pass_rate = None if (n_clips == 0 or unreviewed_clips) else n_pass / n_clips

    return {
        "n_clips": n_clips,
        "n_reviewed": n_reviewed,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "pass_rate": pass_rate,
        "unreviewed_clips": unreviewed_clips,
    }
