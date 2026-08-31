"""Schema and completeness gates over the two hackathon benchmark label tables
(`data/reference/continuity_review.csv`, `data/reference/flag_pull_events.csv`), guarding
plan 02.2-03's labelling session the way `tests/test_capture_artifacts.py` guards the
Phase 2.0 capture CSVs.

Three gates, mirroring the plan's `<action>` spec:

1. `continuity_review.csv` has exactly 61 rows with unique `clip_number` 1..61.
2. Every `verdict` is in `{"pass", "fail", ""}` (the pre-checkpoint state legitimately
   carries empty strings for unreviewed clips -- see `test_cv_continuity.py` /
   `test_cv_gate_artifacts.py` for the "never manufacture a headline rate from a partial
   review" contract this file does not duplicate). `test_n_reviewed_count_is_explicit`
   asserts and prints the exact `n_reviewed` count the plan's SUMMARY quotes.
3. `flag_pull_events.csv` has 61 unique `clip_number` rows; every non-empty `outcome` is
   in the fixed vocabulary; `pull_time_s` parses as a float; every `outcome == "pull"` row
   carries a non-empty `pull_time_s`.

T-2.2-07 (Information Disclosure, threat register): `reviewer_note`/`notes` must never
contain a roster player name -- mirrors
`test_capture_artifacts.py::test_capture_artifacts_contain_no_roster_names`.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.cv.continuity import REVIEW_COLUMNS, summarise_review

REPO_ROOT = Path(__file__).resolve().parent.parent
REFERENCE_DIR = REPO_ROOT / "data" / "reference"
CONTINUITY_CSV = REFERENCE_DIR / "continuity_review.csv"
FLAG_PULL_CSV = REFERENCE_DIR / "flag_pull_events.csv"
ROSTER_CSV = REFERENCE_DIR / "roster.csv"

N_CLIPS = 61

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

FLAG_PULL_COLUMNS: tuple[str, ...] = (
    "clip_number",
    "outcome",
    "pull_time_s",
    "carrier_track_id",
    "puller_track_id",
    "notes",
)

_FLAG_PULL_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "outcome": pl.Utf8,
    "pull_time_s": pl.Float64,
    "carrier_track_id": pl.Int32,
    "puller_track_id": pl.Int32,
    "notes": pl.Utf8,
}

_VALID_VERDICTS = frozenset({"pass", "fail", ""})
_OUTCOME_VOCABULARY = frozenset({"pull", "incomplete", "out_of_bounds", "touchdown", "other"})


def _continuity_df() -> pl.DataFrame:
    return pl.read_csv(CONTINUITY_CSV, schema_overrides=_REVIEW_SCHEMA)


def _flag_pull_df() -> pl.DataFrame:
    return pl.read_csv(FLAG_PULL_CSV, schema_overrides=_FLAG_PULL_SCHEMA)


# --- Gate 1: continuity_review.csv shape -----------------------------------------


def test_continuity_review_csv_has_exact_header() -> None:
    raw = CONTINUITY_CSV.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf"), f"{CONTINUITY_CSV} starts with a BOM"
    first_line = raw.decode("utf-8").splitlines()[0]
    actual = tuple(first_line.split(","))
    assert actual == REVIEW_COLUMNS, f"{CONTINUITY_CSV} header is {actual!r}"


def test_continuity_review_csv_has_61_unique_clip_numbers() -> None:
    df = _continuity_df()
    assert df.height == N_CLIPS, f"expected {N_CLIPS} rows, found {df.height}"

    clip_numbers = df["clip_number"].to_list()
    assert set(clip_numbers) == set(range(1, N_CLIPS + 1)), (
        f"clip_number does not cover exactly 1..{N_CLIPS}: {sorted(set(clip_numbers))}"
    )
    dupes = (
        df.group_by("clip_number")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)["clip_number"]
        .to_list()
    )
    assert not dupes, f"duplicate clip_number in {CONTINUITY_CSV}: {dupes}"


# --- Gate 2: verdict vocabulary + explicit n_reviewed -----------------------------


def test_continuity_review_verdict_vocabulary() -> None:
    """Tolerates empty strings pre-checkpoint -- completeness is a separate assertion
    (`test_n_reviewed_count_is_explicit`), not conflated with vocabulary validity.
    """
    df = _continuity_df()
    verdicts = set(df["verdict"].fill_null("").to_list())
    assert verdicts <= _VALID_VERDICTS, f"unexpected verdict values: {verdicts - _VALID_VERDICTS}"


def test_n_reviewed_count_is_explicit() -> None:
    """Asserts and surfaces the exact `n_reviewed` count the plan's SUMMARY quotes.

    Pre-checkpoint this legitimately reports a partial count (20/61 at plan start); the
    assertion only pins the *shape* of the summary dict, not full completeness -- Task 2
    (the human-verify checkpoint) is what drives `n_reviewed` to 61.
    """
    summary = summarise_review(CONTINUITY_CSV)
    assert summary["n_clips"] == N_CLIPS
    assert 0 <= summary["n_reviewed"] <= N_CLIPS
    assert summary["n_reviewed"] == summary["n_pass"] + summary["n_fail"]
    print(
        f"n_reviewed={summary['n_reviewed']}/{N_CLIPS} "
        f"(n_pass={summary['n_pass']}, n_fail={summary['n_fail']})"
    )


def test_every_fail_verdict_has_a_reviewer_note() -> None:
    df = _continuity_df()
    fails_without_note = df.filter(
        (pl.col("verdict") == "fail")
        & (pl.col("reviewer_note").fill_null("").str.strip_chars() == "")
    )["clip_number"].to_list()
    assert not fails_without_note, (
        f"fail verdict without a reviewer_note for clip(s): {fails_without_note}"
    )


# --- Gate 3: flag_pull_events.csv shape -------------------------------------------


def test_flag_pull_events_csv_has_exact_header() -> None:
    raw = FLAG_PULL_CSV.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf"), f"{FLAG_PULL_CSV} starts with a BOM"
    first_line = raw.decode("utf-8").splitlines()[0]
    actual = tuple(first_line.split(","))
    assert actual == FLAG_PULL_COLUMNS, f"{FLAG_PULL_CSV} header is {actual!r}"


def test_flag_pull_events_csv_has_61_unique_clip_numbers() -> None:
    df = _flag_pull_df()
    assert df.height == N_CLIPS, f"expected {N_CLIPS} rows, found {df.height}"

    clip_numbers = df["clip_number"].to_list()
    assert set(clip_numbers) == set(range(1, N_CLIPS + 1)), (
        f"clip_number does not cover exactly 1..{N_CLIPS}: {sorted(set(clip_numbers))}"
    )
    dupes = (
        df.group_by("clip_number")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)["clip_number"]
        .to_list()
    )
    assert not dupes, f"duplicate clip_number in {FLAG_PULL_CSV}: {dupes}"


def test_flag_pull_events_outcome_vocabulary() -> None:
    df = _flag_pull_df()
    outcomes = set(df["outcome"].fill_null("").to_list()) - {""}
    assert outcomes <= _OUTCOME_VOCABULARY, (
        f"unexpected outcome values: {outcomes - _OUTCOME_VOCABULARY}"
    )


def test_flag_pull_events_pull_time_s_parses_as_float() -> None:
    # schema_overrides above already enforces Float64 at read time; this asserts the
    # dtype survived (a stray non-numeric string would raise at read_csv instead of
    # silently coercing, but pin the contract explicitly for a future schema change).
    df = _flag_pull_df()
    assert df.schema["pull_time_s"] == pl.Float64, df.schema["pull_time_s"]


def test_flag_pull_events_pull_outcome_has_pull_time_s() -> None:
    df = _flag_pull_df()
    missing_time = df.filter(
        (pl.col("outcome") == "pull") & pl.col("pull_time_s").is_null()
    )["clip_number"].to_list()
    assert not missing_time, (
        f"outcome == 'pull' without a pull_time_s for clip(s): {missing_time}"
    )


# --- T-2.2-07: no roster names in free-text label columns -------------------------


def test_label_notes_contain_no_roster_names() -> None:
    if not ROSTER_CSV.exists():
        pytest.skip(f"{ROSTER_CSV} does not exist")

    roster = pl.read_csv(ROSTER_CSV)
    names = [name for name in roster["player_name"].unique().to_list() if name]

    for path, column in (
        (CONTINUITY_CSV, "reviewer_note"),
        (FLAG_PULL_CSV, "notes"),
    ):
        df = pl.read_csv(path, schema_overrides=_REVIEW_SCHEMA if path == CONTINUITY_CSV else _FLAG_PULL_SCHEMA)
        text = " ".join(df[column].fill_null("").to_list()).lower()
        for name in names:
            assert name.lower() not in text, f"{path} column {column!r} contains roster name {name!r}"
