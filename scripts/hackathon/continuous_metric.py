"""Label-free continuous metric layer for the hackathon scoring script (METR-01).

Standalone module, same convention as `score_tracks.py` and `baseline_common.py`:
not part of the installed `flag_football_ep` package, English docstrings, `polars`
for I/O, no deep-learning/GPU training framework import anywhere in this module (the
scoring path stays installable and runnable in seconds). The continuity helpers this
module normalises (`_measure_clip`, `summarise_review`) arrive by injection -- every
function that needs one takes it as a parameter -- so this module stays importable
and unit-testable without ever touching `sys.path` for `flag_football_ep` itself.

Two numbers, two very different jobs:

- `fragments_per_expected_player` (`n_fragments / EXPECTED_PLAYERS`) is the ONE
  officially reported continuous number (METR-01). It moves when tracking gets
  better inside a play the pass/fail threshold still marks failed.
- `active_track_count_deviation` is a DIAGNOSTIC guard column, never an acceptance
  criterion -- see `GUARD_NOTE`. It partially catches the over-merge failure mode
  the primary number rewards (fewer simultaneous tracks than expected).

See `BLIND_SPOT_NOTE` for what neither number can see: a silent identity swap during
an overlap, the dominant real failure mode in this dataset (39/46 pilot fails).
`tests/test_m2_metric.py::test_swap_is_invisible_to_both_metrics` makes that ceiling
an executable claim, not a footnote.

Deliberately NOT implemented (RESEARCH.md rejects it as uncalibrated): a
switch-event proximity heuristic (`track A ends at frame f near position p` ->
`track B starts within r px within n frames`) that would flag the
occlusion-then-reacquire failure mode. It needs two tuned parameters with no ground
truth in this phase to calibrate them against, and it cannot see the crossing-swap
failure mode at all (no track ever ends in that case). Left as a documented,
exploratory option only -- not shipped as an official metric.

This module also carries the split reader (`read_split`/`role_violations`, reading
`hackathon_split.csv` directly with `polars` -- never `cv.testset`, which is
concurrently under construction and file-collision-guarded off), a dialect-tolerant
review-CSV reader (`sniff_review_dialect`/`read_review_table`/
`summarise_review_normalized`, handling the real vault file's semicolon delimiter,
cp1252 encoding and CRLF line endings without ever rewriting it), the partial-review
honesty layer (`reviewed_only_rate`), and a pure Markdown renderer
(`render_markdown`) for the report plan M2-04-02's CLI will assemble.
"""

from __future__ import annotations

import io
import sys
import tempfile
from pathlib import Path

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Path-based import precedent mirrored from `baseline_common.py`: pulls in only the
# `k/n (p%)` formatter from the sibling script, never `flag_football_ep` itself.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from score_tracks import _fmt_rate  # noqa: E402

# 5v5, the lower bound of `baseline_common.IDEAL_TRACK_BAND` -- the same constant the
# challenge description already publishes as the ideal player-track count per clip.
EXPECTED_PLAYERS = 10

BLIND_SPOT_NOTE = (
    "Diese Kennzahl misst Track-Abdeckung und Fragmentierung, NICHT Identitaets-"
    "Korrektheit. Ein stiller Identitaetswechsel waehrend einer Ueberlappung -- der "
    "dominante Fehlerfall in diesem Datensatz, 39 von 46 Pilot-Fails -- hinterlaesst "
    "darin keine Spur: kein Track endet, kein Track wird neu geboren, und die Anzahl "
    "gleichzeitig aktiver Tracks aendert sich nicht. Ein Over-Merge (zwei "
    "Spielerinnen unter einer ID) VERBESSERT die Zahl, waehrend die Identitaet "
    "schlechter wird. Die Kennzahl zeigt Fortschritt innerhalb eines gescheiterten "
    "Plays und ersetzt weder das menschliche Urteil noch eine Assoziationsmetrik mit "
    "Identitaets-Labels."
)

GUARD_NOTE = (
    "active_track_count_deviation ist eine diagnostische Kennzahl -- kein "
    "Abnahmekriterium."
)


def player_view(clip_tracks: pl.DataFrame) -> tuple[pl.DataFrame, bool]:
    """Filter `clip_tracks` to `class_name == "player"` rows when the column
    exists (referees must not inflate the count), otherwise return the frame
    unchanged. Returns `(view, class_name_filtered)` -- the bool says which
    happened, since `REQUIRED_TRACK_COLUMNS` does not guarantee `class_name`.
    """
    if "class_name" in clip_tracks.columns:
        return clip_tracks.filter(pl.col("class_name") == "player"), True
    return clip_tracks, False


def active_track_count_deviation(clip_tracks: pl.DataFrame) -> float:
    """Mean over frames of `abs(distinct active track_ids in the frame - EXPECTED_PLAYERS)`.

    An empty clip (no rows at all) returns the full deviation `float(EXPECTED_PLAYERS)`
    -- zero active tracks is not perfect, it is the worst possible reading.
    """
    if clip_tracks.height == 0:
        return float(EXPECTED_PLAYERS)

    counts = clip_tracks.group_by("frame_index").agg(
        pl.col("track_id").n_unique().cast(pl.Int64).alias("n_active")
    )
    deviations = (counts["n_active"] - EXPECTED_PLAYERS).abs()
    return float(deviations.mean())


def clip_metrics(clip_number: int, clip_tracks: pl.DataFrame, measure_clip_fn) -> dict:
    """Apply `player_view`, call `measure_clip_fn` (injected, e.g.
    `cv.continuity._measure_clip`) on the filtered view, and return one clip's row
    for both the primary and the guard metric.
    """
    filtered, class_name_filtered = player_view(clip_tracks)
    result = measure_clip_fn(clip_number, filtered)
    guard = active_track_count_deviation(filtered)

    return {
        "clip_number": result.clip_number,
        "n_tracks": result.n_tracks,
        "longest_track_frac": result.longest_track_frac,
        "n_fragments": result.n_fragments,
        "auto_flag": result.auto_flag,
        "fragments_per_expected_player": round(result.n_fragments / EXPECTED_PLAYERS, 4),
        "active_track_count_deviation": round(guard, 4),
        "class_name_filtered": class_name_filtered,
        "no_tracks": result.n_tracks == 0,
    }


def aggregate(clip_rows: list[dict]) -> dict:
    """Roll `clip_metrics` rows up into session-level means/medians. Every
    aggregate is `None` when `clip_rows` is empty -- never `0.0` posing as a
    measurement over zero clips.
    """
    n_clips = len(clip_rows)
    if n_clips == 0:
        return {
            "n_clips": 0,
            "mean_fragments_per_expected_player": None,
            "median_fragments_per_expected_player": None,
            "mean_active_track_count_deviation": None,
            "n_clips_without_class_name": None,
            "n_clips_without_tracks": None,
        }

    fpp = [row["fragments_per_expected_player"] for row in clip_rows]
    guard = [row["active_track_count_deviation"] for row in clip_rows]
    n_clips_without_class_name = sum(1 for row in clip_rows if not row["class_name_filtered"])
    n_clips_without_tracks = sum(1 for row in clip_rows if row["no_tracks"])

    return {
        "n_clips": n_clips,
        "mean_fragments_per_expected_player": round(float(np.mean(fpp)), 4),
        "median_fragments_per_expected_player": round(float(np.median(fpp)), 4),
        "mean_active_track_count_deviation": round(float(np.mean(guard)), 4),
        "n_clips_without_class_name": n_clips_without_class_name,
        "n_clips_without_tracks": n_clips_without_tracks,
    }


# --- Split handling ------------------------------------------------------------

# The 6-column contract of `data/reference/hackathon_split.csv` (plan 02.2-21),
# mirroring the `REQUIRED_TRACK_COLUMNS` pattern in `score_tracks.py`. Read directly
# with `polars` here -- never via `cv.testset.read_hackathon_split`, which is
# concurrently under construction and off-limits per this plan's file-collision
# guard.
SPLIT_COLUMNS: tuple[str, ...] = (
    "domain",
    "session_id",
    "clip_number",
    "hackathon_role",
    "frozen_at",
    "note",
)

VALID_ROLES: tuple[str, ...] = ("dev", "private_test")


class SplitSchemaError(ValueError):
    """A split CSV is missing a required column or names a `hackathon_role` value
    outside `VALID_ROLES`.
    """


def read_split(path: Path) -> dict[tuple[str, int], str]:
    """Read `hackathon_split.csv`-shaped `path` and return `(session_id,
    clip_number) -> hackathon_role`. Raises `SplitSchemaError` on a missing column
    or a role value outside `VALID_ROLES` -- both are refused loudly, never
    silently coerced.
    """
    path = Path(path)
    df = pl.read_csv(path)

    missing = [c for c in SPLIT_COLUMNS if c not in df.columns]
    if missing:
        raise SplitSchemaError(f"{path}: fehlende Pflichtspalte(n): {', '.join(missing)}")

    bad_roles = df.filter(~pl.col("hackathon_role").is_in(list(VALID_ROLES)))
    if bad_roles.height:
        offending = sorted(bad_roles["hackathon_role"].unique().to_list())
        raise SplitSchemaError(
            f"{path}: ungueltige hackathon_role-Werte (erlaubt: {VALID_ROLES}): {offending}"
        )

    return {
        (row["session_id"], int(row["clip_number"])): row["hackathon_role"]
        for row in df.iter_rows(named=True)
    }


def role_violations(
    tracks_df: pl.DataFrame, split_map: dict[tuple[str, int], str], expected_role: str
) -> list[str]:
    """Return a sorted list of human-readable German messages, one per distinct
    `(session_id, clip_number)` in `tracks_df` that either does not appear in
    `split_map` at all (unknown pair) or appears under a different role than
    `expected_role` (wrong role) -- the two cases get distinct messages. Returns
    messages rather than raising, so the caller stays in charge of the exit code
    (mirrors `score_tracks.py`'s own error-printing convention).
    """
    pairs = (
        tracks_df.select("session_id", "clip_number").unique().sort(["session_id", "clip_number"])
    )

    messages: list[str] = []
    for row in pairs.iter_rows(named=True):
        key = (row["session_id"], int(row["clip_number"]))
        role = split_map.get(key)
        if role is None:
            messages.append(
                f"{key[0]} Clip {key[1]}: unbekannt in hackathon_split.csv "
                "(kein Eintrag fuer dieses (session_id, clip_number)-Paar)"
            )
        elif role != expected_role:
            messages.append(
                f"{key[0]} Clip {key[1]}: hackathon_role={role!r}, erwartet {expected_role!r}"
            )

    return sorted(messages)


# --- Vault-dialect review reading ------------------------------------------------

# Mirrors `cv.continuity._REVIEW_SCHEMA`/`REVIEW_COLUMNS` exactly (per this plan's
# <interfaces> block) -- duplicated here rather than imported, because this module
# never imports `flag_football_ep` at module level (the continuity helpers arrive
# by injection instead).
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

_REVIEW_DIALECT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "clip_number": pl.Int32,
    "n_tracks": pl.Int32,
    "longest_track_frac": pl.Float64,
    "n_fragments": pl.Int32,
    "auto_flag": pl.Utf8,
    "verdict": pl.Utf8,
    "id_switches": pl.Int32,
    "reviewer_note": pl.Utf8,
}

# Encodings tried in order when sniffing a review CSV's dialect. UTF-8 first (the
# project's own `_write_review_csv` output), cp1252 second (the real vault file's
# encoding -- common in German-locale spreadsheet exports).
_REVIEW_ENCODINGS: tuple[str, ...] = ("utf-8", "cp1252")


class ReviewDialectError(ValueError):
    """A review CSV's bytes decode as neither UTF-8 nor cp1252."""


def sniff_review_dialect(path: Path) -> tuple[str, str]:
    """Read `path`'s raw bytes, decode with UTF-8 then cp1252 (raising
    `ReviewDialectError` if neither works), and pick the delimiter by comparing
    semicolon and comma counts in the decoded header line. Returns `(delimiter,
    encoding)`.
    """
    path = Path(path)
    raw = path.read_bytes()

    text: str | None = None
    used_encoding: str | None = None
    for encoding in _REVIEW_ENCODINGS:
        try:
            text = raw.decode(encoding)
            used_encoding = encoding
            break
        except UnicodeDecodeError:
            continue

    if text is None or used_encoding is None:
        raise ReviewDialectError(
            f"{path}: weder UTF-8 noch cp1252 dekodierbar -- Dialekt nicht erkennbar"
        )

    header_line = text.splitlines()[0] if text else ""
    delimiter = ";" if header_line.count(";") > header_line.count(",") else ","

    return delimiter, used_encoding


def read_review_table(path: Path) -> pl.DataFrame:
    """Read a `continuity_review.csv`-shaped file at `path` using whatever dialect
    `sniff_review_dialect` detects (delimiter + encoding), tolerating CRLF line
    endings. The vault file itself is opened read-only and never modified.
    """
    path = Path(path)
    delimiter, encoding = sniff_review_dialect(path)
    text = path.read_bytes().decode(encoding)

    df = pl.read_csv(
        io.StringIO(text),
        separator=delimiter,
        schema_overrides=_REVIEW_DIALECT_SCHEMA,
    )

    last_column = REVIEW_COLUMNS[-1]
    if last_column in df.columns:
        # Defensive: some CSV readers leave a trailing carriage return on the last
        # field of a CRLF-terminated row. Strip it if present; a no-op otherwise.
        df = df.with_columns(pl.col(last_column).str.strip_chars("\r"))

    return df


def summarise_review_normalized(path: Path, summarise_review_fn) -> dict:
    """Summarise a review CSV of ANY dialect (comma/UTF-8 or the vault's
    semicolon/cp1252/CRLF shape) via the injected `summarise_review_fn` (e.g.
    `cv.continuity.summarise_review`), which only understands comma/UTF-8.

    Short-circuits to `summarise_review_fn(path)` unchanged when the dialect is
    already comma/UTF-8. Otherwise reads the file with `read_review_table`, writes
    a comma/UTF-8 normalised copy into a fresh `tempfile.TemporaryDirectory()` (OS
    temp directory, never inside this repository, never below `data/`), calls
    `summarise_review_fn` on that copy, and lets the context manager delete the
    directory before returning -- the vault file itself is opened read-only and is
    never written to, and no normalised copy ever survives the call or ever lives
    inside the repository tree.
    """
    path = Path(path)
    delimiter, encoding = sniff_review_dialect(path)

    if delimiter == "," and encoding == "utf-8":
        return summarise_review_fn(path)

    df = read_review_table(path)
    with tempfile.TemporaryDirectory() as tmp_dir:
        normalized_path = Path(tmp_dir) / path.name
        df.write_csv(normalized_path)
        return summarise_review_fn(normalized_path)


# --- Partial-review honesty layer ------------------------------------------------

PARTIAL_REVIEW_LABEL = (
    "Teil-Review (nur gepruefte Clips, NICHT die offizielle Referenz-Baseline)"
)


def reviewed_only_rate(summary: dict) -> dict:
    """Turn a `summarise_review`-shaped `summary` dict into
    `{"k", "n", "complete", "note"}` over the REVIEWED-ONLY denominator -- never
    the full clip count. Never writes into or overwrites `summary["pass_rate"]`; a
    partially reviewed file keeps `pass_rate=None` from `summarise_review`
    untouched. `complete` is `True` only when every clip has a verdict; `note`
    carries the German "unvollstaendig (k/n geprueft)" wording whenever it is not.
    """
    unreviewed_clips = summary["unreviewed_clips"]
    complete = not unreviewed_clips
    note = None
    if not complete:
        note = f"unvollstaendig ({summary['n_reviewed']}/{summary['n_clips']} geprueft)"

    return {
        "k": summary["n_pass"],
        "n": summary["n_reviewed"],
        "complete": complete,
        "note": note,
    }


# --- Rendering ---------------------------------------------------------------


def fmt_rate(k: int, n: int) -> str:
    """Delegate to `score_tracks._fmt_rate` so the `k/n (p%)` convention has
    exactly one implementation in the repository.
    """
    return _fmt_rate(k, n)


def render_markdown(report: dict) -> str:
    """Render `report` as a compact Markdown report: one heading, one table (columns
    Split, n, Human-Schwelle, Stetige Kennzahl (Fragmente je erwarteter Spielerin),
    Guard-Kennzahl (diagnostisch)), the `BLIND_SPOT_NOTE` paragraph, and -- for
    every split whose reviewed-only rate is incomplete -- a line naming
    `PARTIAL_REVIEW_LABEL` and that split's own `k/n`.

    `report` is shaped `{"splits": {split_name: {"n": int, "human_rate":
    {"k": int, "n": int} | None, "mean_fragments_per_expected_player": float,
    "mean_active_track_count_deviation": float, "reviewed_only":
    {"k", "n", "complete", "note"} | None}}}` -- this is a forward contract for
    plan M2-04-02's CLI wiring (the JSON report it will write), unit-tested here
    without needing a subprocess. Pure f-string based, no templating dependency.
    """
    lines = [
        "# Messvorschrift-Bericht",
        "",
        "| Split | n | Human-Schwelle | Stetige Kennzahl (Fragmente je erwarteter "
        "Spielerin) | Guard-Kennzahl (diagnostisch) |",
        "|---|---|---|---|---|",
    ]

    partial_lines: list[str] = []
    for split_name, split in report["splits"].items():
        human_rate = split.get("human_rate")
        human_str = fmt_rate(human_rate["k"], human_rate["n"]) if human_rate is not None else "n/a"
        fpp = split["mean_fragments_per_expected_player"]
        guard = split["mean_active_track_count_deviation"]
        lines.append(f"| {split_name} | {split['n']} | {human_str} | {fpp:.4f} | {guard:.4f} |")

        reviewed_only = split.get("reviewed_only")
        if reviewed_only is not None and not reviewed_only["complete"]:
            partial_lines.append(
                f"- **{split_name}**: {PARTIAL_REVIEW_LABEL} -- "
                f"{fmt_rate(reviewed_only['k'], reviewed_only['n'])} ({reviewed_only['note']})"
            )

    lines.append("")
    lines.append(BLIND_SPOT_NOTE)

    if partial_lines:
        lines.append("")
        lines.extend(partial_lines)

    return "\n".join(lines) + "\n"
