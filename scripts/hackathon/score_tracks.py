#!/usr/bin/env python3
"""Hackathon scoring script: turns a team's submitted tracks into the continuity
metric (and, optionally, the flag-pull bonus metric) against the shipped human
benchmark. The single script every team is measured with
(`docs/hackathon-challenge-reid.md`: "Bewertung ausschliesslich mit den
bereitgestellten Skripten (Kontinuitaets-Metrik, Flag-Pull-Metrik), damit alle Teams
dieselbe Zahl messen").

Standalone script, not part of the installed `flag_football_ep` package (same
convention as `scripts/clip_alignment_diagnostics.py`): pulls in `flag_football_ep`
only for the continuity fragment logic (`cv.continuity._measure_clip`,
`cv.continuity.summarise_review`) -- never a reimplementation of that logic -- and
never reaches into any project-internal `Config`/`data/reference` path on its own:
every input is an explicit CLI path, so the script never reads label data beyond what
`--review`/`--review-dev`/`--review-test`/`--flag-pulls` name. The private test
set's labels are never shipped in the test bundle, so they are simply never on a
team's disk to accidentally read; `--review-test` is the ONLY way private labels
enter this script -- it is never derived from `--split` or any other flag, so
scoring the dev split never accidentally opens a vault path.

Two run modes, selected by which flags are given:

Einzel-Modus (unchanged, still the legacy contract `baseline_common.py`'s
`score_with_shared_harness()`/`summarise()` depend on):

    uv run python scripts/hackathon/score_tracks.py \\
        --tracks my_tracks.csv --review data/continuity_review.csv \\
        [--flag-pulls my_pull_events.csv] [--out report.json] [--out-md report.md]

Split-Modus (METR-02: one run, both splits, threshold rate AND continuous metric):

    uv run python scripts/hackathon/score_tracks.py \\
        --tracks-dev dev_tracks.parquet --review-dev data/reference/continuity_review.csv \\
        --tracks-test test_tracks.parquet --review-test data/private/test-labels/<session>/continuity_review.csv \\
        --split data/reference/hackathon_split.csv \\
        --out report.json --out-md report.md

`--tracks`/`--review`/`--flag-pulls` (Einzel-Modus) and `--tracks-dev`/
`--tracks-test`/`--split` (Split-Modus) are mutually exclusive; the flag-pull bonus
stays Einzel-Modus-only in this phase. Split-Modus additionally validates every
`(session_id, clip_number)` pair in each tracks file against
`data/reference/hackathon_split.csv`'s `hackathon_role` column and refuses (exit 1,
naming the offending session/clip rows) to score a clip under the wrong role.

Every printed rate is reported as `k/n (p%)` -- the project's statistical-honesty
convention, never a bare percentage without its denominator. In Split-Modus, per-clip
lines are NOT printed (61 x 2 lines of noise per split); only the aggregate lines are
printed per split. Einzel-Modus keeps printing one line per clip, unchanged.

Continuity denominator caveat: without access to `video_inventory.csv` (a
project-internal file this standalone script deliberately never reads), the clip's
real duration is unknown, so `--tracks`'s own last tracked frame is the coverage
denominator (the same fallback `cv.continuity.measure_continuity` itself uses when no
inventory duration is available). This is printed once, not hidden.

Flag-pull bonus caveat: the +-0.5s time window is always evaluated. The ~2-yard
location window is only evaluated when both the ground-truth (`flag_pull_events.csv`,
found next to `--review`) and the predicted (`--flag-pulls`) rows carry OPTIONAL
`x_yards`/`y_yards` columns -- neither the shipped `flag_pull_events.csv` schema nor
the team submission schema guarantees them. When absent, a time-only match is
reported with an explicit caveat, never a fabricated distance number.

Continuous-metric blind spot: every continuous number printed (stdout, JSON and
Markdown) is accompanied by `continuous_metric.BLIND_SPOT_NOTE` -- the metric measures
track coverage and fragmentation, NOT identity correctness; it cannot see a silent
identity swap during an overlap. See `scripts/hackathon/continuous_metric.py` for the
full text and the executable proof (`tests/test_m2_metric.py::
test_swap_is_invisible_to_both_metrics`).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

REQUIRED_TRACK_COLUMNS: tuple[str, ...] = (
    "session_id",
    "clip_number",
    "frame_index",
    "track_id",
    "bbox_x1",
    "bbox_y1",
    "bbox_x2",
    "bbox_y2",
)

_TIME_TOLERANCE_S = 0.5
_DISTANCE_TOLERANCE_YARDS = 2.0


def _fmt_rate(k: int, n: int) -> str:
    pct = (k / n * 100) if n else 0.0
    return f"{k}/{n} ({pct:.2f}%)"


# `continuous_metric.py` (plan M2-04-01) imports `_fmt_rate` from this module by the
# same path-based-import convention `baseline_common.py` already uses for this
# module's own names. When this script runs as `__main__` (the normal team-facing
# invocation), `sys.modules` has no "score_tracks" entry yet -- without the alias
# below, `continuous_metric`'s `from score_tracks import _fmt_rate` would trigger a
# SECOND, independent execution of this entire file under the module name
# "score_tracks", racing this file's own not-yet-defined names against the nested
# import. Aliasing "score_tracks" to the already-running module here -- AFTER
# `_fmt_rate` is defined, BEFORE `continuous_metric` is imported -- means the nested
# import finds a module that already has the one name it needs, so no second
# execution of this file ever happens. When this module is instead imported BY NAME
# (e.g. by `baseline_common.py`, or transitively via `continuous_metric.py` being
# imported first), Python has already registered "score_tracks" in `sys.modules`
# before running this file's body (standard circular-import handling), so the `if`
# below is a no-op in that case.
if "score_tracks" not in sys.modules:
    sys.modules["score_tracks"] = sys.modules[__name__]

sys.path.insert(0, str(Path(__file__).resolve().parent))
import continuous_metric as cm  # noqa: E402


def _load_continuity_helpers():
    """Import `_measure_clip`/`summarise_review` from the project package, adding
    `src/` to `sys.path` first (mirrors `scripts/clip_alignment_diagnostics.py`'s own
    `_load()` -- this script is standalone, not the installed package, but reuses the
    project's own fragment-continuity logic rather than reimplementing it).
    """
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from flag_football_ep.cv.continuity import _measure_clip, summarise_review

    return _measure_clip, summarise_review


def _read_table(path: Path) -> pl.DataFrame:
    if not path.exists():
        print(f"FEHLER: Datei nicht gefunden: {path}", file=sys.stderr)
        sys.exit(1)
    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    return pl.read_csv(path)


def _validate_tracks_schema(df: pl.DataFrame) -> None:
    missing = [c for c in REQUIRED_TRACK_COLUMNS if c not in df.columns]
    if missing:
        print(
            "FEHLER: --tracks fehlt Pflichtspalte(n): " + ", ".join(missing),
            file=sys.stderr,
        )
        sys.exit(1)


def _clip_numbers_from_review(review_df: pl.DataFrame) -> list[int]:
    return sorted(review_df["clip_number"].to_list())


def _run_automatic_continuity(
    tracks_df: pl.DataFrame, clip_numbers: list[int], measure_clip_fn
) -> list[dict]:
    rows: list[dict] = []
    for n in clip_numbers:
        clip_tracks = tracks_df.filter(pl.col("clip_number") == n)
        result = measure_clip_fn(n, clip_tracks)
        rows.append(
            {
                "clip_number": result.clip_number,
                "n_tracks": result.n_tracks,
                "longest_track_frac": result.longest_track_frac,
                "n_fragments": result.n_fragments,
                "auto_flag": result.auto_flag,
            }
        )
    return rows


def _match_pulls(predicted: list[dict], truth: list[dict]) -> dict:
    """Greedy nearest-in-time match per clip within `_TIME_TOLERANCE_S`, additionally
    gated by `_DISTANCE_TOLERANCE_YARDS` when both sides carry `x_yards`/`y_yards`.
    Returns `{tp, fp, fn, precision, recall, location_evaluated}`.
    """
    truth_by_clip: dict[int, list[dict]] = {}
    for row in truth:
        truth_by_clip.setdefault(row["clip_number"], []).append(row)

    location_evaluated = any(
        row.get("x_yards") is not None and row.get("y_yards") is not None for row in truth
    ) and any(row.get("x_yards") is not None and row.get("y_yards") is not None for row in predicted)

    matched_truth: set[tuple[int, int]] = set()
    tp = 0
    for p in predicted:
        candidates = truth_by_clip.get(p["clip_number"], [])
        for i, t in enumerate(candidates):
            key = (p["clip_number"], i)
            if key in matched_truth:
                continue
            if p.get("pull_time_s") is None or t.get("pull_time_s") is None:
                continue
            if abs(p["pull_time_s"] - t["pull_time_s"]) > _TIME_TOLERANCE_S:
                continue
            if location_evaluated:
                px, py = p.get("x_yards"), p.get("y_yards")
                tx, ty = t.get("x_yards"), t.get("y_yards")
                if None in (px, py, tx, ty):
                    continue
                distance = ((px - tx) ** 2 + (py - ty) ** 2) ** 0.5
                if distance > _DISTANCE_TOLERANCE_YARDS:
                    continue
            matched_truth.add(key)
            tp += 1
            break

    fp = len(predicted) - tp
    fn = len(truth) - tp
    precision = (tp / (tp + fp)) if (tp + fp) else None
    recall = (tp / (tp + fn)) if (tp + fn) else None
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "location_evaluated": location_evaluated,
    }


def _score_one(
    tracks_df: pl.DataFrame,
    *,
    review_path: Path | None,
    review_df: pl.DataFrame | None,
    measure_clip_fn,
    summarise_review_fn,
    dialect_tolerant: bool,
) -> dict:
    """Score ONE split/session: the legacy automatic-continuity block (computed over
    ALL tracks, unfiltered, unchanged from before this plan) plus the continuous and
    guard metric blocks (computed over the player view via `continuous_metric.
    clip_metrics`), plus the human reference block(s) when a review path is given.
    Returns a dict with exactly `n_clips`, `per_clip`, `auto`, `human_reference`,
    `human_reference_reviewed_only`, `continuous`, `guard` -- the full internal shape;
    callers decide which subset to expose in a given output.

    `dialect_tolerant=True` reads the review file through `continuous_metric`'s
    dialect-sniffing reader (comma/UTF-8 or the vault's semicolon/cp1252/CRLF shape,
    read-only, never rewritten in place) -- used for Split-Modus so `--review-test`
    can point directly at the vault file. `dialect_tolerant=False` reads the review
    file exactly as before this plan (plain comma/UTF-8 `pl.read_csv`) -- used for
    Einzel-Modus so the legacy report stays byte-identical for the same input.
    """
    _validate_tracks_schema(tracks_df)

    if review_df is not None:
        clip_numbers = _clip_numbers_from_review(review_df)
    else:
        clip_numbers = sorted(tracks_df["clip_number"].unique().to_list())

    per_clip = _run_automatic_continuity(tracks_df, clip_numbers, measure_clip_fn)
    n_clips = len(per_clip)
    n_auto_ok = sum(1 for row in per_clip if row["auto_flag"] == "ok")
    auto_block = {
        "n_ok": n_auto_ok,
        "n_clips": n_clips,
        "rate": (n_auto_ok / n_clips) if n_clips else None,
    }

    continuous_rows = [
        cm.clip_metrics(n, tracks_df.filter(pl.col("clip_number") == n), measure_clip_fn)
        for n in clip_numbers
    ]
    continuous_agg = cm.aggregate(continuous_rows)
    continuous_block = {
        "n_clips": continuous_agg["n_clips"],
        "mean_fragments_per_expected_player": continuous_agg["mean_fragments_per_expected_player"],
        "median_fragments_per_expected_player": continuous_agg["median_fragments_per_expected_player"],
        "n_clips_without_class_name": continuous_agg["n_clips_without_class_name"],
        "n_clips_without_tracks": continuous_agg["n_clips_without_tracks"],
        "per_clip": continuous_rows,
    }
    guard_block = {
        "n_clips": continuous_agg["n_clips"],
        "mean_active_track_count_deviation": continuous_agg["mean_active_track_count_deviation"],
    }

    human_reference = None
    human_reference_reviewed_only = None
    if review_path is not None:
        summary = (
            cm.summarise_review_normalized(review_path, summarise_review_fn)
            if dialect_tolerant
            else summarise_review_fn(review_path)
        )
        human_reference = summary
        human_reference_reviewed_only = cm.reviewed_only_rate(summary)

    return {
        "n_clips": n_clips,
        "per_clip": per_clip,
        "auto": auto_block,
        "human_reference": human_reference,
        "human_reference_reviewed_only": human_reference_reviewed_only,
        "continuous": continuous_block,
        "guard": guard_block,
    }


def _print_result(
    label: str,
    result: dict,
    *,
    verbose_per_clip: bool,
    human_rate_label: str,
    review_flag_name: str,
) -> None:
    n_clips = result["n_clips"]
    print(f"=== Split: {label} (n={n_clips}) ===")

    if verbose_per_clip:
        for row in result["per_clip"]:
            print(
                f"Clip {row['clip_number']:>3}: n_tracks={row['n_tracks']} "
                f"longest_track_frac={row['longest_track_frac']:.2f} "
                f"n_fragments={row['n_fragments']} auto_flag={row['auto_flag']}"
            )

    auto = result["auto"]
    print(f"Automatische Kontinuitaet (auto_flag=ok): {_fmt_rate(auto['n_ok'], auto['n_clips'])}")

    human = result["human_reference"]
    if human is not None:
        if human["pass_rate"] is not None:
            print(f"{human_rate_label}: {_fmt_rate(human['n_pass'], human['n_clips'])}")
        else:
            print(
                "Referenz-Baseline: nicht auswertbar -- "
                f"{review_flag_name} enthaelt unbewertete Clips: {human['unreviewed_clips']}"
            )
        reviewed_only = result["human_reference_reviewed_only"]
        if reviewed_only is not None and not reviewed_only["complete"]:
            print(
                f"{cm.PARTIAL_REVIEW_LABEL}: "
                f"{_fmt_rate(reviewed_only['k'], reviewed_only['n'])} ({reviewed_only['note']})"
            )

    continuous = result["continuous"]
    print(
        "Stetige Kennzahl (Fragmente je erwarteter Spielerin): "
        f"mean={continuous['mean_fragments_per_expected_player']:.4f} "
        f"median={continuous['median_fragments_per_expected_player']:.4f}"
    )
    guard = result["guard"]
    print(
        "Guard-Kennzahl (active_track_count_deviation, diagnostisch): "
        f"{guard['mean_active_track_count_deviation']:.4f} -- {cm.GUARD_NOTE}"
    )
    print(cm.BLIND_SPOT_NOTE)


def _to_markdown_report(splits: dict[str, dict]) -> dict:
    """Adapt this script's internal per-split result dicts (`_score_one`'s return
    shape) into the `{"splits": {name: {"n", "human_rate",
    "mean_fragments_per_expected_player", "mean_active_track_count_deviation",
    "reviewed_only"}}}` shape `continuous_metric.render_markdown` expects (a forward
    contract fixed by plan M2-04-01). The renderer itself stays untouched -- only
    this call site adapts.
    """
    md_splits: dict[str, dict] = {}
    for name, split in splits.items():
        human = split["human_reference"]
        human_rate = None
        if human is not None and human.get("pass_rate") is not None:
            human_rate = {"k": human["n_pass"], "n": human["n_clips"]}
        md_splits[name] = {
            "n": split["n_clips"],
            "human_rate": human_rate,
            "mean_fragments_per_expected_player": split["continuous"]["mean_fragments_per_expected_player"],
            "mean_active_track_count_deviation": split["guard"]["mean_active_track_count_deviation"],
            "reviewed_only": split["human_reference_reviewed_only"],
        }
    return {"splits": md_splits}


def _write_out(report: dict, out_path: Path | None, out_md_path: Path | None, md_splits: dict[str, dict]) -> None:
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"Bericht geschrieben: {out_path}")

    if out_md_path is not None:
        out_md_path.parent.mkdir(parents=True, exist_ok=True)
        markdown = cm.render_markdown(_to_markdown_report(md_splits))
        out_md_path.write_text(markdown, encoding="utf-8")
        print(f"Markdown-Bericht geschrieben: {out_md_path}")


def _run_single_mode(args: argparse.Namespace, measure_clip_fn, summarise_review_fn) -> int:
    tracks_df = _read_table(args.tracks)
    _validate_tracks_schema(tracks_df)

    print(
        "Hinweis: keine video_inventory.csv verfuegbar -- Abdeckung wird gegen den "
        "letzten getrackten Frame gemessen, nicht gegen die echte Clip-Laenge."
    )

    review_df = None
    if args.review is not None:
        review_df = _read_table(args.review)
    else:
        print(
            "Hinweis: kein --review angegeben -- Clip-Liste aus --tracks abgeleitet, "
            "keine menschliche Referenz-Baseline verfuegbar.",
        )

    result = _score_one(
        tracks_df,
        review_path=args.review,
        review_df=review_df,
        measure_clip_fn=measure_clip_fn,
        summarise_review_fn=summarise_review_fn,
        dialect_tolerant=False,
    )

    _print_result(
        "gesamt",
        result,
        verbose_per_clip=True,
        human_rate_label="Referenz-Baseline (Human-Urteile, aus --review)",
        review_flag_name="--review",
    )

    # LEGACY top-level keys, byte-identical values to before this plan for the same
    # input, plus the new `continuous`, `guard` and `blind_spot` keys.
    report: dict = {
        "n_clips": result["n_clips"],
        "per_clip": result["per_clip"],
        "auto": result["auto"],
        "human_reference": result["human_reference"],
        "flag_pulls": None,
        "continuous": result["continuous"],
        "guard": result["guard"],
        "blind_spot": cm.BLIND_SPOT_NOTE,
    }

    if args.flag_pulls is not None:
        if args.review is None:
            print(
                "FEHLER: --flag-pulls benoetigt --review (die Ground-Truth "
                "flag_pull_events.csv wird neben --review gesucht)",
                file=sys.stderr,
            )
            return 1
        ground_truth_path = args.review.parent / "flag_pull_events.csv"
        if not ground_truth_path.exists():
            print(f"FEHLER: Ground-Truth nicht gefunden: {ground_truth_path}", file=sys.stderr)
            return 1

        truth_df = _read_table(ground_truth_path)
        predicted_df = _read_table(args.flag_pulls)

        def _rows(df: pl.DataFrame) -> list[dict]:
            if "outcome" in df.columns:
                df = df.filter(pl.col("outcome") == "pull")
            df = df.filter(pl.col("pull_time_s").is_not_null())
            return df.to_dicts()

        truth_rows = _rows(truth_df)
        predicted_rows = _rows(predicted_df)
        match = _match_pulls(predicted_rows, truth_rows)

        precision_str = (
            _fmt_rate(match["tp"], match["tp"] + match["fp"]) if (match["tp"] + match["fp"]) else "0/0 (n/a)"
        )
        recall_str = (
            _fmt_rate(match["tp"], match["tp"] + match["fn"]) if (match["tp"] + match["fn"]) else "0/0 (n/a)"
        )
        print(f"Flag-Pull Precision: {precision_str}")
        print(f"Flag-Pull Recall: {recall_str}")
        if not match["location_evaluated"]:
            print(
                "Hinweis: Ort-Kriterium (~2 Yards) nicht ausgewertet -- x_yards/y_yards "
                "fehlen in Ground-Truth oder Vorhersage; nur das Zeit-Kriterium (+-0.5s) "
                "wurde angewendet."
            )
        report["flag_pulls"] = match

    _write_out(report, args.out, args.out_md, {"gesamt": result})

    return 0


def _run_split_mode(args: argparse.Namespace, measure_clip_fn, summarise_review_fn) -> int:
    try:
        split_map = cm.read_split(args.split)
    except cm.SplitSchemaError as exc:
        print(f"FEHLER: {exc}", file=sys.stderr)
        return 1

    splits_config: list[tuple[str, Path, Path | None, str]] = []
    if args.tracks_dev is not None:
        splits_config.append(("dev", args.tracks_dev, args.review_dev, "--review-dev"))
    if args.tracks_test is not None:
        splits_config.append(("private_test", args.tracks_test, args.review_test, "--review-test"))

    print(
        "Hinweis: keine video_inventory.csv verfuegbar -- Abdeckung wird gegen den "
        "letzten getrackten Frame gemessen, nicht gegen die echte Clip-Laenge."
    )

    loaded_tracks: dict[str, pl.DataFrame] = {}
    any_violations = False
    for role, tracks_path, _review_path, _review_flag in splits_config:
        tracks_df = _read_table(tracks_path)
        _validate_tracks_schema(tracks_df)
        loaded_tracks[role] = tracks_df

        violations = cm.role_violations(tracks_df, split_map, role)
        if violations:
            any_violations = True
            capped = violations[:5]
            print(
                f"FEHLER: {role}-Tracks ({tracks_path}) enthalten Clips mit falscher "
                "oder unbekannter hackathon_role gegenueber --split:",
                file=sys.stderr,
            )
            for msg in capped:
                print(f"  - {msg}", file=sys.stderr)
            if len(violations) > 5:
                print(f"  ... und {len(violations) - 5} weitere", file=sys.stderr)

    if any_violations:
        return 1

    splits_result: dict[str, dict] = {}
    for role, tracks_path, review_path, review_flag in splits_config:
        tracks_df = loaded_tracks[role]

        review_df = None
        if review_path is not None:
            review_df = cm.read_review_table(review_path)
        else:
            print(
                f"Hinweis ({role}): kein {review_flag} angegeben -- Clip-Liste aus "
                "Tracks abgeleitet, keine menschliche Referenz-Baseline verfuegbar.",
            )

        result = _score_one(
            tracks_df,
            review_path=review_path,
            review_df=review_df,
            measure_clip_fn=measure_clip_fn,
            summarise_review_fn=summarise_review_fn,
            dialect_tolerant=True,
        )
        splits_result[role] = result

        _print_result(
            role,
            result,
            verbose_per_clip=False,
            human_rate_label="Referenz-Baseline (Human-Urteile)",
            review_flag_name=review_flag,
        )

    report: dict = {
        "mode": "split",
        "blind_spot": cm.BLIND_SPOT_NOTE,
        "guard_note": cm.GUARD_NOTE,
        "splits": splits_result,
    }

    _write_out(report, args.out, args.out_md, splits_result)

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--tracks", type=Path, default=None, help="Einzel-Modus: team's submitted tracks (CSV or Parquet)"
    )
    parser.add_argument(
        "--review",
        type=Path,
        default=None,
        help="Einzel-Modus: continuity_review.csv-shaped file (human pass/fail verdicts, the headline reference)",
    )
    parser.add_argument(
        "--flag-pulls",
        type=Path,
        default=None,
        help=(
            "Einzel-Modus only: team's predicted flag-pull events "
            "(flag_pull_events.csv-shaped); ground truth is read from "
            "flag_pull_events.csv next to --review"
        ),
    )
    parser.add_argument("--out", type=Path, default=None, help="Write a JSON report to this path")
    parser.add_argument("--out-md", type=Path, default=None, help="Write a Markdown report to this path")
    parser.add_argument(
        "--tracks-dev", type=Path, default=None, help="Split-Modus: dev-split tracks (Panama Rojo, CSV or Parquet)"
    )
    parser.add_argument(
        "--review-dev",
        type=Path,
        default=None,
        help="Split-Modus: dev-split continuity_review.csv (public reference)",
    )
    parser.add_argument(
        "--tracks-test",
        type=Path,
        default=None,
        help="Split-Modus: private_test-split tracks (Puerto Rico, CSV or Parquet)",
    )
    parser.add_argument(
        "--review-test",
        type=Path,
        default=None,
        help=(
            "Split-Modus: EXPLICIT path to the vaulted private-test "
            "continuity_review.csv -- the ONLY way private labels enter this "
            "script, never auto-derived from --split, never copied elsewhere"
        ),
    )
    parser.add_argument(
        "--split",
        type=Path,
        default=None,
        help=(
            "Split-Modus (required with --tracks-dev/--tracks-test): "
            "hackathon_split.csv -- validates every (session_id, clip_number) "
            "against its expected dev/private_test role"
        ),
    )
    args = parser.parse_args(argv)

    split_mode = args.tracks_dev is not None or args.tracks_test is not None

    if split_mode and args.tracks is not None:
        print(
            "FEHLER: --tracks kann nicht mit --tracks-dev/--tracks-test kombiniert "
            "werden (Einzel- und Split-Modus schliessen sich aus).",
            file=sys.stderr,
        )
        return 1
    if not split_mode and args.tracks is None:
        print(
            "FEHLER: entweder --tracks (Einzel-Modus) oder --tracks-dev/"
            "--tracks-test (Split-Modus) angeben.",
            file=sys.stderr,
        )
        return 1
    if split_mode and args.split is None:
        print(
            "FEHLER: Split-Modus benoetigt --split (Pfad zu hackathon_split.csv).",
            file=sys.stderr,
        )
        return 1
    if split_mode and args.flag_pulls is not None:
        print(
            "FEHLER: --flag-pulls ist in dieser Phase nur im Einzel-Modus "
            "verfuegbar -- der Flag-Pull-Bonus wird nicht pro Split ausgewertet.",
            file=sys.stderr,
        )
        return 1

    measure_clip_fn, summarise_review_fn = _load_continuity_helpers()

    if split_mode:
        return _run_split_mode(args, measure_clip_fn, summarise_review_fn)
    return _run_single_mode(args, measure_clip_fn, summarise_review_fn)


if __name__ == "__main__":
    raise SystemExit(main())
