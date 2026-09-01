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
`--review`/`--flag-pulls` name. The private test set's labels are never shipped in
the test bundle, so they are simply never on a team's disk to accidentally read.

Usage:

    uv run python scripts/hackathon/score_tracks.py \\
        --tracks my_tracks.csv --review data/continuity_review.csv \\
        [--flag-pulls my_pull_events.csv] [--out report.json]

Every printed rate is reported as `k/n (p%)` -- the project's statistical-honesty
convention, never a bare percentage without its denominator.

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


def _fmt_rate(k: int, n: int) -> str:
    pct = (k / n * 100) if n else 0.0
    return f"{k}/{n} ({pct:.2f}%)"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tracks", required=True, type=Path, help="Team's submitted tracks (CSV or Parquet)")
    parser.add_argument(
        "--review",
        type=Path,
        default=None,
        help="continuity_review.csv-shaped file (human pass/fail verdicts, the headline reference)",
    )
    parser.add_argument(
        "--flag-pulls",
        type=Path,
        default=None,
        help=(
            "Team's predicted flag-pull events (flag_pull_events.csv-shaped); ground "
            "truth is read from flag_pull_events.csv next to --review"
        ),
    )
    parser.add_argument("--out", type=Path, default=None, help="Write a JSON report to this path")
    args = parser.parse_args(argv)

    measure_clip_fn, summarise_review_fn = _load_continuity_helpers()

    tracks_df = _read_table(args.tracks)
    _validate_tracks_schema(tracks_df)

    if args.review is not None:
        review_df = _read_table(args.review)
        clip_numbers = _clip_numbers_from_review(review_df)
    else:
        clip_numbers = sorted(tracks_df["clip_number"].unique().to_list())
        print(
            "Hinweis: kein --review angegeben -- Clip-Liste aus --tracks abgeleitet, "
            "keine menschliche Referenz-Baseline verfuegbar.",
        )

    print(
        "Hinweis: keine video_inventory.csv verfuegbar -- Abdeckung wird gegen den "
        "letzten getrackten Frame gemessen, nicht gegen die echte Clip-Laenge."
    )

    per_clip = _run_automatic_continuity(tracks_df, clip_numbers, measure_clip_fn)
    for row in per_clip:
        print(
            f"Clip {row['clip_number']:>3}: n_tracks={row['n_tracks']} "
            f"longest_track_frac={row['longest_track_frac']:.2f} "
            f"n_fragments={row['n_fragments']} auto_flag={row['auto_flag']}"
        )

    n_clips = len(per_clip)
    n_auto_ok = sum(1 for row in per_clip if row["auto_flag"] == "ok")
    auto_rate_str = _fmt_rate(n_auto_ok, n_clips)
    print(f"Automatische Kontinuitaet (auto_flag=ok): {auto_rate_str}")

    report: dict = {
        "n_clips": n_clips,
        "per_clip": per_clip,
        "auto": {"n_ok": n_auto_ok, "n_clips": n_clips, "rate": (n_auto_ok / n_clips) if n_clips else None},
        "human_reference": None,
        "flag_pulls": None,
    }

    if args.review is not None:
        summary = summarise_review_fn(args.review)
        if summary["pass_rate"] is not None:
            human_rate_str = _fmt_rate(summary["n_pass"], summary["n_clips"])
            print(f"Referenz-Baseline (Human-Urteile, aus --review): {human_rate_str}")
        else:
            print(
                "Referenz-Baseline: nicht auswertbar -- --review enthaelt unbewertete "
                f"Clips: {summary['unreviewed_clips']}"
            )
        report["human_reference"] = summary

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

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"Bericht geschrieben: {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
