"""Per-game validation checks and the quarantine partition (REQ-S1-05).

Six checks run per `game_id`: five sequence/domain checks plus score
reconstruction against a maintained final-score reference. `run_checks`
returns one `CheckResult` per (check, game_id) pair, never a single
aggregate result. `partition_games` turns those results into an accepted /
quarantined split — new-format games with any FAIL are excluded from the
canonical table, while games from `warn_only_sources` (legacy, by default)
keep their rows and have their FAIL results downgraded to WARN so the
report still surfaces the finding.

These checks are irreducibly custom (RESEARCH: no library validates
"drive IDs are monotonic within a game" or "cumulative score matches an
external reference CSV" out of the box) — the value is in making them
well-tested and precisely reported, not in generality.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

import polars as pl

_MAX_EXAMPLES = 5


class Status(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class CheckResult:
    game_id: str
    check: str
    status: Status
    detail: str
    n_offending: int


@dataclass
class GameResult:
    game_id: str
    source: str
    results: list[CheckResult] = field(default_factory=list)
    quarantined: bool = False
    reasons: list[str] = field(default_factory=list)


def _pass(game_id: str, check: str) -> CheckResult:
    return CheckResult(game_id=game_id, check=check, status=Status.PASS, detail="ok", n_offending=0)


def _fail(game_id: str, check: str, detail: str, n_offending: int) -> CheckResult:
    return CheckResult(game_id=game_id, check=check, status=Status.FAIL, detail=detail, n_offending=n_offending)


def _skip(game_id: str, check: str, detail: str) -> CheckResult:
    return CheckResult(game_id=game_id, check=check, status=Status.SKIPPED, detail=detail, n_offending=0)


def _bounded(values: list, limit: int = _MAX_EXAMPLES) -> str:
    shown = values[:limit]
    text = ", ".join(str(v) for v in shown)
    if len(values) > limit:
        text += f", ... ({len(values)} total)"
    return text


def downs_range(plays: pl.DataFrame, **ctx) -> list[CheckResult]:
    """PASS when every `down` is 0..4; FAIL on out-of-range or null values."""
    df = plays.sort(["game_id", "play_id"])
    agg = df.group_by("game_id", maintain_order=True).agg(
        n_null=pl.col("down").is_null().sum(),
        offending_values=pl.col("down")
        .filter(pl.col("down").is_not_null() & ((pl.col("down") < 0) | (pl.col("down") > 4)))
        .unique()
        .sort(),
        n_out_of_range=(
            pl.col("down").is_not_null() & ((pl.col("down") < 0) | (pl.col("down") > 4))
        ).sum(),
    )

    results: list[CheckResult] = []
    for row in agg.to_dicts():
        game_id = row["game_id"]
        n_null = row["n_null"]
        n_out = row["n_out_of_range"]
        n_offending = n_null + n_out
        if n_offending == 0:
            results.append(_pass(game_id, "downs_range"))
            continue
        parts = []
        if n_out:
            parts.append(f"out-of-range down value(s) {_bounded(row['offending_values'])} in {n_out} row(s)")
        if n_null:
            parts.append(f"{n_null} null down value(s)")
        results.append(_fail(game_id, "downs_range", "; ".join(parts), n_offending))
    return results


def yardline_range(plays: pl.DataFrame, **ctx) -> list[CheckResult]:
    """PASS when every `yardline_50` is 0..50 inclusive; FAIL with min/max found."""
    df = plays.sort(["game_id", "play_id"])
    agg = df.group_by("game_id", maintain_order=True).agg(
        min_val=pl.col("yardline_50").min(),
        max_val=pl.col("yardline_50").max(),
        n_offending=((pl.col("yardline_50") < 0) | (pl.col("yardline_50") > 50)).sum(),
    )

    results: list[CheckResult] = []
    for row in agg.to_dicts():
        game_id = row["game_id"]
        n_offending = row["n_offending"]
        if n_offending == 0:
            results.append(_pass(game_id, "yardline_range"))
            continue
        detail = (
            f"yardline_50 out of [0,50]: min={row['min_val']}, max={row['max_val']} "
            f"({n_offending} row(s))"
        )
        results.append(_fail(game_id, "yardline_range", detail, n_offending))
    return results


def half_assigned(plays: pl.DataFrame, **ctx) -> list[CheckResult]:
    """PASS when every `half` is 1 or 2; FAIL on null (no half boundary) or other values."""
    df = plays.sort(["game_id", "play_id"])
    agg = df.group_by("game_id", maintain_order=True).agg(
        n_null=pl.col("half").is_null().sum(),
        bad_values=pl.col("half")
        .filter(pl.col("half").is_not_null() & ~pl.col("half").is_in([1, 2]))
        .unique()
        .sort(),
        n_bad=(pl.col("half").is_not_null() & ~pl.col("half").is_in([1, 2])).sum(),
    )

    results: list[CheckResult] = []
    for row in agg.to_dicts():
        game_id = row["game_id"]
        n_null = row["n_null"]
        n_bad = row["n_bad"]
        n_offending = n_null + n_bad
        if n_offending == 0:
            results.append(_pass(game_id, "half_assigned"))
            continue
        parts = []
        if n_null:
            parts.append(f"no half boundary ({n_null} row(s))")
        if n_bad:
            parts.append(f"invalid half value(s) {_bounded(row['bad_values'])} ({n_bad} row(s))")
        results.append(_fail(game_id, "half_assigned", "; ".join(parts), n_offending))
    return results


def monotonic_drive_ids(plays: pl.DataFrame, **ctx) -> list[CheckResult]:
    """PASS when `drive_id` is non-decreasing across ascending `play_id` and starts at 1."""
    df = plays.sort(["game_id", "play_id"]).with_columns(
        pl.col("drive_id").diff().over("game_id").alias("_diff")
    )
    agg = df.group_by("game_id", maintain_order=True).agg(
        first_drive_id=pl.col("drive_id").first(),
        first_bad_play_id=pl.col("play_id").filter(pl.col("_diff") < 0).first(),
        n_decreasing=(pl.col("_diff") < 0).sum(),
    )

    results: list[CheckResult] = []
    for row in agg.to_dicts():
        game_id = row["game_id"]
        first_drive_id = row["first_drive_id"]
        n_decreasing = row["n_decreasing"]
        parts = []
        n_offending = 0
        if first_drive_id != 1:
            parts.append(f"first drive_id is {first_drive_id}, expected 1")
            n_offending += 1
        if n_decreasing:
            parts.append(
                f"drive_id decreases at play_id {row['first_bad_play_id']} ({n_decreasing} decrease(s))"
            )
            n_offending += n_decreasing
        if not parts:
            results.append(_pass(game_id, "monotonic_drive_ids"))
        else:
            results.append(_fail(game_id, "monotonic_drive_ids", "; ".join(parts), n_offending))
    return results


def gapless_play_ids(plays: pl.DataFrame, **ctx) -> list[CheckResult]:
    """PASS when `play_id` is exactly 1..N with no duplicates."""
    df = plays.sort(["game_id", "play_id"])
    agg = df.group_by("game_id", maintain_order=True).agg(
        max_id=pl.col("play_id").max(),
        all_ids=pl.col("play_id"),
    )

    results: list[CheckResult] = []
    for row in agg.to_dicts():
        game_id = row["game_id"]
        ids = row["all_ids"]
        max_id = row["max_id"]
        expected = set(range(1, max_id + 1)) if max_id is not None else set()
        actual = set(ids)
        missing = sorted(expected - actual)
        counts = Counter(ids)
        duplicated = sorted(pid for pid, c in counts.items() if c > 1)

        parts = []
        n_offending = 0
        if missing:
            parts.append(f"missing play_id(s) {_bounded(missing)} ({len(missing)} missing)")
            n_offending += len(missing)
        if duplicated:
            dup_count = sum(c - 1 for pid, c in counts.items() if c > 1)
            parts.append(f"duplicated play_id(s) {_bounded(duplicated)} ({dup_count} duplicate row(s))")
            n_offending += dup_count

        if not parts:
            results.append(_pass(game_id, "gapless_play_ids"))
        else:
            results.append(_fail(game_id, "gapless_play_ids", "; ".join(parts), n_offending))
    return results


def score_reconstruction(plays: pl.DataFrame, **ctx) -> list[CheckResult]:
    """PASS when the last row's cumulative score matches the final-score reference.

    Aligns by team code (not position) so a home/away convention flip
    surfaces as a distinct failure rather than a wrong-score failure.
    SKIPPED with "no reference entry" when the game is absent from the
    reference frame or no reference frame was supplied at all.
    """
    final_scores: pl.DataFrame | None = ctx.get("final_scores")

    df = plays.sort(["game_id", "play_id"])
    last_rows = df.group_by("game_id", maintain_order=True).agg(
        home_team=pl.col("home_team").last(),
        away_team=pl.col("away_team").last(),
        home_team_score=pl.col("home_team_score").last(),
        away_team_score=pl.col("away_team_score").last(),
    )

    ref_lookup: dict[str, dict] = {}
    if final_scores is not None and final_scores.height:
        ref_lookup = {row["game_id"]: row for row in final_scores.to_dicts()}

    results: list[CheckResult] = []
    for row in last_rows.to_dicts():
        game_id = row["game_id"]
        ref = ref_lookup.get(game_id)
        if final_scores is None or ref is None:
            results.append(_skip(game_id, "score_reconstruction", "no reference entry"))
            continue

        frame_home, frame_away = row["home_team"], row["away_team"]
        ref_home, ref_away = ref["home_team"], ref["away_team"]

        if {frame_home, frame_away} != {ref_home, ref_away}:
            detail = (
                f"home/away convention mismatch: frame has home={frame_home!r} away={frame_away!r}, "
                f"reference has home={ref_home!r} away={ref_away!r}"
            )
            results.append(_fail(game_id, "score_reconstruction", detail, 1))
            continue

        if frame_home == ref_home:
            reconstructed_home, reconstructed_away = row["home_team_score"], row["away_team_score"]
        else:
            reconstructed_home, reconstructed_away = row["away_team_score"], row["home_team_score"]

        if reconstructed_home == ref["home_score"] and reconstructed_away == ref["away_score"]:
            results.append(_pass(game_id, "score_reconstruction"))
        else:
            detail = (
                f"reconstructed home={reconstructed_home} away={reconstructed_away} vs "
                f"reference home={ref['home_score']} away={ref['away_score']}"
            )
            results.append(_fail(game_id, "score_reconstruction", detail, 1))
    return results


CHECKS: dict[str, Callable[..., list[CheckResult]]] = {
    "downs_range": downs_range,
    "yardline_range": yardline_range,
    "half_assigned": half_assigned,
    "monotonic_drive_ids": monotonic_drive_ids,
    "gapless_play_ids": gapless_play_ids,
    "score_reconstruction": score_reconstruction,
}

_CHECK_ORDER: dict[str, int] = {name: i for i, name in enumerate(CHECKS)}


def run_checks(plays: pl.DataFrame, final_scores: pl.DataFrame | None = None) -> list[CheckResult]:
    """Run every registered check, per game_id, in `CHECKS` registry order.

    Never raises on an empty frame — returns `[]`.
    """
    if plays.height == 0:
        return []

    results: list[CheckResult] = []
    for check_fn in CHECKS.values():
        results.extend(check_fn(plays, final_scores=final_scores))
    return results


def partition_games(
    plays: pl.DataFrame,
    results: list[CheckResult],
    warn_only_sources: frozenset[str] = frozenset({"legacy"}),
) -> tuple[pl.DataFrame, pl.DataFrame, list[GameResult]]:
    """Split `plays` into accepted / quarantined frames plus per-game results.

    A game is quarantined when at least one `CheckResult` has `Status.FAIL`
    and its source is not in `warn_only_sources`. For warn-only sources,
    every FAIL is rewritten to WARN in the returned `GameResult` (the input
    `CheckResult` objects are never mutated) so the rows stay in the
    dataset while the report still surfaces the finding. Row conservation
    (accepted + quarantined == input height) is asserted; a violation
    raises `RuntimeError`, since a silent row drop in the gate would be
    indistinguishable from bad data.
    """
    if plays.height == 0:
        return plays.clone(), plays.clone(), []

    source_agg = plays.group_by("game_id", maintain_order=True).agg(
        n_sources=pl.col("source").n_unique(),
        source=pl.col("source").first(),
    )
    inconsistent = source_agg.filter(pl.col("n_sources") > 1)
    if inconsistent.height:
        raise ValueError(
            f"game(s) with inconsistent source column: {inconsistent['game_id'].to_list()}"
        )

    source_by_game: dict[str, str] = dict(
        zip(source_agg["game_id"].to_list(), source_agg["source"].to_list())
    )

    results_by_game: dict[str, list[CheckResult]] = {}
    for r in results:
        results_by_game.setdefault(r.game_id, []).append(r)

    game_results: list[GameResult] = []
    quarantined_ids: set[str] = set()

    for game_id in sorted(source_by_game):
        source = source_by_game[game_id]
        warn_only = source in warn_only_sources
        raw_results = sorted(
            results_by_game.get(game_id, []),
            key=lambda r: _CHECK_ORDER.get(r.check, len(_CHECK_ORDER)),
        )

        final_results: list[CheckResult] = []
        reasons: list[str] = []
        has_blocking_fail = False

        for r in raw_results:
            if r.status == Status.FAIL:
                reasons.append(f"{r.check}: {r.detail}")
                if warn_only:
                    final_results.append(
                        CheckResult(
                            game_id=r.game_id,
                            check=r.check,
                            status=Status.WARN,
                            detail=r.detail,
                            n_offending=r.n_offending,
                        )
                    )
                else:
                    final_results.append(r)
                    has_blocking_fail = True
            else:
                final_results.append(r)

        quarantined = has_blocking_fail
        if quarantined:
            quarantined_ids.add(game_id)

        game_results.append(
            GameResult(
                game_id=game_id,
                source=source,
                results=final_results,
                quarantined=quarantined,
                reasons=reasons,
            )
        )

    quarantined_mask = pl.col("game_id").is_in(sorted(quarantined_ids))
    accepted = plays.filter(~quarantined_mask)
    quarantined_df = plays.filter(quarantined_mask)

    if accepted.height + quarantined_df.height != plays.height:
        raise RuntimeError(
            "row conservation violated in partition_games: "
            f"accepted={accepted.height} + quarantined={quarantined_df.height} != input={plays.height}"
        )

    return accepted, quarantined_df, game_results
