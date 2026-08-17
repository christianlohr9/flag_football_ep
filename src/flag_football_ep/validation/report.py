"""Markdown validation report and console summary rendering (REQ-S1-05).

`render_report` is pure string rendering (no I/O) so it is trivially
testable; `write_report` does the filesystem work. Layout order is fixed:
H1 + metadata, quarantined games (or an explicit none statement), the
summary table, missing reference data, then one section per game —
quarantined games are surfaced first because a human reads this file to
decide what to fix before the next ingest run.

Never render an API key, an absolute home-directory path, or full data
rows here — only game ids, check names, bounded details and relative
paths belong in this artifact.
"""

from __future__ import annotations

from pathlib import Path

import typer

from flag_football_ep.validation.checks import CHECKS, GameResult, Status

_STATUS_CELL: dict[Status, str] = {
    Status.PASS: "pass",
    Status.FAIL: "FAIL",
    Status.WARN: "warn",
    Status.SKIPPED: "skipped",
}

_CHECK_NAMES = list(CHECKS)


def render_report(
    game_results: list[GameResult],
    notices: dict[str, list[str]],
    contract_version: str,
    run_id: str,
) -> str:
    """Render the full per-run Markdown validation report."""
    quarantined_games = [g for g in game_results if g.quarantined]
    warned_games = [g for g in game_results if any(r.status == Status.WARN for r in g.results)]

    lines: list[str] = []
    lines.append(f"# Validation Report — {run_id}")
    lines.append("")
    lines.append(f"- Contract version: {contract_version}")
    lines.append(f"- Run id: {run_id}")
    lines.append(f"- Games ingested: {len(game_results)}")
    lines.append(f"- Games quarantined: {len(quarantined_games)}")
    lines.append(f"- Games with warnings: {len(warned_games)}")
    lines.append("")

    lines.append("## Quarantined games")
    lines.append("")
    if quarantined_games:
        for g in quarantined_games:
            reasons = "; ".join(g.reasons) if g.reasons else "no reason recorded"
            lines.append(f"- **{g.game_id}** ({g.source}): {reasons}")
    else:
        lines.append("No games were quarantined in this run.")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    header_cells = ["game_id", "source"] + _CHECK_NAMES
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("|" + "---|" * len(header_cells))
    for g in game_results:
        by_check = {r.check: r for r in g.results}
        row_cells = [g.game_id, g.source]
        for name in _CHECK_NAMES:
            r = by_check.get(name)
            row_cells.append(_STATUS_CELL.get(r.status, "?") if r is not None else "?")
        lines.append("| " + " | ".join(row_cells) + " |")
    lines.append("")

    lines.append("## Missing reference data")
    lines.append("")
    skipped_score_games = [
        g.game_id
        for g in game_results
        if any(r.check == "score_reconstruction" and r.status == Status.SKIPPED for r in g.results)
    ]
    no_half_boundary_games = [
        g.game_id
        for g in game_results
        if any(
            r.check == "half_assigned" and r.status != Status.PASS and "no half boundary" in r.detail
            for r in g.results
        )
    ]
    if skipped_score_games:
        lines.append(f"- Score reconstruction skipped (no reference entry): {', '.join(skipped_score_games)}")
    if no_half_boundary_games:
        lines.append(f"- No half boundary recorded: {', '.join(no_half_boundary_games)}")
    if not skipped_score_games and not no_half_boundary_games:
        lines.append("None.")
    lines.append("")

    for g in game_results:
        lines.append(f"### {g.game_id}")
        lines.append("")
        for r in g.results:
            lines.append(f"- {r.check}: {_STATUS_CELL.get(r.status, r.status)} — {r.detail}")
        game_notices = notices.get(g.game_id, [])
        if game_notices:
            lines.append("")
            lines.append("Notices:")
            for notice in game_notices:
                lines.append(f"- {notice}")
        lines.append("")

    return "\n".join(lines)


def write_report(markdown: str, out_dir: Path, run_id: str) -> Path:
    """Write `markdown` to `{out_dir}/validation-report-{run_id}.md`.

    Also writes/overwrites `{out_dir}/validation-report-latest.md` with the
    same content, creating `out_dir` when absent. Returns the timestamped
    path.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamped_path = out_dir / f"validation-report-{run_id}.md"
    latest_path = out_dir / "validation-report-latest.md"
    timestamped_path.write_text(markdown, encoding="utf-8")
    latest_path.write_text(markdown, encoding="utf-8")
    return timestamped_path


def console_summary(game_results: list[GameResult]) -> None:
    """Print one line per game plus a final tally. Never raises on an empty list."""
    n_quarantined = 0
    n_warned = 0
    for g in game_results:
        n_failed = sum(1 for r in g.results if r.status == Status.FAIL)
        n_checks = len(g.results)
        if g.quarantined:
            status = "QUARANTINED"
            n_quarantined += 1
        elif any(r.status == Status.WARN for r in g.results):
            status = "WARN"
            n_warned += 1
        else:
            status = "OK"
        typer.echo(f"{g.game_id}  {g.source}  {status}  {n_failed}/{n_checks}")

    typer.echo(
        f"Total: {len(game_results)} game(s), {n_quarantined} quarantined, {n_warned} with warnings"
    )
