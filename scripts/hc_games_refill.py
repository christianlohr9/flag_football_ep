"""Regenerate `data/reference/hc_games.csv`'s rows for the head coach's
numeric-block games, after M3-02-01's unordered-pair segmentation rule
change invalidated every pair-block `block_key`.

Why this exists: `docs/hc-workbook-ingest.md` section `## Wartung` documents
the maintenance loop -- an ingest run emits one "Unbekanntes Spiel ..."
notice per undeclared `(workbook, sheet, block_key)`, a maintainer adds the
matching row, and the next run resolves it. `hc_games.csv` currently has 180+
undeclared games waiting after M3-02-01's segmentation fix; hand-typing that
many rows is itself a real risk of transcription error. This script makes
the maintainer step deterministic and re-runnable instead.

Scope -- what gets declared, and what deliberately does not
(`M3-02-04-PLAN.md`'s decision table, re-verified against the real workbooks
2026-09-03 as part of the header-block-rule deviation in this same plan):

- `Offense Analytics 2026 Camps and Competitions.xlsx` / `Data` (numeric):
  YES -- clean PLAY#-reset segmentation, real ODK, posteam/defteam resolve.
- `Scoring Probability by Situation 2023-2026.xlsx` / `Data`'s numeric
  blocks (block 1, block 3): YES -- real PLAY#-charted games, includes the
  nine already-declared `legacy-39..47` duplicates (preserved, never
  regenerated).
- `Scoring Probability by Situation 2023-2026.xlsx` / `Data`'s PAIR block
  (block 0, 22 games after the unordered-pair fix): NO. The head coach's
  2026-09-03 Frage-2 answer confirmed a team-name-header + O/D/S-marker
  convention that CAN resolve posteam/defteam (see
  `flag_football_ep.ingest.hc_workbook._split_pair_block`/
  `_pair_row_marker`) -- but the real block 0 has ZERO O/D/S marker rows
  (verified this session: every row is a full/abbreviated team-name pair,
  the "possession swap written out every row" era). Its posteam/defteam
  therefore still cannot be derived without guessing, exactly as the
  original plan's decision table already concluded -- this script's
  DECLARED_BLOCK_KINDS excludes it, unchanged.
- `Scoring Probability by Situation 2023-2026.xlsx` / `Data`'s two 1-row
  pair "blocks" (block 2, block 4): NO -- single-row annotation noise
  ("CC 25", "Mark"), not games; MIN_PLAYS below excludes them regardless of
  block kind.
- `Scoring Probability by Situation 2023-2026.xlsx` / `Copy of Data`: NO --
  a materially different, undocumented column layout (M3-02-RESEARCH.md
  Sec 1.3: 14 vs 15 columns, `YARD LN`/`Drive Success` swapped, extra `FH`,
  `Thrown By`/`YAC` absent). Out of `DECLARED_SOURCES` entirely.
- `Germany Analytics Stats EC 2025 vs WC Nations.xlsx` / `Data`: NO -- per
  the head coach's own 2026-09-03 answer to Frage 1, this file's `Data` tab
  is empty by design; the Scoring Probability workbook is the sole
  play-by-play source. Out of `DECLARED_SOURCES` entirely.

Every declared row's `competition` is one of the three strings M3-02-02
locked in `data/reference/competition_tier.csv`; this script asserts a
matching tier row exists for every `(source, competition)` pair it is about
to emit, before writing anything.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from flag_football_ep.config import load_config  # noqa: E402
from flag_football_ep.ingest import hc_workbook  # noqa: E402
from flag_football_ep.reference import (  # noqa: E402
    _HC_GAMES_SCHEMA,
    load_competition_tier,
    load_hc_games,
)

# The pair block's `_stamp_posteam_defteam` returns null posteam/defteam for
# every row that has no real ODK (every row of the real block 0, verified
# 2026-09-03 -- see module docstring): declaring those games would put
# unusable rows into `plays.parquet` and burn hc_games.csv rows a future
# Frage-2-driven header/marker discovery would need to invalidate again.
DECLARED_BLOCK_KINDS: tuple[str, ...] = ("numeric",)

# A game below this many plays is left provisional -- single/double-row
# artifacts (the "CC 25"/"Mark" 1-row pair blocks, RESEARCH Sec 1.1) are
# sheet noise, not games, regardless of block kind.
MIN_PLAYS = 5

# [ASSUMED]: the head coach charts his own team's offense -- `ODK == "O"`
# therefore means Germany has the ball (`_stamp_posteam_defteam` maps
# `ODK == "O"` to `home_team`). `OPP` is the existing canonical placeholder
# in `data/reference/team_mapping.csv` for an unnamed opponent. The nine
# pre-existing `legacy-39..47` duplicate rows keep their own `TM1`/`TM2`
# values untouched -- this script never regenerates them (see `build_rows`).
HOME_TEAM = "GER"
AWAY_TEAM = "OPP"


@dataclass(frozen=True)
class DeclaredSource:
    """One declared `(workbook, sheet)` pair's constant metadata.

    `short_slug` is an explicit, stable prefix for `game_id` -- never
    computed by truncating `workbook_slug` programmatically, so it cannot
    silently change if the workbook's own slug ever grows a duplicate
    prefix collision.
    """

    competition: str
    season: int
    tier: str
    short_slug: str


# Locked competition vocabulary from M3-02-02 (`data/reference/competition_tier.csv`,
# M3-02-02-SUMMARY.md "Competition vocabulary contract for M3-02-04") -- keys
# are `(workbook_slug, sheet_display_name)`; `workbook_slug` is
# `hc_workbook.slugify(path.stem)`, `sheet_display_name` matches
# `hc_workbook.SHEET_NAMES` exactly (used to call `ingest_workbook`-style
# readers, which take the sheet's real display name, not its slug).
DECLARED_SOURCES: dict[tuple[str, str], DeclaredSource] = {
    ("offense-analytics-2026-camps-and-competitions", "Data"): DeclaredSource(
        competition="HC Camps 2026", season=2026, tier="mixed-other",
        short_slug="offense-analytics-2026",
    ),
    ("scoring-probability-by-situation-2023-2026", "Data"): DeclaredSource(
        competition="HC Charting 2023-2026", season=0, tier="mixed-other",
        short_slug="scoring-probability",
    ),
}

_HC_GAMES_COLUMNS: list[str] = list(_HC_GAMES_SCHEMA.keys())


def build_rows(
    workbook_path: Path, sheet: str, existing: pl.DataFrame
) -> tuple[list[dict], list[tuple[str, str, str, str]]]:
    """Build the new `hc_games.csv` rows for one declared `(workbook, sheet)`.

    Returns `(rows, skips)`: `rows` are new-row dicts (schema matching
    `_HC_GAMES_SCHEMA`, as plain Python values) for every numeric-block game
    not already declared and not already excluded; `skips` are
    `(workbook, sheet, block_key, reason)` tuples for every OTHER game found
    (pair-block, below `MIN_PLAYS`, or already declared).

    Raises `ValueError` if an existing declared `(workbook, sheet,
    block_key)` for this sheet no longer appears among the freshly segmented
    slices -- RESEARCH Pitfall 2's warning sign, named rather than dropped
    silently.

    An undeclared `(workbook, sheet)` pair (not in `DECLARED_SOURCES`)
    returns `([], [])` -- nothing to do, not an error (lets a caller iterate
    every sheet `hc_workbook.SHEET_NAMES` names without special-casing).
    """
    workbook_slug = hc_workbook.slugify(Path(workbook_path).stem)
    sheet_slug = hc_workbook.slugify(sheet)
    decl = DECLARED_SOURCES.get((workbook_slug, sheet))
    if decl is None:
        return [], []

    header, raw_rows, _ = hc_workbook.read_sheet_rows(workbook_path, sheet)
    blocks, _ = hc_workbook.segment_blocks(header, raw_rows)

    existing_here = existing.filter(
        (pl.col("workbook") == workbook_slug) & (pl.col("sheet") == sheet_slug)
    )
    existing_keys: set[str] = set(existing_here["block_key"].to_list())

    seen_keys: set[str] = set()
    rows: list[dict] = []
    skips: list[tuple[str, str, str, str]] = []

    for block in blocks:
        slices, _ = hc_workbook.segment_games(block)
        for slice_ in slices:
            block_key = slice_.block_key
            seen_keys.add(block_key)

            if block_key in existing_keys:
                # preserved byte-for-byte by the caller (never regenerated,
                # never re-noted) -- not a skip, not a new row.
                continue

            n = len(slice_.rows)
            if block.kind not in DECLARED_BLOCK_KINDS:
                skips.append(
                    (
                        workbook_slug,
                        sheet_slug,
                        block_key,
                        f"{block.kind} block excluded by design (DECLARED_BLOCK_KINDS = "
                        f"{DECLARED_BLOCK_KINDS}) -- posteam/defteam do not resolve",
                    )
                )
                continue

            if n < MIN_PLAYS:
                skips.append(
                    (
                        workbook_slug,
                        sheet_slug,
                        block_key,
                        f"only {n} play(s), below MIN_PLAYS={MIN_PLAYS} -- sheet noise, not a game",
                    )
                )
                continue

            game_id = f"hc-{decl.short_slug}-{sheet_slug}-{block_key}"
            note = (
                f"refill M3-02-04: {block.kind} block, rows "
                f"{slice_.first_row}-{slice_.last_row}, {n} plays"
            )
            rows.append(
                {
                    "workbook": workbook_slug,
                    "sheet": sheet_slug,
                    "block_key": block_key,
                    "source_team1": None,
                    "source_team2": None,
                    "game_id": game_id,
                    "home_team": HOME_TEAM,
                    "away_team": AWAY_TEAM,
                    "competition": decl.competition,
                    "season": decl.season,
                    "game_date": None,
                    "tier": decl.tier,
                    "corpus_game_id": None,
                    "note": note,
                }
            )

    missing = existing_keys - seen_keys
    if missing:
        raise ValueError(
            f"declared block_key(s) for {workbook_slug}/{sheet_slug} no longer found "
            f"after re-segmentation (RESEARCH Pitfall 2): {sorted(missing)} -- the "
            "segmentation rule or the sheet content changed; investigate before "
            "regenerating hc_games.csv, do not hand-patch"
        )

    return rows, skips


def _assert_tier_coverage(rows: list[dict], tier_frame: pl.DataFrame) -> None:
    """Raise before writing anything if a `(source, competition)` pair this
    run is about to emit has no matching `competition_tier.csv` row
    (M3-02-RESEARCH.md Pitfall 3)."""
    known = {(r["source"], r["competition"]) for r in tier_frame.iter_rows(named=True)}
    missing = sorted(
        {
            (f"hc_workbook:{r['workbook']}:{r['sheet']}", r["competition"])
            for r in rows
        }
        - known
    )
    if missing:
        raise ValueError(
            f"(source, competition) pair(s) about to be emitted with no "
            f"competition_tier.csv row: {missing}"
        )


def _dedupe_game_id_collision(new_rows: list[dict]) -> None:
    ids = [r["game_id"] for r in new_rows]
    dupes = sorted({gid for gid in ids if ids.count(gid) > 1})
    if dupes:
        raise ValueError(f"duplicate game_id(s) generated within this run: {dupes}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path("ffep.toml"), help="Path to ffep.toml"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print per-source emitted/preserved/skipped counts, write nothing",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Override the hc_games.csv path (default: the configured reference.hc_games)",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    out_path = args.out or cfg.reference.hc_games
    tier_path = cfg.reference.competition_tier
    raw_dir = cfg.paths.raw_hc_files

    existing = load_hc_games(out_path)
    tier_frame = load_competition_tier(tier_path)

    all_new_rows: list[dict] = []
    all_skips: list[tuple[str, str, str, str]] = []
    summary_lines: list[str] = []

    # "~$..." is Excel's own lock-file convention for a currently-open
    # workbook -- it slugifies to the SAME workbook_slug as the real file
    # (`slugify` strips the leading "~$" as non-alphanumeric), so it must be
    # excluded explicitly rather than relying on the real file sorting
    # first; a lock file is never a valid workbook (BadZipFile on open).
    workbook_paths = (
        sorted(p for p in raw_dir.glob("*.xlsx") if not p.name.startswith("~$"))
        if raw_dir.exists()
        else []
    )

    for (workbook_slug, sheet), decl in sorted(DECLARED_SOURCES.items()):
        matches = [p for p in workbook_paths if hc_workbook.slugify(p.stem) == workbook_slug]
        if not matches:
            summary_lines.append(
                f"{workbook_slug}/{hc_workbook.slugify(sheet)}: workbook not found under "
                f"{raw_dir}, skipping"
            )
            continue
        for path in matches:
            try:
                new_rows, skips = build_rows(path, sheet, existing)
            except hc_workbook.SheetNotFoundError:
                summary_lines.append(
                    f"{workbook_slug}/{hc_workbook.slugify(sheet)}: sheet {sheet!r} not "
                    f"found in {path.name}, skipping"
                )
                continue

            n_preserved = existing.filter(
                (pl.col("workbook") == workbook_slug)
                & (pl.col("sheet") == hc_workbook.slugify(sheet))
            ).height
            summary_lines.append(
                f"{workbook_slug}/{hc_workbook.slugify(sheet)}: {len(new_rows)} new, "
                f"{n_preserved} preserved, {len(skips)} skipped"
            )
            skip_reasons: dict[str, int] = {}
            for _, _, _, reason in skips:
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
            for reason, count in sorted(skip_reasons.items()):
                summary_lines.append(f"  skip x{count}: {reason}")

            all_new_rows.extend(new_rows)
            all_skips.extend(skips)

    _dedupe_game_id_collision(all_new_rows)
    _assert_tier_coverage(all_new_rows, tier_frame)

    for line in summary_lines:
        print(line)
    print(f"total: {len(all_new_rows)} new row(s), {len(all_skips)} skipped block(s)")

    if args.dry_run:
        print("[dry-run] nothing written")
        return 0

    if not all_new_rows:
        print("nothing to add -- hc_games.csv left unchanged")
        return 0

    new_frame = pl.DataFrame(
        all_new_rows, schema=_HC_GAMES_SCHEMA, orient="row"
    ).sort(["workbook", "sheet", "block_key"])
    merged = pl.concat([existing.select(_HC_GAMES_COLUMNS), new_frame.select(_HC_GAMES_COLUMNS)])
    merged.write_csv(out_path)

    # Round-trip through the real loader: asserts every load_hc_games
    # invariant (duplicate (workbook, sheet, block_key), duplicate game_id,
    # tier domain, "hc-" prefix) holds on what was just written.
    load_hc_games(out_path)

    print(f"wrote {out_path} ({merged.height} row(s) total)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
