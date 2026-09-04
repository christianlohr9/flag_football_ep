---
phase: M3-04-player-analysis-report
plan: 04
subsystem: reports
tags: [polars, reports, hc-workbook, player-analysis, explosiveness, success-rate, splits]

# Dependency graph
requires:
  - phase: M3-04-player-analysis-report
    provides: "M3-04-02's hc_splits.csv/load_hc_splits/resolve_hc_game_splits (camp-window resolution); M3-04-03's reports/player_analysis.py::hc_columns_by_qb/HcColumnTable (his 19+ columns, the module this plan extends); M3-3's features/explosiveness.py::DEFINITIONS/definition_comparison/explosive_score/ExplosivenessCalibration/load_calibration (frozen public API, consumed here, never recomputed)"
provides:
  - "reports/player_analysis.py::m3_columns_by_qb(plays, *, calibration, group_col='thrown_by') -> tuple[pl.DataFrame, tuple[str,...]]: Success Rate, calibrated Explosiveness, the continuous explosive_score_mean, plus n/CI/muted/shrunk_rate for every DEFINITIONS key, one row per resolved player identity"
  - "reports/player_analysis.py::load_report_calibration(config) -> tuple[ExplosivenessCalibration|None, tuple[str,...]]: resolves the M3-3 calibration.json artifact, degrading to a German notice on missing/unreadable rather than raising"
  - "reports/player_analysis.py::PlayerAnalysisSplit/PlayerAnalysisReportData/build_player_analysis_data(plays, *, config, scored=None) -> PlayerAnalysisReportData: the full render-ready object -- korpus + hc-gesamt + one section per hc_splits.csv split_key, unresolved_games named explicitly, never raises"
affects: ["M3-04-05"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "M3-3 delegation via a single live definition_comparison call per section, pivoted to wide {definition_key}_{field} columns, rather than four separate calls or any recomputed rate/CI/shrinkage logic"
    - "Split assembly built from the SAME filtered frame feeding both hc_columns_by_qb and m3_columns_by_qb per section, so the two tables can never silently diverge on which plays they cover"
    - "Notice aggregation via tuple(dict.fromkeys(notices)) -- dedupes across sections while preserving first-occurrence order, avoiding N-times-repeated identical German sentences when every section hits the same degraded condition"

key-files:
  created: []
  modified:
    - src/flag_football_ep/reports/player_analysis.py
    - tests/test_reports_player_analysis.py

key-decisions:
  - "m3_columns_by_qb's identity universe is built from scrimmage_plays(plays) (not raw plays), matching hc_columns_by_qb's own scope exactly, so a caller joining the two tables on 'spieler' can never pick up a player absent from his columns (must_haves key_link, verified by test_m3_player_universe_is_subset_of_hc_columns_players)."
  - "korpus/hc-gesamt sections use label_status='n/a' (not a value from hc_splits.csv) since neither is a declared camp window -- only the per-split_key sections carry the real verified/conflict status from the reference file."
  - "A missing hc_games.csv OR hc_splits.csv degrades the WHOLE split layer, not just the missing file's half: the report falls back to only the korpus section, per the plan's literal behaviour bullet, rather than attempting a partial hc-gesamt-only degradation."
  - "notices are deduplicated (tuple(dict.fromkeys(...))) at the PlayerAnalysisReportData level -- every section's hc_columns_by_qb/m3_columns_by_qb notices are collected first, so an identical German sentence (e.g. the Air-Yards deviation, restated once per section) appears exactly once in the final object rather than once per section."

requirements-completed: [HC-05]

# Metrics
duration: ~55min
completed: 2026-09-04
---

# Phase M3-04 Plan 04: Player Analysis Report — M3-3 Columns and Camp Splits Summary

**`reports/player_analysis.py` now puts Success Rate, calibrated Explosiveness and the continuous explosive_score next to the head coach's own columns (`m3_columns_by_qb`, delegated live to M3-3), and `build_player_analysis_data` assembles the full render-ready object split the way his tabs are split (`korpus`, `hc-gesamt`, one section per `hc_splits.csv` camp window), naming every unresolved game and degraded condition in German instead of raising.**

## Performance

- **Duration:** ~55 min
- **Started:** 2026-09-04 (session start, after worktree base recovery)
- **Completed:** 2026-09-04T09:35:57+02:00
- **Tasks:** 2 (both `tdd="true"`, tests written before implementation, single commit per task per this plan's own `type: execute` convention)
- **Files modified:** 2 (both pre-existing, extended)

## Worktree Recovery (setup, before task execution)

This worktree's branch (`worktree-agent-a0e87da0eb1dd1e4b`) had been created from `origin/main`
(commit `0a45ee2`, a pre-planning commit predating `.planning/` entirely) instead of from the
feature branch carrying M3-04-01 through 03/06's merged work -- the known EnterWorktree
base-selection bug (#2015), already documented as recurring in M3-04-03-SUMMARY.md's own
"Worktree Recovery" section. Verified via `git log --oneline` on the local `worktree-planning-docs`
branch that the correct base (`7fea27e docs(M3-04-03): complete player analysis report data
layer plan`) was present there. Confirmed HEAD was on a proper per-agent branch (not a
protected ref), then `git reset --hard worktree-planning-docs`. All subsequent work proceeded
from the correct base.

## Accomplishments

- `m3_columns_by_qb(plays, *, calibration, group_col="thrown_by")`: one row per resolved player
  identity, six columns per `DEFINITIONS` key (`{key}_rate`, `{key}_n`, `{key}_ci_low`,
  `{key}_ci_high`, `{key}_muted`, `{key}_shrunk_rate}`) plus `explosive_score_mean` -- every value
  read from a single live `definition_comparison`/`explosive_score` call, verified against a
  direct `definition_comparison` call on the same frame (never a hard-coded expected rate).
- `load_report_calibration(config)`: resolves `config.paths.reference / "explosiveness" /
  "calibration.json"`, degrading to `(None, (german_notice,))` on a missing file or
  `UnknownCalibrationSchema`, never raising.
- `PlayerAnalysisSplit`/`PlayerAnalysisReportData`/`build_player_analysis_data(plays, *, config,
  scored=None)`: the full render-ready object. `attach_epa` runs on the full corpus first (same
  reason `build_own_team_data` does), player identities are canonicalised via `own_team.py`'s
  own `_canonicalise_players` (imported, not refactored), splits are resolved through
  `load_hc_games`/`load_hc_splits`/`resolve_hc_game_splits`. Sections: `korpus` (always built),
  `hc-gesamt` and one per declared `split_key` (built only when both reference files load).
  `unresolved_games` names every non-`"matched"` game with its `split_match` state.
- `tests/test_reports_player_analysis.py`: 37 tests total (20 pre-existing from M3-04-03 + 17
  new), covering every `<behavior>` bullet from both tasks plus edge cases (sack-only QB scope
  exclusion, null-epa explosive_score_mean, missing/unreadable calibration, two-window
  Pitfall-3 discrimination, unresolved-game naming, missing-reference-file degradation, conflict
  notice, empty-plays-frame safety).

## Task Commits

Each task was committed atomically (both `tdd="true"`; per M3-04-01/03-SUMMARY's established
precedent, `type: execute` plans do not require separate RED/GREEN commits per task):

1. **Task 1: Our metrics beside his, per player** - `8167213` (feat)
2. **Task 2: Split sections and the render-ready report data object** - `4efa611` (feat)

**Plan metadata:** committed alongside this summary.

## Files Created/Modified

- `src/flag_football_ep/reports/player_analysis.py` - `m3_columns_by_qb`, `load_report_calibration`,
  `_M3_COLUMN_SCHEMA`, `PlayerAnalysisSplit`, `PlayerAnalysisReportData`, `build_player_analysis_data`,
  `_build_split` and their supporting notice-template constants
- `tests/test_reports_player_analysis.py` - `TestLoadReportCalibration`, `TestM3ColumnsByQb`,
  `TestBuildPlayerAnalysisData`, plus `_make_config`/`_calibration`/`_assert_matches`/
  `_write_hc_games`/`_write_hc_splits`/`_hc_plays`/`_pat_filler_rows` synthetic fixture builders

## The final `PlayerAnalysisReportData`/`PlayerAnalysisSplit` shape (for plan 05)

```python
@dataclass(frozen=True)
class PlayerAnalysisSplit:
    key: str                 # "korpus" | "hc-gesamt" | split_key from hc_splits.csv
    heading: str              # German, e.g. "Camp III (vs Switzerland)"
    label_status: str         # "verified" | "conflict" | "n/a" (korpus/hc-gesamt)
    columns: HcColumnTable    # his 19+ columns, from M3-04-03
    m3_table: pl.DataFrame    # our metrics, from m3_columns_by_qb
    basis: SectionBasis
    empty_notice: str | None

@dataclass(frozen=True)
class PlayerAnalysisReportData:
    team: str
    splits: tuple[PlayerAnalysisSplit, ...]
    unresolved_games: tuple[tuple[str, str], ...]   # (game_id, split_match)
    unmapped_players: tuple[str, ...]
    notices: tuple[str, ...]                        # deduplicated, first-occurrence order
    n_hc_rows: int
    overall_basis: SectionBasis
```

`m3_table`'s columns (`_M3_COLUMN_SCHEMA`): `spieler`, then for each of the four `DEFINITIONS`
keys (`baseline_hc_workbook`, `baseline_hc_verbal`, `success_rate_epa`,
`explosive_epa_magnitude`) its `_rate`/`_n`/`_ci_low`/`_ci_high`/`_muted`/`_shrunk_rate`, then
`explosive_score_mean`.

Notice vocabulary plan 05 can render directly: the standing per-opponent (`OPP`) limitation
notice (always present), the missing-calibration notice, the missing-split-reference-file
notice, the Camp IV/VI conflict notice, the missing-player-mapping notice, the
unmapped-players notice, the no-offense-plays notice, and every `hc_columns_by_qb`/
`m3_columns_by_qb` per-section notice already established in M3-04-03 (Air Yards deviation,
Efficiency/Adj-columns unavailability, hc_workbook row count).

## Decisions Made

See `key-decisions` in frontmatter. Summary: the M3-3 identity universe is scoped to
`scrimmage_plays` (not raw `plays`) so it can never exceed `hc_columns_by_qb`'s own player set;
`korpus`/`hc-gesamt` get `label_status="n/a"` since neither is a declared `hc_splits.csv` row;
a missing reference file degrades the whole split layer to `korpus`-only (not a
half-degraded state); notices are deduplicated across sections in first-occurrence order.

## Deviations from Plan

None (Rule 1-4 sense) - both tasks were implemented and verified per the plan's `<behavior>`
bullets, `<interfaces>` table and `<verification>` block. One documentation clarification: the
plan's must_haves artifact spec for `tests/test_reports_player_analysis.py` requires a test
named literally `test_empty_state_zero_hc_rows` (grep-checked); this test was initially written
as `test_zero_hc_rows_every_camp_section_has_empty_notice` and renamed to match the literal
required name before the final commit -- same test body, same assertions, name-only fix, not a
Rule 1-4 deviation.

## Issues Encountered

**Worktree base-selection bug (#2015)**, described under "Worktree Recovery" above -- recovered
via `git reset --hard worktree-planning-docs` after confirming HEAD was on a proper per-agent
branch. Consumed setup time but did not affect implementation.

**Real-corpus verification command not runnable in this sandbox.** The plan's `<verification>`
block includes a command that reads `data/processed/plays_scored.parquet` via
`load_config(Path("ffep.toml"))`. That file is DVC-tracked and not materialised in this worktree
(`dvc` CLI itself is not installed in this sandbox), so the command could not be executed here.
This is a pre-existing environment gap (unrelated to this plan's two files) rather than a defect
in the implementation -- `build_player_analysis_data`'s never-raise contract is fully exercised
by the 8 `TestBuildPlayerAnalysisData` tests against synthetic corpora covering every degraded
condition the real corpus could hit (zero HC rows today, populated HC rows once M3-02-04/05
land, missing reference files, unresolved games). Not fixed; flagged for whoever next runs this
command against a materialised corpus (e.g. plan 05's own verification, or a CI environment with
`dvc pull` available).

**No other issues.** `uv run pytest tests/test_reports_player_analysis.py -q` (37 tests),
`tests/test_reports_own_team.py`, `tests/test_reports_aggregate.py`, `tests/test_m3_drop_column.py`,
`tests/test_reference_hc_splits.py`, `tests/test_reference.py`, `tests/test_config.py`,
`tests/test_features_explosiveness.py`, `tests/test_m3_hc_pii.py`, `tests/test_charts_explosiveness.py`
and `tests/test_cli_report.py` (the widest scope of anything importing from or exercising code this
plan touches, transitively) all pass with exit code 0. The plan's own grep check
(`grep -v '^#' ... | grep -c 'play_type") == "pass"'`) returns `0`. `git status --porcelain` lists
only this plan's two owned files before the SUMMARY commit.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None. Every M3-3 column and every split section either carries a real value or a named
`unavailable`/`empty_notice`/notice explaining why -- no hardcoded empty value flows to a
rendered UI without a corresponding availability flag (mirrors M3-04-03-SUMMARY's own
"Known Stubs: None" finding, extended to the new columns/sections this plan adds).

## Next Phase Readiness

- `PlayerAnalysisSplit`/`PlayerAnalysisReportData`/`build_player_analysis_data` are stable
  public surface for plan 05 (rendering/CLI wiring): the exact field names, the `label_status`
  vocabulary (`"verified"`/`"conflict"`/`"n/a"`), the `m3_table` column-naming scheme
  (`{definition_key}_{field}`), and the full notice vocabulary are documented above.
- The whole object builds today on a corpus with zero head-coach rows (every camp section
  empty, `korpus` carrying the real numbers) and will build unchanged once M3-02-04/05 land
  real `hc_workbook` rows -- no code change required in this module for that transition
  (verified by the synthetic two-window Pitfall-3 test, which proves the split filter
  discriminates correctly once real data exists).
- The real-corpus verification command from this plan's `<verification>` block should be
  re-run once `data/processed/plays_scored.parquet` is materialised (via `dvc pull` or
  equivalent) in whatever environment executes plan 05, to confirm the object also builds
  cleanly against the actual corpus and to capture the real notice text for review.
- No blockers. Frage 7 (Camp IV/VI naming) and Frage 8/9 (Air Yards subtraction term, drop-flag
  looseness) remain open for the head coach, unchanged from M3-04-02/03/RESEARCH.

---
*Phase: M3-04-player-analysis-report*
*Completed: 2026-09-04*

## Self-Check: PASSED

- `src/flag_football_ep/reports/player_analysis.py` — FOUND (856 lines, `min_lines: 400`
  satisfied, `grep -c "def build_player_analysis_data"` = 1)
- `tests/test_reports_player_analysis.py` — FOUND (942 lines, `min_lines: 260` satisfied,
  `grep -c "def test_empty_state_zero_hc_rows"` = 1)
- Commits `8167213` (Task 1) and `4efa611` (Task 2) — both present in `git log --oneline`
- `uv run pytest tests/test_reports_player_analysis.py -q` — 37 passed
- `uv run pytest tests/test_reports_own_team.py tests/test_reports_aggregate.py -q` — regression
  check, all green
- `uv run pytest tests/test_m3_drop_column.py tests/test_reference_hc_splits.py tests/test_reference.py tests/test_config.py tests/test_features_explosiveness.py tests/test_m3_hc_pii.py tests/test_charts_explosiveness.py tests/test_cli_report.py -q`
  — every module this plan imports from or that imports from it, all green
- `grep -v '^#' src/flag_football_ep/reports/player_analysis.py | grep -c 'play_type") == "pass"'`
  — returns `0`
- `git status --porcelain` — lists only this plan's two owned files (plus this SUMMARY.md,
  added after the verification commands above were run) before the final metadata commit
- Real-corpus verification command from the plan's `<verification>` block — NOT RUN
  (`data/processed/plays_scored.parquet` is DVC-tracked and not materialised in this sandbox;
  documented under "Issues Encountered" as a pre-existing environment gap, not a defect)
