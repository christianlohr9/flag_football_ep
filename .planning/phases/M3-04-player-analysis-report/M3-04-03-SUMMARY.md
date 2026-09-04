---
phase: M3-04-player-analysis-report
plan: 03
subsystem: reports
tags: [polars, reports, hc-workbook, player-analysis, explosiveness, efficiency]

# Dependency graph
requires:
  - phase: M3-04-player-analysis-report
    provides: "M3-04-01's corrected HC_PASS_ATTEMPT_SCOPE (Attempts excludes Sacks) and hc_efficiency_table's Attempts+Carries primary denominator; M3-04-02's hc_splits.csv (not consumed directly by this plan, reserved for plan 04/05's splits); M3-04-06's drop canonical extra (NULLABLE_EXTRAS['drop'], DROP header mapping) and its guarded tests/test_m3_drop_column.py cross-layer proof"
provides:
  - "reports/player_analysis.py::hc_columns_by_qb(plays, *, group_col='thrown_by') -> HcColumnTable: the workbook-literal per-QB table (all 19 HC columns plus muted/efficiency_drops) with a named availability state"
  - "HcColumnTable(table, unavailable, notices, basis): unavailable lists schema column keys that cannot be computed today (adj_comp_pct/adj_pass_yards/adj_ypa/efficiency/efficiency_drops on the real corpus as of this plan); notices are German sentences naming why, plus the Air-Yards deviation and the hc_workbook row count"
  - "_HC_COLUMN_SCHEMA: the 22-column output schema (spieler + his 19 columns + efficiency_drops + muted) plans 04/05/06 render against"
affects: ["M3-04-04", "M3-04-05", "M3-04-06", "M3-04-07"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Real-signal availability check (_drop_available/_efficiency_available: column present AND at least one non-null/non-blank value) instead of a plain 'column in plays.columns' presence check -- prevents silently computing Adj Comp % == Comp % or Efficiency == 0.0 against plays_scored.parquet's current typed-null-but-present NULLABLE_EXTRAS state"
    - "Outer-join asymmetric null-vs-zero default: a rushing-only QB gets null pass columns (never appeared in the pass/sack scope), a passing-only QB gets a real zero for carries/rush stats (mirrors hc_efficiency_table's own carries fill_null(0) convention)"

key-files:
  created:
    - src/flag_football_ep/reports/player_analysis.py
    - tests/test_reports_player_analysis.py
  modified: []

key-decisions:
  - "Introduced HcColumnTable/hc_columns_by_qb already in Task 1's commit (not deferred to Task 2 as the plan's task-boundary prose implies) so Task 1's own tests have a stable public entry point matching the exact interface M3-04-06's guarded tests already reference; Task 1's HcColumnTable.unavailable/.notices are empty tuples and the seven delegated columns are unconditional typed nulls, Task 2 replaces that placeholder logic with the real availability computation. The final public interface is identical either way -- this is an implementation-sequencing choice, not a behavior change."
  - "Availability for drop/efficiency is judged on real signal (at least one non-null/non-blank value), not mere column presence. hc_efficiency_table's own _require_columns only checks presence, so calling it as-is against today's corpus (drop/efficiency present as typed-null NULLABLE_EXTRAS columns, zero HC rows ingested) would silently return Adj Comp % identical to Comp % and Efficiency == 0.0 for every player -- exactly the must_haves' forbidden 'copy of a neighbouring column' failure. _drop_available/_efficiency_available gate delegation before ever calling the explosiveness functions."
  - "adj Pass Yards (cell N2) sums air_yards on every row flagged dropped, with no completion-status restriction, matching the workbook formula literally (SUMIFS(Data!M, Data!P, <QB>, Data!W, \"*\") has no result-column term at all) rather than narrowing to incomplete rows only, which would be a plausible but undocumented reinterpretation."
  - "Sacks (I2) is computed from its own scope (sack == 1) rather than reusing the pass-attempt scope's sack exclusion, per the interfaces table's own literal mapping (canonical sack==1) -- avoids writing a local play_type==\"pass\" filter this module's own <verification> grep forbids, since a sack cannot occur on a run play by construction."

requirements-completed: [HC-05]

# Metrics
duration: ~55min
completed: 2026-09-04
---

# Phase M3-04 Plan 03: Player Analysis Report Data Layer Summary

**`reports/player_analysis.py::hc_columns_by_qb` reproduces all 19 columns of the head coach's `Player Analysis All Camps` tab from canonical plays with his own formula-cell definitions, delegating Explosive %/Efficiency to the corrected M3-3 module and naming (never faking) the three columns still blocked on real `drop`/`efficiency` signal.**

## Performance

- **Duration:** ~55 min
- **Started:** 2026-09-04T06:15:00Z (approx, session start)
- **Completed:** 2026-09-04T07:10:00Z
- **Tasks:** 2 (both `tdd="true"`, tests written before each task's implementation)
- **Files modified:** 2 (both new)

## Worktree Recovery (setup, before task execution)

This worktree's branch (`worktree-agent-a10154a31b51b1a29`) had been created from `origin/main`
(commit `0a45ee2`, a pre-planning commit predating `.planning/` entirely) instead of from the
feature branch carrying waves 1-2's merged work -- the known EnterWorktree base-selection bug
(#2015). Neither `.planning/`, `src/flag_football_ep/reports/`, nor the M3-04 plan files existed
in the worktree at session start. Verified via `git log --all` that the correct base (waves 1-2
merged, M3-04-03-PLAN.md present, M3-04-01/02/06 with SUMMARY.md, 03/04/05/07 with PLAN.md only)
lives at local branch `worktree-planning-docs` (tip `4b1fcbd`), and that the current worktree
branch carried zero unique commits beyond the stale base (nothing to lose). Recovered via the
sanctioned `<worktree_branch_check>` pattern: confirmed HEAD was on a proper per-agent branch
(not a protected ref), then `git reset --hard 4b1fcbd`. All subsequent work proceeded from the
correct base.

## Accomplishments

- `reports/player_analysis.py` (`hc_columns_by_qb`, `HcColumnTable`, `_HC_COLUMN_SCHEMA`): all
  19 of his tab's columns (Comps, Incs, Attempts, TDs, Comp %, Adj Comp %, INTs, Sacks, Pass
  Yards, Air Yards, YPA, adj Pass Yards, adj YPA, Exp Plays, Explosive %, Efficiency, Carries,
  Rush Yards, Rush TDs) plus `spieler`, `efficiency_drops` (the Attempts+Drops secondary
  reading) and `muted` -- 22 columns total, one row per resolved QB identity.
- Attempts/Sacks/Comp %/Explosive % all route through `features.explosiveness`'s M3-04-01-
  corrected public API (`HC_PASS_ATTEMPT_SCOPE`, `hc_workbook_explosive_rate`,
  `hc_efficiency_table`) -- zero local `play_type == "pass"` filters, verified by the plan's
  own grep check.
- `_drop_available`/`_efficiency_available` gate the three adjusted columns and
  Efficiency/efficiency_drops on real signal (at least one non-null/non-blank value), not mere
  column presence -- today's real corpus (`drop`/`efficiency` present as typed-null
  `NULLABLE_EXTRAS` columns, zero HC rows) is correctly treated as unavailable rather than
  silently computing `Adj Comp % == Comp %`.
- `tests/test_reports_player_analysis.py`: 24 tests, one per `<behavior>` bullet across both
  tasks plus edge cases (rushing-only vs. sack-only QB defaults, whitespace-only drop values not
  flagged, empty-frame schema correctness).
- `tests/test_m3_drop_column.py`'s two `skipif`-gated `hc_columns_by_qb` assertions (M3-04-06)
  now run as real assertions and pass -- confirmed via `-v`: `4 passed` (previously `2 passed, 2
  skipped`).

## Task Commits

Each task was committed atomically (both `tdd="true"`: tests written and verified failing before
implementation, per this repo's established task-level TDD convention -- see M3-04-01-SUMMARY.md's
note on this not requiring separate RED/GREEN commits for `type: execute` plans):

1. **Task 1: The counting and yardage columns, on his denominators** - `59fe367` (feat)
2. **Task 2: The delegated columns and the named availability state** - `c2c0348` (feat)

**Plan metadata:** committed alongside this summary.

## Files Created/Modified

- `src/flag_football_ep/reports/player_analysis.py` - `hc_columns_by_qb`, `HcColumnTable`,
  `_HC_COLUMN_SCHEMA`, `PLAYER_ANALYSIS_FILENAME`, and the private per-scope table builders
  (`_pass_and_sack_table`, `_run_table`, `_base_table`, `_dropped_aggregates`,
  `_identity_expr`, `_drop_flag_expr`, `_drop_available`, `_efficiency_available`)
- `tests/test_reports_player_analysis.py` - 24 tests across `TestCountingAndYardageColumns`
  (12) and `TestDelegatedColumnsAndAvailability` (12)

## The final `HcColumnTable` shape (for plans 04/05/06)

```python
@dataclass(frozen=True)
class HcColumnTable:
    table: pl.DataFrame       # _HC_COLUMN_SCHEMA-shaped, one row per QB, sorted by "spieler"
    unavailable: tuple[str, ...]  # schema column keys with no real data today
    notices: tuple[str, ...]      # German sentences naming why, plus always-on deviations
    basis: SectionBasis           # reports.aggregate.SectionBasis over scrimmage_plays(plays)
```

`_HC_COLUMN_SCHEMA` column order (all `plays_scored.parquet`-agnostic, dtype in parens):

```
spieler (Utf8), comps (Int64), incs (Int64), attempts (Int64), tds (Int64),
comp_pct (Float64), adj_comp_pct (Float64), ints (Int64), sacks (Int64),
pass_yards (Float64), air_yards (Float64), ypa (Float64), adj_pass_yards (Float64),
adj_ypa (Float64), exp_plays (Int64), explosive_pct (Float64), efficiency (Float64),
efficiency_drops (Float64), carries (Int64), rush_yards (Float64), rush_tds (Int64),
muted (Boolean)
```

On today's real corpus shape (`drop`/`efficiency` present as typed-null `NULLABLE_EXTRAS`
columns, zero `hc_workbook` rows), `unavailable` is exactly:
`("adj_comp_pct", "adj_pass_yards", "adj_ypa", "efficiency", "efficiency_drops")` -- five keys,
confirmed by `test_unavailable_and_notices_nonempty_on_todays_real_column_set`. `exp_plays` and
`explosive_pct` are always computable (no HC-only dependency).

## Decisions Made

- `HcColumnTable`/`hc_columns_by_qb` were introduced already in Task 1's commit rather than
  deferred to Task 2 as the plan's task-boundary prose describes (Task 2's action text reads
  "Add the availability container and the delegated columns... `hc_columns_by_qb(...) ->
  HcColumnTable` is the public entry point," implying the dataclass is new in Task 2). Task 1's
  own tests need a stable public entry point that already matches `tests/test_m3_drop_column.py`'s
  exact interface (`HcColumnTable.table`/`.unavailable`/`.notices`,
  `hc_columns_by_qb(plays, *, group_col="thrown_by")`) -- introducing it one task earlier avoids a
  throwaway private-function interface that Task 2 would just replace, and the final public
  surface is byte-identical to what the plan specifies either way. Task 1's version has empty
  `unavailable`/`notices` tuples and unconditional-null delegated columns; Task 2 replaces that
  placeholder logic with the real availability computation -- verified by re-running Task 1's own
  12 tests against the Task-1-only module state before layering Task 2's diff on top.
- Availability is judged on real signal (`_drop_available`/`_efficiency_available`), not the plain
  `"column" in plays.columns` check the plan's action prose literally describes for the
  `MissingExplosivenessColumns` try/except path. `hc_efficiency_table`'s own `_require_columns`
  only checks presence, and `plays_scored.parquet` already carries `drop`/`efficiency` as
  typed-null `NULLABLE_EXTRAS` columns with zero HC rows (per M3-04-01-SUMMARY.md's own corpus
  census) -- calling `hc_efficiency_table` unconditionally against that shape would return
  `Efficiency == 0.0` for every player (not null, not raised), and a naive `drop`-presence check
  would compute `Adj Comp % == Comp %` everywhere. Both are exactly the must_haves' forbidden
  "copy of a neighbouring column" / silent-zero failures. The `MissingExplosivenessColumns`
  try/except is kept as a defensive second layer around the actual delegation call, but the
  real-signal check is what actually gates today's corpus correctly.
- `adj Pass Yards` (cell `N2`) sums `air_yards` on every row flagged dropped regardless of
  completion status, matching the workbook formula's own literal shape
  (`SUMIFS(Data!M, Data!P, <QB>, Data!W, "*")`, no result-column restriction) rather than the more
  intuitive but undocumented "incompletions only" reading.
- `Sacks` is computed from its own scope (`sack == 1` over `scrimmage_plays`) rather than
  reusing the pass-attempt scope's exclusion logic, per the interfaces table's literal mapping
  (`canonical sack==1`) -- this also sidesteps ever writing a local `play_type == "pass"` filter,
  which the plan's own `<verification>` grep forbids; a sack cannot occur on a run play by
  construction, so no `play_type` term is needed at all.

## Deviations from Plan

None (Rule 1-4 sense) - both tasks were implemented and verified per the plan's `<behavior>`
bullets, `<interfaces>` table, and `<verification>` block. The one implementation-sequencing note
above (introducing `HcColumnTable` in Task 1 rather than Task 2) is not a Rule 1-4 deviation --
it changes when a data structure is introduced internally, not what the plan's final public
interface or behavior is; the delivered `hc_columns_by_qb`/`HcColumnTable` surface, column set,
and availability semantics match the plan's `<interfaces>` section exactly.

## Issues Encountered

**Worktree base-selection bug (#2015), described under "Worktree Recovery" above.** The worktree
was created from a stale base with no `.planning/` and no prior M3-04 work at all, rather than
from the feature branch with waves 1-2 merged the objective described. Recovered via
`git reset --hard` to the correct local branch tip after confirming HEAD was on a proper
per-agent branch and that no unique work existed on the stale branch to lose. This consumed
setup time but did not affect the plan's actual implementation, which proceeded normally once
recovered.

**No other issues.** `uv run pytest tests/test_reports_player_analysis.py tests/test_m3_drop_column.py -q`
passed cleanly (24 + 4 = 28 tests) on the first run after Task 2 landed; the plan-level `<verification>`
grep and `git status --porcelain` scope checks both passed as specified.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None. The three genuinely-blocked columns (`adj_comp_pct`, `adj_pass_yards`, `adj_ypa`) and the
two efficiency columns are not stubs -- they are honestly null with a named `unavailable` entry
and a German notice, which is this module's whole reason for being (REP-D01). No hardcoded
empty value flows to a rendered UI without a corresponding availability flag.

## Next Phase Readiness

- `hc_columns_by_qb`/`HcColumnTable`/`_HC_COLUMN_SCHEMA` are stable public surface for plans
  04 (splits/rendering) and 05 (CLI/build wiring): the exact column order, dtypes, and
  `unavailable` key vocabulary are documented above.
- `tests/test_m3_drop_column.py`'s two previously-`skipif`-gated assertions now run and pass on
  every `uv run pytest` invocation in this worktree with no further action needed.
- Once `M3-02-05` lands the real corpus with `hc_workbook` rows and `drop`/`efficiency` values,
  `hc_columns_by_qb`'s output changes automatically with no code change in this module -- the
  availability gates are data-driven, not corpus-state hardcoded.
- No blockers. The Frage 8 (`Data!Y` subtraction term) and Frage 9 (drop-flag looseness vs. his
  exact `"*"` wildcard) open questions remain for `M3-04-07`, unchanged from RESEARCH.

---
*Phase: M3-04-player-analysis-report*
*Completed: 2026-09-04*

## Self-Check: PASSED

- `src/flag_football_ep/reports/player_analysis.py` — FOUND (457 lines, `min_lines: 220`
  satisfied, `grep -c "def hc_columns_by_qb"` = 1)
- `tests/test_reports_player_analysis.py` — FOUND (302 lines, `min_lines: 160` satisfied,
  `grep -c "test_hc_attempts_excludes_sacks"` = 1)
- Commits `59fe367` (Task 1) and `c2c0348` (Task 2) — both present in `git log --oneline --all`
- `uv run pytest tests/test_reports_player_analysis.py -q` — 24 passed
- `uv run pytest tests/test_m3_drop_column.py -v` — 4 passed (previously 2 passed, 2 skipped;
  both `skipif`-gated `hc_columns_by_qb` assertions now run and pass)
- `uv run pytest tests/test_features_explosiveness.py tests/test_reports_own_team.py -q` —
  regression check, all green (no changes to either module, `player_analysis.py` only imports
  from `features.explosiveness`)
- `grep -v '^#' src/flag_football_ep/reports/player_analysis.py | grep -c 'play_type") == "pass"'`
  — returns `0`
- `git status --porcelain` — lists only this plan's two owned files (plus this SUMMARY.md,
  added after the verification commands above were run) before the final metadata commit
- `uv run pytest -q` (full suite, minus the ten pre-existing `cv`-extras-missing collection
  failures already documented as environment gaps in M3-04-01/02/06-SUMMARY.md) — started as a
  foreground-equivalent run; this sandboxed environment's known resource-contention pattern
  (documented in M3-04-01-SUMMARY.md "Issues Encountered" and M3-04-06-SUMMARY.md "Issues
  Encountered": full-suite runs take 20-30+ minutes or longer under concurrent worktree load)
  reproduced here too. The scoped, unpiped runs above (this plan's own 24+4 tests, plus the two
  modules `player_analysis.py` actually imports from) are the trustworthy signal per that same
  precedent; nothing in this plan's file-collision guard touches `cv/**`, `model/**`, or
  `ingest/**`, so the full-suite run's outcome cannot be affected by this plan's changes.
