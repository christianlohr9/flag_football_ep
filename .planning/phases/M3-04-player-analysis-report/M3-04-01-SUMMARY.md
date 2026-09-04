---
phase: M3-04-player-analysis-report
plan: 01
subsystem: metrics
tags: [polars, explosiveness, efficiency, hc-workbook, data-correction]

# Dependency graph
requires:
  - phase: M3-03-explosiveness-efficiency
    provides: "features/explosiveness.py's DEFINITIONS/definition_comparison/hc_workbook_explosive_rate/hc_efficiency_table public API, the calibration.json artifact, and docs/explosiveness-vorschlag.md/-recherche.md"
provides:
  - "HC_PASS_ATTEMPT_SCOPE: the one shared, sack-excluding pass-attempt scope (workbook D2 = Comps+Incs+INTs) used by hc_workbook_explosive_rate, hc_verbal_explosive_rate, hc_efficiency_table and both baseline_hc_* DEFINITIONS entries"
  - "hc_efficiency_table's corrected primary denominator: attempts + carries (workbook U2 = D2+W2), with the prior Attempts+Drops reading kept as a clearly-named secondary reading (drops/denominator_drops/efficiency_drops)"
  - "Regenerated data/reference/explosiveness/comparison_overall.csv and comparison_by_player.csv on the corrected denominators, with calibration.json and cliff_zone.csv verified byte-identical/unchanged"
  - "Dated 'Korrektur 2026-09-04 (Nenner)' correction sections in docs/explosiveness-vorschlag.md and docs/explosiveness-recherche.md"
affects: [M3-04-player-analysis-report, reports/player_analysis.py (future plans in this phase)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Single shared scope expression (HC_PASS_ATTEMPT_SCOPE) + one private helper (_hc_pass_attempts) that every HC-scoped function routes through, replacing four independent local `play_type == \"pass\"` filters"
    - "Primary reading / secondary reading pattern for genuinely ambiguous HC formulas: hc_efficiency_table now always returns both denominator/efficiency (primary, workbook-exact) and denominator_drops/efficiency_drops (secondary, null unless drops_flag given) rather than picking one silently"

key-files:
  created: []
  modified:
    - src/flag_football_ep/features/explosiveness.py
    - tests/test_features_explosiveness.py
    - scripts/explosiveness_comparison.py
    - data/reference/explosiveness/comparison_overall.csv
    - data/reference/explosiveness/comparison_by_player.csv
    - docs/explosiveness-vorschlag.md
    - docs/explosiveness-recherche.md
    - tests/test_m3_explosiveness_docs.py

key-decisions:
  - "Dated the vorschlag.md/recherche.md correction notes 2026-09-04 (the execution date) rather than the PLAN.md draft's literal '2026-09-03' example text, matching the orchestrator's explicit instruction and this session's actual date; recherche.md's per-formula parentheticals separately cite 2026-09-03 as the date the formula cells were originally read via openpyxl, which is a different, also-correct fact."
  - "scripts/explosiveness_comparison.py's second local pass_epa (in _build_by_player_table, used only for pseudonym-eligibility bucketing, not for the published comparison numbers) was left untouched per the plan's explicit 'change nothing else in the script' instruction, even though it still counts sacks toward the eligibility threshold."
  - "hc_efficiency_table's drops_flag=None case now returns null (not 0) for drops/denominator_drops/efficiency_drops, distinguishing 'no drops reading requested' from 'zero drops observed'."

requirements-completed: [HC-05]

# Metrics
duration: ~25min (across a mid-session resume)
completed: 2026-09-04
---

# Phase M3-04 Plan 01: Explosiveness/Efficiency Denominator Correction Summary

**Corrected the HC Attempts scope (excludes Sacks: workbook `D2` = Comps+Incs+INTs) and the
Efficiency denominator (Attempts+Carries per workbook `U2`, not Attempts+Drops) in
`features/explosiveness.py`, regenerated the committed comparison CSVs on the real corpus, and
added dated correction notes to both German HC documents.**

## Performance

- **Duration:** ~25 min of active work (session was interrupted by a rate limit mid-Task-1 and
  resumed from the uncommitted partial edit)
- **Completed:** 2026-09-04
- **Tasks:** 2 (both `type="auto"`, task 1 `tdd="true"`)
- **Files modified:** 8

## Accomplishments

- `HC_PASS_ATTEMPT_SCOPE` (`play_type == "pass"` minus sack rows, `sack` validated fail-loud)
  replaces the prior, formula-cell-contradicting `HC_PASS_ATTEMPT_FILTER` docstring claim
  ("Comps+Incs+Sacks"); every HC-scoped function (`hc_workbook_explosive_rate`,
  `hc_verbal_explosive_rate`, `hc_efficiency_table`, both `baseline_hc_*` `DEFINITIONS` entries)
  now routes through one shared `_hc_pass_attempts` helper instead of four independent local
  `play_type == "pass"` filters.
- `hc_efficiency_table` now computes `carries` (the same group's `play_type == "run"` count) and
  makes `denominator = attempts + carries` / `efficiency = efficiency_sum / denominator` the
  primary reading, matching workbook cell `U2 = D2 + W2`. The prior `drops_flag`-driven
  Attempts+Drops reading is retained as `drops`/`denominator_drops`/`efficiency_drops`, null
  when `drops_flag` is not given.
- 12 new/retargeted tests added to `tests/test_features_explosiveness.py` covering every
  `<behavior>` bullet: sack exclusion in `n`/`exp_plays`, the sack-scope probe, missing-`sack`
  fail-loud on all three HC-scoped functions, carries extending the primary denominator, the
  drops reading demoted to secondary-only, and `DEFINITIONS` parity with `hc_workbook_explosive_rate`.
- Regenerated `comparison_overall.csv`/`comparison_by_player.csv` against the real corpus
  (`uv run python scripts/explosiveness_comparison.py`, no `--recalibrate`); `calibration.json`
  and `cliff_zone.csv` verified byte-identical/unchanged.
- Added a dated `### Korrektur 2026-09-04 (Nenner)` section to `docs/explosiveness-vorschlag.md`
  and corrected the two wrong formula transcriptions in `docs/explosiveness-recherche.md`, each
  citing its formula cell and read date.

## Corpus State (per hard_rules)

The comparison script reads `data/processed/plays_scored.parquet` (via `ffep.toml`'s
`paths.processed`). Corpus census printed by the regeneration run:

```
total_rows=21437
scrimmage_rows=16067
scrimmage_rows_with_epa=14669
hc_workbook_rows_present=False
efficiency_column_present=False
```

This is **unchanged** from M3-3's original run (`calibration.json`'s `corpus_n=16067` and
`corpus_fingerprint` are byte-identical before/after). M3-02-04's 1,964 `hc_workbook:` rows
(35 games) landed in `data/processed/plays.parquet` (23,401 rows) but have **not** been carried
into `plays_scored.parquet` (still 21,437 rows, 0 `hc_workbook` source rows) — that rescoring is
M3-02-05's job, explicitly out of scope here. Every numeric change in this plan's regenerated
CSVs therefore comes from the two definition fixes alone, not from new data.

`--recalibrate` was correctly NOT used: the corrected Attempts scope changes rate denominators
(pass-attempt counts), not the EPA-quantile calibration input (`scrimmage_plays(..., require_epa=True)`
over ALL scrimmage plays, run+pass, is untouched by the Attempts-scope fix). `calibration.json`
came out byte-for-byte identical, confirming this.

## Before/After: the four pass-scoped `comparison_overall.csv` rows

252 sack rows were removed from the Attempts denominator (14,991 → 14,739).

| definition | before n | before successes | before rate | after n | after successes | after rate |
|---|---|---|---|---|---|---|
| `baseline_hc_workbook` | 14.991 | 2.365 | 15,78 % | 14.739 | 2.365 | 16,05 % |
| `baseline_hc_verbal` | 14.991 | 7.290 | 48,63 % | 14.739 | 7.284 | 49,42 % |
| `verbal_only_yards_clause` | 14.991 | 84 | 0,56 % | 14.739 | 84 | 0,57 % |
| `hc_efficiency_status` (scanned n) | 14.991 | 0 | n/a (no `efficiency` col yet) | 14.739 | 0 | n/a (no `efficiency` col yet) |

`success_rate_epa` and `explosive_epa_magnitude` (scope = all scrimmage plays) and `cliff_zone.csv`
are confirmed **unchanged** (n=16.067 throughout) — the correction is pass-scope only, as expected.
`baseline_hc_verbal`'s successes also dropped by 6 (7290→7284): a handful of the removed sack
rows had `epa > 0`, so the correction changed both the numerator and denominator there, not just
the denominator.

`comparison_by_player.csv`: 360 of 380 rows changed, all and only `baseline_hc_workbook`/
`baseline_hc_verbal` rows (confirmed via diff — 0 `success_rate_epa`/`explosive_epa_magnitude`
rows touched). Some receiver-pseudonym rows lost 1-3 attempts where a sack play happened to carry
a `received_by` value; QB-01's own row was unaffected in this run (no sacks attributed to that
particular pseudonym).

## Final signature: `hc_efficiency_table`

```python
def hc_efficiency_table(
    plays: pl.DataFrame,
    *,
    group_col: str = "thrown_by",
    drops_flag: pl.Expr | None = None,
) -> pl.DataFrame:
    ...
```

Returns one row per group with columns: `group_col`, `efficiency_sum`, `attempts`, `carries`,
`denominator` (= `attempts + carries`, PRIMARY), `efficiency` (= `efficiency_sum / denominator`,
PRIMARY), `out_of_domain`, `drops` (null unless `drops_flag` given), `denominator_drops`
(= `attempts + drops`, null unless `drops_flag` given), `efficiency_drops` (null unless
`drops_flag` given). Raises `MissingExplosivenessColumns` naming `efficiency` (always) or `sack`
(via `_hc_pass_attempts`) when either is absent from the input frame. This is the function plans
03/04 in this phase should call for the report's Efficiency column and its secondary Drops
reading.

## Task Commits

Each task was committed atomically:

1. **Task 1: One shared head-coach Attempts scope (sacks excluded) and the Attempts-plus-Carries
   efficiency denominator** - `0e18a91` (fix)
2. **Task 2: Regenerate the committed comparison tables and correct both German documents** -
   `ddbfc2a` (fix)

**Plan metadata:** committed alongside this summary.

_Note: Task 1 was `tdd="true"` in intent (tests written and extended alongside the implementation
in the same commit, following the existing test file's established conventions) rather than a
strict separate RED/GREEN commit split — this plan's `type` is `execute`, not `type: tdd`, so the
plan-level TDD gate does not apply; task-level `tdd="true"` here means "write the tests first,
verify they fail, then implement," which was followed, not that separate RED/GREEN commits were
required._

## Files Created/Modified

- `src/flag_football_ep/features/explosiveness.py` - `HC_PASS_ATTEMPT_SCOPE`, `_hc_pass_attempts`
  helper, corrected `hc_workbook_explosive_rate`/`hc_verbal_explosive_rate`/`hc_efficiency_table`/
  `DEFINITIONS` scopes and docstrings
- `tests/test_features_explosiveness.py` - 12 new/retargeted tests for the sack exclusion and
  carries/drops split
- `scripts/explosiveness_comparison.py` - `_build_overall_table`'s local pass scope now imports
  and uses `HC_PASS_ATTEMPT_SCOPE`
- `data/reference/explosiveness/comparison_overall.csv` - regenerated, 4 pass-scoped rows moved
- `data/reference/explosiveness/comparison_by_player.csv` - regenerated, 360 `baseline_hc_*` rows
  moved
- `docs/explosiveness-vorschlag.md` - rates rewritten against the regenerated CSVs, dated
  `### Korrektur 2026-09-04 (Nenner)` section added under `## Datengrundlage`
- `docs/explosiveness-recherche.md` - two formula transcription lines corrected, each citing its
  cell and read date
- `tests/test_m3_explosiveness_docs.py` - `_ALLOWED_CALLBACK_PERCENTAGES` updated to `"49,4 %"`,
  new test asserting the Korrektur heading and both corrected denominator phrases

## Decisions Made

- Dated the correction notes `2026-09-04` (execution date, per the orchestrator's explicit
  instruction and this session's actual date) rather than PLAN.md's own literal `2026-09-03`
  example text in its action description — the underlying documents' own `Stand:` lines remain
  `2026-09-03` (unchanged, that's when the documents were first published), and the new
  correction sections are separately dated `2026-09-04`.
- Left `scripts/explosiveness_comparison.py`'s second local `pass_epa` (in
  `_build_by_player_table`, feeding only pseudonym-eligibility bucketing, not the published
  numbers) untouched, per the plan's explicit "change nothing else in the script" instruction.
- `drops_flag=None` now yields `null` (not `0`) for `drops`/`denominator_drops`/
  `efficiency_drops`, so "no drops reading requested" is visibly distinct from "zero drops
  observed."

## Deviations from Plan

None - plan executed exactly as written. The plan's own file-collision guard, cross-phase note
(M3-03-03 review line preserved — this document has no such review-status line to preserve, so
nothing needed removing/preserving there), and read-only boundaries (`calibration.json`,
`charts/explosiveness.py`, `reports/**`, `hc_games.csv`, etc.) were all honored: `git diff
--stat` confirms only the plan's `files_modified` list changed, and `calibration.json`/
`cliff_zone.csv` are byte-identical.

## Issues Encountered

The session hit a rate-limit interruption partway through Task 1's `hc_efficiency_table`
rewrite; the partial edit (adding `HC_PASS_ATTEMPT_SCOPE`/`_hc_pass_attempts` and updating
`hc_workbook_explosive_rate`/`hc_verbal_explosive_rate`) was already uncommitted on disk and the
36 pre-existing tests still passed against it. Resumed cleanly from that partial state, finished
the `hc_efficiency_table` rewrite and its tests, then proceeded through Task 2 without further
interruption. No functional impact — the partial edit was internally consistent at every point
(no half-written function bodies were left uncommitted across the interruption).

Neither the full `uv run pytest -q` run (87 test files, including heavy CV/model integration
tests unrelated to this plan) nor a second, narrower run excluding the same known-slow CV/model
files a concurrently-running sibling parallel executor was already excluding finished within
this session's practical time budget (both were still running after 20-30+ minutes, competing
for CPU with that sibling executor's own identical scoped run in a different worktree); both
were deliberately terminated rather than left consuming shared resources indefinitely. All tests
directly exercising the changed code — `tests/test_features_explosiveness.py` (44 passed),
`tests/test_m3_explosiveness_docs.py` + `tests/test_m3_hc_pii.py` (12 passed), and
`tests/test_charts_explosiveness.py` (17 passed, the only other module importing from
`features/explosiveness.py`) — pass cleanly. No file this plan touches overlaps the CV/model
test surface (`ingest/**`, `model/**`, `cv/**` are all outside this plan's file-collision guard
and were not modified).

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `features/explosiveness.py`'s public API (`hc_workbook_explosive_rate`,
  `hc_verbal_explosive_rate`, `hc_efficiency_table`, `DEFINITIONS`/`definition_comparison`) is now
  correction-complete and matches the head coach's actual workbook formulas (`D2`, `U2`) for
  Attempts and Efficiency. Plans 03/04 in this phase (the report/handout) can consume it directly
  without re-deriving or re-checking these two denominators.
- The Efficiency column still cannot be computed on real rows (no `efficiency` extras column in
  `plays_scored.parquet` yet, unchanged pre-existing gap, tracked separately) — this plan did not
  attempt to fix that, only the formula the function computes once the column exists.
- `docs/hc-rueckfragen-2026-09.md` Frage 4/5/6 remain open and are the correct place for a future
  plan (M3-04-07 per RESEARCH) to ask the head coach whether the Attempts-plus-Carries reading
  matches his intent.

## Self-Check: PASSED

- All 9 `files_modified` (plus this SUMMARY.md) confirmed present on disk with `[ -f ]`.
- Both task commits confirmed in `git log --oneline`: `0e18a91` (Task 1), `ddbfc2a` (Task 2).
- Plan-level `<verification>` re-run:
  - `uv run pytest tests/test_features_explosiveness.py tests/test_m3_explosiveness_docs.py tests/test_m3_hc_pii.py -q` — 56 passed.
  - `git status --porcelain` — only this plan's owned files plus two pre-existing untracked files outside this plan's scope (`.planning/phases/02.2-dataset-buildout/deferred-items.md`, `.planning/todos/pending/2026-09-04-hc-korpus-zulassung-vor-training.md`), left untouched per instructions.
  - `data/reference/explosiveness/calibration.json` and `cliff_zone.csv` — confirmed byte-identical (no diff).
  - `grep -v '^#' docs/explosiveness-recherche.md | grep -c 'Comps+Incs+Sacks'` — returns `0`.
- All `<acceptance_criteria>`/`<behavior>` bullets from Task 1 verified via the new/retargeted tests; Task 2's `<done>` criteria (regenerated CSVs, dated correction notes, doc/PII gate green) verified directly above.

---
*Phase: M3-04-player-analysis-report*
*Completed: 2026-09-04*
