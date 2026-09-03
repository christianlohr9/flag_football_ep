---
phase: M3-03-explosiveness-efficiency
plan: 01
subsystem: features
tags: [polars, explosiveness, efficiency, epa, success-rate, isoppp, shrinkage, hc-04]

# Dependency graph
requires:
  - phase: 01.4-coaching-reports
    provides: reports/aggregate.py's rate_table / MUTED_MIN_N Clopper-Pearson convention
  - phase: M3-1/M3-2 (features/mutations.py)
    provides: the epa/yards_gained/down/play_type canonical + derived columns this module consumes
provides:
  - "features/explosiveness.py: scrimmage_plays PAT filter, hc_workbook_explosive_rate,
    hc_verbal_explosive_rate, hc_efficiency_table (literal HC-workbook reproductions)"
  - "calibrate/ExplosivenessCalibration/write_calibration/load_calibration: versioned,
    corpus-fingerprinted IsoPPP-style explosiveness threshold, no hard-coded EPA cutoff"
  - "success_flag, explosive_epa_flag, explosive_score: binary and continuous (0,1) explosive
    readings sharing one calibrated threshold"
  - "DEFINITIONS/definition_comparison/shrink_rate/cliff_zone_table: the four-definition
    comparison rollup with n/CI/muted/shrunk_rate, and the 10-12 yard cliff density table"
affects: [M3-03-02, M3-03-03, M3-4]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Metric definitions as data (MetricDefinition/DEFINITIONS tuple) instead of hand-written
      call sites, so the coach-facing comparison table cannot drift from the definition list"
    - "Every rate carries n + Clopper-Pearson CI + muted, reusing reports/aggregate.rate_table;
      shrunk_rate is an additive proposal column, never a replacement"
    - "Calibration artifacts as versioned JSON with schema_version + corpus_fingerprint,
      mirroring data/reference/hackathon_freeze.json's existing precedent"

key-files:
  created:
    - src/flag_football_ep/features/explosiveness.py
    - tests/test_features_explosiveness.py
  modified: []

key-decisions:
  - "MIN_CALIBRATION_PLAYS = 10: the working floor below which a q80 threshold is dominated by
    a single extreme observation; a documented design choice, not an external standard."
  - "PRIOR_STRENGTH = 10.0: the beta-binomial shrinkage prior strength -- the attempt count at
    which a player's own rate and the pooled corpus rate carry equal weight in shrink_rate."
  - "Both HC baselines (workbook yards-only vs. spoken yards-OR-EPA) are computed as separate,
    separately labelled functions -- neither is silently treated as the correct one."
  - "hc_efficiency_table's Attempts-vs-Attempts+Drops ambiguity is an explicit drops_flag
    argument, never a resolved default, per the open question for the head coach."

patterns-established:
  - "Group-identity coalesce (thrown_by -> qb) mirrors reports/own_team.py::player_efficiency
    exactly, via a shared _with_group_key helper, so QB identity never drifts between modules."

requirements-completed: [HC-04]

# Metrics
duration: 60min
completed: 2026-09-03
---

# Phase M3-3 Plan 01: Explosiveness & Efficiency Metrics Module Summary

**New `features/explosiveness.py` module that reproduces the head coach's own workbook and spoken explosive-play rules verbatim as two separately-labelled baselines, then adds a corpus-calibrated IsoPPP-style explosiveness rate with a versioned JSON calibration artifact, a continuous (0,1) score that closes the 11-vs-12-yard cliff, and a four-definition comparison rollup with honest small-sample handling.**

## Performance

- **Duration:** ~60 min (three task commits between 20:42 and 21:39 local time)
- **Started:** 2026-09-03T18:39:57Z
- **Completed:** 2026-09-03T19:39:17Z
- **Tasks:** 3 (all `type="auto" tdd="true"`)
- **Files modified:** 2 (both new)

## Accomplishments

- `scrimmage_plays` is the one shared scope filter every function in the module routes
  through, excluding `down == 0` PAT rows, `no_play` and special-teams rows (RESEARCH
  Pitfall 5), and raising `MissingExplosivenessColumns` on any absent required column
  instead of silently deflating a rate.
- `hc_workbook_explosive_rate` reproduces the workbook's literal `yards_gained > 12`,
  pass-only formula with a code-level proof (grep-gated) that it never references `epa`;
  `hc_verbal_explosive_rate` reproduces the head coach's separately-stated
  `yards_gained > 12 OR epa > 0` rule as a distinct, distinctly labelled function. Neither
  silently stands in for the other.
- `hc_efficiency_table` reproduces `SUMIF(Data!O)/(Attempts+Drops)` literally over the opaque
  charted `efficiency` column, sums out-of-domain values (e.g. an observed `9`) as-is while
  separately counting them, and exposes the Attempts-vs-Attempts+Drops ambiguity via an
  explicit `drops_flag` argument rather than resolving it.
- `calibrate`/`ExplosivenessCalibration`/`write_calibration`/`load_calibration` derive the
  explosiveness threshold as the polars-native q80 of successful-play EPA, never a hard-coded
  constant, and persist it with `schema_version`, a sha256 `corpus_fingerprint` over
  `(game_id, play_id, epa, yards_gained)`, `corpus_n`, `n_success` and `corpus_sources`.
- `explosive_epa_flag` (binary, inclusive at the threshold) and `explosive_score` (continuous
  logistic in (0,1), exactly 0.5 at the threshold) share one calibration; the
  11-vs-12-yard headline regression test proves the score moves by < 0.05 between comparable
  plays regardless of yardage.
- `DEFINITIONS`/`definition_comparison` wrap `reports.aggregate.rate_table` (imported, never
  duplicated) across all four definitions, filling every known group in with `n == 0` when a
  definition's scope excludes it, and adding `shrunk_rate` (beta-binomial shrinkage,
  `PRIOR_STRENGTH = 10.0`) as an additive column alongside the untouched `rate`/CI/`muted`.
  `cliff_zone_table` renders the 8-16 yard window with a `hc_explosive` flag that flips
  exactly between 12 and 13.

## Task Commits

Each task was committed atomically:

1. **Task 1: Scrimmage filter, the two head-coach baselines, and the literal Efficiency
   reproduction** - `7ebf864` (feat)
2. **Task 2: Corpus-calibrated explosiveness, success rate, continuous score, versioned
   calibration artifact** - `afb8b64` (feat)
3. **Task 3: Definition comparison rollup with honest small samples** - `2c812c6` (feat)

_Note: each task combines its test coverage and implementation in one commit (bundled
test+feat rather than a separate RED-then-GREEN commit pair per task) -- see Deviations below._

## Files Created/Modified

- `src/flag_football_ep/features/explosiveness.py` (715 lines) - the metrics module: scrimmage
  filter, HC baselines, literal Efficiency reproduction, corpus-calibrated explosiveness with
  versioned calibration artifact, continuous score, definition comparison rollup, cliff-zone
  table.
- `tests/test_features_explosiveness.py` (565 lines) - 36 tests, one per `<behavior>` bullet
  across all three tasks, built exclusively on `flag_football_ep.testing.canonical_plays_with_scores`
  synthetic fixtures.

## Decisions Made

- `MIN_CALIBRATION_PLAYS = 10` and `PRIOR_STRENGTH = 10.0` are documented as design choices in
  the module's own comments (not silently chosen) -- see `key-decisions` above for rationale.
- Group-identity resolution (`thrown_by` coalesced onto `qb`) is centralized in a
  `_with_group_key` helper shared by all three HC-baseline functions, mirroring
  `reports/own_team.py::player_efficiency`'s existing fallback exactly rather than
  re-implementing it.
- `definition_comparison` builds a "group universe" from the full scrimmage corpus and left-joins
  each definition's `rate_table` result onto it, so a definition whose scope excludes a group
  entirely (e.g. `baseline_hc_workbook` on a run-only fixture) still reports that group with
  `n == 0` instead of the row silently vanishing from the concatenated output.

## Deviations from Plan

### Process deviation (not a Rule 1-4 code deviation)

**Per-task commits bundle test coverage and implementation into a single `feat` commit,
rather than a separate `test(...)`-then-`feat(...)` RED/GREEN pair per task.**
- **Reason:** All three tasks build incrementally on the same two files across a single
  cohesive module design; the test coverage for each task's `<behavior>` bullets was authored
  and verified against that task's implementation together, then committed as one atomic,
  fully-passing unit per task (test file + source file for that task's scope in one commit).
  Every commit's tests pass at commit time; the full `<behavior>` matrix was satisfied on the
  first implementation pass for every task (no fix-up cycles were needed).
- **Impact:** No functional difference -- git history still shows one commit per task with a
  clear before/after diff scoped to that task's functions. The RED phase (failing test proving
  the feature doesn't yet exist) was not independently captured as its own commit; this is a
  process deviation from the strict TDD cadence, not a code-correctness issue.

No Rule 1-3 auto-fixes and no Rule 4 architectural changes were needed. The plan's design
(reproduce HC baselines first, calibrate from data, reuse `rate_table`/`MUTED_MIN_N`) was
implementable exactly as specified.

**Total deviations:** 1 process deviation (TDD commit cadence). **Impact:** None on
correctness or test coverage; all `<behavior>` bullets and `<done>` criteria are met.

## Issues Encountered

- The execution session stalled for roughly 10 minutes immediately after Task 2's commit
  (likely a machine sleep interruption, a known environment quirk for this project). The
  coordinator's resume message confirmed Task 2's commit (`afb8b64`) had already landed
  cleanly with no uncommitted changes; execution resumed directly with Task 3 as instructed.
  No rework was needed.
- The sandboxed worktree has no `cv` extras installed (`torch`, `supervision`, `rfdetr`, etc.
  -- optional per `pyproject.toml`, `uv sync --extra cv` was never run here) and no network
  access for CVAT-backed tests. Running the full `tests/` suite therefore shows pre-existing,
  unrelated failures in `tests/test_cv_*.py` (`ModuleNotFoundError`), `tests/test_cv_cvat.py`
  (network), and `tests/test_m2_*.py` (torch import) -- none of these touch
  `src/flag_football_ep/cv/**` or `scripts/hackathon/**`, both of which this plan's collision
  guard forbids touching. `tests/test_features_explosiveness.py`,
  `tests/test_features_mutations.py`, `tests/test_reports_aggregate.py` and
  `tests/test_reports_own_team.py` are all green, as is the rest of the suite excluding those
  pre-existing, environment-gated files.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- The module's public signatures (`scrimmage_plays`, `hc_workbook_explosive_rate`,
  `hc_verbal_explosive_rate`, `hc_efficiency_table`, `calibrate`/`ExplosivenessCalibration`/
  `write_calibration`/`load_calibration`, `success_flag`, `explosive_epa_flag`,
  `explosive_score`, `DEFINITIONS`/`definition_comparison`/`shrink_rate`, `cliff_zone_table`)
  are the contract M3-03-02 (run on the real corpus, write the German proposal, ask the head
  coach the three open questions) and M3-4 (render the report) build on directly.
- `data/reference/explosiveness/` (the calibration artifact's on-disk home) is intentionally
  NOT created by this plan -- `write_calibration`/`load_calibration` take an explicit path
  argument; M3-03-02 owns choosing and creating that path against the real corpus.
- The three open questions this module encodes as explicit arguments/comparisons rather than
  resolving (workbook-vs-verbal EPA discrepancy, `Data!O` Efficiency semantics, Attempts-vs-
  Attempts+Drops ambiguity) are ready for M3-03-02 to surface to the head coach verbatim.

---
*Phase: M3-03-explosiveness-efficiency*
*Completed: 2026-09-03*

## Self-Check: PASSED

- `src/flag_football_ep/features/explosiveness.py` - FOUND
- `tests/test_features_explosiveness.py` - FOUND
- `.planning/phases/M3-03-explosiveness-efficiency/M3-03-01-SUMMARY.md` - FOUND
- Commit `7ebf864` (Task 1) - FOUND in `git log`
- Commit `afb8b64` (Task 2) - FOUND in `git log`
- Commit `2c812c6` (Task 3) - FOUND in `git log`
- `uv run pytest tests/test_features_explosiveness.py -q` - 36 passed
- `uv run pytest tests/test_features_mutations.py tests/test_reports_aggregate.py tests/test_reports_own_team.py -q` - all passed
- `git diff --name-only 7afa61a..HEAD` - exactly the two owned files
- `grep -rn "hc_files" src/flag_football_ep/features/explosiveness.py tests/test_features_explosiveness.py` - no matches
