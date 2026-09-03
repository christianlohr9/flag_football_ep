---
phase: M3-02-epa-refinement
plan: 02
subsystem: model
tags: [polars, xgboost, feature-engineering, ep-model, competition-tier, hc-workbook]

# Dependency graph
requires:
  - phase: M3-01-hc-workbook-ingest
    provides: hc_workbook source labels (hc_source_label), the real-run finding that 0 HC
      rows reach plays.parquet today (half_assigned quarantine)
provides:
  - EP_HALF_UNKNOWN_SENTINEL (0) and HC_SOURCE_PREFIX ("hc_workbook:") constants in
    features/mutations.py
  - make_ep_model_mutations half-column overwrite for hc_workbook-sourced rows, proven a
    no-op for every other source
  - three data/reference/competition_tier.csv rows so hc_workbook (source, competition)
    pairs do not hard-fail train_ep/train_wp with UnmappedCompetitionError
  - regression coverage pinning the half=2 label-construction sentinel (EP No_Score/WP
    Winner) independent of the model-feature sentinel
affects: [M3-02-01 (hc_workbook ingest, must stamp half=2 and the exact source/competition
  strings this plan's tier rows key on), M3-02-04 (hc_games.csv refill, competition
  vocabulary contract), M3-02-05 (training waves, half/tier ablation), M3-02-07 (German
  deliverable, ASSUMED tier rationale)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Label-construction sentinel (half=2, upstream) decoupled from model-feature sentinel
      (EP_HALF_UNKNOWN_SENTINEL=0, downstream) via a source-prefix-scoped overwrite
      immediately before final column selection"
    - "Reference-CSV tier gap closed defensively: no row added for sources/competitions not
      yet in the trainable corpus, so an accidental future inclusion fails loudly via
      UnmappedCompetitionError instead of silently inheriting a tier"

key-files:
  created: []
  modified:
    - src/flag_football_ep/features/mutations.py
    - tests/test_features_mutations.py
    - tests/test_model_train.py
    - data/reference/competition_tier.csv

key-decisions:
  - "EP_HALF_UNKNOWN_SENTINEL = 0 (Int32), applied only to source values starting with
    hc_workbook: -- [ASSUMED] per M3-02-RESEARCH section 2.3, not yet locked; the ablation
    in M3-02-05 reports the measured effect versus feeding half=2 through unchanged"
  - "Three new competition_tier.csv rows, all tier=mixed-other: hc_workbook:scoring-
    probability-by-situation-2023-2026:data x {HC Charting 2023-2026, legacy}, and
    hc_workbook:offense-analytics-2026-camps-and-competitions:data x HC Camps 2026 --
    [ASSUMED] per RESEARCH assumption A4, to be raised with the head coach in M3-02-07"
  - "No tier row added for hc_workbook:...:copy-of-data, the EC-2025 workbook, or the
    Scoring Probability team-pair games -- deliberate absence, a second independent guard
    behind the half sentinel forcing a conscious tier decision if one of those games is
    ever declared trainable"

requirements-completed: [HC-03]

# Metrics
duration: ~2h (interrupted mid-session by a machine-sleep stall; net working time shorter)
completed: 2026-09-03
---

# Phase M3-02 Plan 02: EP half-sentinel decoupling and head-coach competition-tier rows Summary

**Head-coach rows get an EP_HALF_UNKNOWN_SENTINEL (0) model-feature value distinct from the half=2 label-construction constant, and three new `competition_tier.csv` rows close the UnmappedCompetitionError gap before the first HC-inclusive `ffep train` run.**

## Performance

- **Duration:** ~2h wall clock across the session (one long machine-sleep interruption mid-way; task work itself was three focused implementation passes)
- **Started:** 2026-09-03 (session start)
- **Completed:** 2026-09-03T19:53:00Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments
- Pinned the constant-half-2 label-construction chain (both EP's `No_Score`/`Next_Score_Half` and WP's `Winner`) against `_mark_half_end`, including the half-1 and null-half regression cases from M3-02-RESEARCH section 2.2's decision table
- Added a row-scoped, source-prefix-matched `half` overwrite in `make_ep_model_mutations` so head-coach rows carry a distinct "unknown half" signal in the EP feature matrix while `half=2` still drives label construction upstream
- Closed the previously-undocumented competition-tier gap (RESEARCH Pitfall 3): three `hc_workbook:` rows now cover every `(source, competition)` pair this phase's HC corpus will produce, with the `UnmappedCompetitionError` failure mode pinned by a regression test

## Task Commits

Each task was committed atomically:

1. **Task 1: Pin the label-construction chain under a constant half=2 game** - `9245561` (test)
2. **Task 2: EP half-feature sentinel for head-coach rows** - `9e20de4` (feat)
3. **Task 3: Competition-tier rows for every head-coach source** - `d06d683` (feat)

**Plan metadata:** committed alongside this SUMMARY.

## Files Created/Modified
- `src/flag_football_ep/features/mutations.py` - `EP_HALF_UNKNOWN_SENTINEL`/`HC_SOURCE_PREFIX` constants; `make_ep_model_mutations` overwrites `half` to the sentinel for `hc_workbook:`-sourced rows immediately before final column selection
- `tests/test_features_mutations.py` - `TestHalfSentinelLabelConstruction` (6 tests, label-construction chain) and `TestMakeEpModelMutationsHalfSentinel` (8 tests, model-feature sentinel + no-op proof + WP-path-untouched proof)
- `tests/test_model_train.py` - `_hc_ep_training_corpus` helper plus 4 tests covering the `UnmappedCompetitionError` failure mode, the fix, and two read-only assertions against the real repo `competition_tier.csv`
- `data/reference/competition_tier.csv` - three new `hc_workbook:` rows appended after the existing three (ifaf/legacy/legacy-sportapp), all unchanged

## Decisions Made

**Competition vocabulary contract for M3-02-04** (must be reused verbatim when `hc_games.csv` is filled):
- `hc_workbook:scoring-probability-by-situation-2023-2026:data` x `HC Charting 2023-2026` -> `mixed-other`
- `hc_workbook:scoring-probability-by-situation-2023-2026:data` x `legacy` -> `mixed-other` (defensive: the nine confirmed `legacy-39..47` duplicate rows carry `competition=legacy`; expected to be removed by `hc_dedupe` before reaching `plays.parquet`, but this row prevents a hard training failure if dedupe's preference ever changes)
- `hc_workbook:offense-analytics-2026-camps-and-competitions:data` x `HC Camps 2026` -> `mixed-other`

All three tier assignments are `[ASSUMED]` (RESEARCH assumption A4: camps/scrimmage segments have no established tier; `mixed-other` mirrors the existing `legacy`/`legacy-sportapp` precedent). Risk if wrong: head-coach games are systematically mis-tiered in the tier covariate -- low per-row impact, moderate impact on the tier comparison table. This is the exact question M3-02-07 must raise with the head coach in `docs/hc-rueckfragen-2026-09.md`.

**Deliberately no tier row for:** `hc_workbook:...:copy-of-data` (Frage 2 unresolved), `hc_workbook:germany-analytics-stats-ec-2025-vs-wc-nations:data` (Frage 1 unresolved), and the Scoring Probability team-pair games (null `posteam`/`defteam` by design, could not produce an EP/WP label regardless). The absence is intentional: if a maintainer ever declares one of these games trainable, `UnmappedCompetitionError` fires and forces a conscious tier decision instead of silently inheriting one.

**EP_HALF_UNKNOWN_SENTINEL = 0**, not `null` or a re-use of `2`: `0` is outside the real domain `{1, 2}` so XGBoost can split "unknown half" off as its own signal, and it is a plain Int32 (no schema change). Documented `[ASSUMED]` in the source comment per RESEARCH section 2.3 -- the alternative (feed `2` through unchanged, undoing the decoupling) is a one-line revert, and M3-02-05's ablation will report the measured effect.

**No source contradicted M3-02-RESEARCH section 2.2.** Task 1's six tests (constant half=2, constant half=1, all-null half, EP `Next_Score_Half`, WP `Winner`, and two-game scoping) all passed against the current `mutations.py` with zero source changes -- the decision table's mechanism analysis was correct as written.

## Deviations from Plan

None - plan executed exactly as written. `make_ep_model_mutations`'s half-overwrite guard was extended slightly beyond the plan's literal wording (skip when *either* `source` or `half` is absent, not only `source`) as a defensive no-op for any narrower experiment-caller frame; this is within the plan's own stated intent ("Guard it so it is a strict no-op... some experiment callers pass narrower frames") and does not change behavior for any caller that has both columns.

## Issues Encountered

The execution session was interrupted by a machine sleep mid-way through initial source reading (a known environment condition, not a plan or code issue -- see project MEMORY.md "MacBook sleep interrupts agents"). Resumed cleanly from `git status`/`git log` with zero commits lost; no rework was needed since no commits had landed before the interruption.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `data/reference/competition_tier.csv` and the EP half-sentinel are both in place; M3-02-05's training waves can now include `hc_workbook:` rows once M3-02-01 (ingest) and M3-02-04 (`hc_games.csv` refill) land without hitting `UnmappedCompetitionError` or diluting the `half` feature's measured signal.
- M3-02-04 MUST use the three `(source, competition)` strings recorded above verbatim -- a competition string invented later in `hc_games.csv` is a hard training failure, not a silent one.
- M3-02-07's German deliverable has three ready-made `[ASSUMED]` tier rationale bullets (above) to hand to the head coach as a confirmation question, plus the `[ASSUMED]` half-sentinel note for the same document.
- No blockers for this plan's own scope. `tests/test_migration_equivalence.py` confirms the 1.3 baseline (no `hc_workbook` rows in scope) is byte-for-byte unaffected by either change.

## Self-Check: PASSED

- `src/flag_football_ep/features/mutations.py` contains `EP_HALF_UNKNOWN_SENTINEL` and `HC_SOURCE_PREFIX` -- FOUND
- `data/reference/competition_tier.csv` has exactly 3 `hc_workbook:` rows and the original 3 rows intact -- FOUND (verified via `load_competition_tier` height=6 and `git diff --stat` showing 3 added / 0 modified lines)
- Commits `9245561`, `9e20de4`, `d06d683` exist on `worktree-agent-ac28af2ef07a548a4` -- FOUND (`git log --oneline`)
- Plan-level verification `uv run pytest tests/test_features_mutations.py tests/test_model_train.py tests/test_migration_equivalence.py -q` -- PASSED (exit code 0, 1 skip: the real-corpus tier test that requires `data/processed/plays.parquet`, not present in this worktree)
- `uv run python -c "... load_competition_tier(...).height"` -- prints 6, matches plan verification
- `git diff --stat data/reference/competition_tier.csv` -- 3 insertions, 0 deletions, matches plan verification
- File collision guard respected: only `src/flag_football_ep/features/mutations.py`, `tests/test_features_mutations.py`, `tests/test_model_train.py`, `data/reference/competition_tier.csv` were written (`git diff --stat 1145de1 HEAD`) -- nothing under `ingest/`, `model/`, or `docs/`

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-03*
