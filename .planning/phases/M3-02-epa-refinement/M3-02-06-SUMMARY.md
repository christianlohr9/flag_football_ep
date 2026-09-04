---
phase: M3-02-epa-refinement
plan: 06
subsystem: reporting
tags: [polars, clopper-pearson, hc-workbook, out-of-fold, epa-comparison]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "M3-02-03's read-only hc_sp_tables snapshot (sp_by_dd/ep_by_dd/sample_size_by_dd,
      clustered variants) and M3-02-05's with-head-coach LOGO arm (92 trainable HC games,
      6,818 rows) that left oof_predictions_ep.parquet on disk with hc_workbook: rows in it"
provides:
  - "src/flag_football_ep/reports/hc_comparison.py: field_half_expr/distance_bin_expr (the
    down/distance/field-half axis), empirical_sp (his rows recomputed, our rows, both via
    reports.aggregate.rate_table), model_ep_per_cell (out-of-fold expected points per cell,
    joined on game_id/play_id, using add_ep_variables's own weighting), comparison_table
    (the four-way outer join with missing_in gap marking) and coverage_table"
  - "scripts/epa_comparison.py: loads the real corpus + the with-HC oof file + the M3-02-03
    snapshot, asserts the oof file is genuinely the with-HC arm, re-buckets his uncluttered
    axis onto the same 1..14+ grid, builds the clustered comparison on his own/opposite-half
    bin edges, and writes the three committed CSVs"
  - "data/reference/epa_refinement/comparison_by_dd.csv (120 cells), comparison_clustered.csv
    (48 cells), comparison_coverage.csv (4 rows) -- his published SP/EP/n next to our
    recomputation of his rows, our non-HC rows, and the model's out-of-fold EP, every cell
    with n on every side"
affects: [M3-02-07-german-deliverable, M3-02-08-promotion-checkpoint]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "n-weighted re-bucketing of a published table onto a coarser axis (
      build_hc_published_uncluttered): distances above the individual cutoff are pooled with
      a sample-size-weighted average for SP/EP and a plain sum for n, not a mean of ratios"
    - "shared _scope_to_axis/_scope_to_clustered_axis helper: PAT (down==0) exclusion
      extended to every axis column -- a row with a null down/distance_bin/field_half is not
      a cell with a missing value, it is not a cell at all, and must not form a spurious
      null-key group"
    - "axis-specific twins outside the locked public module (empirical_sp_clustered,
      model_ep_per_cell_clustered in the script) instead of parameterizing the Task 1/2
      functions, keeping hc_comparison.py's tested contract exactly as specified while still
      reusing rate_table and the exact expected-points weighting for the second axis"

key-files:
  created:
    - src/flag_football_ep/reports/hc_comparison.py
    - tests/test_reports_hc_comparison.py
    - scripts/epa_comparison.py
    - data/reference/epa_refinement/comparison_by_dd.csv
    - data/reference/epa_refinement/comparison_clustered.csv
    - data/reference/epa_refinement/comparison_coverage.csv
  modified: []

key-decisions:
  - "abs_diff_hc_vs_model compares hc_published_ep against model_ep (points vs points), not
    hc_published_sp against model_ep (probability vs points) -- the plan named both an SP
    and an EP head-coach column without pinning which one the model diff uses; points-vs-
    points is the only unit-consistent reading"
  - "missing_in is a two-bucket classification (his published table vs. the corpus-derived
    side as a whole), not a per-source flag -- a key present in hc_published and ours but
    absent from hc_rows_ours (his own rows recomputed) still reads missing_in=null, because
    that specific gap is already visible as a null hc_recomputed_sp cell in the table itself"
  - "the RESEARCH/plan text's second spot-value anchor (down=1/distance=10/own-half n=324)
    does not hold as a single-down cell -- reused M3-02-03's own prior finding that the real
    n=324 is a workbook Total row summed across all four downs (42+133+102+47), and checked
    the reinterpreted (summed) form instead of failing the run over a plan-text ambiguity
    already resolved once before"
  - "his uncluttered SP/EP tables are re-bucketed onto our corpus's 1..14/15+ axis with an
    n-weighted average (not a plain mean of the individual-distance ratios) so a distance-34
    cell with n=2 doesn't count as much as a distance-15 cell with n=200 inside the pooled
    15+ bin"

requirements-completed: [HC-03]

# Metrics
duration: ~2h
completed: 2026-09-04
---

# Phase M3-02 Plan 06: HC-vs-model comparison table Summary

**Four-way comparison table (his published SP/EP, his rows recomputed, our non-HC rows, the model's out-of-fold EP) on 120 uncluttered and 48 clustered down/distance/field-half cells, with n and Clopper-Pearson on every empirical side and an explicit 4-row coverage gap report.**

## Performance

- **Duration:** ~2h
- **Started:** 2026-09-04T06:55:00Z (approx.)
- **Completed:** 2026-09-04T07:30:00Z (approx.)
- **Tasks:** 3
- **Files modified:** 6 (4 created under `src`/`scripts`/`tests`, 3 CSVs generated and committed)

## Accomplishments

- `field_half_expr`/`distance_bin_expr`/`empirical_sp` (Task 1) build the down/distance/
  field-half axis and reuse `reports.aggregate.rate_table` for every n and Clopper-Pearson
  interval -- no second implementation. `THIN_MIN_N` (30) is a documented threshold distinct
  from `rate_table`'s own `MUTED_MIN_N` (5), so a cell at n=20 reads as "confidently present
  but visibly thinner" rather than either "empty" or "as solid as a n=300 cell".
- `model_ep_per_cell`/`comparison_table`/`coverage_table` (Task 2) join
  `oof_predictions_ep.parquet` on `(game_id, play_id)` -- provably out-of-fold, never a
  champion re-score -- and outer-join all four sources so no key is dropped and no missing
  side is ever coalesced to 0. `missing_in` marks one-sided keys explicitly; `abs_diff_hc_vs_
  model` and `abs_diff_hc_published_vs_hc_recomputed` compare matching units (points-to-
  points, probability-to-probability respectively); no winner/rank/score column exists.
- `scripts/epa_comparison.py` (Task 3) ran the real comparison: 6,818 head-coach rows vs.
  21,437 non-head-coach rows from `plays.parquet`, joined against the with-head-coach LOGO
  arm's out-of-fold predictions (asserted in code to actually be the with-HC arm before
  anything else runs). Both spot values from the real workbook check out. Re-running the
  script reproduces all three CSVs byte-for-byte.
- Found and fixed a real bug mid-Task-3: rows with a null `yardline_50`/`yards_to_go` (4,769
  rows corpus-wide have null `yards_to_go`) have no defined `field_half`/`distance_bin` and
  were forming their own spurious null-key "cell" in both axes instead of being excluded the
  same way a PAT row already is. `_scope_to_axis`/`_scope_to_clustered_axis` fix this; two
  new tests cover it.

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: axis + empirical_sp tests** - `62c38e5` (test)
2. **Task 1 GREEN: axis functions and empirical_sp** - `66e200b` (feat)
3. **Task 2 RED: model column + comparison_table tests** - `481360b` (test)
4. **Task 2 GREEN: model_ep_per_cell, comparison_table, coverage_table** - `b27132f` (feat)
5. **Task 3: run on the real corpus, commit the tables** - `66b4cc1` (feat)
6. **Task 3 follow-up: null-axis exclusion for the clustered comparison** - `a7401c8` (fix)

**Plan metadata:** this SUMMARY's own commit.

## Files Created/Modified

- `src/flag_football_ep/reports/hc_comparison.py` (373 lines) - the axis, `empirical_sp`,
  `model_ep_per_cell`, `comparison_table`, `coverage_table`, `_scope_to_axis`
- `tests/test_reports_hc_comparison.py` (475 lines, 26 tests) - full behavioral coverage of
  every function above
- `scripts/epa_comparison.py` (472 lines) - loads the real inputs, re-buckets his uncluttered
  table onto our 1..14+ axis (n-weighted), builds the clustered comparison on his own/
  opposite-half bin edges, checks both spot values, writes the three CSVs
- `data/reference/epa_refinement/comparison_by_dd.csv` - 120 rows, 0 coverage gaps (every
  key present on both sides after re-bucketing)
- `data/reference/epa_refinement/comparison_clustered.csv` - 48 rows, 4 coverage gaps
- `data/reference/epa_refinement/comparison_coverage.csv` - 4 rows (all clustered axis),
  `axis` column disambiguates which table each gap came from

## Decisions Made

See `key-decisions` in the frontmatter. In short: `abs_diff_hc_vs_model` uses points-vs-
points (`hc_published_ep` vs `model_ep`); `missing_in` classifies by side (published vs.
corpus-derived as a whole), not per individual source column; the plan's second spot-value
anchor was reinterpreted as a downs-summed total per M3-02-03's own prior finding rather than
treated as a fresh join-key misalignment; his uncluttered SP/EP tables are pooled into the
`15+` bin with an n-weighted average, not a plain mean of ratios.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Null-axis rows formed spurious null-key cells in both axes**
- **Found during:** Task 3, first real run against `plays.parquet`
- **Issue:** `field_half_expr`/`distance_bin_expr` correctly return null for rows with a
  null `yardline_50`/`yards_to_go` (by design -- a `[VERIFIED]` requirement of Task 1), but
  neither `empirical_sp` nor `model_ep_per_cell` excluded those null-key rows before
  grouping. They formed their own `(down, null, field_half)`/`(down, distance_bin, null)`
  groups, which broke the plan's own automated verification (`field_half` values must be a
  subset of `{"own", "opponent"}`) and would have shown up as a nonsensical "cell" in the
  German write-up.
- **Fix:** Added `_scope_to_axis` (shared by `empirical_sp`/`model_ep_per_cell`) and
  `_scope_to_clustered_axis` (the script's clustered twin) extending the existing `down == 0`
  PAT exclusion to drop any row missing an axis component.
- **Files modified:** `src/flag_football_ep/reports/hc_comparison.py`,
  `tests/test_reports_hc_comparison.py`, `scripts/epa_comparison.py`
- **Verification:** two new tests
  (`test_null_yardline_row_excluded_not_a_null_field_half_cell`,
  `test_null_yards_to_go_row_excluded_not_a_null_distance_bin_cell`); the plan's own automated
  `<verify>` check (`field_half` subset of `{"own", "opponent"}`) passes; re-ran the script,
  `comparison_coverage.csv` dropped from 20 spurious rows to 4 genuine ones.
- **Committed in:** `66b4cc1` (uncluttered axis fix, same commit as the real run),
  `a7401c8` (clustered axis fix, found on a second read of the coverage output)

**2. [Rule 1 - Bug, carried forward from M3-02-03] Second spot-value anchor reinterpreted**
- **Found during:** Task 3, sanity-checking the two anchors from M3-02-RESEARCH/this plan's
  own `<interfaces>` block
- **Issue:** The plan text states "down 1 / distance 10 / own half should carry n = 324".
  The real snapshot's down=1/distance=10/own-half cell has n=42, not 324. M3-02-03's own
  SUMMARY had already investigated this exact ambiguity and found the real n=324 is a
  workbook "Total" row summed across all four downs (42+133+102+47=324), not a single-down
  cell -- the RESEARCH text's "down=10/distance=1" and the plan's "down=1/distance=10" are
  both typos for a total that was never a per-down cell to begin with.
- **Fix:** The anchor check verifies the reinterpreted (downs-summed) form and prints both
  the down-1-only value and the summed total, so the discrepancy stays visible in the script's
  own output rather than being silently reconciled a second time without a record.
- **Files modified:** `scripts/epa_comparison.py`
- **Verification:** `anchor 2 OK (reinterpreted as a downs-summed total, per M3-02-03):
  distance=10 own, summed across downs -> n=324` printed on every run.
- **Committed in:** `66b4cc1`

---

**Total deviations:** 2 auto-fixed (both Rule 1 bugs). **Impact:** deviation 1 was necessary
for correctness -- without it the comparison tables would have carried a nonsensical
null-key row and failed the plan's own machine verification. Deviation 2 carries forward a
finding M3-02-03 already made rather than re-discovering (or silently re-deciding) it; no
scope creep, no file outside this plan's `files_modified` touched.

## Issues Encountered

None beyond the deviations above -- no blockers, no auth gates, no package installs.

## Measured Results (for M3-02-07)

### Cell counts and thin-cell counts (uncluttered axis, `comparison_by_dd.csv`)

- 120 cells total, 0 with `missing_in` set (every published cell also has recomputed/ours/
  model coverage once re-bucketed onto the same 1..14+ axis).
- `hc_recomputed_thin` (his rows recomputed, n < 30): 78 of 120 cells (65%) -- his own rows,
  even after admission-rule enlargement to 92 trainable games, are frequently thin per
  individual down/distance/field-half cell.
- `ours_thin` (non-head-coach rows, n < 30): 16 of 120 cells (13%) -- the rest of the corpus
  is comparatively deep almost everywhere on this axis.

### Coverage gaps (`comparison_coverage.csv`, 4 rows, all clustered axis)

All four are the clustered `"25+"` opposite-half bin (yards-to-go >= 26 while inside the
opponent's half), one per down (n=1, 18, 16, 9 in his published table respectively). Our
corpus has essentially zero plays there -- physically close to impossible in flag football
(needing 26+ yards for a first down while already inside the opponent's 25-yard half). This
reads as a genuine rare-situation gap, not a join defect: the uncluttered axis (same
situations, unclustered) has zero coverage gaps.

### Largest disagreements, head coach vs. model (`abs_diff_hc_vs_model`, points)

| down | distance | field_half | hc_published_ep (n) | model_ep (n) | abs diff |
|---|---|---|---|---|---|
| 4 | 7 | opponent | 3.72 (29) | 1.02 (51) | 2.70 |
| 4 | 6 | own | 2.91 (70) | 0.36 (134) | 2.55 |
| 4 | 12 | own | 1.50 (40) | -0.76 (82) | 2.26 |
| 4 | 11 | own | 1.54 (39) | -0.72 (77) | 2.25 |
| 3 | 7 | opponent | 5.53 (51) | 3.35 (84) | 2.18 |

All five largest disagreements are 4th (and one 3rd) down -- the model is consistently more
pessimistic than his published table on later-down, mid-distance situations, and the model's
own n is always larger (out-of-fold predictions cover the pooled corpus, his published table
only his own workbook rows).

### Largest disagreements, his published vs. his own rows recomputed (`abs_diff_hc_published_vs_hc_recomputed`, probability)

| down | distance | field_half | hc_published_sp (n) | hc_recomputed_sp (n) | abs diff |
|---|---|---|---|---|---|
| 1 | 14 | own | 0.33 (3) | 1.00 (1) | 0.67 |
| 1 | 7 | own | 0.60 (5) | 1.00 (1) | 0.40 |
| 4 | 14 | opponent | 0.13 (24) | 0.50 (12) | 0.38 |
| 1 | 11 | own | 0.67 (3) | 1.00 (1) | 0.33 |
| 4 | 10 | opponent | 0.25 (32) | 0.58 (19) | 0.33 |

Every one of these five sits on n <= 19 on at least one side (three at n=1 recomputed) -- the
largest published-vs-recomputed gaps are exactly the cells the `thin`/`muted` flags already
warn about, not evidence of a definition mismatch on the wider, better-sampled cells.

### His EP vs. six times his SP

Mean absolute gap between `hc_published_ep` and `6 * hc_published_sp`: **0.0000** over all
120 cells with both values present -- his published EP is, to floating-point precision,
exactly six times his published SP (a touchdown is worth 6 points and his EP tab appears to
be a pure scalar transform of his SP tab, not an independently estimated expectation).

## User Setup Required

None - no external service configuration required.

## Known Stubs

None -- every committed CSV is real, measured data from the real corpus and the real
snapshot; no placeholder values ship in any file.

## Threat Flags

None beyond the plan's own `<threat_model>` register. `oof sanity check` (T-M3-02-23),
the two spot-value anchors (T-M3-02-24) and `missing_in`/`comparison_coverage.csv`
(T-M3-02-25) all ran and passed on the real corpus; no player/team column is read, joined or
emitted anywhere in this plan's code (T-M3-02-26); no winner/rank/score column exists
(T-M3-02-27).

## Next Phase Readiness

- M3-02-07 (German write-up) has the largest head-coach-vs-model and published-vs-recomputed
  disagreement tables above, the EP=6*SP finding, the thin-cell shares, and the 4-row
  coverage gap (with the "26+ yards to go inside the opponent's half is nearly impossible"
  explanation) ready to quote directly.
- `data/reference/hc_sp_tables/**` (the M3-02-03 snapshot) verified untouched:
  `git status --porcelain data/reference/hc_sp_tables/` is empty.
- No blockers.

## Self-Check

Files (all `[ -f ]` checked):
- `src/flag_football_ep/reports/hc_comparison.py` -- FOUND
- `tests/test_reports_hc_comparison.py` -- FOUND
- `scripts/epa_comparison.py` -- FOUND
- `data/reference/epa_refinement/comparison_by_dd.csv` -- FOUND
- `data/reference/epa_refinement/comparison_clustered.csv` -- FOUND
- `data/reference/epa_refinement/comparison_coverage.csv` -- FOUND

Commits (`git log --oneline`):
- `62c38e5` (Task 1 RED) -- FOUND
- `66e200b` (Task 1 GREEN) -- FOUND
- `481360b` (Task 2 RED) -- FOUND
- `b27132f` (Task 2 GREEN) -- FOUND
- `66b4cc1` (Task 3 real run) -- FOUND
- `a7401c8` (Task 3 clustered-axis fix) -- FOUND

Plan-level verification re-run:
- `uv run pytest tests/test_reports_hc_comparison.py tests/test_reports_aggregate.py -q` -- 58 passed (26 + 32)
- `uv run python scripts/epa_comparison.py && git status --porcelain data/reference/epa_refinement/` -- clean on the second run (byte-identical CSVs)
- Both spot values reproduce in `comparison_by_dd.csv` (down=1/distance=1/own sp=0.6667 n=21; distance=10/own summed across downs n=324)
- No committed comparison CSV contains a player or team name (checked for `home`/`away`/`hudl`/`ifaf`/`hc_workbook` tokens and capitalized-name patterns)
- `git status --porcelain data/reference/hc_sp_tables/` -- empty

## Self-Check: PASSED

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-04*
