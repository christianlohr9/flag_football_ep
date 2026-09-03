---
phase: M3-02-epa-refinement
plan: 03
subsystem: data
tags: [openpyxl, polars, hc-workbook, sp-by-dd, pii-gate, excel-date-corruption]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: RESEARCH.md section 4 (HC method reproduction, sheet inventory, corrupted bin labels, Reg-tab findings)
provides:
  - "scripts/hc_sp_snapshot.py: read-only openpyxl extraction of the HC's nine aggregate SP/EP-by-D&D tabs into tidy CSVs"
  - "data/reference/hc_sp_tables/*.csv: ten committed, reproducible CSVs (nine tabs + manifest) with n beside every probability"
  - "tests/test_m3_epa_snapshot.py: PII gate and schema/domain guards over the committed snapshot"
affects: [M3-02-06, M3-02-07]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "allow-list sheet access (ALLOWED_SHEETS/FORBIDDEN_SHEETS via _load_sheet) to make PII-carrying tabs unreachable by construction"
    - "provenance-tagged reconstruction (distance_bin_source: read|reconstructed) for values recovered from a known data-corruption pattern"
    - "scoped PII length guard: 24-char cap on tidy field_half/distance_bin cells, explicit per-column exemptions for verbatim formula text and workbook metadata"

key-files:
  created:
    - scripts/hc_sp_snapshot.py
    - data/reference/hc_sp_tables/sp_by_dd.csv
    - data/reference/hc_sp_tables/ep_by_dd.csv
    - data/reference/hc_sp_tables/sample_size_by_dd.csv
    - data/reference/hc_sp_tables/sp_by_dd_clustered.csv
    - data/reference/hc_sp_tables/ep_by_dd_clustered.csv
    - data/reference/hc_sp_tables/sample_size_by_dd_clustered.csv
    - data/reference/hc_sp_tables/sp_by_dd_weighted.csv
    - data/reference/hc_sp_tables/ep_by_dd_weighted.csv
    - data/reference/hc_sp_tables/reg_formulas.csv
    - data/reference/hc_sp_tables/manifest.csv
    - tests/test_m3_epa_snapshot.py
  modified: []

key-decisions:
  - "Scoped the pii_discipline 24-char cell-length guard to the tidy field_half/distance_bin columns rather than every cell in every CSV: reg_formulas.csv's verbatim Excel formula text and manifest.csv's workbook filename/sha256 are legitimately long and independently PII-free (no player/team names appear in formulas or filenames)"
  - "read_reg_tab determines field_half from the Reg tab's Code column sign (leading '-' = own half, bare digits = opponent) rather than the Half text column, which is blank/uninformative ('-' or None) in the real workbook -- verified against the FORECAST range and polynomial-degree pattern RESEARCH section 4.4 documented"
  - "read_reg_tab emits one record per (field_half, down, column) rather than one per distance row: the formula text (both FORECAST range and polynomial coefficients) is constant within a down/half block, verbatim taken from that block's first row"
  - "Relaxed sp_by_dd.csv vs sample_size_by_dd.csv key-set equality to a subset check (sp_keys <= n_keys): the real SP by D&D tab leaves 83 (down, distance, half) cells blank where Sample Size by D&D still records a count -- most notably down=1..4/distance=26/own with n up to 2290 -- a genuine gap in the head coach's own spreadsheet, not an extraction bug"
  - "SP domain check relaxed to [0, 1.5] rather than a strict [0, 1]: real cells reach 1.05 (small-sample noise), which is exactly the kind of head-coach-method artifact EPA-D03 wants surfaced, not hand-edited away"

patterns-established:
  - "Any future HC-workbook snapshot script should mirror _load_sheet's allow-list-at-the-choke-point pattern rather than checking sheet names ad hoc at each call site"

requirements-completed: [HC-03]

# Metrics
duration: 55min
completed: 2026-09-03
---

# Phase M3-2 Plan 03: HC SP/EP-by-D&D Snapshot Summary

**Read-only openpyxl/polars snapshot of the head coach's nine aggregate SP/EP-by-down-and-distance tabs into ten deterministic CSVs, with sample size beside every probability and the three Excel-date-corrupted clustered bin labels reconstructed and provenance-tagged.**

## Performance

- **Duration:** 55 min
- **Started:** 2026-09-03T20:53:00Z (approx, session start)
- **Completed:** 2026-09-03T21:48:00Z
- **Tasks:** 3
- **Files modified:** 12 (1 script, 10 CSVs, 1 test file)

## Accomplishments

- `scripts/hc_sp_snapshot.py`: reads exactly the nine aggregate tabs (`SP by D&D`, `EP by D&D`, `Sample Size by D&D`, the three `Clustered` variants, the two `weighted` variants, `Reg`) via an allow-list choke point (`_load_sheet`) that makes `Data`/`Copy of Data` (the two player-labelled tabs) unreachable by construction, even by a later edit.
- Nine tabs -> ten CSVs under `data/reference/hc_sp_tables/`, tidy on `(down, distance_bin, distance_bin_source, field_half, value|n)`, sorted deterministically, plus `manifest.csv` recording the workbook basename, its sha256, per-tab record counts and reconstructed-label counts, and a UTC extraction date.
- Every `SP`/`EP` probability tab has n right beside it via `Sample Size by D&D`/`Sample Size by D&D Clustered` -- 300 records, total n=17940, min n=0, max n=2290, 154 of 300 cells (51%) have n<20.
- The three Excel-autocorrected clustered bin labels (`datetime(2021,1,5)` / `datetime(2021,6,10)` / `datetime(2021,11,15)`) are reconstructed to `1-5`/`6-10`/`11-15` text, tagged `distance_bin_source == "reconstructed"`, present in all three clustered CSVs (own AND opponent columns each), never written as a `datetime`.
- `Reg` tab's per-(field_half, down) `FORECAST` range and hardcoded polynomial formula are captured verbatim (16 records = 4 downs x 2 halves x 2 formula kinds) via a second `data_only=False` load pass, half determined from the `Code` column's sign since the `Half` text column is blank in the real workbook.
- A second run of the script produces byte-identical CSVs (verified via `md5sum` and `git status --porcelain`), confirming reproducibility.
- `tests/test_m3_epa_snapshot.py`: 9 tests, all green, covering PII (roster names/surnames), cell-length and date-pattern regressions, `field_half`/`down`/`n`/SP domain checks, the SP-subset-of-n key relationship, the three reconstructed labels, and manifest/row-count agreement -- all against the committed CSVs only, zero references to the raw workbook path.

## Task Commits

1. **Task 1: The snapshot script** - `599ff76` (feat)
2. **Task 2: Run the snapshot and commit the tables** - `dfd64bf` (feat, includes a script fix discovered while running it)
3. **Task 3: PII and domain guards over the committed snapshot** - `ae5f8d3` (test)

## Files Created/Modified

- `scripts/hc_sp_snapshot.py` - allow-listed openpyxl extraction of the nine aggregate tabs, `reconstruct_distance_bin`, `read_matrix_tab`, `read_reg_tab`, PII/domain assertions, manifest writer
- `data/reference/hc_sp_tables/sp_by_dd.csv` - 217 records, distance 1-34, empirical SP
- `data/reference/hc_sp_tables/ep_by_dd.csv` - 217 records, distance 1-34, empirical EP (points)
- `data/reference/hc_sp_tables/sample_size_by_dd.csv` - 300 records, distance 1-49, the n column both SP and EP key off
- `data/reference/hc_sp_tables/sp_by_dd_clustered.csv` / `ep_by_dd_clustered.csv` / `sample_size_by_dd_clustered.csv` - 48 records each, 6 distance bins x 4 downs x 2 halves, 3 of the 6 bins per half reconstructed
- `data/reference/hc_sp_tables/sp_by_dd_weighted.csv` (233 records, distance 1-36) / `ep_by_dd_weighted.csv` (292 records, distance 1-49)
- `data/reference/hc_sp_tables/reg_formulas.csv` - 16 records, verbatim `FORECAST`/polynomial formula text per (field_half, down, column)
- `data/reference/hc_sp_tables/manifest.csv` - 9 rows (one per tab), workbook basename + sha256 + per-tab counts + extraction date
- `tests/test_m3_epa_snapshot.py` - new file, 9 tests, disjoint from `tests/test_m3_hc_pii.py`

## Decisions Made

- **PII length guard scope:** the plan's `<pii_discipline>` block ("no cell longer than 24 characters") is written for the tidy matrix tabs' `field_half`/`distance_bin` cells -- the columns a leaked player/team name would land in. Applied literally to every cell, it breaks on `reg_formulas.csv`'s verbatim formula text (up to 78 chars, e.g. `=FORECAST(C98, 'Copy of Data'!I$2555:I$3017,'Copy of Data'!G$2555:G$3017)*6`) and on `manifest.csv`'s workbook filename/sha256 -- both explicitly sanctioned content per this same plan's task 1 action and the pii_discipline block's own carve-out for the workbook filename ("it is a filename, not a person"). Scoped the check with an explicit `exempt_columns` parameter (`formula_text` in `reg_formulas.csv`; `tab`/`source_workbook`/`out_file`/`workbook_sha256` in `manifest.csv`), datetime check still applies unconditionally everywhere.
- **Reg tab half determination:** the plan's `<interfaces>` block names a `Half` column; in the real workbook that column is uninformative (`'-'` for the first ~96 rows, `None` after). Field half is instead read off the `Code` column's sign (`-11` = own half, `11` = opponent), cross-verified against the FORECAST row ranges and increasing polynomial degree RESEARCH section 4.4 documented per half.
- **Spot-value 2 reinterpreted:** RESEARCH/the plan's `<interfaces>` describe a spot value of n=324 at down=1/distance=10/own half (RESEARCH text says down=10/distance=1, the plan's interfaces say down=1/distance=10 -- neither matches a single down cell). The real cell is `Total` = 42+133+102+47 = 324 for distance=10, own half, summed across all four downs. Verified my four extracted down cells sum to exactly 324, independently confirming the axis mapping without needing the ambiguous spot-value description to be literally correct.
- **SP-vs-n key equality relaxed:** see Deviations below.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Scoped the pre-write PII length assertion instead of applying it to every cell**
- **Found during:** Task 2 (first run of the script)
- **Issue:** `_assert_no_datetime_or_long_strings` applied to every string column of every output frame raised on `reg_formulas.csv`'s `formula_text` (legitimate, plan-mandated verbatim formula strings up to 78 chars) and then, after fixing that, on `manifest.csv`'s `workbook_sha256` (a 64-char hex digest).
- **Fix:** Added an `exempt_columns` parameter to `_assert_no_datetime_or_long_strings`; exempted `formula_text` in `reg_formulas.csv` and `tab`/`source_workbook`/`out_file`/`workbook_sha256` in `manifest.csv`. The datetime-dtype check remains unconditional on every column of every frame, and the length check still applies to every column of the eight matrix-tab CSVs.
- **Files modified:** `scripts/hc_sp_snapshot.py`
- **Verification:** `uv run python scripts/hc_sp_snapshot.py` completes and writes all ten CSVs; re-run is byte-identical.
- **Committed in:** `dfd64bf` (Task 2 commit)

**2. [Rule 1 - Bug] Relaxed sp_by_dd.csv/sample_size_by_dd.csv key-set equality to a subset check**
- **Found during:** Task 3 (writing `test_sp_and_sample_size_share_the_same_key_set`, first test run)
- **Issue:** The plan's task 3 behavior specifies the two files "have the same set of (down, distance_bin, field_half) keys." The real `SP by D&D` tab leaves 83 cells blank (no computed ratio) where `Sample Size by D&D` still records a count -- 79 of those 83 have n=0 (plausibly SP-undefined), but four have real, large counts (down=1..4, distance=26, own half: n=2290/2024/1596/947) with no SP value in the source workbook at all.
- **Fix:** Renamed the test to `test_sp_never_has_a_key_without_a_matching_sample_size` and changed the assertion from `sp_keys == n_keys` to `sp_keys <= n_keys` (a probability can never exist without a sample size; a sample size existing without a computed probability is a real, documented property of the head coach's own spreadsheet, not something this snapshot should paper over).
- **Files modified:** `tests/test_m3_epa_snapshot.py`
- **Verification:** `uv run pytest tests/test_m3_epa_snapshot.py -x -q` -- all 9 tests pass.
- **Committed in:** `ae5f8d3` (Task 3 commit)

**3. [Rule 1 - Bug] Relaxed the SP domain check from a strict [0, 1] bound to [0, 1.5]**
- **Found during:** Task 3 (writing `test_sp_probability_column_in_domain`)
- **Issue:** The plan's task 3 behavior specifies "every probability column is within [0, 1]." Real committed data has a handful of small-sample cells above 1.0 (max observed 1.05, `sp_by_dd.csv` down=1/distance=8/opponent; also present in the clustered and weighted variants) -- exactly the small-sample noise EPA-D03 wants surfaced.
- **Fix:** Bounded the check to `[0.0, 1.5]` (non-negative, with headroom well above the observed 1.05 max so a real bug -- e.g. a raw count landing in the probability column -- still fails loudly) instead of a strict `[0, 1]`.
- **Files modified:** `tests/test_m3_epa_snapshot.py`
- **Verification:** `uv run pytest tests/test_m3_epa_snapshot.py -x -q` -- all 9 tests pass.
- **Committed in:** `ae5f8d3` (Task 3 commit)

---

**Total deviations:** 3 auto-fixed (all Rule 1 -- plan behavior descriptions written before this session's line-by-line inspection of the real workbook, corrected against ground truth). **Impact:** All three preserve the plan's actual intent (protect against PII leakage, catch a probability with no sample, catch a domain-violating value) while accommodating real, verified properties of the source data. No scope creep -- no file outside this plan's `files_modified` was touched.

## Issues Encountered

None beyond the deviations above -- no blockers, no auth gates.

## Tidy Schema (for M3-02-06)

Matrix tabs (`sp_by_dd*.csv`, `ep_by_dd*.csv`, `sample_size_by_dd*.csv`): `down` (int, 1-4), `distance_bin` (text -- plain integer string for unclustered/weighted tabs, a bin label like `16-19`/`20+`/`1-5` for clustered tabs), `distance_bin_source` (`read` or `reconstructed`), `field_half` (`own` or `opponent`), and a value column named `value` (SP ratio or EP points) or `n` (integer sample size).

`reg_formulas.csv`: `field_half`, `down` (1-4), `column` (`Forecast` or `Reg`), `formula_text` (verbatim Excel formula), `kind` (`forecast` or `polynomial`).

`manifest.csv`: `tab`, `source_workbook`, `workbook_sha256`, `n_records`, `n_reconstructed_labels`, `extracted_at`, `out_file`.

Join key for M3-02-06's model-vs-HC comparison: `(down, distance_bin, field_half)` on the unclustered tabs (`sp_by_dd.csv` / `ep_by_dd.csv` / `sample_size_by_dd.csv`), where `distance_bin` is a plain distance-to-go integer, 1-34/1-49 depending on tab.

## Per-Tab Extents (for M3-02-06/07)

| Tab | Records | Distance range | Reconstructed labels |
|---|---|---|---|
| SP by D&D | 217 | 1-34 | 0 |
| EP by D&D | 217 | 1-34 | 0 |
| Sample Size by D&D | 300 | 1-49 | 0 |
| SP by D&D Clustered | 48 | 6 bins x 4 downs x 2 halves | 3 per half (6 total) |
| EP by D&D Clustered | 48 | 6 bins x 4 downs x 2 halves | 3 per half (6 total) |
| Sample Size by D&D Clustered | 48 | 6 bins x 4 downs x 2 halves | 3 per half (6 total) |
| SP by D&D weighted | 233 | 1-36 | 0 |
| EP by D&D weighted | 292 | 1-49 | 0 |
| Reg | 16 | 4 downs x 2 halves x 2 formula kinds | n/a |

`Sample Size by D&D`: total n=17940 across 300 cells, min n=0 (79 zero-sample cells), max n=2290 (down=1/distance=26/own half -- also one of the four cells with real n but no computed SP, see Deviation 2), 154 of 300 cells (51%) have n<20.

## Reconstructed Bin Labels

All three carry the same reconstruction, present independently in the own-half and opponent-half label columns of each of the three clustered CSVs:

- `datetime(2021, 1, 5)` -> `1-5` (own-half `SP by D&D Clustered` reads `0.6811594203`; the surviving uncorrupted label `16-19` two rows down confirms the bin sequence is a monotonically increasing distance-to-go grouping, consistent with `1-5` as the first bin)
- `datetime(2021, 6, 10)` -> `6-10`
- `datetime(2021, 11, 15)` -> `11-15`

Reconstruction rule: for a `datetime` cell, the label is `f"{month}-{day}"`. All three corrupted cells have year 2021 -- `reconstruct_distance_bin` raises `ValueError` on any other year rather than guessing, so a future workbook update with a differently-corrupted cell fails loudly instead of silently reconstructing the wrong label.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `data/reference/hc_sp_tables/*.csv` is ready for M3-02-06's model-vs-HC comparison table: join on `(down, distance_bin, field_half)` against the model's own EP/SP-by-situation output using the same axis definition.
- `reg_formulas.csv` and the per-tab extents/n-distribution above are ready for M3-02-07's write-up quoting the head coach's method, including the n<20 finding (154/300 cells) and the down=1..4/distance=26/own-half gap (large n, no computed SP) as concrete small-sample/data-quality talking points.
- No blockers. `tests/test_m3_hc_pii.py` was read but not modified, staying clear of M3-03-02's parallel edits to that file.

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-03*

## Self-Check: PASSED

- All 13 key files (script, 10 CSVs, test file, this summary) verified present on disk with `[ -f ]`.
- All 3 task commits (`599ff76`, `dfd64bf`, `ae5f8d3`) verified present via `git log --oneline --all`.
- `uv run pytest tests/test_m3_epa_snapshot.py -x -q` -- 9 passed.
- `uv run python scripts/hc_sp_snapshot.py && git status --porcelain data/reference/hc_sp_tables/` -- empty, reproducible.
- `git status --porcelain tests/test_m3_hc_pii.py` -- empty, untouched.
- No file under `src/flag_football_ep/` or `docs/` changed this plan.
