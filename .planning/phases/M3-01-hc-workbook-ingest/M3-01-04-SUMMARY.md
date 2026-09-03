---
phase: M3-01-hc-workbook-ingest
plan: 04
subsystem: ingest
tags: [polars, hc-workbook, dedupe, pipeline-wiring, pii, validation]

# Dependency graph
requires:
  - phase: M3-01-hc-workbook-ingest plan 02
    provides: "ingest/hc_workbook.py reader (read_sheet_rows, segment_blocks, map_block_to_frame)"
  - phase: M3-01-hc-workbook-ingest plan 03
    provides: "data/reference/hc_games.csv schema + load_hc_games, segment_games/resolve_game_identity, ingest_workbook canonical derivation chain"
provides:
  - "ingest/hc_dedupe.py: declared-pair + content-fingerprint dedupe (dedupe_hc_rows, DedupeReport, FINGERPRINT_COLUMNS)"
  - "pipeline.run_ingest wired for hc_workbook as a fifth, last-dispatched source; hc_workbook deliberately excluded from _WARN_ONLY_SOURCES"
  - "data/reference/hc_games.csv: 9 real, evidence-based duplicate declarations from the real run (Scoring Probability vs legacy-39..47)"
  - "docs/hc-workbook-ingest.md: German report of the real run, per HC-D06"
  - "tests/test_m3_hc_pii.py: PII gate over every committed artefact of this phase"
affects: [M3-2 (EPA-Refinement, consumes the enlarged corpus and must decide on the half_assigned/Timeout/Offsetting-Penalties open items named in this summary)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Cross-source dedupe as its own module (hc_dedupe.py), called from pipeline.py after every non-HC source is concatenated -- never inside the single-source ingest module, since it needs both sides of the comparison"
    - "DedupeReport.summary_lines() caps individually-rendered cross_game_overlaps entries (top 20 by n_matching) with an aggregate total line; the full untruncated list stays on the report object for any caller that needs it -- rendering every entry at real-corpus scale produced a multi-million-line report"
    - "Sheet-scoped notices (HcIngestNotices.messages) go into source_notices once per message, never duplicated per resolved game_id -- attaching a whole sheet's messages to every one of its (potentially thousands of) games multiplies catastrophically at real-corpus scale"
    - "A dedupe pairing is declared in hc_games.csv only above a high evidence bar (~95%+ row-level fingerprint match against a real, already-accepted game) -- weaker matches are left provisional and reported, never guessed into a pairing"

key-files:
  created:
    - src/flag_football_ep/ingest/hc_dedupe.py
    - tests/test_ingest_hc_dedupe.py
    - tests/test_m3_hc_pii.py
    - docs/hc-workbook-ingest.md
  modified:
    - src/flag_football_ep/pipeline.py
    - src/flag_football_ep/cli.py
    - tests/test_pipeline_ingest.py
    - data/reference/hc_games.csv

key-decisions:
  - "hc_workbook dispatches last (fixed dispatch order hudl/legacy/sportapp/ifaf/hc_workbook) so dedupe_hc_rows always sees the fully-assembled non-HC corpus before deciding what to exclude."
  - "hc_workbook is deliberately absent from _WARN_ONLY_SOURCES (HC-D05) -- confirmed in the real run: every one of 2,128 HC games quarantines, 2,128/2,128 on half_assigned alone (HC workbooks carry no half-boundary data), 706 of them on half_assigned only. This is the correct, honest outcome of the design, not a defect -- a camp/scrimmage game legitimately failing a structural check must never be waved into the training corpus."
  - "Two Rule-1 scalability fixes were required before the real run could complete usefully: DedupeReport.summary_lines() (uncapped cross_game_overlaps rendering produced 268k-plus lines against the real corpus) and _ingest_hc_workbook's game_notices attachment (duplicating a sheet's full message list across every one of its ~1,800 fragmented games produced a ~5.5M-line validation report). Both are documented in code and below; neither changes stage-1/stage-2 exclusion semantics, only rendering."
  - "hc_games.csv gained exactly 9 new rows from the real run, all backed by 95.7-100% row-level fingerprint matches against already-accepted legacy games (legacy-39..47). Every other candidate -- EC-2025's Data tab (blocked by the still-open Frage 1), the Scoring Probability team-pair blocks (blocked by the still-open Frage 2, plus a newly-found possession-swap fragmentation issue that splits one real game into dozens of tiny slices), and Offense Analytics' 35 numeric games (no team/date metadata in-sheet at all) -- was left provisional. A wrong row would silently misrepresent a game; a provisional id costs nothing but a maintainer follow-up."
  - "player_mapping.csv gained zero new rows this run: with ~2,128 of 2,128 HC games provisional (home_team/away_team unresolved), the jersey-unique-within-team-roster path has no team to resolve against, and none of the 89 name-shaped unmapped labels matched exactly one roster.csv player_name globally."

requirements-completed: [HC-01, HC-02]

# Metrics
duration: 175min
completed: 2026-09-03
---

# Phase M3-01 Plan 04: HC-Workbook Dedupe, Pipeline Wiring, Real Run Summary

**`ingest/hc_dedupe.py` (declared-pair + fingerprint dedupe) wired as `hc_workbook`, a fifth `ffep ingest` source; a real run against all three workbooks found 19,901 HC rows across 2,128 games, confirmed 9 real duplicates against the existing `legacy` corpus (703 rows excluded), and found every single HC game quarantines today because the workbooks carry no half-boundary data.**

## Performance

- **Duration:** ~175 min
- **Started:** 2026-09-03T~18:50Z
- **Completed:** 2026-09-03T~21:45Z
- **Tasks:** 3
- **Files modified:** 8 (4 created, 4 modified)

## Accomplishments

- `src/flag_football_ep/ingest/hc_dedupe.py`: `dedupe_hc_rows(hc_df, corpus_df, hc_games) -> (kept_df, DedupeReport)`. Stage 1 pairs games through `hc_games.csv`'s `corpus_game_id` column (works both HC-vs-corpus and HC-vs-HC, e.g. the `Data`/`Copy of Data` case); stage 2 excludes only the HC rows whose `FINGERPRINT_COLUMNS` fingerprint (`down, yards_to_go, yardline, result_raw, yards_gained, received_by, thrown_by` -- `yards_gained` substitutes for HC-D03's literal `GN/LS`, since `GN/LS` has no canonical column of its own) matches a row in the declared partner. Undeclared cross-game overlaps and intra-game duplicate fingerprints are reported, never excluded. Row conservation (`kept.height + n_excluded == input.height`) is asserted, mirroring `partition_games`'s own wording. 12 unit tests (`tests/test_ingest_hc_dedupe.py`), TDD RED->GREEN.
- `pipeline.py`: `hc_workbook` added to `_KNOWN_SOURCES` (dispatched last, so it dedupes against the fully-assembled non-HC corpus) and `cli.py`'s `DEFAULT_SOURCES`; deliberately **not** added to `_WARN_ONLY_SOURCES` (HC-D05). `_ingest_hc_workbook` iterates every `*.xlsx` under `data/raw/hc_files/`, every existing `SHEET_NAMES` sheet, inside a per-sheet try/except; writes every run's accumulated unmapped player labels once to the gitignored `unmapped_players_<run_id>.txt`. Extended `tests/test_pipeline_ingest.py` with 5 new integration tests covering dispatch, missing-directory skip, quarantine (not warn), declared-pair exclusion, and single-source isolation -- all against a synthetic in-process `openpyxl` fixture, never the real gitignored workbooks.
- **The real run** (`uv run ffep ingest --source hc_workbook`, then the full `uv run ffep ingest`): 19,901 rows read across 4 sheets in 3 workbooks, 2,128 games resolved (2,119 provisional, 9 newly mapped to real duplicates). 703 rows excluded via the 9 declared pairs; 134,040 undeclared cross-game overlaps reported (mostly short-fingerprint noise -- median a handful of matching rows, versus 70-90+ for the 9 real duplicates). Every HC game quarantines (100%, `half_assigned` -- HC workbooks carry no half-boundary data by design); 706 games would otherwise be clean. Contract v1.2's six new RESULT tokens all confirmed present in real data (`Dropped` 94, `Block` 37, `Timeout` 17, `Batted Down` 9, `Offsetting Penalties` 8, `Blocked` 1); exactly one non-vocabulary token found (`-5.0`, 2 occurrences, matching the corrupt-cell finding already on record in `M3-01-RESEARCH.md`).
- `data/reference/hc_games.csv`: 9 new rows, each backed by a 95.7-100% row-level fingerprint match against an already-accepted `legacy` game (`legacy-39` through `legacy-47`) -- discovered via the same fingerprint mechanism dedupe itself uses, then verified by checking the full, unrestricted overlap ratio (not just the capped top-20 summary) before writing the row. `data/reference/player_mapping.csv` deliberately received zero new rows (see key-decisions).
- `docs/hc-workbook-ingest.md`: German report per HC-D06 -- per-sheet counts, the 9 confirmed duplicates with their match ratios, the 100%-quarantine finding and its per-check breakdown, contract v1.2 token counts, why zero player-mapping rows were added, and the three still-open `docs/hc-rueckfragen-2026-09.md` questions plus one new finding (the possession-swap segmentation-fragmentation issue) surfaced for a future maintainer.
- `tests/test_m3_hc_pii.py`: gates `docs/hc-workbook-ingest.md`, `docs/hc-rueckfragen-2026-09.md`, `data/reference/hc_games.csv`, `hc_workbook.py`/`hc_dedupe.py` and their test files against every `roster.csv` full name and every surname >= 6 characters (word-boundary, case-insensitive), plus a second test guarding against a pasted label-list next to any `unmapped_players_` path reference. `player_mapping.csv`/`roster.csv` are deliberately excluded (HC-D02's sanctioned mapping files).

## Task Commits

1. **Task 1 RED: failing hc_dedupe tests** - `88342bb` (test)
1. **Task 1 GREEN: hc_dedupe declared-pair + fingerprint dedupe** - `1a3668c` (feat)
2. **Task 2: wire hc_workbook into ffep ingest** - `343f6e0` (feat)
2. **Task 2 real-run fix: cap cross-game-overlap report lines** - `2d5b648` (fix)
2. **Task 2 real-run fix: stop duplicating sheet notices per game** - `b806a8d` (fix)
2. **Task 2 real-run data: fill hc_games.csv with the 9 confirmed duplicates** - `9c14e86` (feat)
3. **Task 3: German report + PII gate test** - `c11b034` (docs)

## Files Created/Modified

- `src/flag_football_ep/ingest/hc_dedupe.py` - declared-pair + fingerprint dedupe, `DedupeReport`
- `tests/test_ingest_hc_dedupe.py` - 12 unit tests
- `src/flag_football_ep/pipeline.py` - `_ingest_hc_workbook`, `hc_workbook` dispatch + dedupe wiring
- `src/flag_football_ep/cli.py` - `DEFAULT_SOURCES` includes `hc_workbook`
- `tests/test_pipeline_ingest.py` - HC fixture workbook writer, 5 new integration tests
- `data/reference/hc_games.csv` - 9 real duplicate declarations from the real run
- `docs/hc-workbook-ingest.md` - German ingest report
- `tests/test_m3_hc_pii.py` - PII gate over every committed artefact of this phase

## Decisions Made

See frontmatter `key-decisions` for the full rationale on each. In short: `hc_workbook` dispatches last and stays out of `_WARN_ONLY_SOURCES`; two report-rendering scalability bugs were fixed mid-plan once the real run's scale exposed them; `hc_games.csv` gained only evidence-backed rows (9, all >=95.7% match); `player_mapping.csv` gained none, for a fully-reasoned, documented cause.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `DedupeReport.summary_lines()` rendered every `cross_game_overlaps` entry individually**
- **Found during:** Task 2's real run (first full `ffep ingest`)
- **Issue:** Against the real corpus (~20k HC rows vs. ~3.7k+ legacy/ifaf rows), short/common fingerprints coincidentally matched many unrelated games, producing 268,098 individual overlap notice lines -- a ~64MB console/report output, unusable and slow.
- **Fix:** `summary_lines()` now renders at most 20 individual overlap lines (largest `n_matching` first) plus one aggregate total line; the full, untruncated list stays on `DedupeReport.cross_game_overlaps` for any caller (including this plan's own investigation into the real 9 duplicates, which used the full list, not the capped rendering).
- **Files modified:** `src/flag_football_ep/ingest/hc_dedupe.py`
- **Verification:** `tests/test_ingest_hc_dedupe.py` still green (cross_game_overlaps data unaffected); real run's report shrank from 64MB/271,902 lines to a normal-sized notice list.
- **Committed in:** `2d5b648`

**2. [Rule 1 - Bug] `_ingest_hc_workbook` duplicated a sheet's full notice list across every one of its games**
- **Found during:** Task 2's real run (same run as above)
- **Issue:** A sheet's `HcIngestNotices.messages` (sheet-scoped, not per-game) was attached to `game_notices` for every `game_id` that sheet resolved. Scoring Probability's `Copy of Data` tab alone resolves to ~1,800 fragmented games; its ~1,800 messages were each duplicated ~1,800 times into the validation report (~3.2M lines), pushing the total report to 5.5M lines.
- **Fix:** Sheet messages now go into `source_notices` once per message (prefixed with file/sheet), never into `game_notices` -- every finding still reaches the report and console exactly once, under "Source notices" instead of duplicated per game.
- **Files modified:** `src/flag_football_ep/pipeline.py`
- **Verification:** `tests/test_pipeline_ingest.py` still green; real run's report shrank to 30,170 lines (a real, appropriately-sized report for 2,385 games).
- **Committed in:** `b806a8d`

---

**Total deviations:** 2 auto-fixed (both Rule 1, both scalability bugs only discoverable by actually running the real workbooks -- exactly what this task's "real run" step exists to surface).
**Impact:** Both fixes were necessary for the real run to produce a usable report at all; neither changes dedupe's stage-1/stage-2 exclusion semantics or the pipeline's per-source dispatch/quarantine behavior. No scope creep.

## Issues Encountered

- **100% of HC games quarantine in the real run**, all on `half_assigned` (HC workbooks carry no half-boundary data -- `ingest_workbook` stamps `half = null` for every HC row by design, and `half_assigned` requires `half ∈ {1, 2}`). This is not a bug in this plan's code; it is the honest, by-design consequence of HC-D05 (`hc_workbook` deliberately excluded from `_WARN_ONLY_SOURCES`) meeting a real structural gap in the source data. **706 of the 2,128 HC games fail *only* `half_assigned`** -- they would otherwise be clean and accepted. Filling `data/reference/half_boundaries.csv` (or an HC-specific equivalent) for at least the 706 half-assigned-only games is the natural next step, but is out of this plan's scope (`validation/checks.py` is read-only per the file-collision guard) and is flagged for M3-2 or a dedicated follow-up plan.
- **Two report-rendering scalability bugs** (see Deviations) were only discoverable by running the real, full-size workbooks -- synthetic test fixtures at unit/integration scale never exercised the multi-thousand-game case. Both are fixed and covered by the existing test suite continuing to pass; no new test specifically pins the "report stays small" property, since that would require a multi-thousand-row synthetic fixture disproportionate to this plan's scope.
- **The pair-block segmentation heuristic fragments one real game into many tiny slices** whenever the team-pair order swaps within the same game (offense/defense possession alternation charted as `(TeamA, TeamB)` / `(TeamB, TeamA)`). This is a finding about already-merged Wave 1/2 code (`hc_workbook.py`'s `segment_games`, read-only per this plan's file-collision guard), not something this plan changes -- documented in `docs/hc-workbook-ingest.md` as a blocker for further `hc_games.csv` maintenance on the Scoring Probability workbook's team-pair blocks, and flagged as a Frage-2-adjacent follow-up.
- **The full, unrestricted `uv run pytest tests -q` run** (plan verification's aspirational full-suite item, as opposed to this plan's own scoped `test_ingest_hc_dedupe.py`/`test_pipeline_ingest.py`/`test_ingest_hc_workbook.py`/`test_reference.py`/`test_m3_hc_pii.py`/`test_cli_smoke.py` verification, which is green): ran past 67% with zero failures observed before being terminated to avoid blocking phase completion on a long-running, unrelated test tail -- consistent with M3-01-03-SUMMARY.md, which documented the identical M2 model-training test tail (unrelated to this plan's eight owned files) at the same collection point. A stray duplicate background invocation (two concurrent full-suite runs competing for CPU, `#macbook-sleep-interrupts-agents`-style orphaned-background-job pattern) was found and killed mid-way, which is why progress briefly stalled before resuming.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

**What M3-2 inherits:**
- **New game ids:** 2,119 provisional HC games (`hc-<workbook>-<sheet>-<block_key>` ids) plus 9 newly-mapped duplicate declarations (`hc-scoring-probability-dup-legacy-39` .. `-47`, each carrying 2-3 residual, non-duplicate rows after dedupe). All are `source` values `hc_workbook:<file-slug>:<sheet-slug>`.
- **Quarantined:** all 2,128 HC games today (100%), overwhelmingly on `half_assigned` alone (706 games) or `half_assigned` plus `gapless_play_ids`/`downs_range`/`monotonic_drive_ids`. **No HC rows are in `plays.parquet` today** -- `hc_workbook` contributed 0 accepted rows to the real run's 21,437 total accepted plays (all from `legacy`/`legacy-sportapp`/`ifaf`). Filling half-boundary data for HC games is the single highest-leverage next step to actually train on any of this corpus.
- **Timeout/Offsetting Penalties filtering:** both already map to `play_type = "no_play"` in the reused `hudl.derive_outcome_columns` chain (not a trainable play in the current pipeline's typical usage), but whether to additionally exclude them from `plays.parquet` before EP/WP training is an explicit open decision left to M3-2 (see `docs/hc-workbook-ingest.md`'s "Offene Fragen" -> Frage 4).
- **Still open, blocking further `hc_games.csv` work:** `docs/hc-rueckfragen-2026-09.md` Frage 1 (EC-2025's `Data` tab) and Frage 2 (Scoring Probability's team-pair column order) remain unanswered by the head coach; Frage 2 additionally now has a documented segmentation-fragmentation finding that must be resolved before any further `Data`/`Copy of Data` pairing work is attempted.
- No blockers for M3-2 to proceed with the currently-accepted (non-HC) corpus; HC data readiness is gated on the half-boundary gap above.

---
*Phase: M3-01-hc-workbook-ingest*
*Completed: 2026-09-03*

## Self-Check: PASSED

- All 9 files this summary claims (8 owned files + this SUMMARY.md) confirmed present on disk.
- All 7 task commits (`88342bb`, `1a3668c`, `343f6e0`, `2d5b648`, `b806a8d`, `9c14e86`, `c11b034`) confirmed in `git log --oneline`.
- Re-ran `uv run pytest tests/test_ingest_hc_dedupe.py tests/test_pipeline_ingest.py tests/test_ingest_hc_workbook.py tests/test_reference.py tests/test_m3_hc_pii.py tests/test_cli_smoke.py tests/test_canonical.py -q`: exit code 0, no failures.
- `uv run ffep ingest --source hc_workbook` and `uv run ffep ingest` (all five sources) both completed successfully against the real workbooks (see the real-run counts throughout this summary and `docs/hc-workbook-ingest.md`).
- `git status --porcelain data/raw` empty; no `unmapped_players_*.txt` staged (gitignored, confirmed via `git check-ignore`).
- `git diff --name-only` against the plan's base commit (`06bd94c`) lists exactly 8 files, all inside the plan's owned-file list (`hc_workbook.py` and `player_mapping.csv` are legitimately untouched -- no changes were needed there this run).
- `grep -c FINGERPRINT_COLUMNS src/flag_football_ep/ingest/hc_dedupe.py` = 5 (non-zero); file is 312 lines (>= 120 min).
- `docs/hc-workbook-ingest.md` is 227 lines (>= 60 min), contains `## Offene Fragen`, `## Wartung`, `hc_games.csv` and (outside headings) `hudl`.
- `grep -c roster.csv tests/test_m3_hc_pii.py` = 5 (non-zero).
