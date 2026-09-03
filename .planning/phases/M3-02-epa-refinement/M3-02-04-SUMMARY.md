---
phase: M3-02-epa-refinement
plan: 04
subsystem: ingest
tags: [polars, openpyxl, hc-workbook, hc-games-refill, data-contract]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "M3-02-01's unordered-pair segmentation + half=2 sentinel; M3-02-02's competition_tier.csv rows and EP half-feature sentinel"
provides:
  - "scripts/hc_games_refill.py: deterministic, re-runnable hc_games.csv regeneration for the two numeric-block HC sources"
  - "A header-block segmentation rule in hc_workbook.py (team-name row = header, O/D/S marker rows inherit it) implementing the head coach's 2026-09-03 Frage-2 answer, verified against the real workbook to have zero practical effect on the currently declarable corpus"
  - "Non-zero head-coach rows in data/processed/plays.parquet: 1,964 rows across 35 games, all half=2"
  - "docs/hc-workbook-ingest.md and docs/hc-blocks-ohne-kopfzeile.md updated with real before/after counts"
affects: [M3-02-05-training-waves, M3-02-07-german-deliverable]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Header-row-opens-block, marker-row-inherits-header pair-block segmentation, coexisting with the existing unordered-pair-equality merge for the team-name-per-row style, unified in one _split_pair_block"
    - "Verify-against-real-data-before-shipping: two implementation attempts (blank-row-gap boundary, unqualified D/S marker match) were caught and reverted specifically because they were checked against the real workbook, not just synthetic fixtures"

key-files:
  created:
    - scripts/hc_games_refill.py
    - tests/test_m3_hc_games_refill.py
    - docs/hc-blocks-ohne-kopfzeile.md
  modified:
    - src/flag_football_ep/ingest/hc_workbook.py
    - tests/test_ingest_hc_workbook.py
    - tests/test_pipeline_ingest.py
    - data/reference/hc_games.csv
    - docs/hc-workbook-ingest.md
    - .planning/phases/M3-02-epa-refinement/deferred-items.md

key-decisions:
  - "Header-block rule implemented and tested (authorized deviation), but the real Data-tab pair block has zero O/D/S marker rows -- the original plan's pair-block exclusion stands unchanged; the new capability is ready for future workbook updates, not exercised on this run's real output"
  - "Blank-row block boundary (part of the head coach's literal rule) is NOT implemented -- read_sheet_rows/segment_blocks already discard both genuinely blank rows and dtype-unclassifiable rows without preserving which is which, and inferring the boundary from a row-number gap produced a false positive (137->18 instead of the validated 137->22) on the real file. Removed before commit, logged as a deferred item"
  - "A bare 'D'/'S' in column A only counts as an O/D/S marker when column B is empty -- both letters are also real team abbreviations (Deutschland, etc.) in this corpus; the unqualified version silently mis-segmented the real block (137->16) before this was caught"
  - "DECLARED_SOURCES/DECLARED_BLOCK_KINDS stay exactly as the original plan specified (two numeric sources only) -- the deviation investigation confirmed rather than changed this scope"
  - "MIN_PLAYS=5, HOME_TEAM=GER/AWAY_TEAM=OPP kept as [ASSUMED] per the plan's own spec, unchanged"

requirements-completed: [HC-03]

# Metrics
duration: ~3h
completed: 2026-09-03
---

# Phase M3-02 Plan 04: HC games.csv refill and first non-zero real ingest run Summary

**`scripts/hc_games_refill.py` deterministically declares 176 new head-coach games; a real `ffep ingest` run lands 1,964 head-coach rows across 35 games (all `half=2`) in `data/processed/plays.parquet` for the first time, up from zero.**

## Performance

- **Duration:** ~3h (includes an additional, authorized deviation task implementing and empirically debugging a new segmentation rule before the plan's own three tasks)
- **Started:** 2026-09-03T19:00:00Z (approx.)
- **Completed:** 2026-09-03T21:45:00Z (approx.)
- **Tasks:** 4 (1 authorized deviation task + the plan's 3 tasks)
- **Files modified:** 9 (3 created, 6 modified)

## Accomplishments

- Implemented the head coach's 2026-09-03 answer to Frage 2 (`docs/hc-rueckfragen-2026-09.md`) as a header-block segmentation rule in `hc_workbook.py`: a team-name row opens a pair-block game, `O`/`D`/`S` marker rows in column A inherit that header's team identity and get a real `ODK` (so `posteam`/`defteam` can resolve), a new team-name row closes the block. Coexists, unchanged, with the existing unordered-pair-equality merge for the team-name-per-row style M3-02-01 already fixed.
- **Verified the new rule against the real workbook, not just synthetic fixtures — and this caught two real bugs before they shipped:** an initial blank-row-boundary inference (from gaps in physical row numbers) over-fragmented the real `Data`-tab pair block from the validated 137→22 games down to 137→18, because `segment_blocks` already silently discards non-blank rows it cannot classify, leaving an identical symptom. An initial unqualified `"D"`/`"S"` marker match further fragmented it to 137→16, because both letters are also real team abbreviations (Deutschland, etc.) already present in the corpus. Both were caught, fixed, and re-verified to reproduce the exact validated 137→22 before any commit.
- `scripts/hc_games_refill.py`: a deterministic, re-runnable replacement for hand-typing `hc_games.csv` rows. Declares only the two numeric-block sources (Offense Analytics Data, Scoring Probability Data's numeric blocks), preserves existing declared rows byte-for-byte, raises by name if a declared `block_key` vanishes after re-segmentation, skips sub-`MIN_PLAYS` games and pair-kind blocks with a named reason, and asserts competition-tier coverage before writing.
- Real `ffep ingest --source hc_workbook` + full `ffep ingest` run: **1,964 head-coach rows, 35 games, all `half=2`**, now in `data/processed/plays.parquet` (was 0 before this plan). Total corpus: 21,437 → 23,401 accepted plays.
- Fixed the stale `tests/test_pipeline_ingest.py` test flagged in `deferred-items.md` by M3-02-01 (added a still-undeclared, otherwise-clean "game-c" fixture so the test still has a pure `half_assigned`-only quarantine case to demonstrate).
- Updated `docs/hc-workbook-ingest.md` with the real before/after counts and a new `## Nicht eingelesen (bewusst)` section; produced `docs/hc-blocks-ohne-kopfzeile.md` (currently empty — zero headerless pair-block groups found in the real workbook).

## Task Commits

Each task was committed atomically:

1. **Deviation task: header-block segmentation rule (authorized, new_facts)** - `a8259ed` (feat)
2. **Task 1: The refill script** - `d30590b` (feat)
3. **Task 2: Refill, ingest, and report the corpus before and after** - `20f4f4a` (feat)
4. **Task 3: Bring the ingest document up to the real state** - `13de787` (docs)

_Note: Task 1 was written and committed as a single unit (tests + implementation together) rather than with separate RED/GREEN commits — see "Deviations from Plan" below._

## Files Created/Modified

- `scripts/hc_games_refill.py` - deterministic `hc_games.csv` refill (`build_rows`, `main`, `DECLARED_SOURCES`)
- `tests/test_m3_hc_games_refill.py` - 11 tests covering emission scope, game-id stability, tier coverage, preservation, the missing-declared-key raise, `--dry-run`
- `src/flag_football_ep/ingest/hc_workbook.py` - `_pair_row_marker`, rewritten `_split_pair_block` (header-block rule), `_null_pair_block_tail` ODK derivation for marker rows, unified `_stamp_posteam_defteam`, `ingest_workbook`'s `ODK == "S"` no-play override and updated pair-row-total notice
- `tests/test_ingest_hc_workbook.py` - 8 new segmentation tests + 1 map-level ODK test + 3 end-to-end `ingest_workbook` tests for the header-block rule
- `tests/test_pipeline_ingest.py` - `_write_hc_fixture` gains a clean-but-undeclared "game-c"; `test_run_ingest_hc_failing_game_quarantined_not_warned` rewritten to match the post-M3-02-01 behavior
- `data/reference/hc_games.csv` - 176 new declared rows (176 insertions, 0 deletions/modifications — the 9 pre-existing legacy-duplicate rows are byte-identical)
- `docs/hc-workbook-ingest.md` - real AFTER counts next to the documented BEFORE numbers, `## Nicht eingelesen (bewusst)`, Fragen 1-3 status updated to their 2026-09-03 answers, Frage 2 sharpened with the `Copy of Data` header-mismatch finding, `## Wartung` documents the refill script
- `docs/hc-blocks-ohne-kopfzeile.md` - the required user worksheet for headerless pair-block groups (empty this run — see below)
- `.planning/phases/M3-02-epa-refinement/deferred-items.md` - logs the blank-row-boundary gap; marks the M3-02-01-logged stale test as fixed by this plan

## Before/After Corpus (per source, per sheet)

| Workbook / Sheet | Rows read | Games: before → after | Trainable (accepted, reaches plays.parquet) |
|---|---:|---:|---:|
| Germany Analytics EC 2025 vs WC Nations / Data | 269 | 3 → 3 | 0 (Frage 1: answered, permanently excluded) |
| Offense Analytics 2026 Camps and Competitions / Data | 1,926 | 35 → 35 | **25** (10 quarantine on `downs_range`, real null-`DN` charting gaps) |
| Scoring Probability by Situation 2023-2026 / Data | 13,811 | 289 → 174 | **10** (164 quarantine, mostly `downs_range`) |
| Scoring Probability by Situation 2023-2026 / Copy of Data | 3,895 | 1,801 → 1,645 | 0 (different column layout, still excluded) |
| **Sum** | **19,901** | **2,128 → 1,857** | **35** |

`data/processed/plays.parquet` per source (AFTER):

| source | rows | games | competition |
|---|---:|---:|---|
| `hc_workbook:offense-analytics-2026-camps-and-competitions:data` | 1,183 | 25 | HC Camps 2026 |
| `hc_workbook:scoring-probability-by-situation-2023-2026:data` | 781 | 10 | HC Charting 2023-2026 |
| (pre-existing: `ifaf`, `legacy`, `legacy-sportapp`) | 21,437 | — | — |
| **Total plays.parquet** | **23,401** (was 21,437) | 2,114 games | — |

`half` for every head-coach row: `[2]` (Int32) — verified by the plan's own assertion command.

## Trainable-vs-Provisional Split, With Reasons

- **Trainable (declared, reaches plays.parquet if it also passes validation): 35 games.** Both are numeric-block sources with real `ODK`, so `posteam`/`defteam` resolve without guessing.
- **Provisional/excluded, by design, 4 standing reasons** (documented in `docs/hc-workbook-ingest.md` `## Nicht eingelesen (bewusst)` and `docs/hc-blocks-ohne-kopfzeile.md`):
  1. Scoring Probability `Data`'s team-name-pair block (22 games) — `posteam`/`defteam` undetermined; the newly-implemented header/marker convention (which COULD resolve this) does not occur in the real block (0 marker rows, verified).
  2. Two single-row noise blocks (`"CC 25"`, `"Mark"`) — below `MIN_PLAYS=5`, not games.
  3. `Copy of Data` (1,645 games) — different, undocumented column layout (14 vs 15 columns), Frage 2's tail-column question still open.
  4. Germany EC2025 workbook — Frage 1 now answered: permanently empty by design, not "pending."

## Dedupe Outcome

Unchanged from M3-01-04, verified rather than assumed (RESEARCH Pitfall 2): the same 9 `legacy-39..47` pairings match with the same row counts and percentages (90/85/81/75/70/83/87/73/78 HC rows; 87/83/79/75/67/81/85/70/76 matching — 96.7%/97.6%/97.5%/100%/95.7%/97.6%/97.7%/95.9%/97.4%). 703 HC rows excluded. All 9 pairing's `hc_games.csv` rows are byte-identical after the refill (`git diff` shows only additions), confirming the numeric block's `block_key`s were unaffected by the pair-block segmentation change, as predicted.

## Remaining Check Failures (After the Fixes)

| Check | Before (2,128 games) | After (1,857 games) | Assessment |
|---|---:|---:|---|
| `half_assigned` | 2,128 (100%) | 1,673 | **Fixed for 184 declared games** — the half=2 sentinel works exactly as designed |
| `gapless_play_ids` | 1,279 | 1,279 | **Unchanged — expected.** Real charting gaps (Offense Analytics' real `PLAY #` numbering, synthesized numbering on short `Copy of Data` fragments); the check doing its job, not routed around |
| `downs_range` | 172 | 172 | **Unchanged — expected.** Real null `DN` cells in the charting, independent of segmentation/half. **New finding this run:** concentrated in Scoring Probability `Data` (138 of 164 quarantined games there fail on this alone) — more than RESEARCH's hypothesis that it was mostly a `Copy of Data` issue |
| `monotonic_drive_ids` | 8 | 8 | **Unchanged — expected.** Small, unrelated residual |
| `score_reconstruction` | 0 evaluated (SKIPPED) | 0 evaluated (SKIPPED) | **Unchanged — by design.** No `final_scores.csv` reference exists for any `hc-` id |

No validation check was weakened; `hc_workbook` was not added to `_WARN_ONLY_SOURCES`.

## Decisions Made

- Implemented the head coach's header-block rule as a general capability (authorized deviation), but confirmed via real-file verification that it changes NOTHING about the currently declarable/provisional scope — `DECLARED_SOURCES`/`DECLARED_BLOCK_KINDS` in `scripts/hc_games_refill.py` are exactly what the original plan specified.
- Did not implement the blank-row half of the head coach's block-boundary rule — `read_sheet_rows`/`segment_blocks` do not currently preserve enough information to distinguish a genuinely blank row from any other row `segment_blocks` silently skips, and a first attempt that guessed produced a measurable, wrong regression on real data. Logged as a deferred item rather than shipped with a known false positive.
- `"D"`/`"S"` in column A require an empty column B to count as an O/D/S marker (not just the letter) — both are also real, attested team abbreviations in this corpus.
- Deleted a stray Excel `"~$Scoring Probability by Situation 2023-2026.xlsx"` lock file under `data/raw/hc_files/` (gitignored) that was crashing the real `ffep ingest` run (`BadZipFile`) — it slugifies to the same workbook name as the real file. Also hardened `hc_games_refill.py`'s own glob to filter `"~$"`-prefixed files defensively.

## Deviations from Plan

### Auto-fixed / Authorized Issues

**1. [Authorized deviation, new_facts] Header-block segmentation rule for HC pair blocks**
- **Found during:** Pre-task-1 investigation (explicitly authorized as an additional FIRST task per the orchestrator's `new_facts_since_planning`)
- **What:** Implemented `_pair_row_marker`, rewrote `_split_pair_block`, extended `_null_pair_block_tail`/`_stamp_posteam_defteam`/`ingest_workbook` to derive real `ODK`/`posteam`/`defteam`/`play_type` for a team-name-header + O/D/S-marker style pair block, per the head coach's 2026-09-03 Frage-2 answer.
- **Files modified:** `src/flag_football_ep/ingest/hc_workbook.py`, `tests/test_ingest_hc_workbook.py`
- **Verification:** 18 new/changed tests green; real-file verification (see below) confirms zero practical effect on this run's declarable scope.
- **Committed in:** `a8259ed`

**2. [Rule 1 - Bug, caught during real-file verification] Blank-row-gap boundary inference over-fragmented the real block**
- **Found during:** verifying the header-block rule against the real workbook (not just synthetic fixtures) — the plan's own explicit instruction to "verify against the real file... instead of guessing"
- **Issue:** An initial implementation inferred a blank-row block boundary from a gap in physical row numbers. `segment_blocks` already silently discards rows it cannot classify (not just genuinely blank ones), leaving an identical symptom — this produced 5 false-positive boundaries in the real `Data`-tab pair block, changing its game count from the validated 137→22 to 137→18.
- **Fix:** Removed the gap-based inference entirely; documented the limitation in the function's docstring and in `deferred-items.md`.
- **Files modified:** `src/flag_football_ep/ingest/hc_workbook.py`, `tests/test_ingest_hc_workbook.py` (two gap-dependent tests replaced with one test asserting a gap alone is NOT a boundary)
- **Verification:** re-ran `segment_games` against the real workbook after the fix — reproduces 137→22 exactly.
- **Committed in:** `a8259ed` (fixed before the first commit of this deviation task — never shipped)

**3. [Rule 1 - Bug, caught during real-file verification] Unqualified "D"/"S" marker match mis-segmented real abbreviation rows**
- **Found during:** the same real-file verification pass
- **Issue:** `_pair_row_marker` initially matched any column-A value in `{"O","D","S"}` regardless of column B. The real corpus has 6 genuine `"D"` (Deutschland) and 2 genuine `"S"` team-abbreviation-pair rows already documented as ambiguous noise by RESEARCH — these were silently reclassified as ODK markers, changing the block's game count to 137→16.
- **Fix:** `_pair_row_marker` now requires column B to be empty; every real occurrence of `"D"`/`"S"`/`"K"` in this corpus has a populated column B (a genuine team-pair), while a true marker row (per the head coach's description) has nothing else to write there.
- **Files modified:** `src/flag_football_ep/ingest/hc_workbook.py` and its call sites
- **Verification:** re-ran against the real workbook — reproduces 137→22.
- **Committed in:** `a8259ed` (fixed before commit)

**4. [Rule 3 - Blocking] Stray Excel lock file crashed the real ingest run**
- **Found during:** Task 2, first `ffep ingest --source hc_workbook` run
- **Issue:** `~$Scoring Probability by Situation 2023-2026.xlsx` (an Excel open-file lock artifact, gitignored, 165 bytes) slugifies to the same workbook name as the real file, so it matched `hc_games_refill.py`'s glob and (independently) `pipeline.py`'s real ingest glob, crashing with `BadZipFile`.
- **Fix:** Filtered `"~$"`-prefixed files in `hc_games_refill.py`'s own glob (in scope); deleted the stray lock file itself (a gitignored data artifact, not code) so the real `ffep ingest` run — which uses `pipeline.py`'s own glob, out of this plan's edit scope — also runs clean.
- **Files modified:** `scripts/hc_games_refill.py`; `data/raw/hc_files/~$...` deleted (gitignored, not tracked by git)
- **Verification:** second `ffep ingest --source hc_workbook` run has zero `BadZipFile` notices.
- **Committed in:** `d30590b` (script fix); the lock-file deletion itself has no commit (gitignored)

**5. [Hard rule, mandated] Fixed the stale `tests/test_pipeline_ingest.py` test**
- **Found during:** logged by M3-02-01 in `deferred-items.md`, explicitly assigned to this plan by the orchestrator's `<hard_rules>`
- **Issue:** `test_run_ingest_hc_failing_game_quarantined_not_warned`'s fixture no longer demonstrated a pure `half_assigned`-only quarantine after M3-02-01's fix made its declared `game-a` pass cleanly.
- **Fix:** added a clean-but-undeclared `game-c` to `_write_hc_fixture`; rewrote the test's assertions and docstring to match the post-M3-02-01/M3-02-04 behavior (`game-a` OK, `game-b`/`game-c` quarantined for different reasons).
- **Files modified:** `tests/test_pipeline_ingest.py`
- **Verification:** `uv run pytest tests/test_pipeline_ingest.py -q` — 32/32 green.
- **Committed in:** `a8259ed`

---

**Total deviations:** 1 authorized (header-block rule), 2 self-caught bugs (never shipped), 1 blocking fix, 1 hard-rule-mandated fix. **Impact:** the two self-caught bugs are the most consequential finding of this plan's process — both would have silently corrupted the trainable game count if not checked against real data before committing; the plan's own "verify, don't guess" instruction is what caught them.

### Process Deviation (not a Rule 1-4 category, noted for honesty)

Task 1 has `tdd="true"` in the plan frontmatter. Tests and implementation for `scripts/hc_games_refill.py` were written and verified together rather than as separate RED-then-GREEN commits (tests passed on first run against an implementation written in the same pass). Test coverage and real-file verification are equivalent in outcome to what a strict RED/GREEN cycle would have produced (11 tests, all behaviors from the plan's `<behavior>` block covered), but the commit history does not show a failing-test commit before the passing one for this task.

## Issues Encountered

None beyond the deviations above (all resolved before commit).

## User Setup Required

None — no external service configuration required.

## Known Stubs

None.

## Next Phase Readiness

- M3-02-05 (training waves) can now run the with/without-HC ablation: 35 trainable HC games with `half=2`, `posteam`/`defteam` resolved, `competition` in the locked vocabulary, tier rows already covered by M3-02-02.
- M3-02-07 (German deliverable) has the exact numbers to quote: 35 trainable games (not 2,128 or 1,857), the four standing exclusions with what would unlock each, and Frage 2's sharpened remaining question (tail-column layout, separately for `Data` and `Copy of Data`).
- `docs/hc-blocks-ohne-kopfzeile.md` exists and is ready, but currently empty — no user action needed from it this cycle.
- **Not a blocker, logged for a future plan:** the blank-row block-boundary rule needs a `segment_blocks` change (preserving blank-vs-skipped row distinction) before it can be safely implemented — see `deferred-items.md`. No currently-declarable data is affected either way.
- **Not a blocker, new finding for the deliverable:** `downs_range` failures are concentrated in Scoring Probability `Data` (138/164), not primarily `Copy of Data` as RESEARCH's hypothesis suggested — worth a one-line correction if RESEARCH's framing is quoted verbatim anywhere downstream.

## Self-Check

Files (all `[ -f ]` checked):
- `scripts/hc_games_refill.py` — FOUND
- `tests/test_m3_hc_games_refill.py` — FOUND
- `data/reference/hc_games.csv` — FOUND
- `docs/hc-workbook-ingest.md` — FOUND
- `docs/hc-blocks-ohne-kopfzeile.md` — FOUND
- `src/flag_football_ep/ingest/hc_workbook.py` — FOUND
- `tests/test_ingest_hc_workbook.py` — FOUND
- `tests/test_pipeline_ingest.py` — FOUND
- `.planning/phases/M3-02-epa-refinement/deferred-items.md` — FOUND
- `.planning/phases/M3-02-epa-refinement/M3-02-04-SUMMARY.md` — FOUND

Commits (`git log --oneline --all`):
- `a8259ed` (header-block rule) — FOUND
- `d30590b` (refill script) — FOUND
- `20f4f4a` (refill + real ingest run) — FOUND
- `13de787` (docs update) — FOUND

Plan-level verification re-run:
- `uv run pytest tests/test_m3_hc_games_refill.py tests/test_ingest_hc_workbook.py tests/test_ingest_hc_dedupe.py tests/test_validation_checks.py tests/test_pipeline_ingest.py -q` — PASS (all green)
- Head-coach rows in `data/processed/plays.parquet`: 1,964 rows, `half` unique `[2]` — PASS
- `git diff` of the `hc_games.csv` commit shows 176 additions, 0 deletions/modifications — PASS
- Second `scripts/hc_games_refill.py` run leaves `hc_games.csv` byte-identical (diff empty) — PASS
- `git status --porcelain src/flag_football_ep/` empty at time of each Task 1-3 commit — PASS
- Full suite (`uv run pytest tests -q`, then re-confirmed with `--tb=no`) — **exit code 0, PASSED**. No `FAILURES`/`ERRORS` section in either run; only pre-existing, unrelated warnings (CV `umap`/Kalman-filter `RuntimeWarning`s, deprecation notices) plus the expected `hc_games.csv is header-only` warnings from fixtures that intentionally start with an empty reference file.

All 5 `<success_criteria>` from the plan met; all `<hard_rules>` from the orchestrator prompt satisfied (non-zero HC rows asserted and reported; `docs/hc-blocks-ohne-kopfzeile.md` produced; stale pipeline test fixed; PII discipline maintained — no player names in any committed file, verified by grep during test review).

## Self-Check: PASSED

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-03*
