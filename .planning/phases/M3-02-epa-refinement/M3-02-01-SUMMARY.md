---
phase: M3-02-epa-refinement
plan: 01
subsystem: ingest
tags: [polars, openpyxl, hc-workbook, data-contract, half-assigned]

# Dependency graph
requires:
  - phase: M3-01-hc-workbook-ingest
    provides: hc_workbook.py's block segmentation/mapping machinery (segment_blocks, map_block_to_frame, resolve_game_identity) that this plan's segmentation and half fixes build on
provides:
  - Unordered team-pair game segmentation in _split_pair_block (frozenset boundary key)
  - HALF_SENTINEL (2) / HALF_SENTINEL_EXCLUDED_SHEETS module constants and per-game half stamping in ingest_workbook
  - docs/data-contract.md hc_workbook half-sentinel rule, decision table, cost, reversal path
affects: [M3-02-04-hc-games-refill, M3-02-training-waves]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Unordered-pair boundary key (frozenset) for possession-alternating charting data"
    - "Per-game (not per-sheet, not per-frame-blanket) constant-column stamping decided by identity.provisional + sheet exclusion"

key-files:
  created:
    - .planning/phases/M3-02-epa-refinement/deferred-items.md
  modified:
    - src/flag_football_ep/ingest/hc_workbook.py
    - tests/test_ingest_hc_workbook.py
    - docs/data-contract.md

key-decisions:
  - "_split_pair_block groups on frozenset({team1, team2}) instead of the ordered tuple, fixing possession-swap over-segmentation (137 -> 22 fragments on the real Data-tab pair block, per M3-02-RESEARCH Sec 1.2)"
  - "half=2 (HALF_SENTINEL) is stamped per game (not per sheet, not blanket-null after concat) only when the game is declared in hc_games.csv AND its sheet is not in HALF_SENTINEL_EXCLUDED_SHEETS ('Copy of Data'); undeclared games and every Copy of Data row keep half=null and keep quarantining"
  - "Ambiguous single/double-row team-abbreviation noise (S, F, ...) is deliberately left as its own tiny ungrouped slice, never guessed at"
  - "tests/test_pipeline_ingest.py::test_run_ingest_hc_failing_game_quarantined_not_warned is now stale (its declared, clean game-a fixture correctly stops quarantining) but is outside this plan's file-collision guard; logged to deferred-items.md instead of fixed"

requirements-completed: [HC-03]

# Metrics
duration: 45min
completed: 2026-09-03
---

# Phase M3-02 Plan 01: HC workbook segmentation and half-sentinel fixes Summary

**Unordered-pair (`frozenset`) game segmentation and a per-game `half=2` sentinel scoped to `hc_games.csv`-declared, non-`Copy of Data` games, fixing the two ingest defects that kept 100% of head-coach rows out of `plays.parquet`.**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-09-03T19:00:00Z (approx.)
- **Completed:** 2026-09-03T19:41:06Z
- **Tasks:** 3
- **Files modified:** 3 (+1 created: deferred-items.md)

## Accomplishments

- `_split_pair_block` now groups on the unordered `{team1, team2}` pair (`frozenset`), so a real game charted with alternating offense/defense possession stays one game instead of fragmenting into dozens of one-to-few-row "games." A genuine opponent change still splits; single/double-row abbreviation noise (`S`, `F`, `AT`/`D`, ...) stays its own tiny, unguessed slice.
- `ingest_workbook` stamps `half = HALF_SENTINEL` (`2`) per game — never per sheet, never as a post-concat blanket — for a game declared in `data/reference/hc_games.csv` and charted on a sheet outside `HALF_SENTINEL_EXCLUDED_SHEETS` (`("Copy of Data",)`). Undeclared games and every `Copy of Data` row keep `half = null` and keep failing `validation.checks.half_assigned` as designed.
- `docs/data-contract.md` now documents the `half` rule for `hc_workbook:` sources next to the existing `half_boundaries.csv` rule: the sentinel constant, the null/1/2/other decision table (from M3-02-RESEARCH Sec 2.2), the named label-quality cost, the rejected play-count-midpoint heuristic (n=2), and the reversal path.

## Trainable-corpus effect this phase

**Zero net change to the trainable corpus from the segmentation fix alone.** Every existing `block_key` in a pair block is invalidated by the unordered-pair rule change (fewer, larger games renumber the block-scoped `game_index` sequence) — the 9 confirmed `legacy-39..47` duplicates live in the untouched *numeric* block and are unaffected, but the pair-block `Data`-tab games (137 -> 22 fragments) have no matching declared rows in `data/reference/hc_games.csv` yet and stay **provisional** until M3-02-04 regenerates `hc_games.csv` by re-running `ffep ingest --source hc_workbook` (M3-02-RESEARCH Pitfall 2: never hand-patch `block_key`s). `data/reference/hc_games.csv` itself is unchanged by this plan (verified: `git status --porcelain data/` empty throughout).

The `half=2` sentinel fix is the one with an immediate trainable-corpus effect: any *already-declared*, non-`Copy of Data` HC game (e.g. numeric-block camp/scrimmage games declared before this plan) now genuinely passes `half_assigned` instead of unconditionally failing. This surfaced downstream — see Deviations.

## Task Commits

Each task was committed atomically:

1. **Task 1: Unordered team-pair game segmentation** - `2c60b72` (fix)
2. **Task 2: The half=2 label sentinel, scoped to declared games and never to `Copy of Data`** - `cff9570` (fix)
3. **Task 3: Write the half rule into the data contract** - `93f9852` (docs)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified

- `src/flag_football_ep/ingest/hc_workbook.py` - `_split_pair_block` unordered-pair key; `HALF_SENTINEL`/`HALF_SENTINEL_EXCLUDED_SHEETS` constants; per-game `half` stamping in `ingest_workbook`'s per-game loop; two conditional German notices replacing the blanket-null message; docstring updates on `_split_pair_block`, `segment_games`, `ingest_workbook`
- `tests/test_ingest_hc_workbook.py` - 6 new segmentation tests (possession-swap-is-one-game, possession-swap-then-opponent-change-splits, case/whitespace-insensitive swap, single-row-noise-stays-own-slice, three-slice block_key scoping, numeric-block-unaffected) + 6 new half-sentinel tests (declared-game-gets-sentinel, undeclared-coexists-with-declared, Copy-of-Data-stays-null-even-if-declared, notices-carry-counts-and-reasons-no-pii, empty-sheet-half-column-stays-int32) + `half_assigned` import
- `docs/data-contract.md` - new `### half für hc_workbook-Zeilen` subsection under the existing derived-fields material
- `.planning/phases/M3-02-epa-refinement/deferred-items.md` - out-of-scope downstream test staleness finding (see Deviations)

## Measured slice counts (new fixtures, `tests/test_ingest_hc_workbook.py`)

| Fixture | Rows | Slices | Sizes |
|---|---:|---:|---|
| `test_game_segmentation_pair_block_possession_swap_is_one_game` (Germany/Ireland x2, Ireland/Germany x2, Germany/Ireland x1) | 5 | 1 | [5] |
| `test_game_segmentation_pair_block_possession_swap_then_real_opponent_change_splits` (+ Germany/Spain) | 6 | 2 | [5, 1] |
| `test_game_segmentation_pair_block_possession_swap_case_and_whitespace_insensitive` | 3 | 1 | [3] |
| `test_game_segmentation_pair_block_single_row_noise_stays_its_own_slice` (AT/D noise between two possession-swap stretches) | 5 | 3 | [2, 1, 2] |
| `test_game_segmentation_pair_block_three_slices_block_key_scoped` | 4 | 3 | `["b01-g00","b01-g01","b01-g02"]` |
| `test_game_segmentation_numeric_block_unaffected_by_pair_block_change` (regression) | 20 | 2 | [8, 12] |

These synthetic-fixture ratios (5:1 possession-swap collapse, 3-slice noise isolation) are directionally consistent with the real workbook's measured 137 -> 22 (6.2x) reduction on the `Scoring Probability` `Data`-tab pair block (M3-02-RESEARCH Sec 1.2) — the synthetic fixtures exercise the same boundary-key logic at a scale runnable without opening any gitignored real workbook.

## Exact German notice strings (`ingest_workbook`, half sentinel)

Sentinel notice (emitted only when `n_sentinel_rows > 0`):
```
half = 2 (Sentinel) für {n_sentinel_rows} Zeile(n) aus {n_sentinel_games} in hc_games.csv deklarierten Spiel(en) gesetzt -- HC-Workbooks tragen keine echte Halbzeitgrenze; Folge: kein No_Score-Marker zur Halbzeit, eine torlose Drive der ersten Halbzeit erbt den nächsten tatsächlichen Score
```

Null notice (emitted only when `n_null_undeclared_rows + n_null_copy_of_data_rows > 0`):
```
half = null für {n} Zeile(n) ({p} aus nicht in hc_games.csv deklarierten Spielen, {q} aus 'Copy of Data' -- Frage 2 offen): bleiben in Quarantäne (half_assigned)
```

Verified in `test_ingest_workbook_half_sentinel_notices_carry_counts_and_reasons_no_pii`: both substrings (`nicht in hc_games.csv deklariert`, `Copy of Data` + `Frage 2 offen`) present; player labels (`Spieler A`, `Anna Mustermann`) absent from every message.

## `block_key` invalidation — action needed in M3-02-04

The unordered-pair rule change (Task 1) renumbers every `game_index` within an affected pair block, which changes every `block_key` (`b{block_index:02d}-g{game_index:02d}`) for that block. **`data/reference/hc_games.csv` was not touched by this plan** (verified empty `git status --porcelain data/`) and must be regenerated by re-running `ffep ingest --source hc_workbook` and letting the pipeline's "unknown game" notices drive new declarations, per `docs/hc-workbook-ingest.md` § Wartung and M3-02-RESEARCH Pitfall 2 — never hand-patched. The 9 confirmed `legacy-39..47` duplicate declarations live in the untouched *numeric* block and are unaffected by this specific change, but should still be verified explicitly rather than assumed, per the same Pitfall.

## Decisions Made

- Unordered-pair key implemented as `frozenset({_normalize_pair_label(t1), _normalize_pair_label(t2)})`, matching M3-02-RESEARCH's exact recommendation; locals renamed `pair`/`prev_pair` -> `pair_key`/`prev_pair_key` so the unordered form cannot be mistaken for the old ordered tuple.
- No abbreviation alias table added for `S`/`F`/etc. (RESEARCH explicitly forbids guessing these — genuinely ambiguous in this corpus).
- `half` stamped inside the per-game loop (alongside `source`/`competition`/`game_id`/etc.) rather than as a post-concat blanket column — this is what makes the per-game (declared vs. undeclared) and per-sheet (`Copy of Data` exclusion) decision possible; the empty-sheet branch's schema needed no change since `CORE_COLUMNS` already carries `half: pl.Int32`.
- Two separate row counters for the null reason (`n_null_undeclared_rows` vs. `n_null_copy_of_data_rows`) rather than one combined count, so the null notice can name both reasons with real per-reason numbers.

## Deviations from Plan

### Logged, not fixed (scope-boundary discovery)

**1. [Scope boundary] `tests/test_pipeline_ingest.py::test_run_ingest_hc_failing_game_quarantined_not_warned` is now stale**
- **Found during:** Task 2 verification (ran downstream test files to check for regressions beyond the plan's required verification set)
- **Issue:** This test's `hc_tree` fixture declares a clean, non-`Copy of Data` numeric-block HC game (`hc-test-game-a`) in its inline `hc_games.csv`. Before this plan, `ingest_workbook` blanket-nulled `half` for every HC row, so `half_assigned` FAILed unconditionally for every HC game — the test's docstring explicitly frames this as "the honest, expected outcome, not a defect in the fixture." Task 2's fix makes `half_assigned` genuinely PASS for declared, non-`Copy of Data` games — `game-a` is exactly such a game, so it is now `OK` (0/6 checks failed, not quarantined), which is the *intended*, correct effect of this plan's own `<done>` criterion, not a regression in `hc_workbook.py`.
- **Not fixed:** `tests/test_pipeline_ingest.py` is outside M3-02-01's file collision guard (`Owned by this plan: src/flag_football_ep/ingest/hc_workbook.py, tests/test_ingest_hc_workbook.py, docs/data-contract.md. Nothing else may be written.`), and it is not required by this plan's `<verification>` block.
- **Action taken:** Logged to `.planning/phases/M3-02-epa-refinement/deferred-items.md` with the exact failing assertions, root cause, and suggested follow-up owner (plausibly M3-02-04, which also regenerates `hc_games.csv`).
- **Verification that nothing else regressed:** `tests/test_ingest_hc_workbook.py`, `tests/test_validation_checks.py`, `tests/test_ingest_hc_dedupe.py`, `tests/test_m3_hc_pii.py` all green; `tests/test_pipeline_ingest.py` has exactly this one failure, everything else in that file passes.

---

**Total deviations:** 1 logged-not-fixed (scope boundary), 0 auto-fixed.
**Impact on plan:** None on this plan's own scope — all three tasks' `<verify>` commands and the plan-level `<verification>` block pass. The logged item is a downstream test-staleness finding caused by this plan correctly fixing the defect it was scoped to fix; it needs a small follow-up edit in a file this plan is not permitted to write.

## Issues Encountered

None beyond the deviation above.

## User Setup Required

None - no external service configuration required.

## Self-Check

Files:
- `src/flag_football_ep/ingest/hc_workbook.py` - FOUND
- `tests/test_ingest_hc_workbook.py` - FOUND
- `docs/data-contract.md` - FOUND
- `.planning/phases/M3-02-epa-refinement/deferred-items.md` - FOUND

Commits (`git log --oneline --all`):
- `2c60b72` - FOUND
- `cff9570` - FOUND
- `93f9852` - FOUND

Plan-level verification re-run:
- `uv run pytest tests/test_ingest_hc_workbook.py -q` - PASS
- `uv run pytest tests/test_validation_checks.py tests/test_ingest_hc_dedupe.py -q` - PASS
- `grep -q frozenset src/flag_football_ep/ingest/hc_workbook.py` - PASS
- `git status --porcelain data/` - empty (PASS)

All 5 `<success_criteria>` met.

## Next Phase Readiness

- Task 1/2/3 done; wave-1 half+segmentation fixes are in place for M3-02-04 to build on.
- **Blocker for M3-02-04:** `data/reference/hc_games.csv` must be regenerated (re-run `ffep ingest --source hc_workbook`) before any pair-block game can move from provisional to trainable — this plan deliberately left that file untouched.
- **Deferred, not blocking:** `tests/test_pipeline_ingest.py`'s stale quarantine-assumption test (see Deviations / `deferred-items.md`) should be updated by whichever plan next touches that file.

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-03*
