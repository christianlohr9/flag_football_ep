# Deferred Items — M3-02-epa-refinement

Items found during plan execution that are out of the executing plan's
file-collision-guard scope. Not fixed; logged here per deviation-rules scope
boundary.

## From M3-02-01 (unordered pair segmentation + half=2 sentinel)

### `tests/test_pipeline_ingest.py::test_run_ingest_hc_failing_game_quarantined_not_warned` is now stale

**Not fixed** — `tests/test_pipeline_ingest.py` is outside M3-02-01's file
collision guard (`Owned by this plan: src/flag_football_ep/ingest/hc_workbook.py,
tests/test_ingest_hc_workbook.py, docs/data-contract.md. Nothing else may be
written.`).

**What changed:** This test's `hc_tree` fixture declares `hc-test-game-a`
(a clean, 3-row numeric-block HC game with no header/domain/PLAY#/half
defects) in its inline `hc_games.csv`. Before M3-02-01, `ingest_workbook`
blanket-stamped `half = null` for every HC row, so `half_assigned` FAILed for
every HC game unconditionally — including `game-a` — and the test's docstring
explicitly documents this as "the honest, expected outcome, not a defect in
the fixture."

M3-02-01 Task 2 fixes exactly this defect: a declared, non-`Copy of Data`
game now gets the `half=2` sentinel and genuinely PASSes `half_assigned`
(per this plan's own `<done>` criterion). `game-a` is such a game, so it is
now `OK` (not quarantined, 0/6 checks failed) — the *intended*, correct
outcome of the fix, not a regression in `hc_workbook.py`.

**What needs updating (out of scope for M3-02-01):**
- `test_run_ingest_hc_failing_game_quarantined_not_warned`'s docstring
  (lines ~964-972) still describes the pre-fix blanket-null behavior.
- `assert all(g.quarantined for g in hc_games_results)` (line 978) — now
  false; only `game-b` (undeclared, out-of-range `DN`) quarantines.
- `assert any("half_assigned" in reason for reason in game_a.reasons)`
  (line 983) — `game_a.reasons` is now empty (game_a is `OK`).
- The fixture may need a *third* HC game (still-undeclared, otherwise-clean)
  if the test's intent — "hc_workbook is never warn-only, every FAIL means
  quarantine, clean or not" — should keep being demonstrated on a still-FAILing
  case after this fix; `game-b`'s `downs_range` failure alone already covers
  that intent for the undeclared case.

**Verified not a regression:** `tests/test_ingest_hc_workbook.py`,
`tests/test_validation_checks.py`, `tests/test_ingest_hc_dedupe.py`,
`tests/test_m3_hc_pii.py` all green after the fix; only this one test in
`tests/test_pipeline_ingest.py` (out of scope) is affected.

**Suggested owner:** whichever plan next touches `tests/test_pipeline_ingest.py`
or HC pipeline integration (plausibly M3-02-04, which also regenerates
`hc_games.csv` after the segmentation rule change).

**Status: fixed by M3-02-04** — see that plan's SUMMARY for the fixture
change (a third, still-undeclared HC game keeps the "every FAIL means
quarantine" assertion meaningful).

## From M3-02-04 (header-block segmentation rule, Frage 2 Antwort 2026-09-03)

### Blank-row block boundary (head coach's rule) is not implemented — cannot be, without a `segment_blocks` change

**Not fixed** — out of scope for this plan's `src/flag_football_ep/**`
authorization, which is scoped to the header/marker rule only.

**What was found:** the head coach's confirmed Frage-2 answer names a blank
row as one of two block-boundary triggers ("bis zu einer leeren Zeile ...").
By the time a pair block's rows reach `_split_pair_block`, a genuinely blank
row has already been stripped twice over — once by `read_sheet_rows` (every
row where all cells are `None`/`""`), and again by `segment_blocks` (any row
whose column-A value is neither numeric nor a non-empty string, silently
skipped so it never fractures a block). Both leave the identical symptom in
`HcBlock.rows`: a gap in physical row numbers. An implementation that
inferred a blank-row boundary from that gap was written, tested against
synthetic fixtures, and then verified against the real workbook — and it
was WRONG: the real `Data`-tab pair block has 5 rows with a populated
DN/DIST/YARD LN but an empty column A/B (not blank rows, just missing team
identity), and the gap-inference treated each as a boundary, fragmenting the
validated 137 → 22 unordered-pair collapse (M3-02-RESEARCH.md Sec 1.2) down
to 137 → 18. Removed before commit; verified against the real file that
removing it restores the exact validated 22.

**What would fix it:** `segment_blocks` would need to distinguish, in its
return value, "row was blank" from "row was skipped for dtype reasons" —
today both simply increment a counter and vanish. A safe fix threads that
distinction through `HcBlock` (e.g. a `boundary_before: set[int]` of
physical row numbers that had a genuine blank row directly before them) so
`_split_pair_block` can trigger on it without the false-positive risk shown
above.

**Real-world impact of not fixing this:** none this run — the real `Data`-tab
pair block (the only pair-block content in scope for declaration) has zero
O/D/S marker rows at all (verified 2026-09-03), so no block in the currently
declarable corpus is affected by the missing blank-row rule either way; the
new-header-row half of the rule (which IS implemented) is sufficient for
every block boundary actually observed.

**Suggested owner:** whichever plan next needs the header+marker convention
to matter in practice — i.e. if a future workbook update actually contains
O/D/S marker rows spanning more than one real block separated only by a
blank row (not yet observed in this corpus).
