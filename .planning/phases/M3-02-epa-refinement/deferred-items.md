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
