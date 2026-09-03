---
phase: M3-04-player-analysis-report
plan: 02
subsystem: reference-data
tags: [polars, reference-csv, hc-workbook, camp-splits]

requires:
  - phase: M3-02-epa-refinement
    provides: "hc_games.csv's note-column row-window convention (`rows <first>-<last>`) written by the M3-02-04 refill"
provides:
  - "data/reference/hc_splits.csv: one maintained row per HC camp/competition tab, cited to its formula cell, with the Camp IV/VI naming conflict recorded as label_status=conflict"
  - "load_hc_splits(path) -> pl.DataFrame: validates duplicate keys/split_keys, bad ranges, overlapping windows, bad label_status, empty label_de/split_key"
  - "resolve_hc_game_splits(games, splits) -> pl.DataFrame: assigns a split_key to each declared HC game by row-window containment, never raises, names every unresolved case via split_match"
  - "ReferenceFiles.hc_splits / [reference].hc_splits config key, optional with a pre-M3-04-compatible default"
affects: ["M3-04-03", "M3-04-04", "M3-04-05"]

tech-stack:
  added: []
  patterns:
    - "Row-range containment lookup for a workbook whose only split key is manual paste order, not a column -- documented as a compromise in both function docstrings rather than hidden"
    - "Named non-match states (split_match) instead of a nearest-window guess, mirroring load_hc_games's name-the-offending-value rejection style"

key-files:
  created:
    - data/reference/hc_splits.csv
    - tests/test_reference_hc_splits.py
  modified:
    - src/flag_football_ep/reference.py
    - src/flag_football_ep/config.py
    - ffep.toml
    - tests/test_m3_hc_pii.py

key-decisions:
  - "Camp IV/VI naming conflict recorded as label_status=conflict on a single row (split_key=camp-iv-vi) rather than picking either tab's name -- REP-D03 amendment and Frage 7 (M3-04-07) stay the resolution path, not this plan."
  - "resolve_hc_game_splits never raises: unresolved games get a named split_match (spans-multiple, outside-known-windows, no-row-range, no-window-for-source) with a null split_key, so a partial-overlap or unknown-source game is visible in report output rather than silently dropped or guessed."

requirements-completed: [HC-05]

duration: 14min (implementation) + extended verification (resource-contended shared environment)
completed: 2026-09-03
---

# Phase M3-04 Plan 02: HC camp/competition split reference table Summary

**`data/reference/hc_splits.csv` records the five HC camp/competition row-range windows (cited to their formula cells, Camp IV/VI flagged as a naming conflict) with `load_hc_splits` validation and a `resolve_hc_game_splits` containment resolver that names every unresolved game instead of guessing.**

## Performance

- **Duration:** ~14 min of implementation (23:30-23:44 CEST), plus an extended verification pass in a resource-contended shared sandbox (see Issues Encountered)
- **Started:** 2026-09-03T23:30:30+02:00
- **Completed:** 2026-09-03T23:44:13+02:00 (implementation); verification concluded 2026-09-04
- **Tasks:** 2 (Task 2 is `tdd="true"`: RED + GREEN, no REFACTOR needed)
- **Files modified:** 6 (2 created, 4 modified)

## Accomplishments
- `data/reference/hc_splits.csv`: five rows for `offense-analytics-2026-camps-and-competitions`/`data` (camp-i, mexico, camp-iii, camp-iv-vi, camp-v), each with a German provenance note citing the formula cell and date read; `camp-iv-vi` recorded as `label_status=conflict`
- `load_hc_splits(path)`: rejects duplicate `(workbook, sheet, split_key)`, duplicate `split_key`, `first_row < 2`, `last_row < first_row`, overlapping windows within one `(workbook, sheet)`, unknown `label_status`, and empty `label_de`/`split_key` -- every rejection names the offending value(s)
- `resolve_hc_game_splits(games, splits)`: parses `rows <first>-<last>` out of `note` with one compiled regex, joins on `(workbook, sheet)`, assigns by containment; never raises -- `split_match` is one of `matched`, `spans-multiple`, `outside-known-windows`, `no-row-range`, `no-window-for-source`
- `ReferenceFiles.hc_splits` / `[reference].hc_splits`: optional field following the `hc_games` precedent exactly, so a pre-M3-04 `ffep.toml` still loads unchanged
- `tests/test_m3_hc_pii.py::_CHECKED_ARTEFACTS` extended to cover `data/reference/hc_splits.csv`

## Task Commits

Task 2 followed TDD (RED -> GREEN, no REFACTOR needed -- the first implementation passed all tests):

1. **Task 1: The maintained split table, its loader and the config key** - `6acd91f` (feat)
2. **Task 2 RED: failing test for resolve_hc_game_splits** - `280d0e1` (test)
3. **Task 2 GREEN: implement resolve_hc_game_splits** - `cacb778` (feat)

**Plan metadata:** commit follows this SUMMARY.

## Files Created/Modified
- `data/reference/hc_splits.csv` - five row-window rows, formula-cell-cited, Camp IV/VI conflict recorded
- `src/flag_football_ep/reference.py` - `_HC_SPLITS_SCHEMA`, `load_hc_splits`, `HC_SPLIT_MATCH_STATES`, `resolve_hc_game_splits`
- `src/flag_football_ep/config.py` - `ReferenceFiles.hc_splits` optional field + `load_config` resolution
- `ffep.toml` - `hc_splits` key in `[reference]`
- `tests/test_reference_hc_splits.py` - one test per `split_match` state, input-order/empty-frame coverage, one real-file `load_hc_splits` validation test (184 lines)
- `tests/test_m3_hc_pii.py` - `data/reference/hc_splits.csv` added to `_CHECKED_ARTEFACTS`

## Decisions Made
- Camp IV/VI stays a single `conflict`-labelled row rather than two competing rows or a guessed name -- keeps the head coach's open naming question visible to every downstream consumer (plans 03-05) instead of silently resolved.
- `resolve_hc_game_splits` returns null `split_key` plus a named `split_match` for every unresolved case rather than raising or nearest-matching -- matches the plan's "unresolved games are named, not absorbed" success criterion and the threat register's Spoofing mitigation (T-M3-04-06).

## Deviations from Plan

None - plan executed exactly as written. `resolve_hc_game_splits`'s signature, `split_match` vocabulary, and CSV schema all match the plan's `<interfaces>`/`<action>` sections verbatim.

## Issues Encountered

- The sandboxed execution environment ran three additional heavy pytest processes concurrently across worktrees (this plan's own broad verification runs plus a sibling worktree's full suite), causing severe CPU contention and very slow wall-clock full-suite runs. Two independent full-suite attempts both surfaced the same `F` failures at the same relative position; isolating them (`uv run pytest tests/test_cv_continuity.py tests/test_cv_contracts.py tests/test_cv_coordinates.py tests/test_cv_cvat.py`) traced them to `ModuleNotFoundError: No module named 'cvat_sdk'` in `tests/test_cv_cvat.py` -- the optional `cv` extras group was never installed (only base `uv sync` was run, per this plan's setup instructions; `cv/**` is explicitly out of scope per the file collision guard). This is a pre-existing environment gap, unrelated to `reference.py`/`config.py`, and out of scope for this plan (Scope Boundary rule). Not fixed.
- To confirm the above did not mask a real regression, a 20-file targeted subset covering every test module that imports `flag_football_ep.reference`/`config` broadly (`test_reference.py`, `test_config.py`, `test_reference_hc_splits.py`, `test_m3_hc_pii.py`, plus ingest/model/cli/pipeline/reports tests) was run directly (unpiped, so its exit code is trustworthy) and passed cleanly with zero failures.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Plans M3-04-03/04/05 can now filter by `resolve_hc_game_splits`'s `split_key`/`split_match` output instead of inventing their own row-range magic numbers.
- The exact signature, `split_match` vocabulary (`matched`, `spans-multiple`, `outside-known-windows`, `no-row-range`, `no-window-for-source`), and CSV schema (`workbook,sheet,first_row,last_row,split_key,label_de,label_status,source_tabs,note`) are stable public surface for those plans to consume.
- No blockers. The Camp IV/VI naming question (Frage 7) remains open for the head coach and is out of this plan's scope by design.

---
*Phase: M3-04-player-analysis-report*
*Completed: 2026-09-03*

## Self-Check: PASSED

- FOUND: data/reference/hc_splits.csv
- FOUND: tests/test_reference_hc_splits.py
- FOUND: .planning/phases/M3-04-player-analysis-report/M3-04-02-SUMMARY.md
- FOUND commit 6acd91f (feat: hc_splits.csv + load_hc_splits + config wiring)
- FOUND commit 280d0e1 (test: RED phase for resolve_hc_game_splits)
- FOUND commit cacb778 (feat: GREEN phase, resolve_hc_game_splits implementation)
