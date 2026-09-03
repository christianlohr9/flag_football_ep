---
phase: M3-01-hc-workbook-ingest
plan: 02
subsystem: ingest
tags: [openpyxl, xlsx, hc-workbook, block-segmentation, contract-mapping, canonical-schema]

# Dependency graph
requires:
  - phase: M2 (foundation)
    provides: canonical.py schema, validation/schema.py contract loader, config.py, ingest/hudl.py patterns
provides:
  - "openpyxl as an inventoried core runtime dependency (data_only + read_only .xlsx reading)"
  - "config.Paths.raw_hc_files / config.ReferenceFiles.hc_games (optional, pre-M3-compatible)"
  - "ingest/hc_workbook.py: read_sheet_rows, segment_blocks, map_block_to_frame (reading half only)"
  - "canonical.NULLABLE_EXTRAS: bf_action, hand (Utf8), air_yards, efficiency (Int32)"
affects: [M3-01-03 (game-identity + dedupe), M3-01-04 (pipeline wiring)]

# Tech tracking
tech-stack:
  added: [openpyxl 3.1.5]
  patterns:
    - "Block segmentation by column-1 dtype (float/int vs. str), not by header text -- classifies each row before any column mapping is trusted"
    - "Dtype-validated per-block column mapping: materialize absent core columns as null so validate_header never raises, then check_column_domains flags contradictions as DomainViolation instead of a silent cast"
    - "Pair-block unresolved tail: null out columns from a named anchor (RECEIVED BY) onward by position, keep raw values under synthetic hc_pair_team1/hc_pair_team2 names, one explaining notice per block"

key-files:
  created:
    - src/flag_football_ep/ingest/hc_workbook.py
    - tests/test_ingest_hc_workbook.py
  modified:
    - pyproject.toml
    - uv.lock
    - docs/lizenz-inventur.md
    - ffep.toml
    - src/flag_football_ep/config.py
    - src/flag_football_ep/canonical.py

key-decisions:
  - "openpyxl (not fastexcel/calamine) for the reason M3-01-RESEARCH.md documents: calamine assumes one consistent layout per sheet and cannot express dtype-based block segmentation"
  - "raw_hc_files/hc_games config keys resolved outside _PATH_KEYS/_REFERENCE_KEYS (optional, defaulted) so no pre-existing ffep.toml or test fixture needs an edit"
  - "RESULT column is the one column exempted from the integral-float-to-int-string cleanup: it is free text, so a numeric charting error (the corpus's real -5.0) is preserved verbatim as evidence rather than normalized to -5"
  - "Pair-block tail null-out is by header POSITION (index of RECEIVED BY), not by name -- the header names past that point are exactly what is unresolved for that block"

requirements-completed: [HC-01]

# Metrics
duration: 55min
completed: 2026-09-03
---

# Phase M3-01 Plan 02: HC Workbook Reader (Sheet Read + Block Segmentation + Contract Mapping) Summary

**openpyxl-backed HC workbook reader that classifies every row by its first cell's dtype before trusting the header, and maps each block onto the data contract with per-column domain validation instead of a blind cast.**

## Performance

- **Duration:** 55 min
- **Started:** 2026-09-03T~17:38Z
- **Completed:** 2026-09-03T~18:33Z
- **Tasks:** 3 (Task 1 auto; Tasks 2-3 TDD RED/GREEN)
- **Files modified:** 8 (2 created, 6 modified)

## Accomplishments
- `openpyxl` 3.1.5 added as a core (not extras-group) runtime dependency, licence-inventoried in `docs/lizenz-inventur.md`, MIT
- `config.Paths.raw_hc_files` / `config.ReferenceFiles.hc_games` resolve from `ffep.toml`, both optional-with-default so zero existing test fixture needed an edit
- `ingest/hc_workbook.py`: `read_sheet_rows` (formula-resolved, streaming, blank-row and `#N/A` counting, empty-tab reported as a named finding rather than "zero games"), `segment_blocks` (dtype-based block boundaries, immune to the float-not-int trap), `map_block_to_frame` (header normalization/dedup, Utf8 cast, pair-block tail null-out with notice, absent-core materialization, `validate_header`/`check_column_domains`, charting-column rename)
- Four new canonical extras (`bf_action`, `hand`, `air_yards`, `efficiency`) for the head coach's own charting columns with no Hudl equivalent
- 27 unit tests, all against synthetic `openpyxl.Workbook()` fixtures (`Alphaland`/`Betaland`, `Spieler A`/jersey `7`) -- no real workbook opened anywhere in this plan

## Task Commits

Each task was committed atomically (Tasks 2-3 are TDD: RED test commit, then GREEN implementation commit):

1. **Task 1: openpyxl dependency, licence row, config paths** - `9ebdc43` (feat)
2. **Task 2 RED: failing sheet-reader/block-segmentation tests** - `93e650f` (test)
2. **Task 2 GREEN: sheet reader + block segmentation** - `a53d133` (feat)
3. **Task 3 RED: failing per-block mapping tests** - `de53b69` (test)
3. **Task 3 GREEN: map_block_to_frame + canonical extras** - `c340e9c` (feat)
3. **Follow-up: docstring wording fix for the plan's `hc_files` grep check** - `7f98092` (test)

## Files Created/Modified
- `src/flag_football_ep/ingest/hc_workbook.py` (500 lines) - sheet reader, block segmentation, per-block contract mapping (reading half only; RESULT-token parsing, drive/scoring derivation and `conform_to_canonical` convergence are M3-01-03/04's job)
- `tests/test_ingest_hc_workbook.py` (372 lines, 27 tests) - synthetic-workbook coverage for every behaviour in the plan's `<behavior>` block
- `src/flag_football_ep/canonical.py` - four new `NULLABLE_EXTRAS` entries
- `src/flag_football_ep/config.py` - `Paths.raw_hc_files`, `ReferenceFiles.hc_games`, both optional/defaulted, resolved outside `_PATH_KEYS`/`_REFERENCE_KEYS`
- `ffep.toml` - `raw_hc_files`, `hc_games` entries
- `pyproject.toml` / `uv.lock` - `openpyxl>=3.1.5` core dependency
- `docs/lizenz-inventur.md` - openpyxl licence row (MIT, importlib.metadata)

## Decisions Made
- **RESULT vs. everywhere else for integral-float cleanup:** the plan's two behaviour bullets literally conflict for a value like `-5.0` (universal "integral float -> int string" vs. "RESULT lands as the string `-5.0`"). Resolved by exempting only the `RESULT` column from the cleanup: every other numeric-shaped column (jersey/`AIR YARDS`/`DN`/`DIST`/`YARD LN`/`PLAY #`) strips the trailing `.0` (both for `player_mapping.csv` lookup-key usability and because a `.0`-suffixed string fails polars' non-strict str->int cast downstream in `check_column_domains`), while `RESULT` — a free-text contract column — preserves the raw charted value verbatim as evidence of the data-entry error.
- **Pair-block anchor is positional, not name-based:** `PAIR_BLOCK_TAIL_ANCHOR = "RECEIVED BY"` is looked up by header position within the block's own header row, then everything from that index onward is nulled — this correctly nulls `GN/LS` too when the real Scoring Probability workbook's header places `GN/LS` after `RECEIVED BY` (confirmed against M3-01-RESEARCH.md Pitfall 2), even though a different workbook's header (Offense Analytics, no pair rows) places `GN/LS` before `RECEIVED BY`.
- **No new sheet-detection or fingerprint logic added:** this plan is scoped to reading and per-block mapping only; game-identity resolution, dedupe and `conform_to_canonical` convergence are explicitly plan M3-01-03/04's responsibility per the plan's interfaces section.

## Deviations from Plan

None - plan executed exactly as written. One in-scope clarification was required (see "Decisions Made" above: the RESULT/-5.0 vs. jersey/25.0 behaviour conflict) — resolved via the RESULT-column exemption rather than treated as a deviation, since both plan bullets are satisfied by the final tests.

## Issues Encountered
- The plan's verification step `grep -rn "hc_files" tests/test_ingest_hc_workbook.py` returns nothing was tripped by the module docstring's own prose (`data/raw/hc_files/`), not by any test opening a real file. Reworded the sentence to avoid the literal substring (commit `7f98092`) while keeping the same PII-discipline statement.
- The full unrestricted `uv run pytest tests -q` run requires the `cv` extras group (torch, opencv-python, rfdetr, supervision) and a running local CVAT instance, neither present in this worktree (by design -- this plan's `file_collision_guard` explicitly excludes `src/flag_football_ep/cv/**` and the CV/hackathon surface). Ran the full suite excluding the 10 files that fail to *collect* without the `cv` extra plus 2 files needing a live CVAT service; result: all pass except 8 pre-existing failures (6x `ModuleNotFoundError: No module named 'rfdetr'` in `test_cv_detect_train.py`, 2x CVAT-connection failures in `test_cv_cvat.py`) -- all unrelated to this plan's files and present independent of any change made here.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `read_sheet_rows`, `segment_blocks`, `map_block_to_frame`, `HcBlock`, `HcIngestNotices`, `hc_source_label`, `slugify`, `SheetNotFoundError`, `SHEET_NAMES`, `PAIR_BLOCK_TAIL_ANCHOR` are the public surface plan M3-01-03 builds on (game-identity resolution via `hc_pair_team1`/`hc_pair_team2` plus the numeric block's real team assignment, dedupe, RESULT-token parsing reusing `ingest.hudl`, `conform_to_canonical` convergence).
- `docs/hc-rueckfragen-2026-09.md`'s `## Antworten` section was still empty at plan-execution time; the pair-block tail therefore uses the null-and-notice fallback exactly as the plan specifies. If plan M3-01-01 (same wave, parallel worktree) records an answer to "Frage 2" before M3-01-03 starts, that plan should implement the stated column order instead of this fallback.
- No blockers for M3-01-03/04.

---
*Phase: M3-01-hc-workbook-ingest*
*Completed: 2026-09-03*

## Self-Check: PASSED

- All 8 created/modified files confirmed present on disk.
- All 6 task commits (`9ebdc43`, `93e650f`, `a53d133`, `de53b69`, `c340e9c`, `7f98092`) confirmed in `git log`.
- Re-ran `uv run pytest tests/test_ingest_hc_workbook.py tests/test_canonical.py tests/test_config.py tests/test_m2_lizenz_inventur.py -q`: all pass.
- `git diff --name-only` against the plan's base commit lists exactly the eight owned files, no more.
- `grep -rn "hc_files" tests/test_ingest_hc_workbook.py` returns nothing.
