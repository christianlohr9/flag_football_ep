---
phase: M3-04-player-analysis-report
plan: 06
subsystem: ingest
tags: [polars, canonical-schema, hc-workbook, drop-column, adj-comp-pct]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "M3-02-04's real ffep ingest run (1,964 HC rows in plays.parquet) — the corpus this plan's mapping will apply to on the next ingest run"
  - phase: M3-01-hc-workbook-ingest
    provides: "M3-01-02's NULLABLE_EXTRAS / _HC_ONLY_RENAME precedent (bf_action, hand, air_yards, efficiency) this plan copies the shape of"
provides:
  - "canonical.NULLABLE_EXTRAS['drop']: pl.Utf8 — the head coach's Data!W charting column, kept as text (his own COUNTIFS(...,\"*\") criterion matches text only)"
  - "ingest/hc_workbook.py::_HC_ONLY_RENAME['DROP'] = 'drop' — case/whitespace-insensitive header mapping"
  - "tests/test_m3_drop_column.py: cross-layer proof that drop survives conform_to_canonical, plus a guarded (skip-if-absent) proof that flag_football_ep.reports.player_analysis.hc_columns_by_qb picks it up with no report-code change"
affects: ["M3-04-03", "M3-04-04", "M3-04-07"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Guarded cross-layer test (try/import + pytest.mark.skipif) for a consumer module owned by a different, not-yet-landed plan in the same phase — activates automatically on merge instead of faking or duplicating the consumer's contract"

key-files:
  created:
    - tests/test_m3_drop_column.py
  modified:
    - src/flag_football_ep/canonical.py
    - src/flag_football_ep/ingest/hc_workbook.py
    - tests/test_ingest_hc_workbook.py

key-decisions:
  - "drop stored as pl.Utf8, not a boolean — his Adj Comp % formula's COUNTIFS(Data!W,\"*\") wildcard criterion matches text only in Excel; a numeric 1 would not be counted by his own sheet, so the raw charted text is preserved."
  - "Real HC workbook (data/raw/hc_files/) is absent from this worktree — used the research-transcription fallback (Data!W header literally \"Drop\", M3-04-RESEARCH.md Pattern 1) per the plan's <pii_discipline> fallback clause, not a live read."
  - "flag_football_ep.reports.player_analysis (M3-04-03/04's deliverable) does not exist in this worktree — this plan ran ahead of its nominal wave, file-disjoint from plans 03/04 per orchestrator decision. The two hc_columns_by_qb assertions in tests/test_m3_drop_column.py are written against the exact interface M3-04-03-PLAN.md specifies and guarded with pytest.mark.skipif so they activate the moment that module lands, rather than being invented against a module that isn't there or silently dropped from the plan's required cross-layer proof."
  - "Updated two pre-existing tests in tests/test_ingest_hc_workbook.py (test_unknown_headers_named_not_silently_dropped, test_unknown_headers_charting_extras_are_renamed_not_listed) that hard-coded \"Drop\" as an unmapped header — same precedent M3-01-02 set when its four extras were added; this is a necessary consequence of the mapping, not a deviation."

requirements-completed: [HC-05]

# Metrics
duration: 55min (across an interrupted/resumed session — see Issues Encountered)
completed: 2026-09-04
---

# Phase M3-04 Plan 06: HC Drop column canonical extra and header mapping Summary

**`drop` (Utf8) joins `canonical.NULLABLE_EXTRAS` and `ingest/hc_workbook.py::_HC_ONLY_RENAME` maps the head coach's `DROP` header onto it, unblocking `Adj Comp %`/`adj Pass Yards`/`adj YPA` for the report layer with a two-line source diff.**

## Performance

- **Duration:** ~55 min of active work, spread across a session interrupted by a background-process/machine-sleep loss (test-suite re-run had to be redone from scratch; see Issues Encountered)
- **Started:** 2026-09-04T06:50:07Z
- **Task 1 committed:** 2026-09-04T05:54:09Z (f80a47c)
- **Task 2 committed:** 2026-09-04T06:49:57Z (6dfe75f)
- **Tasks:** 2
- **Files modified:** 4 (1 created, 3 modified)

## Accomplishments

- `src/flag_football_ep/canonical.py`: added `"drop": pl.Utf8` to `NULLABLE_EXTRAS`, in the Utf8 group beside `bf_action`/`hand`, with a comment naming its origin (`Data!W`) and the reason for the text dtype (his own `COUNTIFS(..., "*")` criterion matches text only).
- `src/flag_football_ep/ingest/hc_workbook.py`: added `"DROP": "drop"` to `_HC_ONLY_RENAME`, with a comment naming the three report columns it unblocks.
- `tests/test_ingest_hc_workbook.py`: 4 new tests (canonical-column/dtype check, case/whitespace-insensitive header mapping via `map_block_to_frame`, materialize-as-null-when-absent via `ingest_workbook`, survives-to-canonical-frame-when-present via `ingest_workbook`) plus two necessary edits to existing tests that hard-coded "Drop" as an unmapped header.
- `tests/test_m3_drop_column.py` (new, 165 lines): the plan's required cross-layer proof.
  - Unconditional: `drop` survives `conform_to_canonical` with its charted text preserved, and is correctly materialized as a typed null (and named in `ConformReport.materialized_extras`) when the source frame has no `drop` column at all.
  - Guarded (`pytest.mark.skipif` on `flag_football_ep.reports.player_analysis`'s absence): `hc_columns_by_qb` reports none of `adj_comp_pct`/`adj_pass_yards`/`adj_ypa` in `unavailable` when `drop` is present, with `adj_comp_pct`/`adj_pass_yards` strictly greater than their unadjusted neighbours on a fixture with a dropped incompletion; and all three land back in `unavailable` (never silently equal to their unadjusted neighbour) when the `drop` column is genuinely absent from the frame.
- `git diff --stat src/` between the two task commits shows exactly `canonical.py` (+7) and `ingest/hc_workbook.py` (+2) — no other ingest behaviour moved.

## Task Commits

1. **Task 1: The drop extra and its header mapping** - `f80a47c` (feat)
2. **Task 2: Cross-layer proof, guarded for the not-yet-landed report module** - `6dfe75f` (test)

## Files Created/Modified

- `src/flag_football_ep/canonical.py` - `"drop": pl.Utf8` added to `NULLABLE_EXTRAS`
- `src/flag_football_ep/ingest/hc_workbook.py` - `"DROP": "drop"` added to `_HC_ONLY_RENAME`
- `tests/test_ingest_hc_workbook.py` - 4 new tests for the drop mapping/materialization behavior; 2 existing tests updated for the now-mapped `Drop` header
- `tests/test_m3_drop_column.py` - cross-layer proof (drop through `conform_to_canonical`, guarded `hc_columns_by_qb` proof)

## The confirmed header token

The real HC workbook (`data/raw/hc_files/`) is not present in this worktree. Per this plan's
`<pii_discipline>` fallback clause, used the research-transcription value instead of a live read:
`Data!W`'s header is literally `"Drop"` (M3-04-RESEARCH.md Pattern 1, `G2`/`N2` formula citations).
`_HC_RENAME_UPPER`'s lookup is case- and whitespace-insensitive by construction, so `DROP`/`Drop`/
`drop`/` Drop ` all resolve to the same canonical `drop` column regardless of exactly how it reads
in the real file — verified by a dedicated parametrized test
(`test_drop_header_any_case_or_whitespace_maps_to_drop_column_raw_text`).

## Decisions Made

- `drop` is `pl.Utf8`, not a boolean flag — matches the M3-01-02 precedent's reasoning style and the plan's own interface spec: the workbook's own formula only counts non-blank text via a wildcard match, so storing anything other than the raw charted text would silently change what "dropped" means relative to his sheet.
- The two pre-existing `tests/test_ingest_hc_workbook.py` tests that listed `"Drop"` among unmapped headers were updated (not left broken) — this mirrors exactly what happened to `air_yards`/`hand`/`efficiency`/`thrown_by`/`target`/`received_by` when M3-01-02 added those four extras; a header becoming mapped necessarily means it leaves the unmapped-notice test's expected set.
- `flag_football_ep.reports.player_analysis` (owned by M3-04-03/04, `file_collision_guard` READ-ONLY for this plan) does not exist in this worktree because those plans have not landed here (this plan runs ahead of its nominal wave by orchestrator decision). Rather than invent that module's contract here (a collision this plan's precondition mechanism exists specifically to prevent) or silently omit the report-layer half of the required cross-layer proof, the two `hc_columns_by_qb`-dependent tests are written to the letter of `M3-04-03-PLAN.md`'s documented interface (`HcColumnTable.table`/`.unavailable`/`.notices`, `hc_columns_by_qb(plays, *, group_col="thrown_by")`) and gated with `pytest.mark.skipif(not _HAS_PLAYER_ANALYSIS, ...)`. They currently report `2 skipped` with a named reason; they will activate as real assertions the moment that module lands (this worktree's merge, or any later `uv run pytest`), with no further action needed from this plan.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug/expected consequence] Updated two existing tests that hard-coded "Drop" as unmapped**
- **Found during:** Task 1, running `tests/test_ingest_hc_workbook.py` after the mapping change
- **Issue:** `test_unknown_headers_named_not_silently_dropped` asserted `"Drop"` appears in the unmapped-header notice; `test_unknown_headers_charting_extras_are_renamed_not_listed` did not include `"drop"` in its list of renamed extras. Both would fail once `DROP` became a mapped header — the same shape of update M3-01-02 made for its own four extras when it added them.
- **Fix:** Removed `"Drop"` from the unmapped-header assertion (with a comment explaining why); added `"drop"` to the renamed-extras assertion.
- **Files modified:** `tests/test_ingest_hc_workbook.py`
- **Verification:** `uv run pytest tests/test_ingest_hc_workbook.py -q` — all green.
- **Committed in:** `f80a47c` (Task 1 commit)

**2. [Rule 1 - Bug] Corrected an assumed empty-cell reading in a new test**
- **Found during:** Task 1, writing `test_ingest_workbook_drop_header_present_survives_to_canonical_frame`
- **Issue:** Assumed a charted empty-string `Drop` cell would read back as `""`; the real behavior (openpyxl reading an empty cell) is `None`.
- **Fix:** Corrected the test's expected value to `None`, with a comment naming why.
- **Files modified:** `tests/test_ingest_hc_workbook.py`
- **Verification:** re-ran the test, green.
- **Committed in:** `f80a47c` (Task 1 commit)

**3. [Process — not a Rule 1-4 category, noted for honesty] Guarded rather than blindly implemented Task 2's report-layer assertions**
- **Found during:** Task 2, reading the plan's `<interfaces>` block and confirming `flag_football_ep.reports.player_analysis` does not exist in this worktree (`find . -iname "*player_analysis*"` returns nothing outside `.planning/`)
- **Issue:** The plan's own `file_collision_guard` makes `src/flag_football_ep/reports/**` READ-ONLY and explicitly owned by plans 03/04, which have not executed in this worktree — this is precisely the cross-phase collision the plan's precondition mechanism (checked against `M3-02-04-SUMMARY.md`) was meant to prevent, except the precondition check itself did not cover this specific dependency.
- **Fix:** Implemented the unconditional half of the cross-layer proof (drop through `conform_to_canonical`) directly, and wrote the report-layer half against `M3-04-03-PLAN.md`'s documented (not-yet-built) interface, guarded with `pytest.mark.skipif` rather than either inventing/duplicating the module here or dropping that half of the plan's required proof silently.
- **Files modified:** `tests/test_m3_drop_column.py`
- **Verification:** `uv run pytest tests/test_m3_drop_column.py -v` — 2 passed, 2 skipped (reason: module not present). No `FAILED`.
- **Committed in:** `6dfe75f` (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1, both inside Task 1's own test file), 1 process note (Task 2's guard). **Impact:** all three are necessary consequences of the mapping/cross-plan-timing this plan itself documents; no scope creep, no code added to `reports/**`, no other ingest behaviour touched.

## Issues Encountered

- **Background test-run loss to machine sleep, then resumed as a foreground run.** The full-suite verification (`uv run pytest tests -q`, minus the 10 files that fail to *collect* without the `cv` extras group — same exclusion list M3-01-02 documented) was first attempted as a backgrounded command; the sandbox appears to have slept mid-run (matches the known "MacBook sleep interrupts agents" pattern), and the coordinator flagged the background output as lost. Re-ran as one foreground command with an internal poll loop and an explicit long timeout; it completed cleanly. Across three independent full-suite attempts in this session (two partially lost to the sleep, one clean), the `FAILED` set was identical every time: 8 pre-existing failures, all in `tests/test_cv_cvat.py` (2x, real-CVAT-connection) and `tests/test_cv_detect_train.py` (6x, `ModuleNotFoundError: No module named 'rfdetr'`) — exactly the same 8 M3-01-02-SUMMARY.md already documented as pre-existing and environment-dependent, unrelated to any file this plan touches. No failure was ever observed in `tests/test_ingest_hc_workbook.py` or `tests/test_m3_drop_column.py` in any attempt.
- **The trailing pytest summary count line ("N failed, M passed in Ts") did not appear in any of the three full-suite log captures**, even the one that ran to completion (process exit confirmed, no more output arriving). This looks like an artifact of this specific resource-contended shared sandbox (matches M3-02-04-SUMMARY.md's own note about "extended verification pass in a resource-contended shared sandbox") rather than a real test failure — the `FAILED` list itself (which prints immediately before that line) was complete and identical across runs, and `grep -c "FAILED"` consistently returned exactly 8. Verification of the plan's actual scope (`tests/test_ingest_hc_workbook.py`, `tests/test_m3_drop_column.py`) was additionally run standalone with a normal, complete pytest report — both green.

## Verification Log

- `uv run pytest tests/test_ingest_hc_workbook.py -q` — PASS (all green, includes 4 new + 2 updated tests)
- `uv run pytest tests/test_ingest_hc_workbook.py tests/test_m3_drop_column.py -q` — PASS (2 skipped, named reason, no failures)
- `uv run python -c "..."` (canonical-column/dtype/rename-map check from the plan's Task 1 `<verify>`) — printed `ok`
- `git diff --stat src/` (between `f80a47c~1` and `6dfe75f`) — exactly `canonical.py` (+7) and `ingest/hc_workbook.py` (+2)
- `git status --porcelain data/ docs/` — empty
- `uv run pytest tests -q` (full suite, `cv`-extras-missing collection files excluded) — 3 attempts, `FAILED` set identical every time: 8 pre-existing, unrelated failures (`test_cv_cvat.py` x2, `test_cv_detect_train.py` x6); zero failures anywhere in this plan's files

## User Setup Required

None — no external service configuration required.

## Known Stubs

None new. `tests/test_m3_drop_column.py`'s two `hc_columns_by_qb`-dependent tests are intentionally
inert (`skipif`) until `flag_football_ep.reports.player_analysis` lands — this is documented in the
test file's module docstring and in "Decisions Made" above, not a silent gap.

## Next Phase Readiness

- **The one remaining operator step, as the plan requires this be written down rather than assumed:** the real `data/processed/plays.parquet`/`plays_scored.parquet` still carry no `drop` values. This plan only changes code (the canonical schema and the header mapping) — it does not run `ffep ingest`, per its own `file_collision_guard` ("No `ffep ingest` run and no write to `data/processed/**` happens in this plan"). A fresh `ffep ingest` (+ scoring) run against the real HC workbooks is required before the three adjusted columns can show real numbers in the rendered report; that run is owned by M3-2/the operator, same as M3-02-04's precedent.
- M3-04-03/M3-04-04, once landed, will make `flag_football_ep.reports.player_analysis` importable — at that point `tests/test_m3_drop_column.py`'s two guarded tests activate automatically and become real assertions against this plan's mapping; no code change needed in this plan's files for that to happen.
- M3-04-07 (open questions to the head coach) can cite this plan's stored decision directly: `drop` is text, and the report's flag (`drop` non-null and non-empty after stripping) is deliberately more permissive than his own `"*"`-wildcard formula — that divergence is Frage 9, recorded not resolved, per the plan's own `<interfaces>` section.

---
*Phase: M3-04-player-analysis-report*
*Completed: 2026-09-04*

## Self-Check: PASSED

- `src/flag_football_ep/canonical.py` — FOUND, contains `"drop"`
- `src/flag_football_ep/ingest/hc_workbook.py` — FOUND, contains `DROP`
- `tests/test_ingest_hc_workbook.py` — FOUND, 4 new tests + 2 updated
- `tests/test_m3_drop_column.py` — FOUND, 165 lines (min_lines: 60 satisfied), `grep -c "unavailable"` = 6 (key_link pattern satisfied)
- Commits `f80a47c` and `6dfe75f` — both present in `git log --oneline`
- `uv run pytest tests/test_ingest_hc_workbook.py tests/test_m3_drop_column.py -q` re-run — PASS
- `git diff --stat src/` between the two commits — exactly the two owned files
- `git status --porcelain data/ docs/` — empty
