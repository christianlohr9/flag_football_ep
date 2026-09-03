---
phase: M3-01-hc-workbook-ingest
plan: 01
subsystem: data-contract
tags: [data-contract, result-vocabulary, hc-workbook, german-questions]

# Dependency graph
requires:
  - phase: M2 (foundation)
    provides: data contract v1.1, validation/schema.py version gate, ingest/hudl.py token grammar
provides:
  - "Data contract v1.2: six new RESULT base tokens (Block, Blocked, Batted Down, Dropped, Timeout, Offsetting Penalties) with defined semantics, dated Änderungsvermerk naming the head coach as source"
  - "docs/hc-rueckfragen-2026-09.md Fragen 1-3 answered by the head coach (2026-09-03, e-mail), verbatim in ## Antworten with date/channel"
  - "Confirmed: SP workbook Data tab is the master corpus source for all three workbooks; EC-2025 Data tab is empty by design, not a data-loss bug"
  - "Confirmed: pair-block rows use O/D/S markers (S = no-play) or a team name in the block's first row; a block runs to the next blank row or next team-name row"
  - "Confirmed: all six RESULT-token semantics from contract v1.2 match the head coach's intent"
affects: [M3-01-02 (block segmentation - already implemented, consistent with the block rule), M3-01-03 (game-identity + dedupe), M3-2 (EP/WP training - Timeout/Offsetting Penalties confirmed excludable as no_play)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Änderungsvermerk convention for contract amendments: dated, source-attributed, user-approval-noted, semantics-pending marker until domain-expert confirmation arrives"
    - "## Antworten section in a forwarded question doc: answers pasted verbatim with a single date/channel attribution line above them, read directly by downstream plans instead of re-derived"

key-files:
  created: []
  modified:
    - docs/data-contract.md
    - docs/data-contract.schema.json
    - src/flag_football_ep/validation/schema.py
    - src/flag_football_ep/ingest/hudl.py
    - tests/test_ingest_hudl.py
    - tests/test_validation_schema.py
    - .planning/PROJECT.md
    - docs/hc-rueckfragen-2026-09.md

key-decisions:
  - "Six HC RESULT tokens enter the fixed vocabulary in contract v1.2 immediately (user approval 2026-09-03), semantics proposed by us and only their interpretation sent for confirmation - not held back as tok_unknown while awaiting an answer"
  - "Timeout and Offsetting Penalties map to play_type 'no_play' (like Penalty), so they cannot enter EP/WP training as if they were plays; head coach confirmed this reading in Frage 3 ('Ja')"
  - "The EC-2025 workbook's empty Data tab is confirmed intentional: the Scoring-Probability workbook's Data tab is the head coach's actual master source for all his files, not a lost export"
  - "Pair-block boundary rule confirmed by the head coach: a block runs from a row carrying O/D/S (S = no-play) or a team name in the first row, until a blank row or the next team-name row - this is the rule M3-01-02's dtype-based segmentation already implements and needed no code change"
  - "The signature-date marker (SIGNATUR-DATUM-TBD) is untouched - that belongs to plan M2-1's document, not this plan; only the workbook questions were answered here"

patterns-established:
  - "Amendment paragraphs in docs/data-contract.md record source, user approval date, and a pending-confirmation marker until the domain expert (head coach) answers"

requirements-completed: [HC-01]

# Metrics
duration: continuation (task 3 resumed after external e-mail exchange, 2026-09-03)
completed: 2026-09-03
---

# Phase M3-01 Plan 01: HC Workbook Ingest - Contract v1.2 and Head Coach Questions Summary

**Data contract v1.2 (six HC RESULT tokens with defined semantics) plus a German question doc whose three structural questions came back answered by the head coach via e-mail on 2026-09-03: SP workbook `Data` tab is the master corpus, pair-blocks run first-row-marker-to-next-blank-or-team-name, and all six token semantics confirmed.**

## Performance

- **Tasks:** 3 (Task 1 contract v1.2, Task 2 question doc, Task 3 checkpoint - forward and record answer)
- **Files modified:** 8 (contract doc/schema/code/tests/PROJECT.md in Task 1; question doc in Tasks 2-3)
- **Completed:** 2026-09-03

## Accomplishments
- Data contract v1.2: six new RESULT base tokens (`Block`, `Blocked`, `Batted Down`, `Dropped`, `Timeout`, `Offsetting Penalties`) added to `docs/data-contract.md`, `docs/data-contract.schema.json`, `src/flag_football_ep/ingest/hudl.py` (`_BASE_TOKENS`, `_TOKEN_COLUMN`, `derive_outcome_columns`), `src/flag_football_ep/validation/schema.py` (`_BASELINE_MINOR = 2`), with matching grammar-table test rows and PROJECT.md C-07 updated
- `docs/hc-rueckfragen-2026-09.md`: German question doc forwarded to the head coach (Jona Winkel) and answered by e-mail on 2026-09-03
- Frage 1 answer: the EC-2025 workbook's `Data` tab is empty by design - the Scoring-Probability workbook's `Data` tab is the master source for all his files
- Frage 2 answer: pair-block rows carry `O`/`D`/`S` markers in column 1 (`S` = no-play) or, in an earlier/later charting period, a team name in the block's first row only; a block runs until a blank row or the next team-name row
- Frage 3 answer: all six RESULT token semantics from contract v1.2 confirmed ("Ja")
- This continuation added the date/channel attribution line above the three answers in `## Antworten` (2026-09-03, e-mail, HC Jona Winkel) so a later mapping decision traces to a stated answer, not to memory (threat T-M3-01-04)

## Task Commits

1. **Task 1: Data contract v1.2** - `3ed13f3` (feat)
2. **Task 2: German question list for the head coach** - `dd0d93c` (docs)
3. **Task 3: checkpoint resolution - answers pasted into `## Antworten`** - `824b4f9` (docs, prior session)
4. **This continuation: date/channel attribution line for Fragen 1-3's answers** - `d0f270a` (docs)

**Plan metadata:** (this commit) - docs: complete plan

## Files Created/Modified
- `docs/data-contract.md` - v1.2 Änderungsvermerk: six tokens, semantics table, source and approval date
- `docs/data-contract.schema.json` - `contract_version` 1.1 -> 1.2, extended `base_tokens`, extended `RESULT` note
- `src/flag_football_ep/ingest/hudl.py` - six new tokens in `_BASE_TOKENS`/`_TOKEN_COLUMN`; `derive_outcome_columns` folds Block/Blocked/Batted Down/Dropped into `incomplete_pass`, Timeout/Offsetting Penalties into `play_type == "no_play"`
- `src/flag_football_ep/validation/schema.py` - `_BASELINE_MINOR = 2`
- `tests/test_ingest_hudl.py` / `tests/test_validation_schema.py` - grammar-table rows for the six tokens plus `"Blocked, Def TD"`; version assertions updated to 1.2/1.3
- `.planning/PROJECT.md` - C-07 vocabulary list extended, v1.2 note appended
- `docs/hc-rueckfragen-2026-09.md` - question doc; `## Antworten` now carries Fragen 1-3 answered verbatim with a date/channel attribution line above them (Fragen 4-6, added later by plan M3-03-02, remain unanswered and were left untouched)

## Decisions Made
See `key-decisions` in frontmatter. In short: the six tokens went live in the contract on the user's approval alone; only their semantic interpretation needed the head coach's sign-off, and it arrived confirming all six. The empty EC-2025 `Data` tab and the pair-block first-row marker rule are now settled facts rather than open questions - both consequential for M3-2's training corpus and for the pair-block reader plans (M3-01-02/03).

## Deviations from Plan

None - plan executed exactly as written. The forwarding checkpoint (Task 3) resolved in a prior session (the user forwarded the questions and pasted the head coach's e-mail answers into `## Antworten`, commit `824b4f9`). This continuation completed Task 3's remaining on-resume action: recording the date and channel above the answers, per the plan's instruction ("append whatever came back verbatim under `## Antworten`... each with the date and the channel").

The plan's Task 3 text also mentions a `SIGNATUR-DATUM-TBD` marker in its broader on-resume guidance from the phase context; that marker is **not applicable here** - no signature has arrived, only the workbook question answers. The marker was left untouched, as instructed. It belongs to plan M2-1's document, not this plan.

No heading-level "beantwortet" status-line convention exists anywhere in `docs/hc-rueckfragen-2026-09.md` (checked: no `## Frage N` heading in the doc carries a status marker, for Fragen 1-3 or 4-6). Rather than inventing a new convention not used elsewhere in the doc, only the single date/channel attribution line above the answers was added, consistent with the plan's primary instruction.

## Issues Encountered

`uv run pytest tests -q` (full suite) has one pre-existing, unrelated failure: `tests/test_pipeline_ingest.py::test_run_ingest_hc_failing_game_quarantined_not_warned`. This is out of scope for this plan (a doc-only continuation touching only `docs/hc-rueckfragen-2026-09.md`) - not caused by this change, not fixed here, logged here for visibility. `tests/test_m2_legal_docs.py` (7 tests, includes the PII gate over this doc) and the plan's own verify chain (six `## Frage` headings, `Batted Down`, `2.506`, `v1.2`, `## Antworten` all present) both pass.

## Next Phase Readiness

Contract v1.2 and all three forwarded questions are now fully resolved with head-coach-confirmed answers on record. Consequences for downstream work:
- **M3-2 (EP/WP training):** Timeout and Offsetting Penalties are confirmed non-plays (`play_type == "no_play"`) and can be excluded from training weight with the head coach's explicit sign-off, not just our assumption.
- **M3-01-02/03 (pair-block column mapping, already implemented):** the block-boundary rule the head coach described (first-row marker or team name, running to next blank/team-name row) matches the dtype-based block segmentation `ingest/hc_workbook.py` already implements (per `M3-01-02-SUMMARY.md`) - no rework needed, decision now has a traceable answer instead of a research inference.
- **M3-2 corpus selection:** the Scoring-Probability workbook's `Data` tab is the confirmed master source across all of the head coach's files; the EC-2025 workbook's `Data` tab and `Copy of Data` (SP workbook) should be treated as stale/superseded rather than as data-loss.
- Nothing in the phase was blocked on this plan; the checkpoint's resolution only sharpens decisions plans M3-01-02/03 already made from research inference into head-coach-confirmed fact.
- The head coach's separate signature (for licensing/data-release, tracked under plan M2-1) has still not arrived - `SIGNATUR-DATUM-TBD` remains open and is out of scope for this plan.

---
*Phase: M3-01-hc-workbook-ingest*
*Completed: 2026-09-03*
