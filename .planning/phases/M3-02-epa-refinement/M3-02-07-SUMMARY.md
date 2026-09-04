---
phase: M3-02-epa-refinement
plan: 07
subsystem: reporting
tags: [markdown, german-deliverable, hc-rueckfragen, doc-csv-guard, pytest]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "M3-02-05's four-arm ablation (ablation_summary.csv, per_source_metrics_{ep,wp}.csv,
      per_tier_metrics_{ep,wp}.csv, corpus_arms.csv, no_play_rows.csv) and M3-02-06's HC-vs-model
      comparison tables (comparison_by_dd.csv, comparison_clustered.csv, comparison_coverage.csv)
      -- every number in this plan's document is read out of those ten CSVs"
provides:
  - "docs/epa-refinement-2026-10.md: the German October-sync write-up (corpus, half/segmentation
    fixes, LOGO methodology, calibration incl. EP-now-beats-naive, the with/without-HC ablation,
    the HC-vs-model comparison, deliberate non-reproductions, standing exclusions, open questions,
    reproducibility) -- ten sections, every quoted figure test-pinned to a committed CSV"
  - "docs/hc-rueckfragen-2026-09.md: ## Zusatzfragen (M3-2, EPA-Update) appended (Zusatzfrage A/B
    -- half markers, competition classification) without renumbering Fragen 1-6 or touching
    another phase's plan file"
  - "tests/test_m3_epa_docs.py: bidirectional run-id guard, precision-tolerant log-loss figure
    guard over two structured tables, CSV-reference-coverage check, roster PII gate, and the
    hc-rueckfragen Frage/Zusatzfrage structural invariant"
affects: [M3-02-08-promotion-checkpoint, M3-04-handout]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Structured-table-only figure guard: the doc-vs-CSV test parses two specific Markdown
      tables (identified by a unique header substring) rather than regex-scanning the whole
      document for numbers -- keeps legitimately-cited Phase-1.3 historical figures (which are
      NOT in this phase's epa_refinement CSVs) out of the figure-matching scope while still
      catching drift in every number this plan's own measurements produced"
    - "Tolerance derived from written decimal precision (0.5 * 10^-decimals), not a fixed
      epsilon, so a figure written short in prose and a figure written at full CSV precision in
      a table are both checked honestly against their own stated precision"

key-files:
  created:
    - docs/epa-refinement-2026-10.md
    - tests/test_m3_epa_docs.py
  modified:
    - docs/hc-rueckfragen-2026-09.md

key-decisions:
  - "## Zusatzfragen (M3-2, EPA-Update) instead of a numbered ## Frage 7: phase M3-3's parallel
    plan M3-03-02 appends Fragen 4-6 and hard-codes assertions (grep -c '^## Frage' == 6,
    '^### Frage' == 6) plus edits M3-01-01-PLAN.md's own count gate from 3 to 6. A seventh
    numbered question would require a second cross-phase plan edit and would break M3-03-02's
    assertions on re-run. The Zusatzfragen section uses ### Zusatzfrage A/B sub-headings, which
    match neither ^## Frage nor ^### Frage, so both of M3-3's counters are provably unaffected --
    verified directly: grep -c '^## Frage'/'^### Frage' both stayed at 6 before and after this
    plan's edit, and .planning/phases/M3-01-hc-workbook-ingest/ was never touched."
  - "92 trainable HC games (not a range) is the number quoted for the corpus: unlike RESEARCH
    section 1.4's pre-admission-rule estimate (~199, a projection), M3-02-05 already measured the
    real post-admission-rule number precisely. It is cross-derivable from two independent
    committed CSVs (ablation_summary.csv's n_folds delta 306-214=92 for both EP and WP;
    corpus_arms.csv's with_hc-true-minus-without_hc-true game count 339-247=92), so it is quoted
    as a fact, not a range -- while still stating plainly that most of the raw workbooks'
    ~2,013-2,128 row-fragments remain outside the trainable corpus (docs/hc-workbook-ingest.md),
    mostly locked behind Frage 2/Zusatzfrage B."
  - "The doc-vs-CSV figure guard is scoped to two structured Markdown tables (identified by a
    unique header substring: 'Naive Grundrate' for the ablation table, 'EP Verbesserung' for the
    per-source table), not a whole-document regex scan. This was necessary because the document
    legitimately quotes Phase 1.3's own historical log-loss figures (1,027657 for EP; 0,367263
    for WP) in prose for comparison -- those numbers are correctly absent from this phase's
    data/reference/epa_refinement/*.csv (they are Phase-1.3 numbers), and a whole-document scan
    would have failed the guard on legitimately-cited historical context. Documented in the test
    module's own docstring as a scoping decision, not an omission."
  - "Task 3 (tdd=true) produced a test file that passed on its first run rather than following a
    literal RED-then-GREEN commit sequence: the doc and every CSV it cites already existed from
    Tasks 1-2, so there was no separate 'implementation' to make green -- the test file itself IS
    the task's deliverable. Consistent with the same process note M3-02-04's Task 1 (also
    tdd=true, also first-run-green) already made on this project."

requirements-completed: [HC-03]

# Metrics
duration: ~50min
completed: 2026-09-04
---

# Phase M3-02 Plan 07: German EPA October-sync deliverable Summary

**`docs/epa-refinement-2026-10.md` (259 lines, German) reports the 35->92 trainable HC-game
corpus growth, that EP now beats its naive baseline for the first time in this project, the
with/without-HC ablation, and the HC-vs-model comparison table with n on every cell -- every
quoted figure pinned to a committed CSV by `tests/test_m3_epa_docs.py`.**

## Performance

- **Duration:** ~50 min
- **Started:** 2026-09-04T06:50:00Z (approx.)
- **Completed:** 2026-09-04T07:41:00Z
- **Tasks:** 3
- **Files modified:** 3 (2 created, 1 modified)

## Accomplishments

- `docs/epa-refinement-2026-10.md`: ten sections in German (Der Korpus, Was wir reparieren
  mussten, Methode, Kalibrierung, Was deine Daten beitragen, Dein Ansatz und das Modell
  nebeneinander, Was wir bewusst nicht reproduziert haben, Was noch nicht drin ist, Offene Fragen,
  Reproduzierbarkeit), addressed informally to the head coach in the same register as
  `docs/hc-rueckfragen-2026-09.md`. Leads the calibration section with the unflattering-turned-
  positive result (EP beating naive for the first time) in its opening sentence, shows `n` on
  every comparison-table column, and names all four standing exclusions with what would unlock
  each.
- `docs/hc-rueckfragen-2026-09.md`: appended `## Zusatzfragen (M3-2, EPA-Update)` with
  `### Zusatzfrage A` (half markers) and `### Zusatzfrage B` (competition classification), plus
  matching empty stubs under `## Antworten`. Fragen 1-6, their stubs, and the intro sentence are
  byte-unchanged; `.planning/phases/M3-01-hc-workbook-ingest/M3-01-01-PLAN.md` was never opened
  for writing.
- `tests/test_m3_epa_docs.py`: 8 tests -- bidirectional MLflow run-id agreement against
  `ablation_summary.csv`, precision-tolerant log-loss figure matching for two structured tables
  (the four-arm ablation table and the five-source per-source table) against
  `ablation_summary.csv`/`per_source_metrics_{ep,wp}.csv`, a coverage check that the document
  names every one of the ten CSVs under `data/reference/epa_refinement/`, the roster PII gate, and
  two tests asserting the `docs/hc-rueckfragen-2026-09.md` `Frage`/`Zusatzfrage` heading counts
  stay exactly as M3-3 requires.

## Task Commits

Each task was committed atomically:

1. **Task 1: The German write-up** - `53204c8` (docs)
2. **Task 2: Zusatzfragen A/B, without renumbering** - `3c944b2` (docs)
3. **Task 3: Doc-versus-CSV agreement guard** - `220c160` (test)

**Plan metadata:** this SUMMARY's own commit.

## Files Created/Modified

- `docs/epa-refinement-2026-10.md` (259 lines) - the German October-sync deliverable
- `docs/hc-rueckfragen-2026-09.md` - `## Zusatzfragen (M3-2, EPA-Update)` section + two
  `## Antworten` stubs appended (37 insertions, 0 real deletions -- the one line the diff shows
  as "removed" is a diff-algorithm artifact of inserting content immediately after an unchanged
  heading, verified byte-identical before/after at that line)
- `tests/test_m3_epa_docs.py` (310 lines, 8 tests) - the doc-vs-CSV agreement guard

## Decisions Made

See `key-decisions` in the frontmatter: the `## Zusatzfragen` (not `## Frage 7`) framing and its
cross-phase-safety verification, quoting 92 as a measured fact rather than RESEARCH's earlier
projected range, scoping the figure guard to two structured tables rather than a whole-document
regex scan (so legitimately-cited Phase 1.3 historical figures don't false-positive), and the
Task 3 first-run-green process note.

## Deviations from Plan

None -- plan executed exactly as written. The two structured-table-scoping and precision-
tolerance design choices inside `tests/test_m3_epa_docs.py` were explicit instructions in the
plan's own `<action>`/`<behavior>` blocks (tolerant regex, tolerance from written precision,
"do NOT assert on wording/tone/ordering beyond the section headings"), not unplanned work.

## Issues Encountered

- **`_find_table`'s header-marker matching initially picked the wrong table twice** during Task 3
  test authoring: the marker `"Run-ID"` first matched the small 3-column run-id/model/arm table in
  `## Methode` instead of the intended 8-column ablation table in `## Kalibrierung` (fixed by
  using the more specific marker `"Naive Grundrate"`), and the table-row parser's separator-row
  filter (`re.fullmatch(r"-+", c)`) did not recognise Markdown alignment colons (`---:`), causing
  a spurious `run_id == "---"` lookup failure (fixed by widening the pattern to `r":?-+:?"`). Both
  caught and fixed before the first commit -- `uv run pytest tests/test_m3_epa_docs.py -x -q`
  green on 8/8 after the fixes, no test assertion was weakened to make it pass.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None -- every figure in the document is read from a committed CSV this session; no placeholder
values ship in any committed file.

## Threat Flags

None beyond the plan's own `<threat_model>` register. T-M3-02-28 (figure drift), T-M3-02-29
(cross-phase question-file tampering), T-M3-02-30 (selective reporting), T-M3-02-31 (PII
disclosure) and T-M3-02-32 (unreviewed handout) all have their named mitigations in place and
verified: the doc-vs-CSV guard (8/8 green), the balanced `## Frage`/`### Frage` counter check, the
calibration section's opening-sentence framing, the roster PII gate, and this document's own
framing as "a repository document, not yet a handout" (the objective's explicit note that M3-02-08
owns tone and the decision to send).

## Judgement Calls for the M3-02-08 Reviewer

- **The "ohne HC" arm's own metric (0,957593) differs from the identically-shaped Phase-1.3 report
  figure (1,027657)** despite matching `n_plays`/`n_folds` exactly (16,444/214 for EP). This
  plan's document frames the honest comparison as "mit HC" vs "ohne HC" **within this same run**,
  not against the old document's number, and states that explicitly -- worth reading that
  paragraph in `## Kalibrierung` before quoting either number to the head coach in isolation.
- **Two sources exceed the 2% Timeout/Offsetting-Penalties/Penalty threshold**
  (`hc_workbook:scoring-probability-by-situation-2023-2026:data` at 4,74%, `legacy` at 3,92%,
  from `no_play_rows.csv`) -- named in `## Was noch nicht drin ist` as a finding for this
  checkpoint, not resolved in the document.
- **The `## Zusatzfragen` vs `## Frage 7` framing decision** (see key-decisions) is itself worth a
  second look: it is correct and verified not to break M3-3's assertions today, but if a future
  plan needs a THIRD batch of questions, the pattern (a dedicated `## Zusatzfragen (<phase>, ...)`
  section per contributing phase) should probably become the house style rather than a one-off.
- **The document's "92 trainable HC games" framing supersedes `docs/hc-workbook-ingest.md`'s own
  headline number (35)** -- that document is still the authoritative source for the granular
  per-workbook/per-sheet row counts (Task 1's read_first instruction), but its own top-line
  "35 trainable" sentence is now stale relative to M3-02-05's admission rules. Not fixed in this
  plan (that document is READ-ONLY per this plan's file_collision_guard) -- flagged here so a
  future doc-maintenance plan picks it up rather than two documents silently disagreeing.

## Next Phase Readiness

- M3-02-08 (the human-reviewed checkpoint) has a complete, test-pinned German document ready for
  review, the two Zusatzfragen appended for the head coach, and the judgement-call list above to
  work through before any promotion or send decision.
- No blockers. `git status --porcelain data/ src/ scripts/` and
  `git status --porcelain .planning/phases/M3-01-hc-workbook-ingest/` both empty throughout this
  plan's execution.

## Self-Check

Files (all `[ -f ]` checked):
- `docs/epa-refinement-2026-10.md` -- FOUND
- `tests/test_m3_epa_docs.py` -- FOUND
- `docs/hc-rueckfragen-2026-09.md` -- FOUND (modified)

Commits (`git log --oneline`):
- `53204c8` (Task 1) -- FOUND
- `3c944b2` (Task 2) -- FOUND
- `220c160` (Task 3) -- FOUND

Plan-level verification re-run:
- `uv run pytest tests/test_m3_epa_docs.py tests/test_m3_epa_snapshot.py -q` -- 17 passed
- `grep -c '^## Frage' docs/hc-rueckfragen-2026-09.md` == `grep -c '^### Frage'` -- both 6, both
  unchanged relative to before this plan
- `git status --porcelain .planning/phases/M3-01-hc-workbook-ingest/` -- empty
- `git status --porcelain data/ src/ scripts/` -- empty
- `grep -c 'data/raw' tests/test_m3_epa_docs.py` -- 0

## Self-Check: PASSED

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-04*
