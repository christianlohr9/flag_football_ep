---
phase: M3-03-explosiveness-efficiency
plan: 03
subsystem: analytics
tags: [explosiveness, efficiency, epa, calibration, german-docs, pii, hc-04, decision-record]

# Dependency graph
requires:
  - phase: M3-03-explosiveness-efficiency (plan 02)
    provides: "docs/explosiveness-vorschlag.md, docs/hc-rueckfragen-2026-09.md Fragen 4-6,
      data/reference/explosiveness/* reference artifacts, scripts/explosiveness_comparison.py"
  - phase: M3-04-player-analysis-report (plan 01)
    provides: "HC_PASS_ATTEMPT_SCOPE denominator correction, hc_efficiency_table primary reading"
provides:
  - "User checkpoint resolution: candidate B (EPA magnitude on successful plays, 80th-percentile
    threshold) adopted 2026-09-04 as the team's explosiveness metric"
  - "docs/explosiveness-entscheidung.md: standalone German decision record (variants considered,
    reasoning, rejections, threshold recalibration/versioning) for the head coach"
  - "Recalibrated data/reference/explosiveness/* on the HC-enlarged corpus (21,907 scrimmage
    plays incl. 6,818 head-coach rows), dated Nachtrag sections in both German docs"
affects: [M3-04-player-analysis-report, hc-sync-2026-10]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Dated 'Nachtrag' addendum pattern (mirrors the established 'Korrektur' pattern from
      M3-04-01): a recalibration event gets its own dated section with an explicit
      before/after table, never a silent rewrite of previously-published numbers"

key-files:
  created:
    - docs/explosiveness-entscheidung.md
  modified:
    - docs/explosiveness-vorschlag.md
    - docs/hc-rueckfragen-2026-09.md
    - docs/hc-sync-2026-10.md
    - docs/coaching-reports.md
    - docs/explosiveness-recherche.md
    - tests/test_m3_hc_pii.py
    - tests/test_m3_explosiveness_docs.py
    - data/reference/explosiveness/calibration.json
    - data/reference/explosiveness/comparison_overall.csv
    - data/reference/explosiveness/comparison_by_player.csv
    - data/reference/explosiveness/cliff_zone.csv

key-decisions:
  - "Candidate B (EPA magnitude on successful plays, IsoPPP-style, 80th-percentile threshold)
    adopted 2026-09-04 as the team's explosiveness metric, per the user's checkpoint response --
    'den explosiveness vorschlag mit der epa und dem perzentil (also b) finde ich super! nehmen
    wir.'"
  - "Fragen 4-6 delivery: not sent separately, go out together with the October handout
    (docs/hc-sync-2026-10.md), which already bundles them -- recorded as a one-line note in
    hc-rueckfragen-2026-09.md's 'Was wir ohne Antwort liefern' section."
  - "Recalibration authorized and run as a dated Nachtrag rather than silently updating the
    already-reviewed document: the definition itself was approved before the recalibration ran,
    per the pending todo's explicit framing ('sinnvollerweise vor der Freigabe... oder als
    expliziter Nachtrag danach') and the user's own checkpoint answer."
  - "docs/hc-sync-2026-10.md and docs/coaching-reports.md received additive-only edits (one link
    each) per explicit scope; their other stale claims (e.g. 'Vorschlag steht noch bei dir zur
    Freigabe', the un-recalibrated 2,69-EPA mention) were left untouched -- flagged below as a
    follow-up for the M3-04-07 document owner, not fixed here."

requirements-completed: [HC-04]

# Metrics
duration: ~55min
completed: 2026-09-04
---

# Phase M3-3 Plan 03: Checkpoint Resolution, Decision Record & Corpus Recalibration Summary

**Closed the M3-03-03 human-verify checkpoint (candidate B adopted as the explosiveness metric),
wrote a standalone German decision record for the head coach, and recalibrated the explosiveness
threshold on the corpus now including his own 6,818 charted rows (2.69 -> 2.66 EPA, both German
docs carry dated Nachtrag sections with full before/after numbers).**

## Performance

- **Duration:** ~55 min
- **Tasks:** 3 (checkpoint review recorded, decision record created, corpus recalibrated) plus
  this summary
- **Files modified/created:** 12

## Accomplishments

- **Checkpoint resolved:** the user approved candidate B (EPA magnitude on successful plays,
  80th-percentile threshold) as the explosiveness metric, raised no wording objections to the
  head-coach-facing passages, and asked for a durable, transparent decision record. Recorded via
  a dated `Stand:` line in `docs/explosiveness-vorschlag.md` and a `### Entscheidung` pointer
  section; the Fragen 4-6 delivery decision (bundled with the October handout, not separate) is
  recorded as a one-line note under `hc-rueckfragen-2026-09.md`'s "Was wir ohne Antwort liefern".
- **`docs/explosiveness-entscheidung.md` created:** a standalone German decision record --
  variants considered (workbook `Yards > 12` formula, the verbal `Yards > 12 OR EPA > 0` rule,
  down-conditioned percentile, EPA magnitude on successes, continuous score), why candidate B was
  adopted (analytical/reproducible/corpus-calibrated/comparable across situations), what was
  rejected and why, and how the threshold is recalibrated and versioned
  (`corpus_fingerprint`/`calibrated_on` in `calibration.json`, never a silent rerun). Linked from
  `docs/explosiveness-vorschlag.md` (Stand line, `### Entscheidung`, Quellen), `docs/hc-sync-2026-10.md`
  (one additive line plus a Quellen entry) and `docs/coaching-reports.md` (one additive sentence).
  No player names -- the head coach is referred to as "HC"/"Head Coach" throughout; the file is
  added to `tests/test_m3_hc_pii.py`'s `_CHECKED_ARTEFACTS`.
- **Corpus recalibration (authorized Nachtrag):** reran
  `uv run python scripts/explosiveness_comparison.py --recalibrate` against the current
  `data/processed/plays_scored.parquet` (28,255 rows total, 6,818 head-coach rows across two
  workbooks -- up from the 21,437-row/0-HC-row corpus the original M3-3 calibration ran on).
  Regenerated all four `data/reference/explosiveness/*` artifacts and wrote dated
  `### Nachtrag 2026-09-04` sections with full before/after tables in both
  `docs/explosiveness-vorschlag.md` and `docs/explosiveness-recherche.md`. Moved
  `.planning/todos/pending/2026-09-04-explosiveness-kalibrierung-mit-hc-korpus.md` to
  `.planning/todos/done/` with a one-line outcome appended.

## Before/After: the recalibration

| Kennzahl | Vorher (2026-09-03, ohne HC-Zeilen) | Nachher (2026-09-04, mit HC-Zeilen) |
|---|---|---|
| Kalibrierungs-Korpus (alle Scrimmage-Plays) | 16.067 | 21.907 |
| ... davon erfolgreich (EPA > 0) | 7.657 | 10.554 |
| Explosiveness-Schwellenwert (80. Perzentile) | 2,69 EPA | 2,66 EPA |
| `corpus_fingerprint` | `f5f11469...b53c834` | `0ebc5fcc...0dad0f8c4` |
| `baseline_hc_workbook` (Yards>12, Pass) | 16,0 % (2.365/14.739) | 15,4 % (3.097/20.138) |
| `baseline_hc_verbal` (Yards>12 oder EPA>0, Pass) | 49,4 % (7.284/14.739) | 49,7 % (10.011/20.138) |
| `success_rate_epa` (alle Scrimmage) | 47,7 % (7.657/16.067) | 48,2 % (10.554/21.907) |
| `explosive_epa_magnitude` (alle Scrimmage) | 9,6 % (1.535/16.067) | 9,6 % (2.112/21.907) |
| Klippen-Zone 10-12 Yards | 10,7 % (1.727 Plays) | 10,5 % (2.300 Plays) |
| `baseline_hc_workbook`-Vergleich: HC-eigene Zeilen enthalten? | nein (0 HC-Zeilen im Korpus) | ja -- 5.399 seiner eigenen Pass-Attempts jetzt im Nenner |
| `Efficiency`-Spalte (`Data!O`) im Korpus | fehlt | vorhanden (`hc_efficiency_table` berechnet erfolgreich; Bedeutung/Frage 5 bleibt offen) |
| `comparison_by_player.csv` Zeilen | 360 | 416 |

The qualitative finding is unchanged: the verbal "or" rule still lands almost exactly on the
plain success rate, and the threshold moved only slightly (2.69 -> 2.66 EPA). What is new: the
comparison numbers now, for the first time, actually include the head coach's own 5,399
pass-attempt rows (previously the corpus had zero of his rows), and the `Efficiency` column
itself is present in the corpus for the first time (its meaning, Frage 5, remains open, so no
Efficiency numbers were added to the proposal document).

## Test result

`uv run pytest tests/test_m3_explosiveness_docs.py tests/test_m3_player_analysis_docs.py tests/test_m3_epa_docs.py tests/test_m3_hc_pii.py tests/test_features_explosiveness.py tests/test_charts_explosiveness.py -q`
-- **79 passed**, no failures, no skips.

## Task Commits

1. **Task 1: Record checkpoint review and Fragen 4-6 delivery decision** - `1a31759` (docs)
2. **Task 2: Add standalone explosiveness decision record** - `d026883` (docs)
3. **Task 3: Recalibrate explosiveness definitions on the HC-enlarged corpus** - `703098e` (feat)
   (a first, incomplete commit `46e0aab` only carried a file rename after a `git add` pathspec
   error aborted mid-stage without partially applying; `703098e` carries the full remaining diff --
   see "Issues Encountered")

## Files Created/Modified

- `docs/explosiveness-entscheidung.md` - new standalone decision record (created).
- `docs/explosiveness-vorschlag.md` - dated `Stand:` review line, `### Entscheidung` pointer,
  `### Nachtrag 2026-09-04` recalibration section, all CSV-backed numbers regenerated.
- `docs/hc-rueckfragen-2026-09.md` - one-line delivery-decision note under "Was wir ohne Antwort
  liefern".
- `docs/hc-sync-2026-10.md` - one additive line plus one additive Quellen entry pointing at
  `explosiveness-entscheidung.md`.
- `docs/coaching-reports.md` - one additive sentence pointing at the decision record.
- `docs/explosiveness-recherche.md` - regenerated Yards-distribution/EPA-side tables, updated
  Kandidat-B threshold reference, new `### Nachtrag 2026-09-04` section, Quellen extended.
- `tests/test_m3_hc_pii.py` - `_CHECKED_ARTEFACTS` extended with the new decision-record doc.
- `tests/test_m3_explosiveness_docs.py` - `_ALLOWED_CALLBACK_PERCENTAGES` updated `"49,4 %"` ->
  `"49,7 %"` (the recalibrated `baseline_hc_verbal` callback rate).
- `data/reference/explosiveness/{calibration.json,comparison_overall.csv,comparison_by_player.csv,cliff_zone.csv}`
  - regenerated via `scripts/explosiveness_comparison.py --recalibrate`.
- `.planning/todos/done/2026-09-04-explosiveness-kalibrierung-mit-hc-korpus.md` - moved from
  `pending/`, one-line outcome appended.

## Decisions Made

See `key-decisions` in the frontmatter above. In short: candidate B adopted per the user's
explicit checkpoint answer; Fragen 4-6 bundled with the October handout, not sent separately; the
recalibration ran as an authorized, dated Nachtrag (not a silent rewrite) per the pending todo's
own framing; `docs/hc-sync-2026-10.md`/`docs/coaching-reports.md` received additive-only edits
per the objective's explicit scope restriction.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added the new decision-record doc to the HC PII gate**
- **Found during:** Task 2
- **Issue:** `docs/explosiveness-entscheidung.md` is a new head-coach-facing, git-versioned
  document; `tests/test_m3_hc_pii.py`'s `_CHECKED_ARTEFACTS` is a fixed allow-list and would not
  have covered it without an explicit addition, leaving a new public artifact unguarded against
  roster-name leakage.
- **Fix:** Added the new file path to `_CHECKED_ARTEFACTS`.
- **Files modified:** `tests/test_m3_hc_pii.py`
- **Verification:** `uv run pytest tests/test_m3_hc_pii.py -q` -- all passed, including the new
  artefact.
- **Committed in:** `d026883` (Task 2 commit)

**2. [Rule 1 - Bug] Fixed a cliff-zone table `n`-column format break introduced mid-edit**
- **Found during:** Task 3
- **Issue:** First recalibration edit wrote the cliff-zone table's `n` column with German
  thousands separators (e.g. `1.198`), but `tests/test_m3_explosiveness_docs.py`'s regex expects
  the raw CSV integer with no separator in that specific column -- broke
  `test_cliff_zone_section_matches_csv`.
  This is scoped entirely inside the document I was actively editing in the same task, not a
  pre-existing/out-of-scope issue.
- **Fix:** Reformatted the `n` column to plain integers (matching the CSV verbatim), consistent
  with the doc's pre-existing convention for that column.
- **Files modified:** `docs/explosiveness-vorschlag.md`
- **Verification:** `uv run pytest tests/test_m3_explosiveness_docs.py -q` -- all 10 passed.
- **Committed in:** `703098e` (Task 3 commit)

---

**Total deviations:** 2 auto-fixed (1 missing critical / PII-gate coverage, 1 bug / table-format
regex mismatch). **Impact:** Both necessary for correctness (PII coverage) and for the doc-guard
gate to pass (table format); no scope creep.

## Issues Encountered

- **Partial commit from a `git add` pathspec error:** the first `git add` invocation for Task 3
  included a stale rename-source path (`.planning/todos/pending/...md`, already moved by
  `git mv`) alongside the real changed files; `git add` failed fatal on the missing pathspec and
  aborted *before* staging any of the other files, but the already-staged rename (from the prior
  `git mv`) was still committed as `46e0aab`, an incomplete commit carrying only the file rename.
  Recovered immediately: re-ran `git add` with only the correct, existing paths and created a
  second, complete commit (`703098e`) carrying the full diff (regenerated CSVs, both German docs,
  the test update). No data was lost; `46e0aab` is a harmless intermediate commit (a file rename
  that is also present, unchanged, in `703098e`'s tree state) rather than a broken one.
- **`docs/hc-sync-2026-10.md` now has a stale sub-claim not fixed here:** the handout's
  "Explosiveness/Efficiency-Vorschlag" paragraph still says "Dieser Vorschlag steht noch bei dir
  zur Freigabe" (now false -- the checkpoint approved it) and still quotes the pre-recalibration
  "aktuell 2,69 EPA" threshold (now 2,66 EPA). Per this plan's explicit scope (`hc-sync-2026-10.md`
  edits limited to "one line ... additive edit only"), these were left untouched rather than
  rewritten. `docs/hc-sync-2026-10.md`'s own "Was heute noch fehlt" bullet about the pending
  recalibration decision is similarly now resolved-but-not-marked-resolved in that file. This is
  M3-04-07's document (owns `hc-sync-2026-10.md`/`test_m3_player_analysis_docs.py`) and should be
  refreshed there, not here -- flagged for a future plan/session, not silently fixed outside this
  plan's declared boundary.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `docs/explosiveness-vorschlag.md` is now dated, approved, and carries the current (recalibrated)
  numbers -- ready to be sent to the head coach as part of the October handout.
- `docs/explosiveness-entscheidung.md` gives the user a durable, standalone artifact to hand to
  the head coach explaining the reasoning behind the metric, independent of the technical
  proposal document.
- Follow-up for a future M3-04-07-owned session: refresh `docs/hc-sync-2026-10.md`'s
  "Vorschlag steht noch bei dir zur Freigabe" and "aktuell 2,69 EPA" mentions to reflect this
  plan's approval and recalibration (not done here, out of this plan's additive-only scope for
  that file).
- Frage 4/5/6 in `docs/hc-rueckfragen-2026-09.md` remain open and unaffected by this plan; the
  recalibration changed corpus size and rates, not any open question's wording.

---
*Phase: M3-03-explosiveness-efficiency*
*Completed: 2026-09-04*

## Self-Check: PASSED

- `docs/explosiveness-entscheidung.md` - FOUND
- `docs/explosiveness-vorschlag.md` - FOUND, contains `Stand:` line and `### Nachtrag 2026-09-04`
- `docs/explosiveness-recherche.md` - FOUND, contains `### Nachtrag 2026-09-04`
- `.planning/todos/done/2026-09-04-explosiveness-kalibrierung-mit-hc-korpus.md` - FOUND
- `.planning/todos/pending/2026-09-04-explosiveness-kalibrierung-mit-hc-korpus.md` - CONFIRMED ABSENT (moved)
- Commit `1a31759` (Task 1) - FOUND in `git log`
- Commit `d026883` (Task 2) - FOUND in `git log`
- Commit `46e0aab` (Task 3, partial/rename-only) - FOUND in `git log`
- Commit `703098e` (Task 3, full diff) - FOUND in `git log`
- `uv run pytest tests/test_m3_explosiveness_docs.py tests/test_m3_player_analysis_docs.py tests/test_m3_epa_docs.py tests/test_m3_hc_pii.py tests/test_features_explosiveness.py tests/test_charts_explosiveness.py -q` - 79 passed
- `git status --porcelain data/processed` - empty (pseudonym key correctly gitignored)
- `git status --short` - clean except the pre-existing, out-of-scope untracked file
  `.planning/phases/02.2-dataset-buildout/deferred-items.md` (owned by the concurrent executor,
  left untouched)
