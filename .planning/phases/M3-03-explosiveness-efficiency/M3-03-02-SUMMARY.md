---
phase: M3-03-explosiveness-efficiency
plan: 02
subsystem: analytics
tags: [explosiveness, efficiency, epa, calibration, matplotlib, german-docs, pii, hc-04]

# Dependency graph
requires:
  - phase: M3-03-explosiveness-efficiency (plan 01)
    provides: "features/explosiveness.py: scrimmage_plays, hc_workbook_explosive_rate,
      hc_verbal_explosive_rate, hc_efficiency_table, calibrate/ExplosivenessCalibration,
      DEFINITIONS/definition_comparison, cliff_zone_table"
provides:
  - "scripts/explosiveness_comparison.py: reproducible run over plays_scored.parquet
    producing calibration.json plus three committed reference CSVs and a gitignored
    pseudonym key"
  - "data/reference/explosiveness/{calibration.json,comparison_overall.csv,
    comparison_by_player.csv,cliff_zone.csv}: the measured, real-corpus numbers frozen
    as reference artifacts"
  - "src/flag_football_ep/charts/explosiveness.py: render_cliff_zone and
    render_definition_comparison, headless Figures for M3-4 to embed"
  - "docs/explosiveness-vorschlag.md: German coach-facing proposal"
  - "docs/hc-rueckfragen-2026-09.md: Fragen 4-6 with answer stubs"
affects: [M3-03-03, M3-4]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Pseudonymise-before-aggregate: the QB/receiver identity column is replaced with a
      deterministic pseudonym (or a shared 'Sonstige (n<X)' bucket value for thin
      samples) BEFORE calling definition_comparison, so the existing n/successes/CI/
      shrunk_rate aggregation handles the small-sample bucket with no separate rollup"
    - "Doc-vs-CSV agreement test (mirrors tests/test_m2_baseline_docs.py): every quoted
      rate, cliff-zone row and calibration threshold in the German doc is re-read from
      the committed CSV/JSON and fails the suite on any drift"

key-files:
  created:
    - scripts/explosiveness_comparison.py
    - src/flag_football_ep/charts/explosiveness.py
    - tests/test_charts_explosiveness.py
    - tests/test_m3_explosiveness_docs.py
    - data/reference/explosiveness/calibration.json
    - data/reference/explosiveness/comparison_overall.csv
    - data/reference/explosiveness/comparison_by_player.csv
    - data/reference/explosiveness/cliff_zone.csv
    - docs/explosiveness-vorschlag.md
  modified:
    - docs/hc-rueckfragen-2026-09.md
    - tests/test_m3_hc_pii.py
    - .planning/phases/M3-01-hc-workbook-ingest/M3-01-01-PLAN.md

key-decisions:
  - "DEFAULT_MIN_ATTEMPTS = 15 for per-player pseudonym eligibility (documented design
    choice in the script, not an external standard): below this, a player is folded
    into a shared 'Sonstige (n<15)' bucket rather than named individually, since a very
    thin-sample player would be identifiable to teammates by elimination even behind a
    pseudonym."
  - "Pseudonym ranking uses the pass-only attempt count (matching the workbook's own
    Attempts denominator), not the full-scope (run+pass) count used by the two
    full-scope definitions -- documented in the script, not silently chosen."
  - "The still-absent efficiency column and the still-absent hc_workbook corpus rows are
    recorded as extra labelled rows in comparison_overall.csv (verbal_only_yards_clause,
    hc_efficiency_status) rather than left in stdout only, so they survive as committed,
    testable findings."
  - "docs/explosiveness-vorschlag.md's cliff-zone table shows a text-bar column (block
    characters) instead of an embedded chart image, since docs/ tracks zero image files
    in this repo; the two chart renderers exist for M3-4's HTML handout to embed the
    same measured data as real Figures."

requirements-completed: [HC-04]

# Metrics
duration: 36min
completed: 2026-09-03
---

# Phase M3-3 Plan 02: Real-Corpus Comparison Run & German Proposal Summary

**Ran the HC-04 metrics module on the real 16,067-play scrimmage corpus, froze a
corpus-fingerprinted 2.69-EPA explosiveness threshold and three reference CSVs
(pseudonymised per-player), built two headless chart renderers, and wrote the German
coach-facing proposal that puts the head coach's own 15.8% (workbook, yards-only) next to
his 48.6% (spoken, yards-or-EPA) rule and shows the 10-12 yard cliff zone as 10.7% of all
plays instead of an assertion.**

## Performance

- **Duration:** ~36 min
- **Started:** 2026-09-03T19:50:00Z
- **Completed:** 2026-09-03T20:26:00Z
- **Tasks:** 3
- **Files modified:** 12

## Accomplishments

- `scripts/explosiveness_comparison.py` runs end to end on `data/processed/plays_scored.parquet`
  (21,437 total rows; 16,067 run/pass scrimmage plays, down 1-4), prints a corpus census
  including two named findings (no `hc_workbook`-sourced rows yet, no `efficiency` column
  yet), calibrates (or loads) the explosiveness threshold, and writes four committed
  reference artifacts plus a gitignored pseudonym key. Reruns without `--recalibrate`
  are byte-identical.
- Measured, real numbers for all four definitions plus two named findings, all with `n`:
  - `baseline_hc_workbook` (workbook `Yards > 12`, pass-only): **15.8%** (2,365/14,991)
  - `baseline_hc_verbal` (spoken `Yards > 12` OR `EPA > 0`, pass-only): **48.6%** (7,290/14,991)
  - `success_rate_epa` (`EPA > 0`, all scrimmage): **47.7%** (7,657/16,067)
  - `explosive_epa_magnitude` (EPA-magnitude on successes, q80 threshold): **9.6%** (1,535/16,067)
  - `verbal_only_yards_clause` (plays triggering the verbal rule ONLY via yards, not EPA):
    **0.6%** (84/14,991) -- confirms the verbal rule is, in practice, almost the same
    thing as the success rate.
  - `hc_efficiency_status`: efficiency column absent from corpus (0 of 14,991 scanned rows).
- Calibration (`data/reference/explosiveness/calibration.json`): `epa_quantile=0.80`,
  `epa_threshold=2.685250945389271`, `corpus_n=16067`, `n_success=7657`,
  `corpus_sources=["ifaf","legacy","legacy-sportapp"]`.
- Cliff zone (`cliff_zone.csv`, window 8-16): the 10-12 yard zone holds **10.7%** of all
  16,067 scrimmage plays (1,727 plays) -- the direct, numeric answer to "was ist, wenn
  eine Spielerin nur 11 Yards erzielt?".
- `comparison_by_player.csv` (360 rows): per-QB and per-receiver rollups across all four
  definitions, pseudonymised as `QB-01`..`QB-37`/`WR-01`..`WR-53` (descending pass-only
  attempt count, SHA-256-of-label tiebreak) plus a shared `Sonstige (n<15)` bucket per
  role for thin samples. The pseudonym-to-source-label key lives only at the gitignored
  `data/processed/m3-03/pseudonym_key.csv`.
- `src/flag_football_ep/charts/explosiveness.py`: `render_cliff_zone` (bar-per-yard,
  dashed rule between 12/13, combined-share annotation on the 10-12 bars) and
  `render_definition_comparison` (bar-per-definition in `DEFINITIONS` order, asymmetric
  Clopper-Pearson error bars, muted rows greyed but never dropped) -- both headless
  (`Agg` selected inside the function), return Figures only, write no files. 16 new
  structure-only tests; `tests/test_charts_fourth_down.py`/`test_charts_tendency.py`
  stay green.
- `docs/explosiveness-vorschlag.md`: all ten required `##` sections, every quoted number
  traceable to a committed CSV/JSON, no player name, cites
  `docs/explosiveness-recherche.md`.
- `docs/hc-rueckfragen-2026-09.md`: Fragen 4-6 appended after Frage 3 (yards-only vs
  verbal formula discrepancy; `Efficiency`/`Data!O` semantics plus the
  `Attempts`-vs-`Attempts+Drops` ambiguity; run-vs-pass scope, with the measured 12.9%
  run rate vs the already-cited 15.8% pass rate) with matching `### Frage N` stubs under
  `## Antworten`. The document now carries **six** questions to the head coach in one
  message instead of two separate rounds.
- Cross-phase gate correction: `.planning/phases/M3-01-hc-workbook-ingest/M3-01-01-PLAN.md`'s
  stale `grep -qx 3` count gate and its two "exactly three" prose lines are now `6`/"six" --
  a single, deliberate, verified value change, nothing else in that file touched.
- `tests/test_m3_hc_pii.py`'s `_CHECKED_ARTEFACTS` extended with the five new artifacts
  this phase introduces; `_MIN_SURNAME_LEN`/`_ALLOWED_TOKENS` untouched.

## Task Commits

1. **Task 1: Comparison run on the real corpus, calibration and measured tables** -
   `c7d3f4d` (feat)
2. **Task 2: Headless chart renderers for the cliff zone and the definition comparison** -
   `24c5f66` (feat)
3. **Task 3: German proposal document, three new head-coach questions, and the doc
   guards** - `643dcd9` (docs)

## Files Created/Modified

- `scripts/explosiveness_comparison.py` (471 lines) - standalone orchestration script;
  computes nothing itself, imports every metric from `features/explosiveness.py`.
- `data/reference/explosiveness/calibration.json` - versioned, corpus-fingerprinted
  explosiveness threshold.
- `data/reference/explosiveness/comparison_overall.csv` - team-level rates for all four
  definitions plus two named findings.
- `data/reference/explosiveness/comparison_by_player.csv` - per-QB/receiver rates,
  pseudonymised.
- `data/reference/explosiveness/cliff_zone.csv` - per-yard counts/shares, window 8-16.
- `src/flag_football_ep/charts/explosiveness.py` (211 lines) - two headless renderers.
- `tests/test_charts_explosiveness.py` (16 tests) - structure-only chart coverage.
- `docs/explosiveness-vorschlag.md` (160 lines) - the German coach-facing proposal.
- `docs/hc-rueckfragen-2026-09.md` - Fragen 4-6 appended, six `### Frage N` stubs.
- `tests/test_m3_explosiveness_docs.py` (9 tests) - doc-vs-CSV/JSON agreement guard.
- `tests/test_m3_hc_pii.py` - `_CHECKED_ARTEFACTS` extended with five new paths.
- `.planning/phases/M3-01-hc-workbook-ingest/M3-01-01-PLAN.md` - one gate value and two
  prose lines corrected (3 -> 6), nothing else changed.

## Decisions Made

- **`DEFAULT_MIN_ATTEMPTS = 15`** for per-player pseudonym eligibility -- a documented
  design choice (not required by the plan's own numbers), chosen because a player named
  individually below this bar could be identified by teammates through elimination even
  behind a pseudonym. Overridable via `--min-attempts`.
- **Pseudonym ranking uses the pass-only attempt count**, matching the workbook's own
  `Attempts` denominator, rather than the broader run+pass count the two full-scope
  definitions (`success_rate_epa`, `explosive_epa_magnitude`) actually use for their own
  rate computation. This keeps the pseudonym numbering stable and interpretable
  (`QB-01` = the QB with the most pass attempts) even though two of the four definitions
  score a wider play set for that same pseudonym.
- **The efficiency-column-absent and hc-workbook-rows-absent findings are written as
  extra rows in `comparison_overall.csv`** (`hc_efficiency_status`,
  `verbal_only_yards_clause`) rather than left as stdout-only prints, so they are
  committed, testable findings a future rerun cannot silently drop. Both rows carry
  their scanned `n` (not a bare `n=0`) so they read as "0 of N", never as "unchecked".

## Deviations from Plan

None - plan executed exactly as written. Two small interpretive choices (both
documented above as `key-decisions`, not silent departures): the `--min-attempts`
default value (the plan left this to discretion) and the exact placement of the two
"named findings" the plan required for `comparison_overall.csv` (as two extra labelled
rows, since the plan's own required overall-CSV column list did not include a slot for
them).

**Total deviations:** 0 auto-fixed. **Impact:** None -- both interpretive choices are
within the plan's explicitly delegated discretion and are documented in the script and
here.

## Issues Encountered

- **Objective/success-criteria mismatch:** this plan's execution objective named
  `tests/test_m3_legal_docs.py` and `tests/test_m3_baseline_docs.py` as files that must
  stay "untouched-but-green". Neither file exists in this repository (`ls tests/
  test_m3_*` shows only `test_m3_epa_snapshot.py`, `test_m3_hc_pii.py`, and the new
  `test_m3_explosiveness_docs.py`). This looks like a naming mix-up with
  `tests/test_m2_baseline_docs.py` (which does exist and was left untouched and green).
  No action was needed since the named files simply do not exist to touch or break.
- **Full suite (`uv run pytest tests -q`):** ran green except for one pre-existing,
  explicitly out-of-scope failure: `tests/test_pipeline_ingest.py::test_run_ingest_hc_failing_game_quarantined_not_warned`.
  This file is named verbatim in this plan's own `<hard_rules>` as "a stale test there
  belongs to M3-2" and is not in this plan's `files_modified`; the failure is in
  `ingest`/HC-workbook quarantine logic unrelated to explosiveness/efficiency metrics.
  Not fixed here per the plan's explicit scope boundary. All of this plan's own scoped
  verification commands (`tests/test_charts_explosiveness.py`,
  `tests/test_m3_explosiveness_docs.py`, `tests/test_m3_hc_pii.py`, plus
  `tests/test_charts_fourth_down.py`/`test_charts_tendency.py`) are fully green.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- M3-03-03 (human review before the October sync) can proceed: `docs/explosiveness-vorschlag.md`
  and the three new `docs/hc-rueckfragen-2026-09.md` questions are ready for review, with
  every number traceable to a committed CSV.
- M3-4 (report rendering) has its exact public API surface named in the proposal's
  "Was das im Report bedeutet" section: `flag_football_ep.features.explosiveness`'s full
  public surface plus `flag_football_ep.charts.explosiveness.render_cliff_zone`/
  `render_definition_comparison`. Wiring these into `reports/own_team.py` is confirmed
  out of scope for this phase.
- Once M3-1 finishes ingesting the head-coach workbook rows and M3-2 rescoring puts them
  into `plays_scored.parquet`, rerunning `scripts/explosiveness_comparison.py --recalibrate`
  will pick up his real rows automatically -- nothing in the script is HC-specific.

---
*Phase: M3-03-explosiveness-efficiency*
*Completed: 2026-09-03*

## Self-Check: PASSED

- `scripts/explosiveness_comparison.py` - FOUND
- `src/flag_football_ep/charts/explosiveness.py` - FOUND
- `data/reference/explosiveness/calibration.json` - FOUND
- `data/reference/explosiveness/comparison_overall.csv` - FOUND
- `data/reference/explosiveness/comparison_by_player.csv` - FOUND
- `data/reference/explosiveness/cliff_zone.csv` - FOUND
- `docs/explosiveness-vorschlag.md` - FOUND
- `tests/test_charts_explosiveness.py` - FOUND
- `tests/test_m3_explosiveness_docs.py` - FOUND
- Commit `c7d3f4d` (Task 1) - FOUND in `git log`
- Commit `24c5f66` (Task 2) - FOUND in `git log`
- Commit `643dcd9` (Task 3) - FOUND in `git log`
- `uv run pytest tests/test_charts_explosiveness.py tests/test_m3_explosiveness_docs.py tests/test_m3_hc_pii.py -q` - 27 passed
- `uv run pytest tests -q` (full suite) - 1 pre-existing failure in `tests/test_pipeline_ingest.py` (named out-of-scope in this plan's `<hard_rules>`), otherwise green
- `git diff --name-only 106033d..HEAD` - exactly the twelve owned paths
- `git status --porcelain data/processed` - empty (pseudonym key correctly gitignored)
