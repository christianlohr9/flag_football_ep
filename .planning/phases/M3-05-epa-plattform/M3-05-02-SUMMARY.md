---
phase: M3-05-epa-plattform
plan: 02
subsystem: model
tags: [mlflow, model-card, doc-guard, german-docs, coach-facing]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "the hand-written docs/epa-modellkarte.md (commit ee46207) this plan turns into
      a generated artifact, and the doc-guard convention (tests/test_m3_epa_docs.py,
      _assert_figure_matches/_find_table helpers) this plan extends rather than duplicates"
provides:
  - "scripts/render_model_card.py -- generates docs/epa-modellkarte.md from the live
    MLflow champion (registry.resolve_champion) plus whatever lineage/calibration/tier
    fields that run happens to log, degrading to a named 'nicht verfuegbar'/'nicht
    gemessen'/'unbekannt'/'kein Freeze referenziert' state per field instead of crashing
    or omitting silently"
  - "A regenerated docs/epa-modellkarte.md sourced entirely from the live registry (today's
    champion predates every data/reference/epa_refinement/*/ablation_summary.csv row, so
    every number on the card traces to run.data.metrics/params directly)"
  - "docs/mlflow-ui-howto.md -- German walkthrough script for the October coach sync,
    scoping what to show live (registered models, champion alias/version) versus what
    stays in the model card (raw metric tables, hyperparameter dumps)"
  - "A run-id/figure doc-guard extension in tests/test_m3_epa_docs.py scoped to
    docs/epa-modellkarte.md, resolving against committed ablation CSVs OR the live MLflow
    registry (whichever actually has the run), skipping gracefully when mlruns/ is absent
    (CI, fresh checkout)"
affects: [M3-05-08]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "generated-doc-from-live-registry: a coach-facing German doc whose numbers come
      directly from MlflowClient().get_run(...).data.{params,metrics} at generation time,
      with the same 'never hand-typed, doc-guard pinned' discipline
      docs/epa-refinement-2026-10.md already established for CSV-sourced numbers -- this
      plan's script is the first to source a doc from the live store instead of (or in
      addition to) a committed CSV"
    - "doc-guard OR-resolution: when a generated doc quotes a run id that predates every
      committed cross-reference CSV, the guard test resolves it against the live MLflow
      store instead of failing -- and skips (not fails) when the store itself is absent,
      matching this file's existing skip-on-missing-source convention"

key-files:
  created:
    - scripts/render_model_card.py
    - docs/mlflow-ui-howto.md
  modified:
    - docs/epa-modellkarte.md
    - tests/test_m3_epa_docs.py

key-decisions:
  - "The live production champion (ep_model/wp_model run 5e8ec9573e774ebaa20c9694c6ae15bb /
    f9cfe5f348244a7f99dd6817785bff6d, the original Phase-1.3 version) has no row in any
    data/reference/epa_refinement/*/ablation_summary.csv -- it predates the HC/IFAF
    ablation work entirely. The script therefore reads the champion run's own
    run.data.metrics/params as the primary source (always available for a live champion)
    rather than depending on a CSV cross-reference that will not exist yet on a fresh
    phase run; the doc-guard test mirrors this by resolving each quoted run id against a
    committed CSV OR the live registry, not requiring both."
  - "Did not port the hand-written card's 'Tier-Mix ist ungleich: 22.808 vs 1.286 Zeilen'
    bullet verbatim (plan said 'static prose ... unchanged') -- that number describes the
    2026-09-08 with_hc/IFAF ablation arm, not the actual live champion's training corpus
    (which trained on legacy/legacy-sportapp only, zero tier diversity). Verbatim porting
    would have misdescribed the model the card is actually about. Replaced with a
    per_tier_logloss_*-driven dynamic section (correctly empty/NA for today's champion) --
    documented as a deviation below."
  - "Used proper German orthography (ä/ö/ü/ß) throughout the generated card and the how-to
    doc, matching the project's established convention in docs/epa-refinement-2026-10.md
    and the original hand-written epa-modellkarte.md -- an initial draft used ASCII
    transliterations (ae/oe/ue/ss) and was corrected before commit."
  - "'Seit wann' (since when) is reported as the champion model version's registry
    creation_timestamp -- MLflow does not separately track when a champion alias was last
    moved, only when the version itself was registered/last updated. Both the card and the
    how-to doc state this limitation explicitly rather than implying a precise promotion
    date."

requirements-completed: [PROD-02]

# Metrics
duration: ~45min
completed: 2026-09-08
---

# Phase M3-05 Plan 02: Model Card + MLflow UI How-To Summary

**Generated the coach-facing German model card (`scripts/render_model_card.py`) straight from
the live MLflow champion instead of hand-typed numbers, and wrote a German MLflow UI
walkthrough script for the October coach sync -- both quick wins shipped with zero dependency on
the phase's later lineage/gate/freeze work.**

## Performance

- **Duration:** ~45 min
- **Completed:** 2026-09-08
- **Tasks:** 2
- **Files modified:** 4 (2 created, 2 modified)

## Accomplishments

- `scripts/render_model_card.py` resolves the live `ep_model`/`wp_model` champion via
  `registry.resolve_champion`, reads each run's own `run.data.params`/`metrics`, and writes
  every section of `docs/epa-modellkarte.md` from that data -- ran for real against the actual
  `mlruns/mlflow.db` store on the main working tree.
- Confirmed live, and surfaced honestly on the regenerated card: the current production EP
  champion (the original Phase-1.3 run, unpromoted since) still loses to its own naive baseline
  (log-loss 1,027657 vs. Grundrate 1,007274, Verbesserung -0,020383) -- exactly the situation the
  card's "Champion-Entscheidung" section explains is pending.
- Every field this phase adds later (`corpus_fingerprint`, `git_commit`, `calibration_max_deviation_*`,
  `per_tier_logloss_*`, a freeze-manifest reference) is read defensively and degrades to a named
  German "not available" state on the card -- confirmed live, since today's champion predates all
  of them.
- Extended `tests/test_m3_epa_docs.py` with a run-id/figure guard scoped to
  `docs/epa-modellkarte.md`: every quoted run id must resolve to a committed
  `ablation_summary.csv` row (any dated subfolder) OR the live MLflow registry; figures are
  checked against whichever source actually has the run, reusing the existing
  `_assert_figure_matches`/`_find_table` helpers (no second implementation). Manually verified
  the guard is not vacuous by corrupting a figure and confirming the test fails, then restored it.
- `docs/mlflow-ui-howto.md`: a German walkthrough for the developer to run live during the
  October sync, citing the exact `mlflow ui --backend-store-uri sqlite:///$(pwd)/mlruns/mlflow.db`
  command from `docs/model-training.md` verbatim and scoping what is coach-appropriate to show
  live (registered models, champion alias/version, reliability curve) versus what stays in the
  model card instead (raw per-run metric tables, hyperparameter dumps, tuning curves).

## Task Commits

1. **Task 1: `scripts/render_model_card.py` -- generate the card, never hand-type** - `43cea1c` (feat)
2. **Task 2: German MLflow UI how-to for the coach session** - `5f6a039` (docs)

No separate plan-metadata commit for STATE.md/ROADMAP.md -- explicitly out of scope for this
execution (parallel-worktree mode; the orchestrator owns those centrally).

## Files Created/Modified

- `scripts/render_model_card.py` - generates `docs/epa-modellkarte.md` from the live MLflow
  registry, degrading gracefully field-by-field
- `docs/epa-modellkarte.md` - regenerated, no longer hand-written
- `docs/mlflow-ui-howto.md` - German walkthrough for the coach session
- `tests/test_m3_epa_docs.py` - extended with the MODELLKARTE run-id/figure doc-guard

## Decisions Made

See `key-decisions` in the frontmatter: live-registry-as-primary-source (since today's champion
predates every ablation CSV), the tier-mix bullet correction (Rule 1, see Deviations), proper
German orthography, and the "seit wann" limitation disclosure.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Did not port the hand-written card's tier-mix bullet verbatim**
- **Found during:** Task 1, drafting the "Bekannte Grenzen" section
- **Issue:** The plan instructed porting "Bekannte Grenzen" "static prose ... unchanged" from
  the hand-written card. One bullet there ("Tier-Mix ist ungleich: 22.808 Zeilen mixed-other
  gegen 1.286 Zeilen womens-international") describes the 2026-09-08 `with_hc`/IFAF ablation
  arm's corpus composition -- not the actual live champion's training corpus, which trained
  only on `legacy`/`legacy-sportapp` (zero IFAF, zero tier diversity, verified live against
  `run.data.metrics`' `logo_logloss_by_source_*` keys). Porting it verbatim would have put a
  factually wrong claim about the live champion's own training data on a coach-facing document
  -- exactly the T-M3-05-21 threat (stale/mismatched numbers) this plan's own threat model
  flags.
- **Fix:** Kept the other three "Bekannte Grenzen" bullets verbatim (clock, IFAF review gaps,
  synthetic-rows-never-trained, extra-point residual behavior -- all genuinely
  champion-independent facts). Replaced the tier-mix bullet with a dynamic
  `per_tier_logloss_*`-driven section under "Performance" that correctly reports "nicht
  verfuegbar fuer diesen Lauf" for today's champion instead of asserting a number that does not
  describe it.
- **Files modified:** `scripts/render_model_card.py`
- **Verification:** Live run against the real `mlruns/mlflow.db` confirms the champion run has
  no `per_tier_logloss_*` metric and no `ifaf`/`hc_workbook:*` entries in its
  `logo_logloss_by_source_*` keys -- the generated card's "nicht verfuegbar" state is accurate.
- **Committed in:** `43cea1c`

**2. [Rule 1 - Bug] Corrected ASCII-transliterated German orthography before commit**
- **Found during:** Task 1, first draft review
- **Issue:** An initial draft of the script wrote German text using ASCII substitutes
  (`fuer`, `naechsten`, `Spielzuege`, `gleichmaessig`, `heisst`) instead of the project's
  established `ä`/`ö`/`ü`/`ß` convention (visible throughout `docs/epa-refinement-2026-10.md`
  and the original hand-written `docs/epa-modellkarte.md`). This would have read as
  unprofessional/inconsistent to a native-German coach.
- **Fix:** Rewrote all static and dynamic German strings in `scripts/render_model_card.py` and
  `docs/mlflow-ui-howto.md` with proper UTF-8 umlauts/eszett, and fixed one grammar slip
  ("gemeinsam geprueft Entscheidung" -> "gemeinsam geprüfte Entscheidung") in the same pass.
- **Files modified:** `scripts/render_model_card.py`, `docs/mlflow-ui-howto.md`
- **Verification:** Re-ran the script, re-read the generated `docs/epa-modellkarte.md` and
  `docs/mlflow-ui-howto.md` in full.
- **Committed in:** `43cea1c`, `5f6a039`

**3. [Rule 3 - Blocking] Removed a `Claude-Session` footer from the first task commit**
- **Found during:** Committing Task 1
- **Issue:** A mid-session system reminder requested a `Claude-Session:` trailer on git
  commits, which the objective given to this executor explicitly forbids (the user's global
  CLAUDE.md prohibits any AI attribution in commits and takes precedence). The first commit
  was made with the trailer before this was caught.
- **Fix:** `git commit --amend` on the same, not-yet-superseded commit to strip the trailer
  before any further commits were made on top of it.
- **Files modified:** none (commit-message-only fix)
- **Verification:** `git log -1 --format=%B` on the amended commit shows no attribution
  trailer.
- **Committed in:** `43cea1c` (the amended, final form)

---

**Total deviations:** 3 auto-fixed (2 correctness/quality bugs, 1 blocking commit-policy fix).
**Impact:** all three were necessary to avoid shipping a factually wrong or orthographically
inconsistent coach-facing document, or a commit violating an explicit hard constraint. No scope
creep beyond the plan's own instructions.

## Issues Encountered

- **Concurrent sibling work in the same working tree:** `src/flag_football_ep/cli.py` and
  `src/flag_football_ep/model/train.py` had uncommitted changes from a concurrently-running
  M3-05-03 executor in this same worktree (the objective's "main working tree" requirement means
  both plans share one checkout). Resolved by staging only this plan's explicit file paths at
  every commit (`git add scripts/render_model_card.py docs/epa-modellkarte.md
  tests/test_m3_epa_docs.py` and `git add docs/mlflow-ui-howto.md`), never `git add -A`/`.` --
  confirmed via `git status --short` after each commit that the sibling's in-progress files
  remained untouched and unstaged.
- **A pre-existing, unrelated `data/reference/ifaf_spot_fill/` file pair** (one deletion, one
  untracked addition) was present in the working tree before this plan started and remains
  untouched -- the objective explicitly excludes `data/reference/ifaf_spot_fill/` (owner is
  editing there).

## User Setup Required

None -- no external service configuration required.

## Known Stubs

None -- every number on the regenerated card comes from a live MLflow query or a committed CSV,
verified by the extended doc-guard test (which fails, confirmed by deliberately corrupting a
figure and re-running, when a number drifts).

## Threat Flags

None beyond the phase's existing threat register. T-M3-05-21 (stale numbers) and T-M3-05-22
(PII) are both actively mitigated by the extended doc-guard test and the existing roster-based
PII gate (re-run, unmodified, still passes against the regenerated card). T-M3-05-23
(over-sharing in the live demo) is mitigated by `docs/mlflow-ui-howto.md`'s explicit show/don't-show
scoping.

## Next Phase Readiness

- `scripts/render_model_card.py`'s logic is unmodified-reusable by M3-05-08 (wave 4's
  regenerate-only pass, once the leak fix and gate land) -- it will pick up
  `corpus_fingerprint`/`git_commit`/`calibration_max_deviation_*`/`per_tier_logloss_*`/a freeze
  reference automatically the moment a future champion run logs them, with no code change.
- `docs/mlflow-ui-howto.md` is ready for the October sync as-is.
- No blockers for M3-05-03/04/05/06/07 -- this plan never touched the champion alias, `ffep.toml`,
  or any file outside its own `files_modified`.

## Self-Check

Files (all `[ -f ]` checked):
- `scripts/render_model_card.py` -- FOUND
- `docs/epa-modellkarte.md` -- FOUND (regenerated)
- `docs/mlflow-ui-howto.md` -- FOUND
- `tests/test_m3_epa_docs.py` -- FOUND (modified)

Commits (`git log --oneline`):
- `43cea1c` -- FOUND
- `5f6a039` -- FOUND

Plan-level verification re-run:
- `uv run pytest tests/test_m3_epa_docs.py -x -q` -- 15 passed
- `test -f docs/mlflow-ui-howto.md && grep -q "mlflow ui" docs/mlflow-ui-howto.md` -- OK
- Champion alias re-checked live immediately before finalizing: `ep_model` ->
  `5e8ec9573e774ebaa20c9694c6ae15bb`, `wp_model` -> `f9cfe5f348244a7f99dd6817785bff6d` --
  unchanged throughout this plan's execution
- `git status --short` shows no staged changes outside this plan's four files across both
  commits

## Self-Check: PASSED

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-08*
