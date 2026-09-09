---
phase: M3-05-epa-plattform
plan: 01
subsystem: model
tags: [mlflow, model-registry, champion-promotion, epa, wp, model-card]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "The two candidate with_hc/with_ifaf run ids and their measured before/after
      numbers (M3-02-RERUN-2026-09-08-SUMMARY.md), and the promotion semantics
      (docs/model-training.md section 3, ffep promote / registry.resolve_champion)"
  - phase: M3-05-epa-plattform (M3-05-02)
    provides: "scripts/render_model_card.py, the generated-artifact convention for
      docs/epa-modellkarte.md, and its dependency on docs/epa-refinement-2026-10.md's
      'Champion-Entscheidung:' status line as the card's 'Warum dieser Champion' source"
provides:
  - "An explicit, dated, user-made champion-promotion decision ('both') executed via two
    pinned ffep promote calls -- ep_model and wp_model champion aliases now point at the
    2026-09-08 with_hc runs, not a script default"
  - "docs/epa-refinement-2026-10.md's new '## Champion-Entscheidung' section: date, decision,
    one-paragraph reason, exact commands run, and the resolved champion run ids before and
    after promotion"
  - "A freshly regenerated docs/epa-modellkarte.md reflecting the new champion (training
    corpus, performance, versioning sections all now describe run 97259da7... / 2c8c249d...)"
  - "A re-scored corpus (data/processed/plays_scored.parquet, gitignored) under the new
    champion, EPA coverage ~92.6%"
  - "Regenerated CSV exports under data/processed/exports/ (gitignored) reflecting the new
    champion's EP/WP/EPA/WPA values"
affects: [M3-05-06-promotion-gate, M3-05-07-extra-point-fix, M3-05-08-model-card-plan]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Model-card 'Warum dieser Champion' reasoning is sourced live from
      docs/epa-refinement-2026-10.md's single-line 'Champion-Entscheidung:' status line via a
      non-DOTALL regex in render_model_card.py -- that status line must stay on one physical
      line or the card's rendered reason gets truncated mid-sentence"

key-files:
  created:
    - .planning/phases/M3-05-epa-plattform/deferred-items.md
  modified:
    - docs/epa-refinement-2026-10.md
    - docs/epa-modellkarte.md

key-decisions:
  - "Champion promotion decision: 'both' -- promote the 2026-09-08 with_hc runs for both
    ep_model and wp_model (user's verbatim answer: \"natürlich unter den Voraussetzungen
    'both'\", reconfirmed \"both\"). Reason: both with_hc arms beat their without_hc
    counterpart and the naive baseline; the previous live EP champion was itself below naive
    (log-loss 1.027657 vs. naive 1.007274)."
  - "Old champion run ids (5e8ec9573e774ebaa20c9694c6ae15bb / f9cfe5f348244a7f99dd6817785bff6d)
    are deliberately NOT quoted as literal hex strings inside the Nachtrag's new
    Champion-Entscheidung section -- tests/test_m3_epa_docs.py's bidirectional run-id check
    scopes that section strictly to RERUN_ABLATION_CSV's four run ids, and those two ids
    are not in it. Referenced by pointer to M3-02-RERUN-2026-09-08-SUMMARY.md instead."
  - "Regenerated docs/epa-modellkarte.md even though M3-05-01's own PLAN.md text says it is
    NOT touched here -- that guard predates M3-05-02, which turned the card into a generated
    artifact read from the live champion; leaving it stale after this promotion would have
    the card contradict the very decision this plan just recorded (Rule 2: missing-critical
    correctness fix, directed explicitly by this session's launch objective)."

requirements-completed: [PROD-01]

# Metrics
duration: ~30min
completed: 2026-09-09
---

# Phase M3-05 Plan 01: Champion Promotion Decision Summary

**Executed the deferred M3-02-08 champion-promotion decision ("both") via two pinned `ffep promote` calls, moving `ep_model`/`wp_model` to the 2026-09-08 with_hc/with_IFAF runs, recorded the dated decision and reason in `docs/epa-refinement-2026-10.md`, and regenerated `docs/epa-modellkarte.md`, `plays_scored.parquet` and the CSV exports so every downstream artifact matches the new champion.**

## Performance

- **Duration:** ~30 min
- **Tasks:** 2 (checkpoint resolved with the already-provided answer, then executed)
- **Files modified:** 2 doc files, 1 new deferred-items note (all committed); data/processed
  parquet + exports regenerated but gitignored (not committed)

## Accomplishments

- Resolved current champions before promotion: `ep_model` -> `5e8ec9573e774ebaa20c9694c6ae15bb`
  (v1), `wp_model` -> `f9cfe5f348244a7f99dd6817785bff6d` (v1) -- matched the plan's stated
  previous champions exactly.
- Ran `uv run ffep promote --model ep --run 97259da7acaf43f3b2c65e59f7f11694` and
  `uv run ffep promote --model wp --run 2c8c249d295d4ce9a2845800c459c153`. Both succeeded,
  promoting to registered-model version 5 for each.
- Resolved champions after promotion: `ep_model` -> `97259da7acaf43f3b2c65e59f7f11694` (v5),
  `wp_model` -> `2c8c249d295d4ce9a2845800c459c153` (v5) -- confirmed via
  `registry.resolve_champion` against the live tracking store, not just the intended run ids.
- Added `## Champion-Entscheidung` to `docs/epa-refinement-2026-10.md` (date, decision, reason,
  exact commands, resolved champion before/after) and updated the top `Champion-Entscheidung:`
  status line plus both `Stand:` lines (2026-09-04 review line and the 2026-09-08 Nachtrag
  closing line) so no stale "decision still open" claim remains anywhere in the document.
- Re-ran `uv run ffep score` under the new champion: `data/processed/plays_scored.parquet`
  (27,015 rows) now has EPA coverage 92.55% (`epa` non-null share), matching the expected
  ~92% from the corpus's known unknown-field-position nulling fix.
- Regenerated `docs/epa-modellkarte.md` via `uv run python scripts/render_model_card.py` --
  training corpus, performance table, per-source breakdown and versioning section all now
  describe the new champion; doc guard tests stayed green throughout (see below).
- Regenerated CSV exports via the provided `export_pbp_csv.py` script (all gitignored):
  `korpus_alle_quellen_pbp.csv` (27,015 rows), `ifaf_wm2026_pbp.csv` (5,608 rows, EPA share
  0.513 for the women's competition, 0.0 for men's -- men's IFAF plays are not currently
  EP/WP-scored, which is pre-existing and out of this plan's scope), `ifaf_wm2026_video_marks.csv`.

## Task Commits

1. **Task 1 (checkpoint):** Answer was already provided in this session's launch objective
   ("both", verbatim quote recorded above) -- no separate commit, the answer itself is the
   checkpoint's resume-signal artifact.
2. **Task 2: Execute the decision and record it** -- three atomic commits:
   - `13c2eb0` (docs) -- record champion promotion decision in
     `docs/epa-refinement-2026-10.md`
   - `469724d` (docs) -- regenerate `docs/epa-modellkarte.md` for the promoted champions
   - `9964ea9` (docs) -- log the out-of-scope `test_hc_corpus_ablation.py` failure to
     `deferred-items.md`

No separate plan-metadata commit was made -- per this session's explicit objective, STATE.md
and ROADMAP.md are not touched by this executor run.

## Files Created/Modified

- `docs/epa-refinement-2026-10.md` -- new `## Champion-Entscheidung` section (date, decision,
  reason, commands, resolved champion before/after); updated top status line and both `Stand:`
  lines to stop claiming the decision is still open
- `docs/epa-modellkarte.md` -- regenerated from the live MLflow registry via
  `scripts/render_model_card.py`; now describes the 2026-09-08 with_hc champion throughout
- `.planning/phases/M3-05-epa-plattform/deferred-items.md` -- new; logs one out-of-scope test
  failure discovered during verification (see Deviations)

Not committed (gitignored, regenerated for verification/downstream use only):
`data/processed/plays_scored.parquet`, `data/processed/exports/*.csv`

## Decisions Made

- **Champion promotion: "both"** -- both `ep_model` and `wp_model` champion aliases moved to
  the 2026-09-08 with_hc/with_IFAF runs. Reason (recorded verbatim in the document): both
  with_hc arms beat their without_hc counterpart and the naive baseline; the previous live EP
  champion was itself below naive baseline (log-loss 1.027657 vs. naive 1.007274, improvement
  -0.020383). The known WP-on-`ifaf`-alone-without-HC weakness (worse than naive) was
  explicitly considered and noted as not applying to the promoted `with_hc` arm.
- **Old champion run ids kept out of the new Champion-Entscheidung section's literal text** --
  a doc-guard test (`test_rerun_run_ids_match_rerun_ablation_summary_bidirectionally`) scopes
  the Nachtrag section strictly to the four run ids in the 2026-09-08 `ablation_summary.csv`;
  quoting the old champion's hex run ids there would have failed that test. Referenced by
  pointer to `M3-02-RERUN-2026-09-08-SUMMARY.md` instead, where those ids are already
  documented.
- **Regenerated the model card despite the plan text's file_collision_guard saying not to** --
  see key-decisions above; this session's launch objective explicitly directed it, and leaving
  the card stale after promotion would have left it contradicting the newly recorded decision.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed a truncated "Warum dieser Champion" sentence on the model card**
- **Found during:** Task 2, first `render_model_card.py` run
- **Issue:** `render_model_card.py`'s `_champion_reason()` reads
  `docs/epa-refinement-2026-10.md`'s `Champion-Entscheidung:` line with a non-DOTALL,
  single-line regex (`^Champion-Entscheidung:.*$`, `re.MULTILINE`). My first draft of that
  status line wrapped across three physical lines, so the rendered card's "Warum dieser
  Champion" field cut off mid-sentence ("...Champion für EP und WP wurde auf die").
- **Fix:** Collapsed the status line to a single physical line in the source document, then
  re-ran `render_model_card.py`.
- **Files modified:** `docs/epa-refinement-2026-10.md`, `docs/epa-modellkarte.md`
- **Verification:** `grep -n "Warum dieser Champion" docs/epa-modellkarte.md` shows the full,
  un-truncated sentence; `uv run pytest tests/test_m3_epa_docs.py -q` stayed at 15/15 passed.
- **Committed in:** `13c2eb0` / `469724d`

**2. [Rule 2 - Missing critical] Regenerated docs/epa-modellkarte.md for the new champion**
- **Found during:** Task 2, after promoting both champions
- **Issue:** M3-05-01's own PLAN.md text says `docs/epa-modellkarte.md` is deliberately not
  touched by this plan (written before M3-05-02 turned it into a generated artifact). Left
  unregenerated, the card would keep describing the old champion (Phase-1.3 version, log-loss
  below naive) right after this plan recorded the decision to replace it -- a direct
  self-contradiction in coach-facing documentation.
- **Fix:** Ran `uv run python scripts/render_model_card.py` after the promotion and after the
  Champion-Entscheidung status-line edit landed.
- **Files modified:** `docs/epa-modellkarte.md`
- **Verification:** Card's training-corpus, performance and versioning sections all show
  `97259da7acaf43f3b2c65e59f7f11694` (v5) / `2c8c249d295d4ce9a2845800c459c153` (v5);
  `tests/test_modellkarte_run_ids_resolve_to_registry_or_csv` and
  `tests/test_modellkarte_performance_figures_match_live_registry_or_csv` both pass.
- **Committed in:** `469724d`

---

**Total deviations:** 2 auto-fixed (1 bug, 1 missing-critical addition).
**Impact:** Both were necessary for the plan's own stated success criteria ("the resolved
champion for ep_model and wp_model is printed and recorded, not just the intent") to actually
be true and internally consistent across every coach-facing document this session touched. No
scope creep beyond what those criteria already implied.

## Issues Encountered

- **`tests/test_hc_corpus_ablation.py::test_main_both_models_writes_seven_csvs` fails** with
  `mlflow.exceptions.MlflowException: Changing param values is not allowed`, both standalone
  and as part of a broader verification run. This is outside M3-05-01's file ownership
  (`scripts/hc_corpus_ablation.py`, `src/flag_football_ep/model/train.py`, and the test itself
  are all read-only for this plan) and reproduces in a fully isolated fixture unrelated to the
  real `mlruns/` store this plan's `ffep promote`/`ffep score` calls touched. Logged to
  `.planning/phases/M3-05-epa-plattform/deferred-items.md` with a likely-cause hypothesis
  (overlapping `corpus_fingerprint`/`git_commit` param-logging added by M3-02 and M3-05-03 on
  separate code paths) for a future plan to reconcile. Not fixed here.
- **`docs/epa-modellkarte.md`'s "Pro-Tier-Aufschlüsselung"/"Kalibrierung" sections still say
  "nicht verfügbar"/"nicht gemessen"** for the new champion runs, same as before promotion --
  those runs predate M3-05-03's calibration/per-tier-metric logging (they were trained before
  that instrumentation landed), so the card correctly reports what it can measure rather than
  fabricating a figure. Not a defect in this plan's own work; will resolve naturally once a
  training run post-dating M3-05-03 gets promoted.

## User Setup Required

None -- no external service configuration required. `ffep promote`/`ffep score`/
`render_model_card.py`/the export script all ran locally against the existing `mlruns/` store.

## Known Stubs

None -- every figure in the new Champion-Entscheidung section and the regenerated model card
is read directly from the committed `ablation_summary.csv` rows or the live MLflow registry,
not typed by hand.

## Threat Flags

None. This plan's own threat register (T-M3-05-01, T-M3-05-02) covers exactly the surface
touched here -- an explicit, checkpoint-gated promotion with pinned run ids, recorded with
date/reason/resolved-ids. No new network endpoint, auth path, or schema change was introduced.

## Next Phase Readiness

- The champion-promotion decision M3-05-06 (promotion gate), M3-05-07 (extra-point fix
  before/after framing) and M3-05-08 (model card plan) all needed as a precondition is now
  resolved and documented -- `ep_model`/`wp_model` both point at the 2026-09-08 with_hc runs.
- `docs/epa-modellkarte.md` is regenerable end-to-end and currently reflects the live
  champion; any future promotion (e.g. M3-05-07's extra-point-fixed re-run, gated by
  M3-05-06's not-yet-built promotion gate) just needs `ffep promote` + a re-run of
  `scripts/render_model_card.py`, no further manual edits.
- **Blocker for a future plan (not this one):** the `test_hc_corpus_ablation.py` failure in
  `deferred-items.md` should be triaged before the next `hc_corpus_ablation.py` re-run is
  trusted uncritically.

## Self-Check

Files:
- `docs/epa-refinement-2026-10.md` -- FOUND (modified, committed `13c2eb0`)
- `docs/epa-modellkarte.md` -- FOUND (modified, committed `469724d`)
- `.planning/phases/M3-05-epa-plattform/deferred-items.md` -- FOUND (created, committed
  `9964ea9`)

Commits (`git log --oneline`):
- `13c2eb0` -- FOUND
- `469724d` -- FOUND
- `9964ea9` -- FOUND

Verification re-run:
- `uv run pytest tests/test_m3_epa_docs.py -q` -- 15/15 passed
- `uv run pytest tests/test_m3_epa_docs.py tests/test_m3_epa_snapshot.py tests/test_reports_hc_comparison.py -q` -- 48/48 passed
- `git status --porcelain data/ src/ scripts/ tests/` -- empty
- Champion resolves as recorded: `ep_model` -> `97259da7acaf43f3b2c65e59f7f11694` (v5),
  `wp_model` -> `2c8c249d295d4ce9a2845800c459c153` (v5)
- EPA coverage post-rescore: 92.55% (`epa` non-null share on 27,015-row corpus)

## Self-Check: PASSED

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-09*
