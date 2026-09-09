---
phase: M3-05-epa-plattform
plan: 08
subsystem: model
tags: [mlflow, model-card, doc-guard, german-docs, coach-facing, promotion-gate, mlflow-container-platform]

# Dependency graph
requires:
  - phase: M3-05-epa-plattform (M3-05-02)
    provides: "scripts/render_model_card.py's live-champion-driven rendering and the
      tests/test_m3_epa_docs.py MODELLKARTE doc-guard this plan extends"
  - phase: M3-05-epa-plattform (M3-05-03)
    provides: "corpus_fingerprint/git_commit lineage and the corpus-freeze manifest
      mechanism (data/reference/corpus_freeze/) the card now references"
  - phase: M3-05-epa-plattform (M3-05-06)
    provides: "model/gate.py's four-check promotion gate, exercised for real by M3-05-07 --
      this plan surfaces that verdict on the card"
  - phase: M3-05-epa-plattform (M3-05-07)
    provides: "the '## Methodenaenderung: Extrapunkt-Ausschluss' section in
      docs/epa-refinement-2026-10.md (candidate run ids, gate verdicts, owner 'none'
      decision) this plan's new card section parses"
  - phase: M3-05-epa-plattform (M3-05-09)
    provides: "the containerised MLflow platform (docker-compose.mlflow.yml,
      scripts/migrate_mlflow_store.py, docs/mlflow-container-platform.md) this plan links
      from the card and re-syncs as its closing step"
provides:
  - "docs/epa-modellkarte.md regenerated for real against the live mlruns store: still the
    2026-09-08 with_hc champions (ep 97259da7... v5, wp 2c8c249d... v5, unchanged since
    M3-05-01/07), now carrying a '## Beforderungs-Gate: letzter Stand' section (the
    2026-09-09 with_hc candidates, both FAIL, owner decision 'none'), an explicit
    'trained before the extra-point fix, next retrain applies it automatically'
    methodology note, a corpus-freeze-manifest-location note, and a '## Plattform' section
    linking docs/mlflow-container-platform.md + docs/mlflow-ui-howto.md"
  - "scripts/render_model_card.py: _extra_point_fix_status()/_render_gate_status() parse
    the Methodenaenderung section's candidate verdicts + owner decision from the
    refinement doc, then render the gate table's figures from the committed dated
    ablation_summary.csv -- never hand-typed, degrades to omitting the section entirely
    when the doc has no such section yet"
  - "tests/test_m3_epa_docs.py: a new MODELLKARTE-guard test
    (test_modellkarte_gate_status_run_ids_and_figures_match_extra_point_fix_csv) figure-
    checks the new gate table against EXTRA_POINT_FIX_ABLATION_CSV, reusing
    _assert_figure_matches -- existing MODELLKARTE checks unmodified"
  - "The containerised MLflow store (http://127.0.0.1:5000) re-synced via
    scripts/migrate_mlflow_store.py: now carries the 2026-09-09 with_hc/without_hc
    candidate runs alongside the pre-existing migrated history; champion resolution
    verified to match the default store via the migrated_from_run_id tag for both models"
  - "docs/hc-sync-2026-10.md's Quellen list now also cites docs/mlflow-ui-howto.md
    (additive; the model card was already linked)"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Doc-section-parsing-into-generated-doc: render_model_card.py now reads a dated
      methodology-change section's own bold PASS/FAIL and decision markers out of
      docs/epa-refinement-2026-10.md via targeted regex (mirroring the existing
      _champion_reason()/_current_corpus_context() pattern), then re-derives the actual
      numeric figures from the committed CSV rather than the doc's prose -- two
      independent, both-real sources cross-checked by the doc-guard test, never a single
      hand-typed number"

key-files:
  created: []
  modified:
    - scripts/render_model_card.py
    - docs/epa-modellkarte.md
    - tests/test_m3_epa_docs.py
    - docs/hc-sync-2026-10.md

key-decisions:
  - "The champion did NOT change this run -- M3-05-07's owner decision was 'none', so the
    2026-09-08 with_hc runs (97259da7.../2c8c249d...) remain champion. The regeneration's
    value is entirely in the new sections (gate verdict, methodology note, freeze/platform
    references), not a champion move -- verified by diffing the before/after card, not
    assumed from the plan text."
  - "The new '## Beforderungs-Gate: letzter Stand' section sources its verdict word
    (PASS/FAIL) and the owner's decision word via regex from the refinement doc's already
    doc-guard-tested Methodenaenderung section, but re-derives every numeric figure
    (log-loss/naive/improvement) from the committed dated ablation_summary.csv directly --
    avoids a single point of truth being prose that could drift, and gives the new
    doc-guard test something concrete to figure-check via the existing
    _assert_figure_matches helper, per the plan's explicit instruction."
  - "The section (and the corresponding 'trained before the fix' methodology sentence)
    degrades to being omitted/generic when docs/epa-refinement-2026-10.md has no
    Methodenaenderung section yet (or when a cited candidate run id has no CSV row) --
    matches this script's existing degrade-gracefully discipline for every other
    conditionally-present field."
  - "Re-ran scripts/migrate_mlflow_store.py against the running Docker stack as this
    session's explicitly-directed closing step (not listed in M3-05-08-PLAN.md's own
    tasks -- documented as a deviation below). The script is a one-time, non-idempotent
    full copy (verified by reading its source and tests before running): every source run
    is re-copied on every invocation, so the container's ep_model/wp_model registries went
    from 5/6 versions (M3-05-09's original migration) to 12/14 versions after this re-run
    (the earlier migrated runs were not overwritten, just left in place alongside a fresh
    full copy). This is expected behavior for the tool as designed (no incremental/diff
    mode exists), not a bug introduced here -- champion alias resolution was verified
    correct via the migrated_from_run_id tag both before and after."

requirements-completed: [PROD-09]

# Metrics
duration: ~55min
completed: 2026-09-09
---

# Phase M3-05 Plan 08: Model Card Regeneration + Container Re-sync Summary

**Regenerated `docs/epa-modellkarte.md` for real against the live MLflow store -- the champion stayed the 2026-09-08 `with_hc` runs (owner decision on M3-05-07's candidates was "none"), but the card now surfaces the M3-05-06 gate's first real verdict (both 2026-09-09 candidates FAIL), an explicit "trained before the extra-point fix" methodology note, the corpus-freeze reference, and links to the containerised MLflow platform -- then re-synced the running Docker MLflow store so its mirror carries the 2026-09-09 candidate runs, verified champion resolution matches the default store via the `migrated_from_run_id` tag.**

## Performance

- **Duration:** ~55 min
- **Started:** 2026-09-09T07:36:00Z (approx.)
- **Completed:** 2026-09-09T08:31:37Z
- **Tasks:** 1 (plan) + 1 explicitly-directed closing step (container re-sync)
- **Files modified:** 4 (`scripts/render_model_card.py`, `docs/epa-modellkarte.md`,
  `tests/test_m3_epa_docs.py`, `docs/hc-sync-2026-10.md`)

## Accomplishments

- Extended `scripts/render_model_card.py` with `_extra_point_fix_status()` (parses the
  `## Methodenaenderung: Extrapunkt-Ausschluss` section's candidate PASS/FAIL verdicts and
  owner decision out of `docs/epa-refinement-2026-10.md`) and `_render_gate_status()` (renders
  a `## Beforderungs-Gate: letzter Stand` table whose figures come from the committed
  `data/reference/epa_refinement/2026-09-09/ablation_summary.csv`, globbed generically like
  the doc-guard's own `_all_ablation_rows`, never hard-coded to one date).
- `_method_extra_point_sentence()` now states explicitly, when a champion run predates
  `extra_point_rows_excluded` logging and the Methodenaenderung section exists: "dieser
  Champion-Lauf trainierte vor dem Extrapunkt-Ausschluss-Fix ... jeder kunftige Retrain wendet
  den Fix automatisch an" -- replacing the previous generic "unbekannt fur diesen Lauf".
- `_render_trainingskorpus()` now appends a sentence (only when at least one model has no
  referenced freeze) pointing to `data/reference/corpus_freeze/` and explaining why the
  current champion has none.
- New static `## Plattform` section links `docs/mlflow-container-platform.md` (operator
  runbook) and `docs/mlflow-ui-howto.md` (coach-session walkthrough).
- Ran `uv run python scripts/render_model_card.py` for real against this worktree's actual
  `mlruns/mlflow.db`. Diffed old vs. new: confirmed the champion run ids/version/registration
  dates are byte-identical (no champion move -- matches M3-05-07's "none" decision), and the
  four new/changed sections appear exactly as designed.
- Extended `tests/test_m3_epa_docs.py` with
  `test_modellkarte_gate_status_run_ids_and_figures_match_extra_point_fix_csv`: figure-checks
  the new gate table's run ids/log-loss/naive/improvement against
  `EXTRA_POINT_FIX_ABLATION_CSV`, reusing `_assert_figure_matches`; skips gracefully if the
  card has no such section. All 18 `tests/test_m3_epa_docs.py` tests pass (was 15 before
  M3-05-02, extended by M3-05-02/07/08 -- never loosened, only extended).
- **Closing step (explicitly directed, not in the plan's own tasks):** re-ran
  `scripts/migrate_mlflow_store.py --source sqlite:///.../mlruns/mlflow.db --target
  http://127.0.0.1:5000` against the running Docker stack (`docker compose ... ps` confirmed
  healthy, `curl http://127.0.0.1:5000/health` -> `OK`). The container's `ep_model`/`wp_model`
  registries now carry the 2026-09-09 candidate runs (verified via their
  `migrated_from_run_id` tags: `efd9fd3dc457431d917fd6ce59788305` ->
  `f2e0042a3200485ea65d84560d7c3728`, `3b7d571c3f004858b87729ef7b92c30c` ->
  `714ef2cd2e41499c979e70c75ed725ff`). Champion resolution verified to match: default store
  `ep_model` -> `97259da7acaf43f3b2c65e59f7f11694`, container's resolved champion
  `af3ea5f8ad9f49919c1b8517ea3ffc93` carries `migrated_from_run_id=97259da7...` (and
  analogously for `wp_model`/`2c8c249d...` -> `8edb41d8050f447fa64a7c1b20b14278`). Source
  store unmodified (re-confirmed via `resolve_champion` immediately after the migration run).

## Task Commits

1. **Task 1: extend `render_model_card.py` + `test_m3_epa_docs.py`** - `196ebd5` (feat)
2. **Task 1 (continued): regenerate `docs/epa-modellkarte.md`** - `88a92c5` (docs)
3. **Deviation: link the MLflow UI how-to from `docs/hc-sync-2026-10.md`** - `f4c68ee` (docs)

No separate plan-metadata commit for STATE.md/ROADMAP.md -- per this session's explicit
objective, this executor run does not touch those files.

The container re-sync (`scripts/migrate_mlflow_store.py` run against the live Docker stack)
produced no file changes to commit -- `backups/mlflow/migration_report_2026-09-09.json` and
`.env.mlflow` are both gitignored, confirmed absent from `git status --short` throughout.

## Files Created/Modified

- `scripts/render_model_card.py` - gate-status section, methodology note, freeze note,
  platform section
- `docs/epa-modellkarte.md` - regenerated; champion unchanged, four new/changed sections
- `tests/test_m3_epa_docs.py` - new MODELLKARTE gate-status figure-check test
- `docs/hc-sync-2026-10.md` - additive link to `docs/mlflow-ui-howto.md`

## Decisions Made

See `key-decisions` in frontmatter. Most significant: the card's new gate-verdict section
sources its PASS/FAIL/decision words from the doc's own tested prose but re-derives every
number from the CSV directly, and the container re-sync is a full non-idempotent re-copy by
design (verified from source before running) -- both documented above rather than glossed
over.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical] Card extended beyond a byte-identical re-emit, per this
session's launch objective**
- **Found during:** Task 1
- **Issue:** M3-05-07's own Next Phase Readiness note says "No regeneration is required as a
  direct consequence of this plan" (champion unchanged). Taken literally, a byte-identical
  re-emit would satisfy the plan's own `<done>` text ("unless M3-05-07's decision was
  genuinely `none` and nothing changed") -- but this session's launch objective explicitly
  required the card to surface the gate's first real verdict, the methodology note, the
  freeze reference and the container platform links, none of which the pre-existing script
  produced.
- **Fix:** Extended `scripts/render_model_card.py` with the gate-status/methodology/freeze/
  platform additions described above -- all data-driven (CSV figures, doc-parsed verdicts),
  never hand-typed, matching this plan's own must-have.
- **Files modified:** `scripts/render_model_card.py`, `docs/epa-modellkarte.md`,
  `tests/test_m3_epa_docs.py`
- **Verification:** `uv run pytest tests/test_m3_epa_docs.py -q` -- 18/18 passed, including the
  new figure-check test.
- **Committed in:** `196ebd5`, `88a92c5`

**2. [Rule 3 - Blocking / explicit directive] Container store re-sync**
- **Found during:** after Task 1 completed
- **Issue:** Not a task in `M3-05-08-PLAN.md`, but explicitly required by this session's
  launch objective as "the plan's or phase's closing step", with instruction to document as a
  deviation if the plan does not list it.
- **Fix:** Ran `scripts/migrate_mlflow_store.py` for real against the live Docker stack (see
  Accomplishments). Verified read-only against source, additive against target, champion
  resolution matching via the `migrated_from_run_id` tag.
- **Files modified:** None (live infrastructure action; `backups/mlflow/*` and `.env.mlflow`
  are gitignored)
- **Verification:** `registry.resolve_champion` against both stores, cross-checked via the
  `migrated_from_run_id` tag (see Accomplishments); source store's own `resolve_champion`
  unchanged before/after.
- **Committed in:** n/a (no file change)

**3. [Rule 2 - Missing critical] Linked the MLflow UI how-to from the HC sync doc**
- **Found during:** post-Task-1 review of this session's launch objective
- **Issue:** The objective explicitly asked to link the model card + UI how-to from
  `docs/hc-sync-2026-10.md` "if not already (additive)" -- the model card was already linked,
  the UI how-to was not.
- **Fix:** Added one bullet to the Quellen list citing `docs/mlflow-ui-howto.md`; also dropped
  the now-stale "Stand 2026-09-08" callout on the model-card bullet since the card is
  regenerated, not dated to one snapshot.
- **Files modified:** `docs/hc-sync-2026-10.md`
- **Verification:** `git diff docs/hc-sync-2026-10.md` -- 4-line additive change, no other
  content touched.
- **Committed in:** `f4c68ee`

---

**Total deviations:** 3 (2 missing-critical, 1 explicit out-of-plan directive). **Impact:**
All three were directly instructed by this session's launch objective, not scope creep --
the plan's own task alone (a byte-identical, no-op regeneration) would not have satisfied the
objective's explicit requirements.

## Issues Encountered

- `scripts/migrate_mlflow_store.py` has no incremental/idempotent mode (confirmed by reading
  its source and `tests/test_migrate_mlflow_store.py` before running) -- re-running it
  duplicates every previously-migrated run rather than skipping runs already present. Not a
  bug introduced by this plan (the tool's design, documented in its own docstring as a
  "one-time migration"); flagged here since re-running it as this session directed leaves the
  container's `ep_model`/`wp_model` model-version counts at 12/14 (was 5/6 after M3-05-09's
  original run) rather than a clean incremental diff. Champion resolution correctness is
  unaffected -- verified via the `migrated_from_run_id` tag.

## User Setup Required

None -- no external service configuration required. The Docker MLflow stack was already
running (started by M3-05-09's own session) with a real `.env.mlflow` present; this plan only
issued read/write calls against it.

## Known Stubs

None -- every figure the card's new gate-status section prints is read directly from
`data/reference/epa_refinement/2026-09-09/ablation_summary.csv`; the verdict/decision words
are parsed from `docs/epa-refinement-2026-10.md`'s own doc-guard-tested prose. Both sources
are committed and test-checked.

## Threat Flags

None beyond the phase's existing threat register. T-M3-05-24 (this plan's own threat: stale
numbers after M3-05-07) is directly mitigated -- the card is regenerated from the live
registry and the doc-guard test is extended, never loosened.

## Next Phase Readiness

- `docs/epa-modellkarte.md` is the phase's final, accurate model card: correct champion,
  gate verdict, methodology note, freeze reference and platform links all present and
  test-guarded.
- The containerised MLflow store mirrors the current registry state (including the
  2026-09-09 candidates) and resolves the same champion as the default store.
- Flagged, not this plan's scope: `scripts/migrate_mlflow_store.py`'s lack of an incremental
  mode means any future re-sync will again duplicate prior runs -- a real operational
  consideration for whoever owns the container platform long-term, not a defect to fix here.

## Self-Check

Files (all `[ -f ]` checked):
- `scripts/render_model_card.py` -- FOUND (modified)
- `docs/epa-modellkarte.md` -- FOUND (modified)
- `tests/test_m3_epa_docs.py` -- FOUND (modified)
- `docs/hc-sync-2026-10.md` -- FOUND (modified)

Commits (`git log --oneline`):
- `196ebd5` -- FOUND
- `88a92c5` -- FOUND
- `f4c68ee` -- FOUND

Verification re-run:
- `uv run pytest tests/test_m3_epa_docs.py -v` -- 18 passed
- Champion unchanged and matches `docs/epa-refinement-2026-10.md`'s recorded post-M3-05-07
  decision: `ep_model` -> `97259da7acaf43f3b2c65e59f7f11694` (v5), `wp_model` ->
  `2c8c249d295d4ce9a2845800c459c153` (v5)
- Container champion resolution matches default store via `migrated_from_run_id` tag for
  both models (see Accomplishments)
- `git status --short` -- clean

## Self-Check: PASSED

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-09*
