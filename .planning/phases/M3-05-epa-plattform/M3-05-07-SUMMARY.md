---
phase: M3-05-epa-plattform
plan: 07
subsystem: ml-training
tags: [xgboost, polars, extra-point, training-leak, promotion-gate, leave-one-game-out]

# Dependency graph
requires:
  - phase: M3-05-epa-plattform (M3-05-01)
    provides: "the 2026-09-08 with_hc champions (ep 97259da7..., wp 2c8c249d...) this plan's
      candidates are measured against"
  - phase: M3-05-epa-plattform (M3-05-06)
    provides: "model/gate.py::evaluate_gate + `ffep promote`'s gate wiring -- exercised for
      real here for the first time on a genuine methodology-change candidate"
provides:
  - "features/mutations.py: every play_type == 'extra_point' row (successful or failed)
    excluded from EP/WP training, closing the pre-existing leak the 2026-09-08 rerun reported
    but did not fix -- lands in code and applies automatically to the next real retrain"
  - "A fresh, real four-arm LOGO measurement of the fixed methodology
    (data/reference/epa_refinement/2026-09-09/), corpus-fingerprint-identical to 2026-09-08"
  - "The real M3-05-06 gate verdict for both new with_hc candidates: both FAIL (EP on
    calibration, WP on beats_champion) -- owner decision 'none', no alias moved, no --force
    used"
  - "docs/epa-refinement-2026-10.md: dated '## Methodenaenderung: Extrapunkt-Ausschluss'
    section recording the fix, the measured before/after, both gate verdicts and the owner's
    'none' decision with reason"
affects: [M3-05-08-model-card-regeneration]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Read-only gate preview (model.gate.evaluate_gate called directly, not via the ffep
      promote CLI) used to determine in advance whether a live, no---force CLI call would
      refuse or silently promote -- protects the plan's own must-have (no alias move without
      an explicit human decision) from a gate that happens to pass on its own merits"
    - "Dated methodology-change doc section with its own EXTRA_POINT_SECTION_MARKER-scoped
      run-id/figure-match test pair, plus a defensive re-scoping of the prior Nachtrag
      section's own bidirectional check to stop at the new marker -- same section-boundary
      pattern the 2026-09-08 Nachtrag itself established relative to the 2026-09-04 report"

key-files:
  created:
    - data/reference/epa_refinement/2026-09-09/ablation_summary.csv
    - data/reference/epa_refinement/2026-09-09/corpus_arms.csv
    - data/reference/epa_refinement/2026-09-09/no_play_rows.csv
    - data/reference/epa_refinement/2026-09-09/per_source_metrics_ep.csv
    - data/reference/epa_refinement/2026-09-09/per_source_metrics_wp.csv
    - data/reference/epa_refinement/2026-09-09/per_tier_metrics_ep.csv
    - data/reference/epa_refinement/2026-09-09/per_tier_metrics_wp.csv
    - data/reference/corpus_freeze/2026-09-09_ae1f0140.json
  modified:
    - src/flag_football_ep/features/mutations.py
    - tests/test_features_mutations.py
    - docs/epa-refinement-2026-10.md
    - tests/test_m3_epa_docs.py
    - .planning/phases/M3-05-epa-plattform/deferred-items.md

key-decisions:
  - "Verified the gate's real verdict via a read-only model.gate.evaluate_gate() call BEFORE
    running the live `ffep promote --model X --run RUN_ID` (no --force) CLI command the plan's
    own action text specifies -- the plan assumes that call will refuse, but `ffep promote`
    actually executes the promotion immediately if the gate happens to PASS (cli.py: gate
    check only gates the call when it fails; a passing gate proceeds straight to
    registry.promote()). Running the live CLI blind would have risked an accidental,
    unauthorized alias move for a candidate whose gate genuinely passes, violating this plan's
    own must-have ('No alias moves without going through the M3-05-06 gate and an explicit
    human decision'). Both candidates FAILED on the read-only preview, so the live CLI calls
    were then run for real (matching the plan's literal instruction) and confirmed to refuse
    with the exact same verdict, nonzero exit, no promotion."
  - "The fresh re-run used the SAME corpus (fingerprint ae1f0140... identical to 2026-09-08)
    -- only the mutation-layer fix changed the measured numbers, isolating the fix's own
    effect from any corpus drift."
  - "A sibling executor (M3-05-09) committed `ce7abce` (fix(M3-05-09): restrict
    MLFLOW_TRACKING_URI override recognition) WHILE the ablation run was in flight on this
    shared tree -- visible as a git_commit param split between the without_hc arms
    (f470974e..., this plan's fix commit, still HEAD when those arms started) and the with_hc
    arms (ce7abce..., the sibling's commit, HEAD by the time those arms started). Confirmed
    harmless: MLFLOW_TRACKING_URI was never set in this session's environment (verified), so
    the sibling's change had zero behavioral effect on this run, and the sibling's commit never
    touched mutations.py/plays.parquet -- the measured numbers are unaffected. Same class of
    observation M3-05-03's own proof run already documented for this shared tree."
  - "Owner decision (2026-09-09, verbatim): 'none. rauschen passt als begruendung.' -- no
    promotion, no --force, for either candidate. The WP champion-comparison miss (0.00105
    log-loss, a magnitude consistent with the rest of the before/after table) is treated as
    noise; the EP calibration miss (0.316 against a 0.15 threshold, more than double) is taken
    seriously rather than overridden. The leak fix itself stays in the training code
    regardless and applies automatically to the next real retrain -- this decision only
    concerns whether the JUST-measured candidates are promoted immediately, not whether the
    fix is reverted."
  - "docs/epa-refinement-2026-10.md's new dated section does not re-quote the pre-existing
    champion's run ids as literal 32-hex-char strings (same precedent as the
    '## Champion-Entscheidung' section) -- they are not present in
    data/reference/epa_refinement/2026-09-09/ablation_summary.csv (neither candidate was
    promoted), so quoting them would fail the section's own new bidirectional run-id test.
    Referenced by pointer to '## Champion-Entscheidung' above instead; the gate transcripts
    quote every metric number verbatim but omit the specific champion run-id hex for the same
    reason."

requirements-completed: [PROD-08]

# Metrics
duration: ~50min
completed: 2026-09-09
---

# Phase M3-05 Plan 07: Extra-Point Training-Leak Fix Summary

**Closed the pre-existing EP/WP training leak (failed extra-point attempts were never excluded, only successful ones) in `features/mutations.py`, measured it for real with a fresh four-arm LOGO re-run on the identical 2026-09-08 corpus, ran the real M3-05-06 gate against both new `with_hc` candidates (both FAIL), and recorded the owner's "none" promotion decision -- champions stay the 2026-09-08 with_hc runs, the fix itself lands in code for the next real retrain.**

## Performance

- **Duration:** ~50 min
- **Started:** 2026-09-09T07:30:00Z (approx.)
- **Completed:** 2026-09-09T08:20:00Z (approx.)
- **Tasks:** 3/3 complete
- **Files modified:** 5 (2 code/test files, 2 doc/test-guard files, 1 deferred-items note)
- **Files created:** 8 (7 CSVs + 1 freeze manifest)

## Accomplishments

- `make_ep_model_mutations`/`make_wp_model_mutations` (`src/flag_football_ep/features/mutations.py`)
  both now `.filter(pl.col("play_type") != "extra_point")` immediately before their final
  `.select()` -- after label/weight (EP) and `Winner`/label (WP) derivation already ran on the
  full frame, and after `Drive_Score_Dist_W`/`ScoreDiff_W`/`Total_W`/`Total_W_Scaled` were
  computed over the full corpus including extra-point rows (matching the existing
  `DegenerateWeightRange` full-corpus contract). TDD: RED commit `6335a82` (7 new tests, all
  failing against the unfixed code), GREEN commit `f470974` (the two-line fix + docstrings).
- Added `TestMakeEpModelMutationsExtraPointExclusion`/`TestMakeWpModelMutationsExtraPointExclusion`
  (`tests/test_features_mutations.py`): successful-attempt exclusion, failed-attempt exclusion
  (the actual leak this plan closes), non-extra-point-row survival, and a regression guard
  proving the fix's placement matters -- empirically confirmed that pre-filtering extra-point
  rows out of the raw corpus BEFORE `prepare_wp_data` runs (instead of only inside
  `make_wp_model_mutations`'s final `.select()`) shifts `half_seconds_remaining` for every
  OTHER row in the same half (`900.0` vs `800.0` for the second play of a half in the test
  fixture), because `prepare_wp_data`'s synthetic clock is `1200 / max(play_id_half)` -- a
  count that must include every play, extra-point rows included.
- Ran `uv run ffep freeze-corpus` for real (`data/reference/corpus_freeze/2026-09-09_ae1f0140.json`,
  citing `git_commit=f470974e093f2a47aea7c4b066b24d0e80440c70`, the fix commit), then the real
  four-arm `scripts/hc_corpus_ablation.py --model both --out-dir
  data/reference/epa_refinement/2026-09-09` (detached via `nohup`, polled with foreground
  `until` loops, ~26 minutes wall time under heavy CPU contention with a concurrent sibling
  executor on this shared tree -- see Issues Encountered). Corpus fingerprint
  `ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd` is byte-identical to
  2026-09-08's -- the raw corpus did not change; only the mutation-layer fix moved the numbers.
- Verified via a read-only `model.gate.evaluate_gate()` call (no CLI side effect) that both new
  `with_hc` candidates FAIL the M3-05-06 gate before running the live `ffep promote` CLI
  command the plan specifies -- protecting against the scenario where a passing gate would have
  caused `ffep promote` (no `--force`) to promote immediately, with no checkpoint answer yet in
  hand. Confirmed safe, then ran the real CLI calls -- both refused (nonzero exit) with the
  identical verdict, and `registry.resolve_champion` confirmed neither alias moved.
- Added `## Methodenaenderung: Extrapunkt-Ausschluss, Stand 2026-09-09` to
  `docs/epa-refinement-2026-10.md`: the fix in one sentence, affected row counts (measured
  551 EP / 1,117 WP on the `with_hc` arm -- more precise than the earlier 537/1,103 ad hoc
  estimate the 2026-09-08 Nachtrag reported), the full before/after table, both gate verdict
  transcripts, and the owner's "none" decision with reason and the unchanged resolved champion.
- Extended `tests/test_m3_epa_docs.py` with a new run-id/figure-match test pair
  (`test_extra_point_fix_run_ids_match_ablation_summary_bidirectionally`,
  `test_extra_point_fix_table_figures_match_ablation_summary_csv`) scoped to the new section
  and its dated CSV subfolder, mirroring the existing `NACHTRAG_SECTION_MARKER`-scoped pair --
  and re-scoped `_nachtrag_text()` (and the test that used to inline its own duplicate
  scoping) to stop at the new section marker, so the 2026-09-08 Nachtrag's own bidirectional
  check does not spuriously flag this new section's disjoint run ids as "extra". Never loosens
  an existing check -- only re-bounds it and adds a new section-scoped pair, same discipline
  the original Nachtrag addition used relative to the 2026-09-04 report.
- Executed the owner's decision exactly: no `ffep promote` call, no `--force`. Re-verified
  `registry.resolve_champion` afterward -- unchanged.
- Logged a one-line follow-up in `deferred-items.md`: investigate the EP calibration deviation
  at bin level (sparse bin? is 0.15 the right threshold, or should it be n-weighted?) before
  the next real gate exercise.

## Task Commits

1. **Task 1a (RED): failing coverage for the leak fix** - `6335a82` (test) --
   `tests/test_features_mutations.py`
2. **Task 1b (GREEN): the fix** - `f470974` (fix) -- `src/flag_football_ep/features/mutations.py`
3. **Task 1c: fresh four-arm re-run artifacts** - `b1f528e` (feat) --
   `data/reference/epa_refinement/2026-09-09/*.csv`,
   `data/reference/corpus_freeze/2026-09-09_ae1f0140.json`
4. **Partial summary at checkpoint** - `ae9b5f2` (docs) --
   `.planning/phases/M3-05-epa-plattform/M3-05-07-SUMMARY.md`
5. **Task 3a: doc-guard extension** - `a4afd22` (test) -- `tests/test_m3_epa_docs.py`
6. **Task 3b: dated doc section + decision record** - `8d81cc8` (docs) --
   `docs/epa-refinement-2026-10.md`, `.planning/phases/M3-05-epa-plattform/deferred-items.md`

Task 2 was `type="checkpoint:human-verify"` -- no commit of its own; the checkpoint's answer
("none. rauschen passt als begruendung.", 2026-09-09) is recorded in Task 3's doc commit.

## Files Created/Modified

- `src/flag_football_ep/features/mutations.py` -- the two-line filter fix in
  `make_ep_model_mutations`/`make_wp_model_mutations`, plus docstring explaining placement
- `tests/test_features_mutations.py` -- `TestMakeEpModelMutationsExtraPointExclusion` (3
  tests), `TestMakeWpModelMutationsExtraPointExclusion` (4 tests)
- `data/reference/epa_refinement/2026-09-09/*.csv` (7 files) -- the fresh four-arm LOGO
  measurement on the fixed methodology
- `data/reference/corpus_freeze/2026-09-09_ae1f0140.json` -- the dated freeze manifest this
  re-run cites
- `docs/epa-refinement-2026-10.md` -- new dated `## Methodenaenderung: Extrapunkt-Ausschluss,
  Stand 2026-09-09` section
- `tests/test_m3_epa_docs.py` -- new section-scoped run-id/figure-match test pair, re-scoped
  `_nachtrag_text()` helper
- `.planning/phases/M3-05-epa-plattform/deferred-items.md` -- EP calibration bin-level
  follow-up logged

## Before/After: 2026-09-08 (pre-fix, still the live champion basis) vs. 2026-09-09 (post-fix)

| Model | Arm | Log-Loss 08.09. | Log-Loss 09.09. | Naive 09.09. | Verbesserung 08.09. | Verbesserung 09.09. | Delta | n_plays 08.09. -> 09.09. | Run-ID (09.09.) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| EP | ohne HC | 0.952301 | 0.950633 | 0.998108 | 0.049710 | 0.047475 | -0.002235 | 17,730 -> 17,510 (-220) | `c27b3a82c558497fad5cca958742dcac` |
| EP | mit HC | 0.942659 | 0.938953 | 0.989134 | 0.051610 | 0.050181 | -0.001429 | 24,094 -> 23,543 (-551) | `efd9fd3dc457431d917fd6ce59788305` |
| WP | ohne HC | 0.392083 | 0.391614 | 0.690829 | 0.299272 | 0.299214 | -0.000058 | 17,978 -> 17,527 (-451) | `90a9da207ff5439e94d3f8ca4d30d29d` |
| WP | mit HC | 0.372350 | 0.373400 | 0.690405 | 0.319215 | 0.317005 | -0.002210 | 24,705 -> 23,588 (-1,117) | `3b7d571c3f004858b87729ef7b92c30c` |

Both arms/models keep clearing their naive baseline by a wide margin after the fix. EP's
absolute log-loss improved slightly on both arms (fewer noisy/leaked rows). WP's improvement
narrowed slightly on both arms. The row-count drop (EP -551 / WP -1,117 on the `with_hc` arm)
is larger than the earlier ad hoc Nachtrag estimate (537 EP / 1,103 WP); this plan's numbers
are the actual measured row-count delta from two real, back-to-back production runs on the
identical corpus, and are the authoritative figures (both reported in the new doc section).

Corpus fingerprint (both dates): `ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd`.
Fix commit: `f470974e093f2a47aea7c4b066b24d0e80440c70`.

## Gate Verdicts (real, via `uv run ffep promote --model X --run RUN_ID`, no `--force`)

**`ep_model` candidate `efd9fd3dc457431d917fd6ce59788305`: FAIL**
```
ep_model: gate [PASS] beats_naive: logloss_improvement=0.050181 (beats the naive baseline)
ep_model: gate [PASS] beats_champion: candidate logo_mlogloss=0.938953 vs. champion '97259da7acaf43f3b2c65e59f7f11694' logo_mlogloss=0.942659 (epsilon=0.0)
ep_model: gate [FAIL] calibration: exceeds tolerance (0.15): {'calibration_max_deviation_No_Score_Prob': 0.31604071933290234}
ep_model: gate [PASS] no_play_share: no_play_share=0.017840 (threshold 0.02)
ep_model: promotion gate FAILED for run efd9fd3dc457431d917fd6ce59788305 -- refusing to promote. Re-run with --force --reason "..." to override.
```
Exit code 1. Beats both naive and the current champion on raw log-loss, but its
`No_Score_Prob` calibration curve deviates 0.316 from perfect calibration (threshold 0.15,
more than double) -- a real, gate-caught miscalibration, not a marginal near-miss.

**`wp_model` candidate `3b7d571c3f004858b87729ef7b92c30c`: FAIL**
```
wp_model: gate [PASS] beats_naive: logloss_improvement=0.317005 (beats the naive baseline)
wp_model: gate [FAIL] beats_champion: candidate logo_logloss=0.373400 vs. champion '2c8c249d295d4ce9a2845800c459c153' logo_logloss=0.372350 (epsilon=0.0)
wp_model: gate [PASS] calibration: all calibration_max_deviation_* metrics within tolerance (0.15)
wp_model: gate [PASS] no_play_share: no_play_share=0.018102 (threshold 0.02)
wp_model: promotion gate FAILED for run 3b7d571c3f004858b87729ef7b92c30c -- refusing to promote. Re-run with --force --reason "..." to override.
```
Exit code 1. Beats naive comfortably but is marginally worse than the current champion
(log-loss 0.373400 vs. 0.372350, a 0.00105 regression).

## Decision (owner, 2026-09-09)

**"none. rauschen passt als begruendung."** -- no promotion for either candidate, no
`--force`. The WP champion-comparison miss (0.00105) is treated as noise, consistent with the
magnitude of the rest of the before/after table. The EP calibration miss (0.316 against 0.15)
is taken seriously rather than overridden. The leak fix stays in the training code (already
committed, `f470974e093f2a47aea7c4b066b24d0e80440c70`) and applies automatically to the next
real retrain -- this decision only concerns the just-measured candidates, not the fix itself.

**Resolved champion, confirmed unchanged after the decision was executed**
(`registry.resolve_champion`, live tracking store): `ep_model` ->
`97259da7acaf43f3b2c65e59f7f11694` (v5), `wp_model` -> `2c8c249d295d4ce9a2845800c459c153`
(v5) -- identical to before this plan started.

## Decisions Made

See `key-decisions` in frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - missing critical] Read-only gate preview before the live, un-forced `ffep promote` CLI call**
- **Found during:** Task 2
- **Issue:** The plan's action text instructs running `ffep promote --model X --run RUN_ID`
  (no `--force`) "purely to see the gate's printed verdict", explicitly assuming it will
  refuse. But `ffep promote` only gates the call when the gate FAILS -- a passing gate
  proceeds straight to `registry.promote()`. Running the live CLI blind, before the
  checkpoint's answer existed, risked an accidental, unauthorized alias move if either
  candidate's gate happened to pass on its own merits -- directly contradicting this plan's own
  must-have ("No alias moves without going through the M3-05-06 gate and an explicit human
  decision").
- **Fix:** Called `model.gate.evaluate_gate()` directly (read-only, no CLI side effect) first
  to determine the real verdict. Both candidates FAILED on the preview, so the live CLI calls
  were then run for real exactly as the plan specifies, producing the identical verdict with
  zero risk.
- **Files modified:** None (verification-only step, no source change)
- **Verification:** Live CLI calls confirmed to refuse (exit code 1 both times);
  `registry.resolve_champion` confirmed unchanged before and after.
- **Committed in:** n/a (no file change; documented here and in the SUMMARY's key-decisions)

---

**Total deviations:** 1 auto-fixed (missing-critical safety check). **Impact:** Necessary to
honor this plan's own must-have invariant; the plan's literal instruction was still followed to
the letter (the live CLI calls were run, producing the exact printed verdict) -- the preview
only removed the risk of running them blind.

## Issues Encountered

- The real ablation run took ~26 minutes wall time (vs. ~8 minutes total `logo_wall_seconds`
  summed across all four arms in the log) under heavy CPU contention with a concurrent sibling
  executor's own real test/training runs on this shared tree (`ps` showed two other pytest
  processes at 375-403% CPU each throughout). Not a correctness issue -- all four arms
  completed and registered successfully, corpus fingerprint confirms no corpus drift.
- Mid-run, a sibling executor (M3-05-09) committed `ce7abce` on this shared branch, moving HEAD
  between this run's `without_hc` and `with_hc` arms -- visible as a `git_commit` param split
  in `ablation_summary.csv`. Investigated and confirmed harmless (see `key-decisions`); not
  fixed here (not this plan's file to touch), logged for transparency only.
- EP's calibration miss (0.316 vs. 0.15 threshold) is real and not root-caused further here --
  logged as a follow-up in `deferred-items.md` per the owner's checkpoint answer.

## User Setup Required

None -- no external service configuration required.

## Known Stubs

None -- every number in the new document section is read directly from the committed
`data/reference/epa_refinement/2026-09-09/*.csv` files or the real, verbatim `ffep promote`
gate transcripts, never hand-typed. `tests/test_m3_epa_docs.py` enforces this automatically.

## Threat Flags

None beyond the phase's existing threat register (this plan's own T-M3-05-18/19/20 mitigations
apply exactly as designed: the section is explicitly framed as a methodology change, the gate
override path was never exercised since the owner chose not to force, and every number is
test-pinned against the fresh CSV). No new network endpoint, auth path, or schema change.

## Next Phase Readiness

- The extra-point training leak is closed in code for good -- every future `ffep train` run
  (including whatever eventually supersedes the current champion) inherits the fix
  automatically, with no further action needed.
- M3-05-08 (model card regeneration) is unaffected by this plan's outcome: the champion did not
  change, so the existing card (already regenerated for the 2026-09-08 champions in M3-05-01)
  remains accurate. No regeneration is required as a direct consequence of this plan.
- Follow-up flagged, not this plan's scope: `deferred-items.md` now also logs the EP
  calibration bin-level investigation (sparse bin vs. threshold tuning) for whoever next
  revisits the `[promotion_gate]` thresholds or re-exercises the gate on a genuine candidate.
- No blockers. `git status --porcelain data/ src/ scripts/` is clean.

## Self-Check

Files (all `[ -f ]` checked):
- `data/reference/epa_refinement/2026-09-09/ablation_summary.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-09/corpus_arms.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-09/no_play_rows.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-09/per_source_metrics_ep.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-09/per_source_metrics_wp.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-09/per_tier_metrics_ep.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-09/per_tier_metrics_wp.csv` -- FOUND
- `data/reference/corpus_freeze/2026-09-09_ae1f0140.json` -- FOUND
- `src/flag_football_ep/features/mutations.py` -- FOUND (modified)
- `tests/test_features_mutations.py` -- FOUND (modified)
- `docs/epa-refinement-2026-10.md` -- FOUND (modified)
- `tests/test_m3_epa_docs.py` -- FOUND (modified)
- `.planning/phases/M3-05-epa-plattform/deferred-items.md` -- FOUND (modified)

Commits (`git log --oneline`):
- `6335a82` -- FOUND
- `f470974` -- FOUND
- `b1f528e` -- FOUND
- `ae9b5f2` -- FOUND
- `a4afd22` -- FOUND
- `8d81cc8` -- FOUND

Verification re-run:
- `uv run pytest tests/test_features_mutations.py -x -q -k "extra_point"` -- 7 passed
- `uv run pytest tests/test_m3_epa_docs.py tests/test_features_mutations.py -x -q` -- all
  passed (17 doc-guard tests including the 2 new ones, plus the full mutations suite)
- `git status --porcelain data/ src/ scripts/` -- clean
- `registry.resolve_champion` unchanged: `ep_model` -> `97259da7acaf43f3b2c65e59f7f11694`,
  `wp_model` -> `2c8c249d295d4ce9a2845800c459c153`

## Self-Check: PASSED

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-09*
