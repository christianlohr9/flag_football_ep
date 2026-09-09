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
    but did not fix"
  - "A fresh, real four-arm LOGO measurement of the fixed methodology
    (data/reference/epa_refinement/2026-09-09/), corpus-fingerprint-identical to 2026-09-08"
  - "The real M3-05-06 gate verdict for both new with_hc candidates: both FAIL (EP on
    calibration, WP on beats_champion) -- no alias moved, no --force used"
affects: [M3-05-08-model-card-regeneration]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Read-only gate preview (model.gate.evaluate_gate called directly, not via the ffep
      promote CLI) used to determine in advance whether the live, no---force CLI call would
      refuse or silently promote -- protects the plan's own must-have (no alias move without
      an explicit human decision) from a gate that happens to pass on its own merits"

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

requirements-completed: []  # NOT YET -- PROD-08 completes only once Task 3 (doc section +
  # executed promotion decision) lands. This is a PARTIAL summary at the checkpoint.

# Metrics
duration: partial (paused at checkpoint)
completed: 2026-09-09 (Task 1 only; Tasks 2 (checkpoint, answered read-only/informationally
  below)-3 pending the owner's promotion decision)
---

# Phase M3-05 Plan 07: Extra-Point Training-Leak Fix (partial -- paused at checkpoint) Summary

**Closed the pre-existing EP/WP training leak (failed extra-point attempts were never excluded, only successful ones) in `features/mutations.py`, measured it for real with a fresh four-arm LOGO re-run on the identical 2026-09-08 corpus, and ran the real M3-05-06 gate against both new `with_hc` candidates without `--force` -- both FAIL (EP on calibration, WP on beats_champion), no alias moved.**

## Performance

- **Started:** 2026-09-09T07:30:00Z (approx.)
- **Tasks completed:** 1 of 3 (Task 2 is the checkpoint; execution stops here by design, its
  read-only informational content is below)
- **Files modified:** 2 (mutations.py, its test file)
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
  hand. Confirmed safe, then ran the real CLI calls (see Gate Verdicts below) -- both refused
  (nonzero exit) with the identical verdict, and `registry.resolve_champion` confirms neither
  alias moved.

## Task Commits

1. **Task 1a (RED): failing coverage for the leak fix** - `6335a82` (test) --
   `tests/test_features_mutations.py`
2. **Task 1b (GREEN): the fix** - `f470974` (fix) -- `src/flag_football_ep/features/mutations.py`
3. **Task 1c: fresh four-arm re-run artifacts** - `b1f528e` (feat) --
   `data/reference/epa_refinement/2026-09-09/*.csv`,
   `data/reference/corpus_freeze/2026-09-09_ae1f0140.json`

Task 2 is `type="checkpoint:human-verify"` -- execution stops here, no commit. Task 3 (write
the dated document section + execute the promotion decision) is pending the checkpoint
response and will be executed by a continuation agent.

## Files Created/Modified

- `src/flag_football_ep/features/mutations.py` -- the two-line filter fix in
  `make_ep_model_mutations`/`make_wp_model_mutations`, plus docstring explaining placement
- `tests/test_features_mutations.py` -- `TestMakeEpModelMutationsExtraPointExclusion` (3
  tests), `TestMakeWpModelMutationsExtraPointExclusion` (4 tests)
- `data/reference/epa_refinement/2026-09-09/*.csv` (7 files) -- the fresh four-arm LOGO
  measurement on the fixed methodology
- `data/reference/corpus_freeze/2026-09-09_ae1f0140.json` -- the dated freeze manifest this
  re-run cites

## Before/After: 2026-09-08 (pre-fix, still the live champion basis) vs. 2026-09-09 (post-fix)

| Model | Arm | Log-Loss 08.09. | Log-Loss 09.09. | Naive 09.09. | Verbesserung 08.09. | Verbesserung 09.09. | Delta | n_plays 08.09. -> 09.09. | Run-ID (09.09.) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| EP | ohne HC | 0.952301 | 0.950633 | 0.998108 | 0.049710 | 0.047475 | -0.002235 | 17,730 -> 17,510 (-220) | `c27b3a82c558497fad5cca958742dcac` |
| EP | mit HC | 0.942659 | 0.938953 | 0.989134 | 0.051610 | 0.050181 | -0.001429 | 24,094 -> 23,543 (-551) | `efd9fd3dc457431d917fd6ce59788305` |
| WP | ohne HC | 0.392083 | 0.391614 | 0.690829 | 0.299272 | 0.299214 | -0.000058 | 17,978 -> 17,527 (-451) | `90a9da207ff5439e94d3f8ca4d30d29d` |
| WP | mit HC | 0.372350 | 0.373400 | 0.690405 | 0.319215 | 0.317005 | -0.002210 | 24,705 -> 23,588 (-1,117) | `3b7d571c3f004858b87729ef7b92c30c` |

Both arms/models keep clearing their naive baseline by a wide margin after the fix. EP's
absolute log-loss improved slightly on both arms (fewer noisy/leaked rows). WP's improvement
narrowed slightly on both arms (removed rows were, on the whole, easier-than-average for WP to
predict, mechanically -- extra-point rows almost always follow a `Winner`-determining
touchdown, i.e. their opponent-relative label is often the "obvious" one). The row-count drop
(EP -551 / WP -1,117 on the `with_hc` arm) is larger than this plan's own PLAN.md-cited earlier
estimate (537 EP / 1,103 WP) -- that earlier estimate came from the 2026-09-08 Nachtrag's
own ad hoc investigation of the FULL `with_hc` arm's non-null-labeled extra-point count; this
plan's numbers are the actual measured row-count delta from two real, back-to-back production
runs on the identical corpus, and are the authoritative figures.

Corpus fingerprint (both dates): `ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd`.
Fix commit: `f470974e093f2a47aea7c4b066b24d0e80440c70`.

## Gate Verdicts (real, via `uv run ffep promote --model X --run RUN_ID`, no `--force`)

**`ep_model` candidate `efd9fd3dc457431d917fd6ce59788305` (the new `with_hc` run above): FAIL**
```
ep_model: gate [PASS] beats_naive: logloss_improvement=0.050181 (beats the naive baseline)
ep_model: gate [PASS] beats_champion: candidate logo_mlogloss=0.938953 vs. champion '97259da7acaf43f3b2c65e59f7f11694' logo_mlogloss=0.942659 (epsilon=0.0)
ep_model: gate [FAIL] calibration: exceeds tolerance (0.15): {'calibration_max_deviation_No_Score_Prob': 0.31604071933290234}
ep_model: gate [PASS] no_play_share: no_play_share=0.017840 (threshold 0.02)
ep_model: promotion gate FAILED for run efd9fd3dc457431d917fd6ce59788305 -- refusing to promote. Re-run with --force --reason "..." to override.
```
Exit code 1. `ep_model` beats both naive and the current champion on raw log-loss, but its
`No_Score_Prob` calibration curve deviates 0.316 from perfect calibration (threshold 0.15,
more than double) -- a real, gate-caught miscalibration, not a marginal near-miss.

**`wp_model` candidate `3b7d571c3f004858b87729ef7b92c30c` (the new `with_hc` run above): FAIL**
```
wp_model: gate [PASS] beats_naive: logloss_improvement=0.317005 (beats the naive baseline)
wp_model: gate [FAIL] beats_champion: candidate logo_logloss=0.373400 vs. champion '2c8c249d295d4ce9a2845800c459c153' logo_logloss=0.372350 (epsilon=0.0)
wp_model: gate [PASS] calibration: all calibration_max_deviation_* metrics within tolerance (0.15)
wp_model: gate [PASS] no_play_share: no_play_share=0.018102 (threshold 0.02)
wp_model: promotion gate FAILED for run 3b7d571c3f004858b87729ef7b92c30c -- refusing to promote. Re-run with --force --reason "..." to override.
```
Exit code 1. `wp_model` beats naive comfortably but is marginally WORSE than the current
champion (log-loss 0.373400 vs. 0.372350, a 0.00105 regression) -- consistent with the
before/after table above (removing the leaked rows very slightly narrowed WP's own
improvement-over-naive on the `with_hc` arm).

**Confirmed after both calls:** `registry.resolve_champion` for both models still resolves to
the pre-existing 2026-09-08 champions (`ep_model` -> `97259da7acaf43f3b2c65e59f7f11694`,
`wp_model` -> `2c8c249d295d4ce9a2845800c459c153`) -- neither alias moved.

## Decisions Made

See `key-decisions` in frontmatter -- most significantly, the read-only gate preview before
running the live CLI call, to protect the plan's own must-have against a gate that could have
passed and silently promoted.

## Deviations from Plan

None beyond the read-only gate-preview safety step documented in `key-decisions` above (not a
deviation from the plan's outcome -- the live CLI calls were still run exactly as specified,
and produced the exact printed verdict the plan asked to observe; the preview only avoided
running them blind).

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

## User Setup Required

None -- no external service configuration required.

## Next Phase Readiness / Blocked On

**This plan is paused at its mandatory checkpoint (Task 2).** Task 3 (write the dated
`docs/epa-refinement-2026-10.md` section and execute the promotion decision) cannot proceed
without the owner's explicit answer to the question below.

---

## CHECKPOINT REACHED

**Type:** human-verify
**Plan:** M3-05-07
**Progress:** 1/3 tasks complete (Task 1 done and committed; Task 2 is this checkpoint; Task 3
pending the answer below)

### Completed Tasks

| Task | Name | Commit | Files |
| ---- | ---- | ------ | ----- |
| 1a (RED) | Failing coverage for the leak fix | `6335a82` | `tests/test_features_mutations.py` |
| 1b (GREEN) | The fix | `f470974` | `src/flag_football_ep/features/mutations.py` |
| 1c | Fresh four-arm re-run artifacts | `b1f528e` | `data/reference/epa_refinement/2026-09-09/*.csv`, `data/reference/corpus_freeze/2026-09-09_ae1f0140.json` |

### Current Task

**Task 2:** Present the measured before/after and the gate result
**Status:** awaiting decision
**Blocked by:** requires the owner's explicit promotion choice; a gate FAIL is not itself a
"no" -- an override with a written reason is a legitimate answer this checkpoint accepts.

### Checkpoint Details

**What was built:** The extra-point training leak (failed PAT/2-point attempts staying in
EP/WP training) is fixed and committed. A real, fresh four-arm LOGO re-run of the fixed
methodology exists (`data/reference/epa_refinement/2026-09-09/`), on the exact same corpus as
2026-09-08 (fingerprint-identical) so the before/after isolates the fix's own effect. The real
M3-05-06 gate was run against both new `with_hc` candidates (no `--force`) -- see the
before/after table and the two gate transcripts above.

**Summary of the decision to make:**
- **EP candidate** (`efd9fd3dc457431d917fd6ce59788305`): beats naive and beats the current
  champion on raw log-loss (0.938953 vs. 0.942659), but FAILS the gate on calibration
  (`No_Score_Prob` deviates 0.316 from perfect, threshold 0.15).
- **WP candidate** (`3b7d571c3f004858b87729ef7b92c30c`): beats naive comfortably, but is
  marginally WORSE than the current champion on raw log-loss (0.373400 vs. 0.372350) and FAILS
  the gate on beats_champion.
- Both candidates are honest, closed-leak measurements of the SAME corpus the current champion
  was trained on before this fix -- the champion itself was never re-measured with the leak
  closed, so this is not an apples-to-apples "candidate underperforms" story so much as "the
  fix's honest numbers don't clear the gate's bar against a champion that still has the leak in
  it."

**How to verify:** Read the before/after table and both gate transcripts above (already the
exact `ffep promote` output, not a paraphrase).

### Awaiting

**Your promotion choice: `both` / `ep` / `wp` / `none`.**

If you choose to promote a candidate that FAILED its gate check, you must give a reason for
`--force --reason "..."` (M3-05-06's auditable override, tagged on the promoted run). Options,
concretely:

- **`none`** -- leave the 2026-09-08 champions in place; the leak fix lands in code (already
  committed) but does not change what's live. Simplest, and defensible: neither candidate
  clears the gate on its own honest merits.
- **`ep` / `wp` / `both`, un-forced** -- not possible; both candidates FAIL their gate, so an
  un-forced promotion attempt will refuse exactly as shown above. Only meaningful with a
  written override reason.
- **`ep` / `wp` / `both`, forced with a reason** -- e.g. "the leak fix is itself a correctness
  requirement independent of the gate's marginal metric deltas" (EP's calibration miss is real
  but the underlying methodology is more honest; WP's champion-comparison miss is a 0.001
  regression against a champion that still has the leak). This is a real trade-off judgment,
  not a technicality -- your call.

**Resume signal:** Reply with your promotion choice (`both` / `ep` / `wp` / `none`) and, if
overriding a failed gate check, the reason text for `--force --reason`.

---
*Phase: M3-05-epa-plattform*
*Paused at checkpoint: 2026-09-09*
