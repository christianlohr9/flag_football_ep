---
phase: M2-02-ehrliche-baseline
plan: 01
subsystem: cv-tracking-benchmark
tags: [trackers, bytetrack, cbiou, botsort, polars, score-tracks, hackathon]

requires:
  - phase: M2-02-ehrliche-baseline (CONTEXT/RESEARCH)
    provides: locked comparability rules (same frozen detections, same unmodified score_tracks.py, full-61 + n on every rate), verified ByteTrackTracker/CBIoUTracker constructor signatures, CBIoU-not-Deep-EIoU labelling decision
provides:
  - "scripts/hackathon/baseline_common.py: reusable measurement primitives (frozen-detections loader, fps derivation, replay loop with buffer-aging fairness, 13-column tracks writer, score_tracks.py subprocess wrapper, split-aware full-61/dev-43 summarisation, idempotent CSV append) for any future method (plan M2-02-02's GTA measurement reuses it unchanged)"
  - "scripts/hackathon/run_baseline_trackers.py: CLI producing one comparable row per (method, config) -- botsort-existing, bytetrack x{defaults,baseline-matched}, cbiou x{defaults,baseline-matched}"
  - "data/reference/baseline-methods/{summary,per_clip}.csv: 5 measured rows + 305 per-clip rows, the M2-4 continuous-metric input"
affects: [M2-02-02 (GTA measurement, reuses baseline_common.py), M2-02-03 (results table + visual spot-check), M2-4 (continuous metric over per_clip.csv)]

tech-stack:
  added: []
  patterns:
    - "Motion-only tracker replay: iterate frame_index 0..max INCLUSIVE per clip, calling tracker.update() even on detection-free frames, so lost_track_buffer ages identically to the BoT-SORT baseline's per-decoded-frame update() cadence"
    - "Metric-identity separation: automatic continuity rate (comparable across methods) and human pass-rate (BoT-SORT-only, from --review) are never merged into one column; human_pass_k/n is null for every method that received no human review"
    - "Idempotent result-CSV append: existing rows sharing the same (method, config) / (method, config, clip_number) key are replaced via a polars anti-join before concat, so re-running a measurement never duplicates a row"

key-files:
  created:
    - scripts/hackathon/baseline_common.py
    - scripts/hackathon/run_baseline_trackers.py
    - tests/test_m2_baseline_measurement.py
    - data/reference/baseline-methods/summary.csv
    - data/reference/baseline-methods/per_clip.csv
  modified: []

key-decisions:
  - "BoT-SORT is re-scored from the existing tracks parquet, never re-run -- sha256 of data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet verified byte-identical before and after the plan's execution"
  - "human_pass_k/n filled only for botsort-existing (15/61 full, 10/43 dev, both in notes/summary) -- every ByteTrack/CBIoU row's cell is null, with an explicit German note that no human review exists for that method"
  - "Both baseline-matched (BoT-SORT's tuned lost_track_buffer=90/minimum_consecutive_frames=5/IoU=0.1) and library-default configs measured for ByteTrack and CBIoU, so neither an unfairly-tuned nor an unfairly-untuned comparison is the only number recorded"
  - "CBIoU is measured and labelled throughout as trackers.CBIoUTracker, Deep-EIoU's closest permissively-licensed cousin -- never presented as Deep-EIoU itself (RESEARCH.md Open Question 3, resolved)"

requirements-completed: [BASE-01, BASE-03]

duration: 25min
completed: 2026-09-01
---

# Phase M2-02 Plan 01: Motion-Only Baseline Measurement Summary

**Real, reproducible k/n numbers for BoT-SORT (re-scored, not re-run), ByteTrack and CBIoU on the identical frozen 61-clip detections, scored by the unmodified `score_tracks.py`, with the human 15/61 pass-rate attached only to the method it was actually measured on.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-09-01 (session start)
- **Completed:** 2026-09-01T14:38:00Z
- **Tasks:** 2
- **Files created:** 5 (2 scripts, 1 test file, 2 result CSVs)

## Accomplishments

- `scripts/hackathon/baseline_common.py`: shared, method-agnostic measurement primitives (frozen-detections loader with `detector_run_id` assertion, fps derivation with a plausibility gate, the buffer-aging-fair replay loop, the 13-column tracks writer, the `score_tracks.py` subprocess wrapper, full-61/dev-43 split-aware summarisation, idempotent CSV append) -- built once so plan M2-02-02's GTA measurement can reuse it without touching a tracker replay loop again.
- `scripts/hackathon/run_baseline_trackers.py`: one CLI invocation reproduces exactly one result row; five measurements executed this session:

  | Method | Config | Auto (full 61) | Auto (dev 43) | Human pass-rate | Runtime |
  |---|---|---|---|---|---|
  | BoT-SORT (re-scored) | -- | 57/61 (93.44%) | 40/43 (93.02%) | **15/61 (24.59%)** | 0.133s (score only) |
  | ByteTrack | baseline-matched | 57/61 (93.44%) | 40/43 (93.02%) | n/a (no review) | 11.43s |
  | ByteTrack | defaults | 55/61 (90.16%) | 39/43 (90.70%) | n/a (no review) | 10.99s |
  | CBIoU | baseline-matched | 58/61 (95.08%) | 41/43 (95.35%) | n/a (no review) | 16.18s |
  | CBIoU | defaults | 49/61 (80.33%) | 35/43 (81.40%) | n/a (no review) | 15.76s |

  Total wall time for the four tracking runs (ByteTrack + CBIoU, both configs): **54.35s**. No video decode occurred for any of the four (motion-only; verified no `cv2`/`VideoCapture` output).
- The comparability trap named in the plan's objective is defused in the data itself: the human 15/61 appears exactly once (`botsort-existing` row), never copied or implied onto any other row's `human_pass_k`/`human_pass_n` (both `null`/empty for every other row).
- `data/reference/baseline-methods/per_clip.csv` carries 305 rows (5 x 61), each with `n_tracks`, `n_player_tracks`, `longest_track_frac`, `n_fragments`, `auto_flag`, `private_test` -- the M2-4 continuous-metric input, no tracker re-run required.
- On the automatic (not human-validated) continuity metric, the automatic rates for `cbiou/baseline-matched` (58/61) and `bytetrack/baseline-matched` (57/61, tying BoT-SORT) sit at or above BoT-SORT's own 57/61 -- this is reported as-is per the plan's instruction to not editorialise or declare a winner; the automatic metric is known to saturate (BoT-SORT's own auto=93.44% vs human=24.59% in the same row is the documented proof of that saturation) and BASE-04's actual comparison point remains the 15/61=24.59% human reference until a method receives its own human review.

## Task Commits

1. **Task 1: Shared measurement primitives in `baseline_common.py` plus its tests** - `907622b` (feat)
2. **Task 2: Measure BoT-SORT, ByteTrack and CBIoU, write result files** - `a4e8bdb` (feat)

**Plan metadata:** commit pending (this SUMMARY + STATE/ROADMAP are handled by the orchestrator per the execution contract for this plan -- SUMMARY committed separately below).

## Files Created/Modified

- `scripts/hackathon/baseline_common.py` - frozen-detections loader, fps derivation, buffer-aging-fair replay loop, 13-column tracks writer, `score_tracks.py` subprocess wrapper, split-aware summarisation, idempotent CSV append (13 exported symbols)
- `scripts/hackathon/run_baseline_trackers.py` - argparse CLI producing one result row per `(method, config)` invocation; never re-runs BoT-SORT; prints the running comparison table after every invocation
- `tests/test_m2_baseline_measurement.py` - 8 tests: output-schema superset, ByteTrack adapter schema round-trip, empty-frame fairness invariant (via a counting stub tracker), no-video-decode grep guard, fps-plausibility rejection, "summary never invents a human rate" contract, append-results idempotency, `score_tracks.py`-untouched diff-against-HEAD check
- `data/reference/baseline-methods/summary.csv` - 5 rows (one per method/config), every rate carries its denominator, `human_pass_k`/`n` populated only for `botsort-existing`
- `data/reference/baseline-methods/per_clip.csv` - 305 rows, per-clip continuity stats per method for M2-4

## Decisions Made

- BoT-SORT measurement path never constructs a tracker or reads the detections parquet for tracking -- it only re-scores the existing artefact. Verified via `shasum -a 256` before and after the full plan run: `bcaf89e276e54301a9c9908148eda14c56f083c349087ff52f4627a8ba30ee1b` unchanged.
- `run_tracker_over_clip`'s replay loop iterates every `frame_index` from 0 to the clip's max inclusive and calls `tracker.update()` on detection-free frames with `sv.Detections.empty()` -- verified directly by a dedicated test (`test_empty_frames_still_advance_the_tracker`) using a call-counting stub tracker, not just inferred from the ByteTrack/CBIoU output.
- `class_id_codec` builds one bijection from the full detections vocabulary (not per-clip) so `class_id` stays consistent across the 61 fresh-per-clip tracker instances.
- Both `--config defaults` and `--config baseline-matched` were measured for ByteTrack and CBIoU (4 runs total) per the plan's fairness rationale -- comparing a hand-tuned BoT-SORT against library-default competitors only, or a fresh method against its own paper-oriented tuning only, would each be one-sided.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Task 1's own docstring initially broke its own `test_no_video_decode` acceptance check**
- **Found during:** Task 2 (writing `run_baseline_trackers.py`)
- **Issue:** The module docstring explained the no-video-decode guarantee using the literal substring `` `cv2` `` (backtick-quoted, in prose, not an import). `test_no_video_decode` does a plain substring check (`"cv2" not in path.read_text(...)`), so the prose mention alone made the test fail even though no `cv2` import exists anywhere in the file.
- **Fix:** Reworded the docstring to say "no OpenCV import exists" instead of naming the literal package string.
- **Files modified:** `scripts/hackathon/run_baseline_trackers.py`
- **Verification:** `grep -c "cv2" scripts/hackathon/run_baseline_trackers.py` returns 0; `uv run pytest tests/test_m2_baseline_measurement.py -q` passes all 8 tests.
- **Committed in:** `a4e8bdb` (part of Task 2's commit, fixed before commit)

### Noted, Not Fixed (verify-script quirk, not a code bug)

**Task 2's `<verify>` automated one-liner has a latent bug in its null-check.** It does `s.filter(...)['human_pass_k'].cast(str).to_list()` and then asserts every value is in `('', 'null', 'None')`. On the installed `polars` version, casting a null `Int64` to `Utf8` and calling `.to_list()` yields the Python object `None`, not the string `"None"` -- so the literal `<verify>` command as written raises `AssertionError: [None, None, None, None]` even though the underlying data is exactly correct (every non-`botsort-existing` row's `human_pass_k` is null/empty, never `0`, never copied). Re-ran the check with `all(v is None for v in hp)` instead of the string-membership test and confirmed: `OK (corrected null-check) 5 305`. This is a quirk in the plan's verification one-liner's assumption about `cast(str)` on nulls, not a defect in `baseline_common.py`/`run_baseline_trackers.py` or in the committed CSVs -- flagged here rather than silently worked around, per the instruction not to edit `PLAN.md`.

---

**Total deviations:** 1 auto-fixed (Rule 1, cosmetic docstring self-test collision), 1 noted-not-fixed (plan verify-script quirk, data itself correct).
**Impact on plan:** No scope creep; both items are process/tooling nits, not measurement-correctness issues. All `<acceptance_criteria>` for both tasks pass; the corrected form of the plan's own `<verify>` command passes.

## Issues Encountered

None beyond the two items documented above under Deviations.

## User Setup Required

None - no external service configuration required. `trackers==2.6.0` was already installed; zero packages installed this plan (`git diff --name-only pyproject.toml uv.lock` is empty).

## Next Phase Readiness

- Plan M2-02-02 (GTA measurement) can import `scripts/hackathon/baseline_common.py` directly -- `load_frozen_detections`, `score_with_shared_harness`, `private_test_by_clip`, `player_track_counts`, `summarise`, `append_results` are all method-agnostic and already exercised end-to-end by this plan's 5 real measurements.
- Plan M2-02-03 has real numbers to build a results table from (see the Accomplishments table above) and a documented, copy-pasteable `start_command` per row in `summary.csv` for BASE-03.
- BASE-04's "clearly beats 24.6%" test is NOT yet triggered by this plan: no non-BoT-SORT method has received a human review, so no method's *human* pass-rate is known -- only automatic rates, which are documented here as saturated/not directly comparable to the 15/61 reference. This plan deliberately does not declare a winner or recommend a target change; that judgement call is explicitly out of scope per the plan's `<action>` text ("do not declare a winner in this plan").
- No blockers. `data/labels/`, `data/private/`, and `docs/` are untouched; `scripts/hackathon/score_tracks.py`, `pyproject.toml`, `uv.lock` are byte-identical to their pre-plan state.

## Self-Check: PASSED

- `scripts/hackathon/baseline_common.py` exists: FOUND
- `scripts/hackathon/run_baseline_trackers.py` exists: FOUND
- `tests/test_m2_baseline_measurement.py` exists: FOUND
- `data/reference/baseline-methods/summary.csv` exists, 5 data rows: FOUND
- `data/reference/baseline-methods/per_clip.csv` exists, 305 data rows: FOUND
- Commit `907622b` (Task 1) present in `git log --oneline --all`: FOUND
- Commit `a4e8bdb` (Task 2) present in `git log --oneline --all`: FOUND
- `uv run pytest tests/test_m2_baseline_measurement.py tests/test_hackathon_scoring.py -q`: 13 passed
- `git diff --name-only scripts/hackathon/score_tracks.py pyproject.toml uv.lock`: empty
- `shasum -a 256` of the existing BoT-SORT tracks parquet: identical before/after (`bcaf89e276e54301a9c9908148eda14c56f083c349087ff52f4627a8ba30ee1b`)
- `git status --porcelain`: clean (all plan files committed, gitignored intermediates under `data/processed/baseline-methods/` untracked as intended)

---
*Phase: M2-02-ehrliche-baseline*
*Completed: 2026-09-01*
