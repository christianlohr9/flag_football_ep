---
phase: M3-05-epa-plattform
plan: 03
subsystem: ml-training
tags: [mlflow, xgboost, polars, lineage, calibration, promotion-gate-prereq]

requires:
  - phase: M3-02-epa-refinement
    provides: "scripts/hc_corpus_ablation.py's proven corpus_fingerprint/git_commit hashing scheme, per-tier metrics, no-play token matching -- lifted into production here"
provides:
  - "model/freeze.py: build_freeze_manifest/write_freeze_manifest/load_freeze_manifest/latest_freeze_manifest, plus ffep freeze-corpus"
  - "Every ffep train run logs git_commit and corpus_fingerprint params (previously only the ablation script's runs did)"
  - "ffep train --freeze <path> citation, with automatic latest-manifest resolution when omitted"
  - "evaluate.calibration_max_deviation(curves) -> dict[str, float] -- the first scalar calibration metric (previously PNG-only)"
  - "Every ffep train run logs calibration_max_deviation_<class>, per_tier_logloss_<tier> (+naive/improvement), and no_play_share metrics"
affects: [M3-05-06-promotion-gate]

tech-stack:
  added: []
  patterns:
    - "Copy hashing/git-lookup helpers into each consumer (model/freeze.py, model/train.py) rather than importing scripts/ as a library dependency"
    - "Guarded no-op for optional derived metrics (tier_*/result_raw absence) matching the existing half-sentinel discipline in features/mutations.py"
    - "Paths.<new-field> optional-with-default dataclass fields so a pre-existing ffep.toml keeps loading (Paths.corpus_freeze)"

key-files:
  created:
    - src/flag_football_ep/model/freeze.py
    - tests/test_model_freeze.py
    - data/reference/corpus_freeze/2026-09-08_ae1f0140.json
  modified:
    - src/flag_football_ep/model/train.py
    - src/flag_football_ep/model/evaluate.py
    - src/flag_football_ep/cli.py
    - src/flag_football_ep/config.py
    - tests/test_model_train.py
    - tests/test_model_evaluate.py

key-decisions:
  - "no_play_share is resolved by joining model_data's own (game_id, play_id, source) keys back onto the pre-mutate `filtered` frame, since result_raw is not one of EP_TRAINING_COLUMNS/WP_TRAINING_COLUMNS and mutate_fn's final .select() drops it -- computing it directly on model_data (as the plan's literal action text suggested) would have made the metric a permanent no-op in production"
  - "corpus_fingerprint/git_commit and the calibration/no-play helper functions are copied verbatim into both model/freeze.py and model/train.py rather than imported from scripts/hc_corpus_ablation.py, matching the plan's explicit instruction that scripts/ is a driver, not a src/ library dependency"
  - "Task 2 and Task 3's train.py/evaluate.py edits were committed separately (not as one combined diff) by temporarily reverting Task 3's pieces before the Task 2 commit, then reapplying -- keeps each task's commit buildable and testable in isolation despite both touching the same _train function"

requirements-completed: [PROD-03, PROD-04]

duration: ~90min (across several tool-interruption resumes on a shared multi-agent tree)
completed: 2026-09-09
---

# Phase M3-05 Plan 03: Corpus Freeze Manifest and Training-Run Lineage Summary

**`ffep freeze-corpus` writes a dated, fingerprinted JSON manifest from `plays.parquet`, and every `ffep train` run now logs `git_commit`/`corpus_fingerprint` lineage, a `calibration_max_deviation_<class>` scalar, `per_tier_logloss_<tier>` (+naive/improvement), and `no_play_share` -- all previously only computed by the one-off `scripts/hc_corpus_ablation.py`.**

## Performance

- **Duration:** ~90 min effective work (several resumes across a shared, concurrently-edited working tree)
- **Tasks:** 3/3 complete
- **Files modified:** 6 (+3 created)

## Accomplishments

- `model/freeze.py` (new): `build_freeze_manifest`/`write_freeze_manifest`/`load_freeze_manifest`/`latest_freeze_manifest`, reusing `scripts/hc_corpus_ablation.py`'s proven `(game_id, play_id, source)` SHA-256 fingerprint and `git rev-parse HEAD` lineage capture, copied in rather than imported.
- `ffep freeze-corpus` (new CLI command): writes `data/reference/corpus_freeze/<date>_<fingerprint8>.json`, echoes fingerprint/per-source counts/accepted-quarantined game counts. Run for real against this project's actual corpus; the resulting manifest is committed.
- `Paths.corpus_freeze` added to `config.py` as an optional field with a default (same "pre-existing-`ffep.toml`-keeps-loading" convention as `Paths.raw_hc_files`).
- `_train` (shared EP/WP fit-and-log path in `model/train.py`) now logs `git_commit`/`corpus_fingerprint` on every run -- MLflow's automatic git-tag detection never fires for the installed `ffep` console-script entry point, so this was previously a genuine gap for every production run.
- `train_ep`/`train_wp`/`_train` accept an optional `freeze_manifest: Path | None = None`; when given, the run logs which freeze it was trained against and whether that freeze's fingerprint still matches the corpus this run actually saw (`freeze_fingerprint_matches_corpus`). `ffep train --freeze <path>` passes it through; with no flag, the CLI resolves the latest manifest under `data/reference/corpus_freeze` automatically. `freeze_manifest=None` (the default) changes nothing about what a run logs.
- `evaluate.calibration_max_deviation(curves)` -- new function, the first scalar calibration metric this project has (previously only a reliability-curve PNG existed).
- `_train` logs `calibration_max_deviation_<class>` (from the same `curves` object already built for the reliability figure), `per_tier_logloss_<tier>`/`per_tier_naive_logloss_<tier>`/`per_tier_improvement_<tier>` (derived from `model_data`'s existing `tier_*` one-hot columns, reusing `per_source_metrics` generically), and a single `no_play_share` metric -- all guarded no-ops when their required columns are absent, so a narrower frame from a future caller cannot crash a run.
- Zero change to any existing measured number: every pre-existing `test_model_train.py`/`test_model_evaluate.py` assertion passes unmodified (see Self-Check).

## Task Commits

1. **Task 1: `model/freeze.py` -- manifest schema, build/write/load, `ffep freeze-corpus`** - `1085c91` (feat) -- `model/freeze.py`, `config.py` (`Paths.corpus_freeze`), `cli.py` (`freeze-corpus` command), `tests/test_model_freeze.py`, `data/reference/corpus_freeze/2026-09-08_ae1f0140.json`
2. **Task 2: Git commit + corpus fingerprint lineage, plus freeze citation** - `b09094a` (feat) -- `model/train.py` (lineage capture + `freeze_manifest` kwarg), `cli.py` (`--freeze` option on `train`), `tests/test_model_train.py` (9 new tests)
3. **Task 3: Calibration scalar, per-tier log-loss and no-play share** - `d93dbe3` (feat) -- `model/evaluate.py` (`calibration_max_deviation`), `model/train.py` (metric wiring), `tests/test_model_evaluate.py` (5 new tests), `tests/test_model_train.py` (7 new tests)

No separate plan-metadata commit was made for STATE.md/ROADMAP.md -- the orchestrator's instructions for this run explicitly excluded STATE/ROADMAP edits (parallel-execution mode; those files are synced centrally elsewhere).

_Note: Task 2 and Task 3 both touch `_train`'s shared `params`/`metrics` construction in `model/train.py`. To keep each task's commit independently buildable/testable, Task 3's pieces (the `calibration_max_deviation`/`TIER_FEATURE_COLUMNS` imports, `_NO_PLAY_TOKENS` constant, and the three new metric blocks) were temporarily removed before the Task 2 commit, then reapplied and re-tested before the Task 3 commit -- both commits pass their own full test suite in isolation._

## Files Created/Modified

- `src/flag_football_ep/model/freeze.py` - corpus freeze manifest build/write/load/latest
- `src/flag_football_ep/model/train.py` - lineage params, freeze citation, calibration/tier/no-play metrics on every `_train` run
- `src/flag_football_ep/model/evaluate.py` - `calibration_max_deviation(curves)`
- `src/flag_football_ep/cli.py` - `ffep freeze-corpus` command, `ffep train --freeze` option
- `src/flag_football_ep/config.py` - `Paths.corpus_freeze` (optional, defaulted)
- `tests/test_model_freeze.py` - 18 tests (new file)
- `tests/test_model_train.py` - 16 new tests (9 lineage/freeze + 7 calibration/tier/no-play)
- `tests/test_model_evaluate.py` - 5 new tests
- `data/reference/corpus_freeze/2026-09-08_ae1f0140.json` - the real, committed freeze manifest from this project's own corpus

## Decisions Made

See `key-decisions` in frontmatter. The most significant: `no_play_share` could not be computed directly on `model_data` as the plan's action text literally described, because `result_raw` is dropped by `mutate_fn`'s final column selection before `model_data` exists -- computing it there would have made the metric silently absent from every production run, contradicting the plan's own top-level `<verification>` requirement ("metrics include ... `no_play_share`"). Resolved by joining `model_data`'s own `(game_id, play_id, source)` keys back onto the pre-mutate `filtered` frame, which still carries `result_raw`. Verified end-to-end on the real corpus (see Proof Run below): `no_play_share` genuinely reads `0.0` today because the live corpus has zero rows matching the three no-play tokens (cross-checked directly against `plays.parquet`), and a fixture test with synthetic `"Penalty"` rows confirms the computation itself is correct (`no_play_share == 0.25` for a 25%-overridden fixture).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `no_play_share` resolved via join instead of directly on `model_data`**
- **Found during:** Task 3
- **Issue:** The plan's action text said to compute `no_play_share` "on `model_data` if it has a `result_raw` column" -- but `result_raw` is never one of `EP_TRAINING_COLUMNS`/`WP_TRAINING_COLUMNS` (not a feature, not in `GROUP_COLUMNS`), so `mutate_fn`'s final `.select(list(selected_columns))` always drops it. Following the action text literally would make the guard a permanent no-op for every real training run, contradicting the plan's own `<verification>` requirement that a real/fixture run's metrics include `no_play_share`.
- **Fix:** Compute the no-play boolean mask on `filtered` (the pre-`prepare_fn`/`mutate_fn` frame, which still carries the raw canonical `result_raw` column), then join it onto `model_data`'s own `(game_id, play_id, source)` keys (already present via `GROUP_COLUMNS`) before aggregating the share. The guard now checks columns on `filtered`, not `model_data`.
- **Files modified:** `src/flag_football_ep/model/train.py`
- **Verification:** `test_train_ep_logs_no_play_share_metric_matching_overridden_fraction` (fixture, 25% override -> 0.25 measured) and the real proof run below (0.0, cross-checked directly against `plays.parquet`'s `result_raw` column -- genuinely zero matches today, not a guard failure).
- **Commit:** `d93dbe3`

---

**Total deviations:** 1 auto-fixed (Rule 1 - bug in the plan's literal instruction, not the codebase). **Impact:** Necessary for the feature to actually work in production; no scope creep -- still the same metric, same tokens, same aggregate-share-only scope the plan specified.

## Issues Encountered

- The session was interrupted multiple times by network/rate-limit resets while polling long-running background test commands. No work was lost -- every task's commits were already on the branch (`1085c91`, `b09094a`, `d93dbe3`) before each interruption, verified via `git log` on resume.
- This plan ran on a shared (non-isolated) working tree with sibling executors (M3-05-02 on the model card/MLflow-UI docs, and later an IFAF-corrections plan). Staged only the exact files this plan's `files_modified` list names for every commit; never touched `data/reference/ifaf_spot_fill/`, `data/raw/`, or `data/processed/` per the operating constraints. The real proof run (below) incidentally observed a sibling agent's concurrent `ffep ingest` mid-flight, which is discussed there rather than hidden.

## Proof Run (real `ffep train`, not a fixture)

Ran `ffep train --model wp` for real against this project's actual `data/processed/plays.parquet` (registered as a new `wp_model` version only -- **no `ffep promote` call, no champion alias moved**):

- **run_id:** `f4dcb7b1324c419080cf66c0e242f105` (registered as `wp_model` version 6)
- **Logged params:** `git_commit=d93dbe3298acd7d91d15aefcaa224309d4bdb545`, `corpus_fingerprint=1045e099...`, `freeze_manifest_path=.../data/reference/corpus_freeze/2026-09-08_ae1f0140.json`, `freeze_date=2026-09-08`, `freeze_fingerprint_matches_corpus=false`
- **Logged metrics:** `calibration_max_deviation_wp=0.3611`, `per_tier_logloss_womens_international=0.8361` (+`per_tier_naive_logloss_womens_international`, `per_tier_improvement_womens_international`), `no_play_share=0.0`

`freeze_fingerprint_matches_corpus=false` is expected and correct here, not a bug: a sibling executor was concurrently re-running `ffep ingest` on this shared tree between when the freeze manifest was written (full ~27k-row, 5-source corpus) and when this proof run executed (`plays.parquet` was transiently ifaf-only, 1,951 rows, mid-reingest) -- exactly the corpus-drift condition this citation feature exists to catch. `no_play_share=0.0` was cross-checked directly against `plays.parquet` (`result_raw` genuinely has zero rows matching `Timeout`/`Offsetting Penalties`/`Penalty` in the current corpus state) -- not a guard failure; the guarded-join logic itself is separately proven correct by the fixture test using a synthetic 25% no-play override.

## Test Result

`uv run pytest tests/test_model_freeze.py tests/test_model_train.py tests/test_model_evaluate.py -q` -- **110 passed** (18 + 56 + 36 test functions across the three files), 0 failed, run three times independently during this session (after Task 1, after Task 2, after Task 3) with identical green results each time.

`uv run ffep freeze-corpus --help` and `uv run ffep train --help` (lists `--freeze`) both verified working.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- M3-05-06 (promotion gate) can now read `logo_mlogloss`/`logo_logloss`, `calibration_max_deviation_*`, per-tier log-loss and `no_play_share` directly from any MLflow run's own metrics -- no CSV dependency, and no dependency on the ablation script's ad-hoc scope.
- Every future `ffep train` run is self-describing: which git commit, which corpus fingerprint, and (once `ffep freeze-corpus` is run before a retrain) which frozen corpus snapshot it was trained against.
- Follow-up flagged, not this plan's scope: per-source snapshot dates in the freeze manifest are a best-effort file-mtime approximation (explicitly labelled `"approximate": true`) -- a real `_fetched_at.json` sidecar in the fetch layer (RESEARCH Pitfall 3, option b) would make this exact, deferred as documented.

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-09*
