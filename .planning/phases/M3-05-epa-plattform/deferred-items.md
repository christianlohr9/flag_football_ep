# Deferred Items — Phase M3-05-epa-plattform

Out-of-scope discoveries logged during plan execution, per the executor's scope-boundary rule
(only auto-fix issues directly caused by the current task's own changes).

## From M3-05-01 (champion promotion decision)

- **`tests/test_hc_corpus_ablation.py::test_main_both_models_writes_seven_csvs` fails**
  (`mlflow.exceptions.MlflowException: Changing param values is not allowed.`), both in the
  full doc-adjacent test run and in isolation.
  - **Found during:** M3-05-01 Task 2's broader verification pass (mirroring the M3-02
    rerun's own verification scope: `tests/test_m3_epa_docs.py tests/test_m3_epa_snapshot.py
    tests/test_hc_corpus_ablation.py tests/test_reports_hc_comparison.py`).
  - **Not fixed here:** `scripts/hc_corpus_ablation.py`, `src/flag_football_ep/model/train.py`
    and `tests/test_hc_corpus_ablation.py` are all outside M3-05-01's file ownership
    (`docs/epa-refinement-2026-10.md` only; `data/**`, `src/**`, `scripts/**`, `tests/**` are
    read-only for this plan except the gitignored MLflow registry itself).
  - **Likely cause (not verified further):** `git log` shows two recent, separate commits each
    adding `corpus_fingerprint`/`git_commit` MLflow-param logging on overlapping code paths --
    `9bbe03a` (M3-02, `scripts/hc_corpus_ablation.py`) and `b09094a`/`d93dbe3` (M3-05-03,
    `src/flag_football_ep/model/train.py`, adding the same param names plus calibration/
    per-tier/no-play-share metrics to every `ffep train` run). If `hc_corpus_ablation.py`
    calls into the now-changed `train.py` path and both attempt to log `corpus_fingerprint`/
    `git_commit` under the same MLflow run with different computed values, MLflow's
    "changing param values is not allowed" would fire exactly like this.
  - **Scope for a future plan:** M3-05-03 or a follow-up should re-run this specific test in
    isolation and, if still failing, reconcile the two param-logging call sites so they agree
    (log once, or log identical values) rather than each independently `log_param`-ing the
    same key.
  - **Confirmed NOT caused by M3-05-01's own actions:** the failing test registers its own
    isolated model version 1 in a fresh tracking store (`Successfully registered model
    'ep_model'. Created version '1'...`) -- unrelated to the real `mlruns/` store this plan's
    `ffep promote`/`ffep score` calls touched (which is already at champion version 5 for both
    models, unaffected by this test's isolated fixture).

---
*Phase: M3-05-epa-plattform*


Out-of-scope discoveries logged during plan execution, per the executor's scope-boundary
rule (only auto-fix issues directly caused by the current task's changes).

## M3-05-06: `test_modellkarte_run_ids_resolve_to_registry_or_csv` fails in this worktree

- **Found during:** M3-05-06 Task 2 verification (running `tests/test_m3_epa_docs.py` as a
  broader sanity check beyond the plan's own `<verify>` commands).
- **Issue:** `docs/epa-modellkarte.md` quotes MLflow run ids
  (`5e8ec9573e774ebaa20c9694c6ae15bb`, `f9cfe5f348244a7f99dd6817785bff6d`) that resolve to
  neither a committed `ablation_summary.csv` row nor this worktree's live MLflow store
  (`mlruns/mlflow.db`).
- **Why out of scope:** Not caused by any file this plan (M3-05-06) touches
  (`src/flag_football_ep/model/gate.py`, `config.py`, `ffep.toml`, `cli.py`,
  `tests/test_model_gate.py`, `tests/test_cli_smoke.py`, `docs/model-training.md`). This is
  an isolated-worktree environment gap: this worktree never ran a real `ffep train`/
  `ffep promote`, so its local `mlruns/mlflow.db` lacks the runs the model card (written by
  a different plan/executor, M3-05-02) cites. A sibling executor running on the main tree
  (per this plan's own briefing) is promoting these exact runs concurrently — once that
  lands and this worktree merges, the live store should carry them.
- **Action:** Not fixed here. Flagged for whoever runs the phase-level full-suite check
  after all worktrees merge back.

## M3-05-06: two `test_model_score.py` null-feature-row backfill tests fail

- **Found during:** M3-05-06 Task 2, a broader background regression sweep
  (`tests/test_config.py tests/test_model_train.py tests/test_model_evaluate.py
  tests/test_model_freeze.py tests/test_model_score.py -q`) run beyond the plan's own
  `<verify>` commands.
- **Issue:** `test_score_plays_backfills_ep_on_null_feature_row_from_next_play` and
  `test_score_plays_backfills_wp_on_null_feature_row_from_next_play` both fail with
  `assert None is not None`.
- **Why out of scope:** `git diff --stat b940c56 HEAD -- src/flag_football_ep/model/score.py`
  is empty -- this plan's commits never touch `score.py`, and neither test imports or
  exercises `gate.py`/`config.py`'s `promotion_gate`/`cli.py`'s `promote` changes. The base
  commit this worktree started from (`b940c56`) already carries a prior fix titled "stop
  backward-filling ep/wp across null-position rows" (`46c529e`) touching this exact
  behavior; this looks like a pre-existing gap in that area, not a regression from M3-05-06.
- **Action:** Not fixed here -- outside this plan's scope boundary. Flagged for whoever owns
  `score.py`'s null-feature-row backfill behavior next.
