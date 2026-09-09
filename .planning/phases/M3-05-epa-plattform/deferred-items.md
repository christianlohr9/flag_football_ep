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
