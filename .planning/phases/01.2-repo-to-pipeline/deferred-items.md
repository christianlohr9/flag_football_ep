# Deferred Items

Out-of-scope discoveries logged during plan execution, per the executor's scope-boundary
rule (fix only what the current task's changes directly touch; log everything else here
instead of fixing it).

## 01.2-17

- **`notebooks/explainable_ai_ep.ipynb` / `explainable_ai_wp.ipynb`, cell 2: pre-existing
  `.drop("label")` bug in the analysis logic.** `features_ep`/`features_wp` (the SHAP
  feature-selection lists) never include `"label"`, so `make_ep_model_mutations(...)
  .drop("label")` / `make_wp_model_mutations(...).drop("label")` raises
  `polars.exceptions.ColumnNotFoundError`. Confirmed pre-existing: the same
  `features_ep`/`features_wp` lists and the same `.drop("label")` call already existed in
  the original notebook before plan 01.2-17 touched anything, and `make_ep_model_mutations`/
  `make_wp_model_mutations` have always `.select()`ed only the passed columns (both in the
  ported package and in the frozen `Python/helper_add_model_mutations.py`) -- so this cell
  would have failed identically with the unmodified `Python/` helpers. Plan 01.2-17's scope
  was strictly the import cells (cells 0 and 2's `sys.path.insert`/`from helper_*` lines,
  plus the `data_raw.csv` path which moved to `data/raw/legacy/` in plan 01.2-10); the
  `<action>` explicitly said "leave the analysis cells untouched." Verified cells 0-1 (package
  imports + `pickle.load` of `models/{ep,wp}_model.pkl`) execute cleanly against real data;
  cell 2 fails on the pre-existing bug, cells 5+ additionally require `shap`, which is not a
  `pyproject.toml` dependency (unrelated, separate gap). Not fixed here -- flagging for
  whichever future phase next touches these notebooks' analysis logic.
