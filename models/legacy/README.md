# Legacy pre-pipeline pickles

`ep_model.pkl`, `ep_model_simple.pkl`, `wp_model.pkl` and `wp_model_simple.pkl` in this
directory are the pre-pipeline notebook artifacts produced by the fixed-filename
`pickle.dump("ep_model.pkl", ...)`-style calls in `models/ep_model.ipynb`/`models/wp_model.ipynb`
before phase 1.2 migrated training into `src/flag_football_ep/model/train.py`. Every run of the
notebook silently overwrote the same file (T-1.2-07, phase 1.2 threat register) -- these four
files are whatever the notebook happened to write last, with no run id, training-data hash, or
date attached.

They are kept here **only for historical reference**. They are **not loadable by `ffep
score`**: `flag_football_ep.model.score` resolves models through the MLflow registry's
`champion` alias (`flag_football_ep.model.registry.resolve_champion`), never a filesystem
pickle path -- there is no code path anywhere in this pipeline that reads a `.pkl` file back
into a scoring run.

**Do not add files back to `models/` (the parent directory).** Current model exports go
through `flag_football_ep.model.train._export_pickle`, which writes
`{prefix}_model_{YYYYMMDD}_{hash8}.pkl` directly under `models/` and refuses to overwrite an
existing file of the same name -- the dated+hash naming scheme these four legacy files predate.
A hygiene test (`tests/test_repo_hygiene.py::test_models_dir_has_no_fixed_name_pickles`)
enforces that no fixed-name pickle reappears directly under `models/` (this `models/legacy/`
subdirectory is exempt).
