# Model Training

Operator guide to the EP/WP retraining, experiment and promotion workflow built in phase 1.3
(REQ-S1-07 through REQ-S1-11). For the rest of the pipeline (ingest, data layout, reference-file
maintenance, validation semantics), see `docs/pipeline.md` — this file covers only the
training/experiment/promotion/scoring workflow in depth.

## 1. The evaluation protocol

Every reported EP/WP metric comes from **leave-one-game-out (LOGO)** over `game_id`
(REQ-S1-07, D-07): `flag_football_ep.model.evaluate.run_logo` fits one model per held-out game
(`sklearn.model_selection.LeaveOneGroupOut`), on every other game's rows, and accumulates the
held-out predictions. No game's plays ever appear in both a fold's train and test partition.

Every training run logs the params that record this protocol:

| Param | Meaning |
|---|---|
| `cv_scheme` | Always `leave-one-game-out` — never a play-level split. |
| `group_column` | Always `game_id` — the LOGO grouping key. |
| `n_folds` | The number of distinct games measured (one fold per game after exclusions and null-drops — see `01.3-TRAINING-REPORT.md` §2 for why this is usually smaller than the raw game count). |
| `training_data_sha256` | SHA-256 over the training frame's Parquet bytes — reproduces exactly which rows a run was trained on. |

**The shipped model is a separate single refit on every included game**, fit once with the same
(tuned or fixed) hyperparameters the LOGO loop measured with. The 214+ LOGO fold models
themselves are measurement-only — they are never exported, registered, or returned from
`train_ep`/`train_wp`.

## 2. The normal loop

```bash
ffep ingest                          # raw exports -> canonical Parquet
ffep train --model both              # LOGO-measured EP + WP training runs
mlflow ui --backend-store-uri sqlite:///$(pwd)/mlruns/mlflow.db   # review in the browser
ffep promote --model both            # explicit, human-reviewed champion promotion
ffep score                           # score the canonical dataset with the champion models
```

Open `http://127.0.0.1:5000` after the `mlflow ui` command above. In the `ep_model`/`wp_model`
experiments, confirm the newest run's `logo_*`/`naive_*`/`logloss_improvement` metrics, the
`reliability_{ep,wp}.png` artifact, and the `per_source_metrics.md` artifact all look reasonable
before promoting — this is the review step `ffep promote` exists to gate.

## 3. Why promotion is manual

`ffep score` resolves which model to use through the MLflow model registry's `champion` alias
(`flag_football_ep.model.registry.resolve_champion`) — it **never** silently falls back to the
most recent FINISHED run. Before any promotion, running `ffep score` fails with this exact
message (from `registry.resolve_champion`, wrapping the registered model name and the
configured tracking store):

```
no 'champion' alias set for registered model 'ep_model' in tracking store
'sqlite:////path/to/mlruns/mlflow.db' -- run `ffep promote` after reviewing a training run
```

This is deliberate: CONTEXT's promotion decision requires an explicit `ffep promote`, "used
after reviewing the training report" — not an implicit newest-run pickup. `ffep promote --model
<ep|wp|both>` moves the `champion` alias to the most recent FINISHED run of that model's
experiment (or a specific `--run <run_id>` if you want to pin an older one). A second `promote`
call with a different run moves the alias without deleting the previous version — nothing in
the registry is ever destroyed by a re-promotion.

## 4. Running a feature experiment

```bash
ffep experiment --candidate <name|all> --model <ep|wp|both>
```

`<name>` is one of the entries in `flag_football_ep.model.experiments.CANDIDATES` (currently
`recency`, `real_clock` — `half` and `competition_tier` were adopted into production in plan
01.3-09 and are no longer registered, since a candidate already in the production feature set
has nothing left to test through this harness). Verdicts land in a dedicated
`{ep,wp}_model_candidates` MLflow experiment — never the production `ep_model`/`wp_model`
experiment — tagged `candidate=<name>` with a `verdict` param of `adopted` or `rejected` and the
measured `control_logloss`/`candidate_logloss`/`delta` metrics.

**Adoption criterion (CONTEXT):** a candidate is adopted when mean pooled LOGO log-loss
*improves* over the current production feature set (`delta > CANDIDATE_ADOPTION_MIN_DELTA`,
currently `0.0` — no margin required, but no tie counted as an improvement either).

**`ffep experiment` never registers or promotes a model.** It is measurement-only — the harness
structurally cannot call the registry's model-logging or alias-setting APIs. Folding a verdict
into production (editing `hyperparams.py`'s `EP_FEATURES`/`WP_FEATURES`) is a separate,
human-reviewed decision a later plan makes once every candidate under consideration has a
verdict — see `01.3-TRAINING-REPORT.md` §6 for how plan 01.3-09 made that decision for the four
REQ-S1-09 candidates.

## 5. Out-of-fold predictions

Every `ffep train` run writes `data/processed/oof_predictions_{ep,wp}.parquet` — the
**Phase 1.4-facing contract** for own-team EPA reporting:

- **Join key:** `(game_id, play_id)`, unique across the file.
- **Columns:** `game_id` (Utf8), `play_id` (Int32), `source` (Utf8), plus one Float64
  probability column per class — `EP_PROB_LABELS` order for the EP file
  (`Touchdown_Prob`, `Opp_Touchdown_Prob`, `Safety_Prob`, `Opp_Safety_Prob`, `No_Score_Prob`),
  a single `wp` column for the WP file.

**Phase 1.4 must join onto these files for historical own-team EPA, not re-score historical
plays with the champion model.** Every value in `oof_predictions_*` is a prediction from a
model that never saw that play's game during fitting (the LOGO out-of-fold guarantee,
nflfastR's own-team EPA precedent) — re-scoring a historical play with the final champion model
would flatter it with in-sample knowledge. New/unseen games (not part of the training corpus)
are the one case where `ffep score`'s champion-model scoring is the correct source.

## 6. PAT baselines and the break-even chart

`flag_football_ep.features.mutations.estimate_pat_baselines` computes the pooled 1-pt and 2-pt
PAT success rates from the full canonical corpus (REQ-S1-10), each with its attempt count and a
Clopper-Pearson confidence interval (`scipy.stats.binomtest(...).proportion_ci()` — chosen over
a normal approximation because this corpus's PAT counts are small and its rates can be extreme).
`add_ep_variables` requires `pat_baselines` as an explicit keyword argument with no default and
no fallback to a hard-coded constant.

```bash
ffep pat-breakeven --overwrite     # writes data/processed/pat_breakeven.png
```

The chart plots 1-pt expected points (a horizontal line at the observed 1-pt rate) against the
2-pt expected-points curve (`2 * p2`) over a swept 2-point success rate, marking the break-even
rate (`p1 / 2`) and the corpus's own observed 2-point rate (shaded across its CI). See
`01.3-TRAINING-REPORT.md` §7 for the current measured rates and reading.

## 7. Artifacts and versioning

The **MLflow model registry is the source of truth** (REQ-S1-11): every `ffep train` run
registers its production refit as a new version of `ep_model`/`wp_model`
(`registry.register_production_model`) — no version is ever overwritten by a later run, and the
`champion` alias moves only through `ffep promote`.

`--export-pkl` additionally writes a dated, hash-suffixed `.pkl` under `[paths] models`
(e.g. `models/ep_model_20260819_5a71cb29.pkl`) — a **secondary**, offline-compatible export for
existing consumers of the notebook's `{ep,wp}_model.pkl` convention. It refuses to overwrite an
existing file (`_export_pickle`'s write-once guard), so `ep_model.pkl` can never be silently
clobbered the way the pre-phase-1.3 notebook workflow did.

`models/legacy/` holds the pre-phase-1.3 fixed-name pickles (`ep_model.pkl`, `wp_model.pkl`,
`*_simple.pkl`) as a committed, **archive-only** snapshot — nothing in the current pipeline
reads from it, and no current code path writes into it.

## 8. Known limitations

- **`sportapp` (live sportapp.fi) is absent from the current corpus.** The API key rotation is
  a tracked STATE.md blocker deferred by the user; `ffep fetch-sportapp` fails cleanly with an
  actionable "environment variable not set" error rather than silently reusing the old
  compromised key, and simply has not been run in this environment.
- **`half_seconds_remaining`/`game_seconds_remaining` are synthetic in production**, pending
  REQ-S1-02 (real Hudl clock data). The `real_clock` experiment (§4) quantifies what this costs
  on games where real time *is* available (IFAF), but on the current corpus the measurement
  itself is not viable — see `01.3-TRAINING-REPORT.md` §6/§9 for the specific coverage gap that
  blocks it (`yards_to_go` null for every `ifaf` row, unrelated to the clock itself).
- **Calibration correction is deferred.** REQ-S1-08's reliability curves are report-only in this
  phase — no isotonic/Platt correction layer exists. `01.3-TRAINING-REPORT.md` §4 documents
  EP's curves as genuinely miscalibrated on the touchdown-adjacent classes; whether to add a
  correction layer is an explicit follow-up decision, not something this phase's code does.
- **PAT rates are pooled across all sources and competition levels.** Only the `legacy` source
  currently has PAT-shaped rows (`down == 0`) in this corpus — the "pooled" rate is, in
  practice, a `legacy`-source estimate. This will change automatically as other sources'
  ingested data grows to include PAT-shaped plays; no code change is needed when that happens.
