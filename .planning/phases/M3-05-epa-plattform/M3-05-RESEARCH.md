# Phase M3-5: EPA-Modell als Produkt — Reproduzierbarkeit, Beförderungs-Gate, Modellkarte, Plattform-Entscheidung - Research

**Researched:** 2026-09-08
**Domain:** MLOps hygiene for a solo-developer sports-analytics pipeline (MLflow lineage/registry/promotion gates, CI on fixtures) + a one-time platform ADR (single machine vs. containerised OTC vs. Kubernetes, feature store, multi-tenant data model)
**Confidence:** MEDIUM — Part A (repo-internal engineering hygiene) is HIGH confidence, grounded in direct code reading and a live query of the project's actual MLflow store. Part B (platform ADR) is MEDIUM/LOW confidence on cost figures — OTC does not publish fixed list prices for ECS/CCE/OBS, only a price calculator, so every cost figure below is order-of-magnitude and flagged for validation, not a quote.

## Summary

This phase turns a working-but-manual MLflow setup into something a coach can trust and a
second programme could adopt, without over-building for a scale that doesn't exist yet. The
concrete, high-value finding from reading the actual code and querying the live MLflow store
(`mlruns/mlflow.db`) is that **lineage work is already half-started, but scoped to the wrong
place**: an uncommitted change to `scripts/hc_corpus_ablation.py` (working tree diff, not yet
committed) already computes a `corpus_fingerprint` (SHA-256 over `(game_id, play_id, source)`)
and a `git_commit` (via `git rev-parse HEAD`) and logs both as MLflow run params — but only for
runs launched through that one ablation script. Runs launched the normal way, through the
installed `ffep train` console-script entry point, get **no git lineage at all**: verified by
reading the actual champion run's tags in the live store (`mlflow.get_run(...).data.tags` has no
`mlflow.source.git.commit` key), because MLflow's automatic git-detection only fires when the
run's source file is a `.py` path inside the git working tree — not when it's invoked via a venv
console-script (`/…/.venv/bin/ffep`). The smallest fix is small: lift the two functions
(`compute_corpus_fingerprint`, `git_commit_sha`) out of the ablation script and into
`model/train.py`'s shared `_log_run` helper, so every `ffep train` run gets them automatically,
regardless of entry point. Two other gate-relevant numbers — per-tier log-loss and no-play-token
share — are *also* currently computed only by the ablation script (`build_tier_metrics`,
`report_no_play_rows`) and written to CSV files, never logged as MLflow metrics on the
production run. A gate that "reads from the store, not CSVs" (CONTEXT's explicit requirement)
needs these promoted into `_train`'s standard metric-logging path too. A genuine gap, not just a
relocation: there is currently **no scalar calibration metric anywhere in the codebase** — only
a reliability-curve PNG artifact — so "calibration within tolerance" as a gate criterion needs
one new, cheap computation (a max-deviation-per-class summary over the existing
`reliability_curves()` output), not a new library.

The champion alias for both `ep_model` and `wp_model` is, as of this research, still pointing at
version 1 (the original phase-1.3 run) even though five versions are now registered for each
(confirmed live: `client.get_registered_model('ep_model').aliases == {'champion': 1}`) — the
2026-09-08 IFAF-correction re-run (`docs/epa-refinement-2026-10.md`'s Nachtrag section)
explicitly did not call `ffep promote`. The champion-promotion decision this phase's CONTEXT
assumes has already happened ("execution after … the champion decision is taken") **has not
happened yet** — this is a blocking precondition, not a research finding to work around; see
Open Questions.

For the platform ADR, the evidence is unambiguous in one direction and genuinely open in
another. Unambiguous: at today's scale (one programme, ~30k plays, batch scoring under 10
minutes, no real-time requirement, solo developer) a feature store is not warranted — the
decision-framework literature converges on "3 of 5 trigger signals" (multiple models,
training-serving skew incidents, real-time freshness need, engineering bottleneck, duplicated
team work) before a feature store's overhead pays for itself, and this project currently has
zero of the five. Kubernetes is not warranted either: CCE is free for un-managed clusters up to
50 nodes but still requires provisioning and operating the underlying ECS nodes, and the
operational burden (cluster upgrades, network policy, secrets management) has no counterpart
benefit at one batch job a month. Genuinely open, and explicitly deferred to the user per
CONTEXT: whether the *coach-facing web app* (BL-02, out of scope for this phase but the reason
the ADR exists) should live on a single machine with MLflow's file/SQLite store, or move to a
small containerised service (MLflow server + Postgres + OTC OBS artifact store + a lightweight
API) *before* BL-02 is built on top of it — because retrofitting auth/multi-tenancy onto a
single-user file store later is materially harder than building on Postgres from the start. The
recommended staged path (below) treats this as a two-step decision: stay single-machine through
this phase and BL-02's first cut, move to a small OTC VM with docker-compose (MLflow + Postgres
+ OBS) only when a second human other than the current developer needs write access to the
model registry or a coach needs the web app to run without the developer's laptop being on.

**Primary recommendation:** Do the Part A hygiene work now (lineage, gate, freeze, model card,
MLflow UI how-to, CI) entirely within the existing single-machine MLflow/SQLite/file-artifact
setup — nothing here requires new infrastructure. Write the ADR now, as required, but recommend
staying on Option (i) (single machine) for this phase, with an explicit, cheap migration
trigger (second write-access user, or BL-02 web app needing to run independent of the
developer's machine) that promotes to Option (ii) (OTC VM, docker-compose, Postgres + OBS).
Defer Kubernetes and a feature store to "not now, revisit if programme count exceeds ~3 with
materially divergent feature needs."

## Findings by Question (Q1–Q10)

This section answers the ten research questions directly, with evidence pointers into the
sections above (which carry the full code excerpts/citations) rather than repeating them.

### Part A — Engineering Hygiene

**Q1 — Current MLflow setup and smallest lineage change set.** Tracking URI is SQLite
(`sqlite:///{mlruns}/mlflow.db`, `model/mlflow_store.py:24-31`); artifact root is a local
`file://` path (`mlflow_store.py:33-38`) — deliberately not the deprecated local `file:`
*tracking* store, because the registry's alias APIs require a SQL-backed store
(`mlflow_store.py`'s own module docstring). Every `_train` run
(`model/train.py::_log_run`) logs: params (`booster`/`eta`/…, `n_plays`, `n_features`,
`cv_scheme`, `group_column`, `n_folds`, `logo_wall_seconds`, `training_data_sha256`,
`excluded_game_ids`, `tuned`, `max_evals`, `oof_predictions_path`, `best_*` when tuned);
metrics (`logo_mlogloss`/`logo_logloss`, `naive_*`, `logloss_improvement`,
`logo_logloss_by_source_<source>`); artifacts (`reliability_{ep,wp}.png`,
`per_source_metrics.md`, the xgboost model). MLflow 3.15 (installed, verified live) supports
tags, model versions and aliases via `MlflowClient` — all already in use in `registry.py`.
**Smallest change set** (see Pattern 1/2 in Architecture Patterns): lift
`compute_corpus_fingerprint`/`git_commit_sha` from the uncommitted `scripts/
hc_corpus_ablation.py` diff into `model/train.py::_log_run`, so every `ffep train` run gets
them — today only the ablation script's runs do, and `ffep train`'s own runs get *zero* git
lineage because MLflow's automatic git-tag detection does not fire for a console-script entry
point (verified live against the actual champion run's tags this session — Pitfall 1).

**Q2 — Promotion gate design.** Metrics that exist today, per model/arm: LOGO log-loss vs.
naive baseline (`logo_mlogloss`/`naive_mlogloss` for EP, `logo_logloss`/`naive_logloss` for
WP), per-source log-loss breakdown (`logo_logloss_by_source_<source>` IS a metric, verified),
calibration (PNG artifact only — **no scalar metric exists today**, a genuine gap), per-tier
eval and no-play share (both computed only by `scripts/hc_corpus_ablation.py`, written to
CSV, never logged to the run itself — CONTEXT's "reads from the store, not CSVs" requirement
is not met by the current code and needs the same generalization as Q1's lineage fix). Gate
design: `ffep promote` resolves the candidate run's metrics via
`MlflowClient().get_run(candidate_id).data.metrics`, resolves the current champion's metrics
via the existing unmodified `registry.resolve_champion` + `mlflow.get_run(...)`, and compares
both against `[promotion_gate]` thresholds in `ffep.toml` (new table, following the existing
`Paths.raw_hc_files`-style optional-field-with-default convention in `config.py` so old
configs keep loading — Pattern 4). **Threshold proposals derived from the measured M3-2
numbers**: (a) beats naive baseline — `logloss_improvement > 0`, already the case for every
arm measured so far (EP with_hc: 0.052225; WP with_hc: 0.334438, per
`docs/epa-refinement-2026-10.md`'s Kalibrierung table) [VERIFIED: repo
`data/reference/epa_refinement/ablation_summary.csv`]; (b) beats current champion —
`candidate_logloss <= champion_logloss + epsilon`, with `epsilon` defaulting to `0.0` (no tie
counted as an improvement, mirroring `CANDIDATE_ADOPTION_MIN_DELTA`'s existing convention in
`model/hyperparams.py` for feature-candidate adoption — same philosophy, new call site); (c)
calibration within tolerance — a new `calibration_max_deviation_<class>` metric (max
`abs(prob_true - prob_pred)` per class from the existing `reliability_curves()` output),
threshold `[ASSUMED]` at `0.15` absolute deviation pending a baseline measurement against the
current champion's own curves (no historical value exists yet to calibrate this number
against — flag for the planner to measure-then-set, not guess-then-ship); (d) no-play share
under threshold — reuse the **2% escalation threshold M3-2 already established informally**
(`docs/epa-refinement-2026-10.md`: "über der intern verabredeten 2-%-Eskalationsschwelle")
[CITED: repo doc, already a project convention, not a new number]. Escape hatch: `--force
--reason "<text>"` logs the reason as an MLflow tag on the promoted run
(`promotion_override_reason`), not just a CLI echo (Pitfall 4/Code Examples) — itself
auditable in the MLflow UI.

**Q3 — Corpus freeze.** No in-band snapshot-date metadata exists today for IFAF or
HC-workbook sources (verified: no `fetched_at`/`snapshot_date` field anywhere in
`fetch/ifaf.py`/`ingest/hc_workbook.py`) — Pitfall 3 covers this gap and recommends a
best-effort file-mtime fallback, clearly labelled as approximate, rather than blocking this
phase on a fetch-layer change. Manifest design: a dated, fingerprinted JSON
(`data/reference/corpus_freeze/<date>_<fingerprint8>.json`, committed to git like every other
reference dataset in this project) carrying `date`, `corpus_fingerprint` (the same
`(game_id, play_id, source)` SHA-256 already implemented in the ablation script — Pattern 1),
`git_commit`, per-source row counts (`plays.parquet.group_by("source").len()`, trivial to
compute), and accepted/quarantined game counts (already computed by `run_ingest`'s
`_build_games_table`/`games.csv` output, per `pipeline.py` read this session — no new
computation needed, just a re-read of an existing artifact). A new `ffep freeze-corpus`
command writes this; `ffep train` gains an optional `--freeze <path>` (defaulting to the
latest manifest) so every training run can cite which freeze it trained against.

**Q4 — MLflow UI for a coach.** `mlflow ui --backend-store-uri sqlite:///.../mlflow.db`
(already documented, `docs/model-training.md` section 2, unchanged by this phase) exposes the
registered-model list with aliases/versions with zero new code — that part IS
coach-appropriate ("which model is in production, since when"). Raw per-run metric tables and
hyperparameter dumps are NOT coach-appropriate — too much noise, no narrative. Given
`docs/epa-modellkarte.md` is referenced by CONTEXT as an existing template but was **not
found anywhere in the repo this session** (Open Question 2), the honest recommendation is: a
small, generated, German model-card page (numbers pulled from MLflow + the same
`data/reference/epa_refinement/*.csv` files `docs/epa-refinement-2026-10.md` already cites,
guarded by a doc-guard test in the style of `tests/test_m3_epa_docs.py`) is the durable
coach-facing artifact; the live `mlflow ui` is a good *demo* tool for the October sync itself
(walked through once, live, by the developer) but not something a coach should be expected to
navigate unsupervised.

**Q5 — CI on fixtures.** `tests/conftest.py` already provides a `tmp_path`-scoped `Config`
fixture (every path redirected away from the real repo `mlruns/`) and a synthetic canonical
corpus, deliberately small "so the suite stays fast" (that file's own docstring, read this
session) — `tests/test_model_train.py` already exercises `train_ep`/`train_wp` against it. No
repo-wide `.github/workflows/` directory exists yet (verified: `ls .github/workflows` → not
found). Proposed CI: a new `.github/workflows/ci.yml` running `uv sync --dev` (core
dependency group only — **not** the `cv`/`versioning` extras, which pull in multi-GB
torch/rfdetr stacks this phase's tests don't need) then `uv run pytest -q -k "not cv and not
hackathon"` to skip the heavy CV/hackathon suites this phase is out of scope for. No secrets
needed: every fixture-based training/registry test already uses a `tmp_path`-scoped MLflow
store and never touches `SPORTAPP_API_KEY`/`CPX_API_KEY`/OTC credentials. Runtime budget: not
measured this session (full-suite timing was not re-run to avoid an expensive real-corpus
training pass), but the fixture corpora are described as deliberately tiny, so a few minutes
is a reasonable estimate for the non-CV/non-hackathon subset — flag `[ASSUMED]`, confirm with
an actual timed run when the workflow is built.

### Part B — Platform ADR

**Q6 — Platform options.**

| Option | Monthly cost (order of magnitude) | Operational burden (solo dev) | Migration path from today | Breaks first at 10× data / 10 teams |
|---|---|---|---|---|
| (i) Single machine + MLflow file/SQLite + DVC + scheduled batch (**current state**) | $0 incremental | Minimal — already running | None; this IS today | SQLite's single-writer model risks lock contention under concurrent multi-user promotion/training; the registry lives only on one developer's laptop — no availability without it being on, which blocks a coach-facing web app needing independent uptime |
| (ii) Containerised on OTC: one VM (docker-compose: MLflow server + Postgres + OBS artifact root + scoring job + a small web app) | Low tens of euros/month order of magnitude for one small ECS VM + OBS storage (cents/GB) `[ASSUMED — LOW confidence, OTC has no fixed published rate, only a calculator; see Assumption A2]` | Moderate — one VM to patch/secure, Postgres backups, MLflow auth must be explicitly enabled + default admin password rotated (not on by default — Security Domain), a docker-compose file to maintain; still within solo-developer reach after initial setup | `mlflow server --backend-store-uri postgresql://... --default-artifact-root s3://...` replaces the `sqlite:///` URI; `MLFLOW_S3_ENDPOINT_URL` against OTC OBS already proven in this repo's own CV-bundle-upload runbook (`docs/hackathon-otc-upload.md`) | Comfortably handles batch scoring for a handful of programmes; still a single-VM capacity ceiling for many concurrent training jobs, but Postgres removes SQLite's write-concurrency ceiling |
| (iii) Kubernetes (OTC CCE) + MLflow + feature store + model serving | CCE control plane free for ≤50 non-HA nodes, but the underlying ECS worker nodes cost the same per-node as (ii)'s VM, and a toy cluster needs at least 2-3 nodes minimum — plausibly 2-3× (ii)'s floor `[ASSUMED — LOW confidence]` [CITED: t-cloud-public.com/en/prices/pricing-models/computing-container, accessed 2026-09-08, "free for up to 50 nodes (not HA)"] | High — cluster upgrades, network policy, K8s Secrets, YAML/Helm manifests, ingress/TLS: a categorically larger operational surface than (ii) for one developer | (ii) is a strict prerequisite — containerise first, then write K8s manifests/Helm charts around the same images | Nothing at this project's actual scale "breaks" in a way K8s specifically fixes; it earns its complexity at real-time high-QPS serving or many independently-scaled services, neither of which this project has (CONTEXT: "no real-time requirement exists today") |

**Q7 — Feature store.** Solves training-serving skew (features computed differently at train
vs. score time) and cross-team/cross-model feature reuse. This project's EP/WP feature set is
~12 batch-only play-state columns (`hyperparams.EP_FEATURES`/`WP_FEATURES`) computed by the
*same* `prepare_fn`/`mutate_fn` pair at both train time (`model/train.py`) and score time
(`model/score.py` — both import from `features/mutations.py`), so training-serving skew is
already structurally prevented by code sharing, not by infrastructure this project lacks. No
real-time serving requirement exists (CONTEXT, explicit). Lightweight alternative already
effectively in place: `plays.parquet`/`plays_scored.parquet` plus `training_data_sha256`
(existing) and the proposed `corpus_fingerprint` (Q1/Q3) *is* "a versioned feature Parquet
with a schema contract and a fingerprint" — just not labelled as such. Using the Tacnode
decision framework (3-of-5 trigger signals: multiple models with inconsistent feature
definitions, training-serving mismatch incidents, real-time freshness demand,
feature-engineering bottleneck, duplicated cross-team feature work)
[CITED: tacnode.io/post/do-you-need-a-feature-store, accessed 2026-09-08], this project is at
**0 of 5** today. Recommendation: no feature store now. Reconsider (Feast, the standard
open-source choice) only if a second/third programme's model needs materially different
features maintained by a different person than the current developer, or if real-time scoring
is ever added (e.g. a live win-probability overlay during a broadcast, which would also
require solving the game-clock-from-broadcast backlog item first).

**Q8 — Multi-tenant data model.** The canonical plays schema already carries the seeds of
programme scoping: `source`, `competition_tier` (one-hot, adopted into production features in
phase 1.3) and `team_mapping.csv` (already extended with `-M` suffixes to distinguish men's
from women's national teams sharing the canonical code `GER`, per `ffep.toml`'s own
`[sources.ifaf]` comment, read this session). A first-class `programme_id`/`season` scope
column would extend this pattern, not replace it. **Shared model vs. per-programme models**:
the evidence this phase's own re-run already produced argues for shared-model-with-covariates
over per-programme models — the first real per-tier comparison (`per_tier_metrics_ep.csv`,
`docs/epa-refinement-2026-10.md`'s Nachtrag) found `womens-international` (IFAF) at EP
log-loss 0.881 vs. `mixed-other` at 0.946, i.e. a real, measurable tier effect *already
captured by pooling data across the corpus with a tier covariate* — a per-programme model
trained on IFAF alone would have less data and lose exactly this pooling benefit. Given each
individual programme's own volume is small (the women's national team alone is ~20k plays per
CONTEXT; a new U17 programme would start from near zero), a shared model with a
programme/tier covariate is the evidence-backed recommendation, not a per-programme model per
team. **PII boundaries**: `player_mapping.csv` already exists as a per-programme(-ish) roster
mapping with an enforced PII gate pattern (the roster-based checks already present in
`tests/test_m3_epa_docs.py`); a second programme's roster needs its own scoped mapping file
following the same gate discipline, never merged with another programme's roster without
explicit consent. **Auth for a coach web app**: out of this phase's scope (BL-02), but the
platform choice determines where it would live — a single-machine file-store approach
(Option i) has no natural home for per-programme login/session state; a Postgres-backed
deployment (Option ii) is where user/session tables would go if/when BL-02 is built.

**Q9 — MLflow deployment realities.** No authentication by default; multiple CVEs disclosed
in 2026 against unauthenticated MLflow tracking servers (SSRF via webhook delivery,
CVE-2026-64849; job-endpoint auth bypass, CVE-2026-0545; default-credential bypass,
CVE-2026-2635) [CITED, see Sources — Tertiary, single-pass WebSearch summaries, not read at
full advisory depth]. Basic HTTP auth is available (`pip install 'mlflow[auth]'`, `mlflow
server --app-name basic-auth`) but ships a default `admin`/`password1234` credential that
must be rotated immediately [CITED: mlflow.org/docs/latest/self-hosting/security/
basic-http-auth/, accessed 2026-09-08]. Artifact store on OBS: proven pattern already exists
in this repo (`cv/bundle.py`'s `s3fs.S3FileSystem` against the same OTC OBS endpoint used for
CV dataset delivery) — MLflow's own S3 artifact repository uses `boto3` (not `s3fs`)
directly, so `boto3` would need adding (Package Legitimacy Audit: pre-cleared, `slopcheck
[OK]`) even though `botocore` is already transitively present via the `versioning` extras'
`dvc-s3`/`s3fs` chain. Postgres vs. SQLite: SQLite is adequate for the current single-writer
local use (today); a proper RDBMS becomes the standard recommendation once concurrent
multi-user writes happen (Option ii/iii) `[ASSUMED — general MLflow operational guidance, not
independently verified against a specific MLflow doc URL this session]`. Backup: for the
current SQLite setup, `mlruns/mlflow.db` + `mlruns/artifacts/` should be in whatever backup
routine already covers `data/reference/` — not verified whether one currently exists; flag
for the planner. Alternatives worth a sentence: Weights & Biases offers a materially better
UI/collaboration experience out of the box but is a SaaS dependency this project has
deliberately avoided everywhere except the OTC OBS bucket for CV bundle delivery; ZenML/
Metaflow are full pipeline-orchestration frameworks that solve multi-step DAG orchestration
problems this project's three-stage `ffep run` (ingest → train → score) does not have.
**MLflow stays** — it is already deeply load-bearing across `train.py`/`registry.py`/
`score.py`/`experiments.py`, and none of the alternatives address this project's actual pain
points (manual promotion, missing lineage, no gate — all fixable within MLflow, per Part A).

**Q10 — Recommendation, staged migration, decide-with-user vs. Claude's-discretion.** See
"Proposed Plan Breakdown (Waves)" below for how this becomes plans, and the Summary above for
the one-paragraph version. In short: **stay on Option (i)** through this phase and BL-02's
first cut; migrate to **Option (ii)** only when a concrete trigger fires (a second human needs
write access to the registry, or BL-02 needs to run independent of the developer's laptop
being on); reconsider **Kubernetes and a feature store together** only if a real-time
requirement emerges or programme count exceeds ~3 with materially divergent feature needs —
they tend to become relevant at the same scale point, so evaluate them together rather than
independently.

**What must be decided by the user (per CONTEXT: "not by default"):**
1. The ADR's final signed decision line itself — CONTEXT explicitly requires a decision line
   the user signs, not a default Claude picks.
2. Whether/when to trigger the Stage 1 migration (Option ii) — a real cost and operational
   commitment, not reversible without work.
3. Whether the pending champion-promotion decision (Pitfall 5 / Open Question 1) happens
   before or as part of this phase's execution.
4. The exact calibration-tolerance and no-play-share gate thresholds, once a first real
   candidate run exists to calibrate them against (Q2's proposed defaults are a starting
   point, explicitly `[ASSUMED]` where no historical baseline exists).

**What Claude can decide** (per CONTEXT's "Claude's Discretion"): gate threshold *defaults*
(subject to the user's calibration pass above), CI runner/workflow structure, the model-card
template's exact layout, the ADR's option set beyond the three CONTEXT already names, and the
freeze-manifest snapshot-date best-effort approach (Open Question 3) absent a user objection.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Run lineage (corpus fingerprint, git commit, config hash) | Training pipeline (`model/train.py`) | MLflow tracking store (persistence) | Lineage must be captured at the moment of fitting, inside the same process that already computes `training_data_sha256` — not reconstructed after the fact from logs |
| Corpus freeze manifest | Ingest/CLI layer (new `ffep freeze-corpus` command) | Reference data (`data/reference/`) | The freeze is a property of the *corpus* (post-`ffep ingest`, pre-training), not of any one model — one manifest, referenced by both EP and WP training runs |
| Promotion gate | CLI layer (`ffep promote`) reading the MLflow registry/tracking store | `ffep.toml` (`[promotion_gate]` thresholds) | The gate must read metrics MLflow already has, not re-derive them — thresholds are configuration, not code |
| Model card (coach-facing) | Docs generation script (new `scripts/render_model_card.py` or a `reports/` product) | MLflow registry (source of numbers) + `data/reference/epa_refinement/*.csv` (source of comparison numbers) | Same "generated from source, never hand-typed" discipline as `docs/epa-refinement-2026-10.md`'s doc-guard test; a new German doc needs its own `tests/test_m3_epa_docs.py`-style guard |
| MLflow UI (coach-facing browsing) | Local process (`mlflow ui` against the project's SQLite store) | — | No hosting tier needed for this phase — CONTEXT explicitly scopes a hosted variant to the ADR, not a default |
| CI on fixtures | GitHub Actions (or local pre-push hook) | Existing `tests/conftest.py` synthetic-corpus fixtures | The fixture machinery (tmp-path-scoped MLflow store, synthetic canonical frames) already exists (`tests/conftest.py`, `tests/test_model_train.py`) — CI is a runner-config problem, not a new-fixture problem |
| Platform ADR (batch vs. OTC containers vs. Kubernetes) | Decision record (`docs/adr/0001-modell-plattform.md`) | — | Explicitly a document, not code, this phase — CONTEXT: "decided with the user, with costs and a migration path" |
| Multi-tenant data model (future, not this phase's code) | Canonical Parquet schema (`competition_tier`, `team_mapping`) | Database tier (if/when Postgres is adopted) | Today's schema already carries `source`/`competition_tier`/team codes that a future programme/season scope column would extend, not replace |

## Standard Stack

### Core (already project dependencies — no install needed)
| Library | Version (verified) | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `mlflow` | 3.15.1 installed (pyproject floor `>=3.15`) [VERIFIED: repo `uv run python -c "import mlflow; print(mlflow.__version__)"`] | Tracking store, model registry, aliases, artifact logging | Already the project's tracking/registry backend since phase 1.3; the registry (`register_model`/aliases) requires the SQLite-backed store already configured in `model/mlflow_store.py` — this is documented in-repo, not a new finding |
| `xgboost` | `>=2.1.4` (pyproject) | Model family | Unchanged by this phase |
| `hashlib`, `subprocess` (stdlib) | — | Corpus fingerprint, git commit capture | Already used exactly this way in the uncommitted `scripts/hc_corpus_ablation.py` diff — no new dependency, just relocation into `model/train.py` |
| `pytest` | `>=8` (pyproject dev group) | CI fixture-based tests | `tests/conftest.py` already provides a `tmp_path`-scoped `Config` fixture and synthetic canonical-frame factories used by `tests/test_model_train.py` — the exact fixture shape a "train on fixtures" CI job needs already exists |

### Supporting (new, small)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `boto3` | not currently locked; compatible with the already-locked `botocore==1.43.56` (pulled in transitively via the `versioning` extras' `dvc-s3`→`s3fs` chain) [ASSUMED — version compatibility not independently verified this session] | MLflow's built-in `S3ArtifactRepository` uses `boto3` directly for any `s3://…` artifact root, including S3-compatible endpoints via `MLFLOW_S3_ENDPOINT_URL` [CITED: mlflow.org/docs/latest/ml/tracking/artifact-stores/, accessed 2026-09-08] | **Only if the ADR selects Option (ii)/(iii)** (OTC OBS as the MLflow artifact store) — not needed for Part A hygiene work, which stays on the existing local `file://` artifact root |

No new packages are required for the Part A engineering-hygiene work (lineage, gate, freeze,
model card, CI). `boto3` is the only candidate new dependency, and it is gated entirely behind
the ADR's Option (ii)/(iii) — see Package Legitimacy Audit below for a pre-cleared verdict so
the planner does not need to re-run the gate later if the ADR selects that path.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `mlflow.xgboost.log_model` + registry aliases (current) | Weights & Biases (W&B) | Better UI out of the box, but a SaaS dependency this project has deliberately avoided (no cloud footprint beyond OTC OBS for CV bundles); would also mean re-writing every `registry.py`/`mlflow_store.py` call this phase should instead be extending |
| MLflow (current) | ZenML / Metaflow (full pipeline orchestrators) | Both subsume MLflow's tracking role inside a heavier pipeline-DAG abstraction; neither solves anything this project's actual pain points (manual promotion, missing lineage, no gate) need — see Q9 below |
| A hand-rolled scalar calibration metric (proposed) | `sklearn.calibration.CalibrationDisplay`'s built-in scoring, or a dedicated calibration library (e.g. `netcal`) | A dedicated library is overkill for one summary number derived from data the code already computes (`evaluate.reliability_curves`); adding a library here would violate the project's own "Don't Hand-Roll" discipline in reverse — this is the *simple* case a library would be over-engineering |

**Installation:**
```bash
# Part A hygiene work: no install needed.
# Only if the ADR selects an OTC-OBS-backed MLflow artifact store:
uv add boto3
```

**Version verification:** `mlflow==3.15.1` and `botocore==1.43.56` verified live in this
session (`uv run python -c "import mlflow; print(mlflow.__version__)"`, `grep boto uv.lock`).
`boto3`'s exact compatible pin was not verified this session (no network `pip install` was run
in the sandboxed research environment) — flag as `[ASSUMED]`, verify with `uv add boto3` at
implementation time if the ADR selects an OTC-OBS artifact store.

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `boto3` | PyPI | 14+ years (AWS SDK, first released 2011) [ASSUMED — general knowledge, not independently re-verified this session] | Extremely high (top-20 PyPI package by download volume) [ASSUMED] | github.com/boto/boto3 | `[OK]` (verified live this session: `slopcheck install boto3` → `1 OK`) | Approved, conditional on the ADR selecting an OTC-OBS-backed MLflow artifact store — do not install for Part A hygiene work |

**Packages removed due to slopcheck [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** none.

No other new packages are proposed by this research. `slopcheck` itself was already present in
this environment (`/Users/clohr/.local/bin/slopcheck`) and ran successfully against the PyPI
registry for `boto3`.

## Architecture Patterns

### System Architecture Diagram — lineage + gate + promotion flow (Part A)

```
ffep ingest                     ffep freeze-corpus (NEW)
  │                                │
  ▼                                ▼
plays.parquet ──────────────► freeze manifest (dated, fingerprinted)
  │                                │  data/reference/corpus_freeze/<date>_<fingerprint>.json
  │                                │
  ▼                                │
ffep train ◄─── reads freeze ref ──┘  (--freeze <path>, or resolves latest by default)
  │
  ├─ _train() [model/train.py]
  │    ├─ existing: training_data_sha256, cv_scheme, n_folds, LOGO metrics
  │    ├─ NEW: git_commit (git rev-parse HEAD, lifted from hc_corpus_ablation.py)
  │    ├─ NEW: corpus_fingerprint (from the referenced freeze manifest, not recomputed)
  │    ├─ NEW: per_tier_logloss_* metrics (lifted from build_tier_metrics)
  │    ├─ NEW: no_play_share metric (lifted from report_no_play_rows)
  │    └─ NEW: calibration_max_deviation_<class> metric (new, from reliability_curves output)
  │
  ▼
MLflow run (registered as a new ep_model/wp_model version, NOT champion)
  │
  ▼
ffep promote --model ep [--force --reason "..."]
  │
  ├─ existing: resolves target run (latest FINISHED, or --run <id>)
  ├─ NEW: gate check — reads run.data.metrics against [promotion_gate] thresholds in ffep.toml
  │        ├─ beats champion on logo_mlogloss/logo_logloss?          (compare vs current champion run's metric)
  │        ├─ beats naive baseline?                                   (logloss_improvement > 0, already logged)
  │        ├─ calibration_max_deviation_* within tolerance?           (NEW metric, threshold from config)
  │        └─ no_play_share under threshold?                          (NEW metric, threshold from config)
  ├─ PASS → registry.promote() moves the champion alias (existing code, unchanged)
  ├─ FAIL, no --force → raises, alias untouched, exit 1
  └─ FAIL + --force --reason "<text>" → alias moves anyway, reason logged as a run tag on the
       PROMOTED run (`promotion_override_reason`) — itself queryable/auditable in MLflow, never
       a silent bypass
```

A reader can trace the primary use case (a new training run becoming the champion) end to end:
freeze → train (with lineage) → promote (gated, with an auditable escape hatch) → `ffep score`
resolves the new champion via the unchanged `registry.resolve_champion`.

### Recommended Project Structure (new/changed files only)
```
src/flag_football_ep/
├── model/
│   ├── train.py           # MODIFIED: _log_run gains git_commit, corpus_fingerprint,
│   │                       #   per-tier metrics, no_play_share, calibration_max_deviation_*
│   ├── evaluate.py        # MODIFIED: new calibration_max_deviation(curves) -> dict[str, float]
│   ├── freeze.py          # NEW: build_freeze_manifest(plays, config) -> FreezeManifest;
│   │                       #   write_freeze_manifest / load_freeze_manifest
│   ├── gate.py             # NEW: evaluate_gate(run, champion_run, thresholds) -> GateResult
│   └── registry.py        # MODIFIED (small): promote() takes an optional GateResult / force+reason
├── cli.py                  # MODIFIED: `ffep freeze-corpus` (new command),
│                             #   `ffep promote --force --reason` (new options)
data/reference/
└── corpus_freeze/          # NEW: dated, fingerprinted freeze manifests (JSON), committed
docs/
├── epa-modellkarte.md      # Referenced by CONTEXT as "being written today" but NOT present
│                             #   in the repo as of this research — see Open Questions
└── adr/
    └── 0001-modell-plattform.md   # NEW: the ADR this phase requires
.github/workflows/
└── ci.yml                  # NEW: no workflow directory exists in the repo yet (verified:
                              #   `ls .github/workflows` → not found)
```

### Pattern 1: Lift lineage capture from the ablation script into the shared `_log_run` path
**What:** `scripts/hc_corpus_ablation.py`'s uncommitted diff already implements exactly the two
functions this phase's "lineage on every run" requirement needs — `compute_corpus_fingerprint`
(SHA-256 over the sorted `(game_id, play_id, source)` key set) and `git_commit_sha` (subprocess
`git rev-parse HEAD`, with an `"unknown"` fallback if git is unavailable). Both are logged as
MLflow **params** (not tags) on each ablation run.
**When to use:** Move both functions (or equivalents) into `model/train.py` and call them from
inside `_train`, so every `ffep train` run gets them — not just runs launched through the
one-off ablation script.
**Why this is the smallest change, not a rewrite:** the pattern, the hashing scheme, and the
subprocess-fallback discipline are already written and already tested in this exact codebase;
this is relocation plus a general call site, not new design work.
```python
# Source: scripts/hc_corpus_ablation.py (uncommitted working-tree diff, read this session
# via `git diff scripts/hc_corpus_ablation.py`)
def compute_corpus_fingerprint(plays: pl.DataFrame) -> str:
    keys = plays.select("game_id", "play_id", "source").sort(["game_id", "play_id", "source"])
    return hashlib.sha256(keys.write_csv().encode("utf-8")).hexdigest()

def git_commit_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT,
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
```

### Pattern 2: MLflow's automatic git-commit tag is unreliable for this project's entry point
**What:** MLflow auto-populates an `mlflow.source.git.commit` **tag** (not param) on a run when
it can detect the run's source file lives inside a git working tree. This fired for
`scripts/hc_corpus_ablation.py` (invoked as `python scripts/hc_corpus_ablation.py`) but not for
any `ffep train` run (invoked via the installed `.venv/bin/ffep` console-script entry point) —
verified live this session by reading both runs' tags directly from the project's own
`mlruns/mlflow.db`.
**When to use:** Never rely on this auto-tag as the lineage source for `ffep`-invoked commands.
Always capture git commit explicitly (Pattern 1) inside `_log_run`, which runs regardless of
invocation path.
**Example (live verification, this session):**
```python
# Source: live query against this repo's own sqlite:///mlruns/mlflow.db, this session
run = mlflow.get_run("5e8ec9573e774ebaa20c9694c6ae15bb")  # current EP champion (v1)
run.data.tags
# {'mlflow.user': 'clohr', 'mlflow.source.name': '/…/.venv/bin/ffep',
#  'mlflow.source.type': 'LOCAL', 'mlflow.runName': 'classy-rat-100', 'phase': '01.3'}
# -- no 'mlflow.source.git.commit' key at all.
```

### Pattern 3: Registered-model versions already exist for the gate to compare against
**What:** `registry.promote()` and `resolve_champion()` are already alias-based
(`champion` alias on a registered model, moved explicitly, never destroying prior versions).
`MlflowClient().search_model_versions(f"run_id='{run_id}'")` and
`MlflowClient().get_model_version_by_alias(name, "champion")` are both already used in
`registry.py` and both are exactly what a gate needs to fetch "the currently promoted run's
metrics" for comparison against "the candidate run's metrics."
**When to use:** The gate's "beats champion" check should call `resolve_champion` (existing,
unmodified) to get the current champion's `run_id`, then `mlflow.get_run(champion_run_id).data.metrics`
to read its `logo_mlogloss`/`logo_logloss` — no new registry API surface needed.
```python
# Source: src/flag_football_ep/model/registry.py (existing, read this session)
def resolve_champion(name: str, config: Config) -> str:
    """Return the run id of the version currently aliased `champion` under `name`."""
    ...
    mv = MlflowClient().get_model_version_by_alias(name, CHAMPION_ALIAS)
    return mv.run_id
```

### Pattern 4: `Config` dataclasses already have a "new optional table with a safe default" convention
**What:** `Paths.raw_hc_files: Path = Path("data/raw/hc_files")` was added in M3-1 as an
optional field with a default, specifically so pre-existing `ffep.toml` files (and pre-existing
test fixture TOMLs) keep loading without an edit. The same pattern applies to a new
`[promotion_gate]` table: give every threshold a sane default in the dataclass so a project that
hasn't opted into the gate yet doesn't break.
**When to use:** Adding `GateThresholds` to `config.py` for the promotion-gate thresholds.
```python
# Source: src/flag_football_ep/config.py (existing pattern, read this session)
@dataclass(frozen=True)
class Paths:
    ...
    # M3-01: optional, resolved outside _PATH_KEYS/_key() below so that a config written
    # before M3 (missing this key entirely) keeps loading and no pre-existing test fixture
    # TOML needs an edit.
    raw_hc_files: Path = Path("data/raw/hc_files")
```

### Anti-Patterns to Avoid
- **Re-deriving lineage from logs or file mtimes after the fact:** the corpus fingerprint and
  git commit must be captured *inside* the training run at fit time (Pattern 1) — reconstructing
  them later from `git log` timestamps or file modification times is unreliable once multiple
  ingests/retrains have happened on the same machine.
- **A gate that reads CSVs instead of MLflow metrics:** `data/reference/epa_refinement/*.csv`
  is a phase-M3-2-specific, dated, ad-hoc output shape — a general `ffep promote` gate that
  depends on it would break the moment that directory's naming convention changes (it already
  has, mid-phase: see the new `data/reference/epa_refinement/2026-09-08/` subdirectory this
  session found on disk, sibling to the un-dated original). Metrics belong on the run.
- **Treating `training_data_sha256` as the corpus fingerprint:** it hashes one model's
  post-`drop_nulls()` training frame, which differs between EP and WP and between arms by
  design (`model/train.py`'s own docstring: "must be computed on the full training corpus,
  never per game or on any subset with no variation" — referring to sample weights, but the
  same logic makes `training_data_sha256` model-specific). The freeze fingerprint must hash the
  raw ingested corpus once, shared by every model trained against that freeze.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Model versioning / rollback | A custom `models/ep_model_v{n}.pkl` naming scheme | MLflow's registered-model versions (already in use) — every `_log_run(register_as=...)` call already produces a strictly-increasing version, no version ever overwritten | This is exactly what `registry.register_production_model`'s docstring already documents; re-inventing it would duplicate working code |
| Calibration curve computation | Hand-rolled binning/reliability logic | `sklearn.calibration.calibration_curve` (already used in `evaluate.reliability_curves`) | Explicitly called out in the existing code's own docstring: "Never hand-rolls the binning … `calibration_curve` already handles empty and degenerate bins" |
| S3-compatible object storage client (if ADR selects OTC OBS) | A custom HTTP client against the OBS API | `boto3`'s `S3ArtifactRepository` (MLflow built-in) with `MLFLOW_S3_ENDPOINT_URL` pointed at `obs.eu-de.otc.t-systems.com`, or `s3fs`/`dvc-s3` (already a transitive dependency via the `versioning` extras) | The project already has a working, tested OTC-OBS-compatible S3 client pattern in `cv/bundle.py` (`import s3fs; s3fs.S3FileSystem(...)`) — reuse the same endpoint/credential-resolution convention rather than building a second one |
| Feature store (if scale eventually warrants it) | A custom online/offline feature-serving layer | Feast, once ≥3 of the 5 standard trigger signals are present (see Q7) | Feature stores solve training-serving skew and cross-team feature reuse — both solved problems this project doesn't have yet; building a bespoke one would be solving a problem nobody has |

**Key insight:** every "Don't Hand-Roll" item in this phase already has a working, in-repo
precedent (MLflow registry, `calibration_curve`, `s3fs`) — this phase is disproportionately
about *reusing* existing project patterns more completely, not introducing new library
dependencies.

## Common Pitfalls

### Pitfall 1: Assuming MLflow auto-captures git lineage for every run
**What goes wrong:** A developer assumes "MLflow logs the git commit automatically, nothing to
build" because that's true for scripts invoked as `python scripts/foo.py`. It is silently false
for anything invoked through the installed `ffep` console-script entry point — which is every
production training run this project has ever logged.
**Why it happens:** MLflow's git-detection inspects the run's source-file path; a venv
console-script wrapper (`.venv/bin/ffep`) does not resolve to a `.py` path inside the repo.
**How to avoid:** Explicit capture inside `_log_run` (Pattern 1/2 above) — never rely on the
auto-tag for anything invoked via `ffep <command>`.
**Warning signs:** `mlflow.get_run(run_id).data.tags` has no `mlflow.source.git.commit` key —
verified this is the case for the current champion run in this exact repo.

### Pitfall 2: A gate that compares "beats champion" using the wrong metric name across models
**What goes wrong:** EP logs `logo_mlogloss`/`naive_mlogloss`; WP logs `logo_logloss`/
`naive_logloss` (different metric names, per `metric_name`/`naive_metric_name` in `_train`'s
existing params). A gate implementation that hard-codes one metric name for both models will
silently pass or fail the wrong model.
**Why it happens:** `_train`'s `metric_name` parameter is model-specific by design
(`model/train.py::train_ep`/`train_wp`'s call sites pass `"logo_mlogloss"`/`"logo_logloss"`
respectively) — this is intentional, not an oversight, but a gate reading metrics generically
needs to resolve the right key per model prefix.
**How to avoid:** Resolve the metric key from the same `metric_name`/`naive_metric_name`
convention `_train` already uses, keyed by model prefix, rather than a single hard-coded string.
**Warning signs:** A gate that "always fails WP" or "always passes EP regardless of quality" is
almost certainly reading the wrong metric key.

### Pitfall 3: Building the freeze manifest from `data/raw/*` file mtimes
**What goes wrong:** IFAF and HC-workbook source directories carry no in-band "fetched at"
metadata (verified: no `fetched_at`/`snapshot_date` field exists anywhere in
`fetch/ifaf.py`/`ingest/hc_workbook.py`). A freeze manifest that reads filesystem mtimes for
"snapshot date" will silently report the wrong date the moment a file is touched (e.g. `git
checkout`, backup restore, `rsync` without `-t`) without changing content.
**Why it happens:** No fetch script currently writes an explicit sidecar recording when a
snapshot was taken.
**How to avoid:** Either (a) accept mtime as a best-effort, clearly-labelled approximation for
this phase (documented as such in the manifest, not silently treated as ground truth), or (b)
add a small `_fetched_at.json` sidecar write to `fetch/ifaf.py`/the HC-workbook ingest path — a
larger, separate change this phase's CONTEXT does not obviously scope in. Recommend (a) for this
phase, with (b) flagged as a follow-up.
**Warning signs:** Two freeze manifests taken on different days report the same "snapshot date"
for a source that was actually re-fetched between them, or vice versa.

### Pitfall 4: Logging `--force --reason` as a CLI echo instead of an MLflow tag
**What goes wrong:** CONTEXT requires the escape hatch to be "itself logged." If `--force
--reason "..."` only prints to stdout, the audit trail is lost the moment the terminal scrollback
is gone.
**Why it happens:** It's the easy, obvious implementation — `typer.echo(...)` already exists in
every other CLI command in this codebase as the default way to report outcomes.
**How to avoid:** Set the reason as an MLflow **tag** on the promoted run
(`MlflowClient().set_tag(run_id, "promotion_override_reason", reason)`) using the exact same
`client.set_tag(...)` call already used in `scripts/hc_corpus_ablation.py::run_arm` for
`corpus_arm`/`gsd_phase`/`plan` tags — so the override is queryable in the MLflow UI months
later, not just in a terminal that has since closed.
**Warning signs:** `mlflow ui` shows a promoted run with no visible reason for why it was
promoted despite failing the gate.

### Pitfall 5: Assuming the champion-promotion decision this phase depends on has already happened
**What goes wrong:** CONTEXT's Status line says this phase is "Ready for research/planning
(execution after the M3-2 re-run of 2026-09-08 lands and the champion decision is taken)" — but
a live query of the project's own MLflow store (this session) shows the champion alias for both
`ep_model` and `wp_model` still points at version 1, the original phase-1.3 run, and
`docs/epa-refinement-2026-10.md`'s own Nachtrag section (dated 2026-09-08, same day as this
research) explicitly states "Kein `ffep promote`-Aufruf wurde in dieser Nachtrag-Sektion
ausgeführt; der Champion ist unverändert." Planning this phase's model-card content around "the
current champion" without re-checking this at execution time will produce a model card that
describes the wrong run.
**Why it happens:** The M3-2 re-run "landing" and the champion decision being "taken" are two
different, sequential events — the re-run has landed (the data is measured and documented), the
decision has not.
**How to avoid:** Treat the champion-promotion decision as an explicit precondition/checkpoint
for this phase's execution, not an assumption baked into any plan step. See Open Questions.
**Warning signs:** A model card or ADR draft that cites run ids from the with-HC arm as "the
production model" when `resolve_champion` still returns the without-HC-era run id.

## Code Examples

### Reading a run's metrics for gate evaluation (existing MLflow API, no new dependency)
```python
# Source: mlflow>=3.15 public API, pattern already used in registry.py's client calls
from mlflow.tracking import MlflowClient

client = MlflowClient()
run = client.get_run(candidate_run_id)
candidate_metric = run.data.metrics["logo_mlogloss"]   # or "logo_logloss" for WP -- resolve by model prefix
naive_metric = run.data.metrics["naive_mlogloss"]
improvement = run.data.metrics["logloss_improvement"]  # already logged by _train, no change needed
```

### Setting an auditable override reason on promotion (existing `set_tag` pattern)
```python
# Source: scripts/hc_corpus_ablation.py::run_arm (existing pattern in this codebase, read
# this session) -- client.set_tag(run_id, "corpus_arm", arm_name) is the precedent for
# tagging a run with structured, queryable metadata after the fact.
client.set_tag(promoted_run_id, "promotion_override_reason", reason)
client.set_tag(promoted_run_id, "promotion_override_by", os.environ.get("USER", "unknown"))
```

### MLflow UI, local, against this project's actual store (already documented, unchanged)
```bash
# Source: docs/model-training.md section 2 (existing project doc, read this session)
mlflow ui --backend-store-uri sqlite:///$(pwd)/mlruns/mlflow.db
# Open http://127.0.0.1:5000 -- registered models, aliases, and per-run params/metrics/
# artifacts (reliability_{ep,wp}.png, per_source_metrics.md) are all already browsable here
# with zero new code. This IS the "MLflow UI for a coach" deliverable's engine; what this
# phase adds is a German how-to doc pointing at it, and possibly a thin model-card page for
# the parts of this (registered-model list, aliases, headline numbers) that don't need the
# full MLflow UI's metric-table depth.
```

### S3-compatible artifact store, OTC OBS endpoint (only if ADR selects Option ii/iii)
```bash
# Source: mlflow.org/docs/latest/ml/tracking/artifact-stores/ (accessed 2026-09-08) + this
# project's existing OTC OBS endpoint convention (docs/hackathon-otc-upload.md, ffep.toml
# [cv] section: dvc_remote_endpoint = "https://obs.eu-de.otc.t-systems.com")
export MLFLOW_S3_ENDPOINT_URL="https://obs.eu-de.otc.t-systems.com"
export AWS_ACCESS_KEY_ID="$OTC_OBS_ACCESS_KEY_ID"      # same env-var mapping convention
export AWS_SECRET_ACCESS_KEY="$OTC_OBS_SECRET_ACCESS_KEY"  # already used in hackathon-otc-upload.md's Weg B fallback
mlflow server --backend-store-uri postgresql://... \
  --default-artifact-root s3://ffep-mlflow-artifacts/  # bucket TBD, same OTC OBS project
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| No lineage on training runs beyond `training_data_sha256` | Lineage capture (`corpus_fingerprint`, `git_commit`) exists but is scoped to one ad-hoc script, not the production `ffep train` path | Uncommitted working-tree change, 2026-09-08 (same day as this research) | This phase's smallest concrete task is generalizing already-written code, not designing from scratch |
| Promotion decided entirely by human judgement reading the MLflow UI (`docs/model-training.md` section 3) | This phase adds a mechanical pre-check (the gate) that a human can still override with `--force --reason` | This phase (proposed) | Matches CONTEXT's explicit requirement — the gate replaces "judgement by hand" as the *default* path, not judgement itself, which remains available via the escape hatch |
| Champion promotion checked against nothing but "does the reviewer like the numbers" | Beats-champion + beats-naive-baseline + calibration + no-play-share, all as MLflow metrics | This phase (proposed) | The four criteria named in CONTEXT are all derivable from data the codebase already computes somewhere (LOGO metrics, `reliability_curves`, `no_play_rows.csv`'s underlying computation) — none require new statistical machinery |

**Deprecated/outdated:** Nothing in this phase deprecates prior work — MLflow's local
`file:`/SQLite tracking store (the phase-1.3 decision, `mlflow_store.py`) remains correct for
this phase's scope; the ADR's job is to decide *when*, not *whether*, that changes.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `boto3`'s exact version compatible with the already-locked `botocore==1.43.56` was not independently verified via `uv add boto3` this session (network install not run in the sandboxed research environment) | Standard Stack | Low — `boto3`/`botocore` version pairing is a well-known, low-risk compatibility question the planner can resolve with one `uv add boto3` at implementation time, gated behind the ADR outcome anyway |
| A2 | OTC ECS/CCE/OBS costs are order-of-magnitude estimates from web search, not a price-calculator quote — OTC publishes a calculator, not fixed list prices, for these SKUs [CITED: t-cloud-public.com/en/prices/pricing-models/computing-container, accessed 2026-09-08, page explicitly deflects to a calculator] | Q6/Q9 findings below | Medium — if the ADR decision hinges on a specific cost threshold, the user should run the actual OTC price calculator before committing, not rely on this research's order-of-magnitude figures |
| A3 | The mtime-based "snapshot date" fallback proposed for the corpus freeze manifest (Pitfall 3) has not been validated against real IFAF/HC-workbook file histories in this session — it is a reasoned best-effort proposal, not a tested mechanism | Common Pitfalls / Q3 | Low-medium — worst case, a freeze manifest's snapshot-date field is imprecise for one phase, correctable in a follow-up without touching any trained model's validity |
| A4 | `boto3`'s package age/download-volume figures in the Package Legitimacy Audit table are general knowledge, not re-verified via PyPI's own metadata API this session (only the `slopcheck [OK]` verdict was independently re-run) | Package Legitimacy Audit | Very low — `boto3` is an unambiguous, universally-known AWS SDK package; risk of this being wrong is negligible |

## Open Questions

1. **Has the champion-promotion decision for the 2026-09-08 IFAF-corrected re-run actually
   been taken, and by whom?**
   - What we know: as of this research, the champion alias for both `ep_model` and `wp_model`
     still points at version 1 (verified live against the project's own MLflow store), and
     `docs/epa-refinement-2026-10.md`'s own Nachtrag section says explicitly that no promote
     call was made in that section.
   - What's unclear: whether the user intends to promote the `with_hc` (or the newer IFAF-
     corrected) run before this phase executes, or whether this phase's model card/ADR should
     describe the current (v1) champion as-is and note the pending decision.
   - Recommendation: treat this as a blocking checkpoint for the planner — the first plan in
     this phase's Wave 1 should either (a) explicitly defer to the user's M3-02-08 checkpoint
     completing first, or (b) build the model card/gate generically enough that it describes
     "whichever run is currently aliased `champion`" rather than hard-coding a specific run id.

2. **Does `docs/epa-modellkarte.md` already exist, or is it still to be written?**
   - What we know: CONTEXT (dated 2026-09-08) describes it as "being written today" and
     references it as a template ("Model card is coach-readable German (like
     `docs/epa-modellkarte.md` from the 2026-09-08 re-run)"). A repo-wide search this session
     (`find . -iname "*modellkarte*"`) found no such file anywhere in the working tree.
   - What's unclear: whether it is being written concurrently by the user/another session and
     will land before this phase's planning, or whether CONTEXT's phrasing was aspirational and
     this phase is actually the one that creates it.
   - Recommendation: the planner should not assume a template file exists at plan-write time;
     re-check immediately before planning, and if still absent, treat "define the model card's
     German structure from scratch" as this phase's own Wave 1 task rather than "port an
     existing template."

3. **Should the freeze manifest's snapshot-date field for IFAF/HC-workbook sources be
   best-effort (file mtime) or require a new sidecar write in the fetch layer?**
   - What we know: no in-band snapshot-date metadata exists today for either source (Pitfall 3).
   - What's unclear: whether the ~2-line fetch-layer change (writing a `_fetched_at.json`
     sidecar) is in scope for this phase or a follow-up, given CONTEXT scopes this phase to "not
     new features/metrics" but a freeze manifest is arguably infrastructure, not a feature.
   - Recommendation: Claude's discretion per CONTEXT — recommend the best-effort mtime approach
     for this phase (Pitfall 3 option a), explicitly labelled as approximate in the manifest,
     with the sidecar write flagged as a fast-follow.

4. **What exact OTC OBS bucket/pricing tier would host an MLflow artifact store, and has
   anyone priced it against the OTC calculator (not this research's order-of-magnitude
   estimates)?**
   - What we know: the project already has one OTC OBS bucket in flight for the CV
     dataset-versioning DVC remote (`ffep.toml`'s `[cv]` section, currently a `PLACEHOLDER`
     bucket name per plan 02.2-20's pending task) — a second bucket for MLflow artifacts could
     plausibly live in the same OTC project, sharing the endpoint but not the bucket.
   - What's unclear: real monthly cost at this project's actual artifact volume (reliability
     PNGs, `per_source_metrics.md`, model pickles — all small; nothing like the CV dataset's
     multi-GB bundles).
   - Recommendation: not a blocker for the ADR's *decision* (the order-of-magnitude is small
     regardless — MLflow artifacts here are kilobytes to low megabytes per run, nothing like
     the CV bundle sizes that motivated the existing OBS integration), but the user should run
     the actual calculator before signing the ADR's cost line if that line needs a number rather
     than "small."

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `mlflow` (Python package) | All of Part A | ✓ | 3.15.1 | — |
| `git` CLI | Lineage capture (`git rev-parse HEAD`) | ✓ (repo is a git working tree; used throughout this session) | — | Falls back to `"unknown"` string already, per the existing `git_commit_sha()` pattern — never fails a training run |
| `slopcheck` CLI | Package Legitimacy Audit | ✓ (pre-installed at `/Users/clohr/.local/bin/slopcheck`) | not queried | — |
| GitHub Actions (`.github/workflows/`) | CI on fixtures | ✗ (no workflow directory exists yet — verified `ls .github/workflows` → not found) | — | Local pre-push git hook (`.git/hooks/pre-push`) as a fallback if GitHub Actions minutes/setup is out of scope for this phase — the fixture-based test suite (`tests/test_model_train.py` + `tests/conftest.py`) already runs identically in either environment, no CI-specific code needed |
| `boto3` (Python package) | ADR Option (ii)/(iii) only, if selected | ✗ (not currently installed or locked) | — | Not needed unless the ADR selects an OTC-OBS-backed artifact store; `slopcheck`-cleared and ready to add (`uv add boto3`) when/if needed |
| OTC OBS credentials (`OTC_OBS_ACCESS_KEY_ID`/`OTC_OBS_SECRET_ACCESS_KEY`) | ADR Option (ii)/(iii) only | ✗ (per `docs/hackathon-otc-upload.md`: "liegen in dieser Umgebung noch nicht vor — voraussichtlich für einen längeren Zeitraum nicht") | — | Not needed for this phase's code (Part A stays local); relevant only if the ADR's migration path is actually executed later, and that execution is explicitly out of scope for this phase (ADR is a decision record, not an implementation) |
| Docker / docker-compose | ADR Option (ii) only, if selected as a *future* migration | not checked this session (irrelevant to this phase's actual deliverables — the ADR describes, does not implement, the migration) | — | — |

**Missing dependencies with no fallback:** none that block this phase's actual deliverables —
every genuinely missing piece (GitHub Actions workflows, `boto3`, OTC OBS credentials, Docker)
is either optional (has a documented fallback) or belongs to a *future* migration this phase
only has to describe in the ADR, not execute.

**Missing dependencies with fallback:** GitHub Actions → local pre-push hook;
`boto3`/OTC-OBS-artifact-store → stays out of scope unless the ADR is later executed.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest ≥8 (pyproject `[dependency-groups].dev`) |
| Config file | `pyproject.toml`'s `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `addopts = "-q"`) |
| Quick run command | `uv run pytest -q tests/test_model_train.py tests/test_model_registry.py` |
| Full suite command | `uv run pytest -q` (97 test files under `tests/`, verified this session; full-suite wall time not re-measured this session — training-touching tests use tmp-path-scoped synthetic corpora per `tests/conftest.py`, deliberately small "so the suite stays fast" per that file's own docstring) |

### Phase Requirements → Test Map
No `REQ-M3-05-*` identifiers exist in `.planning/ROADMAP.md`'s M3-5 block (unlike other M3
phases, this entry has no `Requirements:` line) — this phase's CONTEXT decisions stand in for
requirement IDs. Mapping CONTEXT's named deliverables to tests:

| Deliverable | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| Lineage on every run | `_train`'s logged params include `git_commit`, `corpus_fingerprint` | unit | `pytest tests/test_model_train.py::test_train_ep_logs_lineage_params -x` | ❌ Wave 0 (new test) |
| Promotion gate | `ffep promote` refuses without `--force` when a candidate fails a threshold, succeeds with `--force --reason` and tags the override | unit + CLI smoke | `pytest tests/test_model_registry.py::test_gate_blocks_below_threshold -x` (new) + existing `tests/test_cli_smoke.py` pattern extended | ❌ Wave 0 (new test) |
| Corpus freeze manifest | `ffep freeze-corpus` writes a dated, fingerprinted manifest; `ffep train --freeze <path>` references it | unit | `pytest tests/test_model_freeze.py -x` (new file) | ❌ Wave 0 (new file) |
| Calibration scalar metric | `evaluate.calibration_max_deviation(curves)` returns a per-class dict, logged as MLflow metrics | unit | `pytest tests/test_model_evaluate.py::test_calibration_max_deviation -x` (extend existing `tests/test_model_evaluate.py` if present, else new) | check — not confirmed to exist this session |
| CI on fixtures | The existing `tests/test_model_train.py`/`tests/conftest.py` synthetic-corpus suite runs green in a clean checkout via GitHub Actions | integration (workflow-level) | `.github/workflows/ci.yml` running `uv run pytest -q` | ❌ Wave 0 (new workflow file) |

### Sampling Rate
- **Per task commit:** `uv run pytest -q tests/test_model_train.py tests/test_model_registry.py tests/test_model_evaluate.py` (fast, fixture-based, no real corpus needed)
- **Per wave merge:** `uv run pytest -q` (full suite)
- **Phase gate:** Full suite green before `/gsd:verify-work`, plus a live smoke check against the real `mlruns/mlflow.db` (not just fixtures) for the gate and freeze commands, since this phase's whole point is trustworthy behaviour against the actual production store

### Wave 0 Gaps
- [ ] `tests/test_model_train.py` — extend with lineage-param assertions (git_commit, corpus_fingerprint)
- [ ] `tests/test_model_registry.py` — extend with gate-blocks/gate-passes/`--force` override tests
- [ ] `tests/test_model_freeze.py` — new file, covers `ffep freeze-corpus`
- [ ] Confirm whether `tests/test_model_evaluate.py` already exists (not verified this session — grep for it before assuming it needs creating vs. extending)
- [ ] `.github/workflows/ci.yml` — new, none exists

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No, for this phase's actual code (local, single-user, `mlflow ui` on loopback) — **Yes, conditionally**, for the ADR's Option (ii)/(iii) if later executed | MLflow basic HTTP auth (`pip install 'mlflow[auth]'`, `mlflow server --app-name basic-auth`) [CITED: mlflow.org/docs/latest/self-hosting/security/basic-http-auth/, accessed 2026-09-08] — **not enabled by default**, and ships with a default `admin`/`password1234` credential that must be rotated immediately if ever enabled |
| V3 Session Management | No (no web session logic in this phase's scope) | — |
| V4 Access Control | No for this phase; relevant to the ADR's future multi-tenant discussion (Q8) | Per-programme roster/PII boundaries via a future `programme_id`/`season` scope column, not implemented this phase |
| V5 Input Validation | Yes, extending existing patterns | `registry.py`'s existing `_validate_model_name`/`_validate_alias` regex-identifier gate is the precedent — any new CLI input (freeze manifest path, `--reason` string) should follow the same "reject anything outside a plain safe pattern before it reaches a URI/query interpolation" discipline already documented in `registry.py`'s own docstring (T-1.3-07) |
| V6 Cryptography | Yes (already satisfied) | SHA-256 (`hashlib.sha256`, stdlib) for both `training_data_sha256` (existing) and the new `corpus_fingerprint` — never hand-rolled, already the project's convention |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Unauthenticated MLflow tracking server exposed to a network (relevant only if/when the ADR's server-based option is executed) | Spoofing / Information Disclosure | Multiple CVEs disclosed in 2026 against unauthenticated MLflow servers (SSRF via webhook delivery, CVE-2026-64849; auth-bypass job endpoints, CVE-2026-0545; default-credential auth bypass, CVE-2026-2635) [CITED: github.com/advisories/GHSA-7gwp-5pfp-969j, sentinelone.com/vulnerability-database/cve-2026-0545, both accessed 2026-09-08] — the standard mitigation is to never expose an MLflow tracking server on a routable interface without basic auth enabled AND the default admin password rotated, matching this project's own existing discipline for `cvat_host` (`ffep.toml`'s comment: "must never bind a routable interface, because the footage it serves is PII") |
| `--reason` free-text injected into a shell/query without validation | Tampering | Log the reason as an MLflow tag value (parameter-bound API call, not string-interpolated into a URI) — MLflow's `set_tag` API takes the value as a normal argument, not part of a constructed query string, so this is already safe by construction if implemented via the existing `MlflowClient().set_tag(...)` pattern rather than any raw SQL/URI building |
| Freeze manifest or gate threshold file world-writable / silently tampered | Tampering | Freeze manifests should be committed to git (like `data/reference/epa_refinement/*.csv` already is) — git history is the tamper-evidence mechanism this project already relies on for every other reference dataset |

## Proposed Plan Breakdown (Waves)

**Wave 1 — Lineage + freeze foundation** *(no dependencies; unblocks Wave 2)*
- Lift `compute_corpus_fingerprint`/`git_commit_sha` from `scripts/hc_corpus_ablation.py`
  into `model/train.py::_log_run`, so every `ffep train` run gets both params regardless of
  entry point (Pattern 1/2). Add the new `calibration_max_deviation_<class>` scalar to
  `evaluate.py` and log it from `_train`. Generalize the ablation script's
  `build_tier_metrics`/`report_no_play_rows` computations into `_train`'s standard metric
  logging (per-tier log-loss, no-play share) so they land on every production run, not just
  ad-hoc ablation runs.
- `ffep freeze-corpus`: new `model/freeze.py` module, manifest schema (date, fingerprint, git
  commit, per-source row counts, accepted/quarantined game counts, best-effort per-source
  snapshot dates per Pitfall 3), `data/reference/corpus_freeze/` output, committed. `ffep
  train --freeze <path>` (optional, defaults to latest).

**Wave 2 — Promotion gate + CI** *(depends on Wave 1's new metrics existing to gate on)*
- `[promotion_gate]` table in `ffep.toml` (thresholds, following the `Paths`
  optional-field-with-default convention), `model/gate.py` (`evaluate_gate`), `ffep promote
  --force --reason` (gate check + auditable override tag per Pitfall 4).
- `.github/workflows/ci.yml` running the fixture-based suite (`-k "not cv and not
  hackathon"`) on push/PR; no secrets needed.

**Wave 3 — Coach-facing deliverables** *(depends on Open Question 1 — the pending
champion-promotion decision — being resolved, and benefits from Wave 1's lineage/gate data
existing to describe)*
- German model card (`docs/epa-modellkarte.md` if not already created elsewhere by the time
  this phase executes — re-check first, per Open Question 2) generated from MLflow +
  `data/reference/epa_refinement/*.csv`, with its own doc-guard test in the
  `tests/test_m3_epa_docs.py` style.
- German `mlflow ui` how-to doc for the HC session (what to show, what NOT to show per Q4).

**Wave 4 — Platform ADR** *(a document, not code — can start in parallel with Waves 1-3, but
benefits from citing Wave 1's concrete lineage work as evidence of what "the current setup"
actually supports)*
- `docs/adr/0001-modell-plattform.md`: the three named options plus the cost/burden/
  migration/breaks-first table (Q6), the feature-store non-decision (Q7), the multi-tenant
  data model recommendation (Q8), MLflow deployment realities (Q9), the staged recommendation
  and decide-with-user-vs-Claude split (Q10) — ends in a **non-autonomous, blocking user
  checkpoint**: the user signs the decision line.

Ordering note: Wave 4 does not block Waves 1-3 (the hygiene work is correct regardless of
which platform option is eventually chosen — it is Option (i)-native and Option (ii)/(iii)
would inherit it unchanged), but Wave 3's model card should ideally cite the ADR's outcome if
Wave 4 finishes first, since "which platform this model card's numbers live on" is a natural
one-sentence addition once decided.

## Sources

### Primary (HIGH confidence)
- `src/flag_football_ep/model/train.py`, `registry.py`, `mlflow_store.py`, `evaluate.py`, `experiments.py` — read in full this session
- `src/flag_football_ep/cli.py` — `train`/`promote`/`experiment`/`score` commands read in full this session
- `src/flag_football_ep/config.py` — `Paths`/`TrainConfig` dataclass patterns read this session
- `docs/model-training.md`, `docs/epa-refinement-2026-10.md` (including its 2026-09-08 Nachtrag) — read in full this session
- `.planning/phases/M3-05-epa-plattform/M3-05-CONTEXT.md`, `.planning/ROADMAP.md` (Milestone 3 section) — read in full this session
- `.planning/phases/M3-02-epa-refinement/M3-02-05-SUMMARY.md`, `M3-02-08-PLAN.md` — read this session
- `docs/hackathon-otc-upload.md`, `docs/hc-notes-2026-09-03.md`, `ffep.toml`, `pyproject.toml` — read in full this session
- Live query against this project's actual `sqlite:///mlruns/mlflow.db` (registered model versions, aliases, run params/tags/metrics) — executed this session via `uv run python -c "..."`
- `git diff scripts/hc_corpus_ablation.py`, `git status --short`, `git log --oneline` — executed this session against this repo's actual working tree
- `slopcheck install boto3` — executed live this session against the real PyPI registry

### Secondary (MEDIUM confidence)
- [MLflow Authentication with Username and Password](https://mlflow.org/docs/latest/self-hosting/security/basic-http-auth/) — accessed 2026-09-08, WebFetch-summarized
- [MLflow Artifact Stores](https://mlflow.org/docs/latest/ml/tracking/artifact-stores/) — accessed 2026-09-08, WebSearch-summarized, S3/`MLFLOW_S3_ENDPOINT_URL`/`boto3` claims cross-checked against the GitHub source file link in the same result set
- [Do You Need a Feature Store? Decision Framework for ML Teams](https://tacnode.io/post/do-you-need-a-feature-store) — accessed 2026-09-08, single-source framework (the "3 of 5 signals" heuristic), not cross-verified against a second independent source — treat the exact "3 of 5" threshold as indicative, not authoritative
- [Open Telekom Cloud / T Cloud Public — Pricing Models: Computing & Containers](https://www.t-cloud-public.com/en/prices/pricing-models/computing-container) — accessed 2026-09-08; confirms CCE is free up to 50 non-HA nodes and that ECS/OBS pricing is calculator-based, not published as fixed rates — no concrete monthly figure could be extracted

### Tertiary (LOW confidence)
- MLflow 2026 CVE summary (SSRF via webhook delivery CVE-2026-64849, job-endpoint auth bypass CVE-2026-0545, default-credential bypass CVE-2026-2635) — WebSearch result summary only, not independently read at the advisory source for full technical detail; sufficient to establish "unauthenticated MLflow servers have a real, current CVE history" as a directional fact, not sufficient to cite specific CVSS scores or exact affected-version ranges
  - [MLflow SSRF advisory](https://github.com/advisories/GHSA-7gwp-5pfp-969j)
  - [CVE-2026-0545 summary](https://www.sentinelone.com/vulnerability-database/cve-2026-0545/)

## Metadata

**Confidence breakdown:**
- Part A engineering hygiene (lineage, gate, freeze, CI): HIGH — every claim is grounded in code read this session or a live query against the project's actual MLflow store; the one live uncertainty (whether the champion-promotion decision has landed) was itself directly verified, not assumed.
- Part B platform ADR — qualitative recommendation (single machine now, staged migration triggers): MEDIUM — grounded in the project's actual current scale (verified: ~30k plays, batch-only, no real-time requirement, solo developer) and industry-standard decision frameworks for feature stores/Kubernetes, but the frameworks themselves come from a small number of web sources, not cross-verified across many independent voices.
- Part B platform ADR — cost figures: LOW — OTC does not publish fixed prices for the relevant SKUs; every cost claim in this document is qualitative ("small", "order of magnitude of a few tens of euros per month for a single small VM," by analogy to comparable cloud providers) rather than a verified quote, and is flagged as such throughout.

**Research date:** 2026-09-08
**Valid until:** 30 days for Part A (stable, repo-internal); 7 days for any specific OTC cost figure quoted informally in discussion (cloud pricing changes without notice) — but the ADR's *qualitative* recommendation (stay single-machine, defer Kubernetes/feature store) is stable regardless of exact pricing, since it does not depend on the precise number.
