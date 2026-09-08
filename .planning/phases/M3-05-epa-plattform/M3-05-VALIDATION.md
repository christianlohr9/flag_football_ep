---
phase: M3-05
slug: epa-plattform
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-09-08
---

# Phase M3-05 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Skeleton from M3-05-RESEARCH.md §Validation Architecture; per-plan map filled by the planner
> after the plan set was restructured (coach-facing quick win front-loaded to wave 1; freeze
> manifest and training lineage/calibration/tier/no-play metrics merged into one plan;
> promotion gate slimmed to gate-only; model-card generation split into a wave-1 first cut and
> a wave-4 regenerate-only pass).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, uv-managed) |
| **Config file** | pyproject.toml (`testpaths = ["tests"]`, `addopts = "-q"`) |
| **Quick run command** | wave 1: `uv run pytest tests/test_m3_epa_docs.py tests/test_model_freeze.py tests/test_model_train.py tests/test_model_evaluate.py -x -q` · wave 2: `uv run pytest tests/test_model_gate.py tests/test_model_registry.py -q` · wave 3: `uv run pytest tests/test_features_mutations.py tests/test_m3_epa_docs.py -x -q` · wave 4: `uv run pytest tests/test_m3_epa_docs.py -x -q` |
| **Full suite command** | `uv run pytest -q` |
| **Estimated runtime** | quick commands < 30 s each (all fixture-based); wave 3's real four-arm LOGO re-run (M3-05-07 task 1) is a separate, real-corpus training pass measured in minutes, not part of any quick command — see `docs/model-training.md` §1 for the ~50+ fold cost this project's LOGO protocol already carries |

---

## Sampling Rate

- **After every task commit:** the task's own `<verify><automated>` command (every auto task in
  this phase's 8 plans has one; the three checkpoint tasks — M3-05-01 T1, M3-05-05 T2,
  M3-05-07 T2 — have none by design, each immediately followed by an automated-verified task)
- **After every plan wave:** the wave's quick command above
- **Before `/gsd:verify-work`:** full suite green (`uv run pytest -q`)
- **Phase gate (before the champion-facing artifacts ship):** a live smoke check against the
  real `mlruns/mlflow.db` for `ffep freeze-corpus`, `ffep train --freeze`, `ffep promote
  [--force --reason]` and `scripts/render_model_card.py` — fixtures alone cannot prove these
  work against the actual production store, which is this phase's whole point (mirrors
  M3-05-RESEARCH.md's own Sampling Rate note)
- **Max feedback latency:** 30 seconds for the quick commands; the wave-3 real re-run and any
  real `ffep freeze-corpus`/model-card generation run are minutes-scale and explicitly excluded
  from the 30 s budget

---

## Per-Plan Verification Map

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|-------------------------|-----------|--------------------|-------------|--------|
| M3-05-01 T1 | 1 | PROD-01 | The champion-promotion decision left open by M3-02-08 is made explicitly by the user via checkpoint, never defaulted or inferred | manual | checkpoint (human-verify) — no automated gate by design; immediately followed by an automated doc gate | n/a | ⬜ |
| M3-05-01 T2 | 1 | PROD-01 | The resolved champion for both models matches exactly what the checkpoint answer said; `docs/epa-refinement-2026-10.md` records the decision, date and reason | integration + doc gate | `uv run pytest tests/test_m3_epa_docs.py -x -q` plus `git status --porcelain data/ src/ scripts/ tests/` empty | ✅ exists (extended) | ⬜ |
| M3-05-02 T1 | 1 | PROD-02 | Every number on the first-generated model card is read from the live MLflow registry or a committed CSV, never hand-typed; a metric this phase adds later degrades to a named "nicht verfuegbar" state rather than crashing or being silently omitted | unit + doc gate | `uv run pytest tests/test_m3_epa_docs.py -x -q` | ❌ Wave 0 (`scripts/render_model_card.py`) | ⬜ |
| M3-05-02 T2 | 1 | PROD-02 | The MLflow UI how-to cites the exact proven command and scopes what is coach-appropriate to show live versus what stays in the card | doc check | `test -f docs/mlflow-ui-howto.md && grep -q "mlflow ui" docs/mlflow-ui-howto.md` | ❌ Wave 0 (`docs/mlflow-ui-howto.md`) | ⬜ |
| M3-05-03 T1 | 1 | PROD-03 | `ffep freeze-corpus` writes a dated, fingerprinted manifest reproducing `scripts/hc_corpus_ablation.py`'s existing hashing scheme byte-for-byte; a repeat freeze on an unchanged corpus is idempotent | unit | `uv run pytest tests/test_model_freeze.py -x -q` | ❌ Wave 0 (`src/flag_football_ep/model/freeze.py`, `tests/test_model_freeze.py`) | ⬜ |
| M3-05-03 T2 | 1 | PROD-04 | Every `ffep train` run logs `git_commit`/`corpus_fingerprint`, distinct from `training_data_sha256`; `ffep train --freeze <path>` and default latest-manifest resolution both work without failing when no freeze exists yet | unit | `uv run pytest tests/test_model_train.py -x -q -k "lineage or git_commit or corpus_fingerprint or freeze"` | ✅ exists (extended) | ⬜ |
| M3-05-03 T3 | 1 | PROD-04 | Calibration, per-tier log-loss and no-play share are logged as standard MLflow metrics on every run, computed from data the run already builds (no second computation); a class with degenerate calibration bins does not crash | unit | `uv run pytest tests/test_model_evaluate.py tests/test_model_train.py -x -q` | ✅ exists (extended) | ⬜ |
| M3-05-04 T1 | 1 | PROD-05 | CI runs the fixture-based suite on push/PR with zero declared secrets and excludes the CV/hackathon suites that would need one | workflow validation | `test -f .github/workflows/ci.yml && grep -q "pytest" .github/workflows/ci.yml && grep -qv "SPORTAPP_API_KEY\|CPX_API_KEY\|OTC_OBS" .github/workflows/ci.yml && python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` | ❌ Wave 0 (`.github/workflows/ci.yml`) | ⬜ |
| M3-05-05 T1 | 1 | PROD-06 | The ADR draft covers all three named platform options plus the feature-store and multi-tenant questions with RESEARCH's cited evidence, cost figures flagged as order-of-magnitude where unverified | doc check | `test -f docs/adr/0001-modell-plattform.md && grep -q "Empfehlung" docs/adr/0001-modell-plattform.md && grep -q "Entscheidung" docs/adr/0001-modell-plattform.md` | ❌ Wave 0 (`docs/adr/0001-modell-plattform.md`) | ⬜ |
| M3-05-05 T2 | 1 | PROD-06 | The user signs the platform decision explicitly via checkpoint — never a Claude default | manual | checkpoint (human-verify) — no automated gate by design; immediately followed by an automated doc gate | n/a | ⬜ |
| M3-05-05 T3 | 1 | PROD-06 | The ADR carries a dated, explicit, signed decision line naming the chosen option, not a placeholder | doc gate | `(grep -q "^Entscheidung getroffen" docs/adr/0001-modell-plattform.md \|\| grep -qi "unterschrieben\|signiert\|freigegeben" docs/adr/0001-modell-plattform.md) && echo OK` — fixed 2026-09-08 to actually fail when neither pattern matches (previous form always exited 0) | n/a | ⬜ |
| M3-05-06 T1 | 2 | PROD-07 | `evaluate_gate` resolves the correct metric key per model (never hard-coded), treats "no champion yet" and "metric absent on an old run" as pass/skip rather than a silent pass | unit | `uv run pytest tests/test_model_gate.py -x -q` | ❌ Wave 0 (`src/flag_football_ep/model/gate.py`, `tests/test_model_gate.py`) | ⬜ |
| M3-05-06 T2 | 2 | PROD-07 | `ffep promote` refuses a failing candidate without `--force`, promotes and tags an auditable override with `--force --reason`, and rejects `--force` with an empty reason | unit + CLI smoke | `uv run pytest tests/test_model_registry.py -q && uv run ffep promote --help \| grep -q -- "--force"` | ✅ exists (extended: `cli.py`) | ⬜ |
| M3-05-07 T1 | 3 | PROD-08 | Every `play_type == "extra_point"` row is excluded from EP/WP training regardless of success; label derivation for non-extra-point rows is unaffected (regression guard) | unit | `uv run pytest tests/test_features_mutations.py -x -q -k "extra_point"` | ✅ exists (extended) | ⬜ |
| M3-05-07 T2 | 3 | PROD-08 | The user decides the promotion outcome after seeing the measured before/after numbers AND the gate's real printed verdict, never a guess about whether it would pass | manual | checkpoint (human-verify) — no automated gate by design; immediately followed by an automated doc gate | n/a | ⬜ |
| M3-05-07 T3 | 3 | PROD-08 | The document records the methodology change, the gate verdict and the promotion decision with its reason; the promotion decision was executed exactly as chosen; no file outside this plan's own `files_modified` changed | doc gate + integration | `uv run pytest tests/test_m3_epa_docs.py tests/test_features_mutations.py -x -q && test -z "$(git status --porcelain data/ src/ scripts/)"` | ✅ exists (extended) | ⬜ |
| M3-05-08 T1 | 4 | PROD-09 | The regenerated card reflects the post-M3-05-07 champion (not a stale M3-05-02 snapshot) with no code change beyond a genuine bug fix, if any; doc-guard coverage only grows | doc gate | `uv run pytest tests/test_m3_epa_docs.py -x -q` | ✅ exists (regenerated) | ⬜ |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Created inside the phase, in this order:

- `scripts/render_model_card.py` — plan M3-05-02 task 1. Reads the MLflow registry +
  `data/reference/epa_refinement/*/ablation_summary.csv`; degrades gracefully for any metric
  added by later plans in this phase.
- `docs/mlflow-ui-howto.md` — plan M3-05-02 task 2.
- `src/flag_football_ep/model/freeze.py` — plan M3-05-03 task 1. Reuses
  `scripts/hc_corpus_ablation.py`'s existing `compute_corpus_fingerprint`/`git_commit_sha`
  hashing scheme verbatim (copied in, not imported — `scripts/` is a driver, not a `src/`
  dependency).
- `tests/test_model_freeze.py` — plan M3-05-03 task 1.
- `data/reference/corpus_freeze/` — plan M3-05-03 task 1's real run. A committed, dated,
  fingerprinted manifest, like every other `data/reference/*` artifact in this project.
- `.github/workflows/ci.yml` — plan M3-05-04 task 1. No `.github/workflows/` directory exists
  in the repo yet.
- `docs/adr/0001-modell-plattform.md` — plan M3-05-05 task 1.
- `src/flag_football_ep/model/gate.py` — plan M3-05-06 task 1.
- `tests/test_model_gate.py` — plan M3-05-06 task 1.
- `tests/conftest.py` is NOT touched — owned by phase 01.2 plan 01; every new fixture-based test
  in this phase (`test_model_freeze.py`, `test_model_gate.py`, extensions to
  `test_model_train.py`/`test_model_evaluate.py`) reuses its existing `tmp_path`-scoped `Config`
  fixture and synthetic canonical corpus.
- Framework install: none. `mlflow`, `xgboost`, `pytest`, `polars` are all existing project
  dependencies (M3-05-RESEARCH §Standard Stack: no new packages required for Part A hygiene
  work; `boto3` stays gated behind the ADR's Option ii/iii, never installed by any plan in this
  set).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|--------------------|
| Whether the 2026-09-08 with-IFAF candidates or the 2026-09-04 without-IFAF candidates (or neither) become champion | PROD-01 | A real trade-off (IFAF coverage vs. a documented WP per-source regression) only the user can weigh | Plan M3-05-01 task 1: read `M3-02-RERUN-2026-09-08-SUMMARY.md`'s Before/After table, reply `2026-09-08` / `2026-09-04` / `none` |
| Which platform option (single machine / OTC VM / Kubernetes) to build the multi-team future on | PROD-06 | A multi-year architectural commitment with real cost/effort trade-offs RESEARCH can inform but not decide | Plan M3-05-05 task 2: read the drafted ADR, reply with the chosen option, any wording changes, and confirm signing |
| Whether to promote the extra-point-fixed candidates, including any override of a failed gate check | PROD-08 | The gate is a mechanical pre-check, not a replacement for judgement on a genuine methodology change; an override needs a real, stated reason | Plan M3-05-07 task 2: read the measured before/after numbers and the gate's real printed verdict, reply `both`/`ep`/`wp`/`none` plus an override reason if applicable |

---

## Validation Sign-Off

- [x] All auto tasks have `<automated>` verify commands (< 30 s each; the wave-3 real four-arm
      LOGO re-run and any real `ffep freeze-corpus`/model-card generation run are wave-level
      manual-execution steps inside an auto task's `<action>`, not part of the task's own
      `<verify>` gate)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify anywhere in the 8
      plans — every checkpoint task (M3-05-01 T1, M3-05-05 T2, M3-05-07 T2) is immediately
      preceded and followed by an automated-verified task
- [x] No watch-mode flags
- [x] Feedback latency < 120 s for every quick command
- [x] `nyquist_compliant: true` set in frontmatter
- [x] The M3-05-05 task-3 verify bug (a `grep ... || grep ...; echo checked` chain that always
      exited 0 regardless of match) is fixed as of 2026-09-08 — the gate now fails when neither
      pattern matches

**Approval:** planned 2026-09-08
