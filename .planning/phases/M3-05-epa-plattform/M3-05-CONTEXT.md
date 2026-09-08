# Phase M3-5: EPA-Modell als Produkt — Reproduzierbarkeit, Beförderungs-Gate, Modellkarte, Plattform-Entscheidung - Context

**Gathered:** 2026-09-08
**Status:** Ready for research/planning (execution after the M3-2 re-run of 2026-09-08 lands and the champion decision is taken)
**Source:** User request 2026-09-08 after the head coach asked for an overview of the EPA model ("wir sollten deiner Einschätzung folgen … Modellkarte und UI helfen Jona … Feature Store und Kubernetes echt mal durchdenken … könnte uns später um die Ohren fliegen, wenn das U17 bis Seniors nutzen wollen")

<domain>
## Phase Boundary

Turn the EP/WP models from "a training script with an MLflow store" into a product a coach can trust and a second team could adopt: (1) **coach-facing transparency** — a German model card per champion and a browsable MLflow UI; (2) **engineering hygiene ("enterprise-light")** — lineage on every run (corpus fingerprint, git commit, config), a dated corpus freeze before every retrain, an automated promotion gate (beats champion AND naive baseline on LOGO log-loss, calibration within tolerance, no-play share under threshold) replacing the manual `ffep promote`, and a CI run of the full pipeline on fixture data; (3) **one deliberate architecture decision (ADR)** on the platform for the multi-team future (U17 to Seniors, men's and women's programmes, other coaches' Excel → web app, BL-02): batch scoring + MLflow on a single machine vs. containerised services on the Open Telekom Cloud vs. Kubernetes; whether a feature store is warranted; multi-tenant data model (team/programme/season scoping of the canonical corpus, models per programme vs. shared model with tier features); auth and PII boundaries. The ADR is decided with the user, with costs and a migration path, not by default.

Not this phase: the web app itself (BL-02, depends on the ADR), new features/metrics, CV work, the IFAF/HC ingest fixes (done), the M3-2 re-run (running separately, its numbers feed the first model card).
</domain>

<decisions>
## Implementation Decisions (from the user, 2026-09-08)

- **Follow the coordinator's assessment:** quick wins first (model card, MLflow UI), then hygiene (lineage, freeze, gate, CI); nothing that only makes sense at scale gets built before the ADR says so.
- **The ADR is mandatory and explicit:** Feature Store and Kubernetes are not dismissed — the user wants the trade-off worked through once, properly, because a wrong default now could hurt when U17–Seniors adopt the tooling. Output: `docs/adr/0001-modell-plattform.md` (German), options with cost/effort/operational burden/migration path, a recommendation, and a decision line the user signs.
- **Model card is coach-readable German** (like `docs/epa-modellkarte.md` from the 2026-09-08 re-run) and regenerated from MLflow + reference CSVs, never hand-typed numbers (doc-guard pattern from M3-2).
- **Promotion gate replaces judgement by hand:** `ffep promote` only succeeds when the gate passes or `--force` with a written reason is given; the gate's thresholds live in `ffep.toml` and are documented.
- **Corpus freeze:** `ffep freeze-corpus` (or equivalent) writes a dated fingerprint manifest; every training run references one; docs cite the freeze date.
- **UI:** MLflow UI served locally (`mlflow ui` against the project store) with a German how-to for the HC session; a hosted variant is an ADR outcome, not a default.

### Claude's Discretion
- Gate thresholds (proposed from the M3-2 numbers), CI runner choice, model-card template, ADR option set beyond the three named.
</decisions>

<canonical_refs>
## Canonical References
- `docs/epa-refinement-2026-10.md` (+ Nachtrag 2026-09-08), `docs/epa-modellkarte.md`, `docs/model-training.md`
- `.planning/phases/M3-02-epa-refinement/M3-02-05-SUMMARY.md`, `M3-02-08-PLAN.md` (promotion semantics), `M3-02-RERUN-2026-09-08-SUMMARY.md`
- `src/flag_football_ep/model/{train,evaluate,experiments,registry,mlflow_store}.py`, `scripts/hc_corpus_ablation.py`, `ffep.toml`
- `docs/hc-notes-2026-09-03.md` (web-app ambition, "seine Excel in eine WebApp"), `docs/hc-sync-2026-10.md`
- OTC context: `docs/hackathon-otc-upload.md`, `ffep.toml` `[cv]` OBS settings (the only cloud footprint so far)
</canonical_refs>

<specifics>
## Specific Ideas
- The head coach's trust bridge is the model card + UI: "welches Modell ist produktiv, warum, wie gut, seit wann".
- Multi-team scaling question to answer in the ADR: is one shared EP model with competition-tier features better than per-programme models (data volume per programme is small: the women's national team alone has ~20k plays)?
- Scoring is batch (reports in < 10 min); no real-time requirement exists today — the ADR must say what would change that.
</specifics>

<deferred>
## Deferred Ideas
- Web app (BL-02) — after the ADR.
- Game clock from broadcast (BL-01), win-driver analysis (BL-04), CV stat collection (BL-03) — unchanged backlog.
</deferred>

---
*Phase: M3-05-epa-plattform*
*Context gathered: 2026-09-08 from the user's decision in chat*
