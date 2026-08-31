# Synthesis Summary

Entry point for downstream consumers (gsd-roadmapper). Mode: new (fresh bootstrap, no existing .planning context). Precedence applied: ADR > SPEC > PRD > DOC. Source docs are German; intel synthesized in English with German proper nouns preserved.

## Docs synthesized (3)

- PRD: 1 — docs/plan-1-analytics-refresh.md (Strand 1: Hudl yearly export, EP/WP retraining, scouting products; confidence medium)
- SPEC: 1 — docs/plan-2-cv-tracking.md (Strand 2: object detection & player tracking on drone footage; confidence medium)
- DOC: 1 — docs/research-notes.md (research findings backing both strands; confidence high)
- ADR: 0 (no formal ADRs; embedded decisions extracted from the plans)

Cycle check: mutual "see also" reference between the two sibling plans detected and cleared as non-blocking (see conflicts report INFO).

## Decisions locked (8)

D-01 two-strand split (own-team CV vs opponent PBP), D-02 license policy (RF-DETR over AGPL Ultralytics), D-03 drone as primary capture domain, D-04 one detector with per-domain eval, D-05 manual homography, D-06 hard go/no-go gates, D-07 GroupKFold by game_id, D-08 Strand-1 non-goals (no live tooling, no CV dependency).
→ decisions.md

## Requirements extracted (22)

- Strand 1 (16): REQ-hudl-data-contract, REQ-time-data, REQ-defense-charting-fields, REQ-package-refactor, REQ-ingest-cli, REQ-source-merge, REQ-split-fix, REQ-calibration, REQ-feature-retest, REQ-pat-baselines, REQ-model-versioning, REQ-opponent-tendency-report, REQ-own-efficiency-report, REQ-decision-charts, REQ-wp-review-charts, REQ-report-turnaround
- Strand 2 (6): REQ-capture-protocol, REQ-cv-pilot, REQ-dataset-buildout, REQ-coaching-metrics, REQ-player-identity, REQ-stretch-broadcast (stretch)
→ requirements.md

## Constraints (12)

- protocol (4): C-01 drone banned at official games, C-02 EU drone regulation, C-03 DSGVO/consent, C-11 battery/capture logistics
- nfr (6): C-04 solo-developer time budget, C-05 camera-domain heterogeneity, C-06 no-AGPL license constraint, C-09 pilot go/no-go thresholds, C-10 compute budget, C-12 ball detection out of scope
- schema (2): C-07 RESULT vocabulary + validation ranges, C-08 synthetic time-data limitation
→ constraints.md

## Context topics (5)

Team and stakeholders (HC, Videoanalyst); competitive calendar (IFAF Worlds 2026 Düsseldorf, LA28); existing codebase and data assets (polars + XGBoost EP/WP per nflfastR, data_raw.csv 47 games / ~3,700 plays, pbp_wc24_static.csv, sportapp.fi); industry state of the art; key resources.
→ context.md

## Conflicts

0 blockers, 0 competing-variants, 6 auto-resolved/informational.
→ ../INGEST-CONFLICTS.md

STATUS: READY — safe to route.
