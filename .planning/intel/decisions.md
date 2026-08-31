# Decisions

Synthesized from ingest set (no formal ADRs present; entries below are embedded decisions extracted from the plans and elevated to locked per ingest directive). Precedence applied: ADR > SPEC > PRD > DOC.

---

## D-01 — Two-strand split: own-team CV vs. opponent PBP

- source: docs/plan-2-cv-tracking.md (Leitentscheidung), corroborated by docs/plan-1-analytics-refresh.md (Nicht-Ziele), docs/research-notes.md (finding 2)
- status: locked
- scope: project architecture
- decision: Two use-cases, two data sources. **Own team** (routes, spacing, separation, speeds) is analyzed via CV tracking on controllable drone footage of trainings/scrimmages (Strand 2). **Opponent analysis** stays manual charting + PBP tendencies (Strand 1) — the industry-standard approach (PFF, Hudl IQ, college programs). CV on third-party/opponent footage is explicitly a stretch goal (Phase 2.5), never a foundation.
- consequence: The "heterogeneous video" domain problem collapses to 1–2 controlled camera domains. Strand 1 works completely without Strand 2 (no CV dependency).

## D-02 — License policy: no AGPL, RF-DETR over Ultralytics

- source: docs/plan-2-cv-tracking.md (Stack Lizenz-sauber), docs/research-notes.md (finding 8)
- status: locked
- scope: CV stack / all deliverables with possible federation or third-party use
- decision: Ultralytics YOLO (YOLO11/26) is deliberately avoided — AGPL-3.0 covers fine-tuned weights, problematic once the federation or third parties use the output. Detector is RF-DETR-S/M (Apache 2.0, DINOv2 backbone), fallback D-FINE + DEIM recipe. Entire stack must be permissively licensed (Apache 2.0 / MIT / free): CVAT self-hosted + SAM2, Grounding DINO + autodistill, OC-SORT/BoT-SORT via BoxMOT or roboflow/trackers, SigLIP + UMAP + KMeans.

## D-03 — Drone as primary capture domain

- source: docs/plan-2-cv-tracking.md (Phase 2.0, Phase 2.2), docs/research-notes.md (findings 3, 6)
- status: locked
- scope: video capture / dataset composition
- decision: Primary capture domain is a drone in fixed hover position at trainings/scrimmages: high oblique angle behind the endzone or near-overhead (~30–60 m, 4K, fixed exposure, whole field in frame, one position per half-field drive). Slightly oblique beats exactly top-down (TeamTrack evidence: vertical view degrades detection; oblique keeps jersey numbers partially visible). A second domain (elevated tripod / Veo-class side camera) is planned from the start; dataset weighted ~60% drone / ~40% second domain.
- consequence: Drone is not allowed at official games (see constraints); official-game footage remains Strand 1 material.

## D-04 — One detector across domains, per-domain evaluation

- source: docs/plan-2-cv-tracking.md (Phase 2.2), docs/research-notes.md (finding 6)
- status: locked
- scope: CV training/evaluation methodology
- decision: Train a single detector on a mixed multi-domain set, but evaluate per domain and set inference settings (resolution, SAHI tiling) per domain. Pooled mAP hides domain collapse. Do not build two pipelines.

## D-05 — Manual homography for static/drone setups

- source: docs/plan-2-cv-tracking.md (Phase 2.1, Stack), docs/research-notes.md (finding 7)
- status: locked
- scope: field-coordinate calibration
- decision: One-time manual 4–8-point calibration per hover position (field corners, midfield line, pylons). A field-keypoint model for moving cameras is deferred to stretch Phase 2.5 (sparse flag-field markings make it the hardest sub-problem).

## D-06 — Hard go/no-go gates; pilot decides investment

- source: docs/plan-2-cv-tracking.md (Phase 2.1, Risiken)
- status: locked
- scope: Strand 2 process
- decision: Every phase has a hard gate. The pilot (one game/training, ~2 weekends) decides via explicit criteria whether further investment happens; if criteria are clearly missed, return to Phase 2.0 (change capture setup) instead of labeling more. Dataset expansion (2.2) only after a passed pilot.

## D-07 — GroupKFold by game_id for model evaluation

- source: docs/plan-1-analytics-refresh.md (Phase 1.3)
- status: locked
- scope: EP/WP model methodology
- decision: Replace play-level train_test_split (leaks plays from the same game/drive into train and test, over-optimistic metrics) with GroupKFold over `game_id` (nflfastR precedent: LOSO over seasons).

## D-08 — Non-goals (Strand 1)

- source: docs/plan-1-analytics-refresh.md (Nicht-Ziele)
- status: locked
- scope: Strand 1
- decision: No live/in-game tooling. No CV dependency — Strand 1 must function entirely without Plan 2.
