# Constraints

Binding technical and operational constraints extracted from the ingest set. Types: api-contract | schema | nfr | protocol.

---

## C-01 — Drone prohibited at official games
- source: docs/plan-2-cv-tracking.md (Leitentscheidung), docs/research-notes.md (finding 3)
- type: protocol
- content: Drones are not allowed at official games (matches NFL/college practice: drone filming standard at trainings, flight bans on game days). CV capture is therefore limited to trainings/scrimmages; for official games the fallback is an elevated tripod/Veo-class position as second domain. Opponent/game analysis stays PBP-based (Strand 1).

## C-02 — EU drone regulation
- source: docs/plan-2-cv-tracking.md (Phase 2.0)
- type: protocol
- content: EU-Drohnenverordnung compliance must be clarified before capture: operation category, registration, insurance.

## C-03 — DSGVO / player consent
- source: docs/plan-2-cv-tracking.md (Phase 2.0)
- type: protocol
- content: Player consent required (DSGVO). The federation presumably has media declarations; must verify that analysis use is covered, not just publication.

## C-04 — Solo-developer time budget
- source: docs/plan-2-cv-tracking.md (Risiken), docs/plan-1-analytics-refresh.md (phase effort estimates)
- type: nfr
- content: One person executes both strands. Every Strand-2 phase has a hard gate; the pilot decides after ~2 weekends whether further investment happens. Strand-1 phases are sized in evenings/weekends (Phase 1.1: 1 conversation + 1 evening; 1.2/1.3/1.4: 1–2 weekends each). Plans must not assume team-scale effort.

## C-05 — Camera-domain heterogeneity / drone regime
- source: docs/plan-2-cv-tracking.md (Phase 2.1/2.2), docs/research-notes.md (finding 6)
- type: nfr
- content: Drone footage is its own detection regime (small objects from altitude; TeamTrack evidence: side view mAP 52.7 vs top view 23.5). Requires higher input resolution and/or SAHI tile inference, a dedicated eval split per domain, and per-domain inference settings. One detector, mixed training set — pooled mAP hides domain collapse. Slightly oblique hover beats exact top-down (detection quality + partial jersey-number visibility).

## C-06 — License constraint: no AGPL
- source: docs/plan-2-cv-tracking.md (Stack), docs/research-notes.md (finding 8)
- type: nfr
- content: Ultralytics YOLO11/26 is AGPL-3.0 including fine-tuned weights — problematic once the federation or third parties use results. All stack components must be permissively licensed (Apache 2.0 / MIT / free): RF-DETR (fallback D-FINE, RT-DETR), CVAT + SAM2, Grounding DINO + autodistill, OC-SORT/BoT-SORT via BoxMOT or roboflow/trackers, SigLIP + UMAP + KMeans.

## C-07 — Hudl data contract: RESULT vocabulary and validation ranges
- source: docs/plan-1-analytics-refresh.md (Phase 1.1/1.2)
- type: schema
- content: The pipeline parses `RESULT` strings from a fixed vocabulary: `Rush`, `Penalty`, `KNEEL`, `Sack`, `Interception`, `Complete`, `Incomplete`, `TD`, `Def TD`, `Good`, `Safety`. Any deviation silently breaks feature construction. Manually maintained/derived fields: `game_id`, `play_id`, `drive_id`, `half`, `posteam`, `yardline_50`. Per-game validation rules: downs in 0–4; `yardline_50` in [0, 50]; drive IDs monotonic; play sequences gapless; reconstructed score == final score per match report. Rich charting fields available in own exports: `OFF FORM`, `Off Str`, `OFF PLAY`, `TARGET ROUTE`, `RECEIVED BY`, `Thrown By`, `YAC`, `GN/LS`.

## C-08 — Time data limitation
- source: docs/plan-1-analytics-refresh.md (Phase 1.1)
- type: schema
- content: `half_seconds_remaining` is currently synthetic (`1200 / max(play_id)` uniform per half). Real clip timestamps/game clock from Hudl are required to fix the WP model's largest quality gap.

## C-09 — Strand-2 pilot go/no-go thresholds
- source: docs/plan-2-cv-tracking.md (Phase 2.1)
- type: nfr
- content: (a) ≥ 90% of a play trackable without ID switch within the play; (b) position error roughly ≤ 1 m (plausibility check against known field dimensions); (c) inference of one game < 1 h on available hardware. Clear miss → return to Phase 2.0 (change capture setup), do not label more.

## C-10 — Compute and cost budget
- source: docs/plan-2-cv-tracking.md (Stack), docs/research-notes.md (finding 5)
- type: nfr
- content: Colab Pro / consumer RTX GPU. Total training cost in the tens of dollars; inference ~15–25 min per game on a T4 (within the < 1 h gate). Fine-tune runs are hours, not days.

## C-11 — Drone battery / capture logistics
- source: docs/plan-2-cv-tracking.md (Phase 2.0)
- type: protocol
- content: Drone battery ~20–25 min → battery swap protocol between drives; fixed hover position, one position per half-field drive; ~30–60 m altitude, 4K, fixed exposure, whole field in frame. Capture protocol condensed to one page; the video analyst is the key ally for adherence.

## C-12 — Ball detection out of scope (early phases)
- source: docs/plan-2-cv-tracking.md (Phase 2.1, Risiken)
- type: nfr
- content: Ball detection (small, motion-blurred) deliberately excluded from the first phases; play structure comes from snap detection + PBP join. Initial classes: `player`, `referee` only.
