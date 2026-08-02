# Requirements

Extracted per strand. Strand 1 from the PRD (docs/plan-1-analytics-refresh.md); Strand 2 functional deliverables from the SPEC (docs/plan-2-cv-tracking.md) — its binding technical thresholds live in constraints.md.

---

## Strand 1 — Hudl yearly export, EP/WP retraining, scouting products

### REQ-hudl-data-contract
- source: docs/plan-1-analytics-refresh.md (Phase 1.1)
- description: Define the full-year Hudl export with the video analyst: which games/tournaments, which columns, a Hudl export preset so all games share identical column names and the same `RESULT` tagging vocabulary. Clarify which fields are added manually today (`game_id`, `play_id`, `drive_id`, `half`, `posteam`, `yardline_50`) and how they are exported or deterministically derivable.
- acceptance: All exported games parse through the existing pipeline without silent feature-construction breakage; `RESULT` values restricted to the known vocabulary (see constraints.md C-07).

### REQ-time-data
- source: docs/plan-1-analytics-refresh.md (Phase 1.1)
- description: Request clip timestamps or game clock in the export so `half_seconds_remaining` becomes real instead of the synthetic uniform `1200 / max(play_id)` approximation. Called out as the single biggest quality lever for the WP model (README marks current times as "flawful bc of missing times").
- acceptance: WP model trains on real time remaining when analyst delivers clip/game-clock times.

### REQ-defense-charting-fields (optional)
- source: docs/plan-1-analytics-refresh.md (Phase 1.1)
- description: Add 2–3 defense fields to the charting protocol (coverage shell, blitz yes/no, flag-pull causer). Seconds per play for the analyst; enables defense scouting without any CV.
- acceptance: Fields present in the agreed charting protocol; optional, non-blocking.

### REQ-package-refactor
- source: docs/plan-1-analytics-refresh.md (Phase 1.2)
- description: Move notebook logic into the started `src/flag_football_ep` package (Hudl ingest, sportapp.fi ingest, feature mutations, training, scoring). Helpers in `Python/` are nearly module-ready.
- acceptance: Pipeline runs from the package, not from notebooks.

### REQ-ingest-cli
- source: docs/plan-1-analytics-refresh.md (Phase 1.2)
- description: Ingest CLI: folder of Hudl exports in → canonical Parquet out, with a per-game validation report (downs 0–4, `yardline_50` in [0, 50], drive IDs monotonic, play sequences gapless, score reconstruction == final score per match report). Without these checks, tagging errors surface only in the EPA chart.
- acceptance: CLI produces canonical Parquet plus validation report per game; validation rules per constraints.md C-07.

### REQ-source-merge
- source: docs/plan-1-analytics-refresh.md (Phase 1.2)
- description: Merge both sources (Hudl own games + sportapp.fi/IFAF tournament data) into one schema; consolidate `data_raw.csv`/`games_plays.csv` sprawl from repo root into `data/`.
- acceptance: Single canonical schema covering both sources; raw files organized under `data/`.

### REQ-split-fix
- source: docs/plan-1-analytics-refresh.md (Phase 1.3)
- description: Switch model evaluation from play-level `train_test_split` to GroupKFold over `game_id` (see decisions.md D-07). Flagged as the most important point of Phase 1.3.
- acceptance: Reported metrics come from grouped CV; no game contributes to both train and test.

### REQ-calibration
- source: docs/plan-1-analytics-refresh.md (Phase 1.3)
- description: Check calibration (reliability curves per class), report log-loss against a naive baseline. EP values are only as good as class-probability calibration.
- acceptance: Reliability curves and baseline-relative log-loss part of the training report.

### REQ-feature-retest
- source: docs/plan-1-analytics-refresh.md (Phase 1.3)
- description: Re-test feature candidates on the larger dataset: `half`, real `half_seconds_remaining` (if REQ-time-data delivers), competition level/gender as covariate, recency weighting.
- acceptance: Documented evaluation of each candidate on grouped CV.

### REQ-pat-baselines
- source: docs/plan-1-analytics-refresh.md (Phase 1.3)
- description: Replace hard-coded PAT baselines (50% for 1-pt, 46% for 2-pt in `helper_add_ep_wp.py`) with empirical estimates from the full dataset; produce a break-even chart for coaching (from which score/time state does going for 2 pay off?).
- acceptance: Empirical PAT rates in the model; break-even chart delivered.

### REQ-model-versioning
- source: docs/plan-1-analytics-refresh.md (Phase 1.3)
- description: Version models (date + training-data hash in filename) instead of overwriting `ep_model.pkl`.
- acceptance: No model artifact is silently overwritten.

### REQ-opponent-tendency-report
- source: docs/plan-1-analytics-refresh.md (Phase 1.4, product 1)
- description: Auto-generated opponent tendency report per team (HTML/PDF): formation × down & distance × field zone, target-route distribution, play-call tendencies by score state, 4th-down and PAT behavior. Sources: exchanged Hudl film charting + IFAF tournament data.
- acceptance: See REQ-report-turnaround.

### REQ-own-efficiency-report
- source: docs/plan-1-analytics-refresh.md (Phase 1.4, product 2)
- description: Own-team efficiency: EPA/play by formation/play-call/route, EPA per QB/receiver (`Thrown By`/`RECEIVED BY` exist in the export), YAC shares, drive success.

### REQ-decision-charts
- source: docs/plan-1-analytics-refresh.md (Phase 1.4, product 3)
- description: Decision charts: PAT break-even, 4th-down conversion rates by distance.

### REQ-wp-review-charts
- source: docs/plan-1-analytics-refresh.md (Phase 1.4, product 4)
- description: Win-probability charts per game as a review tool (after the time-data fix, REQ-time-data).

### REQ-report-turnaround
- source: docs/plan-1-analytics-refresh.md (Phase 1.4, Erfolgskriterium)
- description: Strand-level success criterion: before the next camp/tournament, an auto-generated report exists for every group-stage opponent, and generation from raw exports takes < 10 minutes.
- acceptance: End-to-end raw-export → report runtime under 10 minutes per opponent.

---

## Strand 2 — CV object detection & player tracking

### REQ-capture-protocol
- source: docs/plan-2-cv-tracking.md (Phase 2.0)
- description: Before anything else: material inventory with the video analyst (which games/trainings exist in which camera domain, resolution, frame rate; 2–3 sample clips per domain); agree the drone capture protocol for trainings/scrimmages (highest-leverage item of the whole project — see decisions.md D-03); battery swap protocol between drives (~20–25 min battery); clarify legal (EU drone regulation, DSGVO consent — see constraints.md); agree a sync convention mapping a video play to a Hudl-PBP play (timestamp overlay, clap/board at drive start).
- acceptance: One-page capture protocol agreed with the analyst; sync convention defined (without it, tracking data stays decoupled from Strand 1).

### REQ-cv-pilot
- source: docs/plan-2-cv-tracking.md (Phase 2.1)
- description: Pilot on a single scrimmage/training with the best available drone footage: (1) zero-shot baseline (Grounding DINO / COCO detector, prompt "person") to validate the pipeline before labeling and produce pre-labels; (2) CVAT + SAM2 video tracker, correct ~300–500 frames (classes `player`, `referee`; no ball initially); (3) fine-tune RF-DETR-Small (drone regime: higher input resolution, SAHI tiling if needed); (4) tracking with OC-SORT (static camera) via BoxMOT or roboflow/trackers; team assignment without labels via SigLIP embeddings + UMAP + KMeans; (5) homography via one-time manual 4–8-point calibration per hover position → XY CSV in field coordinates + top-down "radar" clip as demo for HC and analyst.
- acceptance: Go/no-go gate per constraints.md C-09 (≥90% track continuity, ≤1 m position error, <1 h inference). Clear miss → back to Phase 2.0, not more labeling.

### REQ-dataset-buildout
- source: docs/plan-2-cv-tracking.md (Phase 2.2)
- description: Only after a passed pilot. Model-in-the-loop (pilot detector pre-labels, correct errors only, 2 active-learning iterations): 1,500–3,000 verified frames weighted ~60% drone / ~40% best second domain (elevated side camera), targeting hard cases (line-of-scrimmage congestion, blitz, backlight, rain). One detector across all domains with per-domain evaluation and per-domain inference settings. Version the dataset cleanly (Roboflow Universe or DVC). Option: publish as the first public flag-football detection dataset (does not exist yet; differentiator, invites community contributions).
- acceptance: Versioned dataset in target size/mix; per-domain eval splits in place.

### REQ-coaching-metrics
- source: docs/plan-2-cv-tracking.md (Phase 2.3)
- description: The actual value creation; each metric a small testable module on the XY CSV: (1) snap detection per play (motion impulse at the LOS) → play segmentation; (2) route overlays and route classification for receivers (validated against `TARGET ROUTE` from Hudl charting as existing ground truth); (3) separation at catch, QB time-to-throw, speeds/accelerations, defense spacing metrics; (4) join with Strand-1 PBP via the sync convention → EPA per route/concept with positional context.
- acceptance: Modules unit-testable on XY CSV; route classification checked against existing `TARGET ROUTE` labels.

### REQ-player-identity
- source: docs/plan-2-cv-tracking.md (Phase 2.4)
- description: Only if needed. First try: tracking continuity + team clustering + manual assignment of ~10 tracklets per play (minutes of work at 5v5). If automation is required: jersey-number-pipeline recipe (legibility filter → torso crop → PARSeq → tracklet voting) or VLM reads (Qwen2-VL class) on keyframes. Jersey OCR is the most fragile module and barely possible top-down.
- acceptance: Player-level attribution achievable per play; automation optional.

### REQ-stretch-broadcast (stretch)
- source: docs/plan-2-cv-tracking.md (Phase 2.5)
- description: Only when 2.1–2.3 are in operation: TV/side view as third domain + field-keypoint model for moving cameras (roboflow/sports recipe, PnLCalib). Deliberately deferred — sparse flag-field markings make it the hardest sub-problem, and scouting need is covered by Strand 1.
- acceptance: Explicitly out of scope until Strand 2 core is operational.
