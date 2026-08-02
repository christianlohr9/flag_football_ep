# Requirements: flag-football-analytics

**Defined:** 2026-08-02
**Core Value:** Before the next national-team camp/tournament, the HC has an auto-generated tendency report for every group opponent, produced from raw exports in under 10 minutes — and the CV tracking pilot has reached an explicit go/no-go decision.

IDs preserve the intel slugs from `.planning/intel/requirements.md` (shown in parentheses). Source docs (German, authoritative detail): `docs/plan-1-analytics-refresh.md`, `docs/plan-2-cv-tracking.md`.

## v1 Requirements

### Strand 1 — Data Contract (Phase 1.1)

- [ ] **REQ-S1-01** (hudl-data-contract): Full-year Hudl export defined with the Videoanalyst — games/tournaments, columns, an export preset so all games share identical column names and the fixed `RESULT` vocabulary; manual fields (`game_id`, `play_id`, `drive_id`, `half`, `posteam`, `yardline_50`) exported or deterministically derivable. Acceptance: all exported games parse through the pipeline without silent feature-construction breakage.
- [ ] **REQ-S1-02** (time-data): Clip timestamps or game clock in the export so `half_seconds_remaining` becomes real instead of synthetic `1200 / max(play_id)` — the single biggest WP-model quality lever.
- [ ] **REQ-S1-03** (defense-charting-fields, *optional, non-blocking*): 2–3 defense fields added to the charting protocol (coverage shell, blitz yes/no, flag-pull causer) — seconds per play for the analyst, enables defense scouting without CV.

### Strand 1 — Pipeline (Phase 1.2)

- [ ] **REQ-S1-04** (package-refactor): Notebook logic moved into `src/flag_football_ep` (Hudl ingest, sportapp.fi ingest, feature mutations, training, scoring); pipeline runs from the package, not notebooks.
- [ ] **REQ-S1-05** (ingest-cli): Ingest CLI: folder of Hudl exports in → canonical Parquet out, with a per-game validation report (downs 0–4, `yardline_50` in [0, 50], monotonic drive IDs, gapless play sequences, score reconstruction == final score per match report).
- [ ] **REQ-S1-06** (source-merge): Hudl own games + sportapp.fi/IFAF tournament data merged into one canonical schema; `data_raw.csv`/`games_plays.csv` sprawl consolidated into `data/`.

### Strand 1 — Model Retraining (Phase 1.3)

- [ ] **REQ-S1-07** (split-fix): Model evaluation switched from play-level `train_test_split` to GroupKFold over `game_id`; no game contributes to both train and test.
- [ ] **REQ-S1-08** (calibration): Reliability curves per class and log-loss vs a naive baseline in the training report.
- [ ] **REQ-S1-09** (feature-retest): Feature candidates re-tested on grouped CV with documented outcomes: `half`, real `half_seconds_remaining` (if REQ-S1-02 delivers), competition level/gender as covariate, recency weighting.
- [ ] **REQ-S1-10** (pat-baselines): Hard-coded PAT baselines (50%/46% in `helper_add_ep_wp.py`) replaced with empirical estimates from the full dataset; break-even chart for coaching produced.
- [ ] **REQ-S1-11** (model-versioning): Models versioned (date + training-data hash in filename); no artifact silently overwritten.

### Strand 1 — Coaching Products (Phase 1.4)

- [ ] **REQ-S1-12** (opponent-tendency-report): Auto-generated opponent tendency report per team (HTML/PDF): formation × down & distance × field zone, target-route distribution, play-call tendencies by score state, 4th-down and PAT behavior.
- [ ] **REQ-S1-13** (own-efficiency-report): Own-team efficiency: EPA/play by formation/play-call/route, EPA per QB/receiver (`Thrown By`/`RECEIVED BY`), YAC shares, drive success.
- [ ] **REQ-S1-14** (decision-charts): Decision charts: PAT break-even, 4th-down conversion rates by distance.
- [ ] **REQ-S1-15** (wp-review-charts): Win-probability charts per game as a review tool (after the time-data fix).
- [ ] **REQ-S1-16** (report-turnaround): Before the next camp/tournament, an auto-generated report exists for every group-stage opponent; generation from raw exports takes < 10 minutes.

### Strand 2 — CV Tracking

- [ ] **REQ-S2-01** (capture-protocol): Material inventory with the Videoanalyst (domains, resolution, frame rate, 2–3 sample clips each); one-page drone capture protocol agreed (fixed hover, ~30–60 m, 4K, fixed exposure, battery-swap between drives); EU drone regulation + DSGVO consent cleared; sync convention mapping video plays to Hudl-PBP plays defined.
- [ ] **REQ-S2-02** (cv-pilot): Pilot on one scrimmage/training: zero-shot baseline → CVAT + SAM2 correction of ~300–500 frames (`player`, `referee`) → RF-DETR-Small fine-tune → OC-SORT tracking + SigLIP/UMAP/KMeans team split → manual homography → XY CSV + top-down radar clip. Acceptance: explicit go/no-go against gate criteria (≥90% track continuity, ~≤1 m position error, <1 h inference/game); clear miss → back to Phase 2.0.
- [ ] **REQ-S2-03** (dataset-buildout): Only after passed pilot: 1,500–3,000 verified frames via model-in-the-loop (2 active-learning iterations), ~60% drone / ~40% second domain, targeting hard cases; per-domain eval splits; dataset cleanly versioned (Roboflow Universe or DVC); publication option assessed.
- [ ] **REQ-S2-04** (coaching-metrics): Small testable modules on the XY CSV: snap detection → play segmentation; route overlays + route classification validated against `TARGET ROUTE`; separation at catch, QB time-to-throw, speeds/accelerations, defense spacing; join with Strand-1 PBP via sync convention → EPA per route/concept with positional context.
- [ ] **REQ-S2-05** (player-identity): Player-level attribution per play via tracking continuity + team clustering + manual assignment (~10 tracklets/play at 5v5); jersey-number pipeline or VLM reads only if automation proves necessary.

## Stretch Requirements (deferred)

Mapped to a deferred roadmap phase; not part of the v1 gate.

- **REQ-S2-06** (stretch-broadcast): TV/side view as third domain + field-keypoint model for moving cameras (roboflow/sports recipe, PnLCalib). Explicitly out of scope until 2.1–2.3 are in operation; scouting need is covered by Strand 1.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Live/in-game tooling | Strand-1 non-goal (D-08); solo developer |
| CV on opponent/third-party footage as foundation | Opponent analysis stays manual charting + PBP (D-01); broadcast is stretch only |
| Ball detection (early phases) | Small, motion-blurred; play structure from snap detection + PBP join (C-12) |
| Ultralytics YOLO / any AGPL component | AGPL covers fine-tuned weights; federation use problematic (D-02, C-06) |
| Field-keypoint model for moving cameras | Hardest sub-problem (sparse flag markings); deferred with Phase 2.5 (D-05) |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| REQ-S1-01 | Phase 1.1 | Pending |
| REQ-S1-02 | Phase 1.1 | Pending |
| REQ-S1-03 | Phase 1.1 | Pending (optional) |
| REQ-S1-04 | Phase 1.2 | Pending |
| REQ-S1-05 | Phase 1.2 | Pending |
| REQ-S1-06 | Phase 1.2 | Pending |
| REQ-S1-07 | Phase 1.3 | Pending |
| REQ-S1-08 | Phase 1.3 | Pending |
| REQ-S1-09 | Phase 1.3 | Pending |
| REQ-S1-10 | Phase 1.3 | Pending |
| REQ-S1-11 | Phase 1.3 | Pending |
| REQ-S1-12 | Phase 1.4 | Pending |
| REQ-S1-13 | Phase 1.4 | Pending |
| REQ-S1-14 | Phase 1.4 | Pending |
| REQ-S1-15 | Phase 1.4 | Pending |
| REQ-S1-16 | Phase 1.4 | Pending |
| REQ-S2-01 | Phase 2.0 | Pending |
| REQ-S2-02 | Phase 2.1 | Pending |
| REQ-S2-03 | Phase 2.2 | Pending (gated on 2.1) |
| REQ-S2-04 | Phase 2.3 | Pending (gated on 2.1) |
| REQ-S2-05 | Phase 2.4 | Pending (only if needed) |
| REQ-S2-06 | Phase 2.5 | Deferred (stretch) |

**Coverage:**
- v1 requirements: 21 total (+ 1 stretch)
- Mapped to phases: 22
- Unmapped: 0 ✓

---
*Requirements defined: 2026-08-02*
*Last updated: 2026-08-02 after roadmap creation (traceability populated)*
