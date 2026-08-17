# Roadmap: flag-football-analytics

## Overview

Two parallel strands mirror the source plans. Strand 1 (phases 1.1–1.4) turns the Hudl yearly export and sportapp.fi/IFAF data into a reproducible package-based pipeline, retrains the EP/WP models methodically, and ships the coaching products that define Milestone 1: an auto-generated opponent tendency report per group opponent in under 10 minutes from raw exports. Strand 2 (phases 2.0–2.5) builds CV player tracking on controlled drone footage, with a hard go/no-go gate after the pilot (2.1). Strand 1 delivers value first and works entirely without Strand 2; Phase 2.0 is coordination (no code) and runs in parallel with early Strand-1 work. Phase 2.5 is stretch and deferred.

**Phase Numbering:**
This roadmap uses `strand.phase` numbering intentionally (1.1–1.4 = Strand 1, 2.0–2.5 = Strand 2), matching the source plans in `docs/`. These decimals are planned milestone phases, NOT urgent insertions.

**Execution Order (solo developer, pragmatic):**
1.1 → 1.2 → 1.3 → 1.4 (value first), with 2.0 running in parallel from the start (conversation + protocol, no code). Then 2.1 (gate) → 2.2 → 2.3 → 2.4. Phase 2.5 only when 2.1–2.3 are in operation.

## Phases

- [x] **Phase 1.1: Data Contract with the Videoanalyst** - Hudl export preset, RESULT vocabulary, time data, optional defense fields (completed 2026-08-17)
- [x] **Phase 1.2: Repo to Pipeline** - Notebook logic into `src/flag_football_ep`, ingest CLI with per-game validation, source merge (plans 01–17 executed 2026-08-17; verification found gaps, gap-closure plans 18–24 pending) (completed 2026-08-17)
- [ ] **Phase 1.3: Methodical Model Retraining** - GroupKFold split fix, calibration, feature re-tests, empirical PAT baselines, model versioning
- [ ] **Phase 1.4: Coaching Products** - Opponent tendency reports, own-efficiency report, decision charts, WP review charts, <10 min turnaround
- [ ] **Phase 2.0: Capture Protocol & Material Inventory** - Analyst conversation, drone protocol, legal clearance, sync convention (parallel, no code)
- [ ] **Phase 2.1: CV Tracking Pilot (Go/No-Go Gate)** - One scrimmage: label → fine-tune RF-DETR → track → homography → XY CSV + radar demo; explicit gate decision
- [ ] **Phase 2.2: Dataset Buildout** - 1,500–3,000 verified frames, 60/40 domain mix, per-domain eval, versioned dataset (only after passed pilot)
- [ ] **Phase 2.3: Tracks to Coaching Metrics** - Snap detection, route classification vs TARGET ROUTE, separation/speeds/spacing, PBP join
- [ ] **Phase 2.4: Player Identity** - Manual tracklet assignment first; jersey OCR / VLM only if needed
- [ ] **Phase 2.5: Broadcast Footage (STRETCH, deferred)** - Third domain + field-keypoint model; only when 2.1–2.3 are in operation

## Phase Details

### Phase 1.1: Data Contract with the Videoanalyst

**Goal**: The Hudl yearly export is defined so every game parses through the pipeline identically, and the path to real time data is settled
**Depends on**: Nothing (first phase; 1 conversation + 1 evening)
**Requirements**: REQ-S1-01, REQ-S1-02, REQ-S1-03
**Success Criteria** (what must be TRUE):

  1. A Hudl export preset exists and is agreed with the Videoanalyst; all exported games share identical column names and `RESULT` values from the fixed vocabulary (C-07)
  2. For each manually maintained field (`game_id`, `play_id`, `drive_id`, `half`, `posteam`, `yardline_50`) it is documented how it arrives in the export or is deterministically derived
  3. Time data is settled: the analyst delivers clip timestamps/game clock, or their unavailability is explicitly recorded (drives REQ-S1-15 scope)
  4. The charting protocol records the decision on defense fields (coverage shell, blitz, flag-pull causer) — adopted or explicitly skipped

**Plans**: 3 plans
Plans:
**Wave 1**

- [x] 01.1-01-PLAN.md — Draft the full contract (schema JSON + German spec + half-boundary record) with explicit PENDING slots

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01.1-02-PLAN.md — Sight real defense-column values from sample exports; fix COVERAGE/DEF FRONT/BLITZ vocabulary

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01.1-03-PLAN.md — Analyst conversation: record outcomes, fill PENDING slots, finalize contract v1.0

### Phase 1.2: Repo to Pipeline

**Goal**: The analysis runs as a reproducible package-based pipeline from raw exports to canonical, validated Parquet
**Depends on**: Phase 1.1 (export schema and vocabulary fixed)
**Requirements**: REQ-S1-04, REQ-S1-05, REQ-S1-06
**Success Criteria** (what must be TRUE):

  1. The full pipeline (Hudl ingest, sportapp.fi ingest, feature mutations, training, scoring) runs from `src/flag_football_ep` — no notebook required
  2. The ingest CLI turns a folder of Hudl exports into canonical Parquet and emits a per-game validation report: downs 0–4, `yardline_50` in [0, 50], monotonic drive IDs, gapless play sequences, reconstructed score == final score per match report
  3. Hudl own games and sportapp.fi/IFAF tournament data share one canonical schema; raw files are consolidated under `data/` (no more repo-root CSV sprawl)

**Plans**: 25 plans in 11 waves (18–24 close the gaps found in the first `01.2-VERIFICATION.md`; 25 closes the IFAF containment gap found by the re-verification)
Plans:
**Wave 1**

- [x] 01.2-01-PLAN.md — Package foundation: dependencies, `ffep` entry point, Typer app with all subcommands, pytest scaffolding
- [x] 01.2-02-PLAN.md — Freeze the notebook's EP/WP training frames as regression fixtures before any porting

**Wave 2** *(blocked on Wave 1)*

- [x] 01.2-03-PLAN.md — Config layer, `data/` layout, `.env` secrets, removal of the committed API key
- [x] 01.2-04-PLAN.md — Canonical plays schema, conform, shared scoring chain, test-frame factory
- [x] 01.2-05-PLAN.md — Contract loader with version tolerance, reference-data loaders and CSVs, operator seeding

**Wave 3** *(blocked on Wave 2)*

- [x] 01.2-06-PLAN.md — Six per-game validation checks, quarantine partition, Markdown report
- [x] 01.2-07-PLAN.md — Fetch layer: sportapp.fi and cpx.studio snapshot download with redaction and timeouts
- [x] 01.2-08-PLAN.md — Feature engineering port: EP/WP prep, EP/WP variables, model mutations

**Wave 4** *(blocked on Wave 3)*

- [x] 01.2-09-PLAN.md — Hudl ingest: filename, contract header, exact-token RESULT grammar, contract derivations
- [x] 01.2-10-PLAN.md — Legacy `data_raw.csv` ingest with frozen notebook semantics, warn-only validation
- [x] 01.2-11-PLAN.md — sportapp.fi snapshot ingest and the WC24 corpus disposition
- [x] 01.2-12-PLAN.md — IFAF/cpx.studio field mapping and snapshot ingest
- [x] 01.2-13-PLAN.md — EP/WP training with MLflow tracking, optional hyperopt, dated artifact export

**Wave 5** *(blocked on Wave 4)*

- [x] 01.2-14-PLAN.md — `ffep ingest`: source orchestration, atomic canonical Parquet, validation report
- [x] 01.2-15-PLAN.md — `ffep score`: MLflow run resolution, EPA/WPA scoring

**Wave 6** *(blocked on Wave 5)*

- [x] 01.2-16-PLAN.md — `ffep run` chaining, migration-equivalence test, full-suite gate and timing baseline

**Wave 7** *(blocked on Wave 6)*

- [x] 01.2-17-PLAN.md — Repo cleanup, `Python/` removal, thin demo notebooks, pipeline documentation

**Wave 8** *(gap closure — four independent fixes, no shared files)*

- [x] 01.2-18-PLAN.md — CR-01: restore chronological row order before EP/WP variable derivation in `score_plays`
- [x] 01.2-19-PLAN.md — WR-04: scope the EP/WP `backward_fill`/`shift(-1)` operations to `game_id`
- [x] 01.2-20-PLAN.md — CR-02 (containment): non-strict ingest casts, complete per-file/per-game error handling, null-safe `gapless_play_ids`
- [x] 01.2-21-PLAN.md — WR-02: credit a score that happens on a game's first play

**Wave 9** *(blocked on Wave 8)*

- [x] 01.2-22-PLAN.md — CR-02/WR-01 (reporting): source notices and skipped files in the report, console and CLI
- [x] 01.2-23-PLAN.md — WR-03/WR-11: IFAF defensive 2-pt conversions to the defense, one cross-source `play_type` vocabulary

**Wave 10** *(blocked on Wave 9)*

- [x] 01.2-24-PLAN.md — Mixed-corpus end-to-end scoring test, notice/containment documentation, full-suite gate

**Wave 11** *(gap closure after re-verification)*

- [x] 01.2-25-PLAN.md — IFAF per-game ingest containment: null/type-safe play sort key, per-game skip notices, explicit whole-source-drop wording

### Phase 1.3: Methodical Model Retraining

**Goal**: EP/WP models retrained on the full dataset with honest evaluation, calibrated probabilities, and versioned artifacts
**Depends on**: Phase 1.2
**Requirements**: REQ-S1-07, REQ-S1-08, REQ-S1-09, REQ-S1-10, REQ-S1-11
**Success Criteria** (what must be TRUE):

  1. All reported metrics come from GroupKFold over `game_id` (D-07); no game contributes to both train and test
  2. The training report contains reliability curves per class and log-loss against a naive baseline
  3. Each feature candidate (`half`, real `half_seconds_remaining` if delivered, competition level/gender, recency weighting) has a documented grouped-CV evaluation
  4. PAT baselines are empirical estimates from the full dataset (replacing hard-coded 50%/46%), and the PAT break-even chart exists
  5. Model artifacts carry date + training-data hash in the filename; `ep_model.pkl` is never silently overwritten again

**Plans**: TBD

### Phase 1.4: Coaching Products

**Goal**: The HC opens auto-generated, tournament-ready scouting products; Milestone 1's Strand-1 half is met
**Depends on**: Phase 1.3
**Requirements**: REQ-S1-12, REQ-S1-13, REQ-S1-14, REQ-S1-15, REQ-S1-16
**Success Criteria** (what must be TRUE):

  1. An opponent tendency report (HTML/PDF) is auto-generated per team: formation × down & distance × field zone, target-route distribution, play-call tendencies by score state, 4th-down and PAT behavior
  2. The own-team efficiency report shows EPA/play by formation/play-call/route, EPA per QB/receiver, YAC shares, and drive success
  3. Decision charts (PAT break-even, 4th-down conversion by distance) are delivered in a form the HC can use directly
  4. WP charts per game are generated as a review tool (using real time data from REQ-S1-02, or explicitly flagged as synthetic-time if unavailable)
  5. End-to-end: raw exports → finished report in under 10 minutes per opponent, verified for every group-stage opponent before the next camp/tournament

**Plans**: TBD

### Phase 2.0: Capture Protocol & Material Inventory

**Goal**: Everything is in place to capture pilot-quality drone footage legally and reproducibly — before any CV code is written
**Depends on**: Nothing (coordination only; runs in parallel with Strand 1)
**Requirements**: REQ-S2-01
**Success Criteria** (what must be TRUE):

  1. Material inventory with the Videoanalyst is complete: which games/trainings exist per camera domain, resolution/frame rate, with 2–3 sample clips secured per domain
  2. A one-page drone capture protocol is agreed: fixed hover position (~30–60 m, 4K, fixed exposure, whole field in frame, one position per half-field drive) plus battery-swap protocol between drives (D-03, C-11)
  3. Legal is cleared: EU-Drohnenverordnung (category, registration, insurance) and DSGVO consent verified to cover analysis use, not just publication (C-02, C-03)
  4. A sync convention mapping a video play to a Hudl-PBP play is defined (timestamp overlay, clap/board at drive start) — without it, tracking stays decoupled from Strand 1

**Plans**: TBD

### Phase 2.1: CV Tracking Pilot (Go/No-Go Gate)

**Goal**: One scrimmage proves (or disproves) that the drone → XY-coordinates pipeline works; an explicit gate decision closes Milestone 1's Strand-2 half
**Depends on**: Phase 2.0 (~2 weekends)
**Requirements**: REQ-S2-02
**Success Criteria** (what must be TRUE):

  1. A fine-tuned RF-DETR-Small detects `player` and `referee` on the pilot drone footage, bootstrapped from zero-shot pre-labels (Grounding DINO) and ~300–500 CVAT+SAM2-corrected frames (drone regime: higher input resolution, SAHI tiling if needed)
  2. OC-SORT tracking plus SigLIP+UMAP+KMeans team clustering produces per-player tracks with team assignment on the pilot game
  3. Manual 4–8-point homography per hover position yields an XY CSV in field coordinates, and a top-down radar clip has been demoed to HC and analyst
  4. The go/no-go decision is recorded against the gate criteria (C-09): ≥ 90% of a play tracked without ID switch, position error ~≤ 1 m against known field dimensions, inference of one game < 1 h — clear miss routes back to Phase 2.0 (capture setup), not to more labeling (D-06)

**Plans**: TBD

### Phase 2.2: Dataset Buildout

**Goal**: A versioned multi-domain detection dataset robust enough for regular use — built only after the pilot passed its gate
**Depends on**: Phase 2.1 (gate PASSED)
**Requirements**: REQ-S2-03
**Success Criteria** (what must be TRUE):

  1. 1,500–3,000 verified frames exist at ~60% drone / ~40% best second domain, targeting hard cases (line-of-scrimmage congestion, blitz, backlight, rain), produced model-in-the-loop with 2 active-learning iterations
  2. One detector is trained across all domains, with per-domain eval splits reported and per-domain inference settings (resolution, SAHI tiling) configured (D-04)
  3. The dataset is cleanly versioned (Roboflow Universe or DVC), and the option to publish it as the first public flag-football detection dataset has been assessed

**Plans**: TBD

### Phase 2.3: Tracks to Coaching Metrics

**Goal**: XY tracks become coaching insight — the actual value creation of Strand 2
**Depends on**: Phase 2.1 (XY CSV format; refined by Phase 2.2 models)
**Requirements**: REQ-S2-04
**Success Criteria** (what must be TRUE):

  1. Snap detection (motion impulse at the LOS) segments continuous footage into plays on the XY CSV
  2. Route overlays and receiver route classification run per play, validated against existing `TARGET ROUTE` labels from Hudl charting
  3. Separation at catch, QB time-to-throw, speeds/accelerations, and defense spacing metrics each exist as a small unit-tested module on the XY CSV
  4. Tracking data joins Strand-1 PBP via the sync convention, producing EPA per route/concept with positional context

**Plans**: TBD

### Phase 2.4: Player Identity

**Goal**: Every track in a processed play can be attributed to a specific player, with the cheapest workflow that suffices
**Depends on**: Phase 2.3
**Requirements**: REQ-S2-05
**Success Criteria** (what must be TRUE):

  1. Manual workflow works first: tracking continuity + team clustering + hand-assigning ~10 tracklets per play takes minutes at 5v5
  2. Automation (jersey-number pipeline: legibility filter → torso crop → PARSeq → tracklet voting; or VLM reads on keyframes) is evaluated only if the manual flow proves too slow, and the decision is documented

**Plans**: TBD

### Phase 2.5: Broadcast Footage (STRETCH, deferred)

**Goal**: Extend tracking to TV/side-view footage as a third domain — only when Strand 2 core (2.1–2.3) is in operation
**Depends on**: Phases 2.1–2.3 operational
**Requirements**: REQ-S2-06
**Success Criteria** (what must be TRUE):

  1. TV/side view is integrated as a third domain with a field-keypoint model for moving cameras (roboflow/sports recipe, PnLCalib)
  2. Scope is only opened after an explicit decision that Strand-2 core runs in regular operation — until then this phase stays deferred (sparse flag-field markings make it the hardest sub-problem; scouting need is covered by Strand 1)

**Plans**: TBD

## Progress

**Execution Order:**
1.1 → 1.2 → 1.3 → 1.4 (Strand 1, value first) | 2.0 in parallel from the start → 2.1 (gate) → 2.2 → 2.3 → 2.4 → (2.5 deferred)

**Milestone 1 (tournament readiness):** Phase 1.4 success criterion 5 met AND Phase 2.1 gate decision recorded.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1.1 Data Contract | 3/3 | Complete    | 2026-08-17 |
| 1.2 Repo to Pipeline | 25/25 | Complete   | 2026-08-17 |
| 1.3 Model Retraining | 0/TBD | Not started | - |
| 1.4 Coaching Products | 0/TBD | Not started | - |
| 2.0 Capture Protocol | 0/TBD | Not started | - |
| 2.1 CV Pilot (Gate) | 0/TBD | Not started | - |
| 2.2 Dataset Buildout | 0/TBD | Not started (gated) | - |
| 2.3 Coaching Metrics | 0/TBD | Not started (gated) | - |
| 2.4 Player Identity | 0/TBD | Not started (gated) | - |
| 2.5 Broadcast Footage | 0/TBD | Deferred (stretch) | - |
