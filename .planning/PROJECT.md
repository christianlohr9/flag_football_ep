# flag-football-analytics

## What This Is

Analytics support for the German women's flag football national team (5v5), built and run by one person. Two strands: (1) turn the maintained Hudl yearly export plus sportapp.fi/IFAF tournament data into a reproducible polars + XGBoost EP/WP pipeline that auto-generates scouting products the HC actually uses, and (2) a computer-vision player-tracking pilot on controlled drone footage of trainings/scrimmages that either proves itself against hard gate criteria or is stopped cheaply.

## Core Value

Before the next national-team camp/tournament, the HC has an auto-generated tendency report for every group opponent, produced from raw exports in under 10 minutes — and the CV tracking pilot has reached an explicit go/no-go decision.

## Requirements

### Validated

- [x] Data contract: core-plus-optional column model (v1.1), fixed `RESULT` vocabulary, derivation of manual fields — analyst ratification deferred (DEFERRED-ANALYST, tracked todo) (REQ-S1-01) — Validated in Phase 1.1: Data Contract
- [x] Real time data: unavailability explicitly recorded — exports carry no time info; synthetic time stays per C-08, Phase 1.4 WP charts flagged (REQ-S1-02) — Validated in Phase 1.1: Data Contract
- [x] Defense charting fields: sighted vocabulary for `DEF FRONT`/`COVERAGE` adopted from 7 real exports, `BLITZ` documented as person-name column, flag-pull causer explicitly skipped (REQ-S1-03) — Validated in Phase 1.1: Data Contract
- [x] Notebook logic migrated into the `src/flag_football_ep` package (REQ-S1-04) — Validated in Phase 1.2: Repo to Pipeline
- [x] Ingest CLI: Hudl export folder → canonical Parquet + per-game validation report (REQ-S1-05) — Validated in Phase 1.2: Repo to Pipeline
- [x] Hudl + sportapp.fi/IFAF sources merged into one schema; raw files under `data/` (REQ-S1-06) — Validated in Phase 1.2: Repo to Pipeline

### Active

**Strand 1 — Hudl export, EP/WP retraining, scouting products**
- [ ] GroupKFold over `game_id` replaces play-level train_test_split (REQ-S1-07)
- [ ] Calibration report: reliability curves per class, log-loss vs naive baseline (REQ-S1-08)
- [ ] Feature candidates re-tested on grouped CV (REQ-S1-09)
- [ ] Empirical PAT baselines + break-even chart (REQ-S1-10)
- [ ] Model versioning (date + training-data hash) (REQ-S1-11)
- [ ] Auto-generated opponent tendency report per team, HTML/PDF (REQ-S1-12)
- [ ] Own-team efficiency report: EPA by formation/play-call/route/player (REQ-S1-13)
- [ ] Decision charts: PAT break-even, 4th-down conversion by distance (REQ-S1-14)
- [ ] Win-probability charts per game as review tool (REQ-S1-15)
- [ ] Raw export → report in < 10 minutes for every group opponent (REQ-S1-16)

**Strand 2 — CV object detection & player tracking**
- [ ] Material inventory + drone capture protocol + legal clearance + sync convention (REQ-S2-01)
- [ ] CV pilot on one scrimmage with explicit go/no-go gate (REQ-S2-02)
- [ ] Dataset buildout to 1,500–3,000 verified frames, 60/40 domain mix (REQ-S2-03)
- [ ] Coaching metrics on XY tracks: snap detection, routes, separation, spacing, PBP join (REQ-S2-04)
- [ ] Player identity via manual tracklet assignment, automation only if needed (REQ-S2-05)

### Out of Scope

- Live/in-game tooling — Strand 1 non-goal (D-08); solo developer, no game-day operations
- CV on opponent/third-party footage as foundation — opponent analysis stays PBP charting (D-01); broadcast footage is stretch Phase 2.5 only (REQ-S2-06, deferred)
- Ball detection in early CV phases — small, motion-blurred; play structure comes from snap detection + PBP join (C-12)
- Any AGPL component (Ultralytics YOLO) — fine-tuned weights covered by AGPL, problematic for federation use (D-02)
- Field-keypoint model for moving cameras — hardest sub-problem given sparse flag-field markings; deferred with Phase 2.5 (D-05)

## Context

- **Team:** German women's flag football national team. Stakeholders: HC (primary consumer of reports/decision charts) and the Videoanalyst (owns Hudl charting; data-contract partner in 1.1, capture-protocol ally in 2.0).
- **Calendar:** IFAF Flag Football World Championship 2026 in Düsseldorf is the nearest competitive milestone; LA28 (flag football is Olympic) is the horizon.
- **Existing codebase:** nflfastR-style polars + XGBoost EP/WP pipeline; helpers in `Python/` (incl. `helper_add_ep_wp.py` with hard-coded PAT baselines 50%/46%), started package at `src/flag_football_ep`, models overwritten as `ep_model.pkl`. README self-flags WP time handling as "flawful bc of missing times".
- **Data assets:** `data_raw.csv` (Hudl, 47 games / ~3,700 plays, rich charting: `OFF FORM`, `Off Str`, `OFF PLAY`, `TARGET ROUTE`, `RECEIVED BY`, `Thrown By`, `YAC`, `GN/LS`), `pbp_wc24_static.csv` (Worlds PBP, no rich fields), sportapp.fi/IFAF as second PBP source, `games_plays.csv` awaiting consolidation.
- **Industry white space:** No public dataset, open-source project, or product for flag-football player tracking exists. Hybrid (CV + manual charting) is the industry standard; manual PBP charting scales for a single person at 5v5. Producing own tracking data is a real edge.

## Constraints

- **Runtime**: Python 3.12 managed with uv (existing pyproject.toml); notebooks migrate into `src/flag_football_ep` — user-supplied target
- **Time budget**: Solo developer, evenings/weekends; every Strand-2 phase has a hard gate; pilot decides after ~2 weekends (C-04)
- **Compute**: Colab Pro / consumer RTX GPU; training in tens of dollars total; inference ~15–25 min/game on T4 (C-10)
- **License**: No AGPL anywhere in the CV stack; RF-DETR (Apache 2.0), CVAT + SAM2, Grounding DINO + autodistill, OC-SORT/BoT-SORT via BoxMOT, SigLIP + UMAP + KMeans (C-06)
- **Protocol**: Drone banned at official games — CV capture limited to trainings/scrimmages; EU-Drohnenverordnung and DSGVO consent must be cleared before capture; ~20–25 min battery → swap protocol between drives (C-01, C-02, C-03, C-11)
- **Schema**: `RESULT` restricted to fixed vocabulary (`Rush`, `Penalty`, `KNEEL`, `Sack`, `Interception`, `Complete`, `Incomplete`, `TD`, `Def TD`, `Good`, `Safety`); per-game validation ranges (downs 0–4, `yardline_50` in [0, 50], monotonic drive IDs, gapless plays, score reconstruction) (C-07)
- **Data quality**: `half_seconds_remaining` currently synthetic (`1200 / max(play_id)`); real time data is the biggest WP quality lever (C-08)
- **CV methodology**: Drone footage is its own detection regime (small objects, oblique > top-down); per-domain eval and inference settings mandatory; pooled mAP hides domain collapse (C-05)
- **Pilot gate**: ≥ 90% of a play trackable without ID switch, position error ~≤ 1 m, inference < 1 h/game; clear miss → back to Phase 2.0, not more labeling (C-09)

## Key Decisions

<decisions>

All eight decisions below are **locked** (synthesized from docs/plan-1-analytics-refresh.md, docs/plan-2-cv-tracking.md, docs/research-notes.md; see .planning/intel/decisions.md).

| ID | Decision | Rationale | Outcome |
|----|----------|-----------|---------|
| D-01 | Two-strand split: own-team CV on drone footage vs opponent analysis via PBP charting | Collapses the heterogeneous-video problem to 1–2 controlled domains; matches industry practice (PFF, Hudl IQ, colleges) | — Pending |
| D-02 | Apache-2.0-only license policy: RF-DETR over AGPL Ultralytics; fallback D-FINE + DEIM | AGPL covers fine-tuned weights — problematic once federation or third parties use output | — Pending |
| D-03 | Drone in fixed hover as primary capture domain (~30–60 m, 4K, slightly oblique); elevated tripod/Veo-class as second domain, ~60/40 dataset mix | Oblique beats top-down (TeamTrack evidence); drone banned at official games so second domain needed from the start | — Pending |
| D-04 | One detector across domains, evaluated per domain with per-domain inference settings | Pooled mAP hides domain collapse; two pipelines waste solo-dev time | — Pending |
| D-05 | Manual 4–8-point homography per hover position; field-keypoint model deferred to stretch 2.5 | Near-free for static setups; sparse flag markings make keypoint models the hardest sub-problem | — Pending |
| D-06 | Hard go/no-go gates; the pilot decides investment. Clear miss → back to 2.0, not more labeling | Solo time budget; avoids sunk-cost labeling on a broken capture setup | — Pending |
| D-07 | GroupKFold over `game_id` for EP/WP evaluation | Play-level splits leak same-game plays into train and test; nflfastR precedent (LOSO over seasons) | — Pending |
| D-08 | Strand-1 non-goals: no live/in-game tooling, no CV dependency | Strand 1 must deliver value entirely without Strand 2 | — Pending |

</decisions>

---
*Last updated: 2026-08-18 after Phase 1.2 completion (notebook logic migrated to `src/flag_football_ep`, `ffep` CLI with validated four-source ingest into canonical Parquet, per-game validation report)*
