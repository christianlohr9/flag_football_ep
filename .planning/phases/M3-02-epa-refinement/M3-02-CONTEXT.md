# Phase M3-2: EPA-Refinement - Context

**Gathered:** 2026-09-03
**Status:** Ready for planning (one user decision pending — see Open Decision)
**Source:** Express path from `docs/hc-notes-2026-09-03.md` + M3-1 real-run findings

<domain>
## Phase Boundary

EP/WP is retrained on the enlarged corpus (our Hudl/IFAF plays + the head coach's workbook plays) with the established methodology (GroupKFold by `game_id`, calibration report, competition-tier eval, MLflow-versioned champion), and set side by side with the HC's own "Scoring Probability by Situation" method (SP/EP by down & distance, raw/clustered/weighted) so he sees where the model agrees and where it improves (HC-03). Delivery anchor: the October 2026 HC sync — this is THE item he asked for first.

Prerequisite reality from M3-1's real run (`docs/hc-workbook-ingest.md`, todo `2026-09-03-m3-2-eingangsbefunde-hc-korpus.md`): today **0 HC rows reach `plays.parquet`** — 100% of HC games quarantine on `half_assigned` (workbooks carry no half information), and the game segmentation over-splits (2,128 "games" from 19,901 rows ≈ per-drive). Both must be fixed in this phase's first wave before any training.

Not this phase: the explosiveness/efficiency definition (M3-3), the report product (M3-4), any CV/hackathon file, the two open HC questions (answers land via plan M3-01-01's checkpoint).
</domain>

<decisions>
## Implementation Decisions

- **EPA-D01 Fix the corpus first, then train.** Wave 1 = (a) `half` strategy for HC rows, (b) game segmentation fix + `hc_games.csv` refill; only then the training waves. Row counts before/after are reported per source.
- **EPA-D02 Methodology is locked from 1.3:** GroupKFold over `game_id` (D-07), reliability curves + log-loss vs naive baseline (REQ-S1-08), competition tier as covariate, MLflow registry with champion alias — nothing silently overwritten (REQ-S1-11). New corpus = new data hash = new run; the previous champion stays available for the comparison.
- **EPA-D03 HC comparison is tabular and honest:** reproduce his SP/EP-by-down-&-distance tables from HIS `Data` rows and from OUR canonical corpus with the same binning; place model EP next to them; report n per cell; explicitly show where small-sample cells make his point estimates noisy (that is the argument for the model) and where the model disagrees with his intuition (his call to review). Snapshot his tables read-only into `data/reference/hc_sp_tables/*.csv` (aggregates, no names) for reproducibility.
- **EPA-D04 Source provenance in training:** `source` column carried through; ablation "with vs without HC rows" on the frozen GroupKFold folds so the HC sees what his data adds.
- **EPA-D05 Deliverable for the sync:** German `docs/epa-refinement-2026-10.md` (method, corpus counts, calibration, comparison tables, ablation, open questions) — M3-4 turns it into the handout.

### Open Decision (user)
- **`half` for HC rows** — options: (a) allow unknown `half` for HC rows and treat it as its own category/imputed in the EP model (the `half` feature was adopted for EP only in 1.3); (b) heuristic half boundaries from play sequence (PLAY # jumps, drive patterns), marked provisional; (c) ask the HC for half markers (Frage 4). Orchestrator recommendation: (a) for October, (c) in parallel. Planner: plan (a) as the default path with (b)/(c) as documented alternatives unless the user decides otherwise before planning completes.

### Claude's Discretion
- Segmentation rule for HC games (team-name/date columns, sheet structure), binning for the D&D tables, ablation design details, MLflow experiment naming.
</decisions>

<canonical_refs>
## Canonical References
- `docs/hc-notes-2026-09-03.md` — HC priority ("wichtigster Punkt bis Oktober")
- `docs/hc-workbook-ingest.md`, `.planning/phases/M3-01-hc-workbook-ingest/M3-01-04-SUMMARY.md` — real-run counts, quarantine causes, duplicates
- `.planning/todos/pending/2026-09-03-m3-2-eingangsbefunde-hc-korpus.md` — the two entry findings
- `docs/model-training.md`, `src/flag_football_ep/model/{train,evaluate,experiments,registry,mlflow_store}.py` — the 1.3 methodology to reuse
- `src/flag_football_ep/ingest/hc_workbook.py`, `hc_dedupe.py`, `validation/checks.py` (`half_assigned` check), `data/reference/half_boundaries.csv`
- `data/raw/hc_files/Scoring Probability by Situation 2023-2026.xlsx` (gitignored) — the HC method to compare against (tabs `SP by D&D`, `EP by D&D`, clustered/weighted variants, `Reg`)
- `.planning/PROJECT.md` D-07, C-07 (v1.2), C-08
</canonical_refs>

<specifics>
## Specific Ideas
- HC: "mittlerweile haben wir noch mehr Datenpunkte und können diese Berechnung noch professioneller und nachhaltiger gestalten" — the pitch for October is reproducibility + calibration + honest n, not a fancier model.
</specifics>

<deferred>
## Deferred Ideas
- Real game clock for WP (BL-01 OCR); win-driver analysis (BL-04); explosiveness (M3-3).
</deferred>

---
*Phase: M3-02-EPA-Refinement*
*Context gathered: 2026-09-03 via express path*
