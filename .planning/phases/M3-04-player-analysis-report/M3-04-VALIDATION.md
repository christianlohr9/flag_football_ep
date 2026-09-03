---
phase: M3-04
slug: player-analysis-report
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-09-03
---

# Phase M3-04 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Skeleton from M3-04-RESEARCH.md §Validation Architecture; per-plan map filled by the planner.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, uv-managed) |
| **Config file** | pyproject.toml (`testpaths = ["tests"]`, `addopts = "-q"`) |
| **Quick run command** | wave 1: `uv run pytest tests/test_features_explosiveness.py tests/test_reference_hc_splits.py -x -q` · waves 2-3: `uv run pytest tests/test_reports_player_analysis.py -x -q` · wave 4: `uv run pytest tests/test_reports_player_analysis_page.py tests/test_cli_report.py tests/test_ingest_hc_workbook.py -q` · wave 5: `uv run pytest tests/test_m3_player_analysis_docs.py tests/test_m3_explosiveness_docs.py tests/test_m3_hc_pii.py -q` |
| **Full suite command** | `uv run pytest tests -q` |
| **Estimated runtime** | quick < 25 s; full ~6–9 min |

---

## Sampling Rate

- **After every task commit:** the wave's quick command
- **After every plan wave:** the wave's quick command, plus from wave 4 on `uv run ffep report --product player-analysis --skip-ingest` (the real render must succeed and stay inside the REQ-S1-16 budget)
- **Before `/gsd:verify-work`:** full suite green
- **Max feedback latency:** 30 seconds for the quick commands, 120 seconds worst case

---

## Per-Plan Verification Map

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|------------------------|-----------|-------------------|-------------|--------|
| M3-04-01 T1 | 1 | HC-05 | Every head-coach metric uses the denominator his own formula cell uses: Attempts excludes sacks, Efficiency divides by Attempts + Carries, and the older Drops reading stays computable as a clearly named second reading | unit | `uv run pytest tests/test_features_explosiveness.py -q` plus the inline sack-exclusion probe in the plan | ✅ exists (extended) | ⬜ |
| M3-04-01 T2 | 1 | HC-05 | Published numbers and published prose move together; a rerun cannot move the calibrated threshold under an already-reviewed document | integration + doc gate | `uv run pytest tests/test_m3_explosiveness_docs.py tests/test_m3_hc_pii.py -q` plus the `calibration.json`-untouched diff gate | ✅ exists (extended) | ⬜ |
| M3-04-02 T1 | 1 | HC-05 | The camp windows are maintained data with cited provenance, not magic numbers; overlapping or malformed windows are rejected by name; the Camp IV/VI dispute is stored as `conflict`, never silently decided | unit | `uv run python -c` loader assertion in the plan | ❌ Wave 0 (`data/reference/hc_splits.csv`) | ⬜ |
| M3-04-02 T2 | 1 | HC-05 | A game is assigned a camp only when its rows are fully contained in one window; every other case gets a distinct named status instead of a guess | unit | `uv run pytest tests/test_reference_hc_splits.py tests/test_m3_hc_pii.py -q` | ❌ Wave 0 (`tests/test_reference_hc_splits.py`) | ⬜ |
| M3-04-03 T1 | 2 | HC-05 | Thirteen of his columns reproduce their formula cells; sacks stay out of Attempts, runs stay out of every pass column, and a rate on zero attempts is null rather than his `iferror` zero | unit | `uv run pytest tests/test_reports_player_analysis.py -q` | ❌ Wave 0 (`tests/test_reports_player_analysis.py`) | ⬜ |
| M3-04-03 T2 | 2 | HC-05 | A column that cannot be computed is named in German, never approximated: no drop proxy, no zero-drops assumption, no Adj Comp % that silently equals Comp % | unit | `uv run pytest tests/test_reports_player_analysis.py tests/test_features_explosiveness.py -q` | ❌ Wave 0 | ⬜ |
| M3-04-04 T1 | 3 | HC-05 | Our metrics are read from the M3-3 module with n, CI, muted flag and shrunk rate intact — never recomputed, never re-thresholded; a missing calibration degrades to a notice | unit | `uv run pytest tests/test_reports_player_analysis.py -q` | ❌ Wave 0 | ⬜ |
| M3-04-04 T2 | 3 | HC-05 | Camp sections discriminate (two windows produce different counts), unresolved games are listed rather than absorbed, and the object builds on a corpus with zero head-coach rows without raising | unit + real-corpus smoke | `uv run pytest tests/test_reports_player_analysis.py -q` plus the real-parquet `build_player_analysis_data` invocation in the plan | ❌ Wave 0 | ⬜ |
| M3-04-05 T1 | 4 | HC-05 | Charted free text cannot inject into the rendered page (autoescape, no `\|safe`); unavailable columns render a named state distinguishable from zero; a failing chart never fails the page | unit + grep gate | `uv run pytest tests/test_reports_player_analysis_page.py -q` plus the `\|safe` grep gate | ❌ Wave 0 (`tests/test_reports_player_analysis_page.py`) | ⬜ |
| M3-04-05 T2 | 4 | HC-05 | A fifth product cannot take the other four down; the CLI help, the product tuple and the documentation agree; the run stays inside the ten-minute budget and nothing under `reports/` is committed | unit + integration | `uv run pytest tests/test_cli_report.py -q` plus `uv run ffep report --product player-analysis --skip-ingest` | ✅ exists (product assertions updated) | ⬜ |
| M3-04-06 T1 | 4 | HC-05 | The Drop column becomes canonical without any other ingest behaviour moving; the text dtype preserves the text-only semantics of his own `COUNTIFS("*")` criterion | unit | `uv run pytest tests/test_ingest_hc_workbook.py -q` plus the canonical/rename assertion and the two-file `git diff --stat` gate | ✅ exists (extended) | ⬜ |
| M3-04-06 T2 | 4 | HC-05 | The three adjusted columns switch from named-unavailable to computed by data presence alone, with no report-code change | unit (cross-layer) | `uv run pytest tests/test_m3_drop_column.py tests/test_reports_player_analysis.py -q` | ❌ Wave 0 (`tests/test_m3_drop_column.py`) | ⬜ |
| M3-04-07 T1 | 5 | HC-05 | Three phases append to one question document without any of them renumbering or breaking another's counters | doc gate | `uv run pytest tests/test_m3_player_analysis_docs.py tests/test_m3_explosiveness_docs.py -q` plus the six-`## Frage` grep gate | ❌ Wave 0 (`tests/test_m3_player_analysis_docs.py`) | ⬜ |
| M3-04-07 T2 | 5 | HC-05 | Every link in the handout resolves, every rate carries a denominator, and no roster name reaches a committed document | doc gate | `uv run pytest tests/test_m3_player_analysis_docs.py tests/test_m3_hc_pii.py -q` | ❌ Wave 0 | ⬜ |
| M3-04-07 T3 | 5 | HC-05 | A human reads the handout, the report and the three questions before anything goes to the head coach | manual | checkpoint (human-verify) — no automated gate by design; preceded and followed by automated doc gates | n/a | ⬜ |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Created inside the phase, in this order:

- `data/reference/hc_splits.csv` — plan M3-04-02 task 1. Five cited row windows; the only place a workbook row number lives.
- `tests/test_reference_hc_splits.py` — plan M3-04-02 task 2. Synthetic games/splits frames only; never reads `data/reference/hc_games.csv`'s content (M3-02-04 is rewriting it).
- `tests/test_reports_player_analysis.py` — plan M3-04-03 task 1 creates it; M3-04-03 task 2 and both M3-04-04 tasks extend it. Frames come from `flag_football_ep.testing.canonical_plays` / `canonical_plays_with_scores` with `overrides`, mirroring `tests/test_reports_own_team.py`. Synthetic player labels only.
- `tests/test_reports_player_analysis_page.py` — plan M3-04-05 task 1. Rendered-string assertions mirroring `tests/test_reports_own_team_page.py`.
- `tests/test_m3_drop_column.py` — plan M3-04-06 task 2. Cross-layer guard (conform → report availability).
- `tests/test_m3_player_analysis_docs.py` — plan M3-04-07 tasks 1 and 2. Structural/link/PII guard modelled on `tests/test_m3_explosiveness_docs.py`.
- `tests/conftest.py` is NOT touched — owned by phase 01.2 plan 01. Module-local fixtures and the `testing.py` factories only.
- **Correction to RESEARCH §Validation Architecture:** it names `tests/test_reports_build.py` as an existing file to extend. That file does not exist; the `PRODUCTS`/build coverage lives in `tests/test_cli_report.py`, which asserts the four-product tuple verbatim and must be updated by M3-04-05 task 2. Do not create `tests/test_reports_build.py`.
- Framework install: none. pytest, polars, scipy, jinja2, matplotlib and openpyxl are all existing project dependencies (M3-04-RESEARCH §Standard Stack: zero new packages, §Package Legitimacy Audit: not applicable).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Whether the rendered page reads like his tab | HC-05 | Layout fidelity to a hand-built spreadsheet is a judgement about his working habits, not an assertable property | Plan M3-04-07 task 3, step 1: open `reports/latest/player-analysis.html` |
| Tone of the correction paragraph (we corrected ourselves against his sheet) | HC-05 | A relationship judgement; no assertion decides whether it is acceptable to send | Plan M3-04-07 task 3, step 2 |
| Whether Fragen 7-9 are the questions worth asking in October | HC-05 | Only the user knows what he wants answered and how he communicates with the head coach | Plan M3-04-07 task 3, step 3 |
| Whether row window 3001-4000 is Camp IV or Camp VI | HC-05 | Two of his own tabs name the same window differently; only he can decide | Frage 7. Until answered: `hc_splits.csv` carries `label_status = conflict` and the section renders both names |
| What `Data!Y` (header "B") encodes | HC-05 | The column has no documented meaning anywhere in the workbook or the codebase | Frage 8. Until answered: Air Yards omit his subtraction term and the report states the deviation in a footnote |
| How the Drop column is filled (text vs number; does a drop also count as an incompletion) | HC-05 | His own `COUNTIFS(..., "*")` criterion counts text only, so his sheet may already be ignoring numeric marks | Frage 9. Until answered: `drop` is stored as raw text and the report's flag (non-empty text) is documented as more permissive than his formula |
| Whether the head coach's rows are actually in the scored corpus | HC-05 | Owned by M3-2 (plan 04 ingest run, plan 05 rescoring), outside this phase | The report prints `n_hc_rows` on every run; zero is rendered as a named empty state, never as a silent blank |

---

## Validation Sign-Off

- [x] All auto tasks have `<automated>` verify commands (< 30 s each; the slowest gate is the full suite and the real `ffep report` run, both wave-level rather than task-level)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (the only task without one is M3-04-07's human-verify checkpoint, immediately preceded by two automated doc gates)
- [x] No watch-mode flags
- [x] Feedback latency < 120 s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planned 2026-09-03
