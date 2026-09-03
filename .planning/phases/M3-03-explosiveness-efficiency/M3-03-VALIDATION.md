---
phase: M3-03
slug: explosiveness-efficiency
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-09-03
---

# Phase M3-03 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Skeleton from M3-03-RESEARCH.md §Validation Architecture; per-plan map filled by the planner.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, uv-managed) |
| **Config file** | pyproject.toml (`testpaths = ["tests"]`, `addopts = "-q"`) |
| **Quick run command** | `uv run pytest tests/test_features_explosiveness.py -x -q` (wave 1) / `uv run pytest tests/test_charts_explosiveness.py tests/test_m3_explosiveness_docs.py tests/test_m3_hc_pii.py -q` (waves 2-3) |
| **Full suite command** | `uv run pytest tests -q` |
| **Estimated runtime** | quick < 20 s; full ~6–9 min |

---

## Sampling Rate

- **After every task commit:** the wave's quick command
- **After every plan wave:** quick command plus, from wave 2 on, `uv run python scripts/explosiveness_comparison.py` (rerun must be byte-identical without `--recalibrate`)
- **Before `/gsd:verify-work`:** full suite green
- **Max feedback latency:** 30 seconds for the quick commands, 120 seconds worst case

---

## Per-Plan Verification Map

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|------------------------|-----------|-------------------|-------------|--------|
| M3-03-01 T1 | 1 | HC-04 | The head coach's number is reproduced, not reinterpreted: the workbook baseline provably has no EPA term, the verbal rule is a separate labelled metric, PAT rows can never deflate a rate, a missing column fails loudly, and the opaque charted `Efficiency` column is never re-derived by guess | unit | `uv run pytest tests/test_features_explosiveness.py -x -q` plus the no-EPA-in-baseline grep gate in the plan | ❌ Wave 0 | ⬜ |
| M3-03-01 T2 | 1 | HC-04 | "Explosive" is defined by a threshold derived from our own corpus and stored with the corpus fingerprint that produced it; an 11-yard play is no longer a hard no | unit | `uv run pytest tests/test_features_explosiveness.py -x -q` plus the calibration JSON round-trip check in the plan | ❌ Wave 0 | ⬜ |
| M3-03-01 T3 | 1 | HC-04 | Every rate carries n, a Clopper-Pearson interval and a muted flag from the project's one existing convention; shrinkage is an added column, never a silent replacement; denominators are labelled | unit | `uv run pytest tests/test_features_explosiveness.py tests/test_reports_aggregate.py tests/test_features_mutations.py -q` | ❌ Wave 0 | ⬜ |
| M3-03-02 T1 | 2 | HC-04 | Committed tables carry pseudonyms only and the key stays gitignored; an absent `efficiency` column and absent head-coach rows are reported as named findings, not silent gaps; a rerun cannot move the published threshold | integration | `uv run python scripts/explosiveness_comparison.py` plus the artifact/pseudonym/gitignore assertions in the plan | ❌ Wave 0 (artifacts) | ⬜ |
| M3-03-02 T2 | 2 | HC-04 | Charts are headless, write no file, and mute thin rows rather than dropping them | unit | `uv run pytest tests/test_charts_explosiveness.py tests/test_charts_fourth_down.py tests/test_charts_tendency.py -q` | ❌ Wave 0 | ⬜ |
| M3-03-02 T3 | 2 | HC-04 | No number in the German proposal can drift from the measured CSVs; no roster name reaches any committed artifact of this phase; the three unresolvable questions are written down, not guessed | unit + doc gate | `uv run pytest tests/test_m3_explosiveness_docs.py tests/test_m3_hc_pii.py -x -q` plus the `## Frage` count and section greps in the plan | ❌ Wave 0 | ⬜ |
| M3-03-03 T1 | 3 | HC-04 | A human decides whether and how to tell the head coach that his spreadsheet and his description disagree | manual | checkpoint (human-verify) — no automated gate by design | n/a | ⬜ |
| M3-03-03 T2 | 3 | HC-04 | Review feedback changes prose only; a number that "needs fixing" is a re-measurement, not a hand edit | doc gate | `uv run pytest tests/test_m3_explosiveness_docs.py tests/test_m3_hc_pii.py -q` plus the `Stand:` / docs-only-diff greps in the plan | ❌ Wave 0 | ⬜ |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Created inside the phase, in this order:

- `tests/test_features_explosiveness.py` — plan M3-03-01 task 1 creates it; tasks 2 and 3 extend it. Every frame comes from `flag_football_ep.testing.canonical_plays` / `canonical_plays_with_scores` with targeted `overrides`/`extras`, mirroring `tests/test_features_mutations.py`. Synthetic player labels only (`QB A`, `WR 1`). No test may read `data/processed/plays_scored.parquet` or open anything under `data/raw/hc_files/`.
- `data/reference/explosiveness/calibration.json` + the three comparison CSVs — plan M3-03-02 task 1 generates them from the real corpus; they are the source of truth the doc guard checks against.
- `tests/test_charts_explosiveness.py` — plan M3-03-02 task 2; structure-only assertions, mirroring `tests/test_charts_fourth_down.py`.
- `tests/test_m3_explosiveness_docs.py` — plan M3-03-02 task 3; doc-versus-CSV agreement guard modelled on `tests/test_m2_baseline_docs.py`.
- `tests/conftest.py` is NOT touched — it is owned by phase 01.2 plan 01 and later plans must not edit it. Use module-local fixtures and the `testing.py` factories.
- Framework install: none. pytest, polars, scipy, matplotlib and openpyxl are all existing project dependencies (M3-03-RESEARCH § Standard Stack: zero new packages, § Package Legitimacy Audit: not applicable).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Tone and framing of the finding that the head coach's workbook formula and his verbal description disagree | HC-04 | A relationship judgement about another person's work; no assertion can decide whether this is acceptable to send | Plan M3-03-03 task 1 checkpoint. Until reviewed: `docs/explosiveness-vorschlag.md` is a repository document, not a handout |
| Whether questions 4-6 are the right three questions, and whether they go out with questions 1-3 | HC-04 | Only the user knows what he wants answered at the October sync and how he communicates with the head coach | Same checkpoint, steps 2 and 3. The decision is written into `docs/hc-rueckfragen-2026-09.md` so M3-01 plan 01's forwarding checkpoint sees it |
| Whether the workbook's `Efficiency` column (Data!O) means what we assume | HC-04 | Three down/distance/yards re-derivations each explained under 80 % of the charted values (RESEARCH Pitfall 2); only the head coach knows his charting rule | Frage 5 in `docs/hc-rueckfragen-2026-09.md`. Until answered: `hc_efficiency_table` reproduces the literal formula over an opaque input and supports both denominator readings via an argument — no formula is guessed |
| Whether the yards-only `Explosive %` formula is intentional | HC-04 | The formula cell and his spoken description differ; only he knows which is the real rule | Frage 4, same document. Until answered: both baselines are computed and reported side by side, neither stands in for the other |
| Correctness of the pseudonym-to-player assignment | HC-04 | Requires the local, gitignored key and human knowledge of the squad | `data/processed/m3-03/pseudonym_key.csv`, written by `scripts/explosiveness_comparison.py`. Never committed; never needed for any automated assertion |

---

## Validation Sign-Off

- [x] All auto tasks have `<automated>` verify commands (< 30 s; the slowest is the full-suite phase gate)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (the only task without one is plan 03's human-verify checkpoint, which is immediately followed by an automated task)
- [x] No watch-mode flags
- [x] Feedback latency < 120 s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planned 2026-09-03
