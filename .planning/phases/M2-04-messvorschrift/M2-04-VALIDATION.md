---
phase: M2-04
slug: messvorschrift
status: planned
nyquist_compliant: true
wave_0_complete: true
created: 2026-09-02
---

# Phase M2-04 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Skeleton from M2-04-RESEARCH.md §Validation Architecture; per-plan map filled by the planner
> (2026-09-02, three plans in three waves).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, uv-managed) |
| **Config file** | pyproject.toml (`[tool.pytest.ini_options]`, `testpaths=["tests"]`, `addopts="-q"`) |
| **Quick run command** | `uv run pytest tests/test_m2_metric.py tests/test_hackathon_scoring.py tests/test_m2_baseline_docs.py -x -q` (first file created by plan M2-04-01) |
| **Full suite command** | `uv run pytest -q` |
| **Estimated runtime** | quick < 30 s; full ~6–9 min |

---

## Sampling Rate

- **After every task commit:** quick command (the subset owned by that plan)
- **After every plan wave:** quick command + the scorer's own CLI run on the dev split (seconds, no torch)
- **Before `/gsd:verify-work`:** full suite green
- **Max feedback latency:** 120 seconds

---

## Per-Plan Verification Map

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|------------------------|-----------|-------------------|-------------|--------|
| M2-04-01 | 1 | METR-01 | Metric computable from tracks alone; the identity-swap blind spot is an executable test; guard metric labelled diagnostic; empty clip is not scored as perfect; vault CSV (semicolon/cp1252/CRLF) read correctly and left byte-identical; normalised copy never inside the repo; partial review yields a reviewed-only rate with its own n; label-based layer adds no dependency | unit | `uv run pytest tests/test_m2_metric.py -q` | ❌ new (`tests/test_m2_metric.py`) | ⬜ pending |
| M2-04-02 | 2 | METR-01, METR-02 | One run emits both metrics per split with their own n; test labels enter only via `--review-test`; wrong-split tracks fail loudly; legacy report keys frozen (M2-2 numbers cannot drift); blind-spot text has exactly one source and appears in stdout, JSON and Markdown | integration (subprocess CLI) + real-data run | `uv run pytest tests/test_hackathon_scoring.py -q` | ✅ extend | ⬜ pending |
| M2-04-03 | 3 | METR-03, METR-04 | Comparison uses existing M2-2 per-clip rows (no re-run); every documented continuous value recomputed from `per_clip.csv`; GTA over-merge caveat repeated verbatim; no `data/private/` path in any doc; METR-03 wording is the user's decision, not the executor's | contract (doc/CSV drift) + blocking checkpoint | `uv run pytest tests/test_m2_baseline_docs.py -q` | ✅ extend | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

No separate Wave 0 run is needed: pytest and the three test files' conventions already exist, and
`tests/test_m2_metric.py` is created in the first task of the first plan (M2-04-01 Task 1) together
with the module it tests. Every subsequent task extends an existing, already-green file.

`motmetrics` (MIT, verified in RESEARCH.md's audit table) is **not installed** and must not be
installed in this phase — `pyproject.toml`/`uv.lock` are outside the phase's file-collision guard.
The label-based interface tests are therefore split in two: the pure frame-event/error-path tests
always run, and the IDF1/MOTA assertions are `pytest.importorskip("motmetrics")`-guarded and are
expected to SKIP. Adding the dev dependency is M2-3 follow-up work.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| METR-03 wording (acceptance criterion vs direction vs diagnostic) | METR-03 | Changes the submitted challenge's parameters — user's call toward BWI | Blocking checkpoint, task 3 of plan M2-04-03 |
| Plausibility of the continuous metric on 3 clips | METR-01 | Whether fragment counts match what a human sees in the overlay | After plan M2-04-02's real run: open the overlays of the three dev clips with the highest and lowest `fragments_per_expected_player` from `data/processed/m2-04/report.json` |

---

## Validation Sign-Off

- [x] All auto tasks have `<automated>` verify commands (< 30 s, no watch mode)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] No watch-mode flags
- [x] Feedback latency < 120 s
- [x] `nyquist_compliant: true` set in frontmatter (planner set after filling the map)

**Approval:** planner, 2026-09-02
