---
phase: M2-02
slug: ehrliche-baseline
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-09-01
---

# Phase M2-02 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Generated from M2-02-RESEARCH.md §Validation Architecture and the three plans' verify commands.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, uv-managed) |
| **Config file** | pyproject.toml |
| **Quick run command** | `uv run pytest tests/test_m2_baseline_measurement.py tests/test_m2_gta_adapter.py tests/test_m2_baseline_docs.py -x -q` (files exist per wave progress) |
| **Full suite command** | `uv run pytest tests -q` |
| **Estimated runtime** | quick < 30 s; full ~6–9 min |

---

## Sampling Rate

- **After every task commit:** run the quick command restricted to the test files that exist so far
- **After every plan/wave:** quick command over all M2 test files + spot-run of the plan's own CLI verify commands
- **Before `/gsd:verify-work`:** full suite green
- **Max feedback latency:** 120 seconds (measurement CLIs themselves are minutes-scale and are their own evidence)

---

## Per-Plan Verification Map

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|------------------------|-----------|-------------------|-------------|--------|
| M2-02-01 | 1 | BASE-01, BASE-03 | No fabricated human rates (human_pass only for BoT-SORT); same detections for every method; empty-frame update() replay | unit + CLI | `uv run pytest tests/test_m2_baseline_measurement.py -x -q` | ❌ new | ⬜ pending |
| M2-02-02 | 2 | BASE-01, BASE-03 | MIT-only vendoring with SHA pin; OSNet checkpoint SHA-256 re-verified per run; tracks parquet byte-identical (read-only) | unit + CLI | `uv run pytest tests/test_m2_gta_adapter.py -x -q` | ❌ new | ⬜ pending |
| M2-02-03 | 3 | BASE-02, BASE-03, BASE-04 | 77% only in history-marked sentences; Deep-EIoU skip documented; BASE-04 three-case trigger rule auditable | contract | `uv run pytest tests/test_m2_baseline_docs.py -x -q` | ❌ new | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers the framework; each new test module is created inside the plan that introduces the behaviour it covers (M2-02-01 → test_m2_baseline_measurement.py; M2-02-02 → test_m2_gta_adapter.py; M2-02-03 → test_m2_baseline_docs.py). No separate Wave 0 plan required.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| BASE-04 target-mark decision | BASE-04 | The 90% mark of the SUBMITTED challenge is the user's call toward BWI | Plan M2-02-03 Task 3 checkpoint: three-case trigger rule (A/B pause with options, C record finding) |
| Plausibility glance at result table | BASE-01/02 | Sanity of measured numbers against overlays is human judgment | Read docs/baseline-messung.md table; spot-open one overlay if a number looks implausible |

---

## Validation Sign-Off

- [ ] All auto tasks have `<automated>` verify commands (< 30 s, no watch mode) — confirmed at planning time by the plan-checker
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] No watch-mode flags
- [ ] Feedback latency < 120 s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
