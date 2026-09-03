---
phase: M3-01
slug: hc-workbook-ingest
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-09-03
---

# Phase M3-01 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Skeleton from M3-01-RESEARCH.md §Validation Architecture; per-plan map filled by the planner.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, uv-managed) |
| **Config file** | pyproject.toml |
| **Quick run command** | `uv run pytest tests/test_ingest_hc_workbook.py tests/test_validation_checks.py -x -q` (first file created by this phase) |
| **Full suite command** | `uv run pytest tests -q` |
| **Estimated runtime** | quick < 30 s; full ~6–9 min |

---

## Sampling Rate

- **After every task commit:** quick command
- **After every plan wave:** quick command + `ffep ingest` dry run on the synthetic workbook fixture
- **Before `/gsd:verify-work`:** full suite green
- **Max feedback latency:** 120 seconds

---

## Per-Plan Verification Map

(filled by the planner)

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|------------------------|-----------|-------------------|-------------|--------|

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Synthetic workbook fixtures (openpyxl-generated in tests, synthetic names only — never the real HC files, which are gitignored PII) are created in the first plan. `openpyxl` becomes a project dependency in this phase (planner decides group).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Empty `Data` tab of the EC-2025 workbook | HC-01 | Only the HC knows whether rows are expected | Ask the HC; checkpoint in plan 01 |
| Column mapping of the mixed-layout blocks in the Scoring-Probability workbook | HC-01 | Ambiguous from data alone | Ask the HC; checkpoint in plan 01 |
| RESULT vocabulary extension (6 new tokens) | HC-01 | Contract amendment is a data-contract decision | User decision (v1.2 vs tok_unknown) |

---

## Validation Sign-Off

- [ ] All auto tasks have `<automated>` verify commands (< 30 s, no watch mode)
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] No watch-mode flags
- [ ] Feedback latency < 120 s
- [ ] `nyquist_compliant: true` set in frontmatter (planner sets after filling the map)

**Approval:** pending
