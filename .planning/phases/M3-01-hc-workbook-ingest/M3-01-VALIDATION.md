---
phase: M3-01
slug: hc-workbook-ingest
status: planned
nyquist_compliant: true
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
| **Quick run command** | `uv run pytest tests/test_ingest_hc_workbook.py tests/test_ingest_hc_dedupe.py -x -q` (both files created by this phase) |
| **Full suite command** | `uv run pytest tests -q` |
| **Estimated runtime** | quick < 30 s; full ~6–9 min |

---

## Sampling Rate

- **After every task commit:** quick command (plan 01 uses `tests/test_ingest_hudl.py tests/test_validation_schema.py` instead — its files are the contract, not the HC reader)
- **After every plan wave:** quick command + `uv run ffep ingest --source hc_workbook` once the wiring exists (wave 3)
- **Before `/gsd:verify-work`:** full suite green
- **Max feedback latency:** 120 seconds

---

## Per-Plan Verification Map

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|------------------------|-----------|-------------------|-------------|--------|
| M3-01-01 T1 | 1 | HC-01 | Six head-coach RESULT tokens carry defined semantics instead of degrading; Timeout/Offsetting Penalties never enter EP/WP as plays; contract v1.2 loads warning-free | unit | `uv run pytest tests/test_ingest_hudl.py tests/test_validation_schema.py tests/test_cross_source_vocabulary.py -q` | ✅ (extends existing files) | ⬜ |
| M3-01-01 T2 | 1 | HC-01 | The two unresolvable structural questions are written down, not guessed | doc gate | `grep -c '^## Frage' docs/hc-rueckfragen-2026-09.md` == 3 plus the token/answer greps in the plan | ❌ new doc | ⬜ |
| M3-01-01 T3 | 1 | HC-01 | Questions actually reach the head coach; answers recorded verbatim with date and channel | manual | checkpoint (human-action) — no automated gate by design | n/a | ⬜ |
| M3-01-02 T1 | 1 | HC-01 | A new dependency cannot enter without a licence row; config gains optional keys that break no existing fixture | unit | `uv run pytest tests/test_config.py tests/test_m2_lizenz_inventur.py tests/test_cv_dependencies.py -q` | ✅ | ⬜ |
| M3-01-02 T2 | 1 | HC-01 | An empty `Data` tab is reported as empty, not as zero games; team-name rows are separated from play-number rows before any mapping | unit | `uv run pytest tests/test_ingest_hc_workbook.py -x -q -k "block_segmentation or empty_sheet"` | ❌ Wave 0 | ⬜ |
| M3-01-02 T3 | 1 | HC-01 | A header claim the data contradicts becomes a notice or a DomainViolation, never a silent cast; the unresolved pair-block tail stays null | unit | `uv run pytest tests/test_ingest_hc_workbook.py tests/test_canonical.py -x -q -k "header_and_block_mapping or pair_block_tail or jersey_string or unknown_headers or canonical"` | ❌ Wave 0 | ⬜ |
| M3-01-03 T1 | 2 | HC-01 | Game identity is validated data: duplicate keys, duplicate ids, unknown tiers and non-`hc-` ids are rejected by name | unit | `uv run pytest tests/test_reference.py -q -k "hc_games or mapping"` | ✅ (extends existing file) | ⬜ |
| M3-01-03 T2 | 2 | HC-01 | An unrecognised game gets a provisional id plus an actionable notice instead of being attached to an existing game | unit | `uv run pytest tests/test_ingest_hc_workbook.py -x -q -k "game_segmentation or provisional_game"` | ❌ Wave 0 | ⬜ |
| M3-01-03 T3 | 2 | HC-01 | An HC sheet becomes a canonical frame with `source = hc_workbook:<file>:<sheet>`; mixed name/jersey labels never break the run and never leak | unit + integration | `uv run pytest tests/test_ingest_hc_workbook.py tests/test_canonical.py -x -q` | ❌ Wave 0 | ⬜ |
| M3-01-04 T1 | 3 | HC-02 | Rows are excluded only inside a human-declared pairing; undeclared overlap is reported, not deleted; row conservation asserted | unit | `uv run pytest tests/test_ingest_hc_dedupe.py -x -q` | ❌ Wave 0 | ⬜ |
| M3-01-04 T2 | 3 | HC-01, HC-02 | `ffep ingest` dispatches the source, a missing directory only warns, and a failing HC game is quarantined rather than warned through | integration | `uv run pytest tests/test_pipeline_ingest.py tests/test_ingest_hc_dedupe.py tests/test_cli_smoke.py -q` | ✅ (extends existing file) | ⬜ |
| M3-01-04 T3 | 3 | HC-01, HC-02 | No player name reaches a committed artefact of this phase; the German report names counts, quarantines and open questions | unit + doc gate | `uv run pytest tests/test_m3_hc_pii.py -q` plus the `## Offene Fragen` / `## Wartung` greps in the plan | ❌ Wave 0 | ⬜ |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Created inside the phase, in this order:

- `tests/test_ingest_hc_workbook.py` — plan M3-01-02 task 2 creates it; tasks 3 and both later plan-03 tasks extend it. Fixture workbooks are built in-process with `openpyxl.Workbook()` using synthetic team labels (`Alphaland`, `Betaland`) and synthetic player labels (`Spieler A`, `7`, `25`). No test may open a file under `data/raw/hc_files/`.
- `data/reference/hc_games.csv` — plan M3-01-03 task 1 creates the header; plan M3-01-04 task 2 fills the real rows after the first real run.
- `tests/test_ingest_hc_dedupe.py` — plan M3-01-04 task 1; corpus frames come from `flag_football_ep.testing.canonical_plays`.
- `tests/test_m3_hc_pii.py` — plan M3-01-04 task 3; the gate over every committed artefact of this phase.
- `openpyxl` install — plan M3-01-02 task 1 (`uv add openpyxl`, core dependency, licence row in `docs/lizenz-inventur.md`).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Empty `Data` tab of the EC-2025 workbook | HC-01 | Only the head coach knows whether rows are expected | Frage 1 in `docs/hc-rueckfragen-2026-09.md`; checkpoint in plan M3-01-01 task 3. Until answered: the file is reported as "leer, Antwort ausstehend", never as a source with zero games |
| Column mapping of the mixed-layout blocks in the Scoring-Probability workbook | HC-01 | Ambiguous from the data alone; a wrong offset would swap passer, receiver and gain | Frage 2, same checkpoint. Until answered: every column from `RECEIVED BY` onward stays null for those rows, with a loud notice |
| Semantics of the six new RESULT tokens | HC-01 | The values are his charting vocabulary | **Decided:** the tokens are in contract v1.2 (user approval 2026-09-03) with proposed semantics; only his confirmation is outstanding (Frage 3). Behaviour itself is automated in `tests/test_ingest_hudl.py`'s grammar table |
| Correctness of a filled `hc_games.csv` row (which real game a block is) | HC-01 | Requires human knowledge of the head coach's season | Plan M3-01-04 task 2: a row is added only where the game is genuinely identifiable; anything unclear stays provisional and is listed in `docs/hc-workbook-ingest.md` |

---

## Validation Sign-Off

- [x] All auto tasks have `<automated>` verify commands (< 30 s where possible; the slowest is the wave-3 pipeline suite)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (the only task without one is plan 01's human-action checkpoint, which is preceded and not followed by automated tasks)
- [x] No watch-mode flags
- [x] Feedback latency < 120 s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planned 2026-09-03
