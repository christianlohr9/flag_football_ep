---
phase: M3-02
slug: epa-refinement
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-09-03
---

# Phase M3-02 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Skeleton from M3-02-RESEARCH.md § Validation Architecture; per-plan map filled by the planner.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing, uv-managed) |
| **Config file** | pyproject.toml (`testpaths = ["tests"]`, `addopts = "-q"`) |
| **Quick run command** | wave 1: `uv run pytest tests/test_ingest_hc_workbook.py tests/test_features_mutations.py tests/test_model_train.py tests/test_m3_epa_snapshot.py -q` · wave 2: `uv run pytest tests/test_m3_hc_games_refill.py tests/test_ingest_hc_workbook.py tests/test_ingest_hc_dedupe.py tests/test_validation_checks.py -q` · waves 3-6: `uv run pytest tests/test_hc_corpus_ablation.py tests/test_reports_hc_comparison.py tests/test_m3_epa_docs.py -q` |
| **Full suite command** | `uv run pytest tests -q` |
| **Estimated runtime** | quick < 30 s; full ~6-9 min (M3-01 precedent: the full suite slows markedly past ~67 % collection on this machine — budget for it, do not block on it) |

---

## Sampling Rate

- **After every task commit:** the wave's quick command.
- **After every plan wave:** the quick command plus, from wave 2 on, the wave's own real run
  (`ffep ingest` in wave 2, `scripts/hc_corpus_ablation.py` in wave 3, `scripts/epa_comparison.py`
  in wave 4) with its determinism check.
- **Before `/gsd:verify-work`:** full suite green.
- **Max feedback latency:** 30 s for the quick commands; the real training run in wave 3 is the one
  deliberate exception (see below).

**Training-run exception.** M3-02-05 task 2 fits four leave-one-game-out arms. The 1.3 baseline
measured 214 folds at 162.8 s (EP) and 29.7 s (WP) on 16.4 k plays; the enlarged corpus will exceed
that in both folds and rows, so a single arm can run for many minutes. It must be launched as one
foreground command with an extended timeout (up to 600 000 ms) or in the background — never split,
never sub-sampled, never `--tune`. This is the only step in the phase whose feedback latency exceeds
120 s, and the fold scheme is locked by D-07, so it cannot be shortened.

---

## Per-Plan Verification Map

| Plan | Wave | Requirement | Secure/Honest Behavior | Test Type | Automated Command | File Exists | Status |
|------|------|-------------|------------------------|-----------|-------------------|-------------|--------|
| M3-02-01 T1 | 1 | HC-03 | A real game charted with alternating possession is one game; ambiguous team abbreviations are left as their own visible residual games rather than guessed into a neighbour | unit | `uv run pytest tests/test_ingest_hc_workbook.py -k segment -x -q` plus the `frozenset` / no-ordered-tuple grep gate in the plan | ⚠️ extends existing file | ⬜ |
| M3-02-01 T2 | 1 | HC-03 | The half replacement value satisfies `half_assigned`'s real contract instead of bypassing it, and applies only to declared, non-`Copy of Data` games — everything else keeps quarantining | unit | `uv run pytest tests/test_ingest_hc_workbook.py -x -q` plus the blanket-null-removal grep gate | ❌ Wave 0 (new section) | ⬜ |
| M3-02-01 T3 | 1 | HC-03 | The sentinel's decision table and its named label-quality cost are written into the contract, not only into code comments | doc gate | section greps in the plan plus `uv run pytest tests/test_ingest_hc_workbook.py -q` | ⚠️ extends existing doc | ⬜ |
| M3-02-02 T1 | 1 | HC-03 | A constant-half game still ends exactly once for BOTH EP and WP; the `half == 1` and `half is null` regressions are pinned as failures so the sentinel cannot be "simplified" later | unit | `uv run pytest tests/test_features_mutations.py -k half -x -q` | ❌ Wave 0 (new section) | ⬜ |
| M3-02-02 T2 | 1 | HC-03 | Head-coach rows are distinguishable from genuine second-half plays in the model input, and every other source is a proven no-op against the frozen 1.3 baseline | unit | `uv run pytest tests/test_features_mutations.py tests/test_migration_equivalence.py -x -q` | ❌ Wave 0 | ⬜ |
| M3-02-02 T3 | 1 | HC-03 | The unmapped-competition failure fires in the test suite instead of halfway through a multi-minute fit; every tier default is `[ASSUMED]` and validated against the tier vocabulary | integration | `uv run pytest tests/test_model_train.py -k tier -x -q` plus the row-count greps | ❌ Wave 0 | ⬜ |
| M3-02-03 T1 | 1 | HC-03 | The play-level PII tabs are unreadable by the snapshot script by construction; corrupted date labels are reconstructed with provenance, never emitted as dates | script gate | `uv run python scripts/hc_sp_snapshot.py --dry-run` plus the allow-list greps | ❌ Wave 0 | ⬜ |
| M3-02-03 T2 | 1 | HC-03 | The snapshot reproduces his own workbook's spot values and is byte-identical on re-run | integration | the schema/domain assertions and no-date grep in the plan | ❌ Wave 0 (artifacts) | ⬜ |
| M3-02-03 T3 | 1 | HC-03 | No roster name, no long string and no date artifact survives into a committed reference CSV; a probability without its sample size is a failure | unit | `uv run pytest tests/test_m3_epa_snapshot.py -x -q` | ❌ Wave 0 | ⬜ |
| M3-02-04 T1 | 2 | HC-03 | Only games that can actually produce a label are declared; an existing declaration that no longer matches a segmented slice raises instead of being dropped | unit | `uv run pytest tests/test_m3_hc_games_refill.py -x -q` plus `--dry-run` | ❌ Wave 0 | ⬜ |
| M3-02-04 T2 | 2 | HC-03 | Head-coach rows provably reach `plays.parquet` with `half == 2`; the nine confirmed duplicate declarations are byte-identical; no check was weakened to get there | integration | the non-zero-rows / half-values / hc_games-height assertions in the plan | ❌ Wave 0 (artifacts) | ⬜ |
| M3-02-04 T3 | 2 | HC-03 | The document reports the real after-counts next to the before-counts and names every deliberate exclusion with what would unlock it | doc gate | section greps plus `uv run pytest tests/test_m3_hc_games_refill.py -q` | ⚠️ extends existing doc | ⬜ |
| M3-02-05 T1 | 3 | HC-03 | The ablation driver cannot promote, cannot tune, and refuses to compare two identical frames | unit | `uv run pytest tests/test_hc_corpus_ablation.py -x -q` plus the no-promote / no-tune grep gates | ❌ Wave 0 | ⬜ |
| M3-02-05 T2 | 3 | HC-03 | The with-head-coach arm really trained on more games (strictly more folds, different data hash); every log-loss stands next to its own naive baseline, per source AND per competition tier (HC-03 Tier-Eval); the previous champion is untouched | integration | the four-row / more-folds / distinct-hash assertions in the plan | ❌ Wave 0 (artifacts) | ⬜ |
| M3-02-05 T3 | 3 | HC-03 | The Timeout / Offsetting Penalties question is decided with a measured share and an explicit escalation threshold, not an assumption | integration | the `no_play_rows.csv` schema assertions plus `uv run pytest tests/test_hc_corpus_ablation.py -q` | ❌ Wave 0 | ⬜ |
| M3-02-06 T1 | 4 | HC-03 | Small-sample cells are flagged at two documented thresholds and never dropped; PAT rows cannot enter a cell; a missing column fails loudly | unit | `uv run pytest tests/test_reports_hc_comparison.py -x -q` plus the `rate_table` reuse grep | ❌ Wave 0 | ⬜ |
| M3-02-06 T2 | 4 | HC-03 | The model column is out-of-fold and uses the pipeline's own expected-points weighting; a one-sided cell is marked, never coalesced to zero | unit | `uv run pytest tests/test_reports_hc_comparison.py -x -q` | ❌ Wave 0 | ⬜ |
| M3-02-06 T3 | 4 | HC-03 | The comparison is built on the with-head-coach arm's out-of-fold file (asserted, not assumed) and reproduces his own spot values before being committed | integration | the committed-CSV schema/field-half/model-column assertions in the plan | ❌ Wave 0 (artifacts) | ⬜ |
| M3-02-07 T1 | 5 | HC-03 | The calibration section leads with the result even when it is unflattering; every exclusion is named with what would unlock it; the trainable game count is a range, never "2,128 fixed" | doc gate | the ten-section greps in the plan | ❌ Wave 0 | ⬜ |
| M3-02-07 T2 | 5 | HC-03 | M3-2's questions are added without renumbering or breaking phase M3-3's parallel counter assertions, and without editing another phase's plan file | doc gate | the `Zusatzfragen` greps plus the `## Frage` / `### Frage` equality check | ⚠️ appends to shared doc | ⬜ |
| M3-02-07 T3 | 5 | HC-03 | No figure in the coach-facing document can drift from its measured CSV, in either direction; no roster name reaches it | unit + doc gate | `uv run pytest tests/test_m3_epa_docs.py -x -q` | ❌ Wave 0 | ⬜ |
| M3-02-08 T1 | 6 | HC-03 | A human decides tone, which questions go out, and whether the retrained models become the champion | manual | checkpoint (human-verify) — no automated gate by design | n/a | ⬜ |
| M3-02-08 T2 | 6 | HC-03 | Review feedback changes prose only; a wrong number becomes a re-measurement; the promotion pins an explicit run id | doc gate | `uv run pytest tests/test_m3_epa_docs.py tests/test_m3_epa_snapshot.py -x -q` plus the status-line and clean-tree checks | ❌ Wave 0 | ⬜ |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky/partial*

---

## Wave 0 Requirements

Created inside the phase, in this order:

- `tests/test_ingest_hc_workbook.py` — EXTENDED by M3-02-01 (new `segment_games` possession-swap
  fixtures in task 1, a new `# --- half sentinel ---` section in task 2). Every fixture is a
  synthetic `openpyxl` workbook built in `tmp_path`, exactly as the existing tests do. No test in this
  phase opens `data/raw/hc_files/`.
- `tests/test_features_mutations.py` — EXTENDED by M3-02-02 with a `# --- half sentinel ---` section
  built on `flag_football_ep.testing.canonical_plays_with_scores`.
- `tests/test_model_train.py` — EXTENDED by M3-02-02 with a
  `# --- head-coach competition tier ---` section, reusing the module's existing `_make_config`
  tmp_path MLflow-store fixture.
- `tests/test_m3_epa_snapshot.py` — NEW, M3-02-03 task 3. Deliberately a separate file from
  `tests/test_m3_hc_pii.py`, which phase M3-3 owns and edits in parallel.
- `data/reference/hc_sp_tables/*.csv` — NEW artifacts, M3-02-03 task 2; the source of truth the
  comparison and the doc guard check against.
- `tests/test_m3_hc_games_refill.py` — NEW, M3-02-04 task 1; synthetic workbooks and a synthetic
  `hc_games.csv` / `competition_tier.csv` pair in `tmp_path`.
- `tests/test_hc_corpus_ablation.py` — NEW, M3-02-05 task 1; copies the tmp_path config helper from
  `tests/test_model_train.py` rather than importing a private helper across modules, and asserts
  wiring/tags/bookkeeping only, never model quality.
- `data/reference/epa_refinement/*.csv` — NEW artifacts, M3-02-05 task 2/3 and M3-02-06 task 3.
- `tests/test_reports_hc_comparison.py` — NEW, M3-02-06 task 1, extended in task 2.
- `tests/test_m3_epa_docs.py` — NEW, M3-02-07 task 3; doc-versus-CSV guard modelled on
  `tests/test_m2_baseline_docs.py`.
- `tests/conftest.py` is NOT touched — it is owned by phase 01.2 plan 01. Use module-local fixtures
  and the `flag_football_ep.testing` factories.
- Framework install: none. pytest, polars, openpyxl, scipy, xgboost, scikit-learn and mlflow are all
  existing pinned dependencies (M3-02-RESEARCH § Package Legitimacy Audit: zero new packages, no
  `[ASSUMED]` or `[SUS]` entries, so no legitimacy checkpoint applies anywhere in this phase).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Tone of the sections stating that his `Reg` tab looks like hand-transcribed chart trendlines and that two of his three workbooks are currently unusable | HC-03 | A relationship judgement about a colleague's work; no assertion can decide whether it is acceptable to send | M3-02-08 task 1, step 1. Until reviewed, `docs/epa-refinement-2026-10.md` is a repository document, not a handout |
| Whether the comparison cells shown in the document are representative rather than selected toward a conclusion | HC-03 | The figures are test-pinned, but the CHOICE of which cells to display is editorial | M3-02-08 task 1, step 2 — spot-check against `data/reference/epa_refinement/comparison_by_dd.csv` |
| Whether the retrained models become the production champion | HC-03 | `docs/model-training.md` section 3 makes promotion an explicit reviewed step; if EP still does not beat its naive baseline there is a real argument on both sides | M3-02-08 task 1, step 4, then task 2's pinned `ffep promote --run <id>` |
| Which questions go to the head coach and in what order | HC-03 | Only the user knows what he wants answered at the October sync and how he communicates with the head coach | M3-02-08 task 1, step 3. M3-2 adds two Zusatzfragen; M3-3 adds Fragen 4-6; Fragen 1-3 are still unanswered |
| Whether `half = 2` is the right replacement value | HC-03 | The mechanism is verified (M3-02-RESEARCH section 2.2) but the choice is a design judgement with a named label-quality cost | Zusatzfrage A in `docs/hc-rueckfragen-2026-09.md`. Until answered, the sentinel stays and its cost is stated in `docs/data-contract.md` and in the October document |
| The competition tier of his camp and charting games | HC-03 | `mixed-other` is a planner default by precedent (RESEARCH assumption A4), not a fact only derivable from the data | Zusatzfrage B in `docs/hc-rueckfragen-2026-09.md`. Until answered, the default holds and is marked `[ASSUMED]` in the tier rationale and the October document |
| The true column layout of the `Copy of Data` tab and the empty EC-2025 `Data` tab | HC-03 | Frage 2 and Frage 1 from the September round; no local investigation can resolve them (RESEARCH sections 1.3 and Open Questions) | Until answered, both stay provisional, keep `half = null`, keep quarantining, and are listed under `## Nicht eingelesen (bewusst)` in `docs/hc-workbook-ingest.md` |

---

## Validation Sign-Off

- [x] All auto tasks have `<automated>` verify commands
- [x] Sampling continuity: no 3 consecutive tasks without an automated verify (the only task without
      one is M3-02-08 task 1's human-verify checkpoint, immediately followed by an automated task)
- [x] No watch-mode flags
- [x] Feedback latency < 120 s for every command except M3-02-05 task 2's four leave-one-game-out
      training arms, documented above as a deliberate, unavoidable exception (fold scheme locked by
      D-07)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planned 2026-09-03
