---
phase: M2-04-messvorschrift
plan: 02
subsystem: cv-metrics
tags: [polars, hackathon-scoring, split-mode, honesty-testing, markdown-report]

requires:
  - phase: M2-04-messvorschrift
    plan: 01
    provides: "continuous_metric.py (clip_metrics/aggregate/read_split/role_violations/summarise_review_normalized/reviewed_only_rate/render_markdown), identity_metric.py (M2-3 forward interface)"
provides:
  - "score_tracks.py --tracks-dev/--tracks-test/--review-dev/--review-test/--split: one CLI run reporting the threshold rate AND the continuous metric for dev and private_test, each with its own n"
  - "score_tracks.py --out-md: Markdown report mirroring the JSON, one row per split (or 'gesamt' for the legacy single-run form)"
  - "Wrong-split guard: role_violations against hackathon_split.csv, exit 1 naming the offending session/clip"
  - "Legacy Einzel-Modus report contract pinned by a regression test (n_clips/per_clip/auto/human_reference/flag_pulls unchanged)"
  - "Real dev+private_test proof run (data/processed/m2-04/report.{json,md}, gitignored) with the actual measured numbers"
affects: [M2-04-03]

tech-stack:
  added: []
  patterns:
    - "sys.modules alias before a same-directory circular import: sys.modules[\"score_tracks\"] = sys.modules[__name__] before importing continuous_metric.py, so the CLI never re-executes itself when running as __main__"
    - "Internal-full-result / trimmed-legacy-report split: _score_one() always returns all 7 fields; single-run mode trims human_reference_reviewed_only off the JSON it writes, while the Markdown adapter still reads from the untrimmed internal result"

key-files:
  created: []
  modified:
    - scripts/hackathon/score_tracks.py
    - tests/test_hackathon_scoring.py

key-decisions:
  - "Split entries' 'per_clip' key stays the LEGACY-style unfiltered per-clip rows (n_tracks/longest_track_frac/n_fragments/auto_flag over ALL tracks, referees included); the continuous metric's own per-clip breakdown (player-view-filtered fragments_per_expected_player/active_track_count_deviation) is nested inside the 'continuous' block instead of overloading 'per_clip' with two different denominators under one name"
  - "Legacy single-run JSON report gains exactly 3 new top-level keys (continuous, guard, blind_spot) per the plan's action text; human_reference_reviewed_only is Split-Modus-only at the top level (though always computed internally for the Markdown adapter)"
  - "All three tasks (split-mode JSON, Markdown wiring, real-data regression test) landed in one commit -- the incremental diffs were too small and interleaved to split without reverting/reapplying near-identical report-assembly code"
  - "Circular import (continuous_metric.py imports _fmt_rate back from score_tracks.py) resolved by aliasing sys.modules['score_tracks'] to the running module before importing continuous_metric, placed after _fmt_rate's definition -- no code duplication, no changes to the read-only continuous_metric.py"

requirements-completed: [METR-01, METR-02]

duration: 55min
completed: 2026-09-02
---

# Phase M2-4 Plan 02: Split-Mode Scoring Summary

**One `score_tracks.py` run now reports the pass/fail threshold AND the continuous fragments-per-expected-player metric for both the dev split (Panama Rojo, 15/61 human pass) and the private_test split (Puerto Rico, 10/61 reviewed, reviewed-only 2/10), as JSON and Markdown, while the legacy single-run report used by six committed M2-2 rows stays byte-identical.**

## Performance

- **Duration:** 55 min
- **Started:** 2026-09-02 (sequential session, continuing from M2-04-01)
- **Completed:** 2026-09-02T21:55:06Z
- **Tasks:** 3 (landed in 1 commit -- see Deviations)
- **Files modified:** 2 (`scripts/hackathon/score_tracks.py`, `tests/test_hackathon_scoring.py`)

## Accomplishments

- `score_tracks.py` gained a full Split-Modus (`--tracks-dev`/`--review-dev`/`--tracks-test`/`--review-test`/`--split`) alongside the unchanged Einzel-Modus, with mutual-exclusivity checks (`--tracks` vs `--tracks-dev`/`--tracks-test`, `--flag-pulls` disallowed in split mode, `--split` required in split mode).
- Wrong-split guard: `continuous_metric.role_violations` checked per split before any scoring happens; a team's Puerto Rico tracks submitted under `--tracks-dev` (or vice versa) exits 1 and names the offending `(session_id, clip_number)` pairs.
- `--review-test` is the only path that reads the vaulted Puerto Rico labels; `continuous_metric.summarise_review_normalized` normalises a temp copy for the dialect-tolerant read and the vault file itself is never modified (SHA-256 verified identical before/after the real run, see below).
- `--out-md` renders a Markdown report via `continuous_metric.render_markdown`, one row per split (or `"gesamt"` for the legacy form), with `BLIND_SPOT_NOTE` printed once and referenced identically in stdout, JSON and Markdown (no second, drifting copy).
- Legacy Einzel-Modus report contract regression test pins `n_clips`, `per_clip[]` (unfiltered legacy fields, no continuous-metric keys leaking in), `auto{n_ok,n_clips,rate}`, `human_reference`, `flag_pulls` -- the guard for `baseline_common.summarise()` and the six committed M2-2 rows.
- Real one-run proof over the full 61-clip dev and 61-clip private_test splits: see measured values below.

## Task Commits

Landed as one commit (see Deviations for why):

1. **Tasks 1+2+3: Split mode, Markdown, real-data regression test** - `61f1944` (feat) - `score_tracks.py` Split-Modus/`--out-md`/circular-import fix, `tests/test_hackathon_scoring.py` extended from 6 to 20 tests.

**Plan metadata:** (this commit) - SUMMARY.md

## Files Created/Modified

- `scripts/hackathon/score_tracks.py` (676 lines, was 302) - Split-Modus CLI surface, `_score_one`/`_print_result`/`_to_markdown_report`/`_run_single_mode`/`_run_split_mode`, circular-import fix via `sys.modules` alias.
- `tests/test_hackathon_scoring.py` (903 lines, was 266) - 20 tests (was 6): legacy contract regression, split-mode reporting, role-mismatch guard, vault-dialect partial review, Markdown output, denominator discipline, real-data smoke test.

## Measured Values (Real Run, 2026-09-02)

Command:

```
uv run python scripts/hackathon/score_tracks.py \
  --tracks-dev data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet \
  --review-dev data/reference/continuity_review.csv \
  --tracks-test data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE_tracks.parquet \
  --review-test data/private/test-labels/<PR session>/continuity_review.csv \
  --split data/reference/hackathon_split.csv \
  --out data/processed/m2-04/report.json --out-md data/processed/m2-04/report.md
```

(gitignored under `data/processed/*`, not committed; the `--review-test` path is written here per the pii_discipline preferred form, never a real filename literal)

| Split | n | Automatische Kontinuitaet (auto_flag=ok) | Human-Referenz | Stetige Kennzahl (mean / median) | Guard (mean, diagnostisch) |
|---|---|---|---|---|---|
| dev (Panama Rojo) | 61 | 57/61 (93.44%) | 15/61 (24.59%) | 0.8852 / 0.7000 | 5.9546 |
| private_test (Puerto Rico) | 61 | 53/61 (86.89%) | nicht auswertbar (51 unbewertete Clips) | 1.2049 / 1.1000 | 6.7054 |

private_test reviewed-only rate: `2/10 (20.00%)`, flag `"unvollstaendig (10/61 geprueft)"` -- printed in stdout, present in the JSON's `human_reference_reviewed_only` block, and in the Markdown as the `PARTIAL_REVIEW_LABEL` line. `pass_rate` for private_test stays `null` at every layer; no manufactured pass rate anywhere.

Vault file SHA-256 (`data/private/test-labels/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE/continuity_review.csv`): `7520995e55b53252cf3883533a07ed3dd2e10c09901a6cea58e43f04bcf4d543` -- identical before and after the real run (matches the value recorded in M2-04-01's SUMMARY, confirming the file has never been rewritten across both plans).

`git status --porcelain data/ docs/ src/ pyproject.toml uv.lock` after the real run: empty (no stray writes outside the two owned files).

## Construct-Validity Check (Dev Split Only -- Has Full Human Verdicts)

Cross-referenced the dev split's 61 `fragments_per_expected_player` values (from the real report's `continuous.per_clip`) against `data/reference/continuity_review.csv`'s `verdict`/`id_switches` columns, polars only, no scipy:

| Group | n | min | median | max | mean |
|---|---|---|---|---|---|
| all 61 clips | 61 | 0.10 | 0.70 | 2.20 | 0.8852 |
| verdict=pass | 15 | 0.20 | 0.70 | 1.30 | 0.7333 |
| verdict=fail | 46 | 0.10 | 0.70 | 2.20 | 0.9348 |

Clips with `id_switches >= 4` (the clearest silent-swap candidates in the human review):

| clip | id_switches | verdict | fragments_per_expected_player |
|---|---|---|---|
| 3 | 5 | fail | 0.50 |
| 7 | 4 | fail | 2.20 |

**Honest finding:** the relationship is weak. Medians are IDENTICAL between pass and fail (0.70); the fail-group mean is only moderately higher than pass (0.9348 vs 0.7333) with heavy overlap in range (pass 0.20-1.30 fully inside fail's 0.10-2.20). Most tellingly, the two clips with the highest `id_switches` count (the review's proxy for silent identity swaps) do NOT both score high: clip 3 (5 switches) sits at 0.50 -- BELOW the fail-group median -- while clip 7 (4 switches) sits at 2.20, near the top. This is exactly consistent with `BLIND_SPOT_NOTE`'s claim: `fragments_per_expected_player` measures coverage/fragmentation, not identity correctness, and a silent swap during an overlap (no track ends, no track is reborn) leaves no reliable signature in this metric. This is a reportable result, not a defect -- METR-01 asks for a continuous, directional number inside a failed play, not a replacement for the human judgement or a proxy for identity-swap detection.

## Decisions Made

- `per_clip` inside each split entry stays the legacy-style unfiltered rows; the continuous metric's own per-clip breakdown lives nested inside `continuous.per_clip` instead. This avoids two different `n_tracks` meanings (all-classes vs player-only) colliding under one key name, and keeps the legacy shape recognisable inside split mode too.
- Legacy single-run JSON gains exactly `continuous`, `guard`, `blind_spot` at the top level (per the plan's action text) -- `human_reference_reviewed_only` is Split-Modus-only in the JSON, though it is always computed internally (`_score_one` always returns the full 7-key shape) so the `--out-md` adapter can still render the partial-review line for single-run mode if a legacy review happens to be incomplete.
- Circular import between `score_tracks.py` and `continuous_metric.py` (the latter imports `_fmt_rate` back from this module, an M2-04-01 precedent that predates this plan) is resolved with a `sys.modules["score_tracks"] = sys.modules[__name__]` alias placed after `_fmt_rate`'s definition and before the `continuous_metric` import -- verified to work in both directions (`score_tracks.py` run as `__main__` importing `continuous_metric`, and `continuous_metric.py` imported first by `tests/test_m2_metric.py` importing `score_tracks` back). No change to the read-only `continuous_metric.py`.
- All three plan tasks landed in a single commit (see Deviations) rather than three -- see rationale there.

## Deviations from Plan

### Auto-fixed Issues

None -- no bugs, missing-critical-functionality, or blocking issues were hit that needed Rule 1-3 fixes.

### Process Deviations (documented, not code fixes)

**1. Tasks 1, 2 and 3 committed as a single commit instead of three**
- **Found during:** Task 1 implementation
- **Reason:** `--out-md` wiring (Task 2) reused the exact same internal `_score_one` result objects Task 1 builds; splitting them into separate commits would have meant writing Task 1's version without markdown support, committing, then modifying nearly every function again to add markdown support -- reverting and reapplying the same report-assembly logic with no real isolation benefit (the markdown adapter is ~15 lines feeding off data Task 1 already computes). Task 3 added one regression test to the same file already being edited for Tasks 1-2, with no additional source changes.
- **Impact:** All plan verification commands still ran per-task as specified (see Verification below); the single commit still corresponds 1:1 to the plan's `files_modified` list. No scope creep.

**2. `test_m2_baseline_measurement.py::test_score_tracks_is_untouched` transiently failed before commit, not a real regression**
- **Found during:** Task 1 verify step
- **Issue:** This M2-02 test compares the WORKING TREE content of `score_tracks.py` against `git show HEAD:...` -- i.e. it is a "no uncommitted changes to this file" check, not a frozen-content check. It failed while the plan's edits were uncommitted (HEAD still held the pre-plan version) and passed again once the Task 1-3 commit landed (HEAD then matched the working tree). No code or test change was needed; documented here so a future reader is not alarmed by the transient local failure during development.
- **Verification:** `uv run pytest tests/test_m2_baseline_measurement.py -q` -- 8 passed, post-commit.

---

**Total deviations:** 0 auto-fixed code changes; 2 documented process notes (commit consolidation, a transient pre-commit test state). No scope creep, no plan-scope files touched beyond `scripts/hackathon/score_tracks.py` and `tests/test_hackathon_scoring.py`.

## Issues Encountered

None beyond the transient pre-commit test state documented above.

## Known Stubs

None. `--flag-pulls` remains intentionally Einzel-Modus-only per the plan's own scope decision ("the flag-pull bonus stays single-run-only in this phase") -- this is a documented scope boundary, not a stub.

## Threat Flags

None. All threat-model dispositions from the plan (`T-M2-04-06` through `T-M2-04-10`, `T-M2-04-SC`) are `mitigate`/`accept` and were implemented exactly as specified (role guard, `--review-test`-only vault access, legacy regression test, `pass_rate=null` + reviewed-only honesty, single-source `BLIND_SPOT_NOTE`, no new dependency installed). No new security-relevant surface was introduced beyond what the plan's threat register already covers.

## User Setup Required

None -- no external service configuration required.

## Next Phase Readiness

- Plan M2-04-03 (challenge-doc wording, METR-03/METR-04) can cite the measured values above directly: dev 15/61 (24.59%) human pass rate, dev continuous mean 0.8852, private_test continuous mean 1.2049, private_test reviewed-only 2/10 with the `"unvollstaendig (10/61 geprueft)"` flag, and the construct-validity finding (weak/absent pass-vs-fail separation, consistent with the documented blind spot).
- `score_tracks.py --tracks-dev/--tracks-test/--review-dev/--review-test/--split/--out/--out-md` is the final CLI surface for METR-02; `docs/hackathon-challenge-reid.md` (owned by M2-04-03) can document this usage directly.
- No blockers. The real proof run's output (`data/processed/m2-04/report.{json,md}`) is available on disk (gitignored) for M2-04-03 to reference if needed, but is not itself a deliverable of this plan.

---
*Phase: M2-04-messvorschrift*
*Completed: 2026-09-02*

## Self-Check: PASSED

- `scripts/hackathon/score_tracks.py` -- FOUND (676 lines, contains `--out-md`, well above the 380-line minimum)
- `tests/test_hackathon_scoring.py` -- FOUND (903 lines, contains `def test_`, 20 test functions, well above the 400-line minimum)
- Commit `61f1944` -- FOUND in `git log --oneline --all`
- `uv run pytest tests/test_hackathon_scoring.py tests/test_m2_metric.py tests/test_m2_baseline_measurement.py -q` -- 20 + 31 (29 passed/2 skipped) + 8 passed, zero failures
- `uv run pytest -q` (full suite) -- exit code 0, zero failures, only pre-existing warnings (Kalman-filter/CMC numerical warnings from `test_cv_track.py`/`test_m2_baseline_measurement.py`, unrelated to this plan)
- `data/processed/m2-04/report.json` and `report.md` -- FOUND on disk (gitignored, not committed)
- Vault file SHA-256 `7520995e55b53252cf3883533a07ed3dd2e10c09901a6cea58e43f04bcf4d543` -- verified identical before and after the real run
- `git status --porcelain data/ docs/ src/ pyproject.toml uv.lock` -- empty after the real run
- `git diff --name-only` for the commit lists exactly `scripts/hackathon/score_tracks.py` and `tests/test_hackathon_scoring.py`
