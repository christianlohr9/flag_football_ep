---
phase: M2-04-messvorschrift
plan: 01
subsystem: cv-metrics
tags: [polars, numpy, motmetrics, hackathon-scoring, mot-metrics, honesty-testing]

requires:
  - phase: 02.2-dataset-buildout
    provides: "data/reference/hackathon_split.csv (61 dev / 61 private_test clips), the vaulted Puerto Rico continuity_review.csv"
  - phase: M2-02-ehrliche-baseline
    provides: "cv.continuity._measure_clip/summarise_review (reused by injection), baseline_common.IDEAL_TRACK_BAND"
provides:
  - "fragments_per_expected_player: the one METR-01 continuous number (n_fragments / 10)"
  - "active_track_count_deviation: diagnostic guard metric, never an acceptance criterion"
  - "read_split/role_violations: direct polars reader + dev/test role guard for hackathon_split.csv"
  - "sniff_review_dialect/read_review_table/summarise_review_normalized: dialect-tolerant vault review reader (semicolon/cp1252/CRLF), source file never modified"
  - "reviewed_only_rate: honest partial-review rate with its own denominator, never overwrites pass_rate"
  - "render_markdown: pure Markdown renderer for the future score_tracks.py report"
  - "compute_identity_metrics/frame_events: M2-3-ready label-based IDF1/MOTA interface, lazy motmetrics import"
affects: [M2-04-02, M2-04-03]

tech-stack:
  added: []
  patterns:
    - "Continuity-helper injection: functions take _measure_clip/summarise_review as parameters instead of importing flag_football_ep at module level"
    - "Path-based sibling import (sys.path.insert on scripts/hackathon) to reuse score_tracks._fmt_rate, mirroring baseline_common.py"
    - "Dialect sniffing + tempfile.TemporaryDirectory normalisation for read-only external CSVs that must never be rewritten in place"
    - "Lazy library import inside a try/except that re-raises a RuntimeError naming the package and install path, never a bare ImportError"

key-files:
  created:
    - scripts/hackathon/continuous_metric.py
    - scripts/hackathon/identity_metric.py
    - tests/test_m2_metric.py
  modified: []

key-decisions:
  - "Primary metric denominator is the constant EXPECTED_PLAYERS=10 (5v5), not the observed track count, so fragments_per_expected_player scales consistently across clips"
  - "active_track_count_deviation shipped as a diagnostic guard column (GUARD_NOTE), never a second acceptance-adjacent number, per CONTEXT.md's singular 'eine stetige Kennzahl'"
  - "BLIND_SPOT_NOTE and GUARD_NOTE are module constants asserted by tests, not comments a reader can skip"
  - "The vault review file is read via a temp-directory normalised copy that is deleted before the call returns; the source is never rewritten in place"
  - "identity_metric.py's report-key contract (IDENTITY_REPORT_KEYS) and continuous_metric.py's render_markdown report shape are both forward contracts for plan M2-04-02's CLI wiring, documented in-module since M2-04-02 does not exist yet"

requirements-completed: [METR-01]

duration: 45min
completed: 2026-09-02
---

# Phase M2-4 Plan 01: Measurement Core Summary

**Label-free continuous metric (fragments per expected player + diagnostic active-track-count guard) built on `cv.continuity._measure_clip` by injection, a dialect-tolerant reader for the real semicolon/cp1252/CRLF vault review file, and a lazy-import motmetrics IDF1/MOTA interface ready for M2-3 — with the identity-swap blind spot asserted by a passing test, not a footnote.**

## Performance

- **Duration:** 45 min
- **Started:** 2026-09-02T00:00:00Z (approx, sequential session)
- **Completed:** 2026-09-02
- **Tasks:** 3
- **Files created:** 3 (`scripts/hackathon/continuous_metric.py`, `scripts/hackathon/identity_metric.py`, `tests/test_m2_metric.py`)

## Accomplishments

- `fragments_per_expected_player` (`n_fragments / EXPECTED_PLAYERS`) implemented as the ONE officially reported continuous number, reusing `cv.continuity._measure_clip` via injection rather than reimplementing fragment logic.
- `active_track_count_deviation` implemented as a diagnostic guard, catching the GTA-shaped over-merge case (5 long tracks, 0 fragments, guard = 5.0) that the primary metric alone rewards.
- `test_swap_is_invisible_to_both_metrics` proves, as an executable test rather than a caveat, that a silent identity swap (two full-length tracks exchanging bbox streams at the midpoint, track_id assignment unchanged) produces IDENTICAL metric dicts to the non-swapped clip.
- `read_split`/`role_violations` read `hackathon_split.csv` directly with `polars` (never `cv.testset`), guarding against Panama/Puerto-Rico clips crossing the dev/private_test boundary.
- The real vault file (`data/private/test-labels/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE/continuity_review.csv`, semicolon-delimited, cp1252, CRLF, 61 rows, 10/61 reviewed) is read correctly and reported honestly: `pass_rate=None`, `reviewed_only_rate={"k": 2, "n": 10, "complete": False, "note": "unvollstaendig (10/61 geprueft)"}`. The source file's SHA-256 (`7520995e55b53252cf3883533a07ed3dd2e10c09901a6cea58e43f04bcf4d543`) was verified identical before and after every call in this session.
- `identity_metric.py`'s `compute_identity_metrics`/`frame_events` give M2-3 a ready, tested-on-synthetic-data IDF1/MOTA interface with a lazy `motmetrics` import and an actionable `RuntimeError` when the library is absent.

## Task Commits

Each task was committed atomically:

1. **Task 1: Label-free continuous metric core** — `c79de3e` (feat) — `EXPECTED_PLAYERS`, `player_view`, `active_track_count_deviation`, `clip_metrics`, `aggregate`, `BLIND_SPOT_NOTE`, `GUARD_NOTE` + 10 tests.
2. **Task 2: Split reader, vault-dialect review reader, Markdown renderer** — `62a215b` (feat) — `SPLIT_COLUMNS`/`read_split`/`role_violations`, `sniff_review_dialect`/`read_review_table`/`summarise_review_normalized`, `reviewed_only_rate`, `render_markdown` + 14 additional tests.
3. **Task 3: M2-3-ready label-based association interface** — `f896c8c` (feat) — `scripts/hackathon/identity_metric.py` (`frame_events`, `compute_identity_metrics`, `IDENTITY_REPORT_KEYS`) + 7 additional tests.

**Plan metadata:** (this commit) — SUMMARY.md

## Files Created/Modified

- `scripts/hackathon/continuous_metric.py` (446 lines) — label-free continuous metric, split reader, dialect-tolerant review reader, Markdown renderer.
- `scripts/hackathon/identity_metric.py` (147 lines) — M2-3-ready label-based association interface (frame events + motmetrics-backed IDF1/MOTA), not wired into any CLI.
- `tests/test_m2_metric.py` (723 lines, 31 tests: 29 always-running + 2 `importorskip`-guarded) — unit tests for both metric layers, including the honesty tests.

## Measured Values on the Synthetic Fixtures

| Fixture | `fragments_per_expected_player` | `active_track_count_deviation` |
|---|---|---|
| 7 short-lived fragments + 1 full-length track (20 frames) | 0.7 | not asserted (fixture focuses on primary metric) |
| Empty clip (0 rows) | 0.0 | 10.0 (`no_tracks=True`) |
| GTA-shaped over-merge (5 full-length tracks) | 0.0 (looks perfect) | 5.0 (flags it) |
| Identity-swap pair (2 full-length tracks, bbox streams exchanged at frame 10) | identical for swapped vs. non-swapped | identical for swapped vs. non-swapped |

Real vault file (`--review-test` shape): `n_clips=61`, `n_reviewed=10`, `pass_rate=None`, `reviewed_only_rate={"k": 2, "n": 10, "complete": False}`.

## Decisions Made

- `EXPECTED_PLAYERS=10` is a fixed normalisation constant (not the observed track count) so the primary metric is comparable across clips and methods — matches RESEARCH.md's recommendation and `baseline_common.IDEAL_TRACK_BAND[0]`.
- The guard metric is explicitly diagnostic (`GUARD_NOTE`), never promoted to a second acceptance criterion, keeping METR-03's future "one acceptance criterion + one direction" framing clean.
- `summarise_review_normalized` never rewrites the vault file: it reads it read-only, writes a normalised comma/UTF-8 copy into an OS temp directory (never inside the repo, never below `data/`), calls the injected `summarise_review_fn`, and lets the `TemporaryDirectory` context manager delete the copy before returning.
- `render_markdown`'s report-dict shape and `identity_metric.py`'s `IDENTITY_REPORT_KEYS` are documented in-module as forward contracts for plan M2-04-02 (which does not exist yet) rather than left implicit.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed unsigned-integer underflow in `active_track_count_deviation`**
- **Found during:** Task 1 (first test run)
- **Issue:** `polars`' `n_unique()` returns an unsigned integer column; subtracting the Python int `EXPECTED_PLAYERS` (10) from an unsigned count below 10 wrapped around to a huge unsigned value (`4294967291.0` observed) instead of a negative number, corrupting the guard metric whenever fewer than 10 tracks were active in a frame.
- **Fix:** Cast the `n_unique()` result to `pl.Int64` before subtracting, so the deviation computes correctly for both over- and under-populated frames.
- **Files modified:** `scripts/hackathon/continuous_metric.py`
- **Verification:** `test_gta_shaped_overmerge_guard_catches_what_primary_misses` (5 active tracks vs. 10 expected) now correctly reads `5.0`, not `4294967291.0`.
- **Committed in:** `c79de3e` (Task 1 commit)

**2. [Rule 1 - Bug] Removed the literal word "torch" from the module docstring**
- **Found during:** Task 1 (plan's own automated verify command)
- **Issue:** The plan's `<verify>` step runs `assert 'torch' not in src` as a literal substring check against the whole file. The module docstring's prose describing the "no torch dependency" convention itself contained the word "torch", failing the check even though no `import torch` existed anywhere.
- **Fix:** Reworded the docstring to describe the constraint without using the literal string ("no deep-learning/GPU training framework import").
- **Files modified:** `scripts/hackathon/continuous_metric.py`
- **Verification:** `uv run python -c "... assert 'torch' not in src ..."` passes.
- **Committed in:** `c79de3e` (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (both Rule 1 bugs caught by the plan's own test/verify suite before committing).
**Impact on plan:** Both fixes were necessary for correctness of the shipped metric and to satisfy the plan's own literal verification commands; no scope creep, no plan-scope files touched beyond the three named in `files_modified`.

## Issues Encountered

- The plan's third Task-3 verify command (`git diff --name-only -- pyproject.toml uv.lock | wc -l | grep -qx '0'`) does not match on this machine's BSD `wc -l`, which pads its output with leading whitespace (`"       0"` instead of `"0"`). This is a shell-portability quirk in the verify command itself, not a real failure: `git status --short pyproject.toml uv.lock` and `git diff --name-only -- pyproject.toml uv.lock` both confirm zero changes to either file throughout this plan.

## Known Stubs

None — every function shipped is fully implemented and exercised by a passing test (aside from the two `motmetrics`-gated tests, which are an intentionally skipped, documented state, not a stub).

## Motmetrics Skip State (Intended)

`motmetrics` is NOT installed in this environment, per the plan's file-collision guard (`pyproject.toml`/`uv.lock` are outside this phase's scope; no `uv add`/`uv sync`/`pip install` was run). The two `pytest.importorskip("motmetrics")`-guarded tests (`test_compute_identity_metrics_perfect_hypothesis_scores_idf1_one`, `test_compute_identity_metrics_swapped_hypothesis_scores_below_one`) report as **skipped**, not failed — this is the correct, intended state for this plan. Adding `motmetrics` to the `dev` dependency group and un-skipping these tests is explicitly scoped as M2-3 follow-up work, not this plan.

The always-running `identity_metric.py` tests (frame-event construction, distance thresholding, one-sided frames, the missing-dependency `RuntimeError` path) all pass without `motmetrics` installed, confirming the lazy-import contract holds.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Plan M2-04-02 (CLI wiring into `score_tracks.py`) can now import `continuous_metric.py`'s `clip_metrics`/`aggregate`/`read_split`/`role_violations`/`summarise_review_normalized`/`reviewed_only_rate`/`render_markdown` directly, plus `identity_metric.py`'s `IDENTITY_REPORT_KEYS` for the future `identity` JSON block.
- Plan M2-04-03 (challenge-doc wording) can cite `BLIND_SPOT_NOTE`/`GUARD_NOTE` verbatim and reference the now-passing `test_swap_is_invisible_to_both_metrics` as the executable proof of the label-free layer's ceiling.
- No blockers. `motmetrics` install (dev-group) remains open M2-3 follow-up work, already flagged above and in-module.

---
*Phase: M2-04-messvorschrift*
*Completed: 2026-09-02*

## Self-Check: PASSED

- `scripts/hackathon/continuous_metric.py` — FOUND (446 lines, contains `EXPECTED_PLAYERS`)
- `scripts/hackathon/identity_metric.py` — FOUND (147 lines, contains `compute_identity_metrics`)
- `tests/test_m2_metric.py` — FOUND (723 lines, contains `def test_`, 31 test functions)
- Commit `c79de3e` — FOUND in `git log --oneline --all`
- Commit `62a215b` — FOUND in `git log --oneline --all`
- Commit `f896c8c` — FOUND in `git log --oneline --all`
- `uv run pytest tests/test_m2_metric.py -q` — 29 passed, 2 skipped
- `uv run pytest tests/test_hackathon_scoring.py tests/test_cv_continuity.py -q` — 19 passed (unaffected, confirming no unintended edits)
- `git diff --name-only` across the three task commits lists exactly `scripts/hackathon/continuous_metric.py`, `scripts/hackathon/identity_metric.py`, `tests/test_m2_metric.py`
- `git status --porcelain data/ docs/ pyproject.toml uv.lock src/` — empty
- `grep -c torch scripts/hackathon/continuous_metric.py scripts/hackathon/identity_metric.py` — 0 for both
- Vault file SHA-256 verified identical before and after every read in this session
