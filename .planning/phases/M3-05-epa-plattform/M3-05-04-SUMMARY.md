---
phase: M3-05-epa-plattform
plan: 04
subsystem: infra
tags: [github-actions, ci, pytest, uv]

# Dependency graph
requires:
  - phase: M3-05-epa-plattform (prior plans in this wave)
    provides: "tests/conftest.py's existing tmp_path-scoped Config fixture and synthetic canonical corpus (unchanged, not touched)"
provides:
  - "A GitHub Actions workflow (.github/workflows/ci.yml) that runs the fixture-based pytest suite on every push/PR, core dependency group only, zero secrets"
affects: [ci, testing, future-plans-in-this-phase]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "CI test selection uses --ignore/--ignore-glob for collection-time exclusion of heavy-dependency test modules, not -k, because -k only deselects already-collected items -- pytest still imports (and fails to import) every file under testpaths before -k is applied"

key-files:
  created:
    - .github/workflows/ci.yml
  modified: []

key-decisions:
  - "Replaced the plan's proposed `-k \"not cv and not hackathon\"` filter with --ignore-glob=\"tests/test_cv_*.py\" plus two explicit --ignore= entries (tests/test_m2_baseline_measurement.py, tests/test_m2_gta_adapter.py), keeping -k \"not hackathon\" only for the one file that collects cleanly but should still be excluded by name (test_hackathon_scoring.py)"

patterns-established:
  - "Any future test file needing torch/cv2/supervision/rfdetr/etc. must be added to --ignore/--ignore-glob in ci.yml, not just given a cv/hackathon-sounding name -- -k does not protect against collection failures"

requirements-completed: [PROD-05]

# Metrics
duration: 84min
completed: 2026-09-08
---

# Phase M3-05 Plan 04: Fixture-only CI on push/PR Summary

**`.github/workflows/ci.yml` runs `uv sync --dev` + a collection-safe fixture pytest selection on every push/PR, zero secrets, CV/hackathon-heavy suites excluded via `--ignore`/`--ignore-glob` rather than the plan's originally proposed `-k` filter, which does not prevent pytest from importing (and failing on) modules requiring `torch`/`cv2`/`supervision`.**

## Performance

- **Duration:** 84 min (dominated by two full local dry-runs of the fixture suite, ~13-18 min each, needed to discover and then verify the collection-time exclusion fix)
- **Started:** 2026-09-08T12:14:00Z (approx, base verification)
- **Completed:** 2026-09-08T13:38:21Z
- **Tasks:** 1
- **Files modified:** 1 created

## Accomplishments
- `.github/workflows/ci.yml` created: triggers on `push`/`pull_request`, one `ubuntu-latest` job, pinned `actions/checkout@v4` and `astral-sh/setup-uv@v3` (no `@latest`), `uv sync --dev` (core dependency group only), then a fixture-only pytest invocation
- Discovered and fixed a real gap in the plan's proposed test-selection command before it ever reached CI: `-k "not cv and not hackathon"` still lets pytest *collect* (import) every file under `testpaths`, so `tests/test_cv_*.py` (needs `cv2`/`torch`/`supervision`/`rfdetr`) and two `tests/test_m2_*.py` files that transitively import `scripts/hackathon/measure_gta.py` (needs `torch`) blow up collection with `ModuleNotFoundError` before any `-k` filtering happens
- Verified the corrected command locally end-to-end: `uv sync --dev` (core group only, confirmed no `torch`/`rfdetr`/`cv2` pulled in) then `uv run pytest -q --ignore-glob="tests/test_cv_*.py" --ignore=tests/test_m2_baseline_measurement.py --ignore=tests/test_m2_gta_adapter.py -k "not hackathon"` -- exit code 0, no collection errors, reached `[100%]`

## Task Commits

1. **Task 1: `.github/workflows/ci.yml` -- fixture suite on push/PR, no secrets** - `7a6b817` (feat)

**Plan metadata:** committed separately (this SUMMARY + REQUIREMENTS.md)

## Files Created/Modified
- `.github/workflows/ci.yml` - GitHub Actions CI: checkout, install uv, `uv sync --dev`, run the collection-safe fixture pytest selection; no `env:`/secrets declared anywhere

## Decisions Made
- **`-k` alone cannot exclude a whole test file from CI:** pytest collects (imports) every module under `testpaths` regardless of `-k`; `-k` only deselects already-collected test *items*. A file that fails to import aborts the whole session with `Interrupted: N errors during collection` before any test runs, `-k` notwithstanding. Verified this empirically: `uv run pytest -q -k "not cv and not hackathon"` failed collection on 11 files in ~37s (`test_cv_active_learning.py`, `test_cv_detect_infer.py`, `test_cv_detect_train.py`, `test_cv_export.py`, `test_cv_overlay.py`, `test_cv_prelabel.py`, `test_cv_radar.py`, `test_cv_teams.py`, `test_cv_track.py`, `test_m2_baseline_measurement.py`, `test_m2_gta_adapter.py`) -- the last two do not even contain "cv" or "hackathon" in their filename, so `-k` would not have excluded them from running even if collection had somehow succeeded.
- **Fix:** switched to `--ignore-glob="tests/test_cv_*.py"` (covers all 25 `test_cv_*.py` files by one pattern) plus two explicit `--ignore=` entries for the m2 files that import `scripts/hackathon/measure_gta.py`/`baseline_common.py` (which import `torch`/`supervision` transitively). Kept `-k "not hackathon"` for `tests/test_hackathon_scoring.py`, which collects fine (only imports `polars`/`continuous_metric`, both core-safe) but is still a heavy/slow suite out of this phase's scope by name.
- Verified `tests/test_m2_baseline_docs.py` and `tests/test_m2_metric.py` do NOT need exclusion: grepped their imports and the `scripts/hackathon/{continuous_metric,identity_metric,score_tracks}.py` modules they load by path -- all core-only (`numpy`, `polars`), no heavy deps, no need to widen the ignore list further.
- Did not add a pytest marker-based mechanism (e.g. `@pytest.mark.cv`) as an alternative fix -- that would require editing `tests/conftest.py` and/or every CV/hackathon test file's markers, which is explicitly out of scope per this phase's Wave 0 note ("`tests/conftest.py` is NOT touched -- owned by phase 01.2 plan 01") and would be a larger footprint than the command-line `--ignore` fix.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Plan's proposed `-k "not cv and not hackathon"` filter does not achieve the stated fixture-only, no-heavy-deps outcome**
- **Found during:** Task 1, running the plan's own `<verification>` step (`uv sync --dev && uv run pytest -q -k "not cv and not hackathon"` "succeeds locally")
- **Issue:** The command as specified in the plan fails with `Interrupted: 11 errors during collection` because pytest imports every test module under `testpaths` before applying `-k`, and 9 `test_cv_*.py` files plus 2 `test_m2_*.py` files (whose names don't even match `cv`/`hackathon`) require `torch`/`cv2`/`supervision` -- packages deliberately not installed by `uv sync --dev` (core group only)
- **Fix:** Replaced `-k "not cv and not hackathon"` with `--ignore-glob="tests/test_cv_*.py" --ignore=tests/test_m2_baseline_measurement.py --ignore=tests/test_m2_gta_adapter.py -k "not hackathon"` in both the workflow file and the verified local dry-run command. This achieves the plan's stated intent (fixture-based suite only, CV/hackathon multi-GB stacks excluded, zero secrets) that the originally proposed command did not actually deliver.
- **Files modified:** `.github/workflows/ci.yml`
- **Verification:** `uv sync --dev && uv run pytest -q --ignore-glob="tests/test_cv_*.py" --ignore=tests/test_m2_baseline_measurement.py --ignore=tests/test_m2_gta_adapter.py -k "not hackathon"` -- exit code 0, `[100%]` reached, no `ERROR collecting`, ~1065s (17m45s) wall time on a machine with concurrent sibling processes contending for CPU
- **Committed in:** `7a6b817` (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug in the plan's own proposed verification command)
**Impact on plan:** The auto-fix was necessary for the CI workflow to actually work -- the plan's stated success criteria ("a push or PR to this repo now runs the fixture-based suite automatically") would have been false with the originally proposed command, since every PR/push would fail at the collection step, not at any real test failure. No scope creep: the fix stays within `.github/workflows/ci.yml`, the plan's sole `files_modified` entry.

## Issues Encountered
- The measured local dry-run duration (~1065s / 17m45s) is well above the RESEARCH doc's `[ASSUMED]` "a few minutes" estimate for this suite. Two separate timed runs measured ~780s and ~1065s respectively; the variance is consistent with CPU contention from concurrent sibling plan executors on the same machine (this repo's test suite trains real XGBoost models via hyperopt search inside `test_model_train.py`/`test_model_experiments.py`, which is CPU-heavy and sensitive to contention) rather than a change in the suite itself. On a dedicated GitHub Actions runner (2-core `ubuntu-latest`, no sibling contention) the actual CI duration may differ from either local measurement in either direction; this is not something this plan's scope covers fixing (no test file inside `tests/` was modified), only reported.

## User Setup Required
None - no external service configuration required. No secrets are declared or needed by this workflow.

## Next Phase Readiness
- `.github/workflows/ci.yml` is in place and locally verified against the exact command it runs; ready for the next plan in this phase's wave.
- Flag for a later plan (not this one, out of scope for M3-05-04 per its `files_modified`): the fixture suite's ~13-18 min runtime is long for a CI gate; if it becomes a recurring pain point, splitting `test_model_train.py`/`test_model_experiments.py`'s hyperopt-search-heavy tests into a separate, less-frequently-run job would be a reasonable follow-up, but is not this plan's concern.

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-08*

## Self-Check: PASSED

- `.github/workflows/ci.yml` exists: FOUND (verified via `test -f`)
- Commit `7a6b817` exists: FOUND (`git log --oneline --all | grep 7a6b817`)
- Plan-level `<verification>` re-run: `.github/workflows/ci.yml` is valid YAML and references `pytest` (PASS, via `python3 -c "import yaml; yaml.safe_load(...)"`) -- no secret env var name appears anywhere in the file (PASS) -- `uv sync --dev && uv run pytest -q <corrected selection>` succeeds locally (PASS, exit code 0, confirmed via explicit `$?` capture written to log)
