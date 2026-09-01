---
phase: M2-01-freigabe-lizenzlage
plan: 02
subsystem: docs
tags: [licensing, compliance, pyproject, pytest, hackathon]

# Dependency graph
requires:
  - phase: M2-01-freigabe-lizenzlage (plan 01)
    provides: capture-legal / release framing for data artefacts (RECHT-01)
provides:
  - "Verified per-component license inventory (docs/lizenz-inventur.md) covering the ffep package, every distribution in pyproject.toml, model artefacts, and bundle data artefacts"
  - "Coverage test (tests/test_m2_lizenz_inventur.py) that fails when a new dependency lacks an inventory row"
  - "Pending todo surfacing the GPL-3.0-vs-permissive-chain decision for the user"
affects: [M2-2 (tracker measurement — must append candidates to ## Komponenten before installing)]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Markdown table parsed by a stdlib-only pytest coverage test (tomllib + regex), no network, no cv extra required"]

key-files:
  created:
    - docs/lizenz-inventur.md
    - tests/test_m2_lizenz_inventur.py
    - .planning/todos/pending/2026-09-01-lizenz-des-eigenen-codes-klaeren.md
  modified: []

key-decisions:
  - "Collected license facts strictly via installed importlib.metadata (precedence License-Expression > Classifier > License field); PyPI JSON only for the two non-installed versioning-extra packages (dvc, dvc-s3); LICENSE file for the repo itself — never from memory"
  - "Where package metadata only gives a generic classifier without a clause number (seaborn, jinja2, hyperopt, umap-learn: 'BSD'), the inventory records exactly that instead of guessing BSD-2 vs BSD-3"
  - "GPL-3.0 finding on the repository's own LICENSE is stated plainly in ## Befunde, not softened, with the missing pyproject.toml license field flagged as a related but separate discrepancy"

requirements-completed: [RECHT-04]

# Metrics
duration: 35min
completed: 2026-09-01
---

# Phase M2-01 Plan 02: License Inventory Summary

**Verified per-component license inventory (docs/lizenz-inventur.md) covering all 29 delivered/installed distributions plus the repo itself, backed by a pytest coverage gate against pyproject.toml.**

## Performance

- **Duration:** 35 min
- **Started:** 2026-09-01T00:00:00Z (approx.)
- **Completed:** 2026-09-01
- **Tasks:** 2
- **Files modified:** 3 (all created)

## Accomplishments
- Collected verified license metadata for every distribution in `pyproject.toml` (`project.dependencies`, `optional-dependencies.cv`, `optional-dependencies.versioning`, `dependency-groups.dev`) via `importlib.metadata` on the installed `.venv`, with PyPI JSON fallback for the two non-installed `versioning` packages (`dvc`, `dvc-s3`) and the repo's own `LICENSE` file for `flag-football-ep` itself.
- Wrote `docs/lizenz-inventur.md` with the seven required H2 sections: proved the "AGPL-free chain" claim from `ABGLEICH.md` per component instead of asserting it, and surfaced the GPL-3.0 finding on the repository's own code plainly in `## Befunde`, alongside the missing `license` field in `pyproject.toml` and two metadata quirks (mlflow/numpy leading with a copyright notice instead of a clean identifier; numpy's binary wheel bundling GPL/LGPL runtime libraries alongside its own BSD-3-Clause license).
- Wrote `tests/test_m2_lizenz_inventur.py`: six pytest functions, stdlib-only (`tomllib`, `re`, `pathlib`), parsing the `## Komponenten` markdown table and asserting coverage against `pyproject.toml`, the own-package GPL-3.0 row, the no-AGPL guard, per-row license/source completeness, the three permissive anchors named in `ABGLEICH.md` (rfdetr, trackers, supervision), and a minimal PII guard.
- Spot-checked the coverage gate: deleting the `pandas` row from the table makes `test_every_declared_distribution_has_a_row` fail with a clear message; restoring the row makes all six tests pass again.
- Filed `.planning/todos/pending/2026-09-01-lizenz-des-eigenen-codes-klaeren.md`: the one decision this inventory surfaces but cannot make — keep GPL-3.0 and say so, or relicense the delivered surface permissively before the 2026-11-16 handoff.

## Task Commits

Each task was committed atomically:

1. **Task 1: Collect verified license facts and write docs/lizenz-inventur.md** - `f0d7e4e` (docs)
2. **Task 2: Lock the inventory with a coverage test and record the open license decision** - `2fecace` (test)

## Files Created/Modified
- `docs/lizenz-inventur.md` - Per-component license inventory (`## Komponenten` table with 29 rows: repo + 16 core deps + 9 cv-extra deps + 2 versioning deps + pytest), model/data artefact framing, and the GPL-3.0/missing-license-field findings
- `tests/test_m2_lizenz_inventur.py` - Six-test coverage gate parsing the inventory table against `pyproject.toml`
- `.planning/todos/pending/2026-09-01-lizenz-des-eigenen-codes-klaeren.md` - Pending user decision on the repo's own GPL-3.0 license vs the permissive dependency chain

## Decisions Made
- Used strict metadata precedence (License-Expression > Classifier > License field) exactly as specified, and left ambiguous BSD variants (seaborn, jinja2, hyperopt, umap-learn) as plain "BSD" rather than guessing a clause number from memory — the coverage test only requires a non-empty, non-AGPL cell, so precision beyond what the metadata states was not fabricated.
- Documented mlflow/numpy's "copyright notice before license identifier" metadata quirk and numpy's bundled GPL/LGPL runtime libraries (OpenBLAS toolchain artefacts on this platform) as `## Befunde` items rather than omitting them, since a third party re-checking the raw metadata would hit the same surprise.

## Deviations from Plan

None - plan executed exactly as written. The em-dash characters initially used in the doc's German prose were caught and rewritten to plain punctuation before the first commit (self-correction during drafting, not a deviation from the plan's own "no em dashes" instruction).

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `docs/lizenz-inventur.md` and its coverage test are ready for M2-2: any tracker candidate M2-2 measures must get a row in `## Komponenten` before it is installed, or `test_every_declared_distribution_has_a_row` will fail.
- The pending todo `.planning/todos/pending/2026-09-01-lizenz-des-eigenen-codes-klaeren.md` needs a user decision before the 2026-11-16 material handoff to hackathon teams — not a blocker for this phase, but should not be forgotten.
- `pyproject.toml`, `uv.lock`, and every file owned by Phase 2.2 waves 7-11 were left untouched; no package was installed and `uv sync`/`uv add` was not run.

## Self-Check: PASSED

- `docs/lizenz-inventur.md` - FOUND (123 lines, seven H2 sections in order, GPL-3.0 and capture-legal.md both present)
- `tests/test_m2_lizenz_inventur.py` - FOUND (`uv run pytest tests/test_m2_lizenz_inventur.py -q` → 6 passed)
- `.planning/todos/pending/2026-09-01-lizenz-des-eigenen-codes-klaeren.md` - FOUND (frontmatter has `created`, `title`, `area`, `files`; body has `## Problem` and `## Solution`)
- Commit `f0d7e4e` - FOUND in `git log --oneline`
- Commit `2fecace` - FOUND in `git log --oneline`
- `pyproject.toml` diff since before this plan: empty (confirmed via `git diff --stat HEAD~2 HEAD -- pyproject.toml`)

---
*Phase: M2-01-freigabe-lizenzlage*
*Completed: 2026-09-01*
