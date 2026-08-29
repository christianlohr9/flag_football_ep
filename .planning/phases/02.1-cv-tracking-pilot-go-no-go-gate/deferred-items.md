# Deferred items

Out-of-scope discoveries logged during execution, per the executor's scope-boundary rule
(only auto-fix issues directly caused by the current task's changes).

## 02.1-15: `test_pick_points_ref_jpeg_is_gitignored` fails under the worktree's symlinked `data/labels/`

- **Found during:** 02.1-15 Task 1, running the full `cv`-tagged test suite as a sanity check.
- **Symptom:** `tests/test_cv_homography.py::test_pick_points_ref_jpeg_is_gitignored` fails with
  `git check-ignore` returning exit 128 ("ist hinter einer symbolischen Verknüpfung" / "is behind a
  symbolic link").
- **Cause:** this executor's worktree links `data/labels/calibration` (and sibling `data/labels/*`
  dirs) to the orchestrator's checkout so gitignored shared state (real footage, real calibration
  picks) is visible without duplicating it per-worktree. `git check-ignore` refuses to evaluate a path
  that passes through a symlink outside the repo's own working tree, which this specific test
  triggers by writing a probe file under the symlinked `data/labels/calibration/`.
- **Not caused by any 02.1-15 code change:** `tests/test_cv_homography.py` is untouched by this plan;
  the same test passes normally in a non-symlinked checkout (e.g. the orchestrator's own worktree).
- **Action:** left as-is, not auto-fixed (would require changing the test or the worktree's symlink
  setup, both out of this plan's `files_modified` scope). Confirmed no other cv-tagged test is
  affected (`uv run --extra cv pytest tests/ -q -k cv --deselect
  tests/test_cv_homography.py::test_pick_points_ref_jpeg_is_gitignored` is fully green).
