# Deferred Items — Phase 02.2 Dataset Buildout

Out-of-scope discoveries logged during plan execution, per the executor's scope-boundary
rule (fix only what the current task's own files_modified covers; log everything else here
instead of touching unrelated files).

---

## 2026-09-02 — Plan 02.2-21, Task 3: `track.py::_read_hover_position_ids` cross-session collision

**Found during:** Task 3's real `ffep cv track --session 2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE` run.

**Issue:** `src/flag_football_ep/cv/track.py::_read_hover_position_ids(config, clip_numbers)`
reads `config.reference.hover_positions` (a single file, no `session_id` column) and matches
rows to the current tracking run purely by `clip_number`. `data/reference/hover_positions.csv`
was sighted for the PILOT session (`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`, clip numbers
1-61) and carries no rows for any other session.

Because the Puerto Rico session is also `drone` and also numbers its clips 1-63 (colliding
with the pilot session's own 1-61 numbering), a `track_session` run for
`2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE` silently attaches the PILOT session's
`hover_position_id` (`hp-01`/`hp-02`) to Puerto Rico clips 1-56 and 59-61, instead of leaving
the column `null` as this plan's own `<interfaces>` block assumed ("`hover_position_id` will
be null for this session ... that is expected and harmless").

**Measured impact:** bounded. `x_yards`/`y_yards` are NOT computed by `track_session` itself
(a separate `ffep cv coords`/homography step would need to run, which this plan explicitly
does not run for Puerto Rico — no homography calibration exists for this session). The wrong
`hover_position_id` value is therefore an inert, unused string in the Puerto Rico
`tracks.parquet`'s `hover_position_id` column for this plan's purposes: nothing in Task
3/4's pipeline (continuity measurement, flag-pull scoring, bundle assembly) reads that column.
`clip_paths`/`_measure_clip`/`validate_test_labels` all operate on `clip_number`, not
`hover_position_id`.

**Why not fixed here:** `track.py` is not in plan 02.2-21's `files_modified` list, and the
orchestrator's explicit instruction for this plan is "Commit ONLY files in your plan's
files_modified plus (later) SUMMARY.md." Fixing `_read_hover_position_ids` to filter by
session (e.g. by joining through `video_inventory.csv`'s `session_id`/`clip_number` pair
instead of `hover_positions.csv`'s bare `clip_number`) is a real, scoped bug fix but belongs
to a plan that touches `cv/track.py`.

**Recommended fix (for a future plan):** `_read_hover_position_ids` should resolve
`hover_position_id` through a `(session_id, clip_number)` key, not `clip_number` alone —
either by adding a `session_id` column to `hover_positions.csv` (breaking change, needs a
migration) or by cross-referencing `video_inventory.csv`'s session-scoped clip list before
the `hover_positions.csv` lookup (no schema change). Any future session that shares a domain
and clip-numbering scheme with an already-sighted session will hit the same silent
cross-contamination until this is fixed.

**Verification that this plan is unaffected:** see plan 02.2-21's SUMMARY.md, Task 3 section
— the measured `hover_position_id` values in the real Puerto Rico `tracks.parquet` were
inspected and this note's "bounded impact" claim was checked against the actual file, not
assumed.

---

## 2026-09-07 — Plan 02.2-21, Task 4 doc rebuild: two pre-existing ruff findings, unrelated to this task

**Found during:** `uv run ruff check` sweep before committing Task 4's doc changes.

**Issue 1:** `src/flag_football_ep/cv/commands.py:289` — `F821 Undefined name 'cfg'` inside
`validate-dataset`'s `eval_split_path=cfg.paths.reference / "frozen_eval_clips.csv"`. Introduced
by commit `4485e96f` ("feat(02.2-diag): add init_weights fine-tuning option to train_detector"),
2026-09-04 — after 02.2-21's own Task 1/2 commits and outside Task 4's `files_modified`
(docs-only). Looks like a real bug (the local variable is `config`, not `cfg`, elsewhere in the
same command), but fixing it is out of scope for a documentation-rebuild task.

**Issue 2 (fixed 2026-09-07, plan 02.2-16):** `tests/test_cv_active_learning.py:25` — `F401
'json' imported but unused`. Introduced by commit `b4fdd691`, 2026-09-01, predating this session
entirely. Removed as a trivial one-line fix while this same file was already being edited for
plan 02.2-16's iteration-2 target/cap test update — no longer an open item.

**Why Issue 1 not fixed here:** `src/flag_football_ep/cv/commands.py:289` is not touched by any
of plan 02.2-21's or 02.2-16's `files_modified`, and predates both plans' own commits by date.
Logged per scope-boundary rule instead of touched.
