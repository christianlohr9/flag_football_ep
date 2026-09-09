---
phase: M3-05-epa-plattform
plan: 06
subsystem: ml-training
tags: [mlflow, promotion-gate, xgboost, typer, audit-tag]

requires:
  - phase: M3-05-epa-plattform
    provides: "M3-05-03's git_commit/corpus_fingerprint lineage plus calibration_max_deviation_<class>/per_tier_logloss_<tier>/no_play_share metrics on every ffep train run -- the gate reads these directly"
provides:
  - "model/gate.py: evaluate_gate(model_prefix, run_id, config, thresholds=None) -> GateResult, reading exclusively from the candidate run's own MLflow metrics"
  - "GateThresholds dataclass + Config.promotion_gate, resolved from an optional [promotion_gate] TOML table (max_calibration_deviation=0.15, max_no_play_share=0.02, champion_epsilon=0.0, provisional)"
  - "ffep promote refuses a failing candidate by default (nonzero exit, every check's pass/fail/skip printed) unless --force --reason \"...\" is given"
  - "--force override tags the promoted run with promotion_override_reason/promotion_override_by via MlflowClient().set_tag -- auditable, not just a CLI echo"
affects: [M3-05-07-extra-point-fix-repromotion]

tech-stack:
  added: []
  patterns:
    - "GateResult.checks: dict[str, tuple[bool | None, str]] -- None means skipped (metric absent on an old run), distinct from both pass and fail, so a promotion is never silently justified by a check that didn't actually run"
    - "[promotion_gate] optional TOML table resolved the same way as Paths.raw_hc_files -- a pre-existing ffep.toml or test fixture TOML keeps loading unmodified"

key-files:
  created:
    - src/flag_football_ep/model/gate.py
    - tests/test_model_gate.py
  modified:
    - src/flag_football_ep/config.py
    - src/flag_football_ep/cli.py
    - ffep.toml
    - docs/model-training.md
    - tests/test_cli_smoke.py

key-decisions:
  - "evaluate_gate resolves the LOGO log-loss metric key per model_prefix via a local {\"ep\": \"logo_mlogloss\", \"wp\": \"logo_logloss\"} mapping (train.py has no shared constant for this -- train_ep/train_wp each pass a literal metric_name= string to the shared _train pipeline) -- a dedicated test builds a candidate/champion pair where the WRONG key would flip the beats_champion verdict and asserts the gate never reads it"
  - "beats_naive (logloss_improvement) is treated with the same skip-on-absent discipline as calibration/no_play_share, even though the plan's behavior text only explicitly required that for the latter two -- consistent with T-M3-05-16's requirement that GateResult always distinguish a real pass from a skipped check, and defensive against a run that predates M3-05-03's metric additions"
  - "GateResult.passed is the AND of every check whose status is not None (skipped checks never block a promotion by themselves); if every check is skipped, passed=True vacuously -- documented in gate.py's docstring, not a silent default"
  - "docs/model-training.md section 3 rewritten to describe the gate as the default path with an auditable escape hatch, matching the objective's explicit call-out that this doc's current 'promotion is manual' framing needed to change -- not in the plan's own files_modified list, added as Rule 2 (missing critical: shipping behavior-changing CLI semantics undocumented would leave the operator guide actively wrong)"

requirements-completed: [PROD-07]

duration: ~70min
completed: 2026-09-09
---

# Phase M3-05 Plan 06: Promotion Gate Summary

**`ffep promote` now refuses a candidate that fails four mechanical MLflow-metric checks (beats naive, beats champion, calibration tolerance, no-play share) by default, with `--force --reason "..."` as an auditable, MLflow-tagged escape hatch.**

## Performance

- **Duration:** ~70 min
- **Started:** 2026-09-09T08:05:00Z (approx, base verification/reset)
- **Completed:** 2026-09-09T08:18:21+02:00
- **Tasks:** 2/2 complete
- **Files modified:** 7 (2 created)

## Accomplishments

- `GateThresholds` (frozen dataclass) added to `config.py`: `max_calibration_deviation=0.15`, `max_no_play_share=0.02`, `champion_epsilon=0.0`, all documented as provisional pending M3-05-07's first real exercise. `Config.promotion_gate` resolves from an optional `[promotion_gate]` TOML table using the exact `Paths.raw_hc_files` optional-field-with-default convention, so a pre-M3-05-06 `ffep.toml` (or any existing test fixture TOML) keeps loading unmodified -- verified directly against both the real `ffep.toml` and a synthetically-stripped copy with the table removed.
- `model/gate.py` (new): `evaluate_gate(model_prefix, run_id, config, thresholds=None) -> GateResult`. Four checks, every one reading only `MlflowClient().get_run(run_id).data.metrics` (never a CSV):
  1. Beats naive baseline (`logloss_improvement > 0`).
  2. Beats current champion -- resolved via the existing, unmodified `registry.resolve_champion`; `RegistryError` ("no champion set yet") is caught and treated as an automatic pass, not an error.
  3. Calibration within tolerance -- every `calibration_max_deviation_*` metric present on the run.
  4. No-play share under threshold, if the metric is present.
  A check whose metric is absent (e.g. a run predating M3-05-03) is recorded as `status=None` ("skipped"), distinct from both pass and fail -- `GateResult.passed` is the AND of every check that actually ran.
- Metric-key resolution is per `model_prefix` (`logo_mlogloss` for `ep`, `logo_logloss` for `wp`), never hard-coded to one model -- `tests/test_model_gate.py` has a dedicated pair of tests (`test_evaluate_gate_reads_logo_mlogloss_for_ep_never_logo_logloss` / `..._logo_logloss_for_wp_never_logo_mlogloss`) that construct a champion/candidate pair where reading the WRONG key would flip the `beats_champion` verdict, and assert the gate never does.
- `ffep promote` (`cli.py`): calls `evaluate_gate` when `--force` is absent, prints every check's `PASS`/`FAIL`/`SKIP` status and reason, and exits nonzero (`typer.Exit(code=1)`) if the gate fails. `--force --reason "..."` bypasses the gate and tags the promoted run with `promotion_override_reason` (the text) and `promotion_override_by` (`$USER`) via `MlflowClient().set_tag` -- an auditable MLflow tag, never just a terminal echo. `--force` with no or an empty `--reason` raises `typer.BadParameter` before anything runs. `registry.promote`'s own signature and behavior are completely unchanged -- the gate sits entirely in front of it.
- `docs/model-training.md` section 3 (previously "Why promotion is manual") rewritten to describe the gate's four checks, the `[promotion_gate]` thresholds, and the `--force --reason` override -- the objective explicitly flagged this doc's old framing as the thing the gate replaces as the default path.

## Task Commits

1. **Task 1: `[promotion_gate]` thresholds + `model/gate.py`'s `evaluate_gate`** - `5c686a2` (feat) -- `src/flag_football_ep/model/gate.py`, `src/flag_football_ep/config.py`, `ffep.toml`, `tests/test_model_gate.py`
2. **Task 2: Wire the gate into `ffep promote --force --reason`** - `543834b` (feat) -- `src/flag_football_ep/cli.py`, `tests/test_cli_smoke.py` (deviation fix, see below)

**Docs commit:** `016381d` (docs) -- `docs/model-training.md`, `.planning/phases/M3-05-epa-plattform/deferred-items.md`

## Files Created/Modified

- `src/flag_football_ep/model/gate.py` - `evaluate_gate`/`GateResult`/`GateError`, the four gate checks
- `src/flag_football_ep/config.py` - `GateThresholds` dataclass, `Config.promotion_gate`, `[promotion_gate]` optional-table resolution in `load_config`
- `ffep.toml` - `[promotion_gate]` table with provisional defaults and rationale comment
- `src/flag_football_ep/cli.py` - `promote` command: `--force`/`--reason` options, gate call, override tagging
- `tests/test_model_gate.py` - 23 tests: overall pass/fail, each of the four checks' pass/fail/skip paths, champion-epsilon widening, per-model metric-key resolution, invalid prefix, nonexistent/non-hex run id
- `docs/model-training.md` - section 3 rewritten to document the gate and the override
- `tests/test_cli_smoke.py` - two pre-existing promote tests updated to `--force --reason` (see Deviations)
- `.planning/phases/M3-05-epa-plattform/deferred-items.md` - new, logs one out-of-scope pre-existing test failure found during sanity checks

## Decisions Made

See `key-decisions` in frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Two pre-existing `test_cli_smoke.py` promote tests broken by gating `promote` on by default**
- **Found during:** Task 2 (after wiring the gate into `cli.py`, running the broader `test_cli_smoke.py` suite as a sanity check beyond the plan's own `<verify>` command)
- **Issue:** `test_promote_with_explicit_run_id_makes_it_champion` and `test_promote_without_run_promotes_most_recent_finished_run` invoke `ffep promote` without `--force` against a real `train_ep` run on a tiny synthetic corpus. Once the gate became the default path, that real fit genuinely failed `beats_naive` (`logloss_improvement=-10.3`) and `calibration` (all three classes over the 0.15 tolerance) on the toy data -- both tests started exiting nonzero.
- **Fix:** Added `--force --reason "cli smoke test: champion-alias mechanics, not gate compliance"` to both invocations, with a docstring note explaining these tests check champion-alias-setting mechanics, not gate compliance (that's `test_model_gate.py`'s job).
- **Files modified:** `tests/test_cli_smoke.py`
- **Verification:** `uv run pytest tests/test_cli_smoke.py -q` -- all 16 tests green (was 2 failing before the fix).
- **Commit:** `543834b` (same commit as the cli.py wiring, since the test fix is a direct consequence of that change)

---

**Total deviations:** 1 auto-fixed (Rule 1 - regression in pre-existing tests caused directly by this plan's own gating change). **Impact:** Necessary to keep the suite green after making the gate the default `ffep promote` path; no scope creep -- the fix only touches the two tests whose invocations needed the override, and does not weaken or bypass the gate itself.

## Issues Encountered

- `uv run ffep promote --help | grep -q -- "--force"` (the plan's literal `<verify>` command) exits nonzero when run through this session's interactive Bash tool, because `rich`'s color rendering splits `--force` across an ANSI reset escape (`--force` becomes `-` + reset + `-force`) -- the exact same reason `tests/test_cli_smoke.py` has its own `_plain()` ANSI-stripping helper. Confirmed this is a terminal-rendering artifact, not a real gap: `TERM=dumb uv run ffep promote --help | grep -q -- "--force"` passes, and `uv run pytest tests/test_cli_smoke.py::test_promote_help -q` (which uses `CliRunner`, not a real TTY) also passes cleanly.
- A broader confidence-check test run (`tests/test_config.py tests/test_model_train.py tests/test_model_evaluate.py tests/test_model_freeze.py tests/test_model_score.py -q`, beyond what this plan's own `<verify>` requires) was started in the background and completed (exit code 0, meaning pytest ran to completion) while under heavy CPU contention with a sibling executor's concurrent `pytest tests/test_model_train.py` process on a different worktree (`agent-a3793c76ad0bbfc5f`) -- both processes observed at 300-400% CPU each on the same machine. `tests/test_config.py` was also pulled out and run alone (fast, no real training) and passed cleanly (18/18). Result: two pre-existing failures in `tests/test_model_score.py` (`test_score_plays_backfills_ep_on_null_feature_row_from_next_play`/`..._wp_...`, both `assert None is not None`) -- confirmed unrelated to this plan via `git diff --stat b940c56 HEAD -- src/flag_football_ep/model/score.py` (empty: this plan's commits never touch `score.py`), logged in `deferred-items.md`. The remaining files (`test_model_train.py`/`test_model_evaluate.py`/`test_model_freeze.py`) passed. This plan's own required verification (`tests/test_model_gate.py tests/test_model_registry.py -q`, the `--help` check, and the pre-existing-`ffep.toml` load check) is fully green and was not affected by either out-of-scope finding.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- M3-05-07 (extra-point-fix re-promotion) is the gate's first real empirical exercise: it will re-train after excluding `play_type == "extra_point"` rows, run the gate's real printed verdict against the resolved champion, and let the user decide the promotion outcome (including any override) with real numbers instead of the provisional defaults shipped here.
- The gate does not retroactively affect any already-promoted champion -- `registry.promote`'s own behavior is byte-for-byte unchanged, and the sibling M3-05-01 executor's concurrent promotions (on the main tree, using the pre-gate `ffep promote` semantics) are unaffected by this plan; the gate only applies to promotions made after this plan's `cli.py` change lands.
- Flagged, not this plan's scope: `.planning/phases/M3-05-epa-plattform/deferred-items.md` logs one pre-existing, environment-specific test failure (`test_modellkarte_run_ids_resolve_to_registry_or_csv`) unrelated to any file this plan touches.

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-09*

## Self-Check: PASSED

- All 8 claimed files confirmed present on disk (`gate.py`, `test_model_gate.py`,
  `config.py`, `cli.py`, `ffep.toml`, `docs/model-training.md`, `test_cli_smoke.py`,
  `deferred-items.md`).
- All 3 claimed commit hashes (`5c686a2`, `543834b`, `016381d`) confirmed present in
  `git log --oneline --all`.
- Plan `<verification>` re-run and green: `uv run pytest tests/test_model_gate.py
  tests/test_model_registry.py -q` (23 + 32 tests, all pass); `TERM=dumb uv run ffep
  promote --help | grep -q -- "--force"` passes (see Issues Encountered for the
  interactive-terminal ANSI caveat); a pre-existing-style `ffep.toml` with the
  `[promotion_gate]` table stripped still loads with `GateThresholds()` defaults.
- `tests/test_cli_smoke.py` (all 16 tests) and `tests/test_config.py` (18 tests) also
  green, confirming the deviation fix and the `Config` dataclass change introduced no
  regressions in the files this plan touched.
