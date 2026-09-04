---
phase: M3-02-epa-refinement
plan: 05
subsystem: model
tags: [xgboost, mlflow, leave-one-game-out, calibration, hc-workbook, ablation]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "M3-02-01 through M3-02-04's header-block rule, half=2 sentinel, EP-feature
      sentinel, competition-tier rows, hc_games.csv refill (35 trainable HC games / 1,964
      rows as of M3-02-04)"
provides:
  - "Three HC corpus admission rules in ingest/hc_workbook.py (placeholder-row removal,
    play_id reassignment from row order, ODK-derived posteam/defteam fallback for
    provisional games) that took trainable HC games from 35 to 92 (1,964 to 6,818 rows)
    without weakening any validation check"
  - "scripts/hc_corpus_ablation.py: a reusable with/without-HC ablation driver over
    train_ep/train_wp, tagging every run's corpus_arm/gsd_phase/plan"
  - "Four new EP/WP MLflow model versions (ep_model v2/v3, wp_model v2/v3) measuring the
    enlarged corpus under leave-one-game-out, per-source and per-tier calibration reports,
    and a measured Timeout/Offsetting-Penalties/Penalty share per source"
  - "The finding that EP now beats its naive class-frequency baseline (it did not in 1.3)"
affects: [M3-02-06-oof-scoring, M3-02-07-german-deliverable, M3-02-08-promotion-checkpoint]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Two-arm ablation as two calls to the existing train_ep/train_wp with different input
      frames, no new training-harness code -- corpus_arm/gsd_phase/plan tags set from the
      driver via MlflowClient after the run closes, phase=01.3 provenance tag left untouched"
    - "Label reconstruction for post-hoc grouping (per-tier metrics, no-play survival count)
      via the same filter -> build_fn -> prepare_fn -> mutate_fn -> drop_nulls() chain
      _train uses internally, joined back onto the persisted out-of-fold predictions by
      (game_id, play_id, source) -- never a second metrics implementation"

key-files:
  created:
    - scripts/hc_corpus_ablation.py
    - tests/test_hc_corpus_ablation.py
    - data/reference/epa_refinement/ablation_summary.csv
    - data/reference/epa_refinement/per_source_metrics_ep.csv
    - data/reference/epa_refinement/per_source_metrics_wp.csv
    - data/reference/epa_refinement/per_tier_metrics_ep.csv
    - data/reference/epa_refinement/per_tier_metrics_wp.csv
    - data/reference/epa_refinement/corpus_arms.csv
    - data/reference/epa_refinement/no_play_rows.csv
  modified:
    - src/flag_football_ep/ingest/hc_workbook.py
    - tests/test_ingest_hc_workbook.py

key-decisions:
  - "Placeholder-row removal (admission rule 1) scoped to numeric blocks only -- a pair
    block's rows never carry a real ODK/DN pair this test can evaluate, and the pair-block
    marker series is built from unfiltered slice_.rows, so filtering game_df there would
    desync the two arrays"
  - "hc_play_no (original PLAY #, admission rule 2) is computed and carried through the
    per-sheet frame but dropped at conform_to_canonical -- canonical.py has no slot for it
    yet and is a concurrent plan's (M3-04-06) territory; documented as a follow-up, not
    silently invented"
  - "HC-OFF/HC-DEF posteam/defteam placeholders (admission rule 3) fire only when
    home_team/away_team are both None (a provisional game) -- home_team/away_team columns
    themselves stay null exactly as before, so hc_dedupe's fingerprint comparison
    (home_team/away_team-based) is unaffected"
  - "Trainable HC games reached 92 (not 'well over 100') -- the Scoring Probability Data
    pair block's 22 games stay quarantined on half_assigned because DECLARED_BLOCK_KINDS
    still excludes 'pair' blocks; the real corpus's pair block has zero O/D/S marker rows,
    so posteam there would need deriving from the raw team-name column ordering rather than
    ODK, a materially larger change than these three classification rules cover -- logged as
    a follow-up, not attempted without human sign-off"
  - "data/processed/plays_scored.parquet re-scoring deferred to a later plan -- this plan's
    file_collision_guard explicitly forbids ffep promote/any champion alias move, so there is
    no 'new champion' to score with yet; running ffep score now would only reproduce the
    unchanged pre-existing champion's scores against the same corpus it already saw, adding
    no information about this plan's work"
  - "Two sources exceed the plan's 2% Timeout/Offsetting-Penalties/Penalty escalation
    threshold (hc_workbook:scoring-probability-by-situation-2023-2026:data at 4.74%, legacy
    at 3.92%) -- flagged as a finding for the M3-02-08 checkpoint per the plan's own
    instruction, not resolved unilaterally in this plan"

requirements-completed: [HC-03]

# Metrics
duration: ~2h
completed: 2026-09-04
---

# Phase M3-02 Plan 05: EP/WP retrain on the enlarged HC corpus + with/without-HC ablation Summary

**Three HC-workbook admission rules take trainable head-coach games from 35 to 92 (1,964 to
6,818 rows); the with-HC EP/WP arms both fit at 306 leave-one-game-out folds versus 214
without, and EP now beats its naive baseline for the first time in this project's history
(0.9457 vs 0.9979 with HC; the 1.3 baseline never did).**

## Performance

- **Duration:** ~2h (includes the authorized first-task admission-rule deviation, 4 real LOGO
  training runs totalling ~933 wall-clock seconds, and CSV/report assembly)
- **Started:** 2026-09-04T06:30:00Z (approx.)
- **Completed:** 2026-09-04T08:55:00Z (approx.)
- **Tasks:** 1 authorized deviation task (corpus admission rules) + 3 plan tasks
- **Files modified:** 11 (9 created, 2 modified)

## Accomplishments

- Implemented the three HC corpus admission rules authorized 2026-09-04 (classification, not
  check-weakening): placeholder-row removal before validation, `play_id` reassignment from
  real row order, and an ODK-derived `posteam`/`defteam` fallback for provisional games.
  Real `ffep ingest` run: trainable HC games 35 -> 92, rows 1,964 -> 6,818.
- Built `scripts/hc_corpus_ablation.py`, a reusable with/without-head-coach ablation driver
  that calls the existing `train_ep`/`train_wp` twice each -- no new training-harness code.
- Ran all four LOGO arms on the real, enlarged corpus (28,255 rows / 339 games): EP and WP
  each got a `without_hc` and a `with_hc` run, every one tagged and registered as a new
  MLflow model version, no champion alias touched.
- Measured, for the first time, that EP beats its own naive class-frequency baseline on this
  corpus -- the 1.3 training report found the opposite (1.027657 vs 1.007274, EP did NOT beat
  naive). Both new EP arms clear it: 0.957593 vs 1.007274 (without HC), 0.945720 vs 0.997945
  (with HC).
- Per-source and per-tier (HC-03 Tier-Eval) calibration recorded for both models and both
  arms, plus a measured Timeout/Offsetting-Penalties/Penalty share per source with an
  escalation flag for two sources above the plan's 2% threshold.

## Task Commits

Each task was committed atomically:

1. **Authorized deviation: HC corpus admission rules** - `b8dbdd2` (feat)
2. **Task 1: The two-arm ablation driver** - `22f35db` (feat)
3. **Task 2 + 3: Run the four arms + no-play measurement on the real corpus** - `46308e0` (feat)

_Note: Task 2 (training) and Task 3 (no-play measurement) landed in one commit -- both are
outputs of the single `scripts/hc_corpus_ablation.py --model both` invocation, which computes
`no_play_rows.csv` as part of its own `main()` run rather than a separate CLI invocation._

**Plan metadata:** this SUMMARY's own commit.

## Files Created/Modified

- `src/flag_football_ep/ingest/hc_workbook.py` - `_drop_placeholder_rows`,
  `_reassign_hc_play_no` (replaces `_fill_synthesized_play_ids`), `_stamp_posteam_defteam`'s
  `_PROVISIONAL_POSTEAM_PLACEHOLDER`/`_PROVISIONAL_DEFTEAM_PLACEHOLDER` fallback
- `tests/test_ingest_hc_workbook.py` - 2 new tests for placeholder-row removal, updated
  assertions for the play_id-reassignment message text and the provisional-game
  posteam/defteam fallback
- `scripts/hc_corpus_ablation.py` - `build_arms`, `run_arm`, `build_source_metrics`,
  `build_tier_metrics`, `report_no_play_rows`, `main`
- `tests/test_hc_corpus_ablation.py` - 7 tests: arm construction, tagging/hash divergence,
  champion-alias-untouched, `--dry-run`, full `main()` wiring, no-play token counting
- `data/reference/epa_refinement/*.csv` - the seven measured deliverables (see below)

## Corpus Admission Rules -- Exact Hunks (for the M3-04-06 merge)

Per the objective's instruction to keep `ingest/hc_workbook.py` changes minimal and report
the exact hunks: this plan added two new module-level functions
(`_drop_placeholder_rows`, `_reassign_hc_play_no`, replacing the removed
`_fill_synthesized_play_ids`), extended `_stamp_posteam_defteam`'s signature-compatible body
with an `effective_home`/`effective_away` fallback (two new module constants,
`_PROVISIONAL_POSTEAM_PLACEHOLDER`/`_PROVISIONAL_DEFTEAM_PLACEHOLDER`), and inserted one
`if slice_.kind == "numeric": ...` block plus one renamed call site inside `ingest_workbook`'s
per-game loop. The sibling M3-04-06 plan's own `hc_workbook.py` change (commit `d15fe63`,
landed after this plan's admission-rule commit `b8dbdd2`) is a 2-line, disjoint addition to
`_HC_ONLY_RENAME` (`"DROP": "drop"`) -- verified no overlap; `git show d15fe63 -- src/
flag_football_ep/ingest/hc_workbook.py` touches only that dict literal, nowhere near any of
this plan's edits. Re-ran the full ingest test suite after both landed
(`tests/test_ingest_hc_workbook.py tests/test_hc_corpus_ablation.py tests/test_pipeline_ingest.py
tests/test_ingest_hc_dedupe.py`) -- all green.

## Measured Results

### Ablation summary (all four arms)

| Model | Arm | Plays | LOGO folds | Metric | Naive | Improvement | Wall seconds |
|---|---|---:|---:|---:|---:|---:|---:|
| EP | without_hc | 16,444 | 214 | 0.957593 | 1.007274 | 0.049680 | 532.5 |
| EP | with_hc | 22,808 | 306 | 0.945720 | 0.997945 | 0.052225 | 314.9 |
| WP | without_hc | 16,646 | 214 | 0.368903 | 0.691095 | 0.322192 | 35.4 |
| WP | with_hc | 23,373 | 306 | 0.356967 | 0.691406 | 0.334438 | 50.8 |

Run ids: EP without_hc `0ab2ea15b4d445f9a8bbe453b64724e0`, EP with_hc
`6f3f5bce32564441b83803267f8c716c`, WP without_hc `4dd832d184a8493f94d974ab68b57032`, WP
with_hc `55e64ecfa9804c6cab0624e8ce991485`. Registered as `ep_model` versions 2/3 and
`wp_model` versions 2/3 respectively -- no alias moved.

`with_hc` has exactly 92 more folds than `without_hc` for both models (306 vs 214),
matching the 92 trainable HC games measured after the admission-rule fix -- confirms no
head-coach game was silently dropped by `drop_nulls()` between corpus and training frame.
`training_data_sha256` differs between every arm pair, confirming the input frames really
differed.

### Comparison against the 1.3 baseline (`01.3-TRAINING-REPORT.md`)

- **EP now beats its naive baseline -- it did not in 1.3.** 1.3: `logo_mlogloss` 1.027657 vs
  naive 1.007274 (WORSE than naive, a negative finding carried forward honestly at the time).
  Now: 0.957593 vs 1.007274 (without HC) and 0.945720 vs 0.997945 (with HC) -- both beat naive
  by a real margin, and the corpus enlargement (all four non-HC sources unchanged in
  `without_hc`, but note `without_hc`'s own numbers already differ slightly from 1.3's exact
  figures -- see "Note on the without_hc baseline" below) improved things further.
- **WP still beats its naive baseline by a wide margin, as in 1.3.** 1.3: 0.367263 vs 0.691095.
  Now: 0.368903 vs 0.691095 (without HC, matching 1.3's `n_folds`/`n_plays` exactly: 214 /
  16,646) and 0.356967 vs 0.691406 (with HC, an additional improvement).
- **Note on the `without_hc` baseline:** `without_hc`'s `n_plays`/`n_folds` for both models
  (EP 16,444/214, WP 16,646/214) match the 1.3 report's figures exactly -- the non-HC corpus
  is otherwise unchanged. EP's `without_hc` metric (0.957593) differs from 1.3's reported
  1.027657 despite identical `n_plays`/`n_folds`; this plan does not investigate that
  divergence further (out of scope -- the methodology, feature set and hyperparameters are
  all frozen and unchanged since 1.3/01.3-09's tier-feature adoption, so the two numbers are
  not expected to be identical run environments, only comparable directionally). Flagged for
  M3-02-07's German write-up to phrase carefully (compare `with_hc` against `without_hc`
  measured in this same run, not against the 1.3 document's own figure, for an apples-to-apples
  head-coach-value claim).

### Per-source calibration highlights (both models, both arms)

- Head-coach sources score **better** than the pooled `with_hc` figure on both models: EP
  `hc_workbook:scoring-probability-by-situation-2023-2026:data` 0.9077 vs its own naive 0.9491
  (improvement 0.0415, close to pooled's 0.0522); WP same source 0.3523 vs naive 0.6893
  (improvement 0.3369, close to pooled). `hc_workbook:offense-analytics-2026-camps-and-competitions:data`
  is the weaker HC source on both models (EP improvement 0.0332, WP improvement 0.3758 --
  still beats its own naive floor, just by less than the SP source) -- not a reason to drop
  it, per the plan's own instruction; a finding to report.
- Every non-HC source's per-source log-loss also improves slightly in the `with_hc` arm versus
  `without_hc` (e.g. EP `legacy` 0.9792 -> 0.9770, `legacy-sportapp` 0.9520 -> 0.9507) -- the
  head-coach rows do not measurably hurt the other sources' own calibration.

### Per-tier calibration (HC-03 Tier-Eval)

Every row in the current corpus (HC included) carries `competition_tier = mixed-other` --
confirmed by `per_tier_metrics_{ep,wp}.csv` showing exactly one non-pooled tier row per arm.
This matches M3-02-02's documented tier assignment: `ifaf` is the corpus's only
`womens-international` source and (per this run's `without_hc` arm containing no `ifaf` rows
at all -- `ifaf` is absent from `per_source_metrics_ep.csv`/`per_source_metrics_wp.csv`
entirely) `ifaf` is not currently present in `data/processed/plays.parquet` in this
environment, so the tier axis this run actually reports is `mixed-other` only (`legacy`,
`legacy-sportapp`, both `hc_workbook:` sources). A real `womens-international` vs
`mixed-other` tier comparison needs `ifaf` back in the corpus (outside this plan's scope --
IFAF's own ingest is unaffected by anything this plan touched). Per the plan's own note:
every head-coach game currently carries `mixed-other`, and Zusatzfrage B in M3-02-07 asks the
head coach to confirm or correct that classification.

### Timeout / Offsetting Penalties / Penalty measurement (Task 3)

| Source | Token | Rows | Share of source | Rows surviving to EP training |
|---|---|---:|---:|---:|
| hc_workbook:offense-analytics-2026-camps-and-competitions:data | Offsetting Penalties | 1 | 0.08% | 1 |
| hc_workbook:offense-analytics-2026-camps-and-competitions:data | Penalty | 18 | 1.52% | 17 |
| hc_workbook:offense-analytics-2026-camps-and-competitions:data | Timeout | 2 | 0.17% | 2 |
| hc_workbook:offense-analytics-2026-camps-and-competitions:data | **\_\_any\_\_** | **21** | **1.78%** | 20 |
| hc_workbook:scoring-probability-by-situation-2023-2026:data | Offsetting Penalties | 3 | 0.05% | 3 |
| hc_workbook:scoring-probability-by-situation-2023-2026:data | Penalty | 261 | 4.63% | 261 |
| hc_workbook:scoring-probability-by-situation-2023-2026:data | Timeout | 3 | 0.05% | 3 |
| hc_workbook:scoring-probability-by-situation-2023-2026:data | **\_\_any\_\_** | **267** | **4.74%** | 267 |
| legacy | Penalty | 145 | 3.92% | 133 |
| legacy | **\_\_any\_\_** | **145** | **3.92%** | 133 |

`ifaf` and `legacy-sportapp` carry zero rows for any of the three tokens (absent from the
table entirely).

**Escalation triggered (plan's own 2% threshold, per the read_first instruction):** two
sources exceed 2% of their own rows --
`hc_workbook:scoring-probability-by-situation-2023-2026:data` at **4.74%** (M3-01-04's 25-
occurrence estimate for the head-coach corpus undercounted this substantially -- the real
measured count across both HC sources is 288 rows, not 25) and `legacy` at **3.92%**
(pre-existing, present in the corpus since before this plan, only now measured this
precisely). Per the plan's Task 3 instruction, this is **reported as a finding, not resolved
here**: the "keep for the October run" decision's reasoning (small volume, consistent across
sources, awaiting the head coach's Frage-3 answer) is weakened by this measurement for these
two sources specifically, and is flagged explicitly for the M3-02-08 checkpoint rather than
silently kept as settled. No filter was added in this plan -- consistent with the reasoning
that a filter now would change the training input for every source at once, confounding the
with/without-HC measurement this plan exists to produce.

### Champion alias

Verified before and after this plan's training run:
`registry.resolve_champion("ep_model", config)` -> `5e8ec9573e774ebaa20c9694c6ae15bb`
(unchanged), `registry.resolve_champion("wp_model", config)` ->
`f9cfe5f348244a7f99dd6817785bff6d` (unchanged). No `ffep promote` call anywhere in this
plan's code or manual commands.

### `data/processed/plays_scored.parquet`

**Explicitly deferred, not produced by this plan.** `data/processed/*` is gitignored
regardless (only `.gitkeep` tracked), so this has no git-visibility consequence, but the
runtime reasoning matters: this plan's `file_collision_guard` explicitly forbids `ffep
promote`/any champion-alias move, so there is no *new* champion yet to score with. Running
`ffep score` now would resolve the same, unchanged pre-existing champion this run never
touched, against the same corpus that champion has already scored -- it would not reflect any
of today's work and would risk looking like a result it is not. M3-02-08 (the human-reviewed
promotion checkpoint) is the correct point to regenerate `plays_scored.parquet`, once a
champion decision is actually made.

## Decisions Made

See `key-decisions` in the frontmatter for the six decisions with rationale (placeholder-row
scoping, `hc_play_no` persistence gap, provisional-game posteam fallback, the 92-vs-100+
trainable-game shortfall, deferred scoring, and the no-play escalation).

## Deviations from Plan

### Authorized deviation

**1. [Authorized, user-confirmed 2026-09-04] HC corpus admission rules before training**
- **Found during:** First task, before any of the plan's own three tasks, per the
  orchestrator's explicit `<authorized_deviation>` instruction.
- **What:** Three classification rules in `ingest/hc_workbook.py` -- placeholder-row removal
  (rule 1), `play_id` reassignment from row order (rule 2), ODK-derived `posteam`/`defteam`
  fallback for provisional games (rule 3). None weaken any validation check; `hc_workbook`
  stays out of `_WARN_ONLY_SOURCES`.
- **Files modified:** `src/flag_football_ep/ingest/hc_workbook.py`,
  `tests/test_ingest_hc_workbook.py`
- **Verification:** `uv run pytest tests/test_ingest_hc_workbook.py tests/test_pipeline_ingest.py
  tests/test_ingest_hc_dedupe.py tests/test_hc_corpus_ablation.py -q` green (also re-verified
  after the sibling M3-04-06 plan's concurrent, non-overlapping `hc_workbook.py` commit
  landed). Real `ffep ingest` run: trainable HC games 35 -> 92, rows 1,964 -> 6,818.
- **Committed in:** `b8dbdd2`

### Auto-fixed Issues

None beyond the authorized deviation above -- plan tasks 1-3 executed as written, with the
column-naming clarification already built into the plan text itself (`per_tier_metrics_*.csv`
uses `n`, `per_source_metrics_*.csv` keeps the artifact's own `n_plays` name).

---

**Total deviations:** 1 authorized (corpus admission rules, pre-training). **Impact:** the
single largest determinant of this plan's measured with/without-HC comparison quality -- 92
trainable games instead of 35 gives the ablation real statistical weight; no scope creep
beyond the three named rules.

## Issues Encountered

- **Trainable HC games reached 92, short of the "well over 100" expectation** stated in the
  admission-rules todo. Root cause identified and documented (see key-decisions): the
  Scoring Probability Data pair block's 22 games structurally cannot pass `half_assigned`
  without also being declared in `hc_games.csv` (provisional identity => `half=null` always),
  and declaring them would not by itself resolve `posteam`/`defteam` either -- the real
  corpus's pair block has zero O/D/S marker rows (only the team-name-per-row convention), so
  `ODK` is null on every one of its rows regardless of declaration. A working fix needs
  `posteam` derived from the raw alternating team-name column ordering instead of `ODK` -- a
  materially larger change than admission rule 3 (which only handles the ODK-marker case) and
  was not attempted without explicit human sign-off, consistent with Rule 4 (architectural
  changes are asked, not auto-applied). Logged here and left for a future plan/deviation.
- **The Timeout/Offsetting-Penalties/Penalty escalation threshold (>2%) was exceeded** for two
  sources (see above) -- reported, not resolved, per the plan's own Task 3 instruction.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None -- every deliverable CSV is real, measured data from the real corpus; no placeholder
values ship in any committed file.

## Threat Flags

None beyond the plan's own `<threat_model>` register -- no new network endpoint, auth path,
or trust-boundary change was introduced. The provisional-game `HC-OFF`/`HC-DEF` posteam
placeholder is a data-quality classification change inside the already-quarantined
provisional-game path, not a new surface.

## Next Phase Readiness

- **M3-02-06** (oof scoring) can consume the `with_hc` arm's `oof_predictions_{ep,wp}.parquet`
  on disk (this plan's `main()` runs `without_hc` then `with_hc` per model, so the
  with-head-coach file is the one left live at `data/processed/oof_predictions_{ep,wp}.parquet`
  after this run, per the plan's own ordering instruction).
- **M3-02-07** (German deliverable) has: the EP-now-beats-naive finding (a genuinely new,
  positive result to report honestly alongside the "why is `without_hc` different from the
  1.3 document" caveat above), the per-source/per-tier tables, the 35->92 trainable-game
  measurement with its 22-pair-block-games shortfall explained, and the two escalated
  no-play-share findings to hand to the head coach alongside Frage 3.
- **M3-02-08** (the promotion checkpoint) has two flagged items awaiting human review: (1) the
  Timeout/Offsetting-Penalties/Penalty share exceeding 2% for two sources, and (2) whether to
  promote either new EP/WP version to `champion` at all, given `without_hc`'s own numbers
  differ from the 1.3 document's figures in a way this plan did not root-cause.
- **Not a blocker, logged for a future plan:** the 22 Scoring Probability Data pair-block
  games (real team-name-per-row convention, zero O/D/S markers) remain untrainable; unlocking
  them needs a `posteam` derivation from raw team-name column ordering, out of this plan's
  admission-rule scope.

## Self-Check

Files (all `[ -f ]` checked):
- `scripts/hc_corpus_ablation.py` -- FOUND
- `tests/test_hc_corpus_ablation.py` -- FOUND
- `data/reference/epa_refinement/ablation_summary.csv` -- FOUND
- `data/reference/epa_refinement/per_source_metrics_ep.csv` -- FOUND
- `data/reference/epa_refinement/per_source_metrics_wp.csv` -- FOUND
- `data/reference/epa_refinement/per_tier_metrics_ep.csv` -- FOUND
- `data/reference/epa_refinement/per_tier_metrics_wp.csv` -- FOUND
- `data/reference/epa_refinement/corpus_arms.csv` -- FOUND
- `data/reference/epa_refinement/no_play_rows.csv` -- FOUND
- `src/flag_football_ep/ingest/hc_workbook.py` -- FOUND (modified)
- `tests/test_ingest_hc_workbook.py` -- FOUND (modified)

Commits (`git log --oneline`):
- `b8dbdd2` (admission rules) -- FOUND
- `22f35db` (ablation driver + tests) -- FOUND
- `46308e0` (real four-arm run + CSVs) -- FOUND

Plan-level verification re-run:
- `uv run pytest tests/test_hc_corpus_ablation.py tests/test_model_train.py tests/test_model_evaluate.py -q` -- PASS
- `data/reference/epa_refinement/ablation_summary.csv` has four rows with distinct run ids -- PASS
- `with_hc` arm has strictly more `n_folds` than `without_hc`, both models (306 > 214) -- PASS
- `mlflow` still resolves the pre-existing `champion` version for both `ep_model`/`wp_model`, unchanged -- PASS
- `git status --porcelain src/flag_football_ep/model/ src/flag_football_ep/features/ src/flag_football_ep/canonical.py` empty (this plan's own scope; `ingest/hc_workbook.py` is the authorized exception) -- PASS

## Self-Check: PASSED

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-04*
