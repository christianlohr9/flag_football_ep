---
phase: M3-02-epa-refinement
plan: RERUN-2026-09-08
subsystem: model
tags: [xgboost, mlflow, leave-one-game-out, ifaf, corpus-fingerprint, epa-comparison]

# Dependency graph
requires:
  - phase: M3-02-epa-refinement
    provides: "M3-02-05's locked ablation methodology (scripts/hc_corpus_ablation.py),
      M3-02-06's HC-vs-model comparison (scripts/epa_comparison.py), M3-02-07's doc-vs-CSV
      guard (tests/test_m3_epa_docs.py) -- all re-run here on the current corpus, none
      re-authored"
  - phase: IFAF rebuild (2026-09-06/07, docs/ifaf-wm2026-daten.md)
    provides: "the reviewer-feed-based IFAF corpus (1,951 clean rows / 21 games, down from
      3,191 pre-correction rows / 32 games), the SCORE-ledger scoring gate, and the
      events-ledger-synthetic row marking this rerun verifies is excluded from training"
provides:
  - "A re-measurement of the locked LOGO ablation on the 2026-09-08 corpus (27,015 rows / 328
    games): four new MLflow runs (EP/WP x without_hc/with_hc), each tagged with a
    corpus_fingerprint and git_commit param in addition to the existing
    training_data_sha256"
  - "data/reference/epa_refinement/2026-09-08/*.csv (10 files) -- the same shape as the
    2026-09-04 CSVs, in a dated subfolder so the reviewed 2026-09-04 report stays
    byte-reproducible"
  - "A dated Nachtrag in docs/epa-refinement-2026-10.md reporting the before/after numbers,
    and docs/epa-modellkarte.md, a new one-page German model card"
  - "The finding that IFAF is, for the first time, part of EP/WP training (yards_to_go
    derivation landed after 2026-09-04); EP is stable to this, WP loses ~1.5-2.3pp of
    log-loss improvement, still clearly beating naive"
  - "The finding that failed (unsuccessful) extra-point attempts are NOT excluded from EP/WP
    training -- pre-existing since Phase 1.3, not an IFAF regression, not fixed here"
affects: [M3-02-08-promotion-checkpoint]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "corpus_fingerprint (sha256 over sorted (game_id, play_id, source) of the raw corpus)
      and git_commit logged as MLflow params on every ablation run, distinct from the
      existing per-arm training_data_sha256 (which hashes the post-drop_nulls training
      frame, not the raw corpus every arm/model saw)"
    - "dated subfolder under data/reference/epa_refinement/ for a corpus rerun, keeping the
      reviewed/frozen 2026-09-04 CSVs untouched and independently reproducible"
    - "section-scoped doc-guard tests (bidirectional run-id, figure-match, CSV-coverage) --
      the same pattern M3-02-07 used for structured-table scoping, applied to a Nachtrag
      section boundary instead of a table header marker"

key-files:
  created:
    - data/reference/epa_refinement/2026-09-08/ablation_summary.csv
    - data/reference/epa_refinement/2026-09-08/corpus_arms.csv
    - data/reference/epa_refinement/2026-09-08/per_source_metrics_ep.csv
    - data/reference/epa_refinement/2026-09-08/per_source_metrics_wp.csv
    - data/reference/epa_refinement/2026-09-08/per_tier_metrics_ep.csv
    - data/reference/epa_refinement/2026-09-08/per_tier_metrics_wp.csv
    - data/reference/epa_refinement/2026-09-08/no_play_rows.csv
    - data/reference/epa_refinement/2026-09-08/comparison_by_dd.csv
    - data/reference/epa_refinement/2026-09-08/comparison_clustered.csv
    - data/reference/epa_refinement/2026-09-08/comparison_coverage.csv
    - docs/epa-modellkarte.md
  modified:
    - scripts/hc_corpus_ablation.py
    - docs/epa-refinement-2026-10.md
    - docs/hc-sync-2026-10.md
    - tests/test_m3_epa_docs.py

key-decisions:
  - "Non-ifaf sources (legacy, legacy-sportapp, both hc_workbook sources) are byte-identical
    in row/game count between 2026-09-04 and 2026-09-08 -- verified via git log showing zero
    commits touching validation.py/canonical.py/pipeline.py/ingest/hudl.py/
    ingest/legacy_sportapp.py/ingest/hc_workbook.py since M3-02-05. Only IFAF moved. The 92
    trainable head-coach games figure from the main report is therefore still exactly
    correct and did not need re-verifying from scratch."
  - "IFAF rows are, for the first time, part of the EP/WP training frames: on 2026-09-04
    yards_to_go was null for essentially every IFAF row (the derivation landed after that
    date), so drop_nulls() silently excluded IFAF from training entirely even though it was
    present in the raw corpus. This rerun measures IFAF's real training contribution for the
    first time -- not comparable to '0' in the 2026-09-04 report because that 0 was a
    plumbing gap, not a measurement of IFAF's quality."
  - "The extra_point training-leak finding is reported, not fixed. Fixing it (excluding
    every play_type == 'extra_point' row regardless of success) would be a methodology
    change, and this task's mandate was to re-run the LOCKED methodology, not revise it.
    Flagged in both the doc Nachtrag and here for a future plan's explicit decision."
  - "Kept dated-subfolder CSVs rather than a date-suffixed flat filename
    (data/reference/epa_refinement/2026-09-08/*.csv vs
    ablation_summary_2026-09-08.csv) -- lets the rerun reuse hc_corpus_ablation.py's/
    epa_comparison.py's existing --out-dir flag unmodified, and keeps each dated run's ten
    files visually grouped."

requirements-completed: []

# Metrics
duration: ~2h
completed: 2026-09-08
---

# Phase M3-02 Rerun 2026-09-08: EPA refinement re-measured post-IFAF-correction Summary

**Re-ran the locked LOGO ablation (M3-02-05/06 methodology, unmodified) on the corpus after the
2026-09-06/07 IFAF rebuild (3,191 garbage-context rows -> 1,951 clean rows / 21 games): EP is
essentially unchanged, WP loses about 1.5-2.3 percentage points of log-loss improvement (still
clearly beating naive) because IFAF's own WP fit is only marginally better than naive, and the
92 trainable head-coach games and their contribution are confirmed byte-identical to the
2026-09-04 report.**

## Performance

- **Duration:** ~2h
- **Started:** 2026-09-08T11:13:00Z (approx.)
- **Completed:** 2026-09-08T13:35:00Z (approx.)
- **Files modified:** 15 (11 created, 4 modified)

## Accomplishments

- Ran a fresh `ffep ingest` + `ffep score`: current corpus is 27,015 rows / 328 games
  (`legacy` 3,701/47, `legacy-sportapp` 14,545/168, `hc_workbook:offense-analytics...` 1,183/25,
  `hc_workbook:scoring-probability...` 5,635/67, `ifaf` 1,951/21) -- champion alias unchanged
  before and after (`ep_model` -> `5e8ec9573e774ebaa20c9694c6ae15bb`, `wp_model` ->
  `f9cfe5f348244a7f99dd6817785bff6d`).
- Verified the corpus is unchanged outside IFAF since 2026-09-04 (`git log` shows zero commits
  to the shared ingest/validation paths since M3-02-05's admission-rule commit) -- the 92
  trainable head-coach games figure is confirmed still exactly correct.
- Verified `score_source == "events-ledger-synthetic"` rows (27 in the current corpus) have
  zero overlap with the EP/WP training frames (both models, checked directly against the
  reconstructed labeled frame). Found and documented, but did NOT fix, that
  `play_type == "extra_point"` rows are not fully excluded: only *successful* PAT/2pt
  attempts get a null label; 537 (EP) / 1,103 (WP) failed-attempt rows still enter training,
  spread across `legacy`, both `hc_workbook` sources, and `ifaf` -- pre-existing since Phase
  1.3, verified via `git log` that the relevant code in `features/mutations.py` is untouched.
- Extended `scripts/hc_corpus_ablation.py` with `compute_corpus_fingerprint`/`git_commit_sha`,
  logged as MLflow params (`corpus_fingerprint`, `git_commit`) on every run, alongside the
  existing `training_data_sha256`.
- Re-ran all four LOGO arms (`hc_corpus_ablation.py --model both`, detached via `nohup`,
  polled with foreground `until` loops) and `epa_comparison.py`, writing ten CSVs to
  `data/reference/epa_refinement/2026-09-08/` -- the 2026-09-04 CSVs are untouched.
- Appended a dated Nachtrag to `docs/epa-refinement-2026-10.md` with the before/after tables,
  and extended `tests/test_m3_epa_docs.py` with section-scoped run-id, figure-match,
  fingerprint/commit and CSV-coverage checks for the new subfolder -- the original
  2026-09-04 checks are unchanged in strength, only re-scoped to the main-report text so the
  Nachtrag's disjoint run ids don't false-positive them.
- Wrote `docs/epa-modellkarte.md`, a one-page German model card, and linked it additively
  from both `docs/epa-refinement-2026-10.md` and `docs/hc-sync-2026-10.md`.

## Task Commits

1. **Corpus fingerprint + git commit params** - `9bbe03a` (feat)
2. **Re-measured ablation CSVs (2026-09-08 subfolder)** - `0fb5f7e` (feat)
3. **Doc Nachtrag** - `5d84572` (docs)
4. **Doc guard extension** - `0914781` (test)
5. **German model card + links** - `7fbd32a` (docs)

No separate plan-metadata commit exists for this ad hoc task (no PLAN.md/STATE.md/ROADMAP.md
touched, per the objective's explicit rules).

## Before/After Numbers

### Corpus

| | 2026-09-04 | 2026-09-08 |
|---|---:|---:|
| Total rows | 28,255 | 27,015 |
| Total games | 339 | 328 |
| `ifaf` rows / games | 3,191 / 32 (pre-correction, raw-accepted but never reached EP/WP training -- `yards_to_go` null) | 1,951 / 21 (reviewer-feed, SCORE-ledger-checked, synthetic rows flagged) |
| Everything else | 25,064 rows / 307 games | 25,064 rows / 307 games (byte-identical) |

Corpus fingerprint (2026-09-08): `ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd`.
Commit: `82ae8cc88908283094dde036def7a883f1b5214a` (the objective's starting HEAD).

### Ablation (four arms)

| Model | Arm | Verbesserung 04.09. | Verbesserung 08.09. | Run-ID (08.09.) |
|---|---|---:|---:|---|
| EP | ohne HC | 0.049680 | 0.049710 | `e186af941262407696ce7493f5920fc7` |
| EP | mit HC | 0.052225 | 0.051610 | `97259da7acaf43f3b2c65e59f7f11694` |
| WP | ohne HC | 0.322192 | 0.299272 | `84120926c81b49be828f5ad94c89ca76` |
| WP | mit HC | 0.334438 | 0.319215 | `2c8c249d295d4ce9a2845800c459c153` |

Both `with_hc` arms still clear their naive baseline by a wide margin (EP 0.942659 vs 0.994269;
WP 0.372350 vs 0.691566). The fold delta between arms is exactly 92 for both models on both
dates -- the head-coach contribution itself is unchanged; the shift is corpus composition
(IFAF now flows into both arms, since `without_hc` only excludes `hc_workbook:` rows).

### Per-source (arm "mit HC") -- new finding

`ifaf` is, for the first time, part of the EP/WP training frames (n=1,286 EP / 1,332 WP).
EP: 0.881065 vs naive 0.914288 (beats it). WP: 0.689286 vs naive 0.693119 (barely beats it);
in the `without_hc` arm specifically, WP on `ifaf` alone is **worse than naive**
(0.696474 vs 0.693119, improvement -0.003355) -- the first source in this project where WP
loses to the naive baseline. Plausible but unproven cause: WP's synthetic, evenly-decremented
clock may fit IFAF's real game flow worse than the other sources.

### Tier eval -- real for the first time

2026-09-04: every row carried `competition_tier = mixed-other` (no real second tier to
compare). 2026-09-08, with IFAF in training: `mixed-other` (EP 0.946132/0.997945,
WP 0.354289/0.691406) vs `womens-international` (EP 0.881065/0.914288 -- better than
mixed-other; WP 0.689286/0.693119 -- much worse than mixed-other). Same underlying finding as
the per-source table, seen from the tier axis.

## Promotion Candidates

**Not executed in this task** (owner decision, per the objective's explicit rule). If the
2026-09-08 `with_hc` runs are chosen for promotion instead of the 2026-09-04 ones (both are
valid candidates; 2026-09-08 additionally includes IFAF, 2026-09-04 does not):

```bash
uv run ffep promote --model ep --run 97259da7acaf43f3b2c65e59f7f11694
uv run ffep promote --model wp --run 2c8c249d295d4ce9a2845800c459c153
```

The 2026-09-04 `with_hc` runs remain the alternative candidates (no IFAF, see the main report):

```bash
uv run ffep promote --model ep --run 6f3f5bce32564441b83803267f8c716c
uv run ffep promote --model wp --run 55e64ecfa9804c6cab0624e8ce991485
```

Current champion, unmoved by this task: `ep_model` -> `5e8ec9573e774ebaa20c9694c6ae15bb`,
`wp_model` -> `f9cfe5f348244a7f99dd6817785bff6d` (both the original Phase-1.3 version, from
before any head-coach or IFAF data existed in the corpus).

## Decisions Made

See `key-decisions` in the frontmatter: non-IFAF corpus stability verified via git log rather
than re-measured from scratch; the IFAF-now-in-training framing (not comparable to the
2026-09-04 report's implicit zero); the extra-point leak reported-not-fixed decision; the
dated-subfolder CSV layout choice.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - missing critical] Added corpus_fingerprint/git_commit MLflow params**
- **Found during:** Task 1/2 preparation
- **Issue:** The locked ablation driver (`scripts/hc_corpus_ablation.py`) had no way to tie
  a given MLflow run back to the exact corpus/codebase pairing that produced it, beyond the
  existing per-arm `training_data_sha256` (which hashes a post-filter training frame, not the
  raw corpus). The objective explicitly required this.
- **Fix:** Added `compute_corpus_fingerprint`/`git_commit_sha` helpers, threaded through
  `run_arm`/`main`, logged via `MlflowClient.log_param`.
- **Files modified:** `scripts/hc_corpus_ablation.py`
- **Verification:** `uv run pytest tests/test_hc_corpus_ablation.py -q` (7/7 green,
  unmodified assertions still pass); the CSV's `corpus_fingerprint` column is identical
  across all four arms and matches the value independently recomputed by the ad hoc
  corpus-state check before and after `ffep score`.
- **Committed in:** `9bbe03a`

**2. [Rule 1 - test scope fix] Re-scoped the run-id bidirectional check to the main report**
- **Found during:** Writing the Nachtrag's own doc-guard tests
- **Issue:** `test_run_ids_match_ablation_summary_bidirectionally` scans the *entire* document
  for 32-hex-char run ids and checks them against the 2026-09-04 CSV. The Nachtrag legitimately
  cites four different run ids from a different CSV -- a whole-document scan would have failed
  the original, unmodified test the moment the Nachtrag was appended.
- **Fix:** Scoped the existing test to `doc_text.split(NACHTRAG_SECTION_MARKER, 1)[0]` (the
  original test's assertions and strictness are unchanged, only its input text is narrowed to
  the section it was always meant to check) and added a mirror-image test scoped to the
  Nachtrag section against the new CSV.
- **Files modified:** `tests/test_m3_epa_docs.py`
- **Verification:** `uv run pytest tests/test_m3_epa_docs.py -q` -- 16/16 green.
- **Committed in:** `0914781`

---

**Total deviations:** 2 auto-fixed (1 missing-critical addition, 1 necessary test re-scoping).
**Impact:** both were required for the objective's own explicit instructions (fingerprint/commit
params; "extend the guard... never loosen it") to be satisfiable at all -- no scope creep beyond
what those instructions already implied.

## Issues Encountered

- **The extra-point training-leak finding** (failed PAT/2pt attempts entering EP/WP training)
  is real, pre-existing since Phase 1.3, and NOT fixed here -- fixing it would be a methodology
  change outside this task's "re-run the locked methodology exactly" mandate. Flagged in the
  doc Nachtrag and here for a future plan's explicit decision.
- **WP's per-source `ifaf` result in the `without_hc` arm is negative** (worse than naive) --
  reported honestly, not smoothed over. First such case in this project's history.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None -- every number in the Nachtrag and the model card comes from a freshly measured,
committed CSV (`tests/test_m3_epa_docs.py` verifies this automatically).

## Threat Flags

None beyond the phase's existing threat register (M3-02-05/06/07's mitigations still apply
unchanged to this re-run of the same code paths). No new network endpoint, auth path, or
schema change was introduced; the two new MLflow params (`corpus_fingerprint`, `git_commit`)
are provenance metadata, not a new trust boundary.

## Next Phase Readiness

- M3-02-08 (the still-pending human-reviewed promotion checkpoint) now has TWO valid `with_hc`
  candidate pairs to choose between (2026-09-04, without IFAF; 2026-09-08, with IFAF) --
  exact `ffep promote` commands for both are listed above, neither executed.
- `docs/epa-modellkarte.md` is ready to hand to the head coach alongside the October sync
  documents; it is explicitly framed as a summary, not a replacement for
  `docs/epa-refinement-2026-10.md`'s full derivation.
- No blockers. `git status --porcelain data/ src/ scripts/ tests/` is clean except a
  pre-existing, unrelated `data/reference/ifaf_spot_fill/` file pair that predates this task
  and was left untouched.

## Self-Check

Files (all `[ -f ]` checked):
- `data/reference/epa_refinement/2026-09-08/ablation_summary.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/corpus_arms.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/per_source_metrics_ep.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/per_source_metrics_wp.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/per_tier_metrics_ep.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/per_tier_metrics_wp.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/no_play_rows.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/comparison_by_dd.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/comparison_clustered.csv` -- FOUND
- `data/reference/epa_refinement/2026-09-08/comparison_coverage.csv` -- FOUND
- `docs/epa-modellkarte.md` -- FOUND
- `scripts/hc_corpus_ablation.py` -- FOUND (modified)
- `docs/epa-refinement-2026-10.md` -- FOUND (modified)
- `docs/hc-sync-2026-10.md` -- FOUND (modified)
- `tests/test_m3_epa_docs.py` -- FOUND (modified)

Commits (`git log --oneline`):
- `9bbe03a` -- FOUND
- `0fb5f7e` -- FOUND
- `5d84572` -- FOUND
- `0914781` -- FOUND
- `7fbd32a` -- FOUND

Plan-level verification re-run:
- `uv run pytest tests/test_m3_epa_docs.py tests/test_m3_epa_snapshot.py tests/test_hc_corpus_ablation.py tests/test_reports_hc_comparison.py -q` -- 55 passed
- `git status --porcelain data/ src/ scripts/ tests/` -- clean except the pre-existing,
  unrelated `data/reference/ifaf_spot_fill/` file pair (predates this task, left untouched)
- champion resolves unchanged: `ep_model` -> `5e8ec9573e774ebaa20c9694c6ae15bb`,
  `wp_model` -> `f9cfe5f348244a7f99dd6817785bff6d`
- no `ffep promote` call anywhere in this task's commands (verified against full bash history
  of this session)

## Self-Check: PASSED

---
*Phase: M3-02-epa-refinement*
*Completed: 2026-09-08*
