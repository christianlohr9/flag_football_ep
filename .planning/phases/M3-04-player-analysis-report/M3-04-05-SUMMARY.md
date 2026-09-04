---
phase: M3-04-player-analysis-report
plan: 05
subsystem: reports
tags: [jinja2, reports, hc-workbook, player-analysis, explosiveness, headless-matplotlib]

# Dependency graph
requires:
  - phase: M3-04-player-analysis-report
    provides: "M3-04-03's reports/player_analysis.py::hc_columns_by_qb/HcColumnTable (his 19 columns); M3-04-04's m3_columns_by_qb/PlayerAnalysisSplit/PlayerAnalysisReportData/build_player_analysis_data (the render-ready data object, split by camp); Phase 1.4's reports/render.py::render_page/fig_to_data_uri/write_report_run and reports/build.py's per-product dispatch/isolation convention; M3-3's charts/explosiveness.py::render_cliff_zone/render_definition_comparison"
provides:
  - "reports/player_analysis.py::build_player_analysis_page(data, *, generated_on=None) -> str: the standalone German HTML page -- his columns beside ours per split, unavailable columns named via a distinct state, muted rows greyed, conflict splits marked, both M3-3 charts embedded once per page"
  - "src/flag_football_ep/templates/player_analysis.html.j2: extends base.html.j2, no |safe filter anywhere"
  - "reports/build.py::PRODUCTS now includes 'player-analysis' (fifth product), with its own per-product try/except isolation branch and PLAYER_ANALYSIS_FILENAME dispatch"
  - "PlayerAnalysisReportData.corpus_comparison_table/corpus_cliff_table: two new optional (defaulted) fields carrying the page-wide M3-3 chart inputs"
affects: ["M3-04-07"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Page-level (not per-section) chart embedding: render_definition_comparison/render_cliff_zone called once per page from data computed alongside the korpus split, wrapped in try/except so a chart failure never fails the page"
    - "Row-level muted flag (his own attempts < MUTED_MIN_N boolean) applied uniformly to both HC and M3 cells in one joined row, mirroring own_team_report.html.j2's established per-row (not per-cell) muting convention rather than inventing a new one"
    - "A dedicated _UNAVAILABLE German string, distinct from own_team.py's _NO_DATA/_NOT_APPLICABLE, so a column absent from the corpus today can never be visually confused with a real zero or an inapplicable stat"

key-files:
  created:
    - src/flag_football_ep/templates/player_analysis.html.j2
    - tests/test_reports_player_analysis_page.py
  modified:
    - src/flag_football_ep/reports/player_analysis.py
    - src/flag_football_ep/reports/build.py
    - src/flag_football_ep/cli.py
    - tests/test_cli_report.py
    - docs/coaching-reports.md

key-decisions:
  - "Extended PlayerAnalysisReportData (M3-04-04's dataclass) with two new fields, corpus_comparison_table/corpus_cliff_table, both defaulting to a schema-correct empty frame via default_factory -- additive only, no existing field renamed or removed, no M3-04-04 test broke. Necessary because build_player_analysis_page(data) has no raw-plays parameter (per this plan's own literal <action> signature), yet the cliff-zone chart needs a play-level frame no per-player table can reconstruct; computed once, from the same canon frame the korpus split already sees, via the same M3-3 definition_comparison/cliff_zone_table calls (never a second CI/shrinkage implementation) -- documented as a Rule 2 deviation below."
  - "Only three of the four DEFINITIONS keys appear per player (Success Rate, Explosiveness kalibriert, Kontinuierlicher Score) matching M3-04-04-SUMMARY's own framing; baseline_hc_workbook/baseline_hc_verbal (cross-checks against his own Explosive % column) appear only in the page-wide definition-comparison chart, not duplicated per player."
  - "Muted flag applied at the row level (his own attempts<5 boolean), not per cell, mirroring own_team_report.html.j2's only precedent in this codebase rather than inventing a new per-cell muting scheme."

requirements-completed: [HC-05]

# Metrics
duration: ~90min
completed: 2026-09-04
---

# Phase M3-04 Plan 05: Player Analysis Report — Page, Product, Docs Summary

**`ffep report --product player-analysis` now renders his `Player Analysis All Camps` tab as a standalone German HTML page (his 19 columns beside our three M3-3 columns, both M3-3 charts embedded once per page) as the fifth product in the same batch, timed at well under the ten-minute REQ-S1-16 budget on a synthetic corpus.**

## Performance

- **Duration:** ~90 min
- **Started:** 2026-09-04 (worktree base recovered from `worktree-planning-docs` before any work)
- **Completed:** 2026-09-04
- **Tasks:** 2
- **Files modified:** 4 (`player_analysis.py`, `build.py`, `cli.py`, `test_cli_report.py`), 3 created (`player_analysis.html.j2`, `test_reports_player_analysis_page.py`, this SUMMARY)

## Accomplishments

- `build_player_analysis_page(data, *, generated_on=None) -> str` (Task 1): renders a
  standalone HTML document from `PlayerAnalysisReportData` — summary block naming
  `n_hc_rows` and unavailable columns, unmapped-player/notice warning blocks, both M3-3 charts
  embedded once (not per section), then one `<h2>` section per split with his 19 HC columns
  joined to our 3 M3 columns on `spieler` into a single row per player. A column in
  `HcColumnTable.unavailable` always renders `"nicht verfügbar (siehe Hinweise)"`, never a
  blank or a zero. `label_status == "conflict"` splits carry a visible dispute paragraph right
  under the heading. Every cell is pre-formatted in Python (`_format_hc_cell`/
  `_format_m3_rate_cell`); the template does no arithmetic.
- `src/flag_football_ep/templates/player_analysis.html.j2` (101 lines): extends `base.html.j2`,
  zero `|safe` filters (grep-gated in both the test suite and the plan's own verify command).
- `PlayerAnalysisReportData.corpus_comparison_table`/`corpus_cliff_table` (two new, defaulted,
  additive fields on M3-04-04's dataclass): the page-wide `definition_comparison`/
  `cliff_zone_table` inputs the two M3-3 charts render, computed inside
  `build_player_analysis_data` from the same `canon` frame the `korpus` split sees — see
  Deviations below.
- `reports/build.py`: `PRODUCTS` is now a five-tuple; a new dispatch branch builds
  `player-analysis.html` with the same per-product `try`/`except Exception` isolation and
  German-notice discipline every other product uses — proven by a monkeypatched-failure test
  (the other products still render when `build_player_analysis_data` raises).
- `cli.py`: `--product` help string lists all five names; the validation code already read
  `PRODUCTS` dynamically, confirmed unchanged.
- `docs/coaching-reports.md`: "The five products" table, a new "Player Analysis (HC-05)"
  section (what it reproduces, its splits, its command) and a "Was heute noch fehlt" list
  naming the missing WR table, the three drop-dependent columns, the Air-Yards deviation and
  the Camp IV/VI naming conflict.
- **Real end-to-end run** (see "Real-corpus run" below): all five products, including
  `player-analysis.html`, built and written to `reports/<date>/` and `reports/latest/` in one
  `run_report_pipeline` call against a real (synthetic) corpus, self-contained, no network, no
  `<script>` tag, well under budget.

## Task Commits

1. **Task 1: The German page and its template** - `2addd27` (feat)
2. **Task 2: Fifth product, CLI wiring, documentation and the timed real run** - `133ba8c` (feat)

**Plan metadata:** committed alongside this summary.

## Files Created/Modified

- `src/flag_football_ep/reports/player_analysis.py` - added `build_player_analysis_page` and
  its formatting/split-context helpers; added `_corpus_comparison_table`/`_corpus_cliff_table`
  and two new `PlayerAnalysisReportData` fields (see Deviations)
- `src/flag_football_ep/templates/player_analysis.html.j2` - the new page template
- `src/flag_football_ep/reports/build.py` - `PRODUCTS` five-tuple, new dispatch branch
- `src/flag_football_ep/cli.py` - `--product` help string
- `tests/test_reports_player_analysis_page.py` - 19 tests for the page/template
- `tests/test_cli_report.py` - renamed the locked-products test to the five-tuple, added one
  isolation test
- `docs/coaching-reports.md` - fifth product documented

## Decisions Made

See `key-decisions` in frontmatter. Summary: extended `PlayerAnalysisReportData` additively
(two new defaulted fields) rather than giving `build_player_analysis_page` a raw-plays
parameter, since the plan's own literal signature took only `data`; only 3 of 4 `DEFINITIONS`
keys are shown per player (matching M3-04-04-SUMMARY's framing), all 4 appear in the page-wide
comparison chart; muted flag applied per row (his own `attempts < 5`), mirroring
`own_team_report.html.j2`'s only precedent.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added `corpus_comparison_table`/`corpus_cliff_table` fields to `PlayerAnalysisReportData`**
- **Found during:** Task 1, while implementing the two required M3-3 page-level charts
- **Issue:** The plan's own `<action>` text gives `build_player_analysis_page(data: PlayerAnalysisReportData, *, generated_on: date | None = None) -> str` as the literal signature — no raw-plays parameter. But `render_cliff_zone` needs a play-level, per-yard frame that no field on `PlayerAnalysisSplit`/`PlayerAnalysisReportData` (as frozen by M3-04-04) can reconstruct: `HcColumnTable.table`/`m3_table` are already player-aggregated. Without new data, the must-have "two M3-3 charts embedded" bullet is unsatisfiable from `data` alone.
- **Fix:** Added two new fields to `PlayerAnalysisReportData`, both `field(default_factory=...)` to a schema-correct empty frame — fully additive, no existing field touched, no M3-04-04 test broke (`PlayerAnalysisReportData`/`PlayerAnalysisSplit` are never constructed positionally or directly in `tests/test_reports_player_analysis.py`, confirmed by grep before making the change). Computed inside `build_player_analysis_data` from the SAME `canon` frame the `korpus` split's `hc_columns_by_qb`/`m3_columns_by_qb` already see, via the same live `definition_comparison`/`cliff_zone_table` calls `m3_columns_by_qb` and `scripts/explosiveness_comparison.py::_build_overall_table` already use — never a second Clopper-Pearson/shrinkage implementation. Neither new field carries a player name.
- **Files modified:** `src/flag_football_ep/reports/player_analysis.py`
- **Verification:** `tests/test_reports_player_analysis.py` (M3-04-04's own 37 tests) still pass unmodified; `tests/test_reports_player_analysis_page.py::test_both_charts_embedded_once_per_page` and the real end-to-end run both confirm two `data:image/png;base64,` entries appear
- **Committed in:** `2addd27` (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Necessary to satisfy the plan's own must-have ("the two M3-3 charts embedded as data URIs") given the literal page-builder signature it specifies; strictly additive to M3-04-04's data layer, no redesign, no existing behavior changed.

## Issues Encountered

**Worktree base-selection bug (#2015)**, same pattern already documented in M3-04-03/04's own
summaries: this worktree's branch was initially on `origin/main` (a pre-planning commit), not
the feature branch carrying M3-04-01 through 04/06's merged work. Verified `HEAD` was on a
proper per-agent branch (`worktree-agent-a7cb1ceedccd0c314`, not a protected ref), then
`git reset --hard worktree-planning-docs` before any task work, per the objective's own
instruction. Consumed setup time, did not affect implementation.

**Real-corpus render not runnable against `ffep.toml` in this sandbox.** `data/processed/
plays_scored.parquet`/`plays.parquet` are DVC-tracked and not materialised here, and `dvc` is
not installed (confirmed: `data/processed/` does not even exist, `which dvc` fails) — the same
pre-existing environment gap M3-04-04-SUMMARY already documented. Per the objective's explicit
instruction, this is stated here rather than stalled on. Instead: (1) the full `pytest`
suite's own `tests/test_cli_report.py::TestReportEndToEnd` — which ingests, trains, scores and
reports against a real (synthetic) corpus end to end, entirely inside the sandbox, with no DVC
involved — now exercises the `player-analysis` product automatically (it requests
`products=None`, i.e. all five) and passed; (2) a second, standalone real run was performed for
this SUMMARY's own record (below), reusing the same test-helper corpus/training machinery but
writing into this worktree's actual `reports/` directory (`config.paths.reports`) instead of a
pytest `tmp_path`, so the plan's `<verify>` command's *effect* (`reports/<date>/` +
`reports/latest/` on disk, read once, self-contained) is demonstrated even though the literal
`ffep report --product player-analysis --skip-ingest` command against the tracked `ffep.toml`
cannot run here.

**Command the coordinator should run for the real-corpus render** (once
`data/processed/plays.parquet`/`plays_scored.parquet` are materialised via `dvc pull` or
equivalent, and both `champion` model aliases are set, on the main tree):

```bash
uv run ffep report --product player-analysis --skip-ingest
```

or, to build the full five-product batch and get the REQ-S1-16 timing block:

```bash
uv run ffep report --skip-ingest
```

**Real (synthetic-corpus) run performed in this sandbox** — command equivalent to the above,
executed via `run_report_pipeline` directly against a config pointed at this worktree's own
`reports/` directory (reusing `tests/test_cli_report.py::_report_ready_toml`'s synthetic
raw-tree + train + promote machinery, `--skip-ingest` after writing `plays.parquet`):

- **Filenames written:** `decisions.html`, `opponent-AUT.html`, `own-team.html`,
  **`player-analysis.html`**, `wp-review-2026-01-01_GER-vs-AUT_EM.html`,
  `wp-review-2026-01-02_GER-vs-AUT_EM.html`, `wp-review-2026-01-03_GER-vs-AUT_EM.html`
- **Landed in:** `reports/2026-09-04/` and `reports/latest/` (both under this worktree's real,
  gitignored `reports/` root)
- **Measured stage durations:** `ingest: 0.0s` (skipped), `score: 0.06s`, `report: 0.64s`,
  **total: 0.70s** — trivially inside the 10-minute REQ-S1-16 budget on this synthetic corpus;
  the real corpus (with real charted volume) will be slower but the report stage itself scales
  with `plays_scored.parquet`'s row count, not with the number of products, so five products
  vs. four is not expected to be the budget's binding constraint.
- **`player-analysis.html`:** 212 lines, 2 embedded charts (`data:image/png;base64,` × 2 —
  the definition-comparison chart and the cliff-zone chart), no `<script>` tag, no `http(s)`
  reference, 78 KB (well under the project's 8 MB self-contained-page budget), only one section
  rendered (`Alle Camps (Korpus gesamt)` — `hc_games.csv`/`hc_splits.csv` were not seeded in
  this synthetic fixture, so the split layer correctly degraded to korpus-only per
  `build_player_analysis_data`'s documented behaviour).
- **Columns that showed a "nicht verfügbar" state on this run** (exactly three of his 19
  columns, all traced to missing hand-charted extras absent from this synthetic corpus, not to
  a bug): **`Adj Comp %`, `adj Pass Yards`, `adj YPA`** (the `drop` column carries no real
  signal in this fixture) and **`Efficiency`** (the `efficiency` column is absent). This is the
  exact list M3-04-07's handout should quote, modulo whatever the real corpus's `drop`/
  `efficiency` extras look like once M3-02 lands them.
- `git status --porcelain reports` was empty both before and after this run (the output stayed
  gitignored throughout).

**No other issues.** `uv run pytest tests/test_reports_player_analysis_page.py
tests/test_cli_report.py tests/test_reports_render.py -q` — 52 passed. The wider
`uv run pytest -q` (excluding `test_cv_active_learning.py`, `test_cv_detect_infer.py`,
`test_cv_export.py`, `test_cv_overlay.py`, `test_cv_prelabel.py`, `test_cv_radar.py`,
`test_cv_teams.py`, `test_cv_track.py`, `test_m2_baseline_measurement.py`,
`test_m2_gta_adapter.py`, `test_cv_cvat.py`, `test_cv_detect_train.py` — all pre-existing
environment gaps: missing `torch`/`supervision`/`rfdetr` packages and a CVAT network
dependency, entirely in `cv/`/`scripts/hackathon/`-adjacent modules this plan's own objective
explicitly excludes from scope) exited **code 0**. `grep -v '^#'
src/flag_football_ep/templates/player_analysis.html.j2 | grep -c '|safe'` returns `0`.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None. Every rendered cell is either a real value or a named `keine Daten`/`nicht verfügbar
(siehe Hinweise)`/`–` state with a matching notice or footnote explaining why — no hardcoded
empty value flows to the page without a corresponding availability flag.

## Next Phase Readiness

- `player-analysis.html` is the fifth, fully wired `ffep report` product — ready for M3-04-07's
  handout to link/embed directly, and to quote the exact "nicht verfügbar" column list recorded
  above.
- `PlayerAnalysisReportData.corpus_comparison_table`/`corpus_cliff_table` are new, additive,
  stable public fields (documented in this module's own docstrings) — any later plan reading
  `build_player_analysis_data`'s output can rely on them being present (schema-correct, even if
  empty) rather than optional/missing.
- The real-corpus render (`uv run ffep report --product player-analysis --skip-ingest` on the
  main tree, after `dvc pull` materialises `plays.parquet`/`plays_scored.parquet` and both
  `champion` aliases are set) should be re-run by the coordinator to confirm the page also
  builds cleanly against real charted volume and to capture the real notice/unavailable-column
  text for M3-04-07's handout — the synthetic run above exercises every code path but not real
  corpus scale or real HC-workbook rows.
- No blockers. Frage 7 (Camp IV/VI naming) and Frage 8/9 (Air Yards subtraction term,
  drop-flag looseness) remain open for the head coach, unchanged from M3-04-02/03/04/RESEARCH.

---
*Phase: M3-04-player-analysis-report*
*Completed: 2026-09-04*

## Self-Check: PASSED

- `src/flag_football_ep/reports/player_analysis.py` — FOUND, `build_player_analysis_page`
  present (`grep -c "def build_player_analysis_page"` = 1)
- `src/flag_football_ep/templates/player_analysis.html.j2` — FOUND, 101 lines
  (`min_lines: 90` satisfied), `{% extends "base.html.j2" %}` present,
  `grep -v '^#' ... | grep -c '|safe'` = 0
- `src/flag_football_ep/reports/build.py` — FOUND, `grep -c "player-analysis"` = 2
  (`PRODUCTS` tuple entry, dispatch branch condition)
- `docs/coaching-reports.md` — FOUND, `grep -c "player-analysis"` = 4
- `tests/test_reports_player_analysis_page.py` — FOUND, 19 tests, all pass
- Commits `2addd27` (Task 1) and `133ba8c` (Task 2) — both present in `git log --oneline`
- `uv run pytest tests/test_reports_player_analysis_page.py tests/test_cli_report.py tests/test_reports_render.py -q` — 52 passed
- `uv run pytest -q` (with the pre-existing, out-of-scope CV/hackathon dependency gaps
  excluded, see Issues Encountered) — exit code 0
- Real (synthetic-corpus) `run_report_pipeline` run — `player-analysis.html` written to
  `reports/<date>/` and `reports/latest/`, 2 embedded charts, self-contained, `git status
  --porcelain reports` empty
