# Phase M3-4: Player-Analysis-Report für den Oktober-Sync - Research

**Researched:** 2026-09-03
**Domain:** Coaching-report engineering (Jinja2/matplotlib HTML report product) reproducing a
hand-maintained Excel workbook's per-QB analysis tab, on top of an existing, well-established
`reports/` package (Phase 1.4) and a finished metrics module (Phase M3-3)
**Confidence:** HIGH for reuse/architecture (every building block already exists, tested, and
is read directly in this session); MEDIUM-LOW for the split structure (REP-D03) — the row-level
evidence is HIGH confidence (read directly from the workbook's formula cells), but the
canonical-data path to reproduce it does not exist yet, which is this research's central finding

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **REP-D01 Reproduce first, then extend:** every HC column is reproduced with HIS definition
  (documented from the workbook formulas in M3-3's research; ambiguities — Attempts+Drops
  denominator, `Efficiency` charting rule — carried as both readings until his answers) so his
  numbers match ours on the same plays before any new column appears. Differences are shown, not
  hidden.
- **REP-D02 Same product conventions as Phase 1.4:** `reports/` module + `reports/render.py`
  HTML, headless-Agg charts embedded as data URIs, `ffep report` verb/flag, German prose, n on
  every rate, PII via roster mapping (player display names are allowed in the HC's own report —
  he sees his players — but nothing PII-bearing is committed; fixtures synthetic).
- **REP-D03 Splits mirror his tabs:** All Camps / per camp / per competition / per opponent,
  driven by `game_id`/competition tier/date from `hc_games.csv` + our games table — no
  hand-maintained lists.
- **REP-D04 Handout = one document:** `docs/hc-sync-2026-10.md` (German) linking/embedding the
  three deliverables; the user reviews it (checkpoint) before the sync.
- **REP-D05 Runtime budget:** the whole report set stays inside the < 10 min `ffep report`
  budget (REQ-S1-16 spirit).

### Claude's Discretion
- Table layout, chart choices, module structure under `reports/`, CLI flag naming, how the
  HC-original vs. ours comparison is rendered.

### Deferred Ideas (OUT OF SCOPE)
- Coach web app (BL-02); win-driver analysis (BL-04); automated stat collection via CV (BL-03).

### Phase Boundary (from CONTEXT.md, for reference)
Not this phase: new metrics (M3-3, already done), corpus fixes/training (M3-2), the web app
(BL-02), any CV file. "Was gewinnt ein Spiel?" (BL-04) only as an exploratory appendix if time
permits.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| HC-05 | Automatisierter Report äquivalent zum Tab "Player Analysis All Camps" (QB/WR: Comps, Attempts, Comp %, Adj Comp %, YPA, Air Yards, Exp Plays, Explosive %, Efficiency, Carries/Rush) als `ffep report`-Produkt, deutsch, aus kanonischen Plays; Handout für den Oktober-Sync mit EPA-Update und Explosiveness-Vorschlag. | Column-by-column formula citations (below) pin every HC column to a canonical source or a named gap. `reports/own_team.py`/`reports/build.py`/`reports/render.py` reuse map shows the exact functions a new `reports/player_analysis.py` plugs into. `docs/explosiveness-vorschlag.md`'s "Was das im Report bedeutet" section is the M3-3→M3-4 handoff contract for the explosiveness/efficiency columns. Environment Availability documents that 0 HC rows exist in `plays_scored.parquet` today, so the report must degrade gracefully until M3-02 wave 2 lands them. |
</phase_requirements>

## Summary

This phase has an unusually favourable starting position: Phase 1.4 already built the entire
report-product machinery this phase needs (`reports/aggregate.py`'s `rate_table`/`MUTED_MIN_N`/
Clopper-Pearson convention, `reports/render.py`'s Jinja2 + `fig_to_data_uri` + `write_report_run`
pipeline, `reports/build.py`'s per-product dispatch with per-product failure isolation, and the
`ffep report` CLI verb with its `--product`/`--opponent` flags), and Phase M3-3 already built and
froze the exact metrics this report needs to show side-by-side with the HC's own numbers
(`features/explosiveness.py`'s full public surface, `charts/explosiveness.py`'s two renderers,
and a German proposal document, `docs/explosiveness-vorschlag.md`, that explicitly names its own
handoff to this phase). No new package, no new statistical method, and no new report-rendering
mechanism is needed — this phase is data-plumbing and template work on an already-proven
skeleton.

The harder, genuinely open part is **REP-D03's split structure**. Reading the actual workbook
(`data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx`, present locally, read
via `openpyxl(data_only=False)` this session) shows that "Player Analysis Mexico", "... March
Camp", "... vs Switzerland", "... Camp V", "... Camp VI" are **not filtered by any column value**
— every one of their formulas is a hard-coded absolute row-range window into the single `Data`
sheet (e.g. `Data!$P$1001:$P$2000` for Mexico). The HC built these splits by pasting each
charting session's rows at the bottom of one long sheet, in chronological order, and then
hand-drew a new tab with a fixed row window over the new block. There is no `camp` or
`opponent` column in `Data` at all, and the row-range boundaries are the *only* place this
information exists. Because `hc_games.csv` (refilled by the not-yet-executed M3-02-04 plan)
declares one flat `competition = "HC Camps 2026"` for every game in this sheet and stamps a
constant `away_team = "OPP"` for all of them (real opponent identity is not resolvable from the
sheet at all), CONTEXT's REP-D03 assumption — that `hc_games.csv` + competition tier + date can
drive these splits with "no hand-maintained lists" — **does not hold as currently scoped**. A
small, deterministic camp-boundary lookup (documented below, values verified directly from the
formula cells) is required in addition to `hc_games.csv`, either as an extension to the M3-02-04
refill or as a new small reference file this phase owns.

A second concrete gap: the workbook's `Adj Comp %` formula depends on the `Data!W` ("Drop")
column, but **no `DROP` extras mapping exists anywhere in the ingest layer**
(`ingest/hudl.py::_CHARTING_RENAME`, `ingest/hc_workbook.py::_HC_ONLY_RENAME`) — a `drop` column
will not exist in `plays_scored.parquet` even after M3-02 wave 2 lands the HC's other extras
(`air_yards`, `bf_action`, `hand`, `efficiency`). `Adj Comp %` cannot be reproduced from
canonical data until this is fixed upstream (out of this phase's normal scope per the file
ownership boundaries other M3-2 plans already establish); `features/explosiveness.py`'s
`hc_efficiency_table` already anticipated exactly this by taking `drops_flag` as an *optional*,
caller-supplied expression rather than a hard dependency — the same discretion should extend to
`Adj Comp %`.

Finally, a data-availability reality the plan must design around: `plays_scored.parquet` today
has **21,437 rows, 0 of them from `hc_workbook`** (`legacy` 3,701, `legacy-sportapp` 14,545,
`ifaf` 3,191) and none of the HC extras columns (`air_yards`, `bf_action`, `hand`, `efficiency`)
exist in the frame yet. The report's per-QB columns that depend on these extras (`Efficiency`,
and the run-column-derived `Explosive %`'s pass-only Attempts denominator once HC rows exist)
must render a named "keine HC-Daten" state today and switch on automatically once M3-02 wave 2
(plan 04) lands HC rows — this is exactly the graceful-degradation discipline `reports/build.py`
already applies to every other product (per-product try/except with a German notice), so no new
mechanism is needed, only the same discipline applied here.

**Primary recommendation:** Build `reports/player_analysis.py` as a new module following
`reports/own_team.py`'s exact shape (`attach_epa` reuse, `_epa_rollup_by`/`rate_table`/
`section_basis` reuse, one `ReportSection`-returning function per table), register it as a
fifth entry in `reports.build.PRODUCTS`, and reuse `features/explosiveness.py`'s
`DEFINITIONS`/`definition_comparison`/`hc_workbook_explosive_rate`/`hc_efficiency_table`
verbatim for the side-by-side columns. Build the camp-split lookup as one small, well-documented
constant table (row-range boundaries verified below) rather than attempting to derive it from
`hc_games.csv`'s current schema, and flag this scope gap to the user/planner explicitly since it
contradicts REP-D03's literal wording ("no hand-maintained lists").

## Architectural Responsibility Map

This project has no browser/API/CDN tiers — it is a local Python batch pipeline (ingest ->
canonical plays -> features/reports -> rendered HTML). Tiers below are this project's actual
layers, matching M3-3's research.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| HC column reproduction (Comps/Attempts/Comp%/YPA/etc., per-QB rollup) | Report Data Layer (new `reports/player_analysis.py`) | Feature/Metrics Layer (`features/explosiveness.py`, already built) | Pure aggregation over canonical `plays_scored.parquet`, mirrors `reports/own_team.py::player_efficiency`'s existing per-QB/per-receiver rollup pattern — not a new architectural layer |
| M3-3 metric columns (Success Rate, calibrated Explosiveness, continuous score, shrinkage) | Feature/Metrics Layer (`features/explosiveness.py`, DONE in M3-3) | Report Data Layer (this phase only calls `definition_comparison`) | Already implemented, tested and frozen against the real corpus in M3-3; this phase is a consumer, not an implementer |
| Camp/competition/opponent split resolution | Reference/Data Layer (`data/reference/hc_games.csv` + a NEW small camp-boundary lookup) | Report Data Layer (filters plays by the resolved split before calling the rollup functions) | The split key does not exist as a canonical column anywhere (Summary) — it must be resolved once, upstream of the rollup, not re-derived per table |
| Rendered coach-facing output (tables, charts, HC-vs-ours comparison) | Report Rendering Layer (`reports/render.py`, `templates/*.html.j2`, DONE in Phase 1.4) | — | `render_page`/`fig_to_data_uri`/`write_report_run` are generic and already handle every other report product; this phase adds one new template, no new rendering mechanism |
| October handout (German Markdown linking the three deliverables) | Docs Layer (`docs/hc-sync-2026-10.md`, plain Markdown, no new tooling) | — | Same convention as `docs/explosiveness-vorschlag.md`/`docs/epa-refinement-2026-10.md`: hand-written German Markdown with a `Stand:` status line, no image files, no build step |
| Canonical `plays_scored.parquet` + HC extras columns | Data Layer (M3-1/M3-2 output, upstream of this phase) | — | This phase consumes it; the still-open `drop` extras-mapping gap (Summary) is an ingest-layer fix this phase should flag, not silently work around with a guess |

## Standard Stack

No new external packages. Every technique below reuses the project's existing, already-verified
dependencies — the same conclusion M3-3's research reached one phase earlier for the same
`.venv`.

### Core (already project dependencies — no install needed)
| Library | Version (verified via `./.venv/bin/python3 -c "import X; print(X.__version__)"`) | Purpose | Why Standard |
|---------|---------|---------|--------------|
| polars | 1.5.0 `[VERIFIED: installed in project .venv]` | Per-QB/per-split aggregation, joins to `hc_games.csv`/`competition_tier.csv` | Already the project's sole dataframe library; `reports/own_team.py`/`reports/aggregate.py` are polars end-to-end |
| scipy | 1.14.1 `[VERIFIED: installed in project .venv]` | Reused transitively via `reports.aggregate.rate_table`'s Clopper-Pearson calls — this phase never calls `scipy` directly | Same convention every other report/metric module in this repo follows |
| jinja2 | 3.1.6 `[VERIFIED: installed in project .venv]` | New `player_analysis.html.j2` template, rendered via the existing cached `build_environment()` | `reports/render.py` already owns this; a new template is the only jinja2-facing work in this phase |
| matplotlib | 3.9.2 `[VERIFIED: installed in project .venv]` | Embedding `charts/explosiveness.py`'s two existing renderers (`render_cliff_zone`, `render_definition_comparison`) via `fig_to_data_uri` | Both renderers are DONE (M3-3 plan 02); this phase only calls them, headless-Agg discipline already implemented there |
| openpyxl | 3.1.5 `[VERIFIED: installed in project .venv]` | NOT needed at plan/implementation time — every formula this research cites is pinned as documented constants below, mirroring M3-3's own "re-reading the workbook is not required" conclusion | Already used by `ingest/hc_workbook.py`; this research's own workbook read (this session) is the only place `data_only=False` was needed |

### Supporting
None beyond the Core table — this phase's entire technical surface is "call existing functions,
write one new module and one new template".

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| One `reports/player_analysis.py` module producing one HTML page with all splits as tabbed/filterable sections | Multiple files, one per split (`player-analysis-<camp-slug>.html`), mirroring `reports/opponent.py`'s one-file-per-team pattern | A single page keeps the "one document per player table family" feel of the HC's own workbook (one workbook, many tabs) and avoids inventing N new filenames per camp; multiple files would need a `--camp`/`--competition` CLI flag family mirroring `--opponent`, more surface for little benefit given the split count is small (≤7 named splits). Both are viable — this is Claude's Discretion per CONTEXT; the research recommends the single-page approach but does not lock it. |
| Deriving camp splits from `hc_games.csv` + `competition_tier.csv` + dates (REP-D03's literal wording) | A small new hard-coded row-range lookup table (this research's finding) | REP-D03's literal wording assumed the split key exists in the reference files; it does not (Summary). The row-range lookup is the only viable path without re-scoping M3-02-04, and it is fully deterministic and citable to formula cells — not a guess. |

**Installation:** none required — every library above is already pinned in `pyproject.toml` and
present in the project's own `.venv`.

## Package Legitimacy Audit

**Not applicable.** This phase introduces zero new external packages — every computation and
rendering step reuses `polars`/`scipy`/`jinja2`/`matplotlib`, all already installed project
dependencies verified present in `.venv` (see Standard Stack). The Package Legitimacy Gate
protocol is skipped per its own trigger condition ("whenever this phase installs external
packages").

## Architecture Patterns

### System Architecture Diagram

```
canonical plays_scored.parquet (epa, yards_gained, air_yards, hand, efficiency once M3-2 lands HC rows)
        |
        v
[NEW] reports/player_analysis.py
  - resolve_split(plays, split) -> filtered frame          (camp/competition/opponent lookup)
  - hc_columns_by_qb(plays)     -> Comps/Incs/Attempts/TDs/Comp%/Adj Comp%/INTs/Sacks/
                                    Pass Yards/Air Yards/YPA/adj Pass Yards/adj YPA/
                                    Carries/Rush Yards/Rush TDs   (reproduces the workbook literally)
  - m3_columns_by_qb(plays)     -> calls features.explosiveness.DEFINITIONS/definition_comparison,
                                    hc_workbook_explosive_rate, hc_verbal_explosive_rate,
                                    hc_efficiency_table    (ALREADY BUILT, M3-3 -- this phase only calls it)
  - build_player_analysis_data(plays, config, splits) -> PlayerAnalysisReportData
        |
        v
reports/render.py::render_page("player_analysis.html.j2", ...)   (existing, generic)
charts/explosiveness.py::render_cliff_zone / render_definition_comparison -> fig_to_data_uri  (existing)
        |
        v
reports/build.py::build_reports() adds "player-analysis" to PRODUCTS, dispatches like "own-team"
        |
        v
reports/render.py::write_report_run()  ->  reports/<date>/player-analysis.html, reports/latest/
```

Entry point: canonical `plays_scored.parquet` (already produced by M1/M3-1/M3-2, unchanged by
this phase) plus two reference files (`hc_games.csv`, and this phase's new camp-boundary
lookup). Processing: one new report-data module computing the HC's literal columns plus the
M3-3 metrics, split by camp/competition/opponent. Decision point: table layout / single-page vs
multi-page is Claude's Discretion (CONTEXT). Output: one (or a small family of) standalone HTML
page(s) via the existing generic render/write machinery, plus a German Markdown handout.

### Recommended Project Structure
```
src/flag_football_ep/
├── reports/
│   ├── player_analysis.py     # NEW: this phase's only new Python module in src/
│   └── build.py                # MODIFIED: PRODUCTS += ("player-analysis",), one new dispatch branch
├── templates/
│   └── player_analysis.html.j2 # NEW: mirrors own_team_report.html.j2's structure/CSS (base.html.j2 extends)
data/reference/
│   └── hc_camps.csv            # NEW (or an extension to hc_games.csv's schema, see Open Questions):
│                                  game_id -> camp label, derived from the row-range boundaries below
docs/
│   └── hc-sync-2026-10.md      # NEW: the October handout (REP-D04), links the three deliverables
tests/
│   ├── test_reports_player_analysis.py   # NEW, mirrors tests/test_reports_own_team.py conventions
│   └── test_reports_build.py             # MODIFIED: add "player-analysis" to the existing PRODUCTS coverage
```

### Pattern 1: Reproduce every HC column literally, cited to its formula cell (REP-D01)
**What:** Every column in the "Player Analysis All Camps" tab, pinned verbatim from the formula
cells read this session (`openpyxl(data_only=False)`, `data/raw/hc_files/Offense Analytics 2026
Camps and Competitions.xlsx`, sheet `Player Analysis All Camps`, row 2 as the representative
formula row — every other player row is structurally identical, only `$A2` changes). All
formulas below reference `Data!$*$2:$*$19562` (the sheet's own absolute range; the sheet's
actual populated data ends at row 4165 as of this session, see Environment Availability).

| HC column | Header cell | Formula (verbatim) | Canonical mapping |
|---|---|---|---|
| QB (row key) | A1 | player name in column A | `thrown_by` coalesced onto `qb` (same fallback `reports/own_team.py::player_efficiency` and `features/explosiveness.py::_with_group_key` already use) |
| Comps | B1 | `=COUNTIFS(Data!$I$2:$I$19562,"Complete*", Data!$P$2:$P$19562,A2)` | `result_raw`/canonical `complete_pass==1` where `qb`==player, `play_type=="pass"` |
| Incs | C1 | `=COUNTIFS(Data!$I$2:$I$19562,"Incomplete", Data!$P$2:$P$19562,$A2)` — **exact** match `"Incomplete"`, not a wildcard, so it excludes `"Incomplete, Interception"` and any other multi-token incomplete variant | canonical `complete_pass==0 & interception==0` (needs verification against the exact `result_raw` token set — see Open Questions) |
| **Attempts** | D1 | `=B2+C2+H2` — **Comps + Incs + INTs. NOT `+Sacks`.** | **Correction to M3-3's `HC_PASS_ATTEMPT_FILTER` docstring** (`features/explosiveness.py`), which states the workbook's `Attempts = Comps+Incs+Sacks`. Read directly from the "Player Analysis All Camps" tab's own `D2` formula cell this session: Sacks (`I2`) is a SEPARATE column, never added into `D2`. `hc_workbook_explosive_rate`'s current denominator (canonical `play_type=="pass"`, which includes sack rows) therefore does **not** match the workbook's own `Attempts` value — this is a real, tool-verified discrepancy that must be corrected or explicitly re-scoped when this phase wires the column, not silently inherited (see Common Pitfalls #1) |
| TDs | E1 | `=COUNTIFS(Data!$I$2:$I$19562,"Complete, TD", Data!$P$2:$P$19562,$A2)` | `touchdown==1 & play_type=="pass"` for that QB |
| Comp % | F1 | `=iferror(B2/D2,0)` | `Comps/Attempts` (workbook's own Attempts, see above) |
| Adj Comp % | G1 | `=iferror((B2+COUNTIFS(Data!$I$2:$I$19562,"In*", Data!$P$2:$P$19562,$A2, Data!$W$2:$W$19562,"*"))/D2,0)` — Completions plus incompletions with a non-blank `Data!W` (**"Drop"** column) value | **BLOCKED**: no `DROP` extras mapping exists in `ingest/hudl.py::_CHARTING_RENAME` or `ingest/hc_workbook.py::_HC_ONLY_RENAME` — `drop` will not exist in `plays_scored.parquet` even after M3-2 lands HC rows. This column cannot be reproduced from canonical data today (see Common Pitfalls #2, Open Questions) |
| INTs | H1 | `=COUNTIFS(Data!$I$2:$I$19562,"*Interception", Data!$P$2:$P$19562,$A2)` | canonical `interception==1 & play_type=="pass"` |
| Sacks | I1 | `=COUNTIFS(Data!$I$2:$I$19562,"Sack", Data!$P$2:$P$19562,$A2)` | canonical `sack==1` |
| Pass Yards | J1 | `=SUMIFS(Data!$J$2:$J$19562,Data!$I$2:$I$19562,"Complete*", Data!$P$2:$P$19562,$A2)` | `yards_gained` summed where `complete_pass==1` |
| Air Yards | K1 | `=SUMIFS(Data!$M$2:$M$19562,Data!$I$2:$I$19562,"Complete*", Data!$P$2:$P$19562,$A2)-SUMIF(Data!$L$2:$L$19562,$A2,Data!$Y$2:$Y$19562)` | canonical `air_yards` summed on completions, **minus** a subtraction term keyed on `Data!L` (RECEIVED BY) == the QB name against `Data!Y` (header literally `"B"`, semantics unresolved — see Open Questions) |
| YPA | L1 | `=iferror(J2/D2,0)` | `Pass Yards / Attempts` |
| adj Pass Yards | N1 | `=SUMIFS(Data!$J$2:$J$19562,Data!$I$2:$I$19562,"Complete*", Data!$P$2:$P$19562,$A2)+SUMIFS(Data!$M$2:$M$19562,Data!$P$2:$P$19562,$A2, Data!$W$2:$W$19562,"*")` | Pass Yards plus air yards on rows where `Data!W` (Drop) is non-blank — **same blocked dependency as Adj Comp %** |
| adj YPA | O1 | `=iferror(N2/D2,0)` | `adj Pass Yards / Attempts` — inherits the same block |
| Exp Plays | R1 | `=COUNTIFS(Data!$P$2:$P$19562,$A2, Data!$J$2:$J$19562, ">12")` | **Already built** — `features/explosiveness.py::hc_workbook_explosive_rate`'s `exp_plays` column, verbatim (M3-3) |
| Explosive % | S1 | `=R2/D2` — **divides by the workbook's own `Attempts` (D2), i.e. Comps+Incs+INTs, not the canonical `play_type=="pass"` count `hc_workbook_explosive_rate` currently uses as `n`** | `features/explosiveness.py::hc_workbook_explosive_rate` computes `exp_plays/n` where `n` = canonical `play_type=="pass"` row count (includes sacks) — this is the SAME denominator mismatch flagged under Attempts above, propagated into this ratio. The `exp_plays` numerator is correct; only the denominator needs the correction |
| Efficiency | U1 | `=(SUMIF(Data!$P$2:$P$19562, $A2,Data!$O$2:$O$19562))/(D2+W2)` — **W2 here is this SHEET's own column W ("Carries"), not `Data!W` ("Drop"). Denominator = Attempts + Carries (pass + rush attempts combined).** | **Correction to M3-3's assumption**: `features/explosiveness.py::hc_efficiency_table`'s docstring states the denominator is `Attempts + Drops`; the "Player Analysis All Camps" tab's own `U2` formula instead divides by `Attempts + Carries` (this sheet's `D2+W2`, both same-sheet cells). `hc_efficiency_table`'s `drops_flag` parameter, as currently named, does not match what this specific tab computes — carried as an open question for the HC (was "Efficiency" meant to include rushing snaps in its denominator, or is this tab's formula itself inconsistent with a `Data!O`-plus-Drops intent stated elsewhere?), not silently resolved either way |
| Carries | W1 | `=COUNTIFS(Data!$I$2:$I$19682,"Rush*", Data!$P$2:$P$19682,$A2)` (note: this one cell's range is `$19682`, one row longer than every other formula's `$19562` — likely a stray autofill artifact in the HC's own sheet, not meaningful) | canonical `play_type=="run" & qb==player` (project's `qb`/`thrown_by` fallback applies the same way here per CONTEXT's rushing-QB convention already used elsewhere in the codebase) |
| Rush Yards | X1 | `=SUMIFS(Data!$J$2:$J$19562,Data!$I$2:$I$19562,"Rush*", Data!$P$2:$P$19562,$A2)` | `yards_gained` summed where `play_type=="run"` |
| Rush TDs | Y1 | `=COUNTIFS(Data!$I$2:$I$19562,"Rush, TD", Data!$P$2:$P$19562,$A2)` | `touchdown==1 & play_type=="run"` |

**No WR/receiver-side table exists on the "Player Analysis All Camps" tab itself** (verified this
session: columns Z onward and rows below the last QB row are empty; the tab is a QB-row-only
rollup with rushing columns appended). CONTEXT's "the WR-side tables if present" question is
answered: not present in this specific tab. `reports/own_team.py::player_efficiency` already
computes a separate per-receiver EPA/YAC table from the same corpus if the report wants to add
one — that is new-column territory (REP-D01 allows extension after reproduction), not part of
literal reproduction.

**When to use:** Every HC column implementation in this phase's plan should cite the exact
formula row above rather than re-deriving semantics from the column name — two denominator
corrections (Attempts, Efficiency) are tool-verified departures from what M3-3's research and
code comments assumed, and one column (Adj Comp %, adj Pass Yards, adj YPA) is currently
unreproducible from canonical data at all.

### Pattern 2: Reuse `reports/own_team.py::player_efficiency`'s canonicalisation/rollup shape
**What:** `_canonicalise_players` (via `map_players`/player_mapping.csv) then `_epa_rollup_by`-
style per-player aggregation, `ReportSection`/`SectionBasis` as the shared output container.
**When to use:** For every per-QB table this phase builds — do not invent a second
identity-canonicalisation or a second `ReportSection` shape.
**Example:**
```python
# Source: src/flag_football_ep/reports/own_team.py::player_efficiency (existing, read this session)
from flag_football_ep.reports.aggregate import ReportSection, section_basis
from flag_football_ep.reference import load_player_mapping, map_players

def hc_columns_by_qb(plays: pl.DataFrame, mapping: pl.DataFrame) -> ReportSection:
    canon, unmapped = _canonicalise_players(plays, mapping)  # reuse own_team.py's private helper
                                                                # or promote it to aggregate.py if
                                                                # this phase needs it in two modules
    ...
```

### Pattern 3: Consume `features/explosiveness.py`'s finished public API directly (M3-3 handoff)
**What:** `docs/explosiveness-vorschlag.md`'s own "Was das im Report bedeutet (Übergabe an
M3-4)" section names the exact functions this phase should call, verbatim:
`scrimmage_plays`, `hc_workbook_explosive_rate`, `hc_verbal_explosive_rate`,
`hc_efficiency_table`, `calibrate`/`ExplosivenessCalibration`/`write_calibration`/
`load_calibration`, `success_flag`, `explosive_epa_flag`, `explosive_score`,
`DEFINITIONS`/`definition_comparison`/`shrink_rate`, `cliff_zone_table`, plus
`charts.explosiveness.render_cliff_zone`/`render_definition_comparison`.
**When to use:** For every M3-3-column (Success Rate, calibrated Explosiveness, continuous
score, n/CI/muted/shrinkage) this phase renders — never recompute, never re-derive a threshold.
**Example:**
```python
# Source: docs/explosiveness-vorschlag.md + features/explosiveness.py (both DONE, M3-3)
from flag_football_ep.features.explosiveness import (
    DEFINITIONS, definition_comparison, load_calibration,
)
calibration = load_calibration("data/reference/explosiveness/calibration.json")
comparison = definition_comparison(plays, ["thrown_by"], calibration=calibration)
```

### Pattern 4: Camp-split resolution — a documented row-range lookup, not a `hc_games.csv` join
**What:** The HC's five/six named splits over the "Offense Analytics 2026 Camps and
Competitions" workbook's `Data` sheet are absolute row-number windows, verified directly from
each tab's own formula cells this session (`openpyxl(data_only=False)`):

| Split (HC tab name) | `Data` sheet row range | Notes |
|---|---|---|
| March Camp / **Camp I** | `2:1000` | "Player Analysis March Camp" and "Set Analysis Camp I" share this exact range — confirms `March Camp == Camp I` |
| Mexico | `1001:2000` | No matching "Set Analysis" tab for this range exists — Mexico appears to be a competition, not a numbered camp |
| **vs Switzerland / Camp III** | `2001:3000` | "Player Analysis vs Switzerland" and "Set Analysis Camp III" share this exact range — confirms `vs Switzerland == Camp III` |
| **Camp VI (Player Analysis) / Camp IV (Set Analysis)** | `3001:4000` | **Naming conflict, not resolved by this research**: the same row range is labelled "Camp VI" in one tab and "Camp IV" in another, within the same workbook. Flagged as an open question for the HC, not silently picked |
| Camp V | `4001:5000` | "Player Analysis Camp V" and "Set Analysis Camp V" agree |
| All Camps | `2:19562` | Covers every row above; the sheet's real populated data ends at row 4165 as of this session (rows 4166-19562 are pre-formatted empty headroom, harmless for `COUNTIFS`/`SUMIFS`) |
| 12 Hand Split C | `2001:19562` filtered additionally on `Data!N` ("Hand") `== "1"` | A cross-cutting filter, not a camp boundary — orthogonal to the camp/competition/opponent split axis REP-D03 asks for |

**When to use:** Any implementation of REP-D03's per-camp split for the "Offense Analytics 2026
Camps and Competitions" workbook's games. The row-range boundaries above must be joined against
the `first_row`/`last_row` values the ingest segmenter already computes per game
(`ingest/hc_workbook.py::HcGameSlice.first_row`/`.last_row`, and — per M3-02-04's own plan —
embedded as prose in `hc_games.csv`'s `note` column, e.g. `"rows 2145-2189"`) to assign each
canonical `game_id` a camp label. This is new code this phase must write (or request as a
schema extension to the not-yet-executed M3-02-04 plan) — it does not exist today in any form.

**Anti-pattern:** Assuming `hc_games.csv`'s `competition`/`season`/`game_date` columns, as
scoped by M3-02-04 (not yet executed), can drive this split. They cannot: every HC game in this
workbook gets the single flat `competition = "HC Camps 2026"` value, `season = 2026` for all,
and `game_date` stays empty for numeric-block games (M3-02-04's own interfaces section
confirms `source_team1`/`source_team2`/`game_date`/`corpus_game_id` "stay empty for numeric
blocks"). Per-opponent splits for HC games face the same problem one level deeper: the ingest
layer stamps a **constant** `away_team = "OPP"` for every HC game in this workbook
(`HOME_TEAM = "GER"`, `AWAY_TEAM = "OPP"`, both hard-coded `[ASSUMED]` constants in M3-02-04's
own plan) — the real opponent identity (e.g. "Switzerland") is recoverable ONLY from knowing
which camp/tab a game's rows fall under, i.e. the same row-range lookup above, not from any
`posteam`/`defteam` value.

### Anti-Patterns to Avoid
- **Trusting M3-3's `HC_PASS_ATTEMPT_FILTER` docstring comment verbatim for the Attempts
  denominator:** it states `Attempts = Comps+Incs+Sacks`; the workbook's own `D2` formula (read
  directly this session) is `Comps+Incs+INTs`, with Sacks entirely absent from the sum. Verify
  against the formula cited in Pattern 1 above, not against the prior phase's prose summary of
  it.
- **Assuming `hc_efficiency_table`'s `drops_flag` parameter matches this tab's `Efficiency`
  formula:** the "Player Analysis All Camps" tab's own `U2` divides by `Attempts + Carries`
  (same-sheet columns), not `Attempts + Drops`. `hc_efficiency_table` may still be the right
  function to call for a *different* Efficiency reading M3-3 found elsewhere, but this
  specific tab's formula is not that reading — treat as a separate, additional open question.
- **Silently guessing what `Data!Y` (header `"B"`) means for the Air Yards subtraction term:**
  three single-letter columns (`X`, `S`, `C`, `Q`, `Y`, `B` — six total, all one-character
  headers, columns R through Y in `Data`) exist in the workbook with no documented meaning
  anywhere in the codebase or prior research. Do not guess a position-code or role-flag
  interpretation; treat as opaque and flag for the HC (see Open Questions) — reproducing `Air
  Yards` without this term (i.e., dropping the subtraction) is an acceptable documented
  fallback IF flagged as a deviation, never silently.
- **Building a second `ReportSection`/rate-table/Clopper-Pearson convention for this report:**
  every other report in this codebase (`reports/own_team.py`, `reports/opponent.py`,
  `reports/decisions.py`, `reports/wp_review.py`) reuses `reports/aggregate.py`'s vocabulary;
  this phase's whole reason for being low-risk is that it can do the same.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Per-QB Comps/Incs/Attempts/TDs/etc. rollup | A fresh `group_by`/`agg` chain reproducing the workbook's `COUNTIFS`/`SUMIFS` logic from scratch, unverified against the actual formula cells | The formula-cited mappings in Pattern 1 above, each traced to its exact `Player Analysis All Camps!<cell>` source | Two denominators (Attempts, Efficiency) differ from what a naive reading of the column names or M3-3's own prose summary would suggest — only the direct formula-cell read (done this session) is trustworthy |
| Explosiveness/Efficiency/Success-Rate columns | Recomputing quantiles, EPA-magnitude flags, or shrinkage from scratch | `features/explosiveness.py`'s full public API (Pattern 3) — already implemented, tested, and calibrated against the real corpus in M3-3 | Duplicating this work would risk a second, drifted definition of "explosive" in the same codebase, exactly the fragmentation M3-3's own `Don't Hand-Roll` section warned against |
| Camp/competition/opponent split filtering | A hand-typed `if camp == "Mexico": df.filter(...)` per call site, or worse, a per-report guess at row ranges | One small, named constant table (Pattern 4) built once, with every boundary cited to its formula cell, consumed by a single `resolve_split(plays, split_name)` function | The row ranges are workbook-specific magic numbers; scattering them across call sites would make a future workbook restructuring (the HC pasting a new camp's rows) silently wrong everywhere at once |
| Rate confidence intervals, muting, small-sample handling | A third statistical convention for this specific report | `reports/aggregate.py::rate_table`/`MUTED_MIN_N` (Phase 1.4) and `features/explosiveness.py::shrink_rate` (M3-3) | Both already exist, are tested, and are what CONTEXT's "n on every rate" locked decision (REP-D02) literally asks for |
| Chart rendering for the explosiveness/efficiency comparison | A third headless-matplotlib renderer | `charts/explosiveness.py::render_cliff_zone`/`render_definition_comparison` (M3-3, DONE, built explicitly for this handoff) | The module's own docstring states its exact purpose: "these renderers exist so the HTML handout M3-4 builds can show the identical measured data as real matplotlib Figures" |
| HTML page assembly, base64 chart embedding, atomic run-folder writes | A new templating/writing mechanism | `reports/render.py::build_environment`/`render_page`/`fig_to_data_uri`/`write_report_run` (Phase 1.4) | Generic, already used by four other report products; a fifth is exactly what this machinery was built to scale to |

**Key insight:** Almost nothing in this phase needs new infrastructure. The genuine, non-trivial
work is (1) getting the column formulas exactly right (two of the eighteen HC columns have a
denominator that differs from what the prior phase's own comments assumed, verified only by
reading the workbook directly), and (2) building the one missing piece of infrastructure that
does NOT already exist anywhere: the camp/opponent split resolver, because the split key is not
a canonical column at all, only a manual pasting order in the source spreadsheet.

## Common Pitfalls

### Pitfall 1: Reusing `hc_workbook_explosive_rate`'s `n` as if it equalled the workbook's `Attempts`
**What goes wrong:** Rendering "Explosive %" using M3-3's `hc_workbook_explosive_rate` output
directly and labelling its `n` column "Attempts" — the function's `n` is the canonical
`play_type=="pass"` row count, which includes sack rows; the workbook's own `Attempts` (`D2`)
excludes sacks (`Comps+Incs+INTs` only, see Pattern 1).
**Why it happens:** `HC_PASS_ATTEMPT_FILTER`'s own docstring comment in `features/
explosiveness.py` states the two ARE equal ("Comps+Incs+Sacks") — a claim this research found
to be incorrect when read against the actual `Player Analysis All Camps!D2` formula cell.
**How to avoid:** When wiring the Explosive %/Attempts columns into this phase's report, either
(a) recompute a workbook-exact `Attempts` denominator (`Comps+Incs+INTs`, no sacks) alongside
`hc_workbook_explosive_rate`'s existing `n`, labelling both explicitly and showing the
discrepancy (matching REP-D01's "differences are shown, not hidden"), or (b) raise this as a
correction against M3-3's docstring before this phase locks its own numbers. Either way, do not
silently equate the two.
**Warning signs:** A rendered "Attempts" column that includes a QB's sack count when compared
by hand against the raw `Data` sheet's row-by-row values for that QB.

### Pitfall 2: Rendering "Adj Comp %"/"adj Pass Yards"/"adj YPA" with a guessed Drop flag
**What goes wrong:** Building a `drops_flag` expression from some other proxy column (e.g.
`incomplete_pass & something`) because no canonical `drop` column exists, and presenting the
result as if it reproduced the workbook.
**Why it happens:** `Data!W` ("Drop") is a real, named column in the HC's raw sheet, but no
extras-mapping (`_CHARTING_RENAME` in `ingest/hudl.py`, `_HC_ONLY_RENAME` in
`ingest/hc_workbook.py`) maps `"DROP"` to a canonical column — verified by grep this session:
zero matches for `DROP` in either mapping dict.
**How to avoid:** Render these three columns as an explicit "nicht verfügbar (Drop-Spalte noch
nicht kanonisch)" state, matching REP-D01's own carried-both-readings discipline for genuine
ambiguities, and flag the missing extras-mapping as a concrete, actionable finding for a future
small M3-1/M3-2-adjacent fix (this phase's file-ownership boundary should not silently patch
`ingest/hc_workbook.py` without checking whether that file is READ-ONLY under a concurrent
plan's collision guard, as M3-02-04's own guard already demonstrates other plans do).
**Warning signs:** An "Adj Comp %" column that is numerically identical to plain "Comp %" (a
zero-drops assumption silently applied) rather than an explicit not-available state.

### Pitfall 3: Assuming REP-D03's split can be built purely from `hc_games.csv` + dates
**What goes wrong:** Implementing per-camp/per-opponent filtering as a join against
`hc_games.csv`'s `competition`/`game_date`/`away_team` columns, discovering only at
verification time that every HC game in the Offense Analytics workbook carries the identical
flat `competition` value and a placeholder `away_team = "OPP"`.
**Why it happens:** REP-D03's wording ("driven by `game_id`/competition tier/date from
`hc_games.csv` + our games table — no hand-maintained lists") describes how splits work for
every OTHER report in this codebase (`reports/opponent.py`'s per-team pages, `reports/
own_team.py`'s cycle-vs-alltime split) — an entirely reasonable assumption that turns out not
to hold for this one workbook's row-pasted tab structure (Pattern 4).
**How to avoid:** Use the row-range lookup (Pattern 4) for this workbook's games specifically;
`hc_games.csv` + `competition_tier.csv` remain the right mechanism for every non-HC-workbook
split (e.g. splitting by IFAF tournament competition, which already carries real `competition`
values).
**Warning signs:** A "per camp" report section that shows identical numbers for every camp (the
filter silently matched everything because the join key never discriminated).

### Pitfall 4: Building the report before M3-02 wave 2 lands, then never re-verifying against real HC rows
**What goes wrong:** Writing and testing `reports/player_analysis.py` entirely against
synthetic fixtures (correct, required for unit tests) and shipping it without a final
verification pass once `plays_scored.parquet` actually contains `hc_workbook` rows.
**Why it happens:** CONTEXT's own phase boundary allows research to start before M3-2 wave 2
lands ("execution after M3-2 wave 2 and the M3-3 review") — but M3-02-04 (the plan that
actually lands HC rows) has no `SUMMARY.md` yet as of this research session, i.e. it has not
executed. The EPA doc this phase's handout embeds (`docs/epa-refinement-2026-10.md`) is written
even later, by M3-02-07 (wave 6) and reviewed by M3-02-08 — both currently unexecuted plans.
**How to avoid:** Design every HC-extras-dependent column (Efficiency, Air Yards, Hand-split)
to render a named "keine HC-Daten" empty state today (mirroring `reports/build.py`'s existing
per-product notice discipline), and add an explicit execution-order dependency in this phase's
plan: this phase's implementation work can proceed against fixtures now, but its FINAL
verification against real data — and the handout's embedding of `docs/epa-refinement-2026-
10.md` — cannot complete until M3-02-04 AND M3-02-07/08 have run.
**Warning signs:** A plan that treats "M3-2 wave 2" as a single checkpoint when the actual
handout dependency (the EPA doc) lands three waves later in M3-2's own plan sequence.

### Pitfall 5: PII leakage through the row-range camp lookup or the HC-vs-ours comparison tables
**What goes wrong:** Committing a reference file (the new camp-boundary lookup, or any
generated comparison CSV) that embeds a player name in a `note`/`label` column, mirroring the
exact discipline M3-3 already had to build (`comparison_by_player.csv`'s pseudonymisation,
`DEFAULT_MIN_ATTEMPTS = 15` thin-sample bucketing).
**Why it happens:** REP-D02 explicitly distinguishes the two data classes: player display names
ARE allowed in the rendered `reports/` HTML output (git-ignored, per `docs/coaching-reports.md`'s
existing PII discipline for `own-team.html`), but nothing PII-bearing may be committed. A
camp-boundary lookup keyed only by `game_id` (this research's Pattern 4 table) carries no player
data by construction — but any FUTURE reference file this phase adds must be checked against the
same rule before committing.
**How to avoid:** Reuse `tests/test_m3_hc_pii.py`'s `_CHECKED_ARTEFACTS` pattern (already
extended by M3-3 plan 02 for exactly this purpose) — add this phase's new committed reference
files to that same guard rather than inventing a new PII check.
**Warning signs:** A new `data/reference/*.csv` file this phase adds that is not covered by any
existing PII test.

## Code Examples

### Reusing `features/explosiveness.py`'s comparison table for the side-by-side columns
```python
# Source: docs/explosiveness-vorschlag.md ("Was das im Report bedeutet"), features/explosiveness.py
from flag_football_ep.features.explosiveness import (
    DEFINITIONS, definition_comparison, load_calibration, scrimmage_plays,
)
from flag_football_ep.charts.explosiveness import render_definition_comparison
from flag_football_ep.reports.render import fig_to_data_uri

calibration = load_calibration("data/reference/explosiveness/calibration.json")
scoped = scrimmage_plays(offense_plays, require_epa=True)
comparison = definition_comparison(scoped, ["thrown_by"], calibration=calibration)
chart_uri = fig_to_data_uri(render_definition_comparison(comparison))
```

### Reusing `reports/own_team.py`'s player-canonicalisation shape for the new HC-columns table
```python
# Source: src/flag_football_ep/reports/own_team.py::player_efficiency (existing, read this session)
# This phase's reports/player_analysis.py should follow the SAME shape: canonicalise once,
# group by the resolved QB identity, build a ReportSection, attach a SectionBasis.
from flag_football_ep.reports.aggregate import ReportSection, section_basis

def hc_columns_by_qb(plays: pl.DataFrame) -> ReportSection:
    pass_attempts = plays.filter(pl.col("play_type") == "pass")
    # Workbook-exact Attempts = Comps + Incs + INTs (Pattern 1's D2 formula) -- NOT play_type=="pass"
    ...
```

### Registering a fifth report product (mirrors `reports/build.py`'s existing dispatch)
```python
# Source: src/flag_football_ep/reports/build.py (existing, read this session)
PRODUCTS: tuple[str, ...] = ("opponents", "own-team", "decisions", "wp-review", "player-analysis")

if "player-analysis" in resolved_products:
    try:
        pa_data = build_player_analysis_data(plays, config=config, scored=scored)
        rendered[PLAYER_ANALYSIS_FILENAME] = build_player_analysis_page(pa_data)
    except Exception as exc:  # noqa: BLE001 - per-product isolation, matches every other product
        notices.append(f"Player-Analysis-Auswertung fehlgeschlagen: {exc}")
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| N/A — this is an internal-tooling phase, not a domain where external state-of-the-art shifts | N/A | N/A | N/A |

**Deprecated/outdated:** Not applicable — this phase's "state of the art" question was already
answered by M3-3's literature review (Connelly, PFF, Sam Hoppen — see M3-03-RESEARCH.md); this
phase consumes that conclusion rather than re-researching it.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `hc_workbook_explosive_rate`'s `HC_PASS_ATTEMPT_FILTER` docstring comment ("Attempts = Comps+Incs+Sacks") is incorrect; the workbook's actual `D2` formula is `Comps+Incs+INTs` | Pattern 1 (Attempts row), Pitfall 1 | LOW — this is a direct formula-cell read (`data_only=False`) from the actual workbook this session, not an inference; if wrong, it would mean the formula cell itself was misread, which the exact `iferror`/`COUNTIFS` transcription above makes unlikely |
| A2 | `Data!Y`'s header value `"B"` and the K2 (Air Yards) subtraction term it feeds cannot be semantically resolved from available data or code | Pattern 1 (Air Yards row), Anti-Patterns | MEDIUM — if a future source (e.g. asking the HC) reveals `"B"`'s meaning, the Air Yards reproduction could be made exact instead of approximate; until then, dropping the subtraction term is a documented, flagged deviation, not a silent one |
| A3 | The "Camp VI"/"Camp IV" naming conflict for `Data` rows 3001-4000 is a genuine inconsistency in the HC's own workbook, not a research misread | Pattern 4 | LOW — both tab names and both formula ranges were read directly and cross-checked (Player Analysis Camp VI's row range exactly matches Set Analysis Camp IV's row range); the conflict is in the SOURCE, not in this research's transcription |
| A4 | `hc_games.csv`'s current/planned (M3-02-04) schema cannot drive REP-D03's camp/opponent split without an additional lookup, because `competition`/`away_team`/`game_date` are flat/placeholder values for every Offense-Analytics-workbook game | Summary, Pattern 4, Pitfall 3 | MEDIUM-HIGH if wrong — this is the research's single most consequential claim; it is based on M3-02-04's own PLAN.md (not yet executed, could theoretically change before it runs) stating `HOME_TEAM="GER"`, `AWAY_TEAM="OPP"` as constants and `source_team1`/`source_team2`/`game_date`/`corpus_game_id` as empty for numeric blocks — if M3-02-04 is re-scoped before execution to add real per-game opponent/date resolution, this finding would need re-verification against the actual landed `hc_games.csv` rather than its plan |

**If this table is empty:** N/A — see rows above.

## Open Questions (RESOLVED/DEFERRED)

Q1–Q3 (Attempts-denominator intent, `Data!Y` meaning, Camp IV/VI naming): DEFERRED to the head coach as Frage 7–9 via plan M3-04-07; the plans carry documented fallbacks meanwhile. Q4 (hc_games.csv schema vs standalone file): RESOLVED — standalone `data/reference/hc_splits.csv` adopted in plan M3-04-02 (CONTEXT REP-D03 amended).

1. **Does the "Player Analysis All Camps" tab's `Attempts` denominator (Comps+Incs+INTs, no
   Sacks) reflect the HC's real intent, or should this phase's report use a Sacks-inclusive
   denominator to match his verbal description elsewhere (mirroring M3-3's already-open
   "verbal vs. workbook formula" discrepancy for Explosive %)?**
   - What we know: the formula cell is unambiguous (`D2 = B2+C2+H2`).
   - What's unclear: whether this is deliberate or an earlier spreadsheet gap, same category as
     M3-3's Open Question 1 (Explosive %'s missing EPA term).
   - Recommendation: reproduce the literal formula as the baseline (REP-D01), surface both a
     workbook-exact and a canonical-`play_type=="pass"` Attempts count side by side, add this
     as a new question in `docs/hc-rueckfragen-2026-09.md` (Frage 7) rather than resolving
     silently.

2. **What does `Data!Y` (header `"B"`) encode, and does it matter for a faithful Air Yards
   reproduction?**
   - What we know: it feeds a subtraction term in the Air Yards formula, keyed on rows where
     `RECEIVED BY` equals the QB's own name (i.e., the QB himself appears as a pass-catcher
     elsewhere in the sheet).
   - What's unclear: the column's real meaning; five other single-letter columns (`X`, `S`,
     `C`, `Q`, `Y`) exist in `Data` with no documented semantics anywhere in this codebase.
   - Recommendation: ask the HC directly (candidate Frage 8); until answered, reproduce Air
     Yards WITHOUT the subtraction term and flag the simplification visibly in the rendered
     table's footnote, per REP-D01's "differences are shown, not hidden".

3. **Is the row range 3001-4000 "Camp VI" or "Camp IV" — and are there dedicated Player
   Analysis tabs planned for the camps this research found only as "Set Analysis" tabs (I,
   III, IV) without a matching "Player Analysis" counterpart?**
   - What we know: the row range itself is unambiguous; only the two tabs' NAMES for it
     disagree.
   - What's unclear: which name is authoritative, and whether the HC intends a full 1:1
     Player-Analysis/Set-Analysis tab pairing eventually.
   - Recommendation: ask the HC directly (candidate Frage 9); render this split's tab as
     "Camp IV/VI (unklar benannt, Zeilen 3001-4000)" until answered, never silently pick one
     name.

4. **Should REP-D03's split mechanism be a schema extension to `hc_games.csv` (requiring
   coordination with the not-yet-executed M3-02-04 plan) or a wholly separate small reference
   file this phase owns outright?**
   - What we know: `hc_games.csv`'s current/planned schema has no slot for a camp label; adding
     one would need M3-02-04 (or a follow-up plan) to populate it per declared game, since only
     the ingest-layer segmenter has direct access to each game's `first_row`/`last_row`.
   - What's unclear: whether coordinating a schema change into M3-02-04 (still unexecuted, so
     technically still open to a scope amendment) is preferable to this phase deriving the
     mapping independently from the `note` column's embedded row-range text (fragile: `note` is
     free-text prose, not a structured field, per M3-02-04's own `build_rows` docstring) or by
     re-reading the raw workbook once at build time (adds an `openpyxl` dependency at report-
     build time, which no other report currently has).
   - Recommendation: flag this explicitly for the planner/discuss-phase as a cross-phase
     coordination decision, not something this research resolves unilaterally — the cleanest
     technical answer (extend `hc_games.csv` with a `camp` column, populated by M3-02-04) may
     not be achievable if M3-02-04 has already executed by the time this phase plans; in that
     case, a standalone `data/reference/hc_camps.csv` (game_id -> camp, built once by a small
     script using the row-range boundaries in Pattern 4, analogous to M3-02-04's own
     `hc_games_refill.py` pattern) is the fallback this phase should own itself.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| polars | All aggregation | Yes | 1.5.0 (project `.venv`) | -- |
| scipy | Rate CIs (via `reports.aggregate.rate_table`) | Yes | 1.14.1 (project `.venv`) | -- |
| jinja2 | New `player_analysis.html.j2` template | Yes | 3.1.6 (project `.venv`) | -- |
| matplotlib | Embedding M3-3's existing chart renderers | Yes | 3.9.2 (project `.venv`) | -- |
| `data/processed/plays_scored.parquet` | The report's data source | Yes, but **0 `hc_workbook` rows today** (21,437 total: `ifaf` 3,191, `legacy` 3,701, `legacy-sportapp` 14,545) | -- | The HC-extras-dependent columns (Efficiency, Air Yards, Hand-split) must render a named empty state today; will populate automatically once M3-02 plan 04 (unexecuted as of this session) lands HC rows, per Pitfall 4 |
| HC-extras columns (`air_yards`, `bf_action`, `hand`, `efficiency`) | Efficiency/Air-Yards/Hand-split columns | No — absent from `plays_scored.parquet` today (0 HC rows) | -- | Blocked on M3-02 wave 2 landing; no other fallback exists (these are HC-workbook-only fields, not derivable from Hudl/IFAF/legacy sources) |
| `drop` canonical column | Adj Comp %, adj Pass Yards, adj YPA | No — no extras mapping exists for `DROP` in either `ingest/hudl.py` or `ingest/hc_workbook.py`, verified by grep this session | -- | No fallback within this phase's scope; render as "nicht verfügbar" (Pitfall 2) and flag as a finding for a future ingest-layer fix |
| `data/reference/hc_games.csv` | Game identity resolution for HC rows | Yes, present, but only 9 rows today (all pre-existing duplicate declarations); the ~35-190 numeric-block games M3-02-04 would declare are not yet written | -- | Blocked on M3-02-04 execution; this phase's split-resolution work (Pattern 4) can be built and unit-tested against synthetic fixtures now, independent of this blocker |
| `data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx` | Source of every formula citation in this research | Yes, present locally (2.9 MB), gitignored PII | -- | Every formula this research needed is already pinned as documented constants above (Pattern 1, Pattern 4) — re-reading the workbook is not required to implement this phase, mirroring M3-3's own conclusion |
| `docs/epa-refinement-2026-10.md` | REP-D04's handout (embeds the EPA update) | **No — does not exist yet.** Written by M3-02-07 (wave 6), reviewed by M3-02-08 (checkpoint, `autonomous: false`), neither executed as of this session | -- | This phase's handout-writing task cannot complete until M3-02-07/08 land; the report-building work (this document's main subject) has no such dependency and can proceed independently |
| `docs/explosiveness-vorschlag.md` | REP-D04's handout (embeds the explosiveness proposal) | **Yes — exists, DONE** (M3-3 plan 02, committed) | -- | -- |

**Missing dependencies with no fallback:**
- `drop` canonical column (blocks Adj Comp %/adj Pass Yards/adj YPA reproduction entirely)
- `docs/epa-refinement-2026-10.md` (blocks the handout's EPA section specifically, not the
  report product)

**Missing dependencies with fallback:**
- HC-extras columns and `hc_games.csv`'s HC-game rows: both unblock automatically once M3-02
  plan 04 executes; this phase's implementation and unit tests do not need to wait, only its
  final real-data verification pass does (Pitfall 4).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (config: `pyproject.toml` `[tool.pytest.ini_options]`) |
| Config file | `pyproject.toml` (`testpaths = ["tests"]`) |
| Quick run command | `./.venv/bin/pytest tests/test_reports_player_analysis.py -q` (new file, plan creates it) |
| Full suite command | `./.venv/bin/pytest -q` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| HC-05 | Workbook-exact `Attempts` (Comps+Incs+INTs, no Sacks) matches a synthetic fixture built to mirror the formula cell in Pattern 1 | unit | `pytest tests/test_reports_player_analysis.py::test_hc_attempts_excludes_sacks -x` | ❌ Wave 0 |
| HC-05 | Every HC column (Comps/Incs/TDs/Comp%/INTs/Sacks/Pass Yards/YPA/Carries/Rush Yards/Rush TDs) reproduces its cited formula on a fixture frame | unit | `pytest tests/test_reports_player_analysis.py::test_hc_columns_by_qb -x` | ❌ Wave 0 |
| HC-05 | Adj Comp %/adj Pass Yards/adj YPA render an explicit "nicht verfügbar" state when `drop` is absent, never a silently-zero-drops number | unit | `pytest tests/test_reports_player_analysis.py::test_adj_columns_unavailable_without_drop -x` | ❌ Wave 0 |
| HC-05 | M3-3 columns (Success Rate, calibrated Explosiveness, continuous score) render via `features.explosiveness.definition_comparison` unchanged, never recomputed locally | unit | `pytest tests/test_reports_player_analysis.py::test_m3_columns_delegate_to_explosiveness_module -x` | ❌ Wave 0 |
| HC-05 | Camp-split resolver assigns the correct camp label for a synthetic game whose rows fall inside each documented row-range boundary (Pattern 4), including the March-Camp/Camp-I and vs-Switzerland/Camp-III equivalences | unit | `pytest tests/test_reports_player_analysis.py::test_resolve_split_camp_boundaries -x` | ❌ Wave 0 |
| HC-05 | `reports.build.PRODUCTS` includes `"player-analysis"`; a per-product build failure is caught and recorded as a German notice, matching every other product's isolation discipline | unit | `pytest tests/test_reports_build.py::test_player_analysis_product_failure_isolated -x` | ❌ Wave 0 (extends existing file) |
| HC-05 | The rendered page shows zero HC-extras-dependent data today (0 HC rows in the fixture) with a named empty-state notice, not a crash or a silently-empty table | unit | `pytest tests/test_reports_player_analysis.py::test_empty_state_zero_hc_rows -x` | ❌ Wave 0 |
| HC-05 | The German handout doc (`docs/hc-sync-2026-10.md`) links `docs/explosiveness-vorschlag.md` and (once available) `docs/epa-refinement-2026-10.md`, mirroring `tests/test_m3_explosiveness_docs.py`'s doc-vs-artifact agreement pattern | unit | `pytest tests/test_m3_player_analysis_docs.py::test_handout_links_deliverables -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `./.venv/bin/pytest tests/test_reports_player_analysis.py -q`
- **Per wave merge:** `./.venv/bin/pytest -q` (full suite — this module touches `reports.build`'s
  shared `PRODUCTS` dispatch and reuses `features.explosiveness`/`reports.aggregate`, both
  covered elsewhere in the suite)
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_reports_player_analysis.py` — new file, covers HC-05 per the map above; use
      `flag_football_ep.testing.canonical_plays`/`canonical_plays_with_scores` factories
      (`src/flag_football_ep/testing.py`), mirroring `tests/test_reports_own_team.py`'s existing
      conventions — do not invent a new fixture style.
- [ ] `tests/test_m3_player_analysis_docs.py` — new file, mirrors
      `tests/test_m3_explosiveness_docs.py`'s doc-vs-artifact agreement guard for the handout.
- [ ] `tests/test_reports_build.py` — extend existing file's `PRODUCTS` coverage; do not create
      a duplicate build-dispatch test file.
- [ ] No new `conftest.py` fixtures needed — `tests/conftest.py` is owned by phase 01.2 plan 01
      and later plans "must not edit this conftest"; use module-local fixtures or the
      `testing.py` factories instead.
- [ ] Framework install: none — pytest and the `testing.py` factories already exist and are used
      by 100+ existing test files in this repo.

## Security Domain

`security_enforcement` is absent from `.planning/config.json` — treated as enabled per the
protocol default. This phase is a local batch-analytics/report-rendering module with no
network/auth/session surface; most ASVS categories genuinely do not apply, matching M3-3's own
assessment one phase earlier.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | No auth surface in this phase (local CLI/library code) |
| V3 Session Management | No | N/A |
| V4 Access Control | No | N/A |
| V5 Input Validation | Yes | Raise a named exception (mirroring `MissingExplosivenessColumns`/`MissingFeatureColumns`) when a required column is absent rather than letting a silent null deflate a rate — same discipline `features/explosiveness.py`/`features/mutations.py` already enforce. Applies specifically to the `drop`-column-absent case (Pitfall 2): fail loud with a named "not yet available" state, not a silent zero |
| V6 Cryptography | No | N/A |
| Output encoding (XSS via charted free text) | Yes | Already handled by `reports/render.py::build_environment`'s explicit `autoescape=select_autoescape([...])` — player names, `thrown_by`/`received_by`/description text reach Jinja2 templates verbatim from hand-charted exports; this phase's new template must extend `base.html.j2` (never disable autoescape), matching every existing template |
| Path/filename injection | Yes | `reports/render.py::write_report_run`'s existing filename-key validation (rejects `/`, `\`, `..`, null byte) already covers a fifth product's filename the same way it covers the other four — no new validation needed as long as this phase's filename is a fixed constant, not built from user/opponent input like `opponent_filename` is |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Charted free-text (player names, descriptions) rendered unescaped into HTML | Tampering/Injection (stored XSS via HC's own manually-typed spreadsheet cells) | Jinja2 `autoescape` (already enabled project-wide, `reports/render.py`) — verify the new template does not use `\|safe` on any HC-sourced string |
| A committed reference file (new camp-boundary lookup, or any comparison CSV this phase writes) accidentally carrying a player name | Information Disclosure (PII) | Extend `tests/test_m3_hc_pii.py`'s `_CHECKED_ARTEFACTS` guard to this phase's new committed files (Pitfall 5) rather than trusting manual review alone |

## Sources

### Primary (HIGH confidence)
- `data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx` — read directly via
  `openpyxl.load_workbook(..., data_only=False)` this session: sheet list (25 sheets), `Data`
  header row, `Player Analysis All Camps` header row + row-2 formulas (all 18 columns cited in
  Pattern 1), `Player Analysis Mexico`/`March Camp`/`vs Switzerland`/`Camp V`/`Camp VI`'s row-2
  formulas (row-range boundaries, Pattern 4), `Set Analysis Camp I/III/IV/V`'s row-2 formulas
  (camp-naming cross-check) — this research's own tool-verified extraction, not a secondary
  account
- `src/flag_football_ep/features/explosiveness.py` — full 715-line module read directly this
  session (DONE, M3-3 plan 01)
- `src/flag_football_ep/charts/explosiveness.py` — full 211-line module read directly this
  session (DONE, M3-3 plan 02)
- `src/flag_football_ep/reports/{aggregate,own_team,render,build,opponent}.py` — read directly
  this session
- `src/flag_football_ep/ingest/hudl.py::_CHARTING_RENAME`, `ingest/hc_workbook.py::
  _HC_ONLY_RENAME` — grepped directly this session; confirms no `DROP` mapping exists
- `docs/explosiveness-vorschlag.md`, `docs/coaching-reports.md` — read directly this session
- `.planning/phases/M3-02-epa-refinement/M3-02-04-PLAN.md`, `M3-02-07-PLAN.md`,
  `M3-02-08-PLAN.md`, `M3-02-CONTEXT.md` — read directly this session; confirms `hc_games.csv`'s
  planned schema and the EPA doc's dependency chain, and that neither has executed yet
  (no matching `SUMMARY.md` files exist as of this session)
- `data/processed/plays_scored.parquet` — queried directly via polars this session (21,437 rows,
  0 `hc_workbook` rows, no extras columns present)
- `data/reference/{hc_games,competition_tier,roster,player_mapping,team_mapping,
  group_opponents}.csv` — read directly this session

### Secondary (MEDIUM confidence)
None — every claim in this research traces to a direct read of the workbook, the codebase, the
committed reference data, or the (unexecuted) upstream plan documents, all done in this session.

### Tertiary (LOW confidence)
None.

## Metadata

**Confidence breakdown:**
- Reuse map / architecture (Standard Stack, Don't Hand-Roll, Patterns 2-3): HIGH — every function
  cited was read directly this session and is already tested/committed
- HC column formulas (Pattern 1): HIGH for the formula transcriptions themselves (direct
  `data_only=False` read); MEDIUM for the canonical-mapping column (some tokens, e.g. the exact
  `result_raw` values behind "Incomplete" vs "*Interception", were not independently verified
  against `ingest/hudl.py`'s RESULT-token vocabulary in this session)
- Camp-split structure (Pattern 4): HIGH for the row-range boundaries themselves (direct formula
  read, cross-checked between two independent tabs per boundary); LOW-MEDIUM for whether
  `hc_games.csv`'s FINAL schema (post-M3-02-04 execution) will actually lack a camp column — this
  is a claim about a plan's current scope, not about landed data, and could shift if M3-02-04 is
  amended before it runs
- Data availability (Environment Availability): HIGH — queried directly against the real
  `plays_scored.parquet` this session

**Research date:** 2026-09-03
**Valid until:** 30 days for the reuse-map/architecture content (stable, Phase 1.4's own
machinery). The camp-split findings (Pattern 4) and the HC-column formula citations (Pattern 1)
are tied to the CURRENT state of `data/raw/hc_files/Offense Analytics 2026 Camps and
Competitions.xlsx` and `hc_games.csv` — if the HC edits/extends the workbook (adds a Camp VII tab,
resolves the Camp IV/VI naming conflict, etc.) or if M3-02-04 executes with a different scope
than its current plan describes, re-verify against the landed state before trusting these
sections as current. The data-availability numbers (0 HC rows) are expected to change as soon as
M3-02 plan 04 executes — re-query `plays_scored.parquet` at plan time, do not treat "0 HC rows"
as a frozen fact.
