# Coaching reports (REQ-S1-12 .. REQ-S1-16)

`ffep report` turns the canonical dataset into the full coaching-report set: opponent
tendencies, own-team efficiency, decision cheatsheets and per-game win-probability review,
each as a standalone offline HTML file. There is no PDF dependency by design -- the PDF path
is the browser's own print dialog, applied to the HTML files.

## The four products

| Product | Filename | Requirement |
|---|---|---|
| Opponent tendency report | `opponent-<CODE>.html` (one per group opponent, e.g. `opponent-FRA.html`) | REQ-S1-12 |
| Own-team efficiency report | `own-team.html` | REQ-S1-13 |
| Decision cheatsheet (PAT break-even, 4th-down conversion) | `decisions.html` | REQ-S1-14 |
| Win-probability review, one per game | `wp-review-<game>.html` | REQ-S1-15 |

Every file is a self-contained HTML document: charts are embedded as base64 PNG data URIs, no
`<script>` tag, no external `http(s)` reference. Open it directly in a browser, on the HC's
tablet or anywhere else -- no server, no network access, no PDF renderer. To get a PDF, use the
browser's print dialog on the HTML file; `@media print` rules keep charts from splitting across
pages and preserve shading via `print-color-adjust: exact`.

## Commands

```bash
ffep report                        # everything: every group opponent, all four products
ffep report --opponent FRA         # just one opponent's tendency report (repeatable)
ffep report --product own-team     # just one product family (repeatable): opponents,
                                    # own-team, decisions, wp-review
ffep report --skip-ingest          # re-render from the existing plays.parquet, skip re-ingest
```

`--opponent` and `--product` are both repeatable and can be combined (e.g. two opponent codes
at once). An unknown `--product` value is rejected before anything runs, naming the valid
choices. An `--opponent` value outside `data/reference/group_opponents.csv` is rejected the
same way, before any report path is constructed from it.

`ffep report` echoes one `report: <filename>` line per file written, the run and `latest`
directory paths, any `notice:` lines (see below), and a per-stage duration block (`ingest`,
`score`, `report`, `total`) -- this is what REQ-S1-16's ten-minute budget is measured against.

A page-build failure for one opponent, or one game's WP-review page, is caught individually,
recorded as a German `notice:` line naming what failed and why, and does not abort the rest of
the run -- a run that produces four of five products the evening before a tournament is worth
far more than a run that produces none.

## The two-step model discipline

`ffep report` never trains. It scores with whichever model version is currently aliased
`champion` in the MLflow registry -- it never fits a fresh model and never picks up the
newest run automatically. Promoting a model to `champion` is a separate, deliberate,
human-invoked step:

```bash
ffep train --model ep     # fit a candidate, log it to MLflow
ffep train --model wp
ffep promote --model ep   # after reviewing the run: move the champion alias to it
ffep promote --model wp
```

This split exists so a report run can never silently change which model produced its numbers.
If no `champion` alias has ever been set for a model, `ffep report` fails fast with a
`RegistryError` naming the model and telling you to run `ffep promote` after reviewing a
training run -- run `ffep promote --model ep` (and `--model wp`) once, then re-run `ffep
report`.

## The two maintained reference files

Two hand-maintained CSVs feed every report and need attention once per tournament (or
whenever a new player/team spelling shows up in the data):

**`data/reference/group_opponents.csv`** -- `canonical_team,team_name`. One row per opponent in
the current tournament group; `canonical_team` is the code used everywhere else (filenames,
`--opponent`), `team_name` is the display name. Edited once per tournament: add the group's
teams before the first report run, remove last tournament's teams if they've fallen out of the
group. Duplicate `canonical_team` values are rejected on load.

**`data/reference/player_mapping.csv`** -- `source,source_player,canonical_player`. Maps every
spelling of a player's name, as it appears in each raw source, to one canonical display name
used in the own-team report's per-player tables. Extended whenever the own-team report's
unmapped-name warning lists a new spelling.

Maintenance loop for the player mapping: run `ffep report`, open `own-team.html`, read the
"Nicht zugeordnete Spielernamen" warning block at the top (it lists every unmapped name found
in this run), add one row per unmapped name to `player_mapping.csv`, re-run. The warning never
blocks the report -- an unmapped name still appears verbatim in the tables, just outside any
canonical rollup -- so this loop can happen after camp, not before.

## Output layout

Each `ffep report` run writes into a dated folder, `reports/<YYYY-MM-DD>/`, and then replaces
`reports/latest/` with a full copy of that folder -- `reports/latest/` always holds the newest
complete set. The dated folder is written to completion first; `latest/` is only replaced once
every file has landed, so a half-written run is never presented as `latest/`. A second run the
same day replaces, not merges into, that day's folder.

`reports/` is git-ignored (see `.gitignore`'s `/reports/` entry) because the own-team report
contains player names -- treat the contents as containing PII and handle copies to shared
storage or the tablet accordingly.

## Documented discretion decisions

The values below are quoted from `src/flag_football_ep/reports/aggregate.py` and
`src/flag_football_ep/reports/render.py`; every rendered report's footnotes quote the same
values, so the doc and the code must never drift apart.

**Field zones** (over `yardline_50`, yards to the opponent goal line; inclusive bounds, display
order):

| Zone | Range |
|---|---|
| Red Zone | 0-9 |
| Gegnerhälfte | 10-22 |
| Mittelfeld | 23-36 |
| Eigene Hälfte | 37-50 |

The red zone (0-9) is the locked explicit bucket; the remaining 10-50 range is split into three
roughly-equal thirds.

**Score-state band**: bucketed from `score_differential` (posteam - defteam) into three states
-- `< -6` is `Rückstand`, `-6..6` inclusive is `Ausgeglichen`, `> 6` is `Führung`. ±6 is "within
one score" in flag football (a touchdown is 6 points plus a 1- or 2-point PAT); a margin of
exactly 6 stays `Ausgeglichen`, not yet `Führung`/`Rückstand`.

**Thin-sample muting threshold**: `MUTED_MIN_N = 5`. Any rate cell with fewer than 5 plays is
marked `muted` -- it is still shown (never hidden), rendered visibly greyed with a legend
explaining the greying, since 5 is the smallest count at which a Clopper-Pearson interval still
says something more useful than "could be anything" for this corpus's typically small per-cell
counts.

**Current-cycle definition**: `report.cycle_start_season` in `ffep.toml` (currently `2025`).
The own-team report's per-player and per-call efficiency tables split into "current cycle"
(season >= `cycle_start_season`) and full-history views using this boundary.

**WP annotation-selection rule** (`select_wp_annotations` in
`src/flag_football_ep/charts/wp_review.py`): candidate markers are the top 5 plays by `|wpa|`
(`WP_ANNOTATION_TOP_K = 5`), unioned with every scoring play (touchdown, 1- or 2-point
conversion) or turnover (interception, safety) play. When the union exceeds 8 markers
(`WP_ANNOTATION_MAX = 8`), scoring/turnover rows are kept ahead of swing-only rows -- a big
pure swing can never bump a scoring play off the chart.

**Embedded-chart DPI**: `EMBED_DPI = 150` (`src/flag_football_ep/reports/render.py`) -- every
chart is rendered to a base64 PNG data URI at this resolution before embedding.

## The synthetic-clock caveat

Until REQ-S1-02's real clock data arrives, `half_seconds_remaining` is synthetic
(interpolated per play from `1200 / max(play_id)`, not read off game tape). Every WP chart
carries an explicit, unmissable disclosure -- both in the page text and on the chart itself (the
x-axis label reads "Play # (synthetische Spielzeit)" and the chart title and a footnote note the
same) -- and WP values must not be used for clock-management conclusions (e.g. "should they have
gone for it with 40 seconds left") until real clock data lands.
