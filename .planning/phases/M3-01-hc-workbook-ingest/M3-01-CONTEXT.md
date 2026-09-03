# Phase M3-1: HC-Workbook-Ingest - Context

**Gathered:** 2026-09-03
**Status:** Ready for planning
**Source:** Express path from `docs/hc-notes-2026-09-03.md` (head-coach meeting) + workbook inspection

<domain>
## Phase Boundary

The head coach's three hand-maintained Excel workbooks become a canonical plays source for Strand 1 (HC-01, HC-02): parse their Hudl-like `Data` tabs, map to the data contract, dedupe against our existing Hudl/IFAF plays, enrich with the WC 2026 games, validate per source. Delivery anchor: October 2026 HC sync (M3-2 EPA refinement consumes this corpus).

Files (gitignored, PII — player names): `data/raw/hc_files/`
- `Germany Analytics Stats EC 2025 vs WC Nations.xlsx` — `Data` tab columns: O, D, OFF FORM, BF Action, OFF PLAY, DN, DIST, YARD LN, RESULT, TARGET ROUTE, RECEIVED BY, GN/LS, Air Yards, Thrown By, Target, Drive Success, D Target, Blitz, Blitz Time, Tackle, Pass Breakup, QB, C, X/H, Y/CAT, Z, … (first rows show `#N/A` formula residue — data_only read; needs cleaning). ~28 analysis tabs (player stats per opponent, formation/concept/blitz analysis, target depth, routes, QB/WR connections, `EP`).
- `Offense Analytics 2026 Camps and Competitions.xlsx` — `Data` tab: PLAY #, ODK, OFF FORM, Off Str, OFF PLAY, DN, DIST, YARD LN, RESULT, GN/LS, TARGET ROUTE, RECEIVED BY, AIR YARDS, Hand, Efficiency, Thrown By, Target, X, S, C, Q, Y, Drop, B (position columns hold player first names). Tab `Player Analysis All Camps` = the report M3-4 must reproduce (QB table: Comps, Incs, Attempts, TDs, Comp %, Adj Comp %, INTs, Sacks, Pass Yards, Air Yards, YPA, adj Pass Yards, adj YPA, Exp Plays, Explosive %, Efficiency, Carries, Rush Yards, Rush TDs).
- `Scoring Probability by Situation 2023-2026.xlsx` — `Data` tab: PLAY #, ODK, OFF FORM, Off Str, OFF PLAY, DN, DIST, YARD LN, RESULT, Drive Success, TARGET ROUTE, RECEIVED BY, GN/LS, Thrown By, YAC, QB, C, X/H, Y/CAT, Z, Target, Drop, B; first data row shows team names in the first columns ("Germany", "Ireland") — the layout differs from the header (offset/merged columns) and must be inspected row-wise. Analysis tabs: SP/EP by D&D (raw, clustered, weighted), Reg, per-down OppH EPA, frequency tables — the HC's EPA method M3-2 compares against.

Not this phase: retraining (M3-2), metric definitions (M3-3), the report product (M3-4), any CV/hackathon file.
</domain>

<decisions>
## Implementation Decisions

- **HC-D01 Reuse, don't fork:** the HC `Data` tabs are Hudl-export-shaped; the ingest goes through `flag_football_ep.ingest.hudl`-style contract mapping (column aliases: `YARD LN`→`yardline_50` derivation, `AIR YARDS`/`Air Yards`, `BF Action`, `Hand`, `Efficiency` as optional extras), with a dedicated `ingest/hc_workbook.py` reader (openpyxl or polars-excel) that yields one frame per workbook+sheet and records `source = hc_workbook:<file>:<sheet>`.
- **HC-D02 PII discipline:** player names in the position/target columns are mapped through `data/reference/roster.csv` / `player_mapping.csv` (extend the maintained CSVs where names are new); raw names never appear in reports, docs, tests or commits. Fixtures use the `testing.py` frame factories with synthetic names. The workbooks stay gitignored (done 2026-09-03).
- **HC-D03 Dedupe preference (HC's own words):** "wir reichern eher unsere Daten um seine an … Duplikate bei ihm erkennen und nicht berücksichtigen; sonst mit der Doppelung leben". Detect duplicates by (game identity → `game_id` mapping via team names + date/competition; `PLAY #`/`play_id`) first, content fingerprint (DN, DIST, YARD LN, RESULT, GN/LS, RECEIVED BY, Thrown By) second; report overlap counts per game; exclude HC duplicates of our Hudl plays; keep HC-only games (EC 2025, camps, 2023–2024 history) as new games with validation.
- **HC-D04 Game identity:** HC data lacks our `game_id` filename convention; build a `data/reference/hc_games.csv` mapping (workbook, sheet, team pair, date/competition → `game_id`, competition tier) as a maintained CSV; unknown games get provisional ids and a loud warning (existing reference-CSV pattern).
- **HC-D05 Validation honesty:** run the existing six per-game checks; HC-typed camps/scrimmages may legitimately fail score reconstruction — quarantine partition + report, not silent drop.
- **HC-D06 Deliverable for M3-2:** one canonical Parquet (existing `plays.parquet` path/versioning) with a `source` column, plus a German `docs/hc-workbook-ingest.md` recording per-source counts, duplicates found, games added, and open mapping questions for the HC.

### Claude's Discretion
- Reader library choice (openpyxl vs polars/calamine), sheet-detection heuristics, handling of the "Scoring Probability" header offset, fingerprint tolerance.
- Whether the HC analysis tabs (`EP`, `SP by D&D`) are read now for M3-2's comparison (recommended: extract them read-only into `data/reference/hc_sp_tables/` as CSV snapshots for M3-2) or left to M3-2.
</decisions>

<canonical_refs>
## Canonical References
- `docs/hc-notes-2026-09-03.md` — HC priorities and the dedupe preference (verbatim source)
- `docs/data-contract.md` + `docs/data-contract.schema.json` — contract v1.1 the HC rows must satisfy
- `src/flag_football_ep/ingest/hudl.py`, `ingest/legacy.py`, `ingest/ifaf.py` — ingest patterns (RESULT grammar, derivations, quarantine)
- `src/flag_football_ep/validation/checks.py`, `validation/report.py` — the six per-game checks
- `data/reference/roster.csv`, `player_mapping.csv`, `team_mapping.csv`, `competition_tier.csv` — maintained CSVs to extend
- `docs/pipeline.md` — `ffep ingest` orchestration the new source plugs into
- `.planning/PROJECT.md` C-07 (RESULT vocabulary), D-07 (GroupKFold by game)
</canonical_refs>

<specifics>
## Specific Ideas
- HC: "Es könnte sein, dass in seinen Daten doppelte zu unseren sind, aber diesen Weg würde ich bevorzugen" — enrich ours with his, detect his duplicates.
- The `Player Analysis All Camps` column set is the exact target of M3-4 — capture the column semantics (Adj Comp %, adj YPA, Exp Plays, Efficiency) while reading, so M3-3/M3-4 can reproduce his numbers before proposing changes.
</specifics>

<deferred>
## Deferred Ideas
- Web app replacing the Excel (BL-02); automated stat collection from CV (BL-03); game clock OCR (BL-01).
</deferred>

---
*Phase: M3-01-HC-Workbook-Ingest*
*Context gathered: 2026-09-03 via express path*
