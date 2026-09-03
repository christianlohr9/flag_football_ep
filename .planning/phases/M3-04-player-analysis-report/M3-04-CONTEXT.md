# Phase M3-4: Player-Analysis-Report für den Oktober-Sync - Context

**Gathered:** 2026-09-03
**Status:** Ready for planning (research may start now; execution after M3-2 wave 2 and the M3-3 review)
**Source:** Express path from `docs/hc-notes-2026-09-03.md` (HC-05)

<domain>
## Phase Boundary

The head coach's hand-maintained `Player Analysis All Camps` tab becomes an automated `ffep report` product (German HTML, self-contained, same conventions as the existing opponent/own-team reports), computed from canonical plays: per-QB and per-WR tables — Comps, Incs, Attempts, TDs, Comp %, Adj Comp %, INTs, Sacks, Pass Yards, Air Yards, YPA, adj Pass Yards, adj YPA, Exp Plays, Explosive %, Efficiency, Carries, Rush Yards, Rush TDs — plus the M3-3 columns (Success Rate, calibrated Explosiveness, continuous score, n/CI/muted/shrinkage) side by side with his originals, filterable by camp/competition/opponent like his per-tab splits. Second deliverable: the **October handout** — one German document bundling the EPA update (M3-2's `docs/epa-refinement-2026-10.md`), the explosiveness proposal (M3-3), and this report, with the open HC questions in one place. "Was gewinnt ein Spiel?" only as an exploratory appendix if time permits (BL-04 stays backlog).

Not this phase: new metrics (M3-3), corpus fixes/training (M3-2), the web app (BL-02), any CV file.
</domain>

<decisions>
## Implementation Decisions

- **REP-D01 Reproduce first, then extend:** every HC column is reproduced with HIS definition (documented from the workbook formulas in M3-3's research; ambiguities — Attempts+Drops denominator, `Efficiency` charting rule — carried as both readings until his answers) so his numbers match ours on the same plays before any new column appears. Differences are shown, not hidden.
- **REP-D02 Same product conventions as Phase 1.4:** `reports/` module + `reports/render.py` HTML, headless-Agg charts embedded as data URIs, `ffep report` verb/flag, German prose, n on every rate, PII via roster mapping (player display names are allowed in the HC's own report — he sees his players — but nothing PII-bearing is committed; fixtures synthetic).
- **REP-D03 Splits mirror his tabs:** All Camps / per camp / per competition / per opponent, driven by `game_id`/competition tier/date from `hc_games.csv` + our games table — no hand-maintained lists.
- **REP-D04 Handout = one document:** `docs/hc-sync-2026-10.md` (German) linking/embedding the three deliverables; the user reviews it (checkpoint) before the sync.
- **REP-D05 Runtime budget:** the whole report set stays inside the < 10 min `ffep report` budget (REQ-S1-16 spirit).

### Claude's Discretion
- Table layout, chart choices, module structure under `reports/`, CLI flag naming, how the HC-original vs. ours comparison is rendered.
</decisions>

<canonical_refs>
## Canonical References
- `docs/hc-notes-2026-09-03.md` (HC-05 intent, "Automatisierung seiner Excel")
- `.planning/phases/M3-03-explosiveness-efficiency/M3-03-RESEARCH.md` + `docs/explosiveness-recherche.md`, `docs/explosiveness-vorschlag.md` (column formulas, definitions, comparison tables)
- `src/flag_football_ep/features/explosiveness.py` (public API), `reports/aggregate.py`, `reports/own_team.py`, `reports/render.py`, `reports/build.py`, `charts/*`, `cli.py` (report verb)
- `docs/coaching-reports.md` (report conventions from 1.4)
- `data/reference/hc_games.csv`, `roster.csv`, `player_mapping.csv`, `competition_tier.csv`
- `.planning/phases/M3-02-epa-refinement/M3-02-CONTEXT.md` (the EPA doc this handout embeds)
</canonical_refs>

<specifics>
## Specific Ideas
- HC: "Er hat super viel Arbeit in seine Excel gesteckt" — the report must feel like his tab, only automated and honest about n; that is the trust bridge toward the later web app.
</specifics>

<deferred>
## Deferred Ideas
- Coach web app (BL-02); win-driver analysis (BL-04); automated stat collection via CV (BL-03).
</deferred>

---
*Phase: M3-04-Player-Analysis-Report*
*Context gathered: 2026-09-03 via express path*
