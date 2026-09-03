# Phase M3-3: Explosiveness & Efficiency - Context

**Gathered:** 2026-09-03
**Status:** Ready for planning (research may start immediately — independent of M3-1/M3-2 outputs)
**Source:** Express path from `docs/hc-notes-2026-09-03.md`

<domain>
## Phase Boundary

A researched, justified definition of "Explosiveness" (and a documented, reproduced "Efficiency") replaces the head coach's current rule ">12 yards and/or positive EPA (his calculation) = explosive" (HC-04). The user's stated concern: hard thresholds are always a problem ("was ist, wenn eine Spielerin nur 11 Yards erzielt?"). Deliverables: (1) deep research on how explosive plays and efficiency/success are defined elsewhere (NFL conventions such as 20+ yd pass / 10+ yd rush, PFF, nflfastR success rate = EPA > 0, EPA-based and distribution-based definitions), (2) a proposal that is threshold-free or context-calibrated (e.g. percentile of yards-gained distribution per down & distance, EPA-based success + "big play" as EPA above a data-derived quantile, smooth scoring instead of a cliff at 12), (3) implementation as canonical-plays metrics in `flag_football_ep` (module + tests), (4) computed on our data and set against the HC's numbers from his `Player Analysis` tabs (Exp Plays, Explosive %, Efficiency) so he sees the difference per player/QB.

Not this phase: the report product (M3-4 renders the metrics), EP/WP retraining (M3-2), any CV file.
</domain>

<decisions>
## Implementation Decisions

- **EXP-D01 Research before definition:** no metric is coded before `docs/explosiveness-recherche.md` (German) exists with sources; the HC's definition is reproduced FIRST as the baseline row so every alternative is compared against his number on the same plays.
- **EXP-D02 Flag-football specifics matter:** 5v5, 50-yard field (`yardline_50` ∈ [0,50]), 4-down series to midfield/goal, no rushing across the LOS by the QB (rules), pass-heavy — NFL yard thresholds are not transferable 1:1; the proposal must be calibrated on OUR distribution (2023–2026 corpus incl. HC rows once M3-2 unlocks them; until then our Hudl/IFAF plays).
- **EXP-D03 Prefer smooth/relative over cliff thresholds:** e.g. explosiveness score = probability mass above a down-&-distance-conditional quantile, or EPA-per-play z-score; keep ONE simple headline number for coaches ("Explosive %" stays as a name if the definition is defensible) plus the continuous version.
- **EXP-D04 Efficiency:** document exactly how the HC computes `Efficiency` in his workbook (formula cells, data_only=False) and reproduce it; then relate it to success rate (EPA > 0) so the two vocabularies are reconciled, not replaced.
- **EXP-D05 Honest small-sample handling:** per-player rates carry n; shrinkage/minimum-attempt rules proposed (ties into the Timo Riske questions, BL-05).
- PII: player names only via roster mapping; docs show ids/initials or aggregated tables.

### Claude's Discretion
- Exact metric formulas, quantile levels, module layout (`features/` vs `reports/aggregate.py`), plotting.
</decisions>

<canonical_refs>
## Canonical References
- `docs/hc-notes-2026-09-03.md` — HC's rule and the user's threshold concern
- `data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx` (gitignored) — `Player Analysis All Camps` columns Exp Plays / Explosive % / Efficiency; `Data` tab `Efficiency` column
- `src/flag_football_ep/features/mutations.py`, `reports/aggregate.py`, `reports/own_team.py` — where EPA-based aggregates live today
- `docs/research-notes.md` — where research findings are recorded project-wide
- `.planning/todos/pending/2026-09-03-fragen-an-timo-riske-pff.md`
</canonical_refs>

<specifics>
## Specific Ideas
- User: "So krasse Thresholds sind in meinen Augen immer ein Problem" — the deliverable must make the 11-vs-12-yard cliff visibly disappear.
</specifics>

<deferred>
## Deferred Ideas
- Win-driver analysis (BL-04) — uses these metrics later.
</deferred>

---
*Phase: M3-03-Explosiveness & Efficiency*
*Context gathered: 2026-09-03 via express path*
