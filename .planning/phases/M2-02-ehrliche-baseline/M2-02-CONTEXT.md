# Phase M2-2: Ehrliche Baseline - Context

**Gathered:** 2026-09-01
**Status:** Ready for planning
**Source:** PRD Express Path (.planning/imported/challenge-haertung/ — user's draft, reconciled via ABGLEICH.md)

<domain>
## Phase Boundary

The starting position of the ReID challenge is MEASURED, not estimated (BASE-01..04): BoT-SORT (current baseline), ByteTrack, Deep-EIoU, and Global Tracklet Association each run once on the 61-clip benchmark, scored with the shared scoring machinery, results land in the challenge description, each method has a documented start command. If a ready-made method clearly beats the real baseline (15/61 = 24.6% full set / 10/43 = 23.3% dev pool), the challenge's 90% target is adjusted with reasons.

This is CHALLENGE-GIVER work (measuring existing methods), not solving the ReID task — the boundary the main project drew ("the hackathon is the ReID attempt") stays intact.

Not this phase: new labels (M2-3), the continuous metric (M2-4 — but do not preclude it: keep per-clip outputs), touching files that 2.2 waves 7–11 modify (cv/detect.py, cv/dataset.py train paths, dvc files).
</domain>

<decisions>
## Implementation Decisions (from the user's draft + reconciliation, locked)

- **Reference value is the REAL baseline:** 15/61 = 24.6% (BASE-04's comparison point), not 77%. The existing BoT-SORT run IS the first of the four measurements — re-scored with the same harness for comparability, not re-invented.
- **License gate per candidate (D-02):** before installing ANY tracker package, its license must be verified Apache/MIT/BSD-class. Known landscape from the challenge doc: `trackers` (Apache-2.0, already installed, has ByteTrack implementations in recent versions — verify), `gta-link`/Global Tracklet Association research code (license to verify), Deep-EIoU research repo (license to verify). AGPL candidates are measured ONLY if runnable without installing AGPL code into the project (e.g. skipped with a documented reason) — D-02 is not waived for measurements that would leave AGPL in the dependency tree.
- **Same detections, same scoring:** all four methods consume the SAME frozen detections (data/labels/.../bundle-inputs/detections.parquet, frozen run 87a8a522…) and are scored by `scripts/hackathon/score_tracks.py` against `data/reference/continuity_review.csv` — no method gets different inputs.
- **Comparability discipline:** per-domain/per-clip outputs retained; full-61 denominator; n on every rate; documented, repeatable start command per method (BASE-03).
- **Results destination:** measured table into `docs/hackathon-challenge-reid.md` (Baseline section) AND `docs/hackathon-bundles.md`/new doc as appropriate — coordinate with 2.2's pending edits; keep the diff small and commit promptly.
- **Target adjustment (BASE-04):** if a method clearly beats the baseline, the plan surfaces the recommendation as a checkpoint for the USER (the 90% target is a submitted-challenge parameter — changing it is the user's call toward BWI).
- **Package legitimacy:** any NEW package install follows the plan-04 precedent — [ASSUMED]-provenance packages get a blocking human gate; well-known registry-verified packages with clean metadata may proceed with the research documented.

### Claude's Discretion
- Which ByteTrack/Deep-EIoU/GTA implementations to use (prefer already-installed `trackers`; vendoring small permissively-licensed research code is acceptable with license file retained); exact CLI/module layout (new `scripts/hackathon/` or `cv/` extension — avoid files 2.2 waves 7–11 modify); run orchestration and output table format.
</decisions>

<canonical_refs>
## Canonical References

- `.planning/imported/challenge-haertung/REQUIREMENTS.md` (BASE-01..04) + `ABGLEICH.md` (MUST read)
- `scripts/hackathon/score_tracks.py` — the shared scoring harness (extend usage, don't fork)
- `data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet` — frozen detections input (gitignored, exists on the working tree)
- `data/reference/continuity_review.csv` + `data/reference/hackathon_freeze.json` — benchmark + frozen pin
- `docs/hackathon-bundles.md`, `docs/hackathon-challenge-reid.md` §Ziel/Baseline — result destinations
- `src/flag_football_ep/cv/track.py` — current BoT-SORT integration via `trackers`
- `.planning/PROJECT.md` D-02; `pyproject.toml`
</canonical_refs>

<specifics>
## Specific Ideas
- The user's draft: "Die 77% sind der Wert des ersten ausprobierten Werkzeugs" — the spirit is humility about the baseline; the measurement exists so no team beats a strawman.
- Deadline anchor 2026-11-16; this phase is early on purpose so the challenge description ships with honest numbers.
</specifics>

<deferred>
## Deferred Ideas
- Continuous metric (M2-4) — keep per-clip fragment/switch counts in outputs so M2-4 can consume them without re-running trackers.
- Transfer-domain measurements (TRANS-01, v2) — transfer detections already exist.
</deferred>

---
*Phase: M2-02-Ehrliche Baseline*
*Context gathered: 2026-09-01 via PRD Express Path*
