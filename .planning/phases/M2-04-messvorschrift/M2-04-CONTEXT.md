# Phase M2-4: Messvorschrift - Context

**Gathered:** 2026-09-02
**Status:** Ready for planning
**Source:** PRD Express Path (.planning/imported/challenge-haertung/ — user's draft, reconciled via ABGLEICH.md). Started ahead of M2-3 by orchestrator decision: METR-01..04 need only the M2-2 per-clip outputs (retained on purpose), not the multi-game benchmark; running it now keeps the 2026-11-16 deadline safe while M2-3's labelling waits on the user.

<domain>
## Phase Boundary

Progress becomes visible even when it does not cross the threshold (METR-01..04): the shared scoring script `scripts/hackathon/score_tracks.py` gains a CONTINUOUS metric alongside the pass/fail threshold (identity switches per play, or a standard association-quality measure), emits both in ONE run split by dev/test, the challenge description states which metric is the acceptance criterion and which the direction, and a comparison of two M2-2 methods shows a difference the threshold metric swallows.

Not this phase: new labels (M2-3), the starter package (M2-5), changing the 90% target (BASE-04 decision pending with the user), any file 2.2 waves 7–12 modify.
</domain>

<decisions>
## Implementation Decisions (from the user's draft + reconciliation, locked)

- **Ground truth reality (from M2-2):** human continuity verdicts exist ONLY for BoT-SORT overlays (pilot 15/61); no per-frame identity labels exist yet (that is M2-3/DATA-03). The continuous metric must therefore be computable WITHOUT identity GT for the automatic view (e.g. fragment/switch counts per play from the tracks themselves) AND be defined so that, once identity labels exist, a standard measure (IDF1/HOTA-style association) plugs in with the same CLI surface. Plan both layers; implement the label-free layer now, the label-based layer as a ready interface with tests on synthetic data.
- **One run, both metrics, split by dev/test:** `score_tracks.py` reads the hackathon split (data/reference/hackathon_split.csv, plan 21) — dev = Panama Rojo 61 clips, private_test = Puerto Rico 61 clips — and reports threshold rate + continuous metric per split, n on every rate. Test-split scoring must work with the vaulted GT path (data/private/test-labels/…) without ever copying labels into public locations.
- **METR-03 wording lands in both challenge docs** (`docs/hackathon-challenge-reid.md`, `-formular.md`): acceptance criterion vs direction — the plan proposes the wording; the final BASE-04/METR-03 decision remains the user's checkpoint (formulated as a human-verify step at the end).
- **METR-04 comparison uses the M2-2 rows** (BoT-SORT vs ByteTrack-matched vs CBIoU-matched vs GTA) from data/reference/baseline-methods/per_clip.csv — no re-running trackers.
- **File-collision guard:** do not touch cv/*, docs/hackathon-bundles.md, .dvc/*, data/labels/**; `score_tracks.py` and scripts/hackathon/* are M2-owned; `docs/baseline-messung.md` may gain a section.
- German prose in docs; user global rules for commits.

### Claude's Discretion
- Exact continuous metric definition for the label-free layer (e.g. mean identity fragments per play normalized by expected players, or switches/play from track re-assignments) — must be reproducible from tracks + detections alone and documented with its blind spots (it cannot see silent merges — GTA caveat).
- Output format (JSON + Markdown table), CLI flags, test fixtures.
</decisions>

<canonical_refs>
## Canonical References
- `.planning/imported/challenge-haertung/REQUIREMENTS.md` (METR-01..04) + `ABGLEICH.md`
- `.planning/phases/M2-02-ehrliche-baseline/M2-02-0[1-3]-SUMMARY.md`, `docs/baseline-messung.md` (measured rows, the saturated-auto-metric finding, GTA caveat)
- `scripts/hackathon/score_tracks.py`, `scripts/hackathon/baseline_common.py`
- `data/reference/hackathon_split.csv`, `data/reference/baseline-methods/{summary,per_clip}.csv`, `data/reference/continuity_review.csv`
- `src/flag_football_ep/cv/continuity.py` (fragment logic reused by the scorer)
- `.planning/phases/02.2-dataset-buildout/02.2-21-PLAN.md` (test-set/vault conventions)
</canonical_refs>

<specifics>
## Specific Ideas
- The user's draft: "Die Schwellenmetrik verbirgt echte Verbesserungen und belohnt Zufallstreffer" — the continuous metric exists so teams see progress inside a failed play.
- Keep the scorer runnable by a team on a laptop in seconds; no torch dependency in the scoring path.
</specifics>

<deferred>
## Deferred Ideas
- Identity-label-based association metric with real GT — activates in M2-3 once DATA-03 labels exist.
- Transfer-domain scoring (TRANS-01).
</deferred>

---
*Phase: M2-04-Messvorschrift*
*Context gathered: 2026-09-02 via PRD Express Path*
