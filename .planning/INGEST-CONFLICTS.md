## Conflict Detection Report

### BLOCKERS (0)

(none)

### WARNINGS (0)

(none)

### INFO (6)

[INFO] Mutual cross-reference between the two strand plans
  Note: docs/plan-1-analytics-refresh.md and docs/plan-2-cv-tracking.md reference each other ("Strang 1 von 2" / "Strang 2 von 2"). Cycle detection flagged the mutual edge, but these are informational "see also" links between explicitly complementary sibling plans, not derivation dependencies — per-doc extraction is independent and synthesis is well-defined. Not treated as a blocker.

[INFO] Scope boundary between strands verified consistent
  Note: docs/plan-1-analytics-refresh.md declares "Keine CV-Abhängigkeit" (Strand 1 works fully without Plan 2); docs/plan-2-cv-tracking.md assigns opponent analysis to manual charting + PBP (Plan 1) and keeps CV on third-party footage as stretch-only (Phase 2.5). No overlap or contradiction between the strands' scopes.

[INFO] License policy consistent across SPEC and DOC
  Note: docs/plan-2-cv-tracking.md (SPEC, higher precedence) and docs/research-notes.md (DOC) agree: avoid Ultralytics YOLO (AGPL-3.0 incl. fine-tuned weights), prefer RF-DETR/D-FINE/RT-DETR (Apache 2.0). No resolution needed; recorded as locked decision D-02.

[INFO] Drone altitude figures differ between SPEC protocol and DOC research note
  Note: docs/plan-2-cv-tracking.md prescribes ~30–60 m hover for the capture protocol; docs/research-notes.md reports NFL/college training practice at ~15–30 m. The DOC figure is descriptive of others' practice, not prescriptive; the SPEC (higher precedence) governs the protocol. No conflict, no action.

[INFO] Inference-time figures consistent
  Note: docs/plan-2-cv-tracking.md sets the go/no-go gate at < 1 h inference per game and estimates ~15–25 min/game on a T4 (matching docs/research-notes.md finding 5) — estimate sits within the gate.

[INFO] Classification ambiguity handled per classifier notes (both plans medium confidence)
  Note: docs/plan-1-analytics-refresh.md resolved to PRD but contains SPEC-like contract details (RESULT vocabulary, validation ranges) — extracted into constraints.md (C-07, C-08). docs/plan-2-cv-tracking.md resolved to SPEC but contains embedded architectural decisions without ADR structure — extracted into decisions.md (D-01..D-06) and marked locked per ingest directive.
