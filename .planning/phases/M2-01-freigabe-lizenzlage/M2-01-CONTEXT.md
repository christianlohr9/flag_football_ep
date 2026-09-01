# Phase M2-1: Freigabe und Lizenzlage - Context

**Gathered:** 2026-09-01
**Status:** Ready for planning
**Source:** PRD Express Path (.planning/imported/challenge-haertung/ — user's draft, reconciled via ABGLEICH.md)

<domain>
## Phase Boundary

The legal basis for handing challenge material to hackathon teams stands in writing, and every delivered component has a named license (RECHT-01..04). Two strands: (1) the SIGNED federation release naming dev/test/transfer material individually, with a written deletion commitment — user-side signature work, the project prepares the document; (2) the license inventory of every delivered component — fully autonomous, and per the 2026-09-01 fact-check expected to confirm the chain is AGPL-free (rfdetr Apache-2.0, trackers Apache-2.0, supervision MIT; D-02 held throughout).

Not this phase: measuring trackers (M2-2), new labels (M2-3), any file that Phase 2.2's remaining waves (7–11) modify.
</domain>

<decisions>
## Implementation Decisions (from the user's draft, locked)

- **Riegel-Charakter:** Without the signed release the challenge is withdrawn (agreed with the hackathon organisers). The signature itself is the USER's action; the phase prepares a signable one-pager (German, listing material classes, deletion path/deadline, confirmer) and marks the task `autonomous: false`.
- **RECHT-02:** The date placeholder in the challenge description is replaced only when the real signature date exists — same commit as the release documentation.
- **RECHT-04 reframed per ABGLEICH:** license inventory as a table (component, version, license, role in the delivered bundles), landing in a new `docs/lizenz-inventur.md` (German). Expected finding: no AGPL anywhere in the own chain; the inventory PROVES it rather than assumes it. Any tracker candidates M2-2 adds must be appended by M2-2.
- **File-collision guard:** `docs/hackathon-challenge-reid.md` is also touched by 2.2 plans — M2-1 edits to it are limited to the Datenschutz/date placeholder block and must be committed promptly.

### Claude's Discretion
- Exact structure/wording of the signable release one-pager and the inventory table.
</decisions>

<canonical_refs>
## Canonical References

- `.planning/imported/challenge-haertung/PROJECT.md` + `REQUIREMENTS.md` (RECHT-01..04) + `ABGLEICH.md` (reconciliation — MUST read)
- `docs/capture-legal.md` — existing legal record incl. the 2026-08-31 approval addendum this phase upgrades
- `docs/hackathon-challenge-reid.md` §Datenschutz — placeholder location
- `pyproject.toml` — dependency ground truth for the inventory
- `.planning/PROJECT.md` D-02 (Apache-2.0-only policy)
</canonical_refs>

<specifics>
## Specific Ideas
- The user's draft treats the signature as the release valve for shipping material — the license inventory does NOT wait for it.
</specifics>

<deferred>
## Deferred Ideas
- OPS-01 (AGPL-free chain) — already true; inventory documents it. OPS-02, TRANS-* stay v2.
</deferred>

---
*Phase: M2-01-Freigabe und Lizenzlage*
*Context gathered: 2026-09-01 via PRD Express Path*
