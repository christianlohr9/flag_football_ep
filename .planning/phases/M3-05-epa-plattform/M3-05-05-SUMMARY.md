---
phase: M3-05-epa-plattform
plan: 05
subsystem: infra
tags: [adr, mlflow, otc, kubernetes, feature-store, multi-tenant, platform-decision]

# Dependency graph
requires: []
provides:
  - "Drafted platform ADR (docs/adr/0001-modell-plattform.md) covering three options, feature-store non-decision, multi-tenant data model recommendation, MLflow deployment realities"
affects: [BL-02-coach-web-app, future-M3-05-platform-migration]

# Tech tracking
tech-stack:
  added: []
  patterns: []

key-files:
  created:
    - docs/adr/0001-modell-plattform.md
  modified: []

key-decisions:
  - "ADR drafted verbatim from RESEARCH Q6-Q10 (three options table, feature-store 0-of-5 framework, multi-tenant shared-model-with-covariate recommendation, MLflow CVE history), ending in an empty decision section pending user sign-off -- PAUSED at the checkpoint, not filled"

patterns-established: []

requirements-completed: []  # Not yet -- PROD-06 completes only once Task 3 (signed decision) lands. This is a PARTIAL summary at the checkpoint.

# Metrics
duration: partial (paused at checkpoint)
completed: 2026-09-08 (Task 1 only; Tasks 2-3 pending)
---

# Phase M3-05 Plan 05: Platform ADR (partial -- paused at checkpoint) Summary

**ADR drafted in German (docs/adr/0001-modell-plattform.md) covering single-machine vs. OTC-VM vs. Kubernetes, feature-store and multi-tenant data-model recommendations from RESEARCH Q6-Q10 -- paused at the mandatory human-verify checkpoint before the decision line is filled.**

## Performance

- **Started:** 2026-09-08
- **Tasks completed:** 1 of 3 (Task 2 is the checkpoint; execution stops here by design)
- **Files created:** 1

## Accomplishments

- Drafted `docs/adr/0001-modell-plattform.md`: Kontext, three platform options (Option i/ii/iii) with cost/operational-burden/migration-path/breaks-first-at-10x, verbatim from RESEARCH Q6's table, with every OTC cost figure carrying its `[ASSUMED, order-of-magnitude]` flag.
- Feature-store section: the Tacnode 5-signal framework, this project's 0-of-5 status, and the no-feature-store-now recommendation.
- Multi-tenant data-model section: existing `source`/`competition_tier`/`team_mapping.csv` scoping, the shared-model-with-tier-covariate recommendation, and the specific measured evidence (`womens-international` EP log-loss 0.881 vs. `mixed-other` 0.946).
- MLflow deployment-realities section: no default auth, 2026 CVE history for unauthenticated servers, default-credential rotation requirement.
- `## Empfehlung` section stating RESEARCH's staged recommendation verbatim (stay on Option i through this phase and BL-02's first cut; migrate to Option ii on a concrete trigger; defer Kubernetes and a feature store together).
- `## Entscheidung (vom Nutzer zu unterschreiben)` section left explicitly empty/placeholder-marked, per the plan's instruction not to fill it before the checkpoint resume signal.

## Task Commits

1. **Task 1: Draft the ADR from RESEARCH's Part B findings** - `fef707f` (docs)

Task 2 is `type="checkpoint:human-verify"` — execution stops here, no commit. Task 3 (write the signed decision back) is pending the checkpoint response and will be executed by a continuation agent.

## Files Created/Modified

- `docs/adr/0001-modell-plattform.md` — the platform ADR draft, German, covering three options, feature store, multi-tenant data model, MLflow deployment realities, and an empty decision section.

## Decisions Made

None yet by the user — that is exactly what the checkpoint below asks for. No Claude default was applied to the platform question, per CONTEXT's explicit requirement.

## Deviations from Plan

None - plan executed exactly as written through Task 1.

## Issues Encountered

None.

## CHECKPOINT REACHED

**Type:** human-verify
**Plan:** M3-05-05
**Progress:** 1/3 tasks complete

### Completed Tasks

| Task | Name | Commit | Files |
| ---- | ---- | ------ | ----- |
| 1 | Draft the ADR from RESEARCH's Part B findings | `fef707f` | `docs/adr/0001-modell-plattform.md` |

### Current Task

**Task 2:** Sign the platform decision
**Status:** awaiting decision
**Blocked by:** requires the user's explicit sign-off; not something an agent may default or simulate

### Checkpoint Details

**What was built:** A drafted ADR (`docs/adr/0001-modell-plattform.md`) covering:
(a) three platform options — single machine (current), containerised OTC VM
(docker-compose: MLflow server + Postgres + OBS), Kubernetes (OTC CCE) — each with
cost/operational-burden/migration-path/breaks-first-at-10x, cost figures explicitly
flagged `[ASSUMED, order-of-magnitude]` where RESEARCH could not get a fixed OTC price;
(b) a feature-store non-recommendation grounded in a 0-of-5 trigger-signal framework;
(c) a multi-tenant data-model recommendation (shared model, tier covariate) with the
specific measured evidence (`womens-international` 0.881 vs. `mixed-other` 0.946 EP
log-loss); (d) MLflow's unauthenticated-server CVE history, relevant only if a
server-based option is later executed; (e) a staged recommendation (stay single-machine
now, migrate on a concrete trigger, defer Kubernetes/feature store together).

**How to verify:**
1. Read `docs/adr/0001-modell-plattform.md` in full.
2. Decide: (a) which option to run on now — almost certainly Option (i), per the
   recommendation, but this is the user's call; (b) whether the feature-store/Kubernetes
   deferral is acceptable as stated; (c) whether the multi-tenant shared-model-with-
   covariate recommendation is the direction to build BL-02 on later; (d) whether the
   migration trigger's wording needs changing (what exactly should force a move to
   Option ii).
3. If any cost figure needs a real number before signing, run the OTC price calculator
   directly (every OTC cost in the ADR is flagged as order-of-magnitude, not a quote) —
   this plan does not do that automatically.

### Awaiting

Reply with the option chosen (i / ii / iii), any wording changes to the recommendation
or migration trigger, and confirmation of signing. This becomes the
`## Entscheidung (vom Nutzer zu unterschreiben)` section's content, dated to today
(Task 3, executed by a continuation agent after this response).

## Next Phase Readiness

Task 3 (write the signed decision back) and the final SUMMARY/STATE/ROADMAP updates are
pending the checkpoint response above. This SUMMARY is partial by design — the plan's
`type: execute`, `autonomous: false` frontmatter and the checkpoint protocol require
stopping here, not simulating a signature.

---
*Phase: M3-05-epa-plattform*
*Status: PAUSED at checkpoint (Task 2 of 3) — 2026-09-08*

## Checkpoint aufgelöst (2026-09-09)

Der Nutzer hat entschieden: Option (ii), gestaffelt (lokale Containerisierung sofort, OTC-VM beim Migrationsauslöser, Kubernetes/Feature Store zurückgestellt, Multi-Tenant-Scoping jetzt). Eingetragen in `docs/adr/0001-modell-plattform.md` § Entscheidung; Folgeplan `M3-05-09-PLAN.md` (lokale Compose-Umgebung) wird angelegt. Plan 05 damit abgeschlossen.
