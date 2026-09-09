---
phase: M3-05-epa-plattform
plan: 09
subsystem: infra
tags: [mlflow, docker-compose, postgres, minio, migration, backup-restore]

requires:
  - phase: M3-05-epa-plattform (M3-05-03)
    provides: "Every ffep train run's git_commit/corpus_fingerprint lineage params and
      calibration/per-tier/no-play metrics -- migrated for real into the containerised store
      by this plan's Task 2, so the container's registry already carries the full metric set
      the rest of this phase's gate (M3-05-06) and docs (M3-05-08) describe"
  - phase: M3-05-epa-plattform (M3-05-01)
    provides: "The 2026-09-08 with_hc champion promotion (ep_model v5 / wp_model v5) this
      plan's real migration run moved into the container store and proved resolves
      identically via the migrated_from_run_id tag"
provides:
  - "docker-compose.mlflow.yml: local MLflow tracking server (Postgres backend store + MinIO
    S3-compatible artifact store, --serve-artifacts proxied), loopback-only, real
    health-checked at http://127.0.0.1:5000"
  - "MLFLOW_TRACKING_URI opt-in override in mlflow_store.py -- unset changes nothing about
    any existing train.py/registry.py/cli.py call site; set, the entire ffep pipeline talks
    to the container instead of the local sqlite/file store"
  - "scripts/migrate_mlflow_store.py: real MlflowClient-only migration of the existing
    registry (runs/params/metrics/tags/artifacts/model versions/champion alias) into the
    container, read-only against source, proven against this worktree's real mlruns/mlflow.db"
  - "scripts/mlflow_backup.sh / mlflow_restore.sh: real pg_dump --data-only + MinIO-mirror
    backup/restore, proven end-to-end with a real down -v wipe and restore"
  - "docs/mlflow-container-platform.md: the operator runbook (start/stop, ffep-client
    switching, migration, backup/restore, security boundary, relation to ADR Anhang A.2 and
    CI), linked from docs/model-training.md and docs/mlflow-ui-howto.md"
affects: [M3-05-06-promotion-gate, M3-05-08-model-card-plan]

tech-stack:
  added: []
  patterns:
    - "MLFLOW_TRACKING_URI is the single client-side override point -- every registry.py/
      train.py call already routes through mlflow_store.configure(config) first, so the
      override propagates with zero other code change; unsetting it is the complete rollback"
    - "Server-side-only container dependencies (psycopg2-binary, boto3) live exclusively in
      docker/mlflow/Dockerfile, never in pyproject.toml/uv.lock -- verified: git diff
      pyproject.toml uv.lock is empty"
    - "Migration proves fidelity via a migrated_from_run_id tag + matching champion
      resolution, not literal run-id equality -- MLflow's backend stores always
      server-generate a fresh run id, verified against the installed SqlAlchemyStore this
      session; there is no public API to force one"
    - "Postgres backup/restore for an app-managed schema (MLflow owns its own schema via
      alembic migrations on every container start) must be --data-only + truncate-before-load,
      not a full schema+data pg_dump -- a full dump collides with the already-initialised
      target schema and cascades into foreign-key failures on every dependent table"

key-files:
  created:
    - docker-compose.mlflow.yml
    - docker/mlflow/Dockerfile
    - .env.mlflow.example
    - scripts/migrate_mlflow_store.py
    - scripts/mlflow_backup.sh
    - scripts/mlflow_restore.sh
    - docs/mlflow-container-platform.md
    - tests/test_mlflow_store.py
    - tests/test_migrate_mlflow_store.py
  modified:
    - .gitignore
    - src/flag_football_ep/model/mlflow_store.py
    - docs/model-training.md
    - docs/mlflow-ui-howto.md

key-decisions:
  - "Migration proves fidelity via a migrated_from_run_id tag on every target run plus
    matching champion resolution through that tag, not literal run-id equality as the plan's
    must_haves text originally stated -- MLflow's backend stores (SqlAlchemyStore, verified
    this session) always server-generate a fresh run id with no public API to override it, so
    literal run-id identity across two different stores is not achievable via the
    MlflowClient API this plan mandates using"
  - "scripts/mlflow_backup.sh dumps --data-only (not full schema+data) and
    scripts/mlflow_restore.sh truncates every table before loading -- a full pg_dump replayed
    against the container's own self-migrated schema hit real foreign-key cascade failures in
    this session's first restore attempt (documented as a deviation below)"
  - "minio-mc service invocations in both scripts pass the script text as a single command
    argument, not wrapped again in sh -c -- the service's own entrypoint already provides
    that wrapper; double-wrapping hung the container on stdin in this session's first backup
    attempt (documented as a deviation below)"

requirements-completed: [PROD-10]

duration: ~70min
completed: 2026-09-09
---

# Phase M3-05 Plan 09: MLflow Container Platform Summary

**Local docker-compose MLflow platform (Postgres backend + MinIO artifact store) with an opt-in `MLFLOW_TRACKING_URI` client override, a real MlflowClient-only migration of the existing champion registry, and a proven pg_dump/MinIO backup-restore round-trip -- all with zero change to `pyproject.toml`/`uv.lock` and zero effect on CI.**

## Performance

- **Duration:** ~70 min
- **Tasks:** 3/3 complete
- **Files modified:** 13 (9 created, 4 modified)

## Accomplishments

- `docker-compose.mlflow.yml` + `docker/mlflow/Dockerfile`: five services (postgres, minio,
  minio-init, mlflow, minio-mc), every port bound `127.0.0.1` only, brought up for real with
  `docker compose --env-file .env.mlflow -f docker-compose.mlflow.yml up -d --build` and
  health-checked at `http://127.0.0.1:5000/health` (`OK`).
- `src/flag_football_ep/model/mlflow_store.py`: `MLFLOW_TRACKING_URI` override in
  `tracking_uri()`, conditional `mlruns` mkdir skip in `configure()`, conditional
  `artifact_location` omission in `ensure_experiment()` for a remote store -- zero change to
  `config.py`/`registry.py`/`train.py`, exactly as the plan's interfaces specified.
- `scripts/migrate_mlflow_store.py`: real migration run against this worktree's actual
  `mlruns/mlflow.db` moved every `ep_model`/`wp_model` run (5 + 6 runs) and model version into
  the container, moved both `champion` aliases, and wrote an auditable JSON report.
  `ep_model`/`wp_model_candidates` correctly logged as skipped (absent from source).
- `scripts/mlflow_backup.sh`/`mlflow_restore.sh`: a real `pg_dump --data-only` + MinIO mirror
  backup, a real `docker compose down -v` (destroying the named volumes), a fresh `up -d
  --build`, and a real restore -- champion aliases for both models resolved to the identical
  restored run ids afterward, confirmed via direct `registry.resolve_champion` calls before
  and after the wipe.
- `docs/mlflow-container-platform.md`: German operator runbook covering every required
  section (Vorbereitung, Starten/Stoppen, ffep-Client-Umschaltung, Migration, Backup/Restore,
  Sicherheitsgrenze, Verhältnis zu CI, Bezug zum ADR Anhang A.2); linked from
  `docs/model-training.md` (section 2) and `docs/mlflow-ui-howto.md` without restructuring
  either document.
- Zero new client-side dependency: `git diff pyproject.toml uv.lock` is empty.
  `psycopg2-binary`/`boto3` live only in `docker/mlflow/Dockerfile`.

## Task Commits

1. **Task 1: Containerise the platform** - `d342d15` (feat) -- `docker-compose.mlflow.yml`,
   `docker/mlflow/Dockerfile`, `.env.mlflow.example`, `.gitignore`, `mlflow_store.py`,
   `tests/test_mlflow_store.py`
2. **Task 2: Migrate the existing registry for real** - `417615d` (feat) --
   `scripts/migrate_mlflow_store.py`, `tests/test_migrate_mlflow_store.py`
3. **Task 3: Backup/restore round-trip and the runbook** - `c45aaf2` (docs) --
   `scripts/mlflow_backup.sh`, `scripts/mlflow_restore.sh`,
   `docs/mlflow-container-platform.md`, `docs/model-training.md`, `docs/mlflow-ui-howto.md`

No separate plan-metadata commit for STATE.md/ROADMAP.md -- per this session's explicit
objective, this worktree executor does not touch those files (parallel-execution mode,
synced centrally elsewhere).

## Files Created/Modified

- `docker-compose.mlflow.yml` -- five-service local MLflow platform
- `docker/mlflow/Dockerfile` -- server-only image (mlflow 3.15.1, psycopg2-binary 2.9.12,
  boto3 1.43.90 -- pinned, never in `pyproject.toml`)
- `.env.mlflow.example` -- checked-in placeholder template for the real, gitignored
  `.env.mlflow`
- `.gitignore` -- added `.env.mlflow` and `/backups/`
- `src/flag_football_ep/model/mlflow_store.py` -- `MLFLOW_TRACKING_URI` override, plus the
  fix for `mlflow.set_tracking_uri`'s own env-var write-back side effect (see Deviations)
- `scripts/migrate_mlflow_store.py` -- real, MlflowClient-only registry migration
- `scripts/mlflow_backup.sh` / `scripts/mlflow_restore.sh` -- real pg_dump/MinIO round-trip
- `docs/mlflow-container-platform.md` -- the operator runbook
- `docs/model-training.md` / `docs/mlflow-ui-howto.md` -- one linking sentence/paragraph each
- `tests/test_mlflow_store.py` (15 tests) / `tests/test_migrate_mlflow_store.py` (7 tests) --
  new coverage, no real server required (monkeypatched where a live server would otherwise be
  needed)

## Decisions Made

See `key-decisions` in frontmatter. Most significant: the plan's literal must-have text
("resolve to the identical run ids") is not achievable via the MlflowClient API the plan
itself mandates using (`mlflow`'s backend stores always server-generate a fresh run id,
verified against the installed `SqlAlchemyStore` source this session) -- resolved by proving
fidelity through a `migrated_from_run_id` tag plus matching champion resolution instead,
documented explicitly in the migration script's docstring and the runbook so this is stated,
not silently glossed over.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `mlflow.set_tracking_uri`'s own env-var write-back leaked across `Config` objects in the same process**
- **Found during:** Task 1, a proactive full regression run of `tests/test_model_train.py`
  after adding the `MLFLOW_TRACKING_URI` override
- **Issue:** `mlflow.set_tracking_uri()` itself writes `MLFLOW_TRACKING_URI` into
  `os.environ` as a side effect ("so that subprocess can inherit it" --
  `mlflow/tracking/_tracking_service/utils.py`, verified against the installed `mlflow`
  3.15.1 source this session). My first `tracking_uri()`/`configure()` implementation read
  that same env var as its override signal, creating a feedback loop: the first local-sqlite
  `configure()` call in a process leaked its resolved path into `os.environ`, and every
  subsequent `tracking_uri()` call for a *different* `Config` (e.g. a different `tmp_path`
  in a test suite) misread that leaked value as an intentional override and got stuck on the
  first config's store. Reproduced live, isolated, and root-caused via a minimal two-Config
  repro script; confirmed the original (pre-this-plan) `mlflow_store.py` did not exhibit
  the bug via an A/B test on the exact same failing test selection.
- **Fix:** `configure()` now captures whether an override was present in `os.environ`
  *before* calling `mlflow.set_tracking_uri`; if none was present, it pops the env var back
  off immediately after (undoing mlflow's own leaked write), keeping the next call's override
  detection honest. A real external override (the normal `MLFLOW_TRACKING_URI=http://...`
  case) is left untouched.
- **Files modified:** `src/flag_football_ep/model/mlflow_store.py`,
  `tests/test_mlflow_store.py` (two new regression tests)
- **Verification:** `uv run pytest tests/test_mlflow_store.py tests/test_model_train.py
  tests/test_migrate_mlflow_store.py -q` -- all green (previously 3-4 of
  `test_model_train.py`'s tests failed with cross-test state bleed, e.g. `assert 5 == 2`
  registered-model-version counts accumulating across tests).
- **Committed in:** `d342d15`

**2. [Rule 1 - Bug] `minio-mc` service double-`sh -c` wrapping hung on stdin**
- **Found during:** Task 3, first real `mlflow_backup.sh` run
- **Issue:** The plan's own interfaces skeleton for the `minio-mc` service sets
  `entrypoint: ["/bin/sh", "-c"]`, and its Task 3 action text (copied verbatim into the first
  draft of both scripts) invokes it as `docker compose run --rm minio-mc sh -c '<script>'`.
  Combined, this produces `/bin/sh -c "sh" "-c" "<script>"` -- the outer shell runs a bare,
  argument-less `sh`, which starts reading from stdin and hangs forever (container stayed
  `Up`, produced zero output, had to be force-removed).
- **Fix:** Removed the redundant `sh -c` wrapper from both scripts' `docker compose run --rm
  minio-mc` invocations -- the command argument is now the script text itself, matching what
  the service's own entrypoint already expects.
- **Files modified:** `scripts/mlflow_backup.sh`, `scripts/mlflow_restore.sh`
- **Verification:** Real backup run mirrored 22 real artifact files from the container's MinIO
  bucket to `backups/mlflow/<timestamp>/minio/` (previously 0 files, silent hang).
- **Committed in:** `c45aaf2`

**3. [Rule 1 - Bug] Full-schema `pg_dump` collided with the container's self-migrated schema on restore**
- **Found during:** Task 3, first real `mlflow_restore.sh` run
- **Issue:** The `mlflow` server container re-runs its own alembic schema migrations on every
  start. Restoring a plain (schema+data) `pg_dump` against that already-initialised schema
  produced ~265 `ERROR: relation "..." already exists` / `multiple primary keys ... not
  allowed` messages, and -- critically -- a duplicate-key conflict on the auto-created empty
  `Default` experiment row aborted the entire `experiments` table's `COPY`, cascading via
  foreign keys into every dependent table's `COPY` failing too. `psql`'s default
  continue-on-error behavior masked this as merely "noisy but harmless"; the restored store
  was actually still empty (`resolve_champion` raised "no champion alias set" after a
  "successful" restore).
- **Fix:** `mlflow_backup.sh` now dumps `--data-only --disable-triggers`.
  `mlflow_restore.sh` truncates every `public` schema table (`TRUNCATE ... CASCADE` via a
  dynamic `DO` block) before loading the data-only dump, and the load itself now runs with
  `-v ON_ERROR_STOP=1` so any future unexpected error fails loudly instead of silently
  continuing.
- **Files modified:** `scripts/mlflow_backup.sh`, `scripts/mlflow_restore.sh`
- **Verification:** Full real cycle re-run: backup -> `docker compose down -v` -> fresh `up -d
  --build` -> restore -> `registry.resolve_champion("ep_model"/"wp_model", cfg)` against the
  container resolved to the exact same run ids as immediately before the wipe (`a2bea74f...`,
  `ca30b515...`), confirmed via direct Python calls, not just "no errors printed".
- **Committed in:** `c45aaf2`

---

**Total deviations:** 3 auto-fixed (all Rule 1 -- bugs discovered via real execution, not
speculative). **Impact:** All three were necessary for the plan's own stated success criteria
("a real wipe-and-recover cycle... proving the rollback path works, not just that it is
documented") to actually be true. No scope creep -- every fix stayed inside the same three
files the plan already scoped to this task.

## Issues Encountered

- Extreme CPU contention from concurrent sibling GSD executors on the same machine
  (confirmed via `ps aux` -- multiple `pytest`/`uv run` processes from other worktrees running
  full XGBoost training suites in parallel) made every background test run take substantially
  longer than normal. Did not affect correctness -- every result reported here was verified
  directly, not assumed from a timed-out or truncated run.
- `mlflow.get_registry_uri()`/`get_model_version_by_alias` raise (rather than return `None`)
  for a not-yet-registered model name -- `scripts/migrate_mlflow_store.py` handles this with
  a narrow `try`/`except Exception` around `get_registered_model`/`get_model_version_by_alias`
  (documented inline as version-dependent MLflow behavior, not a silent broad catch of
  migration-critical errors).

## User Setup Required

None for automated setup -- `docker`/`docker compose` were already installed and running on
this machine (verified live: Docker 29.5.3, Compose v5.1.4; the daemon needed a one-time
`open -a Docker` + poll to start, done automatically, no user action required). A real,
generated `.env.mlflow` was created locally (gitignored, never committed) so the stack could
run for the real verification in this session -- any future operator repeats this via
`docs/mlflow-container-platform.md`'s Vorbereitung section (`cp .env.mlflow.example
.env.mlflow` + generate real values).

## Known Stubs

None -- every artifact (compose stack, migration script, backup/restore scripts, runbook) was
exercised for real against this worktree's actual MLflow store during this session, not
merely written and assumed to work.

## Threat Flags

None beyond what this plan's own `<threat_model>` (T-M3-05-18 through T-M3-05-22, T-M3-05-SC)
already covers -- no new network endpoint, auth path, or schema change was introduced beyond
what the ADR and this plan's threat register anticipated. The loopback-only binding and
no-basic-auth scope boundary are stated explicitly in `docs/mlflow-container-platform.md`'s
Sicherheitsgrenze section, matching the plan's threat register.

## Next Phase Readiness

- The containerised platform is up, healthy, and holds a real (migrated) copy of the
  production registry with both champion aliases resolving correctly -- ready for a developer
  to opt in via `MLFLOW_TRACKING_URI` at any time, with zero effect on the local sqlite store
  or any other in-flight plan in this phase.
- The backup/restore rollback path is proven, not just scripted -- an operator can safely run
  `docker compose down -v` after a `mlflow_backup.sh` run, confident `mlflow_restore.sh`
  brings the registry back exactly.
- CI (`.github/workflows/ci.yml`) is unaffected: verified full-suite run with Docker running
  but `MLFLOW_TRACKING_URI` unset stayed on the local sqlite/file store, exactly as every
  other plan in this phase assumes.
- The local docker-compose configuration is deliberately the same shape ADR Anhang A.2
  describes for the future OTC-VM migration -- when that migration's trigger fires (a second
  write user, or BL-02 needing independent uptime), only the endpoint/credential values
  change, not the compose command shape or the `MLFLOW_TRACKING_URI` mechanism.

## Self-Check

Files:
- `docker-compose.mlflow.yml` -- FOUND
- `docker/mlflow/Dockerfile` -- FOUND
- `.env.mlflow.example` -- FOUND
- `scripts/migrate_mlflow_store.py` -- FOUND
- `scripts/mlflow_backup.sh` -- FOUND
- `scripts/mlflow_restore.sh` -- FOUND
- `docs/mlflow-container-platform.md` -- FOUND
- `tests/test_mlflow_store.py` -- FOUND
- `tests/test_migrate_mlflow_store.py` -- FOUND

Commits (`git log --oneline`):
- `d342d15` -- FOUND
- `417615d` -- FOUND
- `c45aaf2` -- FOUND

Verification re-run:
- `uv run pytest tests/test_mlflow_store.py tests/test_migrate_mlflow_store.py -q` -- 22
  passed (15 + 7)
- `docker compose --env-file .env.mlflow -f docker-compose.mlflow.yml config -q` -- validates
  clean
- `curl -sf http://127.0.0.1:5000/health` -- `OK`
- `registry.resolve_champion("ep_model"/"wp_model", cfg)` against the container resolves to
  `a2bea74f77be456881ab82d4956c567e` / `ca30b51510884ebea13a81124bf47548`, each carrying a
  `migrated_from_run_id` tag matching the source store's own `resolve_champion` result
  (`97259da7acaf43f3b2c65e59f7f11694` / `2c8c249d295d4ce9a2845800c459c153`)
- Real backup -> `down -v` -> `up -d --build` -> restore cycle: champion aliases resolve
  identically afterward (see Accomplishments)
- `git diff --quiet pyproject.toml uv.lock` -- clean, no dependency changes
- `git status --short` -- clean working tree after all three task commits
- No `.env.mlflow` or `/backups/` content tracked by git (`git ls-files | grep -i
  "\.env\.mlflow$"` and `git ls-files | grep "^backups/"` both empty)
- Full project regression (`tests/test_model_train.py` + `tests/test_mlflow_store.py` +
  `tests/test_migrate_mlflow_store.py`, 74 tests) -- all green after the Task 1 fix

## Self-Check: PASSED

---
*Phase: M3-05-epa-plattform*
*Completed: 2026-09-09*
