#!/usr/bin/env bash
# Real pg_dump-restore + MinIO-mirror restore of the containerised MLflow platform (M3-05-09,
# ADR docs/adr/0001-modell-plattform.md Option (ii) gestaffelt, Punkt 1). Inverse of
# scripts/mlflow_backup.sh. Runbook: docs/mlflow-container-platform.md.
#
# Usage (from the repo root, stack already up -- typically a FRESH stack after
# `docker compose ... down -v && ... up -d --build`, the disaster this backup exists for):
#   bash scripts/mlflow_restore.sh --from backups/mlflow/<UTC-timestamp>
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

COMPOSE_FILE="docker-compose.mlflow.yml"
ENV_FILE=".env.mlflow"

if [ "${1:-}" != "--from" ] || [ -z "${2:-}" ]; then
  echo "usage: $0 --from backups/mlflow/<UTC-timestamp>" >&2
  exit 1
fi
BACKUP_DIR="$2"

if [ ! -f "$ENV_FILE" ]; then
  echo "error: $ENV_FILE not found -- copy .env.mlflow.example to $ENV_FILE and fill in real values first" >&2
  exit 1
fi
if [ ! -f "${BACKUP_DIR}/postgres.sql" ]; then
  echo "error: ${BACKUP_DIR}/postgres.sql not found -- was this backup produced by scripts/mlflow_backup.sh?" >&2
  exit 1
fi

# Source .env.mlflow ourselves so the operator never has to re-export the same variables
# twice -- `set -a` exports every variable sourced below into the environment for the
# `docker compose` invocations that follow.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

TIMESTAMP="$(basename "$BACKUP_DIR")"

echo "==> truncating existing (freshly-migrated, empty) tables before data load"
# The target stack's `mlflow` container runs its own alembic migrations on every start,
# creating an already-populated-with-seed-rows-but-otherwise-empty schema (e.g. the
# auto-created "Default" experiment). Restoring a --data-only dump onto that schema without
# truncating first would hit a duplicate-key conflict on that seed row, aborting the COPY for
# that table and cascading via foreign keys into every dependent table's COPY failing too
# (verified this session's real backup-wipe-restore proof run -- see
# scripts/mlflow_backup.sh's matching comment). CASCADE handles inter-table foreign-key
# ordering automatically -- truncating every public-schema table in one DO block means no
# manual dependency ordering is needed here.
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  psql -U "$MLFLOW_POSTGRES_USER" -d "$MLFLOW_POSTGRES_DB" -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'TRUNCATE TABLE public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END
\$\$;
"

echo "==> restoring Postgres backend store from ${BACKUP_DIR}/postgres.sql"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U "$MLFLOW_POSTGRES_USER" -d "$MLFLOW_POSTGRES_DB" \
  < "${BACKUP_DIR}/postgres.sql"

echo "==> restoring MinIO artifact bucket ${MLFLOW_ARTIFACT_BUCKET} from ${BACKUP_DIR}/minio"
# See scripts/mlflow_backup.sh's comment above its own equivalent call: the minio-mc
# service's own `entrypoint: ["/bin/sh", "-c"]` already provides the `sh -c` wrapper --
# the command argument here must be the single script string itself, not `sh -c '<script>'`
# again (a second wrapper hangs on stdin instead of running the script).
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" run --rm minio-mc \
  "mc alias set local http://minio:9000 \"\$MLFLOW_MINIO_ROOT_USER\" \"\$MLFLOW_MINIO_ROOT_PASSWORD\" && mc mirror \"/backups/${TIMESTAMP}/minio\" \"local/\$MLFLOW_ARTIFACT_BUCKET\""

echo "==> restore complete from ${BACKUP_DIR}"
