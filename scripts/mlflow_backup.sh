#!/usr/bin/env bash
# Real pg_dump + MinIO-mirror backup of the containerised MLflow platform (M3-05-09, ADR
# docs/adr/0001-modell-plattform.md Option (ii) gestaffelt, Punkt 1). Inverse of
# scripts/mlflow_restore.sh. Runbook: docs/mlflow-container-platform.md.
#
# Usage (from the repo root, stack already up via `docker compose ... up -d`):
#   bash scripts/mlflow_backup.sh
#
# Writes backups/mlflow/<UTC-timestamp>/postgres.sql and .../minio/ -- both gitignored
# (same trust level as mlruns/, never committed).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

COMPOSE_FILE="docker-compose.mlflow.yml"
ENV_FILE=".env.mlflow"

if [ ! -f "$ENV_FILE" ]; then
  echo "error: $ENV_FILE not found -- copy .env.mlflow.example to $ENV_FILE and fill in real values first" >&2
  exit 1
fi

# Source .env.mlflow ourselves so the operator never has to re-export the same variables
# twice -- `set -a` exports every variable sourced below into the environment for the
# `docker compose` invocations that follow.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

TIMESTAMP="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
BACKUP_DIR="backups/mlflow/${TIMESTAMP}"
mkdir -p "$BACKUP_DIR"

# --data-only (not a full schema+data dump): the MLflow server itself owns and creates the
# schema via its own alembic migrations on every container start (verified this session --
# a fresh `mlflow server` container against an empty Postgres database auto-creates every
# table/constraint before this script ever runs). A full pg_dump replayed on top of that
# already-initialized schema fails every CREATE TABLE/ADD CONSTRAINT as "already exists",
# and -- critically -- a duplicate-key conflict on the auto-created empty "Default"
# experiment row aborts the whole `experiments` COPY, which cascades via foreign keys into
# every dependent table's COPY failing too (verified this session's real backup-wipe-restore
# proof run). --disable-triggers matches mlflow_restore.sh's truncate-before-load step below:
# it lets the data-only COPY load rows without per-row FK-order sensitivity.
echo "==> pg_dump --data-only -> ${BACKUP_DIR}/postgres.sql"
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T postgres \
  pg_dump --data-only --disable-triggers -U "$MLFLOW_POSTGRES_USER" "$MLFLOW_POSTGRES_DB" \
  > "${BACKUP_DIR}/postgres.sql"

echo "==> MinIO mirror (bucket ${MLFLOW_ARTIFACT_BUCKET}) -> ${BACKUP_DIR}/minio"
mkdir -p "${BACKUP_DIR}/minio"
# The minio-mc service's own `entrypoint: ["/bin/sh", "-c"]` (docker-compose.mlflow.yml)
# already provides the `sh -c` wrapper -- `docker compose run`'s command argument here must
# be the single script string itself, NOT `sh -c '<script>'` again. A second `sh -c` wrapper
# makes the outer shell run a bare, argument-less `sh` (positional params swallowed), which
# hangs reading stdin forever instead of running the script (verified this session).
docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" run --rm minio-mc \
  "mc alias set local http://minio:9000 \"\$MLFLOW_MINIO_ROOT_USER\" \"\$MLFLOW_MINIO_ROOT_PASSWORD\" && mc mirror \"local/\$MLFLOW_ARTIFACT_BUCKET\" \"/backups/${TIMESTAMP}/minio\""

echo "==> backup complete: ${BACKUP_DIR}"
