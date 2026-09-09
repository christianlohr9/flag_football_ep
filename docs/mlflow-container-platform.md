# MLflow-Container-Plattform -- Runbook (M3-05-09)

Diese Anleitung ist das Betriebs-Runbook für die containerisierte MLflow-Plattform aus
`docs/adr/0001-modell-plattform.md` (Entscheidung "Option (ii), gestaffelt", Punkt 1: "Sofort,
lokal: Containerisierung der Modellplattform mit `docker-compose`"). Sie ergänzt
`docs/model-training.md` (Trainings-/Promotion-Workflow, unverändert) und
`docs/mlflow-ui-howto.md` (UI-Demo, funktioniert identisch gegen diesen Store) -- keines der
beiden Dokumente wird durch diese Plattform ersetzt.

**Kurzfassung:** Lokal laufen jetzt zwei mögliche MLflow-Stores nebeneinander -- der
bestehende sqlite-/Datei-Store (`mlruns/mlflow.db`, weiterhin der Standard, unverändert) und
ein containerisierter Store (MLflow-Server + Postgres + MinIO, per Docker Compose). Welcher
Store aktiv ist, entscheidet ausschließlich die Umgebungsvariable `MLFLOW_TRACKING_URI` --
gesetzt heißt Container, ungesetzt heißt sqlite. Kein Code in `train.py`/`registry.py`/
`cli.py` ändert sich dafür.

## Vorbereitung (einmalig)

```bash
cp .env.mlflow.example .env.mlflow
```

Dann in `.env.mlflow` echte, generierte Werte eintragen (niemals die Platzhalter aus dem
Beispiel übernehmen):

- `MLFLOW_POSTGRES_PASSWORD`, `MLFLOW_MINIO_ROOT_USER`, `MLFLOW_MINIO_ROOT_PASSWORD` -- z. B.
  mit `python3 -c "import secrets; print(secrets.token_urlsafe(24))"` erzeugen.
- `MLFLOW_POSTGRES_USER`/`MLFLOW_POSTGRES_DB`/`MLFLOW_ARTIFACT_BUCKET` haben sinnvolle
  Vorgaben in `docker-compose.mlflow.yml` (`mlflow`/`mlflow`/`ffep-mlflow-artifacts`) und
  müssen normalerweise nicht geändert werden.

`.env.mlflow` ist bereits in `.gitignore` eingetragen (`.env.mlflow.example` ist die einzige
getrackte Variante, ohne echte Werte) -- niemals committen.

## Starten und Stoppen

**Jeder Befehl, der diese Datei anfasst, braucht `--env-file .env.mlflow` explizit** --
Docker Compose lädt automatisch nur eine Datei namens `.env`, nie eine anders benannte. Das
ist der wahrscheinlichste erste Stolperstein:

```bash
# Validieren (keine Container, nur Konfigurationscheck)
docker compose --env-file .env.mlflow -f docker-compose.mlflow.yml config -q

# Starten (postgres + minio + minio-init + mlflow; baut das mlflow-Server-Image beim ersten Mal)
docker compose --env-file .env.mlflow -f docker-compose.mlflow.yml up -d --build

# Healthcheck
curl -sf http://127.0.0.1:5000/health   # antwortet "OK"

# Stoppen (Daten bleiben in den benannten Volumes erhalten)
docker compose --env-file .env.mlflow -f docker-compose.mlflow.yml down

# Stoppen UND Daten löschen (nur für einen bewussten Neuanfang, siehe Backup/Restore unten)
docker compose --env-file .env.mlflow -f docker-compose.mlflow.yml down -v
```

Die UI ist danach unter `http://127.0.0.1:5000` erreichbar -- identisch zur lokalen
`mlflow ui`, siehe `docs/mlflow-ui-howto.md`.

## `ffep` gegen den Container zeigen

```bash
export MLFLOW_TRACKING_URI=http://127.0.0.1:5000
ffep train --model both
ffep promote --model both
ffep score
```

Alternativ in der lokalen, gitignorierten `.env` (nicht `.env.mlflow` -- diese Datei gehört
ausschließlich `docker-compose.mlflow.yml` und wird von `config.load_dotenv()` nie gelesen):

```
MLFLOW_TRACKING_URI=http://127.0.0.1:5000
```

**Zurücksetzen = Variable entfernen.** `unset MLFLOW_TRACKING_URI` (oder die Zeile aus `.env`
löschen) bringt `ffep` sofort zurück auf den lokalen sqlite-Store -- kein anderer Schritt
nötig. Das ist der komplette Rollback-Pfad auf der Client-Seite: der sqlite-Store wird von
keinem Schritt hier je verändert oder gelöscht.

## Einmalige Migration der bestehenden Registry

`scripts/migrate_mlflow_store.py` kopiert die bestehende Registry (Experimente, Runs, Params,
Metriken, Artefakte, registrierte Modellversionen, `champion`-Alias) in den Container-Store:

```bash
uv run python scripts/migrate_mlflow_store.py \
  --source "sqlite:///$(pwd)/mlruns/mlflow.db" \
  --target "http://127.0.0.1:5000"
```

Nutzt ausschließlich die öffentliche `MlflowClient`-API von `mlflow` selbst -- bewusst nicht
das Drittanbieter-Paket `mlflow-export-import` (kein RESEARCH-Package-Legitimacy-Audit, bei
diesem Projekt-Lauf-Umfang unnötig). Das Skript **liest nur** aus `--source` und **schreibt
nur** in `--target` -- der Quell-Store wird nie verändert, gelöscht oder gesperrt. Ein
Migrationslauf schreibt zusätzlich einen JSON-Report nach
`backups/mlflow/migration_report_<Datum>.json` (gitignored) mit den Lauf-/Versions-Zählern je
Experiment/Modell und den aufgelösten Champion-Run-IDs im Zielstore.

**Wichtige, verifizierte Einschränkung:** MLflow bietet keine öffentliche API, um beim
Anlegen eines Runs in einem neuen Store dieselbe Run-ID wie im Quell-Store zu erzwingen --
jeder Backend-Store generiert seine eigene (`uuid.uuid4().hex`, session-verifiziert gegen
den installierten SQLAlchemy-Store). Migrierte Runs bekommen deshalb zwangsläufig eine neue
Run-ID im Container-Store. Nachvollziehbarkeit bleibt trotzdem vollständig erhalten: jeder
migrierte Run trägt einen `migrated_from_run_id`-Tag mit der exakten Quell-Run-ID, und
`registry.resolve_champion("ep_model"/"wp_model", cfg)` löst im Container-Store auf einen Run
auf, dessen `migrated_from_run_id`-Tag exakt der Run-ID entspricht, die `resolve_champion`
gegen den unveränderten Quell-Store liefert -- das ist der eigentliche Nachweis, dass die
Migration inhaltlich korrekt war, nicht die (unerreichbare) Identität der Run-ID selbst.

## Backup und Restore

```bash
# Backup (Stack muss laufen)
bash scripts/mlflow_backup.sh
# -> backups/mlflow/<UTC-Zeitstempel>/postgres.sql + .../minio/ (beides gitignored)

# Restore in einen (typischerweise frischen) laufenden Stack
bash scripts/mlflow_restore.sh --from backups/mlflow/<UTC-Zeitstempel>
```

**Wann nötig:** vor jedem `docker compose ... down -v` (löscht die benannten Volumes
unwiderruflich), vor größeren Docker-/Postgres-Versionssprüngen, oder einfach als
regelmäßige Sicherung, solange die Plattform der einzige Ort ist, an dem der
Container-Store-Stand existiert.

**Funktionsweise, ein Implementierungsdetail mit Betriebsrelevanz:** `mlflow_backup.sh` nutzt
`pg_dump --data-only` (nicht das Schema) -- der `mlflow`-Server legt sein eigenes Schema bei
jedem Containerstart selbst per Alembic-Migration an, ein Full-Schema-Dump würde beim Restore
mit dem bereits vorhandenen Schema kollidieren (session-verifiziert: führt zu
Fremdschlüssel-Fehlern, die das Laden abbrechen). `mlflow_restore.sh` leert deshalb vor dem
Laden zuerst alle Tabellen im frisch migrierten Ziel-Schema (`TRUNCATE ... CASCADE`), damit
die reinen Datenzeilen ohne Konflikt mit den vom `mlflow`-Server selbst angelegten
Startwerten (z. B. das automatisch erzeugte `Default`-Experiment) geladen werden können.
Beide Skripte lesen `.env.mlflow` selbst ein (`set -a; source .env.mlflow; set +a`) -- die
Variablen müssen nicht doppelt exportiert werden.

**Bewiesen, nicht nur dokumentiert:** dieser Zyklus (`mlflow_backup.sh` ->
`docker compose ... down -v` -> `... up -d --build` -> `mlflow_restore.sh`) wurde in dieser
Session real durchgeführt -- danach lösten `ep_model`/`wp_model`s `champion`-Aliase im
Container-Store exakt wieder auf dieselben (durch den Restore wiederhergestellten) Run-IDs
auf wie unmittelbar vor dem `down -v`.

## Sicherheitsgrenze

MLflow liefert standardmäßig keine Authentifizierung aus (mehrere 2026 gemeldete CVEs gegen
unauthentifizierte MLflow-Tracking-Server, RESEARCH Q9). Diese Plattform bindet **jeden**
veröffentlichten Port ausschließlich an `127.0.0.1` -- nichts hier ist heute vom Netzwerk aus
erreichbar -- und aktiviert **kein** MLflow-Basic-Auth. Das ist eine bewusste,
größenordnungsgerechte Entscheidung für einen lokalen Loopback-Store, keine übersehene
Lücke: Basic-Auth (inkl. sofortiger Rotation des Default-Credentials `admin`/`password1234`)
plus Reverse-Proxy und TLS sind explizit erst zur Migrationszeit auf die OTC-VM (ADR Anhang
A.2) Pflicht, bevor irgendein Port über Loopback hinaus gebunden wird. Vor diesem Zeitpunkt
diese Plattform nicht ohne die A.2-Checkliste öffentlich erreichbar machen.

## Verhältnis zu CI

`.github/workflows/ci.yml` bleibt von dieser Plattform vollständig unberührt: CI läuft ohne
Docker und ohne gesetztes `MLFLOW_TRACKING_URI`, also ausschließlich gegen den lokalen
sqlite-/Datei-Store -- exakt wie jeder andere Plan dieser Phase es voraussetzt. Die
`MLFLOW_TRACKING_URI`-Weiche in `mlflow_store.py` ist rein opt-in: ungesetzt ändert sich am
bestehenden Verhalten nichts.

## Bezug zum ADR (Anhang A.2, künftiger OTC-VM-Umzug)

Diese lokale docker-compose-Plattform ist bewusst dieselbe Konfigurationsform, die
`docs/adr/0001-modell-plattform.md`s Anhang A.2 für den künftigen Umzug auf eine OTC-VM
beschreibt (MLflow-Server + Postgres-Backend + S3-kompatibler Artefakt-Store) -- lokal ist
der S3-kompatible Store MinIO, auf der VM wäre es OTC OBS, unter denselben Umgebungsvariablen
(`MLFLOW_S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, dieselbe Konvention
wie `docs/hackathon-otc-upload.md`). Der Umzug selbst ist ein eigener, im ADR benannter
Backlog-Eintrag mit einem konkreten Auslöser (ein zweiter schreibender Nutzer, oder eine
Web-App, die unabhängig vom Entwickler-Laptop laufen muss) -- nicht Teil dieses Runbooks.
