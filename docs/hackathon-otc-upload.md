# OTC-OBS-Upload — Runbook für die Hackathon-Bundle-Auslieferung (Stand: 2026-09-07)

**Status: Zugangsdaten (`OTC_OBS_ACCESS_KEY_ID`/`OTC_OBS_SECRET_ACCESS_KEY`) liegen in
dieser Umgebung noch nicht vor — voraussichtlich für einen längeren Zeitraum nicht.
Die Auslieferung ist deshalb bis hierhin vollständig lokal vorbereitet (Plan 02.2-14):
alle drei Bundle-Archive liegen gehasht und mit Prüfsummen-Manifest unter
`data/processed/hackathon-delivery/<datum>/` (gitignored, siehe `## Lokale Staging`
unten). Dieses Dokument ist der Ablauf für den Tag, an dem die Zugangsdaten
eintreffen — beide Wege, exakt ausführbar, ohne dass irgendjemand einen Wert raten
oder recherchieren muss.**

Bucket und Endpoint sind in `ffep.toml` konfiguriert (`[cv]`-Sektion):

```
dvc_remote_name     = "otc-obs"
dvc_remote_url      = "s3://ffep-datasets-PLACEHOLDER/flag-football-datasets"
dvc_remote_endpoint = "https://obs.eu-de.otc.t-systems.com"
```

Der Bucket-Name ist noch der **Platzhalter** `ffep-datasets-PLACEHOLDER` — Plan 02.2-20
ersetzt ihn durch den echten, sobald das OTC-OBS-Projekt provisioniert ist
(`docs/cv-setup.md`). Bis dahin in allen Befehlen unten `<BUCKET>` durch den echten
Namen ersetzen, sobald er feststeht; der Präfix `flag-football-datasets` bleibt gleich.

**Unverifiziert (RESEARCH Pitfall 3, siehe `docs/cv-setup.md`):** der eigentliche Upload
gegen den echten OTC-OBS-Endpunkt wurde noch nie erfolgreich getestet — Plan 02.2-13s
Versuch schlug erwartungsgemäß mit `403 Forbidden` gegen den Platzhalter-Bucket fehl
(kein echter Bucket, keine echten Credentials). Die S3-Kompatibilitätssymptome, auf die
laut RESEARCH zuerst zu prüfen ist, falls der Endpoint sich seltsam verhält: hängender
Upload, Signatur-Mismatch (Path-Style vs. Virtual-Hosted-Addressing) — beide Fallbacks
sind unten in `### Bei einem Signatur-/Adressierungsfehler` benannt.

## Zugangsdaten setzen (einmalig, niemals ins Repo)

```bash
export OTC_OBS_ACCESS_KEY_ID="<access-key>"
export OTC_OBS_SECRET_ACCESS_KEY="<secret-key>"
```

Oder in der lokalen, gitignored `.env` (siehe `.env.example`) — niemals in `ffep.toml`,
niemals in `.dvc/config`, niemals in einer Commit-Message oder einem Issue eingefügt.

## Lokale Staging (bereits erledigt, kein Netzwerk nötig)

```bash
uv run --extra cv ffep cv stage-delivery --bundles-dir data/bundles
```

Erzeugt `data/processed/hackathon-delivery/<heutiges-datum>/` mit:

- `<kind>-set/<archiv>.zip` — Hardlink auf das bereits gebaute Archiv (kein doppelter
  Plattenplatz für die mehrere-GB-Archive)
- `manifest.json` — Größe + voller SHA-256 je Archiv, plus der geplante Objekt-Schlüssel
- `README.md` — dieselbe Tabelle, deutsch, für die Teams

Dieser Schritt ist bereits durchgeführt und geprüft (Plan 02.2-14, 2026-09-07) — beim
tatsächlichen Upload wird nur noch aus diesem Staging-Verzeichnis hochgeladen, nicht aus
`data/bundles/` direkt, damit die hochgeladene Datei exakt der lokal geprüften entspricht.

## Weg A — Projekt-CLI (`ffep cv deliver`)

Für jedes der drei Archive im Staging-Verzeichnis:

```bash
uv run --extra cv --extra versioning ffep cv deliver \
  --archive data/processed/hackathon-delivery/<datum>/dev-set/dev-set_<datum>_<hash>.zip \
  --remote s3://<BUCKET>/flag-football-datasets

uv run --extra cv --extra versioning ffep cv deliver \
  --archive data/processed/hackathon-delivery/<datum>/test-set/test-set_<datum>_<hash>.zip \
  --remote s3://<BUCKET>/flag-football-datasets

uv run --extra cv --extra versioning ffep cv deliver \
  --archive data/processed/hackathon-delivery/<datum>/transfer-set/transfer-set_<datum>_<hash>.zip \
  --remote s3://<BUCKET>/flag-football-datasets
```

`deliver_bundle` (`src/flag_football_ep/cv/bundle.py`) resolviert die Zugangsdaten
ausschließlich über die Umgebungsvariablen oben (`config.secret()`, T-2.2-42), baut den
Objekt-Schlüssel deterministisch aus dem Archiv-Dateinamen (`<kind>-set/<archiv>`) und
verifiziert nach dem Upload die Objekt-Größe gegen die lokale Datei — bei einer
Abweichung bricht der Befehl mit `BundleError` ab, statt eine stille Teil-Übertragung als
Erfolg zu melden. Kein Zugangsdatenwert erscheint jemals in der Ausgabe.

## Weg B — Fallback: AWS-CLI oder obsutil direkt gegen den Endpoint

Falls Weg A aus irgendeinem Grund nicht funktioniert (z. B. `s3fs`/`dvc-s3`-Version
inkompatibel mit dem konkreten OBS-Verhalten), denselben Upload direkt mit einem
generischen S3-Client:

**AWS-CLI** (liest dieselben `OTC_OBS_*`-Variablen nicht automatisch — auf die
AWS-Standardnamen mappen):

```bash
export AWS_ACCESS_KEY_ID="$OTC_OBS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$OTC_OBS_SECRET_ACCESS_KEY"

aws s3 cp \
  data/processed/hackathon-delivery/<datum>/dev-set/dev-set_<datum>_<hash>.zip \
  s3://<BUCKET>/flag-football-datasets/dev-set/dev-set_<datum>_<hash>.zip \
  --endpoint-url https://obs.eu-de.otc.t-systems.com

# wiederholen für test-set/ und transfer-set/
```

**obsutil** (Huawei/OTC-natives CLI-Tool, oft zuverlässiger gegen OBS-Eigenheiten als
generische S3-Clients):

```bash
obsutil config -i="$OTC_OBS_ACCESS_KEY_ID" -k="$OTC_OBS_SECRET_ACCESS_KEY" \
  -e=https://obs.eu-de.otc.t-systems.com

obsutil cp \
  data/processed/hackathon-delivery/<datum>/dev-set/dev-set_<datum>_<hash>.zip \
  obs://<BUCKET>/flag-football-datasets/dev-set/dev-set_<datum>_<hash>.zip -f

# wiederholen für test-set/ und transfer-set/
```

### Bei einem Signatur-/Adressierungsfehler

Die zwei dokumentierten ersten Abhilfen (RESEARCH Pitfall 3):

1. **Path-Style statt Virtual-Hosted-Addressing erzwingen** — AWS-CLI:
   `aws configure set default.s3.addressing_style path`; für `deliver_bundle`/Weg A:
   `s3fs.S3FileSystem(..., config_kwargs={"s3": {"addressing_style": "path"}})`
   (Codeänderung in `_build_s3_client`, falls der Standard-Stil scheitert).
2. **DVCs `listobjects`-Option**, falls die Bucket-Auflistung selbst (nicht der Upload)
   nicht standardkonform reagiert — relevant für den späteren `dvc push`
   (Plan 02.2-20), nicht für einen Einzel-Datei-Upload mit `aws s3 cp`/`obsutil cp`.

## Nach dem Upload — Verifikation (Liste + Prüfsummen-Abgleich)

**1. Objekt-Liste gegen das Staging-Manifest:**

```bash
aws s3 ls s3://<BUCKET>/flag-football-datasets/ --recursive \
  --endpoint-url https://obs.eu-de.otc.t-systems.com
```

Muss genau die drei Objekt-Schlüssel aus
`data/processed/hackathon-delivery/<datum>/manifest.json` (Feld `planned_object_key`)
zeigen, mit identischer Dateigröße (`aws s3 ls` zeigt die Größe in Byte in Spalte 3).

**2. Größen-/ETag-Abgleich je Objekt:**

```bash
aws s3api head-object \
  --endpoint-url https://obs.eu-de.otc.t-systems.com \
  --bucket <BUCKET> --key flag-football-datasets/dev-set/dev-set_<datum>_<hash>.zip
```

`ContentLength` muss exakt `size_bytes` aus dem Manifest sein — das prüft `deliver_bundle`
(Weg A) bereits automatisch; bei Weg B (AWS-CLI/obsutil) ist dieser Schritt der einzige
Nachweis, dass der Upload vollständig war. Der `ETag` ist NUR bei einem Single-Part-Upload
(kleine Dateien) identisch mit dem MD5 der Datei; die mehrere-GB-Bundle-Archive werden
typischerweise multipart hochgeladen, dann ist der `ETag` kein direkter Hash-Vergleich
mehr (S3-Konvention: `<md5-der-teile-konkateniert>-<teile-anzahl>`) — verlasse dich für
die Inhalts-Integrität auf die Größe (oben) plus, bei Zweifel, einen erneuten Download +
`sha256sum`-Vergleich gegen `archive_sha256` im Staging-Manifest (Schritt 3).

**3. Stichprobenhafter Download-Rücktest (bei Zweifel, nicht routinemäßig):**

```bash
aws s3 cp s3://<BUCKET>/flag-football-datasets/dev-set/dev-set_<datum>_<hash>.zip \
  /tmp/verify-dev-set.zip --endpoint-url https://obs.eu-de.otc.t-systems.com
sha256sum /tmp/verify-dev-set.zip
```

Muss exakt `archive_sha256` aus dem Staging-Manifest ergeben.

## Was danach an die Teams geht

Nach erfolgreicher Verifikation (oben):

1. Objekt-URIs (oder, falls der Bucket nicht öffentlich lesbar ist — er sollte es nicht
   sein, siehe unten — signierte, zeitlich begrenzte Download-Links) für das Dev-Set und
   das Transfer-Set an die Teams. Das Test-Set-Archiv wird NICHT vorab verteilt (siehe
   `docs/hackathon-bundles.md` §Test-Set: nur für die Endwertung).
2. Den vollen `archive_sha256`-Wert je Archiv aus dem Staging-Manifest zur eigenständigen
   Verifikation nach dem Download (siehe `data/processed/hackathon-delivery/<datum>/README.md`,
   dieselbe Tabelle geht an die Teams).
3. Den Link/Verweis auf `docs/hackathon-bundles.md` und
   `docs/hackathon-challenge-reid.md` für Inhalt, Schema und Nutzungsregeln.
4. **Bucket-Policy vor dem Versand bestätigen: privat, kein öffentliches Lese-/
   Schreibrecht.** Zugriff ausschließlich über signierte Links oder Zugangsdaten, die
   nur an registrierte Teams gehen — nie ein offener Bucket-Listing-Zugriff.

## Bezug zu Plan 02.2-20

Plan 02.2-20 ersetzt den Platzhalter-Bucket in `ffep.toml`/`.dvc/config` durch den echten
Namen und führt den `dvc push` des Trainings-Datensatzes gegen denselben Endpoint aus
(anderer Datensatz als diese drei Hackathon-Bundles, aber derselbe Bucket/Endpoint) —
sobald dieser Plan läuft, sind Bucket und Endpoint bereits aus diesem Runbook bekannt und
müssen nicht neu ermittelt werden.
