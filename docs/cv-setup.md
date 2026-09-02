# CV-Setup — Umgebung & Installationsprotokoll

**Status: Installationsprobe auf der Primärmaschine erfolgreich am 2026-08-24 (Phase 2.1, Plan 01, Task 3) — CVAT-Setup folgt in Plan 02.1-08.**

## Zweck & Abgrenzung

Dieses Dokument hält fest, was beim ersten `uv sync --extra cv` auf der Primärmaschine tatsächlich passiert ist: installierte Paketversionen, Lizenzen, MPS-Verfügbarkeit, die Pro-Maschine-torch-Falle (M4/M5 Max vs. Dell-CUDA) und die Grounding-DINO-Backend-Entscheidung. Es ist der lebende Umgebungs-Nachweis, den spätere Pläne dieser Phase erweitern (CVAT-Abschnitt: Plan 02.1-08).

Nicht in Scope: der eigentliche Pilotablauf (Sighting, Labeling, Training, Tracking — eigene Pläne), die Gate-Entscheidung (`docs/pilot-gate-decision.md`, Plan 02.1-17 o.ä.).

## Hardware-Hinweis: Namensabweichung M4 Max vs. M5 Max

`.planning/phases/02.1-cv-tracking-pilot-go-no-go-gate/02.1-CONTEXT.md` (D-05) und das Nutzer-Gedächtnis bezeichnen die Primärmaschine als "M5 Max". `system_profiler SPHardwareDataType | grep Chip` auf der tatsächlichen Ausführungsmaschine dieser Task meldet:

```
Chip: Apple M4 Max
```

Diese Abweichung ändert nichts an einer technischen Entscheidung dieses Dokuments — beide Chips teilen dieselbe MPS/Metal-Fähigkeitsklasse für die Zwecke dieser Phase — wird hier aber wie von RESEARCH.md gefordert wörtlich festgehalten statt stillschweigend "M5 Max" zu wiederholen.

## Installierte Versionen

`uv sync --extra cv` auf der Primärmaschine installiert die folgenden Pakete (Stand 2026-08-24, alle Apache-2.0/MIT/BSD — kein AGPL, C-06):

| Paket | Version | Lizenz | Zweck |
|---|---|---|---|
| `rfdetr` | 1.9.3 | Apache-2.0 | RF-DETR-Small Detector-Fine-Tune |
| `trackers` (roboflow) | 2.6.0 | Apache-2.0 | OC-SORT Tracking (`trackers.OCSORTTracker`) |
| `supervision` | 0.30.0 | MIT | gemeinsames `Detections`-Datenmodell |
| `sahi` | 0.12.6 | MIT | Sliced/Tiled Inference für kleine Objekte |
| `transformers` | 5.15.1 | Apache-2.0 | `SiglipVisionModel` (Team-Clustering) + `GroundingDinoForObjectDetection` (Zero-Shot-Vorlabeling, siehe unten) |
| `umap-learn` | 0.5.12 | BSD | Dimensionsreduktion vor KMeans |
| `opencv-python` | 5.0.0.93 | Apache-2.0 | Homographie (`findHomography`/`warpPerspective`/`perspectiveTransform`), Radar-Rendering |
| `torch` | 2.13.0 | Apache-2.0 / BSD-2-Clause / BSD-3-Clause / BSL-1.0 / MIT (Mischlizenz, keine AGPL-Komponente) | Backend für rfdetr/transformers |
| `torchvision` | 0.28.0 | BSD | Torch-Begleitpaket (transitiv über `rfdetr`/`transformers`) |
| `cvat-sdk` | 2.73.0 | MIT | programmatischer CVAT-Import/Export |

`scikit-learn` (bereits Core-Dependency, `>=1.5.1`) liefert `KMeans` für den letzten Team-Clustering-Schritt — kein zusätzliches Paket nötig.

**Nachtrag (Per-Clip-Homographie-Verfeinerung, 2026-08-30):** `homography.clip_alignment` nutzt zusätzlich `cv2.SIFT_create` (ORB-Alternative, siehe `docs/homography-calibration.md`) aus demselben bereits installierten `opencv-python`-Paket — SIFT liegt seit Patentablauf 2020 im Standard-`opencv-python`/`opencv-python-headless`, kein `opencv-contrib`-Zusatzpaket nötig, keine neue Zeile in obiger Tabelle.

**Nachtrag (ECC-Zweitstufe, 2026-08-30 Follow-up):** `homography._ecc_align` nutzt zusätzlich `cv2.findTransformECC` — ebenfalls Teil des Standard-`opencv-python`-Pakets, kein neues Paket, keine neue Zeile in obiger Tabelle. Siehe `docs/homography-calibration.md`s "ECC-Zweitstufe"-Abschnitt für das Design und die empirische Schwellenwert-Kalibrierung.

**Ausdrücklich nicht installiert:** `boxmot` — AGPL-3.0-lizenziert (verifiziert über die eigene `LICENSE`-Datei des Projekts und die GitHub-API, Stand 2026-08-24), verletzt C-06. `trackers` (roboflow, Apache-2.0) implementiert OC-SORT nativ und ersetzt es vollständig — siehe `tests/test_cv_dependencies.py` für den automatisierten Guard.

## MPS/CPU-Verfügbarkeit auf der Primärmaschine

```
torch.backends.mps.is_available() -> True
```

Torch erkennt die Apple-Silicon-GPU (Metal) auf der Primärmaschine. Für rechenintensive, aber seltene Schritte (Zero-Shot-Vorlabeling, SAM2-Propagierung) ist reines CPU-Fallback dennoch akzeptabel (RESEARCH Pitfall 3) — kein Grund, Setup-Zeit in MPS-Feintuning zu stecken, solange es nicht die heiße Trainingsschleife betrifft.

## Pro-Maschine-torch-Falle

Eine `pyproject.toml`, zwei Umgebungen: die Primärmaschine (M4 Max, siehe Namensabweichung oben) installiert das MPS-fähige `torch`-Wheel; der Dell-Rechner (8 GB CUDA-GPU, D-05) braucht ein CUDA-fähiges `torch`-Wheel, das zur installierten CUDA-Toolkit-Version passt. **Eine `uv.lock`-Auflösung bedient nicht automatisch beide Maschinen identisch** — vor dem RF-DETR-Fine-Tune auf dem Dell-Rechner muss dort separat synchronisiert und die tatsächlich installierte `torch`-Variante geprüft werden (`torch.cuda.is_available()`), nicht die hier dokumentierte MPS-Version angenommen werden.

## Dataset-Versionierung (DVC)

Phase 2.2 (D-18) führt Datensatz-Versionierung mit DVC ein, in einer eigenen Extras-Gruppe getrennt von `cv`:

```
uv sync --extra versioning
```

**Warum eine eigene Gruppe:** `dvc`/`dvc-s3` sind reine CLI/Storage-Tools ohne Bezug zu `torch`/`rfdetr`. Eine separate Gruppe erlaubt einen `dvc pull`-only-Workflow (z. B. auf einer reinen Labeling-Maschine) ohne den Multi-GB-Torch-Stack der `cv`-Gruppe zu installieren. Beide Pakete sind Apache-2.0 (C-06); Legitimität gegen pypi.org/project/dvc und pypi.org/project/dvc-s3 geprüft per menschlichem Checkpoint am 2026-09-01 (Plan 02.2-04, Task 1) — `dvc`s Quellcode liegt seit der lakeFS-Übernahme im November 2025 unter `github.com/treeverse/dvc`, `dvc-s3` weiterhin unter `github.com/iterative/dvc-s3`.

**Layout-Entscheidung (RESEARCH Open Question 1, Planner-Entscheidung):** ein einziges, wachsendes DVC-getracktes Datensatz-Verzeichnis `data/labels/dataset/`, nicht pro-AL-Iteration getrennte Verzeichnisse. Der Freeze-Punkt für ein Hackathon-Bundle (D-05) wird als DVC/Git-Commit-Hash + MLflow-`run_id`-Paar festgehalten, nicht als eigenes Verzeichnis — einfacher, und passt zum "ein Detector über alle Domänen"-Framing (D-04).

**Verhältnis zu `dataset_hash()`:** `cv/dataset.py::dataset_hash()` berechnet bereits einen reproduzierbaren SHA-256-Content-Hash über ein COCO-Paket (Bild-Bytes + kanonisches `instances.json`). DVCs eigener MD5-Hash pro getrackter Datei ist ein separater Mechanismus mit anderem Zweck (Content-Addressierung für Push/Pull/Cache) — **beide bleiben nebeneinander bestehen, keiner ersetzt den anderen.** `dataset_hash()` ist die projekt-interne Reproduzierbarkeits-Prüfsumme (z. B. für Trainings-Provenienz-Logging); DVCs MD5 ist DVCs interne Storage-Buchhaltung.

**Remote (OTC OBS, Platzhalter-Bucket):** `.dvc/config` konfiguriert den Remote `otc-obs` ausschließlich über Endpoint/Region — keine Zugangsdaten im Repo:

```
[core]
    remote = otc-obs
['remote "otc-obs"']
    url = s3://ffep-datasets-PLACEHOLDER/flag-football-datasets
    endpointurl = https://obs.eu-de.otc.t-systems.com
    region = eu-de
```

Der Bucket-Name ist ein **Platzhalter** (`ffep-datasets-PLACEHOLDER`) — Plan 02.2-14 provisioniert den echten OTC-OBS-Bucket, Plan 02.2-20 ersetzt den Platzhalter durch den echten Namen. Zugangsdaten (`OTC_OBS_ACCESS_KEY_ID`/`OTC_OBS_SECRET_ACCESS_KEY`) werden nie literal in `ffep.toml` oder `.dvc/config` geschrieben, sondern ausschließlich über `secret(config.cv.otc_obs_access_key_env)` aus der Umgebung/`.env` aufgelöst — dasselbe Muster wie `cvat_username_env`/`cvat_password_env`.

**Unverifiziert in dieser Umgebung (RESEARCH Pitfall 3):** der eigentliche `dvc push`/`dvc pull` gegen den echten OTC-OBS-Endpunkt wurde noch nicht getestet (keine Credentials, kein Test-Bucket in dieser Session) -- Plan 02.2-13s Versuch schlug wie erwartet mit `403 Forbidden` gegen den Platzhalter-Bucket fehl. `tests/test_dvc_layout.py` beweist die DVC-Mechanik gegen einen lokalen Verzeichnis-Remote; Plan 02.2-13 hat denselben lokalen Rückfallweg zusätzlich gegen den echten Iteration-1-Datensatz (ursprünglich 739 Bilder, seit der D-17-Korrektur vom 2026-09-02 558 Bilder, nicht nur ein Test-Fixture) verifiziert (`.dvc/config.local`, git-ignoriert, `docs/dataset-buildout.md` → `### DVC-Versionierung`, `### Korrektur 2026-09-02`). Der OTC-OBS-Endpunkt selbst bleibt die einzige noch offene Variable, sobald echte Credentials existieren (behandelt als Checkpoint-würdiger erster Versuch, nicht als Annahme).

**Pro-Maschine-`uv sync`-Falle gilt auch hier:** wie beim `cv`-Extra oben (siehe "Pro-Maschine-torch-Falle") muss `uv sync --extra versioning` auf jeder Maschine separat laufen, die DVC nutzt — es gibt keine automatische Übertragung des installierten Zustands zwischen Primärmaschine und Dell-Rechner.

## Grounding-DINO-Backend

RESEARCH Open Question 2 fragte, ob `autodistill-grounding-dino` auf der Primärmaschine sauber installiert. Das Ergebnis der Installationsprobe:

**Befund (wörtlich):** `autodistill` 0.1.29 installiert erfolgreich, aber `from autodistill.detection import CaptionOntology` schlägt fehl:

```
ModuleNotFoundError: No module named 'roboflow'
```

Ursache: `autodistill/detection/detection_base_model.py` importiert `roboflow` auf Modulebene, aber `roboflow` ist in den PyPI-Metadaten von `autodistill` 0.1.29 (`requires_dist`) **nicht** als Abhängigkeit deklariert — ein Paketierungs-Fehler stromaufwärts, keine lokale Fehlkonfiguration. Das betrifft nicht nur `autodistill-grounding-dino` spezifisch, sondern jede Nutzung von `autodistill.detection.CaptionOntology`, dem gesamten Orchestrierungs-Einstiegspunkt, den RESEARCH.md als Standard-Stack vorsah.

**Entscheidung (bindend für Plan 02.1-07, gemäß diesem Plan-Fallback):** `autodistill` und `autodistill-grounding-dino` werden aus der `cv`-Extras-Gruppe entfernt (siehe `pyproject.toml`-Kommentar). Stattdessen übernimmt die HuggingFace-`transformers`-Reimplementierung das Zero-Shot-Vorlabeling:

```python
from transformers import GroundingDinoForObjectDetection, AutoProcessor

CHECKPOINT = "IDEA-Research/grounding-dino-tiny"
model = GroundingDinoForObjectDetection.from_pretrained(CHECKPOINT)
processor = AutoProcessor.from_pretrained(CHECKPOINT)
```

Diese Variante ist reines PyTorch ohne custom CUDA-Op (RESEARCH RESEARCH.md "Alternatives Considered"), bereits mit `transformers>=5.15.1` in der `cv`-Gruppe abgedeckt — kein zusätzliches Paket nötig. Import verifiziert auf der Primärmaschine:

```
from transformers import GroundingDinoForObjectDetection  # -> ok
```

## OC-SORT-Tracker-Klasse

Die installierte `trackers`-Version (2.6.0) exportiert die OC-SORT-Implementierung als:

```python
from trackers import OCSORTTracker
```

(verifiziert per `hasattr(trackers, "OCSORTTracker")` auf der Primärmaschine — daneben u. a. `SORTTracker`, `ByteTrackTracker`, `BoTSORTTracker`, `McByteTracker` im selben Modul, hier nicht verwendet.)

## Bekannte, harmlose Warnung

Beim Import von `cv2` und `transformers`/`av` gemeinsam meldet macOS eine doppelte Objective-C-Klassenregistrierung (`AVFFrameReceiver`/`AVFAudioReceiver`, aus `cv2`s und `av`s jeweils gebündelter `libavdevice`-Version). Rein informativ — keine Laufzeitfehler in der Installationsprobe beobachtet, keine weitere Maßnahme nötig.

## CVAT

Selbst gehostetes CVAT (D-06) auf der Primärmaschine, ausschliesslich auf Loopback erreichbar
-- keine Vendoring des CVAT-Quellbaums in dieses Repository (T-2.1-22).

### Start

CVAT wurde ausserhalb dieses Repos nach `~/src/cvat` geklont, nicht als Vollklon, sondern als
partieller/sparse Klon (Begruendung siehe "Apple-Silicon-Realitaet" unten):

```bash
git clone --filter=blob:none --sparse --no-checkout --depth 1 \
  https://github.com/cvat-ai/cvat.git ~/src/cvat

git -C ~/src/cvat sparse-checkout set --no-cone \
  '/*.yml' '/*.yaml' '/*.md' '/*.env*' '/serverless'

git -C ~/src/cvat checkout develop
```

Das holt exakt die Docker-Compose-Dateien, das README und den `serverless/`-Baum (fuer den
SAM2-Versuch, siehe unten) -- nicht den vollen Monorepo-Quellbaum von `cvat-server`/`cvat-ui`,
deren Code ohnehin nicht aus dem Quelltext gebaut, sondern als vorgebaute Docker-Images
(`cvat/server`, `cvat/ui`) gezogen wird.

Der Standard-`docker-compose.yml` published Traefik (den einzigen extern erreichbaren Dienst)
ohne Host-IP (`8080:8080`, `8090:8090`), was sich zu `0.0.0.0` aufloest -- im gesamten LAN
erreichbar. Das Filmmaterial, das diese UI ausliefert, ist PII nach `docs/capture-legal.md`
(Gesichter der Spielerinnen); ein LAN-erreichbarer Annotationsserver ist eine unnoetige
Offenlegungsflaeche (T-2.1-05). Deshalb ein lokales Override, das die Ports auf Loopback bindet
(`docker-compose.override.yml` neben `docker-compose.yml`, automatisch von `docker compose up`
geladen, nicht Teil dieses Repos):

```yaml
# ~/src/cvat/docker-compose.override.yml
services:
  traefik:
    ports: !override
      - "127.0.0.1:8080:8080"
      - "127.0.0.1:8090:8090"
```

(`!override` ist noetig statt eines einfachen `ports:`-Overrides, weil Compose Listen wie
`ports:` standardmaessig ueber mehrere Dateien hinweg **verkettet**, nicht ersetzt -- ohne den
Tag stuende die `0.0.0.0`-Basiszeile weiterhin zusaetzlich im aufgeloesten Compose-Modell. Per
`docker compose config` verifiziert: nach dem Override enthaelt die aufgeloeste Konfiguration
ausschliesslich die beiden `127.0.0.1`-Eintraege, keinen `0.0.0.0`-Eintrag mehr.)

Start- und Admin-Befehle (Standard-CVAT-Ablauf, `~/src/cvat/README.md`):

```bash
docker compose --project-directory ~/src/cvat \
  -f ~/src/cvat/docker-compose.yml -f ~/src/cvat/docker-compose.override.yml up -d
docker exec -it cvat_server bash -ic 'python3 ~/manage.py createsuperuser'
```

**Status dieser Ausfuehrung:** Der Compose-Aufbau (Klon, Sparse-Checkout, Override-Datei) ist
fertig und per `docker compose config` gegen Syntaxfehler und die Loopback-Bindung verifiziert.
Das tatsaechliche Hochfahren der Container (`docker compose up -d`, Image-Pull, Superuser
anlegen, Live-`curl`-Verifikation gegen `127.0.0.1:8080`) konnte in dieser automatisierten
Ausfuehrungs-Session **nicht** abgeschlossen werden -- Grund siehe naechster Abschnitt. Die
obigen Befehle sind copy-paste-fertig fuer eine Session mit normaler Bandbreite.

### Apple-Silicon-Realitaet

Primaermaschine weiterhin Apple M4 Max (Namensabweichung zu "M5 Max" siehe oben), Docker
Desktop 29.5.3, Docker Compose v5.1.4 (Docker Desktop, nicht der alte `docker-compose`-Standalone).

**Docker Desktop mit korruptem VM-Zustand:** Vor dem ersten Start dieser Session verweigerte
der Docker-Desktop-Daemon den Start mit einem intern gespeicherten, offensichtlich korrupten
Datenträger-Groessenwert:

```
running engines: starting engine: engine linux/virtualization-framework failed to start:
ensuring disk: cannot resize ".../Docker.raw" to 1422465MiB: truncate ...: permission denied
```

(1422465 MiB ~= 1,4 PB -- kein plausibler Zielwert, ein Altzustand aus einer frueheren Sitzung).
Docker Desktop hat daraufhin selbststaendig einen "Reset to factory defaults" ausgeloest
(Application-Support-Verzeichnis und VM-Datentraeger geloescht) und ist danach sauber
gestartet. Kein CVAT-spezifischer Fehler, aber wörtlich festgehalten, weil es in dieser Session
tatsaechlich passiert ist, bevor irgendein Compose-Befehl lief.

**Netzwerk-Durchsatz in dieser Ausfuehrungsumgebung:** Nach dem Neustart war der Daemon
funktionsfaehig, aber der beobachtete Netzwerkdurchsatz zu GitHub und Docker Hub aus dieser
automatisierten Ausfuehrungs-Session war extrem gering:

- `curl` zu `github.com`: ~24-36 KB/s gemessen (mehrere Messungen, `-w '%{speed_download}'`)
- `docker pull alpine:latest` (ein ~3,6-MB-Image): 2 Minuten 29 Sekunden fuer den einzigen Layer

Der volle CVAT-Stack zieht rund zehn Images (`postgres`, `redis`, `apache/kvrocks`,
`cvat/server` -- das amd64-only-Image aus RESEARCH Pitfall 1, per Rosetta/QEMU-Emulation auf
Apple Silicon --, `cvat/ui`, `traefik`, `openpolicyagent/opa`, `clickhouse-server`,
`timberio/vector`, `grafana-oss`), zusammen deutlich im Gigabyte-Bereich. Bei der gemessenen
Rate waere das Nachladen dieser Images allein zehn bis dreissig Stunden -- innerhalb dieser
Session nicht time-boxbar. Statt das (RESEARCH Pitfall 1's eigene Anweisung: "budget real setup
time, but do not keep fighting it") zu erzwingen, wird das hier als Umgebungs-Limitation dieser
konkreten Ausfuehrungs-Session festgehalten, nicht als generelles Problem der Internetanbindung
des Nutzers -- die vorherigen Plaene (02.1-01, `uv sync --extra cv`, mehrere Gigabyte an
PyPI-Paketen inkl. `torch`) liefen in einer anderen Sitzung ohne diese Einschraenkung durch.
Rosetta/QEMU-Emulationsverhalten des `cvat/server`-Images selbst (Restart-Loops,
Startup-Zeiten) konnte dadurch in dieser Session nicht beobachtet werden -- das ist die
tatsaechliche, unvermeidliche Luecke: nicht erfunden, sondern ehrlich als "nicht getestet"
markiert.

### SAM2-Status

**Nicht deployt, Fallback in Kraft.** Zwei unabhaengige Gruende, beide vor jedem Image-Pull
durch Lesen von `~/src/cvat/README.md` und `~/src/cvat/serverless/` (sparse ausgecheckt, siehe
oben) ermittelt -- der zweite Grund war so nicht in RESEARCH.md vorausgesehen:

1. **Netzwerk (siehe oben):** der CVAT-Stack selbst (inkl. der Nuclio-Serverless-Infrastruktur,
   `docker-compose.yml -f components/serverless/docker-compose.serverless.yml`) konnte in dieser
   Session nicht hochgefahren werden -- ohne laufenden Stack gibt es nichts, wohin `nuctl deploy`
   deployen koennte.
2. **SAM2 ist in dieser CVAT-Version gar nicht Teil der Serverless-Funktionsgalerie.** Die
   offizielle Modelltabelle in `~/src/cvat/README.md` (Stand `develop`-Branch, heute geklont)
   listet `facebookresearch/sam` (Segment Anything, **v1** -- punkt-/box-basierter Interactor)
   als verfuegbare Funktion, aber **keine `sam2`-Funktion**. Der Verzeichnisbaum
   `serverless/pytorch/facebookresearch/` bestaetigt das: er enthaelt `sam/` und `detectron2/`,
   kein `sam2/`. Es gibt in dieser CVAT-Version schlicht keine mitgelieferte SAM2-Nuclio-Funktion
   zum Deployen -- eine funktionierende SAM2-Integration muesste zunaechst aus einem
   Community-Beitrag von Grund auf gebaut werden, was RESEARCH Pitfall 1's
   Zeitbudget-Warnung ("this is not a 10-minute docker compose up") bereits fuer den
   guenstigeren Fall (SAM2 existiert, deployt aber fehlerhaft) aussprach.

**Fallback (bindend, durch RESEARCH Pitfall 1 vorautorisiert):** SAM2-Propagierung laeuft, falls
ueberhaupt gewuenscht, als eigenstaendiges Python-Skript ausserhalb von Docker (CPU- oder
MPS-Fallback, RESEARCH Pitfall 3), das vorpropagierte Masken/Boxen direkt im
CVAT/COCO-importierbaren Format schreibt. CVAT selbst dient ausschliesslich als
Korrektur-UI (manuelle Box-Korrektur der Grounding-DINO-Vorlabels aus Plan 02.1-07, optional
SAM2-vorpropagiert). Das erfuellt weiterhin D-06 ("footage never leaves the user's machines"),
da sowohl das Skript als auch CVAT lokal laufen.

### Zugangsdaten

CVAT-Zugangsdaten werden ausschliesslich ueber Umgebungsvariablen aufgeloest (nie ueber eine
fest codierte URL oder ein fest codiertes Passwort, siehe `flag_football_ep.config.secret()`):

- `CVAT_USERNAME` -- der beim `createsuperuser`-Schritt angelegte Benutzername
- `CVAT_PASSWORD` -- das zugehoerige Passwort

Beide Namen (nie Werte) stehen in `.env.example`; echte Werte gehoeren ausschliesslich in die
git-ignorierte `.env`. `ffep.toml`s `[cv]`-Tabelle referenziert nur die Variablennamen
(`cvat_username_env = "CVAT_USERNAME"`, `cvat_password_env = "CVAT_PASSWORD"`), nie einen Wert.

### Abbau

Stack stoppen und alle Volumes entfernen (Datenbank, hochgeladene Frames, Keys, Logs -- alles,
was ein no-go die Entscheidung sauber rueckgaengig machen soll):

```bash
docker compose --project-directory ~/src/cvat \
  -f ~/src/cvat/docker-compose.yml -f ~/src/cvat/docker-compose.override.yml down -v
```

Named Volumes (`cvat_db`, `cvat_data`, `cvat_keys`, `cvat_logs`, `cvat_inmem_db`,
`cvat_events_db`, `cvat_cache_db`) leben unter Docker Desktops interner VM-Disk, nicht im
Projektbaum -- `down -v` entfernt sie vollstaendig. Den geklonten Compose-Baum selbst entfernt
`rm -rf ~/src/cvat` (er liegt, wie oben beschrieben, ausserhalb dieses Repos und war nie Teil
der Versionskontrolle hier).

### Datensatz

Der korrigierte Trainingsdatensatz aus Plan 02.1-09 -- Grounding-DINO-Vorlabels (Plan 02.1-07),
von Hand in CVAT korrigiert, per `ffep cv dataset` gegen das Sample-Manifest validiert
(`cv/dataset.py::validate_coco`) und mit einem reproduzierbaren Inhalts-Hash versehen
(`cv/dataset.py::dataset_hash`).

**CVAT-Task:** id `1`, Name `pilot-2026-05-16`, 404 gepushte Frames (Sitzung
`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`).

**Korrigierter Bereich:** Frames 1-304 von 404 (CVAT-Frame-Reihenfolge = Push-/Manifest-
Reihenfolge). Frames 305-404 wurden **nicht** korrigiert -- sie tragen weiterhin nur die
rohen Grounding-DINO-Vorlabels aus Plan 02.1-07 und sind aus dem Trainingsdatensatz
ausgeschlossen (siehe "Trim auf 304 Frames" unten).

**Export-Datum:** 2026-08-28

**Datensatz-Kennzahlen** (nach Trim, `ffep cv dataset --coco
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/corrected --manifest
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/manifest.json`):

| Kennzahl | Wert |
|---|---|
| `n_images` | 304 |
| `player`-Boxen | 5962 |
| `referee`-Boxen | 652 |
| Bilder ohne Annotation (`_empty_images`) | 0 |
| Split `train` | 239 Bilder |
| Split `val` | 65 Bilder |
| `content_sha256` | `ab3a9673d61bc348d37ce298ba12d18b76395d1ade82a735c5b3d82d2e46aec0` |

Beide Splits tragen mindestens eine `player`-Box (von `validate_coco` erzwungen). 304 Bilder
liegen innerhalb des von REQ-S2-02/D-06 erzwungenen Bands `[250, 600]`.

**Hover-Positionen im Datensatz** (`data/reference/hover_positions.csv`, Plan 02.1-03):

| Hover-Position | Frames | Clips | davon `train` | davon `val` |
|---|---|---|---|---|
| hp-01 | 164 | 24 | 138 | 26 |
| hp-02 | 140 | 22 | 101 | 39 |

Beide der zwei bestätigten Hover-Positionen sind im 304-Frame-Subset gut vertreten (54 % / 46 %,
nah am ursprünglichen 61-Clip-Verhältnis 30/31) -- der Trim auf die ersten 304 Frames hat keine
Hover-Position kollabieren lassen, weil `sample_training_frames` (Plan 02.1-07) die Clips
zwischen den Hover-Positionen interleaved statt sie block-weise anzuordnen.

**Rueckverfolgbarkeit (D-15):** `content_sha256` ist der Wert, den Plan 02.1-10s
MLflow-Trainingslauf als Parameter loggt -- jeder registrierte Detector laesst sich damit exakt
auf diesen Label-Stand zurueckverfolgen, nicht nur auf "irgendeine Version von `corrected/`".

**Labeling-Konvention** (vom Nutzer in dieser Sitzung angewandt, bindend fuer Plan 02.1-15s
Ground-Truth-Labeling zur Konsistenz):

- Jede klar sichtbare Person wird geboxt.
- Nur Personen mit einer aktiven Schiedsrichterrolle auf dem Feld erhalten das Label `referee`.
- **Alle** anderen Personen -- inklusive Trainerstab, Ersatzspielerinnen und Seitenlinien-
  Personal -- erhalten `player`, nicht etwa eine dritte Klasse oder werden ausgelassen. Die
  raeumliche Filterung (wer tatsaechlich auf dem Feld ist) passiert stromabwaerts in
  Feldkoordinaten (Plan 02.1-11ff.), nicht bereits beim Boxen.
- Boxen umschliessen den vollstaendig sichtbaren Koerper inklusive Gliedmassen.
- Die Boxen-Unterkante sitzt eng an den Fuessen, der Schatten wird ausgeschlossen -- Begruendung:
  der Fusspunkt ist der Punkt, der spaeter per Homographie in Feldkoordinaten projiziert wird
  (Plan 02.1-13), ein lockerer Unterkanten-Rand oder ein eingeschlossener Schatten wuerde direkt
  in einen Positionsfehler uebersetzen.
- Frames bis ca. Frame 103 wurden als Polygon annotiert (CVAT leitet daraus die exportierte Box
  ueber die Koordinaten-Extrema ab); spaetere Frames direkt als Rechteck. Das erklaert, warum
  `validate_coco`s Bounds-Check eine Sub-Pixel-Toleranz (`_BBOX_BOUNDS_EPSILON_PX = 1.0`, siehe
  `cv/dataset.py`) braucht: die Polygon-zu-Box-Ableitung landet bei am Bildrand geboxten Personen
  gelegentlich um Bruchteile eines Pixels ausserhalb des Frames (gemessen bis 0.26px auf diesem
  Datensatz) -- ein Rundungsartefakt der Ableitung, kein Labeling-Fehler.

**Trim auf 304 Frames (Abweichung vom Plan, siehe SUMMARY):** Die Labeling-Sitzung wurde nach
304 von 404 Frames als vollstaendig markiert und in CVAT gespeichert; die Begruendung des
Nutzers war Qualitaet vor Quantitaet -- nach 304 sorgfaeltig korrigierten Frames war der
Ermuedungspunkt erreicht, an dem eine erzwungene Fortsetzung eher Korrekturfehler eingefuehrt
haette als zusaetzlichen Trainingswert zu liefern. Die verbleibenden 100 Frames (305-404) tragen
weiterhin nur die unkorrigierten Grounding-DINO-Vorlabels aus Plan 02.1-07 und wurden vor dem
`ffep cv dataset`-Lauf aus `instances.json` und dem Sample-Manifest entfernt (Annotation-IDs
neu durchnummeriert fuer Konsistenz) -- sie duerfen nicht ins Training einfliessen. Der volle
404-Frame-Export bleibt als `instances.full-404.json` neben `instances.json` erhalten
(git-ignoriert), falls die restlichen 100 Frames in einer spaeteren Sitzung nachkorrigiert
werden sollen. 304 liegt weiterhin innerhalb des REQ-S2-02/D-06-Bands `[250, 600]`.

**Ehrliche Einschraenkung:** Diese Boxen stammen von einer einzelnen Annotatorin/einem
einzelnen Annotator auf dem Material eines einzigen Spiels (GER vs. Panama Rojo,
2026-05-16) -- es gibt keine Zweit-Annotator-Uebereinstimmungsmessung (Inter-Annotator
Agreement). Das ist eine bekannte Grenze dieses Piloten, keine versteckte: bei einem
Solo-Entwickler-Projekt mit einer Annotationsperson ist eine IAA-Messung nicht budgetiert:
diese Kennzahlen und der Content-Hash belegen Konsistenz-mit-sich-selbst und
Rueckverfolgbarkeit, nicht Inter-Annotator-Zuverlaessigkeit.

**Nachfolger (Phase 2.2, Plan 02.2-13, korrigiert 2026-09-02):** Die 304-Frame-Piloten-Datenmenge
oben bleibt als historischer Beleg unveraendert stehen, fliesst aber gemaess dem
Seed-Set-Verdikt `nicht uebernommen` (`docs/dataset-plan.md` `## 6`) **nicht** in den
wachsenden Multi-Domaenen-Datensatz ein. Der laufende, DVC-getrackte Nachfolge-Datensatz liegt
unter `data/labels/dataset/` (Drohne/GoPro-Hinterfeld/TV-Broadcast, 558 Bilder nach
AL-Iteration 1 und der D-17-Korrektur, die 181 ungeprueft-vorgelabelte GoPro-Frames wieder
ausgeschlossen hat -- Datensatz v1.1, nicht mehr der 739-Bilder-Stand v1) -- vollstaendiger
Ausfuehrungsnachweis inkl. Domaenen-Aufschluesselung, DVC-MD5 und `content_sha256` in
`docs/dataset-buildout.md` (`## Iteration 1` -> `### Merge & Validierung`,
`### DVC-Versionierung`, `### Korrektur 2026-09-02`). Die hier dokumentierte
Labeling-Konvention (Boxen-Vollstaendigkeit, `referee`-nur-fuer-aktive-Schiedsrichter,
Fusspunkt-Unterkante) gilt fuer den Nachfolge-Datensatz unveraendert weiter; die einzige
domaenenspezifische Ergaenzung -- GoPro/Hinterfeld-Fernfeld wird bewusst uebersprungen statt
korrigiert -- ist in `docs/dataset-buildout.md`s Nachtrag vom 2026-09-02 festgehalten, nicht
hier, da sie eine Iteration-1-Ausfuehrungsentscheidung ist, keine Aenderung der Konvention
selbst.

## Detector-Training

RF-DETR-Small Fine-Tune auf dem korrigierten 304-Frame-Datensatz (`### Datensatz` oben),
`src/flag_football_ep/cv/detect.py::train_detector`, Plan 02.1-10.

**Maschine: Primärmaschine (Apple M4 Max, MPS) statt Dell-CUDA-Box (D-05) -- vom Nutzer
für diese Ausführung ausdrücklich autorisierte Abweichung, 2026-08-28.** D-05 sieht den
Dell-Rechner (8 GB CUDA) als Trainingsmaschine vor; für diesen Lauf wurde stattdessen
direkt auf der Primärmaschine trainiert (`--device mps`), weil sie ohnehin am Stück
verfügbar war und das Fine-Tune nachweislich MPS-tauglich ist (siehe unten). Die
Dell-/Colab-Pfade bleiben unverändert dokumentiert und einsatzbereit (`ffep cv train
--no-register --device cuda`, siehe Task 2 dieses Plans) -- diese Ausführung ersetzt sie
nicht als Standardweg, sondern zeigt einen zusätzlichen, funktionierenden Fallback.
`PYTORCH_ENABLE_MPS_FALLBACK=1` wurde vorsorglich gesetzt (RESEARCH Pitfall 3); im
tatsächlichen Lauf war kein einziger CPU-Fallback-Hinweis für einen nicht unterstützten
MPS-Operator zu beobachten -- RF-DETRs DINOv2-Backbone lief durchgehend nativ auf MPS.

**Chunked/resumable statt eines einzelnen Laufs:** Ein einzelner `ffep cv train`-Aufruf
für alle 30 Epochen hätte das Werkzeug-Zeitlimit dieser Ausführungsumgebung (10 Minuten
pro Kommando) klar überschritten. `train_detector`/`ffep cv train` wurden um ein
`--resume`-Flag erweitert (`rfdetr`s eigenes `TrainConfig.resume`, ein vollständiger
PyTorch-Lightning-Checkpoint inkl. Optimizer-/LR-Scheduler-Zustand, geschrieben als
`last.ckpt` bei jeder Epoche) -- der Lauf wurde in sechs Fünf-Epochen-Abschnitten
ausgeführt (Ziel-Epochenzahl bleibt bei jedem Aufruf `30`, PyTorch Lightning setzt einfach
bei `current_epoch` aus dem Checkpoint fort). Dieselbe Mechanik macht den Lauf auch gegen
einen Schlafzustand der Maschine robust (bekanntes Risiko dieser Umgebung), da jeder
Abschnitt sauber am Epochenende endet, nie mitten in einer Epoche abgebrochen wird.

**Resolved Settings** (`ffep.toml [cv]`, unverändert übernommen -- keine MPS-bedingte
Anpassung nötig):

| Setting | Wert |
|---|---|
| `resolution` | 896 |
| `epochs` | 30 (Gesamtziel, über 6 Abschnitte à 5 Epochen erreicht) |
| `batch_size` | 4 |
| `grad_accum_steps` | 4 |
| effektive Batchgröße | 16 (`batch_size * grad_accum_steps`, identisch zum geplanten Wert) |
| `device` | `mps` |
| `dataset_content_sha256` | `ab3a9673d61bc348d37ce298ba12d18b76395d1ade82a735c5b3d82d2e46aec0` (== `### Datensatz` oben) |
| `torch_version` | 2.13.0 |
| `cuda_available` | `false` (erwartet -- kein CUDA auf dieser Maschine) |

**Wall-Clock:** ~40 Minuten 35 Sekunden reine Trainingszeit über die 6 Abschnitte
(2026-08-28T22:10:46Z bis 2026-08-28T22:51:58Z, jeder Abschnitt inkl. Datensatz-Neuaufbau,
Modell-Init und einer vollen Val-Evaluation am Ende), plus ~1 Minute für den separaten
Registrierungsschritt (`ffep cv train --from-artifacts`, ohne erneutes Training).

**Validierungsmetriken** (Split `val`: 65 Bilder aus 10 Clips, siehe `### Datensatz`
oben für den vollen Split-Kontext):

| Klasse | AP50 | AP50-95 |
|---|---|---|
| Gesamt (`mAP`) | 0.9571 | 0.8112 |
| `player` | -- (RF-DETRs Callback liefert je Klasse nur den über IoU 0.5:0.95 gemittelten AP, keinen separaten AP50) | 0.8266 |
| `referee` | -- (s.o.) | 0.7958 |

Die installierte `rfdetr==1.9.3`-Trainingsstack (`COCOEvalCallback`) berechnet den
Gesamt-AP50 und Gesamt-AP50-95 getrennt, aber pro Klasse nur einen einzigen,
COCO-typisch über IoU 0.5:0.95 gemittelten AP-Wert (`val/AP/<class>`) -- kein separater
Pro-Klasse-AP50 existiert in der installierten Bibliothek. `train_detector` meldet
deshalb genau das, was der Trainer tatsächlich liefert, statt mit einer zweiten
mAP-Implementierung (`supervision`) einen Pro-Klasse-AP50 nachzubauen, der mit der
offiziellen COCO-Auswertung uneins sein könnte.

**MLflow-Run:** `87a8a5222f7a472787875e974d089c44` (Experiment `cv_detector` ==
`cfg.cv.detector_experiment`), registriertes Modell `cv_detector_model`, **Version 1**.
(Die Registrierung lief zweimal über denselben `--from-artifacts`-Pfad: der erste Lauf
landete im kurzlebigen MLflow-Store des Executor-Worktrees und wurde mit ihm verworfen;
massgeblich ist ausschliesslich der hier genannte Run im persistenten Store.)

**Promotion:** Auf `champion` promotet (`ffep cv promote --run
87a8a5222f7a472787875e974d089c44`, Version 1) -- Entscheidung des Orchestrators, nicht
eigenmächtig durch die Trainings-Task selbst: einziger registrierter Kandidat, objektiv
starke Metriken (`mAP_50=0.9571`, `mAP_50_95=0.8112`), ein MLflow-Alias ist trivial
reversibel (`ffep cv promote --run <anderer-run>` verschiebt ihn jederzeit neu, keine
Version wird dabei gelöscht), und der Nutzer hatte für diese Nacht-Session explizit
mechanische Weiterarbeit ohne Rückfragen an menschlichen Gates angewiesen. `resolve_champion`
verifiziert nach der Promotion: löst auf `87a8a5222f7a472787875e974d089c44` auf.

**Statistische Ehrlichkeit:** Der Val-Split besteht aus einer Handvoll Clips (10 von 46
im 304-Frame-Subset) eines einzigen Spiels, gelabelt von einer einzigen Annotationsperson
(siehe `### Datensatz`s eigene IAA-Einschränkung oben) -- diese Zahlen beschreiben die
Anpassung an genau dieses Piloten-Regime, keine allgemeine Leistungsfähigkeit. Der
niedrigere `referee`-AP ist bei der Klassenhäufigkeit (652 `referee`- vs. 5962
`player`-Boxen im Gesamtdatensatz) erwartet und akzeptabel -- weniger Trainingsbeispiele
für diese Klasse, nicht ein Modellfehler. Die eigentlichen Gate-Kriterien (C-09) sind
Tracking-Kontinuität, Positionsfehler und Inferenzzeit -- **nicht** mAP; dieser Abschnitt
ist Kontext für die Gate-Entscheidung (Plan 02.1-17), kein Gate-Kriterium selbst.

## Hackathon-Freeze (D-05)

`src/flag_football_ep/cv/freeze.py`, Plan 02.2-07. Macht die Hackathon-Baseline zu einem
bewussten, versionierten Artefakt statt "was `champion` gerade zufällig am Build-Tag
auflöst" (RESEARCH Pitfall 5, Phase 02.2) -- Strand 2 dieser Phase (aktives Nachtrainieren
gegen den Champion-Alias) und der Hackathon-Bundle-Build laufen in derselben Phase, ohne
expliziten Freeze würde der Detektor unter den Teilnehmern jederzeit unbemerkt wechseln.

**Zwei getrennte MLflow-Aliase auf `cv_detector_model`:**

| Alias | Bewegt sich | Zweck |
|---|---|---|
| `champion` (`cv.registry.CHAMPION_ALIAS`) | Bei jeder Active-Learning-Nachtrainings-Iteration dieser Phase | Interner Rolling-Stand, den `cv.detect.load_detector` standardmässig auflöst |
| `hackathon-frozen` (`cv.freeze.FROZEN_ALIAS`) | Nur durch einen expliziten erneuten `ffep cv freeze`-Aufruf | Der Stand, gegen den die Hackathon-Bundles (`ffep cv bundle`) gebaut werden |

**Gefrorener Lauf:** `87a8a5222f7a472787875e974d089c44` (`cv_detector_model` Version 1,
derselbe Phase-2.1-Champion-Lauf aus `## Detector-Training` oben) -- der Detektor, dessen
Metriken (`mAP_50=0.9571`, `mAP_50_95=0.8112`) die Challenge-Beschreibung zitiert. Ein
erneuter Freeze auf einen anderen Lauf ist bewusst kein einfacher Retry: `write_freeze_pin`
verweigert das stille Überschreiben einer bestehenden Pin-Datei mit einem anderen `run_id`
(`FreezeError`); `ffep cv freeze --force` löscht die bestehende Pin-Datei zuerst, wenn ein
Re-Freeze tatsächlich gewollt ist.

**Pin-Datei:** `data/reference/hackathon_freeze.json` (getrackt, keine PII -- nur
Identifier und Hashes, siehe Threat-Register T-2.2-21) statt des git-ignorierten
`data/processed/`, damit Bundle-Reproduzierbarkeit einen sauberen Checkout übersteht.
Felder: `run_id`, `dataset_hash` (== `content_sha256` aus `### Datensatz` oben,
`ab3a9673d61bc348d37ce298ba12d18b76395d1ade82a735c5b3d82d2e46aec0`), `model_version`,
`frozen_at`.

**Operationale Regel:** `cv/bundle.py::build_bundle` liest ausschliesslich
`read_freeze_pin`/`resolve_frozen` -- niemals `cv.registry.resolve_champion` direkt. Ein
mechanischer Source-Gate (`tests/test_cv_freeze.py`) prüft, dass `bundle.py` keinen
`resolve_champion`-Verweis enthält, damit eine spätere Änderung den Bundle-Builder nicht
still auf den rollenden Champion umlenkt.

**Ehrliche Einschränkung (Ausführung in einem isolierten Worktree):** Der eigentliche
`ffep cv freeze --run 87a8a5222f7a472787875e974d089c44 --dataset-hash
ab3a9673d61bc348d37ce298ba12d18b76395d1ade82a735c5b3d82d2e46aec0`-Aufruf gegen den
persistenten MLflow-Store (`mlruns/`, git-ignoriert) konnte in diesem Ausführungs-Worktree
nicht laufen -- `mlruns/` existiert nur im Haupt-Checkout, nicht in der isolierten
Worktree-Kopie (verifiziert: ein Live-Versuch schlägt sauber mit `FreezeError: run ... has
no registered version` fehl, statt still falsche Daten zu schreiben). Die oben getrackte
Pin-Datei enthält deshalb die aus diesem Dokument übernommenen, bereits verifizierten Werte
(Run-ID, Dataset-Hash, Modellversion); der tatsächliche MLflow-Alias-Set-Aufruf muss einmalig
im Haupt-Checkout nachgeholt werden, damit `resolve_frozen("cv_detector_model", cfg)` dort
`87a8a5222f7a472787875e974d089c44` zurückgibt (siehe SUMMARY dieses Plans).

## Inferenz-Durchsatz

`src/flag_football_ep/cv/detect.py::detect_video` + `src/flag_football_ep/cv/benchmark.py::
extrapolate_game_runtime`, Plan 02.1-11. Misst den tatsächlichen Detektions-Durchsatz auf der
Primärmaschine (D-11 verlangt genau das -- Dell-/Colab-Zahlen sind nur Referenz) und
beantwortet die in Plan 02.1-03 offen gelassene SAHI-Frage mit gemessenen statt
angenommenen Werten (RESEARCH Pitfall 4: SAHI darf nicht per Default aktiviert werden).

**Maschine:** Primärmaschine (Apple M4 Max, siehe `## Hardware-Hinweis` oben),
`platform.node()` = `MacBook-Pro-2.fritz.box`. Der champion-aufgelöste Detektor lädt
automatisch auf `mps` (`torch.backends.mps.is_available() -> True`, kein expliziter
Geräte-Parameter in `detect_video`/`load_detector` -- die Geräteauswahl liegt bei
`RFDETRSmall` selbst).

**Methodik:** Drei Clips aus `data/reference/hover_positions.csv` gewählt, um die
gemessene Spannweite der scheinbaren Spielergröße (`apparent_player_px_p50`)
abzudecken -- kleinstes p50, Median-p50, größtes p50:

| Clip | `apparent_player_px_p50` | Rolle |
|---|---|---|
| 001 | 25.0 px | kleinstes p50 |
| 052 | 30.0 px | Median-p50 |
| 003 | 61.0 px | größtes p50 |

Jeder Clip einmal mit `sahi=false` und einmal mit `sahi=true`, jeweils bei der
freigegebenen `[cv] resolution = 896` (Plan 02.1-03) durchlaufen (volle Clip-Länge, kein
Frame-Subsampling); Detektionszahlen und `StageTiming` (`decode`/`detect`/`postprocess`)
über alle drei Clips je Modus aufsummiert und durch `extrapolate_game_runtime(...,
footage_seconds=954/30.0, game_seconds=3000.0)` (50 min) geschickt:

| Modus | Auflösung | s pro Footage-Sekunde | Extrapolation auf 50 min | Erkannte Boxen pro Frame |
|---|---|---|---|---|
| `sahi=false` | 896 | 0.79 | 39.73 min | 20.1 |
| `sahi=true` | 896 | 5.21 | 260.67 min | 32.4 |

**Entscheidung: `[cv] sahi = false` bleibt unverändert.** Volle-Bild-Inferenz erkennt
bereits reichlich Boxen (~20 pro Frame über alle drei Clips, bei durchweg *Brauchbar*
eingestuften Größen) und bleibt mit 39.73 von 60 erlaubten Minuten deutlich im C-09-Budget,
während `sahi=true` das Budget um mehr als das Vierfache überschreitet (260.67 min) für nur
gut die anderthalbfache Boxzahl pro Frame -- ein Hinweis auf zusätzliche Tile-Seam-Duplikate
statt proportional besserer Recall (RESEARCH Pitfall 4s dokumentierte Tuning-Lücke), nicht
auf einen entscheidenden Recall-Gewinn, der die Kostenexplosion rechtfertigen würde.
`ffep.toml`s `[cv] sahi = false` war bereits so gesetzt (Plan 02.1-03) und bleibt damit
konsistent mit dieser Messung -- keine Konfigurationsänderung nötig.

**Das ist nicht die Gate-Messung.** Dies ist eine Drei-Clip-Durchsatzstichprobe, die den
vollständigen 61-Clip-Tracking-Lauf aus Plan 02.1-12 de-riskt; die tatsächliche C-09-Zahl
kommt aus diesem vollständigen Lauf und wird in der Gate-Entscheidung (Plan 02.1-17)
festgehalten -- diese Tabelle darf dort nicht als Gate-Ergebnis zitiert werden.

## Tracking-Lauf

`src/flag_football_ep/cv/track.py::track_session` + `src/flag_football_ep/cv/teams.py::
assign_teams`, Plan 02.1-12 (v1), Gap-Fix-Iteration nach Plan 02.1-12/vor Plan 02.1-14
(v2, BoT-SORT + Torso-Crop). Der vollständige Tracking-Lauf über alle 61 Pilot-Clips --
das ist die reale C-09-Messung, die die Drei-Clip-Stichprobe aus `## Inferenz-Durchsatz`
oben ausdrücklich vertagt hat.

**v1 ist die ursprüngliche OC-SORT-/Vollkörper-Crop-Messung aus Plan 02.1-12, unten in
`### v1` archiviert. Nach dem menschlichen Kontinuitäts-Review wurden zwei Gap-Fixes
gemergt (`faf75dd` -- Tracker-Wechsel zu getuntem BoT-SORT; `b870a72` -- Torso-Region-Crops
für die Team-Zuordnung), und der volle Lauf wurde als `### v2` neu gemessen. Beide Läufe
bleiben hier nebeneinander erhalten; `### v2` ist der aktuelle Stand, den Plan 02.1-17s
Gate-Entscheid zitiert.**

### v1 (OC-SORT-Tracker, Vollkörper-Crops -- Plan 02.1-12)

**Datum:** 2026-08-29
**Maschine:** Primärmaschine (Apple M4 Max), `platform.node()` = `MacBook-Pro-2.fritz.box`
**Detector Run ID:** `87a8a5222f7a472787875e974d089c44` (champion-Alias von `cv_detector_model`)
**Settings:** `resolution=896`, `sahi=false` (unverändert aus `ffep.toml`s `[cv]`-Tabelle,
Plan 02.1-03/02.1-11)

**Methodik-Hinweis (Ausführung):** `track_session` selbst lief unverändert und ungekürzt --
aber statt eines einzelnen `ffep cv track --session ...`-Aufrufs wurde der Lauf über vier
Bash-Tool-Aufrufe verteilt (Clip-Bereiche 1-16 / 17-32 / 33-47 / 48-61, nach kumulativer
Clip-Dauer balanciert), weil ein einzelner Prozess das 10-Minuten-Limit eines
Ausführungsschritts hätte reißen können. Jeder Teil-Lauf ist ein echter,
unveränderter `track_session`-Aufruf gegen eine Teilmenge der Clips (`frames.clip_paths`
prozesslokal auf die jeweilige Clip-Nummern-Menge eingeschränkt, kein Code im Repository
geändert); die vier Teil-Parquets wurden anschließend zu einer Session-Tabelle
zusammengeführt. Jeder Clip durchlief exakt dieselbe Pro-Clip-`try`/`except`-Absicherung
wie bei einem einzelnen durchgehenden Lauf.

**Ergebnis (Tracking):**

| Metrik | Wert |
|---|---|
| Clips verarbeitet | 61 / 61 |
| Clips mit Hinweis (Notice) | 0 |
| Clips mit null Tracks | 0 |
| Distinkte Tracks (Session) | 2031 (1874 `player`, 157 `referee`) |
| Wall-Clock (Summe der 4 Teil-Läufe) | 542.9 s (~9.05 min) |
| Output-Parquet | `data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet` |
| Zeilen im Output-Parquet | 341.461 |

**Pro-Stage-Sekunden (aufsummiert über alle 61 Clips):**

| Stage | Sekunden | Realtime-Faktor (s / 664.41 s Footage) |
|---|---|---|
| `decode` | 29.21 | 0.0440x |
| `detect` | 473.00 | 0.7119x |
| `track` | 11.03 | 0.0166x |
| `write` | 0.09 | 0.0001x |

**C-09-Extrapolation (die reale Gate-Zahl, nicht mehr die Drei-Clip-Schätzung):** über
`cv.benchmark.extrapolate_game_runtime` mit den obigen vier Stage-Summen,
`footage_seconds=664.41` (Summe aller 61 `duration_seconds` aus `video_inventory.csv`) und
`game_seconds=3000.0` (50-min-Default, D-11 Annahme 3, unverändert aus Plan 02.1-11):

**38.63 min** extrapolierte Laufzeit für ein 50-minütiges Spiel -- deutlich innerhalb des
60-Minuten-C-09-Budgets, konsistent mit der Drei-Clip-Schätzung aus `## Inferenz-Durchsatz`
(39.73 min). Vollständige Formel:

```
[machine=MacBook-Pro-2.fritz.box] linear extrapolation (assumption 1):
total real-time factor = decode(29.211s / 664.41s footage = 0.043966x realtime)
  + detect(473.003s / 664.41s footage = 0.711915x realtime)
  + track(11.029s / 664.41s footage = 0.016600x realtime)
  + write(0.087s / 664.41s footage = 0.000132x realtime)
  = 0.772611x realtime;
extrapolated game duration = 0.772611x * 3000.0s game (continuous-game denominator,
  assumption 2) = 2317.83s = 38.63 min
```

#### Hinweise (v1)

Keine -- alle 61 Clips wurden erfolgreich verarbeitet, kein Clip löste eine Ausnahme aus,
kein Clip lieferte null Tracks, und kein Clip wich um mehr als zwei Frames von der in
`video_inventory.csv` deklarierten Dauer ab.

**Team-Zuordnung (`assign_teams`):** Ein `TeamClassifier` wurde einmal für die gesamte
Session gefittet (SigLIP `google/siglip-base-patch16-224`, UMAP, KMeans), gespeist aus je
bis zu 10 gleichmäßig über die Track-Frames verteilten Crops pro Track (17.626 Crops
insgesamt, 1874 `player`-Tracks -- `referee`-Tracks nie eingespeist). Von den 1874
`player`-Tracks erhielten **1864 (99.47 %)** eine `team_id`; 10 Tracks fielen unter die
0.6-Mehrheitsschwelle und blieben `null` (siehe `#### Team-Zuordnung: Ambivalente Tracks (v1)`
unten). Cluster-Label 0/1 ist arbiträr -- welches Label welches reale Team ist, wird erst
in Plan 02.1-16 anhand des Radar-Reels von Hand festgelegt.

Bei der Vorbereitung dieses Laufs wurde ein Bug in `cv/teams.py::TeamClassifier._ensure_siglip`
gefunden und behoben: `AutoProcessor.from_pretrained` versucht immer auch den zugehörigen
Text-Tokenizer aufzulösen und schlägt ohne installiertes `sentencepiece` mit `ImportError`
fehl, obwohl `TeamClassifier` nur den Bild-Pfad braucht. Fix: `SiglipImageProcessor`
direkt statt `AutoProcessor` (siehe Commit-Historie Plan 02.1-12).

#### Team-Zuordnung: Ambivalente Tracks (v1)

Die folgenden 10 Tracks lagen bei der Mehrheits-Cluster-Zuordnung exakt bei 0.50 (unter der
0.6-Schwelle) und blieben `team_id = null`:

- track 46 (clip 7): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 9 (clip 9): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 41 (clip 15): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 15 (clip 18): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 8 (clip 27): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 34 (clip 37): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 7 (clip 38): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 12 (clip 45): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 8 (clip 50): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 14 (clip 53): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null

### v2 (BoT-SORT-Tracker, Torso-Crops -- Gap-Fix-Iteration nach dem Kontinuitäts-Review)

Orchestrator-angeordneter Re-Lauf der v1-Pipeline nach zwei gemergten Gap-Fixes: `faf75dd`
(Tracker-Wechsel von OC-SORT zu getuntem `trackers.BoTSORTTracker`, Kamerabewegungs-
Kompensation/CMC, siehe `## OC-SORT-Tracker-Klasse` -- der Name dort bleibt historisch, der
tatsächlich verwendete Tracker ist inzwischen BoT-SORT) und `b870a72` (`cv/teams.py::
extract_track_crops`, Torso-Region-Crops statt Vollkörper-Crops für die Team-Zuordnung).
Beide Fixes reagieren auf den Befund des menschlichen Kontinuitäts-Reviews (systematische
ID-Fragmentierung und Kamera-Schwenk-Kaskaden bei OC-SORT; Grün/Hintergrund-Bleed an
Box-Rändern und Beinen bei Vollkörper-Team-Crops) und wurden an einer 11-Clip-Stichprobe
(5 human-reviewte + 6 statistisch schlechteste Clips) gegen die jeweilige Baseline
gemessen, bevor sie gemergt wurden (siehe die Commit-Historie für die vollen
Experiment-Zahlen).

**Datum:** 2026-08-29
**Maschine:** Primärmaschine (Apple M4 Max), `platform.node()` = `MacBook-Pro-2.fritz.box`
**Detector Run ID:** `87a8a5222f7a472787875e974d089c44` (champion-Alias von `cv_detector_model`,
unverändert gegenüber v1 -- nur Tracker und Crop-Geometrie änderten sich, nicht der Detektor)
**Settings:** `resolution=896`, `sahi=false` (unverändert), Tracker `BoTSORTTracker
(lost_track_buffer=90, minimum_iou_threshold_first_assoc=0.1, minimum_consecutive_frames=5,
enable_cmc=True)`, Team-Crops `extract_track_crops(..., torso=True)` (innere 60 % der
Box-Breite, obere 50 % der Box-Höhe, bis zu 6 Crops/Track)

**Methodik-Hinweis (Ausführung):** wie beim v1-Lauf wurde `track_session` selbst unverändert
und ungekürzt ausgeführt, aber über vier Bash-Tool-Aufrufe verteilt (Clip-Bereiche
1-16 / 17-32 / 33-47 / 48-61), weil ein einzelner Prozess das Ausführungsschritt-Zeitlimit
hätte reißen können; die vier Teil-Parquets wurden anschließend zu einer Session-Tabelle
zusammengeführt. `extract_track_crops` lief ebenfalls in denselben vier Clip-Bereichen (rein
lesend/dekodierend, keine Modell-Inferenz, daher deutlich schneller), die vier
`crops_by_track`-Teilmengen wurden vor dem einmaligen `assign_teams`-Aufruf zusammengeführt
-- `assign_teams` selbst lief exakt einmal für die gesamte Session (KMeans-Cluster-Label
müssen über die ganze Session konsistent sein, nie pro Clip gefittet).

**Ergebnis (Tracking):**

| Metrik | v1 (OC-SORT) | v2 (BoT-SORT) |
|---|---|---|
| Clips verarbeitet | 61 / 61 | 61 / 61 |
| Clips mit Hinweis (Notice) | 0 | 0 |
| Clips mit null Tracks | 0 | 0 |
| Distinkte Tracks (Session) | 2031 (1874 `player`, 157 `referee`) | **1592 (1453 `player`, 139 `referee`)** |
| Zeilen im Output-Parquet | 341.461 | 354.404 |
| Wall-Clock (Summe der 4 Teil-Läufe) | 542.9 s (~9.05 min) | 729.7 s (~12.16 min) |

Die Track-Zahl sinkt um **21.6 %** (2031 -> 1592 gesamt, 1874 -> 1453 bei `player`) -- weniger
Tracks bei unveränderter Clip-/Detektions-Basis heisst hier weniger Fragmentierung/ID-Wechsel
je Spielzug, konsistent mit dem 50.5%-Ergebnis der 11-Clip-Gap-Fix-Experiment-Stichprobe (die
volle 61-Clip-Session zeigt einen kleineren, aber klar positiven Effekt als die Stichprobe --
erwartbar, da die Stichprobe gezielt die statistisch schlechtesten Clips einschloss). Die
Zeilenzahl steigt trotzdem leicht (354.404 vs. 341.461), weil BoT-SORTs längerer
`lost_track_buffer` (90 vs. OC-SORTs Default) Tracks über kurze Verdeckungen hinweg am Leben
hält statt sie zu beenden und neu zu vergeben -- mehr Frames pro (im Schnitt selteneren) Track.

**Pro-Stage-Sekunden (aufsummiert über alle 61 Clips):**

| Stage | v1 Sekunden | v2 Sekunden | Kommentar |
|---|---|---|---|
| `decode` | 29.21 | 29.82 | BoT-SORTs CMC braucht das dekodierte Frame zusätzlich zu `detect_video`s eigenem Decode -- ein zweiter Decode-Durchlauf pro Clip, aber Decode bleibt insgesamt klein |
| `detect` | 473.00 | 480.53 | Rauschen zwischen Läufen, gleicher Detektor/gleiche Settings |
| `track` | 11.03 | 125.14 | Grösster Unterschied: BoT-SORTs CMC-Schritt (Kamerabewegungs-Kompensation über das echte Frame) ist deutlich teurer als OC-SORTs reine Kalman-Filter-Assoziation |
| `write` | 0.09 | 0.09 | unverändert |

**C-09-Extrapolation (v2, die aktuelle Gate-Zahl):** über `cv.benchmark.extrapolate_game_runtime`
mit den obigen vier v2-Stage-Summen, `footage_seconds=664.41` (unverändert, dieselben 61 Clips)
und `game_seconds=3000.0` (50-min-Default, D-11 Annahme 3):

**47.83 min** extrapolierte Laufzeit für ein 50-minütiges Spiel -- weiterhin klar innerhalb des
60-Minuten-C-09-Budgets, aber der Sicherheitsabstand schrumpft merklich gegenüber v1s
38.63 min, ausschliesslich durch BoT-SORTs teureren `track`-Schritt (CMC). Vollständige Formel:

```
[machine=MacBook-Pro-2.fritz.box] linear extrapolation (assumption 1):
total real-time factor = decode(29.816s / 664.41s footage = 0.044876x realtime)
  + detect(480.525s / 664.41s footage = 0.723236x realtime)
  + track(125.139s / 664.41s footage = 0.188346x realtime)
  + write(0.091s / 664.41s footage = 0.000136x realtime)
  = 0.956594x realtime;
extrapolated game duration = 0.956594x * 3000.0s game (continuous-game denominator,
  assumption 2) = 2869.78s = 47.83 min
```

#### Hinweise (v2)

Keine -- wie beim v1-Lauf wurden alle 61 Clips erfolgreich verarbeitet, kein Clip löste eine
Ausnahme aus, kein Clip lieferte null Tracks, und kein Clip wich um mehr als zwei Frames von
der in `video_inventory.csv` deklarierten Dauer ab.

**Team-Zuordnung (`assign_teams` mit `extract_track_crops(..., torso=True)`):** Ein
`TeamClassifier` wurde wie in v1 einmal für die gesamte Session gefittet (SigLIP
`google/siglip-base-patch16-224`, UMAP, KMeans), diesmal gespeist aus Torso-Region-Crops
(innere 60 % der Box-Breite, obere 50 % der Box-Höhe) statt Vollkörper-Crops, bis zu 6
gleichmässig über die Track-Frames verteilten Crops pro Track. Gezählt nach derselben
Methode wie v1 (erster `class_name`-Wert je Track, da 55 Tracks im Session-Verlauf zwischen
`player`/`referee` wechseln -- ein bekanntes Detektor-Rauschen, unverändert von v1): von
**1453** `player`-Tracks erhielten **1436 (98.83 %)** eine `team_id`; 17 Tracks fielen unter
die 0.6-Mehrheitsschwelle und blieben `null` (siehe `#### Team-Zuordnung: Ambivalente Tracks
(v2)` unten) -- gegenüber v1s 99.47 % (1864/1874) ein leichter Rückgang um 0.64
Prozentpunkte, nicht die in der 11-Clip-Stichprobe gemessene Verbesserung. Ehrlich
eingeordnet: die 11-Clip-Stichprobe war so gebaut, dass sie die 8 konkreten, von Hand
gefundenen Fehlzuordnungen aus dem Kontinuitäts-Review traf (wo Torso-Crops nachweislich
half); die volle 61-Clip-Session enthält viele weitere, in der Stichprobe nicht vertretene
Tracks, bei denen der Torso-Crop stattdessen zu weniger diskriminativen Embeddings führen
kann (kleinerer Bildausschnitt, weniger Textur). Cluster-Label 0/1 bleibt arbiträr -- die
reale Team-Zuordnung erfolgt weiterhin erst in Plan 02.1-16 von Hand.

Kein neuer Bug wurde bei diesem Lauf gefunden -- der `SiglipImageProcessor`-Fix aus v1 bleibt
in Kraft und die SigLIP-Embedding-Pipeline lief unverändert.

#### Team-Zuordnung: Ambivalente Tracks (v2)

Die folgenden 17 Tracks lagen bei der Mehrheits-Cluster-Zuordnung exakt bei 0.50 (unter der
0.6-Schwelle) und blieben `team_id = null`:

- track 4 (clip 3): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 1 (clip 12): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 12 (clip 12): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 12 (clip 16): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 20 (clip 17): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 21 (clip 17): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 2 (clip 22): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 1 (clip 23): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 13 (clip 25): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 5 (clip 27): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 27 (clip 29): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 6 (clip 31): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 19 (clip 35): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 3 (clip 39): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 3 (clip 50): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 9 (clip 57): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null
- track 21 (clip 57): majority team-cluster share 0.50 is below the 0.6 threshold -- team_id left null

#### Kontinuität (v2, `cv/continuity.py::measure_continuity`)

Die Auto-Spalten von `data/reference/continuity_review.csv` wurden gegen den v2-Tracking-Output
neu berechnet (61 Zeilen, `verdict` absichtlich leer -- das menschliche Kontinuitäts-Review von
Plan 02.1-14 Task 3 ist ein separater, noch offener Schritt auf einem pausierten Ausführungs-
Zweig und nicht Teil dieses Re-Laufs): `auto_flag` verteilt sich auf 57 `ok` und 4 `fragmented`,
kein Clip mit `few-tracks` oder `no-tracks`. Diese Datei ersetzt keine menschliche Verdikt-Spalte
-- `summarise_review` verweigert einen `pass_rate` solange auch nur eine Zeile unbewertet ist
(D-09, T-2.1-31).

#### Positionsfehler (v2, `cv/accuracy.py::measure_position_error`)

Gemessen gegen dieselben 250 hand-markierten GT-Punkte wie v1 (`data/reference/gt_positions.csv`,
unverändert), diesmal gegen den v2-Tracking-Output samt neu projizierten `x_yards`/`y_yards`
(`ffep cv coords`):

| Kennzahl | v1 | v2 |
|---|---|---|
| Median (yd) | 0,169 | 0,171 |
| p90 (yd) | 0,415 | 0,422 |
| Max (yd) | 1,527 | 1,527 |
| Match-Rate | 98,4 % (246/250) | **99,6 % (249/250)** |
| Unmatched | 4 | **1** |

Median/p90/Max bleiben praktisch unverändert (Max ist exakt identisch -- derselbe Ausreisser-
GT-Punkt in `midfield`/hp-02, siehe `docs/pilot-accuracy.md`s Fehlerzerlegung, die von der
Tracker-Wahl unabhängig ist). Die Match-Rate verbessert sich klar (98,4 % -> 99,6 %): BoT-SORTs
längerer `lost_track_buffer` lässt Tracks über kurze Lücken hinweg bestehen, wodurch mehr
GT-Punkte einen Track-Fusspunkt im 3-Yard-Suchradius finden. Die volle Zerlegung (pro Feldzone,
Massstabs-Kontrolle, Fehlerzerlegung gegen die Homographie) steht in `docs/pilot-accuracy.md`.

#### Overlay-Videos (v2, `cv/overlay.py::render_track_overlay`)

Alle 61 Clip-Overlays wurden gegen den v2-Tracking-Output neu gerendert (`ffep cv overlay`,
~2:49 min Wall-Clock für alle 61 Clips) und ersetzen die vorherigen Overlays unter
`data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/overlays/` (PII, gitignored, nicht
Teil dieses Commits). `render_track_overlay`/`measure_continuity` selbst wurden für diesen
Re-Lauf aus dem pausierten Plan-02.1-14-Zweig übernommen (Commits `ff027bc`/`47acf56`,
unverändert) -- das menschliche Kontinuitäts-Review (Plan 02.1-14 Task 3) bleibt auf jenem
Zweig offen und ist nicht Teil dieses Re-Laufs.

#### Präsentations-Fix: Team-Farb-Anker + Radar-Label-Z-Order (nach v2, orchestrator-angeordnet)

Zwei rein darstellungsbezogene Bugs im Showcase-Reel wurden gefunden und behoben, ohne
erneutes Tracking (`track_session` selbst lief nicht erneut):

**1. Invertierte Anzeigefarbe (Team-Clustering selbst war korrekt):** Das Team-Clustering
(`assign_teams`) war schon in v1/v2 inhaltlich richtig -- 97 % konsistent mit den
menschlichen Team-Hinweisen aus dem GT-Datensatz (208 gematchte GT-Punkte). Der Bug lag
ausschliesslich in der Anzeige-Palette (`cv/overlay.py`, `cv/radar.py`): sie zeichnete
`team_id` 1 rot und `team_id` 0 blau, ohne jede Garantie, dass das die tatsächlich rote
Cluster-Seite war -- auf dem Side-by-Side-Showcase-Reel sah dadurch jeder Punkt
farblich verkehrt aus. Fix, in zwei Teilen:

- `cv/teams.py::assign_teams` verankert `team_id` jetzt nach dem Fit an der tatsächlichen
  Trikotfarbe (neue private Hilfsfunktionen `_crop_median_rgb`/`_redness_score`/
  `_anchor_cluster_labels`): **`team_id` 0 ist immer das Cluster, dessen Fit-Crops am
  röstesten wirken** (sättigungsgewichtete Rot-Farbton-Distanz, Median über die
  Fit-Crops), `team_id` 1 das andere. Bei mehrdeutigen Trikotfarben (kein Cluster
  merklich röter) bleibt die arbiträre KMeans-Reihenfolge erhalten und ein Hinweis wird
  erzeugt statt zu raten.
- `cv/overlay.py` und `cv/radar.py` importieren jetzt beide dieselbe Palette aus dem
  neuen Modul `cv/palette.py` (vorher zwei unabhängige, aber identische Kopien) --
  `team_id` 0 zeichnet rot, `team_id` 1 zeichnet blau, passend zum Anker-Vertrag.

Team-Zuordnung wurde NUR neu berechnet (kein erneutes Tracking): `extract_track_crops`
(Torso-Crops, unverändert) lief erneut in denselben vier Clip-Bereichen (~30 s
Wall-Clock, rein Decode/Crop) und `assign_teams` (jetzt inklusive Farb-Anker) lief einmal
für die gesamte Session (766,5 s Wall-Clock -- die zusätzliche `_anchor_cluster_labels`-
Runde embedded die Fit-Crops ein drittes Mal, daher länger als der ursprüngliche
Team-Zuordnungs-Schritt). Ergebnis: identische `team_id`-Zähler wie vor dem Fix
(112.568 × `team_id=0`, 197.672 × `team_id=1`, 44.164 × `null`, dieselben 17
ambivalenten Tracks wie oben) -- das KMeans-Cluster, das schon vorher zufällig als
`0` beschriftet war, ist tatsächlich das rötere; der Anker musste in diesem Lauf also
nichts vertauschen, garantiert die Zuordnung aber jetzt strukturell statt zufällig.
`x_yards`/`y_yards`/`hover_position_id` und alle übrigen Spalten blieben unverändert
(354.404 Zeilen, atomar über `schema.write_tracking_parquet` zurückgeschrieben). Ein
Backup des Vor-Fix-Stands liegt unter
`data/processed/tracking/v2_pre_color_anchor_fix_tracks.parquet` (gitignored).

**2. Fehlende Track-Nummern im Radar (`cv/radar.py::_draw_marker`):** Nutzer-Meldung
("blau hat keine Zahlen") beim Betrachten des zuvor gerenderten Showcase-Reels. Ursache:
`render_radar_frame` zeichnete pro Zeile SHAPE dann LABEL, eine Zeile nach der anderen --
bei eng beieinander stehenden Spielern (normal bei echtem Filmmaterial) konnte die
gefüllte Marker-Form einer später gezeichneten Zeile direkt über das bereits gezeichnete
Label einer früheren Zeile malen. Fix: `_draw_marker` in `_draw_marker_shape`/
`_draw_marker_label` aufgeteilt, `render_radar_frame` zeichnet jetzt zuerst alle Formen
und danach erst alle Labels in einem zweiten Durchgang -- keine Marker-Form kann mehr
das Label einer anderen Zeile übermalen, unabhängig von der Zeilenreihenfolge.

**War offen, jetzt behoben (separater Fix, dann gemeinsam neu gerendert):** Der Nutzer
meldete zusätzlich ein gespiegeltes Radar-Feld (Süd-/Nord-Seitenlinie der Kalibrierung
gegenüber der physischen Realität vertauscht -- eine Spiegelung ist abstandserhaltend,
daher fiel das keiner Metrik auf). Dieser Fix (Kalibrierungs-y-Achse drehen, siehe
`docs/homography-calibration.md`, `ffep cv coords` erneut gelaufen) ist inzwischen
gelandet; siehe `#### Finaler kombinierter Re-Render` unten für den Abschlussrender, der
diesen Fix zusammen mit dem Farb-Anker-Fix oben und einem neuen On-Field-Radarfilter
zusammenführt.

#### Finaler kombinierter Re-Render: Farbe + Orientierung + On-Field-Filter (2026-08-30, orchestrator-angeordnet)

Alle drei ausstehenden Präsentations-Fixes sind jetzt in einem einzigen Re-Render
zusammengeführt (kein erneutes Tracking/Team-Zuordnung/Koordinaten-Projektion -- das
Tracking-Parquet trug die Fixes bereits, siehe oben und `docs/homography-calibration.md`):

1. **Farb-Anker** (`assign_teams`/`cv/palette.py`, siehe oben) -- `team_id` 0 zeichnet
   rot, `team_id` 1 zeichnet blau, verankert an der tatsächlichen Trikotfarbe.
2. **Kalibrierungs-Spiegelung behoben** (siehe `docs/homography-calibration.md`) --
   Radar-Orientierung stimmt jetzt mit der physischen Realität überein.
3. **Neu in diesem Lauf -- On-Field-Radarfilter** (`cv/radar.py::_is_on_field`):
   `render_radar_frame` zeichnet nur noch Marker, deren `(x_yards, y_yards)` innerhalb
   des Spielfelds (inkl. beider Endzonen) plus 1 Yard Toleranz liegt. Seitenlinien-/
   Bank-Personen werden weiterhin absichtlich getrackt (~25 % der Zeilen, für
   Kontinuitäts-/Genauigkeits-Metriken) -- sie werden nur aus der Radar-Zeichnung
   gefiltert, nicht aus den Tracking-Daten selbst.

Alle 61 Overlays (`ffep cv overlay`, ohne `--clip`-Filter) und das Showcase-Reel
(`ffep cv radar --clip 11 --clip 2 --clip 6 --clip 13 --clip 4`) wurden mit diesem
Stand neu gerendert.

**Orientierung empirisch verifiziert** (Clip 5, Frame 51 -- `data/processed/experiments/
orientation_check_c5f51.png`, Footage links / Radar rechts): der Track mit dem kleinsten
`foot_x_px` im Footage (Track 3, `foot_x_px=184,5` von 1920 px Bildbreite, also nahe am
LINKEN Bildrand) liegt bei `y_yards=25,56` -- nahe der Nordseitenlinie (`field_width_yards
=25`), also OBEN im Radar, exakt wie erwartet. Der Track mit dem größten `foot_x_px`
(Track 14, Schiedsrichter, `foot_x_px=1570,6`, nahe am RECHTEN Bildrand) liegt bei
`y_yards=0,85` -- nahe der Südseitenlinie, also UNTEN im Radar. Beide Beobachtungen
bestätigen: Bild-links → Radar-Norden (oben), Bild-rechts → Radar-Süden (unten) -- die
Spiegelung ist behoben.

Drei stichprobenartig aus dem neu gerenderten Showcase-Reel gezogene Frames (Clip 11
Frame 30, Clip 2 Frame 138, Clip 13 Frame 76) wurden visuell geprüft: Team-Farben
korrekt (`team_id` 0 rot, `team_id` 1 blau), Track-Nummern neben jedem Marker lesbar
(auch bei dicht stehenden Markern, z. B. Schiedsrichter-Dreieck Nr. 5), und die
Seitenlinien-/Bank-Personencluster am linken Bildrand (deutlich im Footage sichtbar,
z. B. Clip 2 Frame 138) erscheinen erwartungsgemäß NICHT im Radar -- der On-Field-Filter
greift.

## ECC-Zweitstufe: Koordinaten-Re-Projektion + Showcase-Re-Render (2026-08-30 Follow-up)

Nach `homography._ecc_align`s Ergänzung (siehe `docs/homography-calibration.md`s
"ECC-Zweitstufe"-Abschnitt für Design und Schwellenwert-Kalibrierung) wurden Koordinaten-Projektion,
Genauigkeitsmessung und Showcase-Reel neu gelaufen (kein erneutes Tracking/Team-Zuordnung --
`track_session`/`assign_teams` liefen nicht erneut, nur die Homographie-Projektionsschicht ändert
sich):

1. `ffep cv coords --tracks .../..._tracks.parquet` erneut gelaufen -- 354.404 Zeilen, atomar
   zurückgeschrieben. Backup des Vor-Fix-Stands unter
   `data/processed/tracking/pre_ecc_fallback_backup_tracks.parquet` (gitignored).
2. `ffep cv accuracy --measure` erneut gelaufen -- siehe `docs/homography-calibration.md`s
   Genauigkeits-Vergleichstabelle (SIFT-only vs. SIFT+ECC): Match-Rate 96,4% → **99,6%**, Max-Fehler
   2,808 → **2,172 yd**, Median/p90 strukturell unverändert (dieselbe lokale-Vergleich-Begründung wie
   beim ursprünglichen Fix).
3. Showcase-Reel (`ffep cv radar --clip 11 --clip 2 --clip 6 --clip 13 --clip 4`) neu gerendert.
   Backup des Vor-Fix-Stands unter
   `data/labels/.../showcase/showcase_pre_ecc_backup.mp4` (gitignored, PII). Frame 50 (Clip 11,
   "play 1/5") visuell geprüft: Footage links zeigt dieselbe Szene wie zuvor (Team-Boxen,
   Track-IDs, Schiedsrichter-Box), Radar rechts zeigt jetzt eine plausible Cluster-Verteilung nahe
   der Westgoallinie statt der zuvor unkorrigierten (aber wegen des lokalen Charakters des
   Positionsfehlermasses ohnehin nicht sichtbar falschen) Projektion.
4. Die 61 Overlay-Videos (`ffep cv overlay`) wurden für diesen Follow-up NICHT neu gerendert -- sie
   sind reine Footage-Annotationen ohne Radar-Halbbild und damit von der Homographie-Projektion
   unberührt (Overlay zeichnet Boxen/IDs direkt in Pixelkoordinaten, keine Feld-Yard-Projektion).

## Bundle-Eingaben (Hackathon)

`src/flag_football_ep/cv/export.py::export_detections_parquet`/`export_track_crops`,
Plan 02.2-08. Erzeugt die zwei Artefakte, die `docs/hackathon-challenge-reid.md`s
"Verfügbare Daten"-Abschnitt verspricht und die Phase 2.1 nie persistiert hat: eine
Pro-Frame-Detektionen-Parquet (kein Team muss selbst einen Detektor laufen lassen) und
Torso-Crops (Trainingsmaterial für Erscheinungsmodelle) — beide an den eingefrorenen
Detektor-Lauf gepinnt (`data/reference/hackathon_freeze.json`, Plan 02.2-07).

**Session:** `2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE` (61 Drohnen-Clips, alle
registriert in `video_inventory.csv`). **Detector Run ID:**
`87a8a5222f7a472787875e974d089c44` (`cv_detector_model` v1, aufgelöst über
`freeze.read_freeze_pin` gegen den Freeze-Pin, **nicht** über `resolve_champion` --
T-2.2-24). **Dataset-Hash:**
`ab3a9673d61bc348d37ce298ba12d18b76395d1ade82a735c5b3d82d2e46aec0` (aus dem Pin).

### Detektionen (`bundle-inputs/detections.parquet`)

```
uv run --extra cv ffep cv detections --session 2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE \
  --domain drone \
  --out data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet
```

(kein `--run` -- der Freeze-Pin wird verwendet.)

| Metrik | Wert |
|---|---|
| Pfad | `data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/crops` bzw. `.../detections.parquet` (gitignored, PII) |
| Zeilen (Detektionen) | 384.689 |
| Clips abgedeckt | 61 / 61 |
| `class_name`-Verteilung | 346.573 `player`, 38.116 `referee` |
| `detector_run_id` (jede Zeile) | `87a8a5222f7a472787875e974d089c44` (einheitlich, per polars-Check verifiziert) |
| `detected_at` (jede Zeile) | ein einziger Zeitstempel (Export ist diffbar bei erneutem Lauf) |
| Auflösung/SAHI | `resolution=896`, `sahi=false` (aus `[cv]` in `ffep.toml`, deckt alle drei Domänen laut `## 4` in `docs/dataset-plan.md`) |
| Wall-Clock | ~11 min (12:17:41--12:28:29, Primärmaschine) -- deutlich unter der Stunde, die `## Inferenz-Durchsatz`s Drei-Clip-Hochrechnung für einen vollen Lauf mit Tracking nahelegt, weil dieser Export (anders als `track_session`) jeden Clip nur **einmal** dekodiert statt zweimal (kein CMC-Zweitdecode, kein BoT-SORT) |

Verifiziert (`detector_run_id` == Pin-Run-ID auf jeder Zeile):

```
uv run --extra cv python3 -c "
import polars as pl
df = pl.read_parquet('data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet')
print(df['detector_run_id'].unique().to_list())
"
# -> ['87a8a5222f7a472787875e974d089c44']
```

### Crops (`bundle-inputs/crops/`)

```
uv run --extra cv ffep cv crops --session 2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE \
  --tracks data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet \
  --out-dir data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/crops
```

| Metrik | Wert |
|---|---|
| Crops geschrieben (tatsächlich) | **17.059** -- nahe an, aber nicht identisch mit der in `docs/hackathon-challenge-reid.md` versprochenen ~17.000-Schätzung (das ist die tatsächliche Zahl, nicht die versprochene) |
| Layout | `crops/clip_XXX/track_YYYY/frame_ZZZZZ.jpg` |
| Index | `crops/index.csv`, 17.059 Datenzeilen (+ Header), eine Zeile pro geschriebenem JPEG |
| Referee-Zeilen im Index | 0 (siehe "Gefundener Bug" unten) |
| Cap pro Track | 12 Samples/Track (interne Konstante `export._EXPORT_MAX_CROPS_PER_TRACK`, nicht Teil der eingefrorenen Signatur -- siehe Plan 02.2-07s "`force` ist CLI-only"-Präzedenzfall für dieselbe Einschränkung), gemessen gegen die reale v2-Tracking-Parquet so gewählt, dass sie die ~17k-Zielzahl reproduziert (1.508 `player`-Tracks, Cap 12 → 17.638 Ober-Schranke vor dem Referee-Filter) |
| Provenienz | `crops/crops_meta.json`: `max_crops_per_track`, `n_crops`, `detector_run_ids` (aus den Tracking-Zeilen, hier `["87a8a5222f7a472787875e974d089c44"]`), `generated_at` |
| Wall-Clock | < 2 min (kein Detektor-Inferenz-Schritt, nur Video-Dekodierung + JPEG-Schreiben) |

**Gefundener Bug (Rule 1, während dieses Laufs behoben):** Der erste Lauf gegen die
echten Session-Tracks lieferte 17.174 Crops mit **115 `referee`-Zeilen** im Index, obwohl
`export_track_crops`s eigene `<behavior>`-Vorgabe null verlangt. Ursache: die
Track-Ebenen-Prüfung (`class_name` der ersten, frame-sortierten Zeile) lässt einen Track
durch, dessen erste Zeile `player` ist -- aber ~55 Tracks in der Session wechseln
`class_name` mitten im Track (bekanntes Detektor-Rauschen, siehe `## Tracking-Lauf` oben),
sodass einzelne der bis zu 12 gesampelten Frames trotzdem `referee` sein können. Fix:
zusätzlich zur Track-Ebenen-Prüfung wird jede einzelne gesampelte Zeile geprüft und bei
`class_name != "player"` übersprungen, nicht nur die erste. Regressionstest
`test_export_track_crops_skips_referee_labeled_rows_within_a_flip_noise_player_track` in
`tests/test_cv_export.py` deckt das jetzt ab. Der zweite (korrigierte) Lauf lieferte die
oben stehenden 17.059 Crops, 0 `referee`-Zeilen.

### PII und Git-Grenze

Beide Artefakte liegen unter dem gitignorierten `data/labels/`-Baum (`data/labels/*` in
`.gitignore`, wie `data/video/*`) und **treten nie in git ein** -- die Crops sind
identifizierbare Personenbilder (T-2.2-22). Verifiziert:

```
git status --porcelain data/labels
# -> (leer)
```

### Hinweis für Plan 02.2-10 (Bundle-Builder)

`bundle-inputs/` enthält absichtlich **alle** 61 Drohnen-Clips, einschließlich der 18
`role = frozen_eval`/`private_test = true`-Clips aus `data/reference/frozen_eval_clips.csv`
(D-07: dieselben 18 Clips sind das private Hackathon-Testset). Das ist richtig so für
diesen Export-Schritt -- Plan 02.2-15s `evaluate_per_domain` braucht Detektionen für
genau diese Clips, und ein zweiter, gefilterter Export wäre redundant. **Der eigentliche
`role = pool`-Filter gehört in `cv/bundle.py::build_bundle` (Plan 02.2-10), nicht hierher:**
`build_bundle` darf `bundle-inputs/` niemals ungefiltert in ein dev-facing Paket kopieren,
sondern muss `data/reference/frozen_eval_clips.csv` gegen jede `clip_number` in
`detections.parquet`/`crops/index.csv` joinen und nur `role = pool`-Zeilen/Crops
übernehmen -- exakt die "Bindende Regel für alle nachgelagerten Pläne" aus
`docs/dataset-plan.md`s `## 8`, dort für die Active-Learning-Auswahl formuliert, hier
sinngemäß auf die Bundle-Auslieferung übertragen, damit kein privates Testset-Material in
ein dev-facing Artefakt gelangt.
