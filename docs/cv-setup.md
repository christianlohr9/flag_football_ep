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

**Ausdrücklich nicht installiert:** `boxmot` — AGPL-3.0-lizenziert (verifiziert über die eigene `LICENSE`-Datei des Projekts und die GitHub-API, Stand 2026-08-24), verletzt C-06. `trackers` (roboflow, Apache-2.0) implementiert OC-SORT nativ und ersetzt es vollständig — siehe `tests/test_cv_dependencies.py` für den automatisierten Guard.

## MPS/CPU-Verfügbarkeit auf der Primärmaschine

```
torch.backends.mps.is_available() -> True
```

Torch erkennt die Apple-Silicon-GPU (Metal) auf der Primärmaschine. Für rechenintensive, aber seltene Schritte (Zero-Shot-Vorlabeling, SAM2-Propagierung) ist reines CPU-Fallback dennoch akzeptabel (RESEARCH Pitfall 3) — kein Grund, Setup-Zeit in MPS-Feintuning zu stecken, solange es nicht die heiße Trainingsschleife betrifft.

## Pro-Maschine-torch-Falle

Eine `pyproject.toml`, zwei Umgebungen: die Primärmaschine (M4 Max, siehe Namensabweichung oben) installiert das MPS-fähige `torch`-Wheel; der Dell-Rechner (8 GB CUDA-GPU, D-05) braucht ein CUDA-fähiges `torch`-Wheel, das zur installierten CUDA-Toolkit-Version passt. **Eine `uv.lock`-Auflösung bedient nicht automatisch beide Maschinen identisch** — vor dem RF-DETR-Fine-Tune auf dem Dell-Rechner muss dort separat synchronisiert und die tatsächlich installierte `torch`-Variante geprüft werden (`torch.cuda.is_available()`), nicht die hier dokumentierte MPS-Version angenommen werden.

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

**MLflow-Run:** `b9ab055c13de4366ab5b41e44d0d60e3` (Experiment `cv_detector` ==
`cfg.cv.detector_experiment`), registriertes Modell `cv_detector_model`, **Version 1**.

**Promotion:** Auf `champion` promotet (`ffep cv promote --run
b9ab055c13de4366ab5b41e44d0d60e3`, Version 1) -- Entscheidung des Orchestrators, nicht
eigenmächtig durch die Trainings-Task selbst: einziger registrierter Kandidat, objektiv
starke Metriken (`mAP_50=0.9571`, `mAP_50_95=0.8112`), ein MLflow-Alias ist trivial
reversibel (`ffep cv promote --run <anderer-run>` verschiebt ihn jederzeit neu, keine
Version wird dabei gelöscht), und der Nutzer hatte für diese Nacht-Session explizit
mechanische Weiterarbeit ohne Rückfragen an menschlichen Gates angewiesen. `resolve_champion`
verifiziert nach der Promotion: löst auf `b9ab055c13de4366ab5b41e44d0d60e3` auf.

**Statistische Ehrlichkeit:** Der Val-Split besteht aus einer Handvoll Clips (10 von 46
im 304-Frame-Subset) eines einzigen Spiels, gelabelt von einer einzigen Annotationsperson
(siehe `### Datensatz`s eigene IAA-Einschränkung oben) -- diese Zahlen beschreiben die
Anpassung an genau dieses Piloten-Regime, keine allgemeine Leistungsfähigkeit. Der
niedrigere `referee`-AP ist bei der Klassenhäufigkeit (652 `referee`- vs. 5962
`player`-Boxen im Gesamtdatensatz) erwartet und akzeptabel -- weniger Trainingsbeispiele
für diese Klasse, nicht ein Modellfehler. Die eigentlichen Gate-Kriterien (C-09) sind
Tracking-Kontinuität, Positionsfehler und Inferenzzeit -- **nicht** mAP; dieser Abschnitt
ist Kontext für die Gate-Entscheidung (Plan 02.1-17), kein Gate-Kriterium selbst.
