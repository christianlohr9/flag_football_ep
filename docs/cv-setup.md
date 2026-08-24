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

<!-- gefuellt von Plan 02.1-08 -->
