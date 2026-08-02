# Plan 2: Object Detection & Player Tracking für Flag Football

Strang 2 von 2 (siehe `plan-1-analytics-refresh.md`, Recherchebasis in `research-notes.md`). Ziel: Aus Trainings-/Testspiel-Video Positionsdaten (XY pro Spielerin pro Frame in Feldkoordinaten) erzeugen und daraus Coaching-Metriken bauen. 

## Leitentscheidung: Zwei Use-Cases, zwei Datenquellen — ein Problem weniger

Das Kernproblem „heterogenes Videomaterial" löst sich auf, wenn man die Use-Cases trennt:

| Use-Case | Material | Methode |
|---|---|---|
| **Eigenes Team** (Routen, Spacing, Separation, Speeds) | Kontrollierbar: Drohne bei Trainings/Testspielen (bei offiziellen Spielen nicht erlaubt — dort ggf. erhöhte Stativ-/Veo-Position als Zweitdomäne) | CV-Tracking (dieser Plan) |
| **Gegneranalyse** | Unkontrollierbar: getauschtes Film, TV, IFAF.TV | Manuelles Charting + PBP-Tendenzen (Plan 1) — so machen es auch PFF/Hudl IQ/alle College-Programme |

CV auf Fremdmaterial ist explizit **Stretch-Goal (Phase 2.5), nicht Grundlage**. Damit kollabiert das Domain-Problem auf 1–2 kontrollierte Kameradomänen.

## Phase 2.0 — Gespräch mit dem Videoanalysten + Capture-Protokoll (vor allem anderen)

- Materialinventur: Welche Spiele/Trainings liegen in welcher Kameradomäne vor (Drohne / TV-Seite / GoPro-Längsseite)? Auflösung, Bildrate, typische Länge. Je 2–3 Beispielclips pro Domäne sichern.
- **Capture-Protokoll für Trainings/Testspiele vereinbaren** (der höchste Hebel des ganzen Projekts): Drohne in fester Hover-Position, hoher Schrägwinkel hinter der Endzone oder Overhead (~30–60 m, 4K, feste Belichtung), ganzes Feld im Bild, pro Half-Feld-Drive eine Position. Wichtig wegen Akku (~20–25 min): Wechselprotokoll zwischen Drives. Erkenntnis aus TeamTrack: senkrechte Top-Down-Sicht macht Detection messbar schwerer (kleine Objekte, untypische Posen) — leicht schräg ist besser als exakt senkrecht, außerdem bleiben Trikotnummern teilweise sichtbar.
- Rechtliches klären: EU-Drohnenverordnung (Kategorie, Registrierung, Versicherung), Einverständnis der Spielerinnen (DSGVO — Verband hat vermutlich Medienerklärungen, prüfen ob Analyse-Nutzung abgedeckt ist).
- Sync-Konvention: Wie wird ein Play im Video einem Play im Hudl-PBP zugeordnet (Timestamp-Overlay, Klatschen/Board zu Drive-Beginn, o. ä.)? Ohne diesen Join bleiben Tracking-Daten von Plan 1 entkoppelt.

## Phase 2.1 — Pilot auf einem Spiel (2 Wochenenden, Go/No-Go-Gate)

Ein einziges Testspiel/Training mit dem besten vorhandenen Drohnenmaterial:

1. Zero-Shot-Baseline: Grounding DINO / COCO-Detektor mit Prompt „person" über gesampelte Frames — validiert die Pipeline, bevor irgendwas gelabelt wird, und liefert die Pre-Labels.
2. CVAT + SAM2-Video-Tracker aufsetzen; ~300–500 Frames korrigieren (Klassen: `player`, `referee`; Ball zunächst weglassen).
3. RF-DETR-Small fine-tunen (Apache 2.0; Colab/Consumer-GPU, Stunden pro Run). Drohnen-Regime beachten: höhere Input-Auflösung, ggf. SAHI-Kachel-Inferenz.
4. Tracking mit OC-SORT (statische Kamera) via BoxMOT oder roboflow/trackers; Team-Zuordnung ohne Labels via SigLIP-Embeddings + UMAP + KMeans.
5. Homographie: einmalige manuelle 4–8-Punkt-Kalibrierung pro Hover-Position (Feldecken, Midfield-Linie, Pylonen) → XY-CSV in Feldkoordinaten + Top-Down-„Radar"-Clip als Demo für HC und Analysten.

**Go/No-Go-Kriterien:** (a) ≥ 90 % einer Play ohne ID-Switch innerhalb des Plays trackbar, (b) Positionsfehler grob ≤ 1 m (Plausibilitätscheck über bekannte Feldmaße), (c) Inferenz eines Spiels < 1 h auf verfügbarer Hardware. Werden sie klar verfehlt, zurück zu Phase 2.0 (Capture-Setup ändern) statt mehr labeln.

## Phase 2.2 — Dataset-Ausbau (das „2-Wochen-Label-Budget")

Erst nach bestandenem Pilot. Model-in-the-Loop (Pilot-Detektor pre-labelt, nur Fehler korrigieren, 2 Active-Learning-Iterationen):

- 1.500–3.000 verifizierte Frames, gewichtet: ~60 % Drohne (Primärdomäne), ~40 % beste Zweitdomäne (erhöhte Seitenkamera), gezielt harte Fälle (Line-of-Scrimmage-Gedränge, Blitz, Gegenlicht, Regen).
- **Ein** Detektor über alle Domänen trainieren, aber pro Domäne evaluieren und Inferenz-Settings pro Domäne setzen (Auflösung/Tiling). Gepoolte mAP versteckt Domänen-Kollaps.
- Dataset sauber versionieren (Roboflow Universe oder DVC). Option mit Strahlkraft: als **erstes öffentliches Flag-Football-Detection-Dataset** publizieren — existiert bisher nicht, wäre ein Alleinstellungsmerkmal und bringt Community-Beiträge.

## Phase 2.3 — Von Tracks zu Coaching-Metriken

Die eigentliche Wertschöpfung; jede Metrik ein kleines, testbares Modul auf der XY-CSV:

1. Snap-Erkennung pro Play (Bewegungsimpuls an der LOS) → Play-Segmentierung.
2. Routen-Overlays und Routen-Klassifikation der Receiverinnen (Abgleich mit `TARGET ROUTE` aus dem Hudl-Charting als Ground Truth — die Label existieren schon!).
3. Separation beim Catch, QB Time-to-Throw, Speeds/Beschleunigungen, Spacing-Metriken der Defense.
4. Join mit Plan-1-PBP über die Sync-Konvention → EPA pro Route/Konzept mit Positionskontext.

## Phase 2.4 — Identität (nur wenn nötig)

Trikotnummern-OCR ist das fragilste Modul (bei Top-Down kaum möglich). Erst versuchen: Tracking-Kontinuität + Team-Clustering + manuelle Zuordnung von ~10 Tracklets pro Play (bei 5v5 in Minuten erledigt). Falls Automatisierung nötig: jersey-number-pipeline-Rezept (Legibility-Filter → Torso-Crop → PARSeq → Tracklet-Voting) oder VLM-Reads (Qwen2-VL-Klasse) auf Keyframes.

## Phase 2.5 — Stretch: Fremdmaterial

Erst wenn 2.1–2.3 im Betrieb sind: TV-/Seitenansicht als dritte Domäne + Feld-Keypoint-Modell für bewegte Kameras (Roboflow-Sports-Rezept/PnLCalib). Bewusst zurückgestellt — spärliche Flag-Feldmarkierungen machen das zum härtesten Teilproblem, und der Scouting-Bedarf ist durch Plan 1 abgedeckt.

## Stack (Lizenz-sauber)

| Baustein | Wahl | Lizenz |
|---|---|---|
| Auto-Labeling | Grounding DINO + SAM2 (autodistill / CVAT) | permissiv |
| Annotation | CVAT self-hosted (SAM2-Tracker) | frei |
| Detektor | RF-DETR-S/M (Fallback D-FINE + DEIM) | Apache 2.0 |
| Tracking | OC-SORT (statisch/Drohne), BoT-SORT (bewegt) via BoxMOT / roboflow/trackers | MIT/Apache |
| Team-Split | SigLIP + UMAP + KMeans | permissiv |
| Homographie | manuell (statisch/Drohne); Keypoint-Modell erst in 2.5 | — |
| Compute | Colab Pro / RTX-Consumer-GPU; Training gesamt zweistelliger $-Betrag, Inferenz ~15–25 min/Spiel (T4) | — |

Ultralytics-YOLO bewusst vermeiden (AGPL-3.0 inkl. fine-getunter Gewichte — problematisch, sobald der Verband oder Dritte das Ergebnis nutzen).

## Risiken

- **Capture-Protokoll wird nicht eingehalten** → wichtigster Mitigations-Hebel ist der Videoanalyst als Verbündeter; Protokoll auf eine Seite eindampfen.
- **Ball-Detection** (klein, motion-blurred) → bewusst nicht im Scope der ersten Phasen; Play-Struktur kommt aus Snap-Erkennung + PBP-Join.
- **Drohnen-Akku/Wetter/Genehmigung** → Zweitdomäne (hohes Stativ/Veo-Klasse-Kamera) von Anfang an mitdenken, deshalb 60/40-Split im Dataset.
- **Zeitbudget** (Einzelperson) → jede Phase hat ein hartes Gate; der Pilot entscheidet nach 2 Wochenenden, ob weiter investiert wird.
