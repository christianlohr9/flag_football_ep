# Hackathon-Challenge — Vorbereitungs-Checkliste (Stand 2026-08-31)

Was bis zum BWI Data Analytics Hackathon (23.–27. November 2026) fertig sein muss. Gegliedert nach
Verantwortung: **[NUTZER]** = deine Handarbeit (v. a. Labeln, Freigaben), **[PROJEKT]** = kann ich
vorbereiten (Skripte, Pipeline-Läufe, Packaging). Challenge-Inhalt: `docs/hackathon-challenge-reid.md`;
Formulartext: `docs/hackathon-challenge-reid-formular.md`.

---

## 1. Freigaben und Organisation (blockiert alles Weitere)

- [ ] **[NUTZER] Verbands-Freigabe** für die Nutzung des Spielmaterials im Hackathon (intern,
  zweckgebunden, keine Weitergabe, Löschung danach). Ohne diese Freigabe darf die Challenge nicht
  eingereicht werden (Formular schließt fremde personenbezogene Daten aus).
- [ ] **[NUTZER] Bundeswehr-Freigabe** für Transfer-Set 2 (gestellte Drohnenszene mit Soldaten) —
  Drohnenflug auf dem Übungsplatz + Verwendung des Materials. Optional, aber der stärkste
  Praxisbeweis. Kann auch nach der Einreichung kommen (Überraschungs-Testset).
- [ ] **[NUTZER] Infrastruktur mit der BWI klären:** eine GPU pro Team (24 GB reichen, siehe §5),
  Internetzugang für Modell-Downloads ODER vorab gecachte Gewichte (siehe §4), lokaler Storage.

---

## 2. Labelling — was du noch von Hand machen musst

Wichtiges Prinzip vorab: Für die **Kern**-Metrik (Identitäts-Kontinuität) brauchst du **keine**
vollständige Frame-für-Frame-Identitäts-Ground-Truth — es genügt das Play-für-Play-Urteil
`pass`/`fail` auf den Overlay-Videos, das du beim Gate schon kennst. Das hält deinen Aufwand klein.

### Muss (für ein sauberes Dev-/Test-Set)

- [ ] **[NUTZER] A — Kontinuitäts-Urteile Pilot-Clips 21–61 (~1 h).** Die restlichen 41 Clips der
  Pilot-Session auf den v2-Overlays bewerten (`pass`/`fail` + Kurznotiz), Eintrag in
  `data/reference/continuity_review.csv`. Ohne das werden die schon bewerteten 20 Clips faktisch
  zum Testset und alle Teams optimieren darauf. **Wichtigste Stunde.**
- [ ] **[NUTZER] B — Flag-Pull-Ereignisse Pilot-Session (~1 h).** Vorlage liegt bereit:
  `data/reference/flag_pull_events.csv` (61 Zeilen). Pro Clip: `outcome`, `pull_time_s` (Timecode im
  Player, ±0,5 s), `carrier_track_id`, `puller_track_id` (Nummern aus dem Overlay-Video), `notes`.
  Nur für die **Bonus**-Wertung nötig — wenn du den Bonus streichst, entfällt B.
- [ ] **[NUTZER] C — Zweites Drohnenspiel als privates Test-Set (~1,5 h).** Sobald [PROJEKT] die
  Pipeline auf eine zweite Drohnen-Session angewandt hat (Detektionen, Baseline-Tracks, Overlays),
  dort A (+ optional B) wiederholen. Labels bleiben unter Verschluss → verhindert Overfitting.

### Kann (verbessert die Challenge, kein Muss)

- [ ] **[NUTZER] D — Kleine Voll-GT für automatisches Scoring (~2 h, optional).** In 3–5 Clips jede
  Spielerin über den ganzen Clip mit einer konsistenten ID versehen (das ist echtes
  Frame-Labeln). Nur nötig, falls die Teams während der Woche eine automatische IDF1/HOTA-Zahl
  statt nur der menschlichen `pass`/`fail`-Wertung wollen. Entscheidung offen (§6).
- [ ] **[NUTZER] E — Transfer-Stichprobe GoPro/TV (~1 h, optional).** Auf 10–15 Clips aus dem
  GoPro- und TV-Material Kontinuitäts-Urteile, damit die Transfer-Wertung eine Zahl bekommt statt
  nur „schau es dir an".
- [ ] **[NUTZER] F — Transfer-Set 2 Ground Truth (Bundeswehr, mit dem Dreh).** Kein nachträgliches
  Labeln nötig: das Drehbuch pro Take IST die Ground Truth (wer geht in welcher Reihenfolge rein/
  raus). Nur sauber protokollieren.

### Ausdrücklich NICHT nötig

- **Kein** neues Detektor-Labeln (Boxen). Der Detektor ist fertig (mAP@50 0,957); für 2.2 wird
  separat gelabelt, aber die Challenge braucht es nicht.
- **Kein** Homographie-Neuklicken. Die Kalibrierung der Pilot-Session liegt vor; fürs zweite
  Drohnenspiel macht [PROJEKT] das mit deinen 4–8 Klicks pro Hover-Position (~10 min, kein
  Frame-Labeln).
- **Kein** Ball-Labeln (bewusst außen vor).

### Material, das du schon hast (kein Labeln, nur bereitstellen)

- Weitere Drohnen-, TV- und GoPro-Spiele — Rohmaterial reicht; [PROJEKT] wählt daraus das zweite
  Drohnenspiel (Test-Set) und die Transfer-Stichproben und lässt die Pipeline darüber laufen.

---

## 3. Datenpakete, die [PROJEKT] schnürt (aus deinem Material)

- [ ] **[PROJEKT] Dev-Set-Bundle:** 61 Pilot-Clips + Detektionen (Parquet) + Baseline-Tracks +
  ~17k Crops + Overlays + Radar + alle Referenz-CSVs, in einer dokumentierten Ordnerstruktur.
- [ ] **[PROJEKT] Test-Set-Bundle:** zweites Drohnenspiel identisch aufbereitet, Labels getrennt
  weggeschlossen.
- [ ] **[PROJEKT] Transfer-Bundle:** GoPro- + TV-Clips mit Detektionen.
- [ ] **[PROJEKT] Scoring-Skript:** liest Team-Tracks im festen Schema, gibt die Kontinuitäts- und
  (falls Bonus) Flag-Pull-Metrik aus — dasselbe Skript für alle, damit Zahlen vergleichbar sind.
- [ ] **[PROJEKT] Starter-Kit / README:** „Baseline in 5 Minuten laufen lassen", Schnittstellen
  (Detektionen rein, Tracks raus), die genannten Bibliotheken vorinstalliert, ein gescheiterter
  Beispiel-Clip mit Fehlerbeschreibung als Einstieg.

---

## 4. Bibliotheken und Ressourcen für die Teams (alles permissiv lizenziert)

Als Startpunkte im Starter-Kit nennen — und als Modell-Cache vorab hinterlegen, falls das
BWI-Netz Hugging-Face-/GitHub-Downloads blockiert.

| Zweck | Baustein | Lizenz |
|---|---|---|
| Tracking-Baseline (Bewegung + Kamerakompensation) | `trackers` (roboflow, BoT-SORT/OC-SORT) | Apache-2.0 |
| Datenmodell für Detektionen/Boxen | `supervision` | MIT |
| ReID-Backbones (OSNet, ResNet-IBN) | `torchreid` | MIT |
| Tracklet-Splitting + -Merging (fertig, hinter BoT-SORT) | `gta-link` (GTA) | MIT |
| Gelernte In-Context-ID-Zuordnung (Analogon DanceTrack) | MOTIP | Apache-2.0 |
| Bewegungsbasierte Assoziation als Alternativen | OC-SORT, Hybrid-SORT | MIT |
| Erscheinungs-Encoder zum Feintunen | SigLIP / CLIP-ViT / DINOv2 (timm, transformers) | Apache-2.0 |
| Klassische CV-Bausteine (Homographie, Optical Flow, Features) | OpenCV | Apache-2.0 |

**Analog-Datensätze zum Vortrainieren/Vergleichen (nicht Pflicht):** TeamTrack (Drohnen-Top-View,
CC BY 4.0), SoccerTrack v2 (CC BY 4.0), DanceTrack (gleich gekleidete Ziele, non-commercial —
nur Forschung/Eval), PRAI-1581 (Luftbild-Personen bei 30–150 px).

**Vermeiden (Lizenz):** Ultralytics YOLO, boxmot (AGPL); Deep-EIoU, GTATrack (keine LICENSE);
PRTreID/BPBreID (Hippocratic); sn-gamestate/sn-reid (GPL); SportsMOT (non-commercial).

---

## 5. Rechen-Instanz

**24 GB pro Team reichen** (L4, A10, RTX 4090/A5000; eine L40 mit 48 GB ist komfortabel). Grund:
der teure Teil (Detektion) ist vorgerechnet. ReID-Encoder feintunen 2–6 GB; ein größeres
Kontrastiv-Feintuning von SigLIP/CLIP 12–18 GB; Tracklet-Optimierung läuft auf CPU.

Wichtiger als die Kartengröße:
- **Eine GPU pro Team**, nicht eine geteilte (auf A100/H100 alternativ MIG-Partitionen 20–40 GB).
- **Modellgewichte vorab cachen** (`~/.cache/huggingface`, torchreid) — sonst Stillstand am Tag 1,
  falls kein Internet.
- **Lokale NVMe** (~50–100 GB), **≥ 8 CPU-Kerne, ≥ 32 GB RAM** fürs Video-Dekodieren und die
  CPU-lastige Assoziation.
- Linux, CUDA 12.x, Python 3.12, `uv`, ffmpeg; Pipeline installiert sich mit `uv sync --extra cv`.
- [PROJEKT] liefert ein Image mit geklontem Repo, gecachten Gewichten und einmal durchgelaufener
  Baseline.

---

## 6. Offene Entscheidungen (klären wir beim Datenaufbereiten)

- Bonus Flag-Pull drin lassen oder weglassen? (bestimmt, ob Labelling B nötig ist)
- Automatisches Scoring während der Woche (dann Labelling D) oder rein menschliche `pass`/`fail`-
  Wertung an den Checkpoints?
- Umfang Transfer-Wertung (nur anschauen, oder Labelling E für eine Zahl)?
- Test-Set: zweites Drohnenspiel allein, oder zusätzlich Transfer-Set 2 (Bundeswehr) als Krönung?

---

## 7. Warum geht das nicht „einfach mit OpenCV"?

Kurz: **OpenCV ist die Werkzeugkiste, nicht die Lösung** — und das eigentliche Problem ist keins,
für das es ein fertiges Werkzeug gibt.

OpenCV kann Bilder lesen, Features finden (ORB/SIFT), Homographien rechnen, Optical Flow schätzen,
Farbräume umrechnen. All das nutzen wir bereits (Homographie, die Drift-Korrektur zwischen Clips
lief über OpenCVs Feature-Matching). Was OpenCV **nicht** hat, ist eine Funktion „gib jeder Person
über die Zeit eine stabile Identität". Das zerfällt in drei Teilprobleme:

1. **Erkennen, dass da eine Person ist** — gelöst, aber von einem trainierten neuronalen Netz
   (RF-DETR), nicht von OpenCV. Klassische Verfahren (HOG, Haar-Kaskaden) scheitern an 30-px-Zielen
   aus der Vogelperspektive.
2. **Frame zu Frame verketten** — das kann ein Tracker (BoT-SORT) über Bewegung: „die Box, die dem
   letzten Ort am nächsten ist, ist wohl dieselbe Person". Genau das bricht bei Verdeckung: Wenn
   zwei Spielerinnen sich kreuzen und wieder trennen, weiß die Bewegung nicht mehr, wer wer ist —
   der Tracker rät, und oft falsch. Das ist der 77-%-Fehler.
3. **Nach der Verdeckung wieder zuordnen (Re-Identification)** — hier gibt es klassisch **kein**
   Verfahren. Die einzige Information, die bleibt, ist das **Aussehen**: „Diese Person sieht aus wie
   die, die vor drei Sekunden hinter der anderen verschwand." Bei Fußgängern in Alltagskleidung
   funktioniert das (verschiedene Farben, Muster) — und dafür gibt es trainierte ReID-Netze. Bei
   **gleich gekleideten** Spielerinnen (oder Soldaten in Uniform) fällt genau dieses Signal weg:
   dieselbe Trikotfarbe, dieselbe Silhouette, 30 Pixel groß, keine lesbare Nummer. Ein
   Pixelvergleich (OpenCV) würde sie alle für identisch halten.

Deshalb ist es offen: Es gibt kein fertiges Modell, das gleich gekleidete Personen ohne
Rückennummer aus der Luft unterscheidet — die Recherche (`docs/hackathon-challenge-reid.md` Teil 4)
hat es über Fußball, American Football und kommerzielle Anbieter bestätigt: Alle lösen es über
Rückennummern, Sensoren oder Menschen im Loop. Was bleibt, ist, ein Modell zu bauen, das die
**feinen** verbliebenen Unterschiede lernt (Statur, Frisur, Gang, Socken, minimale Farbunterschiede)
und mit den **Regeln** kombiniert (5 gegen 5, Team bekannt, wer wo verschwand). Das ist Machine
Learning und Optimierung, nicht ein OpenCV-Funktionsaufruf — und genau das macht es zu einer
Challenge statt zu einer Nachmittagsaufgabe.
