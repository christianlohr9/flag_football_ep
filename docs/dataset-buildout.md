# Datensatz-Aufbau — Laufendes Protokoll (Phase 2.2)

**Status: Iteration 1 gezogen, vorgelabelt und als CVAT-Aufgaben bereitgestellt am 2026-09-01.
Noch offen: die Nutzerin-Korrektursitzung (Plan 02.2-13, D-16/D-17).**

## Zweck & Abgrenzung

Dieses Dokument ist der laufende Ausführungs-Nachweis der Active-Learning-Iterationen, die
`docs/dataset-plan.md` vorschreibt: was pro Iteration tatsächlich gezogen, vorgelabelt und in
CVAT hochgeladen wurde, mit welchem Seed, mit welchen Zahlen. `docs/dataset-plan.md` fixiert die
Zielwerte vor der Ausführung; dieses Dokument berichtet, was die Ausführung davon eingehalten hat
— dieselbe Trennung von Plan und Ausführungsprotokoll wie zwischen `docs/pilot-gate-decision.md`s
Kriterien und `docs/cv-setup.md`s Umgebungsnachweis.

Nicht Teil dieses Dokuments: die eigentliche CVAT-Korrektursitzung, die DVC-Versionierung und die
Datensatz-Validierung (Plan 02.2-13, ergänzt diesen Abschnitt nach der Sitzung) sowie die zweite
AL-Iteration (Plan 02.2-17, eigener Abschnitt unten in einer späteren Fassung).

## Iteration 1

### Ziel-Ableitung

Aus `docs/dataset-plan.md` (`## 1`/`## 2`) und der `<interfaces>`-Formel des ausführenden Plans:

```
target_iteration_1 = ceil((floor_total - seed_frames) / 2)
                    = ceil((1500 - 0) / 2)
                    = 750
```

`floor_total = 1500` ist der verbindliche REQ-S2-03-Floor. `seed_frames = 0`, weil der
Piloten-Seed (304 Frames, Drohne) laut `docs/dataset-plan.md`s Seed-Set-Prüfung (`## 6`, Verdikt
`nicht übernommen`) nicht in die Drohnen-Frame-Zielzahl eingerechnet wird — die 900 Drohnen-Frames
dort sind bereits vollständig neu zu labelnde AL-Frames, keine Korrektur war nötig.

Die 750 Iteration-1-Frames verteilen sich auf die drei Domänen exakt nach dem in
`docs/dataset-plan.md` `## 1` fixierten Mix (Drohne 60 % / GoPro-Hinterfeld 26,7 % / TV-Broadcast
13,3 %), was — da der Gesamt-Floor pro Domäne glatt durch 2 teilbar ist (900/400/200) — exakt der
halben Domänen-Zielzahl entspricht:

| Domäne | Domänen-Floor (`docs/dataset-plan.md`) | Iteration-1-Ziel | Tatsächlich gezogen |
|---|---:|---:|---:|
| Drohne (`drone`) | 900 | 450 | 450 |
| GoPro/Hinterfeld (`sideline`) | 400 | 200 | 200 |
| TV/Broadcast (`broadcast`) | 200 | 100 | 100 |
| **Summe** | **1500** | **750** | **750** |

**Seed:** `20260516` (derselbe Seed wie `data/reference/frozen_eval_clips.csv`, aus
Nachvollziehbarkeitsgründen wiederverwendet, nicht weil ein technischer Zwang dazu bestünde —
`select_al_frames` akzeptiert jeden Integer-Seed unabhängig vom Eval-Split-Seed).

### Ausführung: `ffep cv active-learn`

Jede Domäne wurde als eigener CLI-Aufruf gezogen (eine Domäne = eine Session in
`video_inventory.csv`), wie in der `<interfaces>`-Vorgabe des ausführenden Plans festgehalten —
ein einzelner Mehr-Domänen-Aufruf hätte die Domänen-Aufteilung der proportionalen
Diversitäts-Allokation überlassen, statt dem hier fixierten Mix zu folgen:

```bash
ffep cv active-learn --iteration 1 --target 450 --seed 20260516 \
  --session 2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE \
  --out-dir data/labels/al-iteration-1/drone

ffep cv active-learn --iteration 1 --target 200 --seed 20260516 \
  --session 2026-08-14_WC-GER-vs-MEX-GOPRO \
  --out-dir data/labels/al-iteration-1/sideline

ffep cv active-learn --iteration 1 --target 100 --seed 20260516 \
  --session 2026-08-14_WC-USA-vs-AUS-TV \
  --out-dir data/labels/al-iteration-1/broadcast
```

Jeder Aufruf schreibt `<out-dir>/selection_manifest.json` (das `ALSelection`-Format:
Sessions, Iteration, Ziel, Seed, Frames mit Uncertainty-Score und Diversity-Key) sowie neu in
dieser Ausführung `<out-dir>/manifest.json` — eine `FrameSampleManifest`-Brücke
(`active_learning.py::selection_to_frame_manifest`), die für `ffep cv prelabel`/`ffep cv dataset`
nötig ist, da diese Befehle das `FrameSampleManifest`-Format erwarten, nicht `ALSelection`s
eigenes Schema. Beide Dateien liegen unter `data/labels/` (git-ignoriert, PII).

### Pool-Sicherheit (T-2.2-32)

Verifikation nach jeder Ziehung: die Schnittmenge zwischen den gezogenen Clip-Nummern und den
`role = frozen_eval`-Zeilen von `data/reference/frozen_eval_clips.csv` ist für jede Domäne leer.

```python
selected_clips = {f['clip_number'] for f in selection['frames']}
frozen = {row clip_number for row in frozen_eval_clips.csv if domain matches and role == 'frozen_eval'}
selected_clips & frozen
```

| Domäne | Gezogene Clips (Schnittmenge mit `frozen_eval`) |
|---|---|
| Drohne | `set()` — leer |
| GoPro/Hinterfeld | `set()` — leer |
| TV/Broadcast | `set()` — leer (TV hat noch keinen eingefrorenen Eval-Split, `## 8` in `docs/dataset-plan.md`; der gesamte Pool ist `role = pool`) |

### Ergebnis pro Domäne

| Domäne | Frames gezogen | Distinkte Clips (von Pool-Clips) | Max Frames/Clip | Uncertainty min / median / max |
|---|---:|---:|---:|---|
| Drohne | 450 | 43 (von 43) | 12 | 0.232 / 0.323 / 0.620 |
| GoPro/Hinterfeld | 200 | 40 (von 48) | 11 | 0.597 / 0.724 / 1.000 |
| TV/Broadcast | 100 | 41 (von 51) | 9 | 0.422 / 0.463 / 0.878 |

Drohne deckt jeden einzigen Pool-Clip mindestens einmal ab (43/43) — plausibel bei
`target / per_clip_cap = 450 / 12 = 37,5`, mit der Zwei-Hover-Positionen-Stratifizierung, die
jede Gruppe mindestens einmal bedient. GoPro/Hinterfeld und TV/Broadcast liegen beide deutlich
höher in der Uncertainty-Verteilung als Drohne (Median 0,72 bzw. 0,46 gegenüber 0,32) — der
erwartete Domain-Shift-Effekt: der champion-Detektor wurde bisher ausschließlich auf Drohnen-Frames
feingetunt, sieht die beiden neuen Domänen also zum ersten Mal. GoPro/Hinterfeld erreicht sogar
den theoretischen Maximalwert 1,0 (leere Detektion) bei mindestens einem gezogenen Frame — exakt
das Signal, das die Uncertainty-Auswahl in einer neuen Domäne suchen soll, kein Fehlschlag.

### Vorlabeln mit dem feingetunten Detektor

```bash
ffep cv prelabel --frames data/labels/al-iteration-1/<domain> \
  --out data/labels/al-iteration-1/<domain>-prelabel --backend finetuned
```

`--backend` existierte an der CLI-Oberfläche vorher nicht (`prelabel_frames` akzeptierte den
Parameter bereits seit Plan 02.2-09, `cv/commands.py`s `prelabel`-Befehl exponierte ihn aber
nicht) — als blockierender Fund während dieser Ausführung ergänzt (siehe SUMMARY). Jeder Lauf
bestätigt im Log `backend=finetuned`, nie eine `transformers`/Grounding-DINO-Zeile — kein
Zero-Shot-Fallback möglich, da `--backend` das explizit erzwingt.

| Domäne | Frames | Boxen gesamt | Boxen/Frame | `player` | `referee` | Frames ohne Detektion | Laufzeit |
|---|---:|---:|---:|---:|---:|---:|---:|
| Drohne | 450 | 8650 | 19,22 | 7795 | 855 | 0/450 | 13,6 s |
| GoPro/Hinterfeld | 200 | 816 | 4,08 | 498 | 318 | 11/200 | 6,3 s |
| TV/Broadcast | 100 | 1375 | 13,75 | 1291 | 84 | 0/100 | 3,4 s |

GoPro/Hinterfeld fällt gegenüber den beiden anderen Domänen deutlich ab (4,08 Boxen/Frame
gegenüber 19,22 bzw. 13,75) und zeigt einen für diese Domäne auffällig hohen `referee`-Anteil
(318 von 816 Boxen, ca. 39 % — gegenüber ca. 10 % bei Drohne). Beides ist konsistent mit
Domain-Shift auf einen Detektor, der nur Drohnen-Perspektiven kennt: die Hinterfeld-/
Endzone-Ansicht (siehe `docs/material-sighting.md`s Geometrie-Korrektur — `sideline` ist keine
Seitenansicht) unterscheidet sich am stärksten vom Trainingswinkel, und 11 von 200 Frames liefern
gar keine Detektion. Das ist genau der Bestätigungs-statt-Neuzeichnen-Fall, den die Vorlabelung
abfangen soll — bei GoPro/Hinterfeld wird die Nutzerin spürbar mehr Boxen selbst nachtragen müssen
als bei Drohne oder TV.

### CVAT-Aufgaben

Push via `ffep cv cvat-push --coco <domain>-prelabel --name al-1-<domain> --max-images 300` gegen
den lokalen, ausschließlich auf Loopback erreichbaren CVAT-Stack (`config.cv.cvat_host =
http://localhost:8080`, T-2.2-33). `--max-images` existierte vorher nicht (`create_cvat_task`
pusht immer genau ein Verzeichnis als eine Aufgabe) — als blockierender Fund ergänzt
(`dataset.py::split_coco_for_task_upload`, siehe SUMMARY), notwendig für die
"höchstens 300 Frames pro Aufgabe"-Vorgabe aus der Phase-2.1-Erfahrung (eine einzelne
404-Frame-Aufgabe überschritt eine Sitzung; die Nutzerin stoppte bei 304, siehe
`docs/cv-setup.md`s Labeling-Konvention-Abschnitt).

| CVAT-Task-ID | Name | Frames |
|---:|---|---:|
| 2 | `al-1-drone-1` | 300 |
| 3 | `al-1-drone-2` | 150 |
| 4 | `al-1-sideline-1` | 200 |
| 5 | `al-1-broadcast-1` | 100 |

Alle vier Aufgaben in CVAT bestätigt (`GET /api/tasks`, Status `annotation`) mit exakt
übereinstimmenden Annotations-Zahlen auf der CVAT-Seite (Task 2: 5917 Shapes, Task 3: 2733,
zusammen 8650 — deckungsgleich mit der Drohnen-Vorlabel-Box-Zahl oben; Task 4: 816; Task 5: 1375).
Keine Zugangsdaten wurden in irgendeiner Befehlsausgabe dieser Sitzung ausgegeben
(`secret()`-Auflösung, nie ein Literal in `ffep.toml` oder Log).

## Labelling-Anleitung Iteration 1

Verbindliche Konvention, wörtlich aus `docs/cv-setup.md`s `### Datensatz` →
"Labeling-Konvention" übernommen (galt für den Piloten, gilt unverändert für alle drei
Iteration-1-Domänen):

1. Jede klar sichtbare Person wird geboxt.
2. Nur Personen mit einer aktiven Schiedsrichterrolle auf dem Feld erhalten das Label `referee`.
3. **Alle** anderen Personen — inklusive Trainerstab, Ersatzspielerinnen und
   Seitenlinien-Personal — erhalten `player`, nicht etwa eine dritte Klasse oder werden
   ausgelassen. Die räumliche Filterung (wer tatsächlich auf dem Feld ist) passiert
   stromabwärts in Feldkoordinaten, nicht bereits beim Boxen.
4. Boxen umschliessen den vollständig sichtbaren Körper inklusive Gliedmassen.
5. Die Boxen-Unterkante sitzt eng an den Füssen, der Schatten wird ausgeschlossen — der
   Fusspunkt ist der Punkt, der später per Homographie in Feldkoordinaten projiziert wird.

**Domänen-spezifische Ergänzungen aus der Sichtung** (`docs/material-sighting.md`):

- **Kleinere scheinbare Spielergrösse:** GoPro/Hinterfeld misst p50 = 27,0 px, TV/Broadcast
  p50 = 23,0 px, gegenüber der Drohnen-Domäne mit p50 = 30,0 px. Alle drei bleiben über der
  20-px-Schwelle (`resolution = 896`, `sahi = false` gilt unverändert für alle drei), aber Boxen
  sind entsprechend kleiner und die Fuss-/Schatten-Trennung (Regel 5) braucht bei GoPro/Hinterfeld
  und TV/Broadcast eine Spur mehr Sorgfalt als bei Drohne.
- **GoPro/Hinterfeld zeigt das eigene Team (GER vs. MEX)** aus einer Hinterfeld-/
  Endzone-Perspektive (keine echte Seitenansicht, siehe Geometrie-Korrektur in
  `docs/material-sighting.md`) — Trainerstab und Ersatzspielerinnen am Spielfeldrand sind in
  dieser Kameraposition sichtbar dichter im Bild als bei der Drohnen-Aufsicht; Regel 3 (alles
  `player`, keine dritte Klasse) trägt das bereits ab, verdient hier aber die ausdrückliche
  Erinnerung, weil es in dieser Domäne häufiger vorkommt als bei Drohne.
- **TV/Broadcast zeigt ein fremdes Spiel (USA vs. AUS)** aus der tatsächlichen Seitenansicht —
  keine eigene Trainerstab-/Spielerinnen-Wiedererkennung nötig oder möglich, reine
  Objekt-Geometrie-Aufgabe wie bei den anderen beiden Domänen.
- Frames dieser Iteration sind durchgehend als Rechteck zu boxen (kein Polygon-Modus) — der
  Polygon-zu-Rechteck-Übergang aus der Piloten-Sitzung (`docs/cv-setup.md`, Frame ~103) betraf
  nur die damalige CVAT-Konfiguration, nicht diese Aufgaben.

**Sitzungsbudget (D-16):** ~1 Wochenende pro AL-Iteration. 750 Frames über vier Aufgaben (300 +
150 + 200 + 100), jede einzelne Aufgabe innerhalb der aus der Piloten-Sitzung gelernten
Aufmerksamkeits-Obergrenze (die Nutzerin stoppte damals bei 304 von 404 Frames in einer einzigen
Aufgabe) — die Aufteilung ist bewusst so gewählt, dass jede einzelne Aufgabe für sich in einem
Sitzungsblock abschliessbar ist, nicht nur die Summe über das Wochenende.

**Verifikationsgrad (D-17):** 100 % der 750 Frames werden von der Nutzerin in CVAT gesichtet und
korrigiert/bestätigt — die Vorlabels aus dem feingetunten Drohnen-Detektor (überwiegend hohe
Trefferquote bei Drohne und TV/Broadcast, spürbar mehr Korrekturaufwand bei GoPro/Hinterfeld,
siehe oben) machen das überwiegend zu Bestätigungsarbeit, ersetzen aber nicht die Prüfung jedes
einzelnen Frames — kein Frame gilt als "verifiziert", ohne dass ein Mensch es tatsächlich gesehen
hat.

## Iteration 2

Noch nicht gezogen — folgt in Plan 02.2-17, nach Abschluss der Iteration-1-Korrektursitzung
(Plan 02.2-13) und ihrer Auswirkung auf das Abbruchkriterium (`docs/dataset-plan.md` `## 3`).
