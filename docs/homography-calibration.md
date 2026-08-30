# Homographie-Kalibrierung — Pilot-Session Drohne (2026-05-16)

**Status: Kalibrierung abgeschlossen am 2026-08-29 — Punktkorrespondenzen vom Nutzer über die
lokale Picker-Seite gesetzt (hp-01: 4 Fit + 1 Held-out, 0.27 yd Reprojektionsfehler; hp-02: 4 Fit,
exakt bestimmt), Feldkoordinaten für alle 341.461 Track-Zeilen berechnet (25.03% out-of-bounds,
dominiert von absichtlich mitgetrackten Randpersonen).**

Maschinenlesbares Gegenstück: `data/reference/homography_calibration.csv`
(`ffep cv calibrate` / `homography.pick_points`, `homography.load_calibration`).

## Zweck & Abgrenzung

Dieses Dokument ist das deutschsprachige Protokoll der manuellen 4–8-Punkt-Homographie-Kalibrierung
(D-05) für die beiden in `docs/pilot-sighting.md` (`## Konsequenzen`) bestätigten Hover-Positionen der
Session `2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`. Es beantwortet: welche Feld-Landmarken die
Zielkoordinaten in Yards definieren, wie die Punkte je Hover-Position ausgewählt wurden, und wie genau
die resultierende Homographie ist (gemessen an zurückgehaltenen Landmarken, nicht angenommen).

Nicht Teil dieses Dokuments: die eigentliche Sichtung/Gruppierung in Hover-Positionen (das leistet
`docs/pilot-sighting.md`) und die End-to-End-Positionsgenauigkeit gegen den C-09-Gate-Schwellenwert
(das leistet das Ground-Truth-Set aus Plan 02.1-15 — die hier gemessene Reprojektionsabweichung ist
ein Grobcheck der Homographie selbst, kein Ersatz dafür).

## Feld-Landmarken

**Vokabular-Korrektur nach Feldrealität (2026-08-29):** Das Pilotfeld trägt keine
10/20/30/40-Yard-Linien — markiert sind nur Goallinien, Endzonen-Rücklinien, Mittellinie und die
beiden 5-Yard-No-Run-Zone-Linien. Die ursprünglich geplanten `yardline_10/20/30/40`-Landmarken
wurden aus dem Vokabular entfernt (niemand soll unsichtbare Linien raten); an ihre Stelle treten
`yardline_5_*` (x = 5) und `yardline_45_*` (x = 45). Das Vokabular umfasst damit 14 Namen.

`homography.field_landmarks(config)` berechnet die feste Landmarken-Vokabel
(`homography.FIELD_LANDMARKS`) aus `config.cv.field_length_yards` (50.0),
`field_width_yards` (25.0) und `endzone_yards` (10.0). Koordinatenkonvention (D-13):
**x = 0** an der Westgoalline, **x = 50** an der Ostgoalline (negative x bzw. x > 50 liegt in einer
Endzone); **y = 0** an der Südseitenlinie, **y = 25** an der Nordseitenlinie. "Süd"/"Nord",
"West"/"Ost" sind hier reine Konventionsnamen für die beiden Seitenlinien bzw. Spielfeldenden, nicht
zwingend die geografische Ausrichtung vor Ort.

**Orientierungs-Korrektur (2026-08-30, Spiegel-Fix):** Die ursprüngliche Kalibrierung hatte
"links im Bild = Süd" gesetzt; der visuelle Abgleich des Showcase-Reels mit dem Originalmaterial
zeigte, dass das gesamte Radar dadurch spiegelverkehrt war. Eine Spiegelung ist abstandserhaltend —
weder die Held-out-Reprojektion (0.27 yd) noch die GT-Positionsfehlermessung konnten sie erkennen;
nur der Mensch am Side-by-Side-Vergleich. Die Süd/Nord-Zuordnung aller Kalibrierungspunkte wurde
getauscht (Bild-rechts = Süd), die Homographien neu gefittet und alle Feldkoordinaten neu berechnet.
Nachweislich unverändert danach: Held-out 0.27 yd, Positionsfehler Median 0.171 yd / p90 0.422 yd /
Match 99.6% (Spiegelinvarianz bestätigt).

| Landmarke | x (Yards) | y (Yards) |
|---|---|---|
| `goalline_west_south` | 0.0 | 0.0 |
| `goalline_west_north` | 0.0 | 25.0 |
| `goalline_east_south` | 50.0 | 0.0 |
| `goalline_east_north` | 50.0 | 25.0 |
| `endzone_west_back_south` | -10.0 | 0.0 |
| `endzone_west_back_north` | -10.0 | 25.0 |
| `endzone_east_back_south` | 60.0 | 0.0 |
| `endzone_east_back_north` | 60.0 | 25.0 |
| `yardline_5_south` | 5.0 | 0.0 |
| `yardline_5_north` | 5.0 | 25.0 |
| `yardline_45_south` | 45.0 | 0.0 |
| `yardline_45_north` | 45.0 | 25.0 |
| `midfield_south` | 25.0 | 0.0 |
| `midfield_north` | 25.0 | 25.0 |

## Vorgehen

1. **Ein repräsentativer Clip je Hover-Position:** statt eines beliebigen Clips wählt Task 1 den Clip
   mit der gemessenen Spielergröße (`apparent_player_px_p50` aus `data/reference/hover_positions.csv`),
   die dem Median der Gruppe am nächsten liegt — das Referenzbild ist damit typisch für die Position,
   nicht ein Extremfall (Nahaufnahme oder besonders weite Einstellung).
2. **Referenzbild-Export:** `homography.pick_points(clip, hover_position_id, out_csv, at_second=...)`
   liest bei `at_second` einen Frame mit klar sichtbarem Spielfeld, zeichnet ein 100px-Pixelraster mit
   Achsenbeschriftung ein und schreibt das annotierte JPEG nach
   `data/labels/calibration/{hover_position_id}_ref.jpg` (gitignored, zeigt identifizierbare Personen).
   `pick_points` druckt zusätzlich die vollständige Landmarken-Checkliste in alphabetischer Reihenfolge
   auf stdout.
3. **Punkte auswählen (manuell, Plan 02.1-13 Task 2):** entweder interaktiv per
   `ffep cv calibrate --clip <n> --hover-position <id> --at-second <t>`
   (`FFEP_CV_CALIBRATE_INTERACTIVE=1`, Mausklicks in Checklisten-Reihenfolge) oder händisch, indem die
   Pixelkoordinaten am Raster des Referenzbilds abgelesen und direkt in
   `data/reference/homography_calibration.csv` eingetragen werden. Beide Wege validiert
   `homography.load_calibration` gleich streng (Landmarken-Vokabular, Zielkoordinaten-Übereinstimmung,
   Pixel-Grenzen, Kollinearität).
4. **Mindestens 4 Fit-Punkte, möglichst breit über das sichtbare Feld verstreut** (nahe den
   Bildrändern/-ecken statt in einem engen Cluster — ein enger Punkt-Cluster macht die Homographie
   instabil, siehe `homography._is_degenerate`). Diese Punkte bekommen `use_for_fit = true`.
5. **Mindestens 2 weitere Landmarken zurückhalten** (`use_for_fit = false`) — sie fließen nie in die
   Anpassung ein und sind der einzige unabhängige Beleg dafür, wie gut die Homographie tatsächlich ist
   (D-10). Ohne Hold-out-Punkte wäre jede Genauigkeitsangabe unbelegt (T-2.1-33).
6. **Wiederholen für jede Hover-Position** aus `data/reference/hover_positions.csv` — aktuell `hp-01`
   und `hp-02`.

## Kalibrierungen pro Hover-Position

### hp-01

- Repräsentativer Clip: **Clip 028** (`apparent_player_px_p50 = 30.0px`, exakt der Gruppen-Median von
  `hp-01`), Referenzbild-Zeitpunkt `at_second = 3.0`.
- Referenzbild: `data/labels/calibration/hp-01_ref.jpg`.
- Sichtbar im Referenzbild (visuelle Grobeinschätzung, keine Landmarken-Zuordnung — das bleibt die
  Aufgabe des Checkpoints): eine Goalline/Endzonen-Kante quer durchs Bild, eine Seitenlinie mit
  Team-Bank und einem orangen "40"-Pylon links im Bild, ein zweiter "40"-Pylon rechts jenseits der
  Endzone (deutet auf beide Seitenlinien im Bild, konsistent mit der in `docs/pilot-sighting.md` als
  "weite Rahmung" beschriebenen Einstellung), sowie ein Eckpylon am rechten Bildrand nahe der
  Endzonen-Hinterlinie.
- Punktkorrespondenzen: **5** (vom Nutzer am 2026-08-29 über die lokale Picker-Seite geklickt) —
  mehr Landmarken sind in diesem Bildausschnitt physisch nicht identifizierbar. 4 Fit-Punkte
  (`goalline_west_north`, `midfield_south`, `midfield_north`, `yardline_5_south`) spannen ein
  Viereck über Goallinie ↔ Mittellinie und beide Seitenlinien auf.
- Reprojektionsabweichung (zurückgehaltener Punkt `yardline_5_north`): **0.27 Yards (~0.25 m)**.
- Anzahl zurückgehaltener Punkte: **1** statt der geplanten 2 — dokumentierte Abweichung, siehe
  Vokabular-Korrektur oben: das Feld bietet schlicht nicht mehr klickbare Linienkreuzungen.
- Out-of-Bounds-Anteil: **30.42%** der hp-01-Zeilen (56.515 von 185.776) liegen außerhalb des
  Spielfelds — dominiert von absichtlich mitgetrackten Randpersonen (Bank/Coaches, siehe
  Labelling-Konvention in `docs/cv-setup.md`), nicht von Projektionsfehlern; die räumliche
  Filterung auf Spielfeld-Inhalt ist der vorgesehene nachgelagerte Schritt.

### hp-02

- Repräsentativer Clip: **Clip 044** (`apparent_player_px_p50 = 30.0px`, exakt der Gruppen-Median von
  `hp-02`), Referenzbild-Zeitpunkt `at_second = 3.0`.
- Referenzbild: `data/labels/calibration/hp-02_ref.jpg`.
- Sichtbar im Referenzbild (visuelle Grobeinschätzung): Torpfosten und Endzonen-Beschriftung oben
  links (bestätigt Nähe zur Endzone, konsistent mit der in `docs/pilot-sighting.md` als
  "gezoomt/rotiert" beschriebenen Einstellung), eine Seitenlinie mit Team-Bank und einem orangen
  "40"-Pylon rechts im Bild, weites offenes Spielfeld ohne klar erkennbare zweite Seitenlinie im
  unteren Bildbereich.
- Punktkorrespondenzen: **4** (vom Nutzer am 2026-08-29 über die lokale Picker-Seite geklickt) —
  mehr Landmarken sind in diesem Bildausschnitt physisch nicht identifizierbar: `midfield_south`,
  `midfield_north`, `yardline_45_south`, `yardline_5_north`, alle als Fit-Punkte.
- Reprojektionsabweichung: **nicht messbar** — mit exakt 4 Punkten ist die Homographie exakt
  bestimmt (Restfehler 0 per Konstruktion), ein unabhängiger Kontrollpunkt existiert nicht.
  Die Qualität dieser Homographie wird ausschließlich end-to-end über das Ground-Truth-Set aus
  Plan 02.1-15 gemessen — genau die Abgrenzung, die dieses Dokument oben zieht.
- Anzahl zurückgehaltener Punkte: **0** statt der geplanten 2 — dokumentierte Abweichung
  (Feldrealität, siehe Vokabular-Korrektur oben).
- Out-of-Bounds-Anteil: **18.61%** der hp-02-Zeilen (28.969 von 155.685), gleiche Interpretation
  wie bei hp-01 (Randpersonen, kein Projektionsartefakt).

## Wiederverwendung

Diese Kalibrierung gehört zum festen Hover-Setup (Kameraposition, Neigung, Zoom), nicht zu einer
einzelnen Aufnahme. Solange dasselbe physische Setup wiederverwendet wird, gilt dieselbe Homographie
unverändert für jedes spätere Filmmaterial — deshalb liegt `homography_calibration.csv` unter
`data/reference/` (session-übergreifend gültig) und nicht unter einem Run-Output-Verzeichnis. Plan
2.3 (Coaching-Metriken auf XY-Tracks) liest diese Kalibrierung unverändert weiter, solange Drohne und
Hover-Position dieselben bleiben; ein neuer Hover-Aufbau (andere Position, anderer Winkel, anderer
Zoom) braucht eine neue Kalibrierung mit eigener `hover_position_id`.

## Per-Clip-Homographie-Verfeinerung (Drift-Korrektur, 2026-08-30)

**Befund:** "Wiederverwendung" oben nennt die Annahme wörtlich: dieselbe Homographie gilt für jedes
Filmmaterial derselben Hover-Position. Grid-Overlay-Diagnosen (das kalibrierte Raster über einen Frame
gelegt) zeigten, dass diese Annahme nicht exakt stimmt: das Raster passt auf den jeweiligen
Kalibrierungs-Referenzclip (hp-01: Clip 028, hp-02: Clip 044) pixelgenau, sitzt aber auf anderen Clips
derselben Hover-Position teils deutlich daneben — die Drohne driftet und dreht sich zwischen Clips
(handgehalten/manuell gehovert, kein fest montiertes Stativ), auch wenn "dieselbe Hover-Position"
grob dieselbe physische Position meint.

**Fix:** `homography.clip_alignment(clip_frame, reference_frame)` registriert einen repräsentativen
Frame jedes Clips (Clip-Mittelpunkt) gegen einen frisch aus dem Referenzclip extrahierten sauberen
Frame (NICHT das mit Rasterlinien annotierte `*_ref.jpg`) über SIFT-Merkmale + Ratio-Test-Matching +
`cv2.findHomography(..., cv2.RANSAC)`, mit einem Plausibilitäts-Filter auf Rotation/Determinante (siehe
Modul-Docstring `homography.py`) und Fallback auf Identität + Warnung, wenn keine ausreichend
plausible, gut gestützte Lösung gefunden wird. `coordinates.composed_transformer_for` verkettet dieses
`H_align` mit der kalibrierten Homographie (`M_gesamt = M_kalibriert @ H_align`) — sowohl für
Pipeline-Tracks (`coordinates.add_field_coordinates`) als auch für Ground-Truth-Punkte
(`accuracy._transform_gt_to_yards`), damit beide Seiten der Genauigkeitsmessung dieselbe Korrektur
durchlaufen.

**Drift-Verteilung** (`scripts/clip_alignment_diagnostics.py drift`, alle 59 Nicht-Referenzclips beider
Hover-Positionen, `data/processed/experiments/clip_alignment_drift.csv`): 31 von 59 Clips (53%) erhalten
eine plausible, gut gestützte Korrektur; 28 (47%) fallen sicher auf Identität zurück (keine Verschlechterung
gegenüber vorher, nur keine Korrektur). Unter den korrigierten Clips: Translation (gemessen am
Bildmittelpunkt) Median 243px / p90 382px / Max 891px; Rotation (absolut) Median 4.7° / p90 9.6° /
Max 21.6°. Diese Größenordnung — teils fast 900 Pixel, fast 22 Grad — bestätigt, dass die
Einzelhomographie-Annahme für mehrere Clips deutlich verletzt war, keine Kleinigkeit.

**Intra-Clip-Drift** (`scripts/clip_alignment_diagnostics.py intra-clip`, drei Stichprobenclips, früher
vs. später Frame desselben Clips): Clip 11 14.1px/0.10°, Clip 30 11.5px/0.03°, Clip 50 5.9px/-0.06°.
Alle drei unter der vereinbarten 15px-Schwelle (Clip 11 knapp darunter) — Drift INNERHALB eines Clips
ist deutlich kleiner als Drift ZWISCHEN Clips derselben Hover-Position. Eine Pro-Frame-Ausrichtung ist
damit (noch) nicht gerechtfertigt; sollte künftiges Material regelmäßig über 15px intra-Clip-Drift
zeigen, ist das nachzumessen und neu zu entscheiden.

**Grid-Overlay-Validierung** (`scripts/clip_alignment_diagnostics.py grid`,
`data/processed/experiments/grid_check_clip{N}.jpg`, sechs benannte Clips): für Clip 2, 4, 5, 6, 13
sitzt das zusammengesetzte Raster nach der Korrektur sichtbar auf der Goalline/Endzonen-Kante — vorher
lag es teils hunderte Pixel daneben. Clip 11 (hp-01, gegen Referenzclip 28) fällt trotz mehrfacher
Ratio-Schwellen und Plausibilitätsprüfung auf Identität zurück: manuelles Template-Matching auf dem
gemeinsam sichtbaren Sternlogo bestätigte, dass eine plausible Transformation existiert (Skala ~0.85,
Rotation nahe null), aber die automatische SIFT/RANSAC-Suche fand dafür keine ausreichend gestützte,
plausible Lösung — vermutlich, weil Clip 11 und Referenzclip 28 nur einen schmalen, vom sich
wiederholenden Rasentexturmuster dominierten Überlappungsbereich teilen. Das ist der beabsichtigte
Sicherheits-Fallback (nie eine Ratewerttransformation), keine Regression: Clip 11 bleibt exakt beim
vorherigen (unkorrigierten) Stand.

**Genauigkeitsmessung vorher/nachher** (`ffep cv accuracy`, dieselben 250 GT-Punkte,
`data/reference/gt_positions.csv`; "vorher" = ausschließlich Einzelhomographie ohne Clip-Ausrichtung
für sowohl GT als auch Tracks, exakt der ursprüngliche Gate-Lauf; "nachher" = beide Seiten konsistent
über `composed_transformer_for`):

| Metrik | vorher | nachher |
|---|---|---|
| Median | 0.171 yd | 0.189 yd |
| p90 | 0.422 yd | 0.457 yd |
| Max | 1.527 yd | 1.527 yd |
| Match-Rate | 99.6% | 99.6% |

Das Gate-Distanzmaß selbst ändert sich praktisch nicht (Differenz im Rauschbereich). Das ist strukturell
erwartbar, keine Überraschung: `measure_position_error` vergleicht einen GT-Punkt mit dem nächstgelegenen
Track IM SELBEN Clip — ein für den ganzen Clip konstanter Kalibrierungsfehler betrifft GT-Punkt und
Track-Punkt exakt gleich und kürzt sich in der lokalen Distanz heraus. Dieses Maß allein hätte den
eingangs beschriebenen Drift nie sichtbar gemacht (Clip 11 zeigte VOR der Korrektur schon Median-Fehler
im gleichen Bereich wie jeder andere Clip) — das ist genau der Grund, warum die Grid-Overlay-Prüfung und
der Massstabs-Paar-Check als unabhängige, clip-übergreifende Belege nötig sind.

**Massstabs-Paare (Skalen-Check, vorzeichenbehafteter Fehler), vorher/nachher:**

| Paar | Clip | vorher | nachher |
|---|---|---|---|
| sp-1 | 1 | +0.570 yd | +1.813 yd |
| sp-2 | 2 | -0.126 yd | +0.090 yd |
| sp-3 | 2 | -0.328 yd | +1.621 yd |
| sp-4 | 2 | +0.405 yd | +3.543 yd |
| sp-6 | 5 | -0.867 yd | +0.675 yd |
| sp-7 | 5 | +0.200 yd | +2.722 yd |
| sp-8 | 6 | +0.102 yd | +0.148 yd |
| sp-99 | 8 (Identität) | -0.075 yd | -0.075 yd |

**Ehrlicher Befund, kein Schönfärben:** die Massstabs-Paare sind das Maß, das eine absolute
Kalibrierungsverbesserung tatsächlich zeigen könnte (anders als das lokale Positionsfehlermass oben) —
und hier ist das Ergebnis GEMISCHT, nicht durchweg besser. sp-2 und sp-6 verbessern sich (kleinerer
Betrag); sp-1, sp-3, sp-4, sp-7 verschlechtern sich deutlich (sp-4 z.B. von 0.4 auf 3.5 Yards Fehler
bei einer 16-Yard-Referenzdistanz). sp-99 bleibt unverändert (Clip 8 fällt auf Identität zurück, siehe
oben). Interpretation: die Per-Clip-SIFT-Registrierung korrigiert grobe, augenfällige Fehlausrichtung
(hunderte Pixel, falscher Feldbereich) zuverlässig — das bestätigen sowohl die Grid-Overlays als auch
die rohen Drift-Beträge oben —, liefert aber keine durchweg bessere Sub-Yard-Skalengenauigkeit: die aus
~15-40 SIFT-Korrespondenzen auf einem texturwiederholenden Rasenbild geschätzte Homographie trägt selbst
ein Restrauschen im Bereich weniger Prozent, das bei einzelnen Clips (1, 2, 5) die vorherige
Massstabs-Genauigkeit unterbietet. Das ist eine dokumentierte Grenze dieser Methode, keine verdeckte
Regression -- "Richtwert, kein Messprotokoll" gilt hier ausdrücklich auch für diesen Fix selbst.

**Reproduzierbarkeit:** `uv run python scripts/clip_alignment_diagnostics.py {drift|intra-clip|grid}`
(siehe Skript-Docstring). `data/processed/experiments/*.csv`/`*.jpg` sind gitignored (regenerierbare
Diagnose-Artefakte, keine Referenzdaten).
