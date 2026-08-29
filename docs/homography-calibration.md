# Homographie-Kalibrierung — Pilot-Session Drohne (2026-05-16)

**Status: Referenzbilder automatisiert exportiert am 2026-08-29 (`uv run --extra cv` gegen
`homography.pick_points`, ein repräsentativer Clip je Hover-Position) — Punktkorrespondenzen
ausstehend, Freigabe durch Nutzer-Checkpoint (Plan 02.1-13 Task 2).**

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

`homography.field_landmarks(config)` berechnet die feste 18-Namen-Landmarken-Vokabel
(`homography.FIELD_LANDMARKS`) aus `config.cv.field_length_yards` (50.0),
`field_width_yards` (25.0) und `endzone_yards` (10.0). Koordinatenkonvention (D-13):
**x = 0** an der Westgoalline, **x = 50** an der Ostgoalline (negative x bzw. x > 50 liegt in einer
Endzone); **y = 0** an der Südseitenlinie, **y = 25** an der Nordseitenlinie. "Süd"/"Nord",
"West"/"Ost" sind hier reine Konventionsnamen für die beiden Seitenlinien bzw. Spielfeldenden, nicht
zwingend die geografische Ausrichtung vor Ort.

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
| `yardline_10_south` | 10.0 | 0.0 |
| `yardline_10_north` | 10.0 | 25.0 |
| `yardline_20_south` | 20.0 | 0.0 |
| `yardline_20_north` | 20.0 | 25.0 |
| `yardline_30_south` | 30.0 | 0.0 |
| `yardline_30_north` | 30.0 | 25.0 |
| `yardline_40_south` | 40.0 | 0.0 |
| `yardline_40_north` | 40.0 | 25.0 |
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
- Punktkorrespondenzen: *ausstehend — Plan 02.1-13 Task 2 (Nutzer-Checkpoint)*.
- Reprojektionsabweichung (zurückgehaltene Landmarken): *ausstehend*.
- Anzahl zurückgehaltener Punkte: *ausstehend*.
- Out-of-Bounds-Anteil (aus `add_field_coordinates`-Notices): *ausstehend*.

### hp-02

- Repräsentativer Clip: **Clip 044** (`apparent_player_px_p50 = 30.0px`, exakt der Gruppen-Median von
  `hp-02`), Referenzbild-Zeitpunkt `at_second = 3.0`.
- Referenzbild: `data/labels/calibration/hp-02_ref.jpg`.
- Sichtbar im Referenzbild (visuelle Grobeinschätzung): Torpfosten und Endzonen-Beschriftung oben
  links (bestätigt Nähe zur Endzone, konsistent mit der in `docs/pilot-sighting.md` als
  "gezoomt/rotiert" beschriebenen Einstellung), eine Seitenlinie mit Team-Bank und einem orangen
  "40"-Pylon rechts im Bild, weites offenes Spielfeld ohne klar erkennbare zweite Seitenlinie im
  unteren Bildbereich.
- Punktkorrespondenzen: *ausstehend — Plan 02.1-13 Task 2 (Nutzer-Checkpoint)*.
- Reprojektionsabweichung (zurückgehaltene Landmarken): *ausstehend*.
- Anzahl zurückgehaltener Punkte: *ausstehend*.
- Out-of-Bounds-Anteil (aus `add_field_coordinates`-Notices): *ausstehend*.

## Wiederverwendung

Diese Kalibrierung gehört zum festen Hover-Setup (Kameraposition, Neigung, Zoom), nicht zu einer
einzelnen Aufnahme. Solange dasselbe physische Setup wiederverwendet wird, gilt dieselbe Homographie
unverändert für jedes spätere Filmmaterial — deshalb liegt `homography_calibration.csv` unter
`data/reference/` (session-übergreifend gültig) und nicht unter einem Run-Output-Verzeichnis. Plan
2.3 (Coaching-Metriken auf XY-Tracks) liest diese Kalibrierung unverändert weiter, solange Drohne und
Hover-Position dieselben bleiben; ein neuer Hover-Aufbau (andere Position, anderer Winkel, anderer
Zoom) braucht eine neue Kalibrierung mit eigener `hover_position_id`.
