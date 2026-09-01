# GTA-Stichprobe: menschlicher Blick auf die Overlays (Vorbereitung fuer BASE-04)

Diese Datei bereitet einen manuellen Stichprobencheck vor, BEVOR ueber die Zielmarke
(90 %) in Plan M2-02-03 Task 3 entschieden wird. Sie enthaelt keine eigene
Bewertung -- die Beurteilung der Overlays ist Aufgabe der Nutzerin/des Nutzers.

## Ausgangslage

`scripts/hackathon/measure_gta.py` (Plan M2-02-02) hat GTA (Global Tracklet
Association) auf allen 61 Clips gemessen. Die automatische Kontinuitaets-Kennzahl
liegt bei 61/61 (100,00 %) -- deutlich ueber BoT-SORTs automatischem Wert (57/61,
93,44 %). Diese Zahl ist aber NICHT durch eine menschliche Review bestaetigt
(`human_pass_k`/`human_pass_n` sind fuer die GTA-Zeile bewusst leer), und die
automatische Kennzahl misst nur Track-Laenge, nicht Identitaetskorrektheit --
genau der Fehlermodus, der bei BoT-SORT 39 von 46 Fails ausmacht. Bei GTA kamen
364 Merge-Operationen unter einem generischen (nicht sportspezifischen) Embedding
bei nur median 12 Crops/Track zustande; das koennte Tracks verschiedener
Spielerinnen faelschlich zusammengefuehrt haben, ohne dass dies bisher verifiziert
waere (Details: `docs/baseline-messung.md`, `notes`-Feld der GTA-Zeile in
`data/reference/baseline-methods/summary.csv`).

Diese Stichprobe verschafft vor der BASE-04-Entscheidung einen ersten, informellen
menschlichen Eindruck -- kein Ersatz fuer eine vollstaendige Review, aber genug, um
zu sehen, ob die 100-%-Zahl plausibel ist oder ob sich das befuerchtete
Ueber-Zusammenfuehren (over-merging) in den Videos zeigt.

## Auswahl der Clips

9 Clips aus `data/reference/continuity_review.csv`, geschichtet:

- **7 Fail-Clips**, alle mit dem dominanten Fehlermodus "Switch bei Ueberlagerung"
  (ID-Wechsel bei Spielerinnen-Ueberlappung, der haeufigste Fehlermodus in der
  BoT-SORT-Review), verteilt auf beide Hover-Positionen (`hp-01`, `hp-02`,
  siehe `data/reference/hover_positions.csv`).
- **2 Pass-Clips** als Kontrolle, je einer pro Hover-Position.

| Clip | Hover-Position | BoT-SORT-Urteil (Mensch) | id_switches | Grund der Auswahl |
|---|---|---|---|---|
| 9  | hp-01 | fail | 1 | dominanter Fehlermodus (Switch bei Ueberlagerung), hp-01 |
| 11 | hp-01 | pass | -- | Kontrolle: BoT-SORT bestand die Review, hp-01 |
| 19 | hp-01 | fail | 1 | dominanter Fehlermodus, hp-01 |
| 27 | hp-01 | fail | 1 | dominanter Fehlermodus, hp-01 |
| 37 | hp-01 | fail | 1 | dominanter Fehlermodus, hp-01 |
| 6  | hp-02 | pass | -- | Kontrolle: BoT-SORT bestand die Review, hp-02 |
| 21 | hp-02 | fail | 1 | dominanter Fehlermodus, hp-02 |
| 30 | hp-02 | fail | 1 | dominanter Fehlermodus, hp-02 |
| 45 | hp-02 | fail | 1 | dominanter Fehlermodus, hp-02 |

Alle sieben Fail-Clips tragen in `continuity_review.csv` exakt den Reviewer-Vermerk
"Switch bei Ueberlagerung" (kein Mischfall mit falscher Teamzugehoerigkeit oder
Kamera-Artefakt), damit die Stichprobe genau den einen Fehlermodus prueft, der laut
`docs/baseline-messung.md` fuer BoT-SORT 39 von 46 Fails ausmacht.

## Overlays

Gerendert mit `ffep cv overlay` aus den GTA-Tracks
(`data/processed/baseline-methods/gta/tracks.parquet`). Die GTA-Tracks liegen vor
dem Koordinaten-/Team-Zuordnungsschritt vor und enthalten daher keine
`foot_x_px`/`foot_y_px`-Spalten; diese wurden mit derselben Formel wie
`cv/track.py` (`foot_x_px = (bbox_x1+bbox_x2)/2`, `foot_y_px = bbox_y2`) ergaenzt
in einer Kopie unter
`data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/tracks_with_foot.parquet`
(gitignored). Team-Zuordnung fehlt im Overlay entsprechend (GTA macht keine
Team-Zuordnung); alle Boxen erscheinen in der "no team"-Farbe der Legende, das ist
erwartet und keine Fehlermeldung.

Alle Dateien liegen unter (gitignored, nicht im Repository):

```
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_006.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_009.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_011.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_019.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_021.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_027.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_030.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_037.mp4
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/gta-spotcheck/overlays/clip_045.mp4
```

Jede Box zeigt die GTA-`track_id`; ein Farbwechsel derselben Spielerin waehrend
der Ueberlappung ist genau das Signal, das gesucht wird.

## Beurteilungskriterium

Dieselbe Regel wie in `continuity_review.csv` (BoT-SORT-Review, Plan 02.2-03/-10):
ein Clip "besteht", wenn eine Spielerin ueber mindestens 90 % ihrer sichtbaren
Zeit im Clip dieselbe Track-ID behaelt, ohne einen ID-Wechsel bei einer
Ueberlappung. Ein einzelner ID-Wechsel bei einer Ueberlappung genuegt fuer "fail",
auch wenn die automatische Kennzahl (Track-Laenge) unauffaellig bleibt -- exakt der
Unterschied, der bei BoT-SORT zur Luecke zwischen 93,44 % (automatisch) und
24,59 % (menschlich) fuehrt.

## Ergebnis-Tabelle (bitte ausfuellen)

| Clip | BoT-SORT-Urteil | GTA-Urteil (pass/fail) | Notiz |
|---|---|---|---|
| 9  | fail | | |
| 11 | pass | | |
| 19 | fail | | |
| 27 | fail | | |
| 37 | fail | | |
| 6  | pass | | |
| 21 | fail | | |
| 30 | fail | | |
| 45 | fail | | |

Nach dem Ausfuellen: die neun Urteile dienen als informeller Hinweis fuer die
BASE-04-Entscheidung in Plan M2-02-03 Task 3 (Zielmarken-Diskussion), nicht als
vollstaendige, dokumentierte Review. Falls GTA hier auffaellig gut oder auffaellig
schlecht abschneidet, ist der naechste Schritt der bereits vorgesehene
`.planning/todos/pending/2026-09-01-menschliche-review-kandidatenverfahren.md`
(vollstaendige Review auf mehr Clips, Plan M2-3/DATA-03) -- kein Ersatz durch diese
Stichprobe.
