# Positionsfehler-Messung — Pilot-Session Drohne (2026-05-16)

Maschinenlesbares Gegenstück: `data/reference/gt_positions.csv`
(`src/flag_football_ep/cv/accuracy.py`, `ffep cv accuracy --prepare` / `--measure`).

**Status: v1 gemessen am 2026-08-29 gegen die ursprünglichen OC-SORT-Pipeline-Tracks
(`ffep cv accuracy --measure`), v2 nachgemessen am selben Tag gegen den nach dem
Kontinuitäts-Review gemergten BoT-SORT-/Torso-Crop-Gap-Fix (`faf75dd`/`b870a72`) —
250 von einer Person hand-markierte Fusspositionen, 21 Frames, 12 Clips, 8 Massstabs-Paare,
identisch für beide Messungen (`data/reference/gt_positions.csv` unverändert). Beide
Messungen bleiben hier nebeneinander erhalten (`## Gemessener Positionsfehler (v1)` = v1,
`## v2-Messung (BoT-SORT-/Torso-Crop-Gap-Fix)` = v2 mit denselben Tabellen). Der formale
C-09-Gate-Entscheid selbst wird nicht hier, sondern in Plan 02.1-17 getroffen — dieses
Dokument liefert die Messmaschinerie und beide Befunde, nicht das Urteil.**

## Zweck & Abgrenzung

Dieses Dokument misst C-09s zweites Kriterium ehrlich: wie weit die von der Pipeline berechneten
Spielerpositionen von hand-markierten, unabhängigen Fusspositionen abweichen — als gemessene
Verteilung (Median/p90/Max in Yards und Metern), nie als bloße Pass/Fail-Zahl (die "Richtwert, kein
Messprotokoll"-Ehrlichkeitshaltung, die `docs/capture-protocol.md` für dieses Projekt bereits
etabliert).

Nicht Teil dieses Dokuments: die eigentliche Homographie-Kalibrierung selbst (das leistet
`docs/homography-calibration.md`, Plan 02.1-13 — die hier gemessene Zahl zerlegt sich explizit gegen
deren Reprojektionsfehler, siehe `## Fehlerzerlegung`) und der formale Gate-Verdikt (das leistet Plan
02.1-17, nach der für das Tracking-Rework angekündigten Neu-Messung).

## Ground-Truth-Set

**250 hand-markierte Fusspositionen** über **21 Frames** aus **12 Clips**, von einer einzigen
Annotatorin/einem einzigen Annotator markiert — keine Zweit-Annotator-Übereinstimmungsmessung (IAA),
dieselbe bekannte Grenze wie beim Trainingsdatensatz (`docs/cv-setup.md` `### Datensatz`).

Ausgewählt wurde stratifiziert über Clips, Feldzonen und Hover-Positionen sowie bevorzugt an Momenten
mit vielen gleichzeitig getrackten Personen (`cv.accuracy.prepare_gt_frames`), damit ein Frame mehrere
GT-Punkte liefert:

| Kennzahl | Wert |
|---|---|
| GT-Punkte gesamt | 250 |
| Frames | 21 |
| Clips | 12 |
| Hover-Position hp-01 | 156 Punkte |
| Hover-Position hp-02 | 94 Punkte |
| `class_name = player` | 213 Punkte |
| `class_name = referee` | 37 Punkte |
| Massstabs-Paare (`scale_pair_id`) | 8 |

**Ehrliche Korrelations-Notiz zu Clip 8:** 7 der 21 Frames stammen aus Clip 8, auf 7 **aufeinander
folgenden** Frames (Frame-Index 1–7) mit nahezu identischen Spielerpositionen — statistisch keine 7
unabhängigen Stichproben, sondern eine einzige Spielszene, mehrfach markiert. Ehrlich gerechnet sind
es damit eher **~15 effektiv unabhängige Szenen** (21 nominelle Frames minus 6 der 7 stark
korrelierten Clip-8-Frames), auch wenn die nominelle Punktzahl (250) weiterhin klar über der
Mindestschwelle von 50 (`cv.accuracy.measure_position_error`) und über dem Zielwert von 200 liegt.

**Feldzonen-Abdeckung — 4 von 5:** `west-endzone`, `west-half`, `midfield`, `east-half` sind vertreten;
**`east-endzone` ist leer.** Das ist keine Auslassung beim Labeln, sondern ein bestätigter
Aufnahme-Befund: kein einziger der 341.461 getrackten Track-Zeilen dieser Session (beide
Hover-Positionen) projiziert je über x = 44.2 Yards hinaus (Feldlänge 50 Yards) — das gefilmte
Spielgeschehen kam der echten Ost-Endzone in dieser Aufnahme nie nahe genug. Das ist eine
**Phase-2.0-Rückmeldung zum Aufnahme-Setup** (Hover-Position/Kameraausrichtung deckte dieses Ende des
Felds über die gesamte Session nicht ab), keine Pipeline- oder Labeling-Schwäche dieses Plans.

## Gemessener Positionsfehler (v1)

Schwellenwert C-09: ~1 m ≈ **1,094 Yards**.

| Kennzahl | Yards | Meter |
|---|---|---|
| Median | 0,169 | 0,154 |
| p90 | 0,415 | 0,380 |
| Max | 1,527 | 1,396 |
| **Schwellenwert (~1 m)** | **1,094** | **1,000** |
| Match-Rate | 98,4 % (246/250) | — |
| Unmatched (kein Track-Punkt < 3 Yards) | 4 | — |

Median und p90 liegen klar unter dem ~1-m-Schwellenwert (Median bei 15 % davon, p90 bei 38 %) — die
typische Positionsgenauigkeit dieser v1-Pipeline ist deutlich besser als das Kriterium verlangt. Der
Max-Wert (1,527 Yards / 1,396 m) liegt **über** dem Schwellenwert: ein einzelner Ausreisser, kein
Muster (siehe `## Fehler pro Feldzone (v1)`). Die 4 unmatched GT-Punkte (1,6 % aller Punkte) sind Fälle ohne
Track-Fusspunkt innerhalb von 3 Yards im selben Frame — sie zählen in die Match-Rate, nie in die
Fehlerverteilung (T-2.1-37: ein stillschweigendes Verwerfen würde nur die einfachen Fälle
vermessen).

## Fehler pro Feldzone (v1)

| Feldzone | n | Median (yd) | p90 (yd) | Max (yd) | Match-Rate | Unmatched |
|---|---|---|---|---|---|---|
| west-endzone | 36 | 0,154 | 0,371 | 0,828 | 97,2 % | 1 |
| west-half | 120 | 0,151 | 0,325 | 0,621 | 100,0 % | 0 |
| midfield | 48 | 0,238 | 0,529 | **1,527** | 100,0 % | 0 |
| east-half | 46 | 0,163 | 0,410 | 0,557 | 93,5 % | 3 |
| east-endzone | 0 | — | — | — | — | — (keine Daten, s.o.) |

Der einzige über dem Schwellenwert liegende Einzelwert (Max 1,527 Yards) stammt aus `midfield`, unter
Hover-Position hp-02 (siehe `## Fehlerzerlegung (v1)`). Der Median dieser Zone (0,238 yd) bleibt trotzdem
klar im Budget — ein einzelner Ausreisser hebt den Max-Wert, nicht die zentrale Tendenz. `east-half`
hat mit 93,5 % die niedrigste Match-Rate (3 von 46 unmatched) — plausibel am Rand der hp-01/hp-02-
Sichtfelder, wo ein Track gelegentlich fehlt oder ausserhalb des 3-Yard-Suchradius liegt, aber keine
strukturelle Häufung, die auf ein Kamerageometrie-Problem hindeutet (im Sinne von D-06 wäre ein
zonenspezifisches, durchgängiges Versagen der Befund, der auf Phase 2.0 zurückrouten würde — das zeigt
sich hier nicht).

## Massstabs-Kontrolle

8 vom Nutzer per Augenmass geschätzte Distanzen zwischen zwei Fusspunkten (z. B. beide Füsse auf
derselben Yardlinie, oder ein bekannter Feld-Abstand) — **ausdrücklich Schätzungen ("Richtwert"), keine
präzise Vermessung.** Die präzise, unabhängige Massstabskontrolle der Homographie selbst bleibt die
zurückgehaltene Landmarke aus der Kalibrierung (`docs/homography-calibration.md`: hp-01
Reprojektionsfehler 0,27 Yards / 0,25 m an einem echten, gemessenen Feld-Landmark). Die folgende
Tabelle prüft stattdessen etwas anderes: ob die Homographie-Projektion *im Feld selbst* mit vom
Menschen grob eingeschätzten Distanzen übereinstimmt, als Plausibilitätscheck, nicht als
Präzisionsmessung.

| Paar | Gemessen (yd) | Geschätzt/wahr (yd) | Signierter Fehler (yd) |
|---|---|---|---|
| sp-1 | 4,570 | 4,0 | +0,570 |
| sp-2 | 0,874 | 1,0 | -0,126 |
| sp-3 | 14,672 | 15,0 | -0,328 |
| sp-4 | 16,405 | 16,0 | +0,405 |
| sp-6 | 10,133 | 11,0 | -0,867 |
| sp-7 | 15,200 | 15,0 | +0,200 |
| sp-8 | 0,902 | 0,8 | +0,102 |
| sp-99 | 2,425 | 2,5 | -0,075 |

Mittlerer signierter Fehler: **-0,015 Yards** — nahe null und die Vorzeichen wechseln unsystematisch
(4x positiv, 4x negativ), also **kein Hinweis auf eine systematische Skalen-Verzerrung** der
Homographie. Die grösste Einzelabweichung (sp-6, -0,867 yd auf ein geschätztes 11-Yard-Paar, ~7,9 %
relativ) liegt im erwartbaren Bereich für eine über Augenmass geschätzte, nicht real vermessene
Distanz — die Streuung dieser Tabelle spiegelt vor allem die Schätzunsicherheit des Nutzers wider, nicht
zwingend die Homographie-Güte. Die einzige Zahl in diesem Dokument, die eine echte, unabhängig
vermessene Distanz gegen die Homographie prüft, bleibt `docs/homography-calibration.md`s 0,27-Yard-
Reprojektionsfehler.

## Fehlerzerlegung (v1)

Der oben gemessene Positionsfehler (Median 0,169 / p90 0,415 / Max 1,527 Yards) ist **der
Fusspunkt-Fehler der Pipeline unter der eigenen Homographie des Projekts** — nicht die
Gesamtabweichung von der wahren Feldposition. Beide Seiten des Vergleichs (der von Hand markierte
GT-Fusspunkt und der von der Pipeline getrackte Fusspunkt) laufen durch **dieselbe**
Hover-Positions-Homographie (`homography.transformer_for`), sodass ein grosser Teil einer
etwaigen Homographie-Verzerrung sich in der paarweisen Distanz aufhebt — was gemessen wird, ist primär
die Differenz zwischen "wo der Mensch die Füsse sieht" und "wo die Pipeline (Detektion + Fusspunkt-
Extraktion + Tracking) sie platziert".

Die Homographie selbst trägt eine **eigene, nicht in obiger Zahl enthaltene Unsicherheit**, gemessen in
`docs/homography-calibration.md` an zurückgehaltenen (nicht in den Fit eingeflossenen) Landmarken:

- **hp-01:** 0,27 Yards (0,25 m) Reprojektionsfehler an 1 zurückgehaltenem Landmark.
- **hp-02:** **keine unabhängige Reprojektionsmessung** — die Homographie ist mit genau 4 Punkten
  exakt bestimmt (Restfehler 0 per Konstruktion), es gibt keinen zurückgehaltenen Kontrollpunkt. Ihre
  Güte ist ausschliesslich über die vorliegende End-to-End-Messung sichtbar (wie in
  `docs/homography-calibration.md` bereits vermerkt).

Aufgeschlüsselt nach Hover-Position bestätigt das die obige Erwartung — hp-02 (ohne unabhängige
Kalibrierungs-Kontrolle) zeigt einen etwas höheren gemessenen Fehler als hp-01:

| Hover-Position | n | Median (yd) | p90 (yd) | Max (yd) |
|---|---|---|---|---|
| hp-01 | 156 | 0,152 | 0,365 | 0,828 |
| hp-02 | 94 | 0,206 | 0,452 | 1,527 |

**Kombinierter Worst-Case (additiv, konservativer Richtwert — keine strenge statistische
Fehlerfortpflanzung):** für hp-01 lässt sich der gemessene Fehler mit dem bekannten
Reprojektionsfehler zu einer konservativen oberen Schranke addieren: Median 0,152 + 0,27 ≈ **0,42 yd**
(0,39 m), p90 0,365 + 0,27 ≈ **0,64 yd** (0,58 m), Max 0,828 + 0,27 ≈ **1,10 yd** (1,00 m) — bemerkenswert
nah am Schwellenwert selbst im additiven Worst-Case. Für hp-02 gibt es keine separate Reprojektions-
zahl zum Addieren; die oben gemessenen 0,206 / 0,452 / 1,527 Yards **sind** bereits die einzige
verfügbare End-to-End-Güteaussage dieser Homographie, und ihr Max-Wert (1,527 yd / 1,396 m) liegt
**bereits ohne jede Addition über dem ~1-m-Schwellenwert** — der einzige Einzelpunkt in diesem
gesamten Datensatz, bei dem das zutrifft. Diese Zahl wird hier offen berichtet, nicht unter der
Aggregat-Statistik versteckt.

## v2-Messung (BoT-SORT-/Torso-Crop-Gap-Fix)

Nachgemessen am 2026-08-29 gegen denselben GT-Satz (`data/reference/gt_positions.csv`,
250 Punkte, 21 Frames, 12 Clips, 8 Massstabs-Paare — unverändert, keine Neu-Labelling
nötig), nachdem die beiden Gap-Fixes aus dem menschlichen Kontinuitäts-Review gemergt
wurden: `faf75dd` (Tracker-Wechsel OC-SORT -> getuntes `trackers.BoTSORTTracker`) und
`b870a72` (`extract_track_crops`, Torso-Region-Crops statt Vollkörper-Crops für die
Team-Zuordnung — betrifft `team_id`, nicht die hier gemessenen Positionen direkt). Volle
Lauf-Details (Track-Zahlen, Stage-Timing, C-09-Laufzeit-Extrapolation) stehen in
`docs/cv-setup.md`s `### v2 (BoT-SORT-Tracker, Torso-Crops -- Gap-Fix-Iteration nach dem
Kontinuitäts-Review)`.

### Gemessener Positionsfehler (v2)

| Kennzahl | v1 (Yards) | v2 (Yards) |
|---|---|---|
| Median | 0,169 | 0,171 |
| p90 | 0,415 | 0,422 |
| Max | 1,527 | 1,527 |
| **Schwellenwert (~1 m)** | **1,094** | **1,094** |
| Match-Rate | 98,4 % (246/250) | **99,6 % (249/250)** |
| Unmatched | 4 | **1** |

Median und p90 verschieben sich nur marginal (+0,002 / +0,007 yd) — im Rauschen der
Messung, kein praktisch bedeutsamer Unterschied. Der Max-Wert ist **exakt identisch**
(1,527 yd): derselbe Ausreisser-GT-Punkt in `midfield`/hp-02 matcht in beiden Läufen
denselben Track-Fusspunkt, weil die zugrundeliegende Detektion (nicht die Tracker-
Zuordnung) diesen Fehler verursacht — siehe `### Fehlerzerlegung (v2)` unten. Die
Match-Rate verbessert sich klar (98,4 % -> 99,6 %, 3 der 4 vormals unmatched GT-Punkte
finden jetzt einen Track-Fusspunkt im 3-Yard-Suchradius): BoT-SORTs längerer
`lost_track_buffer` (90 Frames) lässt Tracks über kurze Verdeckungen/Lücken hinweg
bestehen statt sie zu beenden, wodurch mehr GT-Frames einen passenden Track vorfinden.

### Fehler pro Feldzone (v2)

| Feldzone | n | Median (yd) v1 | Median (yd) v2 | p90 (yd) v2 | Max (yd) v2 | Match-Rate v2 | Unmatched v2 |
|---|---|---|---|---|---|---|---|
| west-endzone | 36 | 0,154 | 0,154 | 0,371 | 0,828 | 97,2 % | 1 |
| west-half | 120 | 0,151 | 0,151 | 0,325 | 0,621 | 100,0 % | 0 |
| midfield | 48 | 0,238 | 0,238 | 0,529 | **1,527** | 100,0 % | 0 |
| east-half | 46 | 0,163 | 0,196 | 0,460 | 0,944 | **100,0 %** | **0** |
| east-endzone | 0 | — | — | — | — | — | — (weiterhin keine Daten) |

`west-endzone`, `west-half` und `midfield` sind zwischen v1 und v2 praktisch unverändert
(identische Detektionen, nur die Tracker-Zuordnung änderte sich, und für diese drei Zonen
matchte der greedy nearest-neighbour in beiden Läufen denselben Fusspunkt). `east-half`
ist die einzige Zone mit einem sichtbaren Unterschied: die Match-Rate springt von 93,5 %
(3 von 46 unmatched) auf **100 %** — aber Median/p90/Max steigen zugleich leicht (0,163 ->
0,196 / 0,410 -> 0,460 / 0,557 -> 0,944 yd). Das ist kein Widerspruch: die 3 vormals
unmatched GT-Punkte in dieser Zone erhalten jetzt einen Track-Fusspunkt (BoT-SORTs
Coverage-Gewinn am Rand der hp-01/hp-02-Sichtfelder), aber diese neu gematchten Punkte
tragen einen grösseren Fehler als die bereits zuvor gematchten — sie ziehen den Median und
den Max-Wert der Zone nach oben. Insgesamt ein ehrlicher Tradeoff: mehr Coverage
(weniger Blindstellen für spätere Analysen), leicht höhere Zonen-Fehlerzahlen an genau
den Rand-Punkten, die vorher schlicht fehlten. Kein zonenspezifisches, durchgängiges
Versagen (kein D-06-Befund).

### Massstabs-Kontrolle (v2)

Unverändert von v1 — die Massstabs-Paare sind reine GT-zu-GT-Distanzen durch dieselbe
Homographie projiziert (`## Massstabs-Kontrolle` oben) und hängen nicht vom
Tracking-Output ab. Kein neuer Lauf nötig; dieselben 8 Werte (mittlerer signierter Fehler
-0,015 yd) gelten unverändert für v2.

### Fehlerzerlegung (v2)

Wie bei v1 (`## Fehlerzerlegung (v1)`) ist der gemessene Fehler primär die Differenz
zwischen "wo der Mensch die Füsse sieht" und "wo die Pipeline sie platziert", nicht die
Homographie-Güte selbst. Aufgeschlüsselt nach Hover-Position:

| Hover-Position | n | Median (yd) v1 | Median (yd) v2 | p90 (yd) v2 | Max (yd) v2 | Match-Rate v2 |
|---|---|---|---|---|---|---|
| hp-01 | 156 | 0,152 | 0,152 | 0,365 | 0,828 | 99,4 % |
| hp-02 | 94 | 0,206 | 0,210 | 0,479 | 1,527 | **100,0 %** |

hp-01 ist zwischen v1 und v2 **exakt identisch** (Median/p90/Max) — dieselbe Detektion,
derselbe greedy-Match für alle 156 GT-Punkte dieser Hover-Position, nur die Match-Rate
sinkt marginal (100 % -> 99,4 %, ein einzelner GT-Punkt bleibt jetzt unmatched statt vorher
gematcht; keine praktische Bedeutung bei n=156). hp-02 verschiebt sich minimal nach oben
(Median 0,206 -> 0,210 yd, p90 0,452 -> 0,479 yd), erreicht aber jetzt **100 % Match-Rate**
(vorher waren einige der v1-weiten 4 Unmatched-Punkte hier). Der Max-Wert (1,527 yd) bleibt
identisch — derselbe Einzelpunkt, weiterhin **über** dem ~1-m-Schwellenwert ohne jede
Addition, weiterhin offen berichtet statt versteckt. Für hp-01 bleibt der additive
Worst-Case aus v1 unverändert gültig (Median 0,152 + 0,27 ≈ 0,42 yd; p90 0,365 + 0,27 ≈
0,64 yd; Max 0,828 + 0,27 ≈ 1,10 yd), da die zugrundeliegenden Rohwerte identisch sind.

## Grenzen

- **Ein Spiel, eine Session** (`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`) — keine Aussage über
  andere Spiele, Lichtverhältnisse oder Kamera-Setups.
- **Zwei Hover-Positionen, eine davon (hp-02) ohne unabhängige Homographie-Kontrolle** — siehe
  `## Fehlerzerlegung (v1)` / `### Fehlerzerlegung (v2)`.
- **Eine Annotatorin/ein Annotator, keine IAA-Messung** — dieselbe Grenze wie beim Trainingsdatensatz
  (`docs/cv-setup.md`).
- **Hand-markierte Fusspunkte tragen eigene Markierungsunsicherheit.** Bei den hier typischen
  Bildgrössen (`apparent_player_px_p50` 25–61px, `data/reference/hover_positions.csv`) entspricht ein
  angenommener Klick-Fehler von ±3–5px, projiziert durch die Homographie, überschlägig etwa
  0,05–0,15 Yards (5–15 cm) — deutlich kleiner als der gemessene Median (0,169 yd), aber in derselben
  Grössenordnung wie der p90-Wert, also nicht vernachlässigbar für die Interpretation der unteren
  Fehlerbandbreite.
- **Massstabs-Paare sind Augenmass-Schätzungen** (siehe `## Massstabs-Kontrolle`), keine Ersatzmessung
  für die Homographie-Kalibrierung selbst.
- **`east-endzone` hat keine Daten** — Aufnahme-Abdeckungslücke dieser konkreten Session, siehe
  `## Ground-Truth-Set`. Als Phase-2.0-Feedback zu behandeln, nicht als Pipeline-Schwäche.
- **v1/v2-Pipeline-Zahlen, formaler Gate-Entscheid noch offen:** das Tracking-Rework
  (BoT-SORT-Tracker-Wechsel `faf75dd` + Torso-Crop-Team-Zuordnung `b870a72`) ist inzwischen
  gemergt und v2 wurde gegen denselben GT-Satz nachgemessen (`## v2-Messung
  (BoT-SORT-/Torso-Crop-Gap-Fix)` unten). Beide Zahlensätze stehen hier nebeneinander; der
  formale C-09-Gate-Entscheid selbst wird weiterhin erst in Plan 02.1-17 getroffen.

## Showcase-Reel

Erzeugt von `src/flag_football_ep/cv/radar.py` (Plan 02.1-16, D-16): ein Seite-an-Seite-Reel aus
`overlay.draw_frame`s annotiertem Drohnen-Footage (links) und einer synchronisierten Top-Down-Radar-
Ansicht der Feld-Yard-Positionen (rechts), gedacht als HC-/Analyst-Demo-Asset, nicht als Messung.

### Auswahlregel

Die gezeigten Clips wurden nicht nach Geschmack ausgesucht, sondern per fester, reproduzierbarer Regel
aus `data/reference/continuity_review.csv` (dem Kontinuitäts-Review aus Plan 02.1-14) gezogen:

1. Filtere auf `verdict = pass` und `id_switches = 0` (in `continuity_review.csv` für alle
   Pass-Verdikt-Clips als leeres Feld eingetragen — konsistent damit, dass jeder Fail-Verdikt-Clip
   einen expliziten, von null verschiedenen `id_switches`-Wert trägt).
2. Sortiere nach `longest_track_frac` absteigend (bei diesem Datensatz für alle 6 Pass-Clips auf
   1.0 gebunden — die Regel bleibt für zukünftige Läufe wirksam, in denen dieser Wert
   diskriminiert), dann nach `n_fragments` aufsteigend als reproduzierbarer Tie-Breaker (weniger
   Track-Fragmente = ein sauberer durchgängiger Track), dann nach `clip_number` aufsteigend als
   letzter, deterministischer Tie-Breaker.
3. Stelle sicher, dass mindestens ein Clip pro Hover-Position vertreten ist, sobald in den
   Pass-Kandidaten mehr als eine vorkommt (hier: hp-01 und hp-02) — nötigenfalls würde der
   bestplatzierte Kandidat der fehlenden Hover-Position gegen den schwächsten der bereits gewählten
   getauscht; bei diesem Lauf war kein Tausch nötig (siehe Tabelle unten).
4. Bevorzuge unter gleichrangigen Kandidaten Clips, deren `reviewer_note` echtes Spielgeschehen statt
   Huddle/Stillstand belegt — bei diesem Lauf trägt keiner der 6 Pass-Clips eine `reviewer_note`, das
   Kriterium war für diese Auswahl also inert (für zukünftige Läufe dennoch Teil der festen Regel).
5. Nimm die Top 5 (Ober­grenze der D-16-Spanne 3–5 Clips).

Von 61 Clips erfüllen 6 `verdict = pass` und `id_switches = 0` (Clips 1, 2, 4, 6, 11, 13). Die Regel
wählt daraus 5; Clip 1 fällt als letztplatzierter (11 Fragmente, die meisten der Pass-Gruppe) heraus.

### Gewählte Clips

| Clip | Hover-Position | `longest_track_frac` | `n_fragments` | `n_tracks` |
|---|---|---|---|---|
| 11 | hp-01 | 1,0 | 2 | 18 |
| 2  | hp-01 | 1,0 | 3 | 28 |
| 6  | hp-02 | 1,0 | 7 | 27 |
| 13 | hp-02 | 1,0 | 9 | 20 |
| 4  | hp-02 | 1,0 | 10 | 27 |

Beide Hover-Positionen sind vertreten (hp-01: 2 Clips, hp-02: 3 Clips).

### Render

```
ffep cv radar \
  --tracks data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet \
  --clip 11 --clip 2 --clip 6 --clip 13 --clip 4 \
  --out data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/showcase/showcase.mp4
```

| Kennzahl | Wert |
|---|---|
| Ausgabepfad | `data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/showcase/showcase.mp4` (gitignored, PII) |
| Dauer | 46,1 s |
| Auflösung | 3840 x 1120 px (2 x 1920 px Breite je Hälfte + 40 px Kopfzeile, 1080 px Höhe) |
| Bildrate | 30 fps |
| Frames gesamt | 1383 (357+285+267+220+234 Quell-Frames der 5 Clips + 4 x 5 schwarze Trenn-Frames zwischen den Plays) |

End-to-end per Frame-Stichprobe geprüft (Frame 0, 50, 200, 360, 700, 1000, 1382): jede der 5 Clip-
Segmente zeigt links das annotierte Drohnen-Footage (Team-Boxen, Track-IDs, Schiedsrichter-Box gelb),
rechts synchron die Radar-Ansicht (Feldlinien alle 5 Yards, dieselben Team-Farben/Marker-Formen —
Kreis für Spielerinnen, Dreieck für Schiedsrichter, Quadrat für Tracks ohne Team-Zuordnung); die
Kopfzeile zählt Clip-Nummer und Play-Index (`clip 11 -- play 1/5` ... `clip 4 -- play 5/5`) korrekt
hoch; an den Clip-Grenzen liegt jeweils ein kurzer schwarzer Trenn-Abschnitt.

**Dieses Reel zeigt die BESTEN Plays der Pipeline und ist damit Demonstrations-Evidenz, keine
Leistungsmessung — die gemessenen Zahlen stehen in `## Gemessener Positionsfehler (v1)`/`### Gemessener
Positionsfehler (v2)` oben und in `data/reference/continuity_review.csv` (61-Clip-Kontinuitäts-Review,
Plan 02.1-14: nur 6 von 20 reviewten Clips bestehen, konservative Ober­schranke 77 % < 90 %-Zielwert).**

### Präsentations-Fix (nach diesem Lauf, orchestrator-angeordnet)

Der oben beschriebene Render (46,1 s, Frame-Stichprobe geprüft) stammt aus VOR dem
Präsentations-Fix in `docs/cv-setup.md`s `#### Präsentations-Fix: Team-Farb-Anker +
Radar-Label-Z-Order` und weist beide dort beschriebenen Bugs auf: die Anzeige-Palette
zeichnete `team_id` 1 rot / `team_id` 0 blau (ohne Garantie, dass das die tatsächliche
Trikotfarbe traf), und bei eng beieinander stehenden Markern im Radar konnte eine später
gezeichnete Marker-Form die Track-Nummer eines früher gezeichneten Markers übermalen
("blau hat keine Zahlen"). `assign_teams`s Team-Zuordnung selbst (`team_id`-Werte je
Track) war von diesen Bugs nicht betroffen und musste beim Re-Lauf nicht geändert werden
(siehe `docs/cv-setup.md` für die Zähler-Bestätigung) -- nur die Anzeige-Palette und die
Radar-Zeichenreihenfolge wurden korrigiert.

Alle 61 Overlays und dieses Showcase-Reel blieben zunächst UNVERÄNDERT (alter Render,
alte Palette) bis der separate, ebenfalls gemeldete Spiegelungs-Bug (Süd-/Nord-Seitenlinie
der Kalibrierung vertauscht, siehe `docs/homography-calibration.md`) behoben war -- beide
Fixes wurden dann gemeinsam in einem einzigen Re-Render zusammengeführt, um nicht zweimal
alle 61 Clips plus das Reel neu zu rendern.

### Finaler kombinierter Re-Render (2026-08-30, orchestrator-angeordnet)

Der ausstehende Re-Render ist jetzt erledigt. Alle 61 Overlays und dieses Showcase-Reel
wurden mit dem aktuellen Tracking-Parquet-Stand neu gerendert -- korrekte Team-Farben
(Farb-Anker-Fix), korrekte (nicht mehr gespiegelte) Radar-Orientierung
(Kalibrierungs-Fix), alle Track-Nummern lesbar, und ein neuer On-Field-Radarfilter
(`cv/radar.py::_is_on_field`): Seitenlinien-/Bank-Personen (absichtlich getrackt, ~25 %
der Zeilen) werden aus der Radar-Zeichnung gefiltert, ohne die zugrundeliegenden
Tracking-Daten zu verändern.

Die Radar-Orientierung wurde empirisch anhand Clip 5, Frame 51 verifiziert (Footage
neben Radar in `data/processed/experiments/orientation_check_c5f51.png`): der Track
nahe am LINKEN Bildrand des Footage liegt bei `y_yards≈25,6` (nahe der Nordseitenlinie,
oben im Radar); der Track nahe am RECHTEN Bildrand liegt bei `y_yards≈0,9` (nahe der
Südseitenlinie, unten im Radar) -- beide bestätigen die erwartete, nicht mehr
gespiegelte Orientierung. Details siehe `docs/cv-setup.md`, Abschnitt "Finaler
kombinierter Re-Render".
