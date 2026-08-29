# Positionsfehler-Messung — Pilot-Session Drohne (2026-05-16)

Maschinenlesbares Gegenstück: `data/reference/gt_positions.csv`
(`src/flag_football_ep/cv/accuracy.py`, `ffep cv accuracy --prepare` / `--measure`).

**Status: gemessen am 2026-08-29 gegen die v1-Pipeline-Tracks (`ffep cv accuracy --measure`) —
250 von einer Person hand-markierte Fusspositionen, 21 Frames, 12 Clips, 8 Massstabs-Paare.
Diese Zahlen sind explizit v1-Pipeline: ein Tracking-Rework (BoT-SORT/Team-Zuordnungs-Fix) läuft
parallel; eine Neu-Messung gegen den überarbeiteten Tracking-Output folgt. Der formale C-09-
Gate-Entscheid wird bis dahin nicht hier, sondern erst in Plan 02.1-17 nach der Neu-Messung
getroffen — dieses Dokument liefert die Messmaschinerie und den v1-Befund, nicht das Urteil.**

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

## Gemessener Positionsfehler

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
Muster (siehe `## Fehler pro Feldzone`). Die 4 unmatched GT-Punkte (1,6 % aller Punkte) sind Fälle ohne
Track-Fusspunkt innerhalb von 3 Yards im selben Frame — sie zählen in die Match-Rate, nie in die
Fehlerverteilung (T-2.1-37: ein stillschweigendes Verwerfen würde nur die einfachen Fälle
vermessen).

## Fehler pro Feldzone

| Feldzone | n | Median (yd) | p90 (yd) | Max (yd) | Match-Rate | Unmatched |
|---|---|---|---|---|---|---|
| west-endzone | 36 | 0,154 | 0,371 | 0,828 | 97,2 % | 1 |
| west-half | 120 | 0,151 | 0,325 | 0,621 | 100,0 % | 0 |
| midfield | 48 | 0,238 | 0,529 | **1,527** | 100,0 % | 0 |
| east-half | 46 | 0,163 | 0,410 | 0,557 | 93,5 % | 3 |
| east-endzone | 0 | — | — | — | — | — (keine Daten, s.o.) |

Der einzige über dem Schwellenwert liegende Einzelwert (Max 1,527 Yards) stammt aus `midfield`, unter
Hover-Position hp-02 (siehe `## Fehlerzerlegung`). Der Median dieser Zone (0,238 yd) bleibt trotzdem
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

## Fehlerzerlegung

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

## Grenzen

- **Ein Spiel, eine Session** (`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`) — keine Aussage über
  andere Spiele, Lichtverhältnisse oder Kamera-Setups.
- **Zwei Hover-Positionen, eine davon (hp-02) ohne unabhängige Homographie-Kontrolle** — siehe
  `## Fehlerzerlegung`.
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
- **v1-Pipeline-Zahlen:** ein Tracking-Rework (BoT-SORT/Team-Zuordnungs-Fix) läuft parallel zu dieser
  Messung. Diese Zahlen gelten für den zum Messzeitpunkt aktuellen Tracking-Output und werden nach dem
  Rework neu erhoben, bevor Plan 02.1-17 den formalen C-09-Gate-Entscheid trifft.
