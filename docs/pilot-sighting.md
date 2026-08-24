# Sichtungsprotokoll — Pilot-Session Drohne (2026-05-16)

Maschinenlesbares Gegenstück: `data/reference/hover_positions.csv` (`ffep cv sight`).

**Status: automatisiert erzeugt am 2026-08-24 (`uv run --extra cv ffep cv sight`, Korrelationsschwelle 0.05) — Review durch Nutzer ausstehend (Checkpoint Plan 02.1-03 Task 3, siehe Ratifizierungs-Block).**

## Zweck & Abgrenzung

Dieses Dokument beantwortet D-03 mit Messung statt Annahme: wie viele Hover-Positionen die 61
Piloten-Clips der Session `2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE` tatsächlich enthalten, wie
groß eine Spielerin darin im Bild erscheint, und welche Detektor-Auflösung (und ob SAHI-Tiling) die
Drohnen-Domäne daraus braucht. Die RESEARCH-Vorannahme "eine Hover-Position" stammte aus 3
stichprobenartig gesichteten Clips, nicht aus einer vollständigen Sichtung — dieser Durchlauf sichtet
alle 61.

Nicht Teil dieses Dokuments: die eigentliche Homographie-Kalibrierung (Plan 02.1-13 rechnet mit der
hier ermittelten Positionsanzahl weiter) und die Detektor-Trainingsentscheidung selbst (Plan 02.1-10).

## Hover-Positionen

`ffep cv sight` gruppiert Clips über eine normalisierte Kreuzkorrelation eines
Framing-Fingerprints (8 über den Clip verteilte Frames, grayscale, stark geglättet, auf 64x36
herunterskaliert, gemittelt) gegen den laufenden Gruppen-Mittelwert. Ergebnis: **2 Hover-Positionen**,
stabil über jede Schwelle im Bereich [-0.1, 0.1] (siehe Deviations-Notiz im Quellcode,
`src/flag_football_ep/cv/sighting.py::_CORRELATION_THRESHOLD`), stichprobenartig gegen die
Originalclips visuell verifiziert (Clips 1, 8, 51 zeigen die gleiche weite Rahmung mit sichtbarer
"40"-Yard-Markierung unten links; Clips 4, 13, 61 zeigen eine erkennbar andere, näher herangezoomte
Rahmung nahe der "PANAMA"-Endzonenbeschriftung). Kontaktabzüge (ein Referenzbild pro Position,
mit Positions-ID, Clip-Bereich und gemessener Größe annotiert) liegen unter
`data/labels/sighting/` (gitignored, PII).

| Position | Clip-Bereich | n Clips | Tier |
|---|---|---|---|
| hp-01 | 001–056 | 30 | Brauchbar |
| hp-02 | 004–061 | 31 | Brauchbar |

Die Clip-Nummern beider Positionen sind über die Session verschachtelt (kein sauberer Block "erste
Hälfte Position A, zweite Hälfte Position B") — vereinbar mit zwei Kamerastandpunkten, deren Aufnahme
je nach Spielrichtung/Spielfeldseite abwechselt, nicht mit einem einzigen Hover-Wechsel mitten in der
Session.

## Gemessene Spielergroesse

Methode: `cv2.createBackgroundSubtractorMOG2` über (nahezu) den gesamten Clip laufen lassen
(sequenzielle Frames, nicht über den Clip verstreute Einzelbilder — die Hintergrundmodellierung
braucht zeitliche Kontinuität), erste ~60 Frames als Aufwärmphase verwerfen, dann bewegte
Vordergrund-Komponenten sammeln: Flächen-Mindestgröße, Füllgrad- und Seitenverhältnis-Filter gegen
dünne Feldlinien-/Kompressionskanten-Artefakte, Höhen-Deckel bei 1/4 der Bildhöhe gegen
Tribünen-/Anzeigetafel-Artefakte, Rand-Ausschluss. Ergebnis pro Clip: `apparent_player_px_p10`/`_p50`
(10.-Perzentil / Median der erkannten Blob-Höhe in Pixeln).

**Ausdrücklicher Hinweis: Richtwert, kein Messprotokoll.** MOG2-Hintergrundsubtraktion ist eine
Bewegungsheuristik, kein kalibriertes Messinstrument — sie erkennt vor allem Spielerinnen, die sich im
analysierten Fenster tatsächlich bewegen, und ihre Trennschärfe gegen Kompressionsrauschen ist
begrenzt. Alle Zahlen unten sind aus genau diesem Grund als Median/Perzentil über hunderte
Einzelmessungen pro Clip gebildet, nicht als Einzelwert.

| Größe | Wert |
|---|---|
| p10 über alle 61 Clips (Median der Clip-Werte) | 16.0 px |
| p50 über alle 61 Clips (Median der Clip-Werte) | 30.0 px |
| p50-Spanne über alle Clips | 25.0–61.0 px |
| p50 Median hp-01 | 30.0 px |
| p50 Median hp-02 | 30.0 px |

Beide Hover-Positionen liegen bei praktisch identischer gemessener Spielergröße — plausibel, da beide
aus vergleichbarer Flughöhe und vergleichbarem Abstand zum Spielfeld aufgenommen wurden, nur mit
unterschiedlichem Rahmungswinkel.

## Ableitung Inferenz-Einstellungen

`recommend_inference_settings` bildet den Median von `apparent_player_px_p50`/`_p10` über alle
Zeilen von `hover_positions.csv` und wendet die in Plan 02.1-03 festgelegten Bänder an:

> measured apparent player height p50=30.0px, p10=16.0px across 61 clips; band 20-40 px ->
> resolution=896, sahi=False

Empfohlen: **`resolution = 896`**, **`sahi = false`**. `896` ist ein Vielfaches von 224 (=
kgV(32, 56), gültig unter beiden dokumentierten RF-DETR-Teilbarkeitsregeln) und liegt über dem
aktuell in `ffep.toml` provisorisch eingetragenen Wert `672` — die gemessene Spielergröße (Median 30
px, p10 16 px) ist kleiner als der Wert, den `672` ohne Tiling zuverlässig auflöst. `sahi = false`
bleibt, weil p50 nicht unter die 20-px-Schwelle fällt, an der RESEARCH Pitfall 4 (SAHI+RF-DETR hat
eine offene Tuning-Lücke bei hoher Kachelauflösung) den Tiling-Zuschlag gegen das
`< 1 h/Spiel`-Laufzeitziel (C-09) gegenrechnet.

**Vorgeschlagene `ffep.toml`-Änderung (noch nicht angewendet):**

```diff
 [cv]
 pilot_session_id = "2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE"
 detector_model = "cv_detector_model"
 detector_experiment = "cv_detector"
-resolution = 672
+resolution = 896
 sahi = false
```

## Konsequenzen

Die Session enthält **2 bestätigte Hover-Positionen**. Plan 02.1-13 muss entsprechend **2
Homographien** manuell kalibrieren (eine pro Position, 4–8-Punkt-Korrespondenz je D-05) statt der von
RESEARCHs Stichprobe nahegelegten einen. Das ist ein kleiner, gut planbarer Mehraufwand gegenüber der
ursprünglichen Annahme, keine strukturelle Überraschung.

## Ratifizierungs-Block

> Ausstehend: Nutzer-Review der Kontaktabzüge (`data/labels/sighting/`), stichprobenartige
> Sichtprüfung der `hover_position_id`-Zuordnung gegen Originalclips, Plausibilitätsprüfung der
> gemessenen Spielergröße, und Entscheidung über die vorgeschlagene `resolution`/`sahi`-Änderung
> (Checkpoint Plan 02.1-03 Task 3). Nach Freigabe wird die `ffep.toml`-Änderung angewendet und dieser
> Abschnitt aktualisiert.
