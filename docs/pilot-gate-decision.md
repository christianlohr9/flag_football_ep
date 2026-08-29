# CV-Tracking-Piloten-Gate — Go/No-Go-Entscheid (2026-05-16-Session)

**Status: Entscheidung offen — Entwurf vom 2026-08-29**

Maschinenlesbare Grundlagen: `data/reference/continuity_review.csv` (Kontinuitäts-Review),
`data/reference/gt_positions.csv` (Positionsfehler-GT), `docs/pilot-accuracy.md`,
`docs/cv-setup.md`, `docs/homography-calibration.md`.

## Zweck & Abgrenzung

Dieses Dokument trifft den in `.planning/PROJECT.md` (C-09) und `.planning/REQUIREMENTS.md`
(REQ-S2-02) festgelegten Go/No-Go-Entscheid für den CV-Tracking-Piloten: stellt jedes der drei
C-09-Kriterien neben seine gemessene Zahl, hält einen expliziten, datierten menschlichen Verdikt
fest, und schreibt die Konsequenz dieses Verdikts fest, bevor er getroffen wird. Es ist der
Schließungs-Artefakt von Milestone 1s Strang-2-Hälfte (D-12).

Nicht Teil dieses Dokuments — und explizit gesperrt, bis der Verdikt hier steht:
Datensatz-Versionierung/Active-Learning-Buildout (Phase 2.2), Snap-Erkennung und
Coaching-Metriken (2.3), Spieler-Identität (2.4), Broadcast-Domäne (2.5). Diese Phasen sind
laut `.planning/STATE.md` hart auf diesen Entscheid gegated und werden nicht im Detail
geplant, bevor er steht (D-06, C-09).

## Ausgangslage

Der gesamte Pilot lief auf **61 Hudl-re-encodeten Einzelplay-Clips** (1920x1080, 30 fps, ~10.3 s
je Clip, ~10.5 min Gesamtmaterial) aus einem einzigen Freundschaftsspiel
(`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`, GER vs. Panama Rojo). Für dieses Spiel existiert
**kein Hudl-PBP** — Plays werden als `session_id` + Clip-Nummer geschlüsselt (D-02), nicht über
`game_id`/`play_id`. Flughöhe und exakte Hover-Positionen der Aufnahme waren zu Phasenbeginn
unbekannt; `docs/pilot-sighting.md` hat das nachträglich vermessen: **2 Hover-Positionen**
(hp-01: Clips 001-056, 30 Clips; hp-02: Clips 004-061, 31 Clips), beide mit einer gemessenen
scheinbaren Spielergröße von p50 = 30 px, von `docs/capture-protocol.md`s eigenem
Richtwert-Raster als **Brauchbar** eingestuft (Auflösungs-Tabelle: 1080p ist in der Drohnen-Domäne
brauchbar, solange die Höhe den Kleinobjekt-Malus nicht auslöst — die tatsächliche Flughöhe dieser
Session wurde nie unabhängig gemessen; `docs/capture-protocol.md`s Unbrauchbar-Schwelle liegt bei
1080p **und** über 40 m Höhe zugleich, nicht bei 1080p allein).

Das ist die einzige verfügbare Materialbasis dieses Piloten: ein Spiel, eine Annotationsperson
(Trainingsdatensatz wie GT-Set), zwei Hover-Positionen, Hudl-Re-Encode statt Rohmaterial (D-01,
bewusste Entscheidung — Hudl ist der tatsächliche Verteilkanal, der Pilot testet damit das
Material, das real ankommen wird, nicht einen hypothetischen Rohdaten-Pfad). Wer dieses Dokument
in einem halben Jahr liest, muss anhand dieses Abschnitts entscheiden können, ob ein schlechtes
Ergebnis an der Pipeline oder am Filmmaterial liegt.

## Gate-Kriterien und Messung

| Kriterium (C-09) | Zielwert | Gemessen | Datenbasis | Erfüllt? |
|---|---|---|---|---|
| 1. Track-Kontinuität | >= 90 % der Plays ohne ID-Switch | Review gestoppt bei 20/61 Clips (6 pass / 14 fail); konservative obere Schranke über alle 61 Clips (alle 41 ungeprüften Clips als "pass" gezählt) = **47/61 = 77.0 % < 90 %-Ziel** — das Kriterium ist bereits am oberen Rand des Möglichen verfehlt | `data/reference/continuity_review.csv` | _(Task 2)_ |
| 2. Positionsfehler | ~<= 1 m (1,094 Yards) | Median **0,171 Yards (0,156 m)**, p90 **0,422 Yards (0,386 m)**, n = 250 GT-Punkte, Match-Rate 99,6 % (249/250) — v2-Messung gegen den BoT-SORT-/Torso-Crop-Gap-Fix | `docs/pilot-accuracy.md` (`### Gemessener Positionsfehler (v2)`) | _(Task 2)_ |
| 3. Inferenz pro Spiel | < 1 h | **47,83 Minuten** extrapoliert für ein 50-minütiges Spiel (v2, BoT-SORT) | `docs/cv-setup.md` (`### v2 (BoT-SORT-Tracker, Torso-Crops)`) | _(Task 2)_ |

**Kriterium 1 im Detail.** Das menschliche Kontinuitäts-Review (Plan 02.1-14) prüfte 20 von 61
Clips von Hand gegen die v2-Tracking-Overlays: 6 bestehen, 14 scheitern. Ab diesem Punkt wurde das
Review durch eine explizite Nutzer- und Orchestrator-Entscheidung gestoppt, **nicht** weil der
Denominator verkleinert werden sollte, sondern weil das Zielkriterium bereits mathematisch
unerreichbar geworden war: selbst wenn **alle** 41 noch ungeprüften Clips bestünden (die
großzügigste denkbare Annahme, die zugunsten des Kriteriums wirkt, nie gegen es), ergäbe das
`(6 + 41) / 61 = 47/61 ≈ 77,05 %` — klar unter der 90-%-Zielmarke. `summarise_review()` liefert
für diesen Zustand korrekt `pass_rate: None` (D-09/T-2.1-31 verhindern genau das Gegenteil — einen
manipulierten Pass-Rate-Wert aus einem geschrumpften Nenner). Für **diese** Entscheidung wird
stattdessen bewusst das obere-Schranken-Argument verwendet, weil es alle ungeprüften Clips zu
Gunsten des Kriteriums zählt statt sie zu verwerfen: **das Kriterium ist verfehlt, auch im
bestmöglichen Fall.**

Dominanter Fehlermodus (aus den 14 tatsächlich beobachteten Fails): **ID-Switches bei
Spielerinnen-Verdeckungen** (Kreuzungen, Coverage, Flag-Pulls) — dem schrägen Kamerawinkel dieser
Aufnahme inhärent, kein zufälliges Rauschen. Sekundär: residuale Team-Fehlzuordnungen (3 Clips) und
ein Kamera-Schnitt mitten in einem Clip (1 Clip, ein Aufnahme-Artefakt, keine Pipeline-Schwäche).
Volle Review-Notizen: `data/reference/continuity_review.csv`, Rationale:
`.planning/phases/02.1-cv-tracking-pilot-go-no-go-gate/02.1-14-SUMMARY.md`.

**Remediation-Pfade** (beide für den Verdikt-Konsequenz-Abschnitt unten relevant):

1. **Appearance-basierte Re-Identifikation** (ReID-Embedding in der Tracker-Assoziation, oder ein
   Post-hoc-Track-Merge) — die identifizierte nächste technische Iteration, falls am
   Tracking-Algorithmus weitergearbeitet wird.
2. **Steilerer, überkopfigerer Drohnen-Winkel** — die Verdeckungshäufigkeit ist eine direkte
   Funktion des schrägen Aufnahmewinkels dieser Session; das ist ein Aufnahme-Setup-Befund, kein
   Trainings- oder Modell-Defizit, und routet damit per D-06 zwingend nach Phase 2.0 zurück, **nie**
   zu mehr Trainings-Labels.

Zusätzlicher Aufnahme-Befund (aus `docs/pilot-accuracy.md`): die **Ost-Endzone wurde in der
gesamten Session nie erreicht** — kein Track-Punkt (von 341.461 Zeilen, beide Hover-Positionen)
projiziert je über x = 44,2 Yards auf einem 50-Yards-Feld. Beide Hover-Positionen enden nah an
derselben Stelle. Das ist ein konkretes, evidenzbasiertes Feedback für ein künftiges
Aufnahme-Setup (Hover-Position/Framing deckte dieses Feldende nie ab), keine Pipeline-Lücke.

## Extrapolationsformel

Die C-09-Laufzeitzahl (47,83 Minuten, Zeile 3 der obigen Tabelle) stammt aus
`cv.benchmark.extrapolate_game_runtime`, angewendet auf den vollständigen v2-Tracking-Lauf über
alle 61 Clips (`docs/cv-setup.md` `### v2`). Die Formel wird hier **wörtlich**, nicht paraphrasiert,
wiedergegeben (D-11 verlangt genau das als Diskretions-Dokumentationspunkt):

```
[machine=MacBook-Pro-2.fritz.box] linear extrapolation (assumption 1):
total real-time factor = decode(29.816s / 664.41s footage = 0.044876x realtime)
  + detect(480.525s / 664.41s footage = 0.723236x realtime)
  + track(125.139s / 664.41s footage = 0.188346x realtime)
  + write(0.091s / 664.41s footage = 0.000136x realtime)
  = 0.956594x realtime;
extrapolated game duration = 0.956594x * 3000.0s game (continuous-game denominator,
  assumption 2) = 2869.78s = 47.83 min
```

**Annahmen** (aus `cv.benchmark`s eigener Dokumentation, hier ausgeschrieben statt implizit
gelassen):

1. **Linearität:** jede Pipeline-Stufe (Decode, Detect, Track, Write) kostet pro Sekunde Footage
   einen konstanten Sekundenbruchteil; die Stufen laufen sequenziell, ihre Kosten summieren sich
   (kein Overlap).
2. **Kontinuierlicher-Spiel-Nenner:** die Hochrechnung nimmt 3000 s (50 Minuten) durchgehend
   aufgenommenes Spielmaterial an, nicht 50 Minuten Spielzeit mit Pausen/Totzeiten dazwischen — ein
   konservativer (eher zu langer, nicht zu kurzer) Nenner.
3. **50-Minuten-Default** (`game_seconds=3000.0`) ist D-11s eigene Annahme, kein aus dieser Session
   gemessener Wert — das tatsächliche Freundschaftsspiel selbst wurde nicht in voller Länge zeitlich
   erfasst.
4. **Primärmaschinen-Messung:** alle Sekundenwerte wurden auf der Primärmaschine (Apple M4 Max,
   `MacBook-Pro-2.fritz.box`) gemessen, nicht auf dem Dell-CUDA-Rechner oder Colab — D-11 verlangt
   genau das, weil das die Maschine ist, auf der die Pipeline routinemäßig laufen wird.

Zum Vergleich: der v1-Lauf (OC-SORT, vor dem Kontinuitäts-Gap-Fix) extrapolierte auf 38,63 Minuten
— der BoT-SORT-Wechsel (Kamerabewegungs-Kompensation, `## Gate-Kriterien und Messung` Kriterium 1)
kostet in der `track`-Stufe rund 9 Minuten zusätzliche Hochrechnungszeit, bleibt aber weiterhin klar
innerhalb des 60-Minuten-Budgets — der Sicherheitsabstand schrumpft jedoch merklich gegenüber v1.

## Fehlerzerlegung und Vorbehalte

Jede Zahl in diesem Dokument trägt dieselben strukturellen Grenzen, hier gebündelt statt an jeder
einzelnen Stelle wiederholt:

- **Eine Annotationsperson, ein Spiel, keine Inter-Annotator-Übereinstimmung (IAA)** — sowohl beim
  Trainingsdatensatz (`docs/cv-setup.md` `### Datensatz`) als auch beim GT-Positions-Set
  (`docs/pilot-accuracy.md` `## Ground-Truth-Set`) und beim Kontinuitäts-Review selbst. Eine zweite
  Bewertungsperson könnte andere ID-Switch-Urteile fällen; das ist eine bekannte, nicht versteckte
  Grenze eines Solo-Entwickler-Projekts.
- **Homographie-versus-Pipeline-Fehler-Split** (Details: `docs/pilot-accuracy.md`
  `### Fehlerzerlegung (v2)`): der gemessene Positionsfehler (Median 0,171 / p90 0,422 Yards) ist
  der Fusspunkt-Fehler **unter der eigenen Homographie des Projekts**, nicht die Gesamtabweichung
  von der wahren Feldposition — GT und Pipeline-Tracks laufen durch dieselbe
  Hover-Positions-Homographie, was einen Teil einer etwaigen Homographie-Verzerrung in der
  paarweisen Distanz aufhebt.
- **Reprojektionsfehler pro Hover-Position** (`docs/homography-calibration.md`): **hp-01** hat einen
  unabhängig gemessenen Reprojektionsfehler von **0,27 Yards (0,25 m)** an einem
  zurückgehaltenen Landmark (n=1 Held-out-Punkt — das Feld bietet keine weiteren eindeutig
  identifizierbaren Linienkreuzungen im Bildausschnitt). **hp-02** ist mit exakt 4 Punkten
  **exakt bestimmt** (Restfehler 0 per Konstruktion) — es gibt **keinen** unabhängigen
  Kontrollpunkt für diese Homographie; ihre einzige Güte-Aussage ist die End-to-End-Messung selbst
  (hp-02: Median 0,210 / p90 0,479 / Max 1,527 Yards, der einzige Einzelpunkt im gesamten
  250-Punkte-Datensatz über dem ~1-m-Schwellenwert, offen berichtet statt versteckt).
- **Massstabs-Kontrolle:** 8 vom Nutzer per Augenmass geschätzte Feld-Distanzen (mittlerer
  signierter Fehler -0,015 Yards, kein Hinweis auf systematische Skalen-Verzerrung) — ausdrücklich
  ein Plausibilitätscheck ("Richtwert"), keine Ersatzmessung für die Reprojektions-Kontrolle oben.
- **Ost-Endzone ohne GT-Daten** — Aufnahme-Abdeckungslücke dieser Session (siehe
  `## Gate-Kriterien und Messung` oben), keine Pipeline-Schwäche.
- **Clip-8-Korrelations-Hinweis:** 7 der 21 GT-Frames stammen aus 7 aufeinanderfolgenden Frames
  eines einzigen Clips (keine 7 unabhängigen Stichproben) — ehrlich als ~15 effektiv unabhängige
  Szenen statt 21 nomineller Frames gezählt (`docs/pilot-accuracy.md` `## Ground-Truth-Set`).
- **Jede Rate in diesem Dokument trägt ihr eigenes `n`** — keine zusammengefasste Zahl steht ohne
  ihren Nenner (61 Clips, 250 GT-Punkte, 8 Massstabs-Paare je nach Zeile).

Kein Wert in diesem Dokument stammt aus einer geschönten Teilmenge — jede Kennzahl deckt die
volle verfügbare Grundgesamtheit (alle 61 Clips für Kontinuität und Laufzeit, alle 250 GT-Punkte
für den Positionsfehler) oder benennt explizit, welcher Teil noch fehlt.

## Entscheidung

_Wird in Task 2/3 nach dem menschlichen Go/No-Go-Verdikt ausgefüllt._

**AWAITING: Verdikt (GO / NO-GO / TEILWEISE)**
**AWAITING: Datum**
**AWAITING: Begründung / Datenbezug**
**AWAITING: bei TEILWEISE — benanntes gescheitertes Kriterium, gewünschte Aufnahme-Setup-Änderung, Re-Trigger-Bedingung**

## Konsequenzen

Festgelegt **vor** dem Verdikt, damit er im Nachhinein nicht abgeschwächt werden kann:

- **GO** -> Phase 2.2 (Datensatz-Buildout) und Phase 2.3 (Coaching-Metriken) werden entsperrt;
  `.planning/REQUIREMENTS.md`s REQ-S2-03/REQ-S2-04 dürfen im Detail geplant werden.
- **NO-GO** -> zurück zu Phase 2.0 (Aufnahme-Setup) per D-06 — ein klarer Fehlschlag wird mit
  besserem Capture beantwortet (Hover-Position, geringere Höhe, höhere Auflösung, anderer
  Belichtungs-Umgang), **niemals** mit mehr Trainings-Labels. Phase 2.2/2.3/2.4 bleiben gesperrt,
  bis eine neue Aufnahme-Session den Gate-Entscheid erneut durchläuft.
- **TEILWEISE** -> genau benennen, welches Kriterium verfehlt wurde und welche
  Aufnahme-Setup-Änderung es adressiert, unter Zitat der Pro-Zone- und Pro-Clip-Evidenz aus
  `docs/pilot-accuracy.md` und `data/reference/continuity_review.csv`; eine explizite
  Re-Trigger-Bedingung wird festgehalten, damit dieser Zustand nicht zu einem offenen "fast"
  ohne Ablaufdatum wird.

## Demo

_Wird in Task 3 ausgefüllt: HC-Demo-Versand-Record und DEFERRED-ANALYST-Block._
