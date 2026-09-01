# Datensatz-Plan — Phase 2.2 (Dataset Buildout)

**Status: verfasst am 2026-09-01, vor der ersten Active-Learning-Iteration.** Dieses
Dokument fixiert die Zahlen, die spätere Pläne ausführen statt neu zu interpretieren:
Domänen-Mix, Frame-Ziele, das Abbruchkriterium für weiteres Labeln, die
Pro-Domäne-Inferenz-Einstellungen, die Hard-Case-Mining-Strategie, die RF-DETR-Varianten-
Entscheidung und den Seed-Set-Verdikt für die Piloten-304-Frames. Jede Zahl hier ist vor
dem ersten AL-Zyklus festgeschrieben, exakt aus demselben Grund, aus dem das Gate-Dokument
(`docs/pilot-gate-decision.md`) seine Kriterien vor der Messung fixierte: nachträglich
festgelegte Kriterien sind keine Kriterien, sondern Rationalisierungen.

## Zweck & Abgrenzung

Beantwortet D-10 (Mix, gemessen statt angenommen), D-12 (metrikgetriebenes Frame-Ziel),
RESEARCH Open Question 3 (numerischer Abbruch-Schwellwert für "mAP verbessert sich
klar"), D-04/D-13 (Pro-Domäne-Eval-Splits und -Inferenz-Einstellungen) und die
Claude's-Discretion-Punkte aus `.planning/phases/02.2-dataset-buildout/02.2-CONTEXT.md`
(Hard-Case-Mining, Seed-Set-Übernahme, RF-DETR-Variante). Nicht Teil dieses Dokuments:
die tatsächliche Ausführung der AL-Iterationen (spätere Pläne, ab 02.2-09) und die
DVC-/Bundle-Mechanik (02.2-07/02.2-08/02.2-10).

## 1. Domänen-Mix

Grundlage: `docs/material-sighting.md`s Ratifizierungs-Block (D-11, 2026-09-01). Der
Nutzer hat **beide** neu gesichteten Domänen als Trainingsdomänen zugelassen — GoPro
(Domänenwert `sideline`, tatsächlich eine Hinterfeld-/Endzone-Ansicht) und TV
(Domänenwert `broadcast`, tatsächlich die Seitenansicht) — TV jedoch **bedingt**: eine
Pro-Domäne-mAP-Ablationsstudie (mit vs. ohne TV-Anteil, gemessen auf den eingefrorenen
Eval-Splits, Plan 02.2-15/02.2-18) entscheidet, ob TV als Trainingsdomäne bleibt oder auf
Transfer-Material-Status zurückfällt. Der Datensatz-Mix hier reflektiert diesen Verdikt:
drei Domänen, mit TVs Anteil bewusst klein gehalten, bis die Ablation vorliegt.

Der ursprüngliche 60/40-Richtwert (D-03-Projektentscheidung, "Orientierung, keine
Vorgabe") wird auf drei Domänen erweitert: Drohne bleibt bei ~60 % (sie ist die
Fundament-Domäne mit der bestehenden Trainingshistorie aus Phase 2.1 und dem Gate-Bezug),
die verbleibenden ~40 % teilen sich GoPro (unbedingt zugelassen, größerer Anteil) und TV
(bedingt zugelassen, kleinerer Anteil, damit ein negativer Ablations-Ausgang nur einen
kleinen Teil des Budgets verbraucht hat).

| Domäne | Frame-Anteil | Frame-Ziel (Floor) | Begründung |
|---|---:|---:|---|
| Drohne (`drone`) | 60 % | 900 | Fundament-Domäne, Gate-Bezug, bestehende Trainingshistorie (Phase 2.1) |
| GoPro/Hinterfeld (`sideline`) | 26,7 % | 400 | Unbedingt zugelassen (D-11), p50 = 27,0 px nahe an der Drohnen-Domäne |
| TV/Seitenansicht (`broadcast`) | 13,3 % | 200 | Bedingt zugelassen (D-11, Ablation aussteht), kleinerer Erstanteil |
| **Summe** | **100 %** | **1500** | entspricht exakt dem REQ-S2-03-Floor |

Der Floor von 1.500 Frames ist verbindlich; bis zu 3.000 Frames sind erlaubt, aber nur
soweit das Abbruchkriterium in `## 3` das für eine Domäne weiterhin erlaubt — kein Labeln
auf Vorrat (D-12).

**Wichtiger Vorbehalt (siehe `## 6` unten):** Der Piloten-Datensatz (304 Frames, Domäne
Drohne) wird **nicht** als v0-Seed in dieses Frame-Ziel eingerechnet — die 900
Drohnen-Frames oben sind vollständig neue, in dieser Phase zu labelnde AL-Frames. Der
Grund steht in `## 6`.

## 2. Frame-Ziele

- **Floor (verbindlich):** 1.500 verifizierte Frames, Aufteilung siehe Tabelle oben.
- **Ceiling (optional):** bis zu 3.000 Frames, domänenweise nur solange Abschnitt 3s
  Bedingung erfüllt ist.
- **"Verifiziert" heißt (D-17):** jeder Frame wird zu 100 % von der Nutzerin in CVAT
  gesichtet und korrigiert/bestätigt — Vorlabels aus dem feingetunten Drohnen-Detektor
  (Plan 02.2-09) machen das überwiegend zu Bestätigungsarbeit, ersetzen aber nicht die
  menschliche Prüfung jedes einzelnen Frames.
- Der Piloten-Datensatz (304 Frames) zählt **nicht** gegen diesen Floor (siehe `## 6`) —
  die 1.500 Frames oben sind vollständig zusätzlich zum Piloten zu labelnde Frames.

## 3. Abbruchkriterium (Stopping Rule, D-12, RESEARCH Open Question 3)

Labeln über den 1.500-Frame-Floor einer Domäne hinaus setzt sich **nur fort, solange**
diese Bedingung erfüllt ist:

> Eine Domäne wird über den Floor hinaus weiter gelabelt, nur solange ihre `mAP_50_95`
> auf den eingefrorenen Pro-Domäne-Eval-Clips (`data/reference/frozen_eval_clips.csv`,
> `role = frozen_eval`) sich gegenüber dem zuletzt registrierten Detektor um mindestens
> **+0.010 absolut** verbessert, **und** `mAP_50` sich in dieselbe Richtung bewegt.
> Unterhalb dieser Schwelle stoppt das Labeln für diese Domäne, auch wenn das
> 3.000-Frame-Ceiling noch nicht erreicht ist.

**Ehrlicher Vorbehalt:** Bei der Größe der hier eingefrorenen Eval-Sets (18 Drohnen-Clips,
12 GoPro-Clips) ist ein Delta unterhalb von ca. 0.01 mAP_50_95 statistisch nicht
auflösbar — die Schwelle ist als **Auflösbarkeits-Untergrenze** zu verstehen, nicht als
Behauptung praktischer/statistischer Signifikanz. Ein gemessenes Delta unterhalb der
Schwelle bedeutet "nicht unterscheidbar von Rauschen bei dieser Stichprobengröße", nicht
zwingend "keine Verbesserung" — die Konsequenz (Labeln stoppen) ist dieselbe, die
Interpretation ist eine andere. Dieselbe statistische-Ehrlichkeits-Konvention gilt wie im
Gate-Dokument: n wird bei jeder gemeldeten Rate mitgeführt.

Referenz-Ausgangspunkt (Plan 2.1, Drohne, einziger bisher registrierter Detektor,
`cv_detector_model@champion`, Run `87a8a5222f7a472787875e974d089c44`): `mAP_50 = 0.9571`,
`mAP_50_95 = 0.8112`, `AP_player = 0.8266`, `AP_referee = 0.7958`. Für GoPro und TV
existiert noch kein registrierter Vorgänger-Lauf — die erste AL-Iteration jeder neuen
Domäne hat keinen Vorgänger, gegen den sie das Delta bilden könnte, und labelt daher
immer bis mindestens zum Floor, unabhängig vom Abbruchkriterium (das erst ab der zweiten
Iteration einer Domäne greift).

## 4. Pro-Domäne-Inferenz-Einstellungen (D-04/C-05)

Aus `recommend_inference_settings` (Plan 02.1-03), angewendet auf die gemessenen
Sichtungswerte aus `docs/pilot-sighting.md` (Drohne) und `docs/material-sighting.md`
(GoPro, TV):

| Domäne | p50 (px) | p10 (px) | Band | `resolution` | `sahi` |
|---|---:|---:|---|---:|---|
| Drohne (`drone`) | 30,0 | — | 20–40 px | 896 | `false` |
| GoPro/Hinterfeld (`sideline`) | 27,0 | 16,5 | 20–40 px | 896 | `false` |
| TV/Seitenansicht (`broadcast`) | 23,0 | 14,0 | 20–40 px | 896 | `false` |

Alle drei Domänen landen zufällig im selben Band und teilen sich dieselbe
`ffep.toml`-Einstellung — das ist eine gemessene Koinzidenz, keine Vereinfachung: jede
Domäne wurde unabhängig gesichtet (`## 4` bleibt bindend, falls eine künftige Session
außerhalb dieses Bands misst). `resolution = 896` ist durch 224 teilbar (lcm(32, 56)),
gültig unter beiden dokumentierten RF-DETR-Teilbarkeitsregeln. **Pooled mAP wird nie
allein berichtet** (C-05/D-04) — jeder Trainingslauf meldet `AP`/`mAP_50`/`mAP_50_95` pro
Domäne getrennt (`cv/detect.py::evaluate_per_domain`, Plan 02.2-15).

## 5. Hard-Case-Mining-Strategie

Quelle: `data/reference/continuity_review.csv`s `reviewer_note`-Spalte (61/61 Clips
review, Plan 02.2-03) — die einzige Stelle im Projekt, an der dokumentierte
Fehlermodi aus echtem Tracking-Verhalten vorliegen. Häufigkeitsverteilung der
Fehlermodi über alle 46 `fail`-verdikteten Clips:

| Fehlermodus | n Clips (von 46 `fail`) | Mining-Implikation |
|---|---:|---|
| Switch bei Überlagerung (Okklusion) | 31 | Frames mit dichter Spielernähe (Line-of-Scrimmage-Gedränge, Blitz-Pakete) bevorzugt in die AL-Auswahl aufnehmen |
| Falsche Teamzugehörigkeit + Switch bei Überlagerung | 4 | Frames mit ähnlichen Trikotfarben/Kontrastarmut priorisieren |
| Sonstige (Kameraschnitt-Artefakt, Spielerverlust ohne Überlagerung, Track springt auf gegnerischen Blitz, u. a.) | 11 | Einzelfälle — Diversitäts-Sampling (nicht Uncertainty allein) fängt diese ab, da sie sich nicht auf ein Muster reduzieren lassen |

**Konkrete Strategie** (`cv/active_learning.py`, Plan 02.2-09, Uncertainty + Diversity
per D-15): Uncertainty-Score bevorzugt Frames mit niedriger/grenznaher
Detektions-Konfidenz oder gänzlich fehlenden Detektionen in einer Domäne, in der
Spieler erwartet werden (starkes Domain-Shift-Signal). Diversity-Key stratifiziert über
`(domain, session_id, hover_position_id/Kamera-Position, Spielphase)` — dieselbe
zweistufige "Gruppieren, dann pro Gruppe allokieren"-Logik wie
`frames.py::sample_training_frames`, damit die Auswahl nicht auf eine einzelne schwierige
Szene kollabiert (RESEARCH Anti-Pattern). Line-of-Scrimmage-Gedränge und Blitz-Phasen
werden über Zeitfenster-Heuristiken (Frames nahe der Play-Mitte, wo Spielerdichte am
höchsten ist) explizit mit-priorisiert, nicht dem Zufall überlassen.

**Dokumentierte Datensatz-Lücken** (Material kann diese Bedingungen nicht liefern, wird
hier ausdrücklich benannt statt verschwiegen): kein Regenwetter, keine
Flutlicht-/Kunstlicht-Bedingungen in irgendeiner der drei Domänen (alle Sessions bei
Tageslicht aufgenommen). Eine Wetter-/Licht-Augmentierung im Trainingsrezept ist nicht
Teil dieser Phase — sie würde eine Bedingung simulieren, die nie real gesichtet wurde,
und wird hier bewusst nicht ergänzt, um keine unbelegte Robustheitsannahme ins Training
zu tragen.

## 6. Seed-Set-Prüfung (Pilot, 304 Frames)

Siehe eigener Abschnitt `## Seed-Set-Prüfung (Pilot, 304 Frames)` unten (Plan-Task 3)
für die Messung und den Verdikt. Vorwegnahme für die Zahlen oben: Der Verdikt lautet
`nicht übernommen` — die Frame-Ziele in `## 1`/`## 2` sind bereits so geschrieben, dass
der 1.500-Floor **ohne** den Piloten-Seed erreicht wird (keine nachträgliche Korrektur
nötig).

## 7. RF-DETR-Variante

**Small bleibt Standard** für alle drei Domänen und alle AL-Iterationen dieser Phase.
Ein Größenvergleich (Medium/Large) findet **in dieser Phase nicht statt**: das
8-GB-VRAM-Budget der Dell-CUDA-Maschine (D-21) ist ohne AWS-Instanz die garantierte
Trainingsumgebung, und ein Vergleich gegen einen sich noch verändernden (wachsenden)
Datensatz zwischen AL-Iterationen würde das Ergebnis konfundieren — ein fairer Vergleich
bräuchte einen eingefrorenen Datenstand, den es vor Abschluss der AL-Iterationen dieser
Phase nicht gibt. Sollte eine AWS-GPU-Instanz verfügbar werden (D-21, Bonus, keine
Abhängigkeit), bleibt ein Vergleich für eine spätere Phase eine Option, nicht für 2.2.

**Verbindliches Verbot (C-06/PML-1.0):** RF-DETR-XLarge und -2XLarge sind ausgeschlossen
— sie stehen unter der PML-1.0-Lizenz, nicht Apache-2.0, und verletzen damit die
projektweite No-AGPL/lizenzsaubere-Stack-Regel. Der mechanische Guard
`tests/test_cv_dependencies.py::test_rfdetr_pml_variants_absent_from_source` verhindert,
dass `RFDETRXLarge`, `RFDETR2XLarge` oder `rfdetr_plus` überhaupt in den Quellcode
gelangen. Nano/Medium/Large bleiben technisch zulässig, sind aber ohne einen konkreten
Anlass (siehe oben) für diese Phase nicht geplant.

## 8. Eval-Split-Vertrag (D-04/D-13/D-07)

`data/reference/frozen_eval_clips.csv` (Plan 02.2-06 Task 1, `cv/frames.py::
freeze_eval_clips`/`read_eval_split`) ist die verbindliche, eingefrorene Eval-/
Privat-Test-Aufteilung:

- **Drohne:** 18 von 61 Clips eingefroren (`fraction = 0.30`, Seed `20260516`), 9 aus
  `hp-01`, 9 aus `hp-02`. Diese 18 Clips tragen `private_test = true` und dienen
  **doppelt** als eingefrorener Drohnen-Eval-Split **und** als privates
  Hackathon-Testset (D-07) — dieselben Clips, kein zweiter Ziehungsschritt, keine
  Kontamination zwischen den beiden Verwendungen möglich.
- **GoPro/Hinterfeld:** 12 von 60 Clips eingefroren (`fraction = 0.20`, Seed `20260516`),
  alle aus der einzigen registrierten Kamera-Position `hp-01`.
- **TV/Broadcast:** noch nicht eingefroren — TVs Status als Trainingsdomäne ist bedingt
  (siehe `## 1`); ein eigener Eval-Split für TV wird erst gezogen, falls die
  Ablationsstudie TV endgültig als Trainingsdomäne bestätigt und ein domänenspezifischer
  mAP-Bericht für TV benötigt wird. Bis dahin dient TVs Beitrag ausschließlich dazu, den
  Effekt auf Drohnen-/GoPro-mAP zu messen (Ratifizierungs-Block, `docs/material-sighting.md`).

**Bindende Regel für alle nachgelagerten Pläne:** Active-Learning-Frame-Auswahl
(`cv/active_learning.py`, Plan 02.2-09) zieht ausschließlich aus Clips mit
`role = pool` in `frozen_eval_clips.csv`. Kein Frame aus einem `role = frozen_eval`-Clip
darf in einen Trainings- oder AL-Auswahlschritt einfließen — das würde sowohl den
Eval-Split als auch (für die Drohnen-Domäne) das private Hackathon-Testset kontaminieren.
