# Dataset Card — Flag-Football-Detektionsdatensatz (Phase 2.2)

**Status: verfasst am 2026-09-11, Datensatz v2.** Dieses Dokument ist auf Deutsch verfasst
(Projektsprache); eine englische Übersetzung wird zum Veröffentlichungszeitpunkt produziert
(`docs/dataset-publication.md ## 6`, Checklistenpunkt 2) — bis dahin ist dies die verbindliche
Fassung.

## Zweck und beabsichtigte Nutzung

Der Datensatz trainiert und evaluiert einen Objektdetektor (RF-DETR) für Personen (`player`,
`referee`) auf Flag-Football-Videomaterial aus drei Kamera-Domänen: Drohne (Vogelperspektive),
GoPro/Hinterfeld (bodennahe Seitenlinien-/Endzone-Kamera) und TV/Broadcast (professionelle
Fernsehübertragung). Beabsichtigte Nutzung: Trainingsgrundlage für Tracking- und
Analyse-Pipelines im Flag-Football-Kontext (dieses Projekt), Referenzmaterial für die
Objekterkennungs-Forschung im Amateur-/Semi-Profi-Sportkontext, und — nach Veröffentlichung,
siehe `docs/dataset-publication.md` — allgemeine CV-Community-Nutzung unter der noch zu
bestätigenden Lizenz. **Nicht** beabsichtigt: Gesichtserkennung, biometrische Identifikation
oder jede Form der Reidentifikation über den Objekterkennungs-Zweck hinaus — der Datensatz
enthält keine Identitäts-Labels, nur Klassenlabels (`player`/`referee`).

## Quellen und Aufnahmebedingungen je Domäne

Jede Domäne stammt aus **einem einzigen Spiel** (siehe „Split-Methodik" unten für die
Konsequenz dieser Tatsache für die Split-Struktur):

| Domäne | Session | Partie | Kamera | Gemessene Spielergrösse (p50 / p10, `docs/dataset-plan.md ## 4`, `docs/material-sighting.md`) |
|---|---|---|---|---|
| Drohne (`drone`) | `2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE` | GER vs. Panama Rojo (Testspiel), 16.05.2026 | Drohne, zwei Hover-Positionen (`hp-01`, `hp-02`) | p50 = 30,0 px |
| GoPro/Hinterfeld (`sideline`) | `2026-08-14_WC-GER-vs-MEX-GOPRO` | GER vs. Mexiko (WM), 14.08.2026 | bodennahe GoPro, Hinterfeld-/Endzone-Blickwinkel | p50 = 27,0 px, p10 = 16,5 px |
| TV/Broadcast (`broadcast`) | `2026-08-14_WC-USA-vs-AUS-TV` | USA vs. Australien (WM), 14.08.2026 | professionelle TV-Übertragung, Seitenansicht | p50 = 23,0 px, p10 = 14,0 px |

Alle drei Domänen liegen im selben Inferenz-Band (20–40 px, `resolution=896`, `sahi=false`) —
eine gemessene Koinzidenz, keine angenommene Vereinfachung (`docs/dataset-plan.md ## 4`). Alle
Sessions wurden bei Tageslicht aufgenommen; siehe „Bekannte Limitierungen" für die daraus
folgende Lücke.

## Zusammensetzung

**Versionsverlauf** (jede Version ist eine wachsende Obermenge der vorherigen, nie ein Ersatz):

| Version | Bilder gesamt | Drohne | GoPro/Hinterfeld | TV/Broadcast | `player`-Boxen | `referee`-Boxen | `content_sha256` |
|---|---:|---:|---:|---:|---:|---:|---|
| v1 (fehlerhaft, historisch) | 739 | 450 | 189 | 100 | 9799 | 1371 | `e27c1b60d60e240d8f6bc9d4b6b2cd276b135776cb2cd812ff36ff6661fabb8b` |
| v1.1 (D-17-Korrektur) | 558 | 450 | 8 | 100 | 9305 | 1063 | `82f0feb7c4d678a44bdc7e90be416561bb2e27fabb5a657eb0dc005dbc54fa92` |
| v1.2 (GoPro-Nachsitzung) | 572 | 450 | 22 | 100 | 9444 | 1109 | `d4528a9958305c267e6257be26c07466fe78e286d4777108c29d9476003b56b1` |
| **v2 (aktuell, Iteration 2 gemergt)** | **755** | **514** | **57** | **184** | **12139** | **1427** | `d87dd04cb7ed53cc3436e02596233937971df0edfbcf9cff628192a9d8963dce` |

**v1 ist historisch und nicht verwendbar:** v1 enthielt 181 GoPro-Frames, die nur vom Vorlabel-
Modell, nie von einem Menschen gesehen wurden (D-17-Verstoss, siehe `docs/dataset-buildout.md
### Korrektur 2026-09-02`) — v1.1 ist die korrigierte, tatsächlich verifizierte Fassung
desselben Standes. v1 wird hier nur aus Provenienz-Gründen aufgeführt, nicht als nutzbare
Version.

**Aktuelle Version (v2) nach Domäne, Klasse und Annotationsstatus:**

| Domäne | Bilder | `player`-Boxen | `referee`-Boxen | Bilder ohne Annotation |
|---|---:|---:|---:|---:|
| Drohne | 514 | 9046 | 1035 | 0 |
| GoPro/Hinterfeld | 57 | 656 | 133 | 0 |
| TV/Broadcast | 184 | 2437 | 259 | 0 |
| **Summe** | **755** | **12139** | **1427** | **0** |

**Splits:** v2 trägt ausschliesslich `split: "train"` — die Active-Learning-Iterationen liefern
reines Trainingsmaterial; Evaluierung läuft separat über den eingefrorenen Eval-Split (siehe
„Split-Methodik" und „Provenienz" unten), nicht über einen `val`-Split innerhalb von
`data/labels/dataset/` selbst.

**1.500-Frame-Floor:** REQ-S2-03 setzt einen verbindlichen Floor von 1.500 verifizierten
Frames. v2 erreicht 755/1.500 (50,3 %) — der Floor ist nach den beiden für diese Phase
geplanten Active-Learning-Iterationen **nicht erreicht**, ehrlich berichtet, siehe
`docs/dataset-buildout.md ## Iteration-2-Merge, Validierung und Dataset v2` für die volle
Herleitung und Begründung (die strikte, diff-basierte "berührt"-Regel hält die zählbare
Ausbeute unter der ursprünglichen Projektion).

## Eingefrorene Evaluierungs-Ground-Truth (separat vom Trainingsdatensatz)

Getrennt von `data/labels/dataset/` existiert eine eigene, eingefrorene Eval-Ground-Truth
unter `data/labels/eval/<domain>/corrected/` — DVC-getrackt in `data/labels/eval.dvc`
(md5 `6c093e25816bab6b132ea14da4d44465.dir`, 561 Dateien). Sie ist die einzige Grundlage, auf
der Detektor-Läufe in diesem Projekt verglichen werden (nie der Trainingsdatensatz selbst):

| Domäne | Bilder | Boxen | Clips (aus `data/reference/frozen_eval_clips.csv`, `role=frozen_eval`) |
|---|---:|---:|---|
| Drohne | 90 | 1834 | 18 Clips (9 `hp-01`, 9 `hp-02`) |
| GoPro/Hinterfeld | 72 | 676 (nach Korrektur, siehe unten) | 12 Clips (`hp-01`) |
| TV/Broadcast | — | — | kein eingefrorener Eval-Split existiert für diese Domäne (siehe `docs/dataset-plan.md ## 8` — TVs Trainingsdomänen-Status ist bedingt) |

**Vorlabel-Bias-Vorbehalt (wichtig für jede Nutzung dieser GT als Referenz):** Die Eval-GT
wurde ursprünglich aus den Vorlabels des Phase-2.1-Champion-Detektors heraus geprüft (95 % der
Drohnen-Boxen, 75 % der GoPro-Boxen blieben unverändert zum Vorlabel) — das begünstigt
champion-ähnliche Läufe systematisch. Ein am 2026-09-11 durchgeführter, unabhängiger
Bias-Test (30 Frames komplett neu gezeichnet, ohne Vorlabel-Anker,
`docs/dataset-buildout.md ## Vorlabel-Bias-Test`) bestätigte den Effekt: auf der Drohne
verschwindet der scheinbare Vorsprung des champion-ähnlichen Laufs gegenüber den
Active-Learning-Iterationen fast vollständig, sobald auf feldinterne Personen gefiltert wird
(homographie-basierter "on-field"-Modus); bei GoPro deckte der Test 53 vom Vorlabel übersehene
Spielerinnen-Boxen auf, die in die bestehende GoPro-Eval-GT nachgemerged wurden (623 → 676
Boxen). **Konsequenz für Nutzerinnen dieses Datensatzes:** absolute mAP-Werte gegen diese GT
sind für champion-ähnliche Modelle nach oben verzerrt; relative Vergleiche zwischen
Active-Learning-Modellen sind belastbarer, aber der Bias-Test deckte nur 30 von 162
Eval-Bildern ab — die verbleibende Unsicherheit ist real, nicht vollständig ausgeräumt.

## Split-Methodik

**Clip-level, niemals Frame-level.** Grund: benachbarte Frames desselben Clips sind
Near-Duplikate (dieselbe Spielszene, wenige Millisekunden auseinander) — ein Frame-Level-Split
würde nahezu identische Bilder auf Train und Eval verteilen und Leckage erzeugen, die die
gemessene Genauigkeit künstlich nach oben verzerrt (exakt der Fehler, den die
Champion-Leckage-Diagnose in `docs/dataset-buildout.md ### Nachtrag 2026-09-04 (Diagnose,
Korrektur)` nachträglich für den ursprünglichen Piloten-Vergleich aufgedeckt hat).

**Spiel-Level-Splits sind mit diesem Material unmöglich**, weil jede Domäne aus genau einem
Spiel besteht (siehe „Quellen und Aufnahmebedingungen je Domäne" oben) — es gibt kein zweites Drohnenspiel im
Trainings-/Eval-Pool dieser Domäne, aus dem ein sauberer Zweitspiel-Holdout gebildet werden
könnte. (Das private Hackathon-Testset, GER vs. Puerto Rico, ist die eine Ausnahme — es ist
absichtlich als komplett separates Spiel gewählt, aber nicht Teil dieses veröffentlichbaren
Datensatzes, siehe `docs/dataset-publication.md ## 1`.) Die eingefrorenen Eval-Clips
(`data/reference/frozen_eval_clips.csv`) sind daher als **Clip-Stichprobe innerhalb desselben
Spiels** zu verstehen, nicht als unabhängiges Spiel — ein Clip landet vollständig entweder im
Trainings-Pool (`role=pool`) oder im Eval-Split (`role=frozen_eval`), nie beides, aber die
Generalisierung, die diese Eval-GT misst, ist "neue Szenen desselben Spiels", nicht "neues
Spiel". Diese Grenze ist eine dokumentierte Limitierung, siehe unten.

## Labelling-Konvention

Bindend seit dem Piloten (Phase 2.1), unverändert über alle Domänen und Versionen hinweg,
bestätigt am 2026-09-11 als "Konvention A" (`docs/cv-setup.md`, Nachtrag 2026-09-11):

- Jede klar sichtbare Person wird geboxt — Spielerinnen, Ersatzspielerinnen, Trainerstab,
  Seitenlinien-Personal, Zuschauer am Rand.
- Nur Personen mit einer aktiven Schiedsrichterrolle auf dem Feld erhalten das Label
  `referee`; **alle** anderen Personen erhalten `player`, nie eine dritte Klasse, nie ein
  Ausschluss.
- Die räumliche Filterung (wer tatsächlich auf dem Feld ist) passiert stromabwärts in
  Feldkoordinaten (Homographie-Projektion), nicht bereits beim Boxen.
- Boxen umschliessen den vollständig sichtbaren Körper inklusive Gliedmassen; die Unterkante
  sitzt eng an den Füssen, Schatten werden ausgeschlossen — der Fusspunkt ist der Punkt, der
  später in Feldkoordinaten projiziert wird.
- Nur zwei Klassen im gesamten Datensatz: `player`, `referee`.

## Provenienz

| Version | `content_sha256` | DVC-Pointer-Git-Commit | DVC-MD5 |
|---|---|---|---|
| v1 (historisch) | `e27c1b60d60e240d8f6bc9d4b6b2cd276b135776cb2cd812ff36ff6661fabb8b` | `7b528cd` | `b0a33db5bb3269c8fdd594e198dcab9f.dir` |
| v1.1 | `82f0feb7c4d678a44bdc7e90be416561bb2e27fabb5a657eb0dc005dbc54fa92` | `495af65` | `1659e351c063750eea94b536eb9f10e1.dir` |
| v1.2 | `d4528a9958305c267e6257be26c07466fe78e286d4777108c29d9476003b56b1` | `8e52101` | `b39db72109a25376fe50628405ab6e48.dir` |
| **v2** | **`d87dd04cb7ed53cc3436e02596233937971df0edfbcf9cff628192a9d8963dce`** | **`dbe6a10`** | **`4b1652c97f6ca4e2f0032a6a7e2334a3.dir`** |

**Detektor-Trainingsläufe (MLflow Run-IDs), zur Einordnung, keiner davon aktuell `champion`:**

| Lauf | Trainingsdatensatz | MLflow Run-ID | Status |
|---|---|---|---|
| Phase-2.1-Champion | Piloten-Datensatz (304 Bilder, nicht sauber held-out gegen die Eval-Clips) | `87a8a5222f7a472787875e974d089c44` | **`champion`/`hackathon-frozen`, unverändert** |
| Ablation D ("sauberer Champion") | Piloten-Rezept, 18 eingefrorene Drohnen-Clips entfernt | `a6d53662e6fa4df88d10debd1551de6b` | nicht aliasiert, dient als saubere Referenz |
| Iteration 1 | Datensatz v1.2 (572 Bilder) | `be854a1adebf4eb4b01d98dc39022ee1` | nicht befördert |
| Iteration 2 | Datensatz v2 (755 Bilder) | `682d62f94eff47b798f8a1ddecceee78` | nicht befördert |

`content_sha256` wird als MLflow-Parameter jedes Trainingslaufs geloggt — jeder registrierte
Detektor lässt sich damit exakt auf seinen Label-Stand zurückverfolgen, nicht nur auf
"irgendeine Version" des Datensatzes.

## Bekannte Limitierungen

1. **Ein einzelner Annotator, keine Inter-Annotator-Agreement-Messung.** Alle Boxen stammen von
   derselben Person (der Projekt-Nutzerin). Bei einem Solo-Entwickler-Projekt ist eine
   Zweit-Annotator-Messung nicht budgetiert — die dokumentierten Content-Hashes belegen
   Konsistenz-mit-sich-selbst und Rückverfolgbarkeit, nicht Inter-Annotator-Zuverlässigkeit.
2. **Ein Spiel pro Domäne.** Jede der drei Domänen basiert auf genau einer Partie (siehe
   „Quellen und Aufnahmebedingungen je Domäne"); es gibt keine Spiel-zu-Spiel-Varianz (Wetter, Gegner, Trikotfarben,
   Beleuchtung) innerhalb einer Domäne. Ein Modell, das ausschliesslich auf diesem Datensatz
   trainiert, ist gegen Szenen desselben Spiels validiert, nicht gegen ein zweites,
   unabhängiges Spiel derselben Domäne (siehe „Split-Methodik").
3. **Fehlende Wetter-/Lichtbedingungen.** Alle Sessions wurden bei Tageslicht aufgenommen — es
   gibt weder Regen- noch Flutlicht-/Kunstlicht-Aufnahmen in irgendeiner Domäne
   (`docs/dataset-plan.md ## 5`). Ein Modell, das auf diesem Datensatz trainiert wird, ist für
   diese Bedingungen nicht validiert.
4. **Der 1.500-Frame-Floor ist nicht erreicht** (755/1.500, 50,3 %), am stärksten sichtbar bei
   GoPro/Hinterfeld (57 von 400 Domänen-Floor, 14,3 %) — die Domäne mit der kleinsten
   Stichprobe im gesamten Datensatz.
5. **Piloten-Seed nicht enthalten, Polygon-Artefakt daher nicht relevant für v2.** Der
   304-Bilder-Piloten-Datensatz (Frames bis ca. 103 als Polygon statt Rechteck annotiert, siehe
   `docs/cv-setup.md ### Datensatz`) wurde geprüft und laut `docs/dataset-plan.md ## 6`
   ausdrücklich **nicht** als Seed übernommen — v2 enthält keinen einzigen Piloten-Frame, das
   Polygon-zu-Box-Rundungsartefakt (bis 0,26 px Grenzabweichung) betrifft diesen Datensatz
   also nicht.
6. **Eval-Ground-Truth trägt einen gemessenen, aber nicht vollständig quantifizierten
   Vorlabel-Bias** (siehe „Eingefrorene Evaluierungs-Ground-Truth" oben) — absolute mAP-Werte
   gegen die bestehende GT sind für champion-ähnliche Modelle nach oben verzerrt.
7. **Kein eingefrorener Eval-Split für TV/Broadcast.** TVs Status als Trainingsdomäne blieb über
   die Phase bedingt (`docs/dataset-plan.md ## 1`/`## 8`); es existiert daher kein
   domänenspezifischer mAP-Bericht für Broadcast, nur Trainingsdaten.

## Datenschutz

Das Material zeigt identifizierbare Personen (Gesichter, teils Rückennummern) in Vollbildern
(D-20, keine Unschärfe, keine Ausschnitte). Rechtsgrundlage: die Verbandsfreigabe vom
2026-08-31 (`docs/capture-legal.md ## Nachtrag 2026-08-31`), die Analyse, Hackathon-Nutzung UND
Veröffentlichung abdeckt (Details: `docs/dataset-publication.md ## 2`). Zurückgehalten bleiben
bis nach dem BWI-Hackathon (23.–27.11.2026): die 18 eingefrorenen Drohnen- und 12 GoPro-Clips
(die zugleich das private Hackathon-Testset sind) sowie die komplette Puerto-Rico-Partie. Dieser
Datensatz-Stand (v2) enthält keinen dieser zurückgehaltenen Clips (verifiziert per
`assert_no_frozen_eval_clips`, siehe „Provenienz").

## Lizenz

**Entscheidung des Nutzers (2026-09-11): `defer`** — die Lizenzwahl (CC BY-NC 4.0, CC BY 4.0,
eine Custom-Research-Use-Lizenz, oder Verschiebung; alle vier Optionen mit Begründung in
`docs/dataset-publication.md ## 4`) wird bewusst auf den tatsächlichen
Veröffentlichungszeitpunkt verschoben, nicht jetzt festgeschrieben (volle Begründung und
Wortlaut: `docs/dataset-publication.md ## 7`).

**Veröffentlichung selbst ist unentschieden, nicht nur die Lizenz.** D-19 legt lediglich den
frühestmöglichen Zeitpunkt fest (nach dem Hackathon), keine Zusage, dass zu diesem Zeitpunkt
tatsächlich veröffentlicht wird. Der Nutzer hält das Code-Repository weiterhin privat
(`docs/lizenz-inventur.md ## Entscheidung 2026-09-09`, eine unabhängige, bereits getroffene
Entscheidung über den Code, nicht über den Datensatz) — ein Signal in dieselbe Richtung: ob und
wann dieser Datensatz tatsächlich veröffentlicht wird, bleibt zum Zeitpunkt dieses
Phasenabschlusses offen. Bis dahin gilt: dieser Datensatz ist **nicht veröffentlicht** und
unterliegt ausschliesslich der projektinternen Nutzung im Rahmen der Verbandsfreigabe.
