# Explosiveness & Efficiency: Vorschlag für den Oktober-Sync

Stand: 2026-09-04 (reviewt und freigegeben durch dich am 2026-09-04, vor dem Oktober-Sync --
Kandidat B, EPA-Magnitude mit Perzentil-Schwelle, angenommen als unsere Explosiveness-Kennzahl;
Herleitung und Alternativen in `docs/explosiveness-entscheidung.md`). Antwort auf deine Frage aus
dem Sync ("was ist, wenn eine Spielerin nur 11 Yards erzielt?") und auf die Recherche in
`docs/explosiveness-recherche.md`. Alle Zahlen unten sind auf unserem eigenen Korpus gemessen,
nicht aus der Literatur übernommen; die Messtabellen liegen unter `data/reference/explosiveness/`
und dieses Dokument wird gegen sie geprüft (jede Zahl hier hat eine Zeile dort).

Keine Spielernamen in diesem Dokument (öffentlich, git-versioniert).

## Worum es geht

Du berechnest "Explosive %" aktuell als: mehr als 12 Yards und/oder positive EPA (nach
deiner eigenen Berechnung). Deine Frage dazu war berechtigt: was ist, wenn eine Spielerin
nur 11 Yards erzielt -- ist der Play dann automatisch "nicht explosive", obwohl er fast
identisch war? So krasse Thresholds sind immer ein Problem, weil sie zwei fast gleiche
Plays unterschiedlich behandeln, nur weil einer zufällig einen Yard mehr oder weniger
Raumgewinn hatte.

Dieses Dokument zeigt drei Dinge: erstens deine heutige Zahl, wörtlich und unverändert auf
unseren Plays nachgerechnet; zweitens, was diese Zahl eigentlich misst, wenn man sie in
ihre zwei Bestandteile zerlegt; drittens einen Vorschlag, der die Klippe bei 12 Yards
auflöst, ohne deine Kennzahl zu ersetzen oder falsch zu erklären.

## Datengrundlage

Basis ist `data/processed/plays_scored.parquet`, unser aktueller kanonischer Spielstand:
21.907 Scrimmage-Plays (Lauf oder Pass, Down 1-4) aus fünf Quellen -- `legacy` (3.701
Zeilen, ohne echtes Saison-Tag), `legacy-sportapp` (14.545 Zeilen, Saison 2024), `ifaf`
(3.191 Zeilen, Saison 2026) und, seit dem Nachtrag unten, zwei deiner eigenen Workbooks
(`hc_workbook:offense-analytics-2026-camps-and-competitions`, 1.183 Zeilen, und
`hc_workbook:scoring-probability-by-situation-2023-2026`, 5.635 Zeilen).

Bis zum 2026-09-04 fehlten zwei Dinge in diesem Korpus, ausdrücklich benannt statt
stillschweigend übergangen: deine eigenen Workbook-Zeilen und die `Efficiency`-Spalte
(dein `Data!O`). Beide sind jetzt vorhanden -- siehe den Nachtrag unten für die
Neukalibrierung, die das ausgelöst hat.

### Korrektur 2026-09-04 (Nenner)

Zwei Nenner in diesem Dokument waren falsch und sind jetzt korrigiert, direkt anhand der
Formelzellen `D2` und `U2` im Tab `Player Analysis All Camps` nachgeprüft. Erstens: deine
`Attempts`-Zelle (`D2`) addiert Comps + Incs + INTs -- Sacks zählen nicht mit. Unsere frühere
Fassung dieses Dokuments hatte einen etwas größeren Nenner (inklusive Sacks) verwendet; alle
Zahlen unten sind jetzt auf dem korrekten, kleineren Nenner nachgerechnet. Zweitens: deine
`Efficiency`-Zelle (`U2`) teilt durch Attempts + Carries (Pass- und Lauf-Attempts zusammen),
nicht durch Attempts + Drops -- die Efficiency-Reproduktion im Code (`hc_efficiency_table`)
folgt jetzt deiner Tabellenformel als Hauptzahl, während die ältere Attempts-plus-Drops-Lesart
als zweite, klar benannte Zahl daneben stehen bleibt. Beide Lesarten bleiben sichtbar
nebeneinander, bis Frage 4, 5 und 6 in `docs/hc-rueckfragen-2026-09.md` beantwortet sind --
das ist unsere Korrektur, nicht dein Fehler.

### Nachtrag 2026-09-04 (Neukalibrierung auf dem erweiterten Korpus)

Autorisiert, weil die Definition selbst am 2026-09-04 freigegeben wurde (siehe `Stand:` oben und
`docs/explosiveness-entscheidung.md`) -- die Zahlen unten waren aber auf einem Korpus gerechnet,
der deine eigenen Zeilen noch gar nicht enthielt
(`.planning/todos/pending/2026-09-04-explosiveness-kalibrierung-mit-hc-korpus.md`). Zwischen der
ursprünglichen Kalibrierung (2026-09-03, 19:55 UTC) und heute wurden deine Workbook-Zeilen
eingelesen und neu gescort; alle Zahlen in diesem Dokument sind jetzt auf dem erweiterten Korpus
neu gerechnet (`uv run python scripts/explosiveness_comparison.py --recalibrate`).

| Kennzahl | Vorher (03.09., ohne deine Zeilen) | Nachher (04.09., mit deinen Zeilen) |
|---|---|---|
| Kalibrierungs-Korpus (alle Scrimmage-Plays) | 16.067 | 21.907 |
| ... davon erfolgreich (`EPA > 0`) | 7.657 | 10.554 |
| Explosiveness-Schwellenwert (80. Perzentile) | 2,69 EPA | 2,66 EPA |
| `corpus_fingerprint` | `f5f11469...b53c834` | `0ebc5fcc...0dad0f8c4` |
| Workbook-Formel (Yards>12, nur Pass) | 16,0 % (2.365/14.739) | 15,4 % (3.097/20.138) |
| Mündliche Regel (Yards>12 oder EPA>0, nur Pass) | 49,4 % (7.284/14.739) | 49,7 % (10.011/20.138) |
| Success Rate (EPA>0, alle Scrimmage-Plays) | 47,7 % (7.657/16.067) | 48,2 % (10.554/21.907) |
| Explosiveness (EPA-Magnitude, alle Scrimmage-Plays) | 9,6 % (1.535/16.067) | 9,6 % (2.112/21.907) |
| Klippen-Zone 10-12 Yards | 10,7 % (1.727 Plays) | 10,5 % (2.300 Plays) |
| `baseline_hc_workbook`-Vergleich mit deinen eigenen Zeilen | 0 deiner Zeilen enthalten | 5.399 deiner Pass-Attempts jetzt Teil des Nenners |
| `Efficiency`-Spalte (`Data!O`) im Korpus | fehlt | vorhanden, `hc_efficiency_table` berechnet erfolgreich |

Die Kernaussage bleibt unverändert -- die mündliche "oder"-Regel liegt weiterhin fast exakt bei
der Success Rate, der Schwellenwert verschiebt sich nur geringfügig (2,69 auf 2,66 EPA), und die
Klippen-Zone bleibt bei rund einem von neun Plays. Neu ist, dass die Vergleichszahlen jetzt zum
ersten Mal tatsächlich deine eigenen 5.399 Pass-Attempts enthalten (nicht mehr nur die drei
älteren Quellen), und dass die `Efficiency`-Spalte selbst jetzt im Korpus vorhanden ist -- ihre
Bedeutung (Frage 5) bleibt trotzdem offen, deshalb zeigt dieses Dokument weiterhin keine
Efficiency-Zahlen. `data/reference/explosiveness/comparison_by_player.csv` trägt jetzt 416
statt 360 Zeilen (neue Pseudonyme für zuvor nicht enthaltene Spieler:innen).

## Deine heutige Zahl, wörtlich reproduziert

Wir haben zwei unterschiedliche Regeln bei dir gefunden und beide -- getrennt, ohne eine
für die andere zu halten -- auf denselben 20.138 Pass-Attempts nachgerechnet (seit dem
Nachtrag oben einschließlich deiner eigenen 5.399 Pass-Attempts):

| Regel | Formel | Ergebnis |
|---|---|---|
| Workbook-Formel (`Player Analysis All Camps!R2:S2`) | `Yards > 12`, nur Pass | 15,4 % (3.097/20.138) |
| Mündliche Regel (aus dem Sync) | `Yards > 12` ODER `EPA > 0`, nur Pass | 49,7 % (10.011/20.138) |

Der Befund ohne Wertung: deine Tabellen-Formel prüft ausschließlich Yards -- keine EPA
irgendwo in der Formelkette. Deine mündliche Beschreibung im Sync war eine andere,
zusätzliche Regel, die die Tabelle nicht umsetzt. Beide Zahlen sind real und beide sind
"deine Zahl" -- nur zwei verschiedene. Quelle der Formelzellen:
`docs/explosiveness-recherche.md`.

## Der Befund: zwei Fragen in einer Kennzahl

Die Football-Analytics-Literatur (Bill Connelly, siehe `docs/explosiveness-recherche.md`)
trennt seit Jahren zwei Fragen, die eine einzelne "Explosive %"-Zahl vermischt:
**Efficiency** ("war der Play gut genug?", binär) und **Explosiveness** ("wie groß war er,
gegeben dass er gut war?", nur über die guten Plays gemittelt).

Auf unseren Daten zeigt sich das konkret: die mündliche "oder"-Regel (49,7 %) liegt fast
exakt bei der reinen Success Rate (EPA > 0: 48,2 %, 10.554/21.907). Die Yards-Klausel trägt
kaum etwas Eigenes bei -- nur 102 von 20.138 Pass-Attempts (0,5 %) werden AUSSCHLIESSLICH
durch "Yards > 12" explosive, während EPA bei ihnen nicht positiv ist. Deine mündliche
Regel misst also in der Praxis fast nur Efficiency, nicht "große Plays" -- genau die
Verwechslung, die die Literatur als Kernproblem benennt.

## Die Klippe, in Zahlen

Das ist die direkte Antwort auf deine Frage: die 10-12-Yard-Zone (unmittelbar um deinen
Cutoff bei 12) im Detail, Yard für Yard, mit Anzahl und Anteil an allen 21.907
Scrimmage-Plays.

| Yards | n | Anteil | Balken |
|---|---|---|---|
| 8 | 1198 | 5,5 % | █████ |
| 9 | 1093 | 5,0 % | █████ |
| 10 | 1050 | 4,8 % | █████ |
| 11 | 724 | 3,3 % | ███ |
| 12 | 526 | 2,4 % | ██ |
| — Cutoff (12/13) — | | | |
| 13 | 412 | 1,9 % | ██ |
| 14 | 347 | 1,6 % | ██ |
| 15 | 296 | 1,4 % | █ |
| 16 | 272 | 1,2 % | █ |

Die Zone 10-12 Yards allein hält 10,5 % aller Scrimmage-Plays (2.300 Plays). Das ist keine
Randerscheinung: mehr als jeder zehnte Play in unserem Korpus liegt so nah am Cutoff, dass
ein einziger Yard mehr oder weniger das Etikett "explosive" kippt -- genau dein Einwand,
jetzt als Zahl statt als Vermutung.

## Vorschlag

Efficiency als Success Rate (`EPA > 0`) -- stabil, kontextsensitiv (Down, Distance,
Feldposition stecken schon im EP-Modell), keine neue Logik nötig.

Explosiveness als Kandidat B (EPA-Magnitude auf erfolgreichen Plays, IsoPPP-Stil): ein Play
gilt als explosive, wenn er erfolgreich war (`EPA > 0`) UND seine EPA über einem
Schwellenwert liegt, der aus unserem eigenen Korpus stammt -- der 80. Perzentile der EPA
aller erfolgreichen Plays. Aktuell (kalibriert auf 21.907 Plays, davon 10.554 erfolgreich):
Schwellenwert = 2,66 EPA. Dieser Wert ist nicht geraten oder aus der NFL übernommen -- er
wird direkt aus unseren Daten berechnet und zusammen mit dem Korpus, aus dem er stammt, in
`data/reference/explosiveness/calibration.json` gespeichert. Das heißt: der Wert kann
jederzeit neu abgeleitet werden, statt einfach geglaubt zu werden.

Kandidat C (ein stetiger Score zwischen 0 und 1) als zusätzliche, unterstützende Ansicht,
nicht als Kopfzahl -- Coaches brauchen weiterhin eine einzelne griffige Zahl.

Warum das die Klippe auflöst: ein Play mit 11 Yards und ein Play mit 12 Yards bei
vergleichbarer Situation (Down, Distance, Feldposition) haben fast dieselbe EPA und
bekommen deshalb fast dieselbe Einordnung -- statt eines harten "nein" gegen ein hartes
"ja" an einer Yard-Grenze, die nichts über den tatsächlichen Wert des Plays für den
Spielausgang aussagt.

### Entscheidung

Am 2026-09-04 angenommen: Kandidat B wird unsere Explosiveness-Kennzahl. Welche Varianten
geprüft wurden, warum B und nicht A oder die reine mündliche Regel, und wie der Schwellenwert
künftig neu berechnet und versioniert wird, steht ausführlich und eigenständig in
`docs/explosiveness-entscheidung.md`.

## Kleine Stichproben

Jede Rate in unseren Tabellen trägt ihr `n` und ein Konfidenzintervall (Clopper-Pearson);
Zeilen mit `n` unter 5 werden als "muted" markiert (grau/gekennzeichnet), aber nie
versteckt -- eine dünne Stichprobe wird gezeigt und als dünn benannt, nicht weggelassen.
Zusätzlich bieten wir eine Shrinkage-Spalte (`shrunk_rate`) an: sie zieht die Rate eines
Spielers mit wenigen Attempts leicht Richtung Team-Durchschnitt, stärker je kleiner die
Stichprobe -- eine ergänzende Ansicht neben der echten Rate, kein Ersatz für sie. Keine
Rate wird je ohne ihr `n` gezeigt.

## Was das im Report bedeutet (Übergabe an M3-4)

Die folgenden Funktionen aus `flag_football_ep.features.explosiveness` sind die fertige
Schnittstelle: `scrimmage_plays`, `hc_workbook_explosive_rate`, `hc_verbal_explosive_rate`,
`hc_efficiency_table`, `calibrate`/`ExplosivenessCalibration`/`write_calibration`/
`load_calibration`, `success_flag`, `explosive_epa_flag`, `explosive_score`,
`DEFINITIONS`/`definition_comparison`/`shrink_rate`, `cliff_zone_table`. Dazu zwei
headless Chart-Renderer aus `flag_football_ep.charts.explosiveness`:
`render_cliff_zone` und `render_definition_comparison`. Die Verdrahtung dieser Zahlen in
`reports/own_team.py` ist bewusst nicht Teil dieser Phase -- das übernimmt M3-4.

## Offene Fragen

Drei Fragen konnten wir nicht aus den Daten allein beantworten -- sie stehen ausführlich in
`docs/hc-rueckfragen-2026-09.md` als Frage 4, 5 und 6:

- Frage 4: ist die Yards-only-Formel für "Explosive %" so gewollt, oder soll die
  EPA-Klausel in die Tabelle?
- Frage 5: was genau steckt hinter der manuell gecharteten `Efficiency`-Spalte (`Data!O`)?
- Frage 6: sollen Läufe in "Explosive %" und "Efficiency" mitzählen, oder bleibt es
  pass-only wie heute?

## Quellen

- `docs/explosiveness-recherche.md` -- die Literaturrecherche (Connelly, PFF, Sam Hoppen)
  und die wörtliche Formel-Herkunft.
- `docs/explosiveness-entscheidung.md` -- das Entscheidungsprotokoll: welche Varianten geprüft
  wurden, warum Kandidat B, und wie der Schwellenwert versioniert wird.
- `data/reference/explosiveness/comparison_overall.csv` -- alle Team-Zahlen dieses
  Dokuments.
- `data/reference/explosiveness/comparison_by_player.csv` -- dieselben Zahlen pro Spieler
  (pseudonymisiert).
- `data/reference/explosiveness/cliff_zone.csv` -- die Klippen-Zone-Tabelle oben.
- `data/reference/explosiveness/calibration.json` -- der kalibrierte Schwellenwert und
  seine Herkunft.
