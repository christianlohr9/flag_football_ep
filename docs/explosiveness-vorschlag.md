# Explosiveness & Efficiency: Vorschlag für den Oktober-Sync

Stand: 2026-09-03. Antwort auf deine Frage aus dem Sync ("was ist, wenn eine Spielerin nur
11 Yards erzielt?") und auf die Recherche in `docs/explosiveness-recherche.md`. Alle Zahlen
unten sind auf unserem eigenen Korpus gemessen, nicht aus der Literatur übernommen; die
Messtabellen liegen unter `data/reference/explosiveness/` und dieses Dokument wird gegen
sie geprüft (jede Zahl hier hat eine Zeile dort).

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
16.067 Scrimmage-Plays (Lauf oder Pass, Down 1-4) aus drei Quellen -- `legacy` (3.701
Zeilen, ohne echtes Saison-Tag), `legacy-sportapp` (14.545 Zeilen, Saison 2024) und `ifaf`
(3.191 Zeilen, Saison 2026).

Zwei Dinge fehlen in diesem Korpus noch, ausdrücklich benannt statt stillschweigend
übergangen: erstens sind deine eigenen Workbook-Zeilen noch nicht enthalten (Quelle
`hc_workbook` kommt nicht vor) -- M3-1 liest sie ein, M3-2 rechnet sie neu durch, danach
aktualisieren sich diese Zahlen automatisch, ohne dass dieses Dokument neu geschrieben
werden muss. Zweitens hat der Korpus noch keine `Efficiency`-Spalte (dein `Data!O`) --
deine Efficiency-Reproduktion ist im Code fertig vorbereitet (`hc_efficiency_table`), aber
auf echten Zeilen noch nicht berechnet, weil die Spalte fehlt.

## Deine heutige Zahl, wörtlich reproduziert

Wir haben zwei unterschiedliche Regeln bei dir gefunden und beide -- getrennt, ohne eine
für die andere zu halten -- auf denselben 14.991 Pass-Attempts nachgerechnet:

| Regel | Formel | Ergebnis |
|---|---|---|
| Workbook-Formel (`Player Analysis All Camps!R2:S2`) | `Yards > 12`, nur Pass | 15,8 % (2.365/14.991) |
| Mündliche Regel (aus dem Sync) | `Yards > 12` ODER `EPA > 0`, nur Pass | 48,6 % (7.290/14.991) |

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

Auf unseren Daten zeigt sich das konkret: die mündliche "oder"-Regel (48,6 %) liegt fast
exakt bei der reinen Success Rate (EPA > 0: 47,7 %, 7.657/16.067). Die Yards-Klausel trägt
kaum etwas Eigenes bei -- nur 84 von 14.991 Pass-Attempts (0,6 %) werden AUSSCHLIESSLICH
durch "Yards > 12" explosive, während EPA bei ihnen nicht positiv ist. Deine mündliche
Regel misst also in der Praxis fast nur Efficiency, nicht "große Plays" -- genau die
Verwechslung, die die Literatur als Kernproblem benennt.

## Die Klippe, in Zahlen

Das ist die direkte Antwort auf deine Frage: die 10-12-Yard-Zone (unmittelbar um deinen
Cutoff bei 12) im Detail, Yard für Yard, mit Anzahl und Anteil an allen 16.067
Scrimmage-Plays.

| Yards | n | Anteil | Balken |
|---|---|---|---|
| 8 | 877 | 5,5 % | █████ |
| 9 | 789 | 4,9 % | █████ |
| 10 | 806 | 5,0 % | █████ |
| 11 | 536 | 3,3 % | ███ |
| 12 | 385 | 2,4 % | ██ |
| — Cutoff (12/13) — | | | |
| 13 | 314 | 2,0 % | ██ |
| 14 | 263 | 1,6 % | ██ |
| 15 | 235 | 1,5 % | █ |
| 16 | 212 | 1,3 % | █ |

Die Zone 10-12 Yards allein hält 10,7 % aller Scrimmage-Plays (1.727 Plays). Das ist keine
Randerscheinung: mehr als jeder zehnte Play in unserem Korpus liegt so nah am Cutoff, dass
ein einziger Yard mehr oder weniger das Etikett "explosive" kippt -- genau dein Einwand,
jetzt als Zahl statt als Vermutung.

## Vorschlag

Efficiency als Success Rate (`EPA > 0`) -- stabil, kontextsensitiv (Down, Distance,
Feldposition stecken schon im EP-Modell), keine neue Logik nötig.

Explosiveness als Kandidat B (EPA-Magnitude auf erfolgreichen Plays, IsoPPP-Stil): ein Play
gilt als explosive, wenn er erfolgreich war (`EPA > 0`) UND seine EPA über einem
Schwellenwert liegt, der aus unserem eigenen Korpus stammt -- der 80. Perzentile der EPA
aller erfolgreichen Plays. Aktuell (kalibriert auf 16.067 Plays, davon 7.657 erfolgreich):
Schwellenwert = 2,69 EPA. Dieser Wert ist nicht geraten oder aus der NFL übernommen -- er
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
- `data/reference/explosiveness/comparison_overall.csv` -- alle Team-Zahlen dieses
  Dokuments.
- `data/reference/explosiveness/comparison_by_player.csv` -- dieselben Zahlen pro Spieler
  (pseudonymisiert).
- `data/reference/explosiveness/cliff_zone.csv` -- die Klippen-Zone-Tabelle oben.
- `data/reference/explosiveness/calibration.json` -- der kalibrierte Schwellenwert und
  seine Herkunft.
