# EPA-Update für den Oktober-Sync 2026

Status: am 2026-09-04 geprüft, freigegeben für den Oktober-Sync (keine Textänderungen).
Champion-Entscheidung: siehe Schlusszeile (Stand) — Beförderung der `with_hc`-Läufe steht als Entscheidung an, bis dahin bleibt der bisherige Champion.

Das hier ist eine reproduzierbare, kalibrierte Neuberechnung deiner EPA/WP-Größen auf einem
größeren Korpus — **kein neues Modell und keine fancigere Mathematik.** Gleiche Methode wie im
letzten Bericht (Leave-one-game-out, dieselben Features, dieselben Hyperparameter), nur mehr
Datenpunkte: deine drei Workbooks sind jetzt Teil des trainierbaren Korpus. Jede Zahl in diesem
Dokument steht in einer committeten CSV unter `data/reference/epa_refinement/` (oder
`data/reference/hc_sp_tables/` für deine eigenen Tabellen) — ein automatisierter Test
(`tests/test_m3_epa_docs.py`) prüft das bei jedem Lauf, damit hier nie eine Zahl "aus dem
Gedächtnis" landen kann.

## Der Korpus

Was beim Einlesen deiner drei Workbooks tatsächlich ankommt (`docs/hc-workbook-ingest.md`, Lauf
vom 2026-09-03, vor der zusätzlichen Korpuserweiterung unten):

| Workbook / Sheet | Zeilen gelesen | Spiele: vorher → nachher | trainierbar (damals) |
|---|---:|---:|---:|
| Germany Analytics Stats EC 2025 vs WC Nations / Data | 269 | 3 → 3 | 0 (Frage 1: dauerhaft ausgeschlossen) |
| Offense Analytics 2026 Camps and Competitions / Data | 1.926 | 35 → 35 | 25 |
| Scoring Probability by Situation 2023-2026 / Data | 13.811 | 289 → 174 | 10 |
| Scoring Probability by Situation 2023-2026 / Copy of Data | 3.895 | 1.801 → 1.645 | 0 (Frage 2 offen) |
| **Summe** | **19.901** | **2.128 → 1.857** | **35** |

**Bitte nicht "2.128 Spiele korrigiert" zitieren** — das ist die rohe Zeilen-Fragmentierung vor
jeder Bereinigung, nicht die trainierbare Zahl. Die aussagekräftige Zahl ist, wie viele Spiele
tatsächlich als Trainingsdaten in `plays.parquet` landen. Seit diesem Bericht sind drei weitere
Zulassungsregeln für deine numerisch segmentierten Blöcke (Platzhalter-Zeilen entfernen,
`PLAY #` aus der echten Zeilenreihenfolge neu vergeben, `posteam`/`defteam` aus deinem
`O`/`D`/`S`-Marker für vorläufige Spiele ableiten) dazugekommen, ohne dass dabei irgendeine
Prüfung abgeschwächt wurde. Ergebnis, direkt aus `data/reference/epa_refinement/ablation_summary.csv`
nachrechenbar: das Leave-one-game-out-Training mit deinen Daten läuft jetzt über **306 Folds**
statt **214** — eine Differenz von **92 Spielen**, bei beiden Modellen (EP und WP) identisch, was
unabhängig bestätigt, dass kein einziges deiner Spiele beim Training verlorengeht. Dieselbe Zahl
92 ergibt sich auch direkt aus `data/reference/epa_refinement/corpus_arms.csv` (339 Spiele im
"mit HC"-Arm minus 247 im "ohne HC"-Arm). **92 trainierbare Kopftrainer-Spiele, hoch von 35** —
das ist die Zahl für den Oktober-Sync.

Der Rest der ursprünglichen ~2.013–2.128 Zeilen-Fragmente bleibt weiterhin gesperrt, größtenteils
hinter Frage 2 (`docs/hc-rueckfragen-2026-09.md`) — siehe `## Was noch nicht drin ist` unten für
die vier konkreten Blöcke und was jeden davon freischalten würde. 22 Spiele aus dem
Team-Namenspaar-Block der Scoring-Probability-Tabelle bleiben speziell deshalb gesperrt, weil
posteam/defteam dort weiterhin unbestimmbar sind (siehe unten).

## Was wir an deinen Daten reparieren mussten

**(a) Die Halbzeit-Information fehlt in den Workbooks.** Für jedes trainierbare Kopftrainer-Spiel
setzen wir einen dokumentierten Ersatzwert, `half = 2` (konstant fürs ganze Spiel) — nicht `null`
und nicht `1`, weil nur `2` sowohl die `half_assigned`-Prüfung echt erfüllt als auch die
Spielende-/Sieger-Logik beider Modelle korrekt feuern lässt. Der Preis: eine torlose Drive vor der
(nicht existierenden) Halbzeitgrenze erbt im Label den nächsten tatsächlichen Score des Spiels,
statt an der echten Halbzeit als "kein Score" zu enden. Details und die vollständige
Entscheidungstabelle stehen in `docs/data-contract.md` (Abschnitt "`half` für
hc_workbook-Zeilen"), der reale Lauf in `docs/hc-workbook-ingest.md`. Ob du echte
Halbzeit-Marker liefern kannst, ist jetzt Zusatzfrage A (siehe `## Offene Fragen`).

**(b) Die Spielgrenzen wurden früher an jedem Ballbesitzwechsel getrennt.** Ein Wechsel von
Offense zu Defense innerhalb desselben echten Spiels zählte als "neues Spiel" — ein einzelnes
internationales Spiel zerfiel dadurch in dutzende Ein-bis-Zehn-Zeilen-Fragmente. Das ist jetzt
nicht mehr so: die Segmentierung gruppiert nach dem (ungeordneten) Team-Paar bzw. nach deiner
Kopfzeilen-/`O`/`D`/`S`-Marker-Konvention, die du am 2026-09-03 bestätigt hast. Details in
`docs/hc-workbook-ingest.md`.

## Methode

Leave-one-game-out über `game_id` (jedes Spiel genau einmal Testspiel, niemals gleichzeitig
Trainings- und Testdaten) — dieselbe Methodik wie seit Phase 1.3, keine Änderung. Wettbewerbs-Tier
läuft als Kovariate mit; aktuell trägt jede Zeile im Korpus (deine eingeschlossen)
`competition_tier = mixed-other` (`data/reference/epa_refinement/per_tier_metrics_ep.csv` /
`per_tier_metrics_wp.csv` zeigen je Arm genau eine Nicht-Pool-Tier-Zeile) — eine Annahme, keine
Tatsache; Zusatzfrage B fragt dich danach. Kalibrierungsreport (Reliability-Kurven,
Log-Loss-gegen-Grundrate) wie gehabt, MLflow-Versionierung mit manueller Freigabe
(`docs/model-training.md`) — kein Modell wird automatisch "Champion", das bleibt eine bewusste,
von dir/uns gemeinsam geprüfte Entscheidung. Vier neue, gemessene Trainingsläufe
(`data/reference/epa_refinement/ablation_summary.csv`), je ein Arm "ohne" und "mit" deinen Daten
für EP und WP:

| Run-ID | Modell | Arm |
|---|---|---|
| `0ab2ea15b4d445f9a8bbe453b64724e0` | EP | ohne HC |
| `6f3f5bce32564441b83803267f8c716c` | EP | mit HC |
| `4dd832d184a8493f94d974ab68b57032` | WP | ohne HC |
| `55e64ecfa9804c6cab0624e8ce991485` | WP | mit HC |

## Kalibrierung — schlägt das Modell die einfache Grundrate?

**Für EP: ja, zum ersten Mal in diesem Projekt.** Der letzte Bericht (Phase 1.3,
`.planning/phases/01.3-methodical-model-retraining/01.3-TRAINING-REPORT.md`) musste damals
offen zugeben, dass EP seine eigene Grundrate NICHT schlägt (Log-Loss 1,027657 gegen 1,007274,
214 Folds, ohne deine Daten). Beide neuen EP-Läufe schlagen ihre Grundrate jetzt real:

| Modell | Arm | Plays | Folds | Metrik | Naive Grundrate | Verbesserung | Run-ID |
|---|---|---:|---:|---:|---:|---:|---|
| EP | ohne HC | 16.444 | 214 | 0,957593 | 1,007274 | 0,049680 | `0ab2ea15b4d445f9a8bbe453b64724e0` |
| EP | mit HC | 22.808 | 306 | 0,945720 | 0,997945 | 0,052225 | `6f3f5bce32564441b83803267f8c716c` |
| WP | ohne HC | 16.646 | 214 | 0,368903 | 0,691095 | 0,322192 | `4dd832d184a8493f94d974ab68b57032` |
| WP | mit HC | 23.373 | 306 | 0,356967 | 0,691406 | 0,334438 | `55e64ecfa9804c6cab0624e8ce991485` |

Wichtige Einschränkung, damit das nicht als "wir haben EP repariert" missverstanden wird: der
"ohne HC"-Lauf hat exakt dieselbe `n_plays`/`n_folds`-Zahl wie der Phase-1.3-Bericht (16.444/214),
sein eigener gemessener Wert (0,957593) weicht trotzdem von 1,3s Zahl (1,027657) ab — Feature-Set
und Hyperparameter sind seit Phase 1.3 unverändert eingefroren, die beiden Läufe sind also nicht
aus derselben exakten Laufumgebung und nicht bit-identisch vergleichbar. Der ehrliche Wertevergleich
ist deshalb "mit HC" gegen "ohne HC" **innerhalb desselben Laufs** oben, nicht gegen die alte
1,3-Dokument-Zahl. **WP schlägt seine Grundrate weiterhin deutlich**, wie schon in Phase 1.3
(damals 0,367263 gegen 0,691095) — hier keine Überraschung, nur bestätigt auf mehr Daten.

**Pro-Quelle-Aufschlüsselung, Arm "mit HC"** (`data/reference/epa_refinement/per_source_metrics_ep.csv`,
`per_source_metrics_wp.csv`) — deine eigenen Quellen schlagen ihre eigene Grundrate genauso gut
wie der Rest des Korpus, teils besser:

| Quelle | EP n | EP Log-Loss | EP Naive | EP Verbesserung | WP n | WP Log-Loss | WP Naive | WP Verbesserung |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `hc_workbook:offense-analytics-2026-camps-and-competitions:data` | 1.010 | 0,978589 | 1,011820 | 0,033230 | 1.097 | 0,303328 | 0,679168 | 0,375840 |
| `hc_workbook:scoring-probability-by-situation-2023-2026:data` | 5.354 | 0,907694 | 0,949146 | 0,041452 | 5.630 | 0,352311 | 0,689261 | 0,336950 |
| `legacy` | 3.386 | 0,976962 | 1,022547 | 0,045586 | 3.583 | 0,344014 | 0,693068 | 0,349055 |
| `legacy-sportapp` | 13.058 | 0,950668 | 1,002672 | 0,052003 | 13.063 | 0,367032 | 0,690090 | 0,323058 |
| `__pooled__` | 22.808 | 0,945720 | 0,997945 | 0,052225 | 23.373 | 0,356967 | 0,691406 | 0,334438 |

Deine `scoring-probability`-Quelle ist auf beiden Modellen die beste Einzelquelle im Korpus (EP
Log-Loss 0,907694 — deutlich besser als der gepoolte Wert 0,945720). Deine `offense-analytics`-
Quelle ist auf beiden Modellen die schwächste Quelle des Korpus, schlägt ihre eigene Grundrate
aber trotzdem — kein Grund, sie auszuschließen, nur eine ehrliche Beobachtung.

## Was deine Daten beitragen

Gleiche Methode, einmal mit und einmal ohne deine Zeilen, aus derselben Tabelle oben:
EP verbessert sich von 0,049680 auf 0,052225 (Log-Loss-Verbesserung gegen Grundrate), WP von
0,322192 auf 0,334438. Beides ist eine reale, aber **kleine bis moderate** Verbesserung — kein
großer Sprung. Der größere, unmittelbare Effekt ist nicht die Kalibrierung selbst, sondern die
Statistik dahinter: 92 zusätzliche Folds (306 statt 214) geben jeder gemessenen Zahl mehr
Tragfähigkeit, unabhängig davon, wie groß die Verbesserung selbst ausfällt.

## Dein Ansatz und das Modell nebeneinander

Deine Methode: empirische Trefferquote (Scoring Probability) je Down, Distanz-zu-gehen und
Feldhälfte, aus deinen eigenen gecharteten Zeilen — die Tabs `SP by D&D`/`EP by D&D` in
`Scoring Probability by Situation 2023-2026.xlsx`, hier read-only gespiegelt in
`data/reference/hc_sp_tables/sp_by_dd.csv`/`ep_by_dd.csv`. Wir haben dieselbe Achse aus unserem
Korpus nachgebaut (`data/reference/epa_refinement/comparison_by_dd.csv`, 120 Zellen, 0 fehlende
Kombinationen) und dein veröffentlichtes SP/EP, unsere Neuberechnung deiner eigenen Zeilen, unsere
übrigen Zeilen und das Modell (out-of-fold, nie ein Champion-Rescore) nebeneinandergestellt, mit
`n` in jeder Spalte:

| Down | Distanz | Hälfte | Dein SP (n) | Dein EP | Modell-EP (n) | Abstand |
|---|---:|---|---|---:|---|---:|
| 4 | 7 | gegnerisch | 29 | 3,72 | 51 (1,02) | 2,70 |
| 4 | 6 | eigen | 70 | 2,91 | 134 (0,36) | 2,55 |
| 4 | 12 | eigen | 40 | 1,50 | 82 (-0,76) | 2,26 |
| 4 | 11 | eigen | 39 | 1,54 | 77 (-0,72) | 2,25 |
| 3 | 7 | gegnerisch | 51 | 5,53 | 84 (3,35) | 2,18 |

Die fünf größten Abweichungen sind alle 4. (und eine 3.) Down — das Modell ist bei späten Downs
und mittlerer Distanz durchweg pessimistischer als deine Tabelle, und das Modell-`n` ist dabei
immer größer (Out-of-Fold deckt den gesamten gepoolten Korpus ab, deine Tabelle nur deine eigenen
gecharteten Zeilen). Genau das ist der Sample-Size-Kontrast, der für das Modell spricht: von den
120 Zellen sind **78 (65 %)** deiner eigenen neu berechneten Zellen dünn besetzt (`n < 30`,
`hc_recomputed_thin` in `comparison_by_dd.csv`) — bei unseren übrigen Zeilen sind es nur
**16 von 120 (13 %)**.

Zum Kontrollieren, ob deine veröffentlichten Zahlen selbst reproduzierbar sind: die Neuberechnung
deiner eigenen Zeilen trifft dein veröffentlichtes SP meistens gut, mit denselben Ausreißern, die
schon dünn besetzt sind — kein Hinweis auf eine systematisch andere Definition:

| Down | Distanz | Hälfte | Veröffentlicht (n) | Neu berechnet (n) | Abstand |
|---|---:|---|---|---|---:|
| 1 | 14 | eigen | 0,33 (3) | 1,00 (1) | 0,67 |
| 1 | 7 | eigen | 0,60 (5) | 1,00 (1) | 0,40 |
| 4 | 14 | gegnerisch | 0,13 (24) | 0,50 (12) | 0,38 |
| 1 | 11 | eigen | 0,67 (3) | 1,00 (1) | 0,33 |
| 4 | 10 | gegnerisch | 0,25 (32) | 0,58 (19) | 0,33 |

Alle fünf sitzen auf `n <= 19` mindestens einer Seite (drei sogar bei `n = 1` neu berechnet) —
das ist Kleinstichproben-Rauschen, keine Definitionslücke. Sollte dieser Abstand auf einer
größer besetzten Zelle einmal systematisch auftauchen, wäre das eine andere Definition von
"Scoring Probability" wert nachzufragen, kein Datenfehler.

Eine strukturelle Randnotiz: **dein `EP by D&D`-Tab ist, bis auf Rundungsfehler, exakt sechs mal
dein `SP by D&D`-Tab** (Touchdown = 6 Punkte) — über alle 120 Zellen liegt der mittlere absolute
Abstand bei 0,0000. Dein EP-Tab ist also eine reine Skalierung deines SP-Tabs, keine eigenständig
geschätzte Erwartung.

Vier Zellen fehlen in der geclusterten Achse (`data/reference/epa_refinement/comparison_clustered.csv`,
`comparison_coverage.csv`): der "25+ Yards zu gehen, bereits in der gegnerischen Hälfte"-Fall,
einmal je Down (`n` bei dir 1/18/16/9). In unserem Korpus kommt das praktisch nicht vor — 26+
Yards vor dem nächsten First Down zu brauchen, während man bereits auf gegnerischer Hälfte steht,
ist im Flag Football fast unmöglich. Eine echte Rand-Situation, kein Join-Fehler.

## Was wir bewusst nicht reproduziert haben

Der `Reg`-Tab in deiner Scoring-Probability-Tabelle enthält per-Down-Polynome steigenden Grades je
Feldhälfte (z. B. `=5,71 + -0,0377*C98 + -0,00222*C98²` für Down 1, gegnerische Hälfte) — das Muster
einer aus einem Excel-Trendlinien-Diagramm übernommenen Kurve, verbatim gesichert in
`data/reference/hc_sp_tables/reg_formulas.csv` (16 Formeln, 4 Downs × 2 Hälften × 2 Formelarten).
Wir bieten stattdessen ein kreuzvalidiertes Modell an statt einer Zelle-für-Zelle-Trendlinie —
warum, sagen die Kalibrierungszahlen oben besser als jede Bewertung hier.

## Was noch nicht drin ist

| Was | Warum | Was es freischalten würde |
|---|---|---|
| Team-Namenspaar-Block, Scoring Probability `Data` (22 Spiele) | `posteam`/`defteam` unbestimmt: jede Zeile trägt ein Team-Namenspaar statt eines echten `ODK`; deine `O`/`D`/`S`-Marker-Konvention kommt in diesem realen Block kein einziges Mal vor (verifiziert) | Eine künftige Chartierung mit der Marker-Konvention löst das automatisch; sonst eine von dir bestätigte Zuordnung pro Spiel |
| `Copy of Data` (1.645 Fragmente) | Anderes, undokumentiertes Spaltenlayout ab `RECEIVED BY` (Frage 2) | Deine Bestätigung der Spaltenreihenfolge für diesen Tab |
| Germany Analytics EC-2025-Workbook | Frage 1: bewusst leer, Scoring Probability ist die alleinige Grundlage für alle drei Workbooks | Nichts mehr — dauerhaft geklärt |
| 3 korrupte Bin-Labels in den geclusterten Tabs | Excel-Autokorrektur wandelte `"1-5"`/`"6-10"`/`"11-15"` in Datumswerte um; wir rekonstruieren sie als Text und markieren sie `[ASSUMED]` | Nichts — kein Fehler von dir, eine bekannte Excel-Falle |

Zusätzlich, gemessen in `data/reference/epa_refinement/no_play_rows.csv`: `Timeout`/`Offsetting
Penalties`-Zeilen zählen bei uns als "kein Play" und fließen nicht ins Training. Der gemessene
Anteil an den jeweiligen Quellenzeilen: `hc_workbook:offense-analytics-2026-camps-and-competitions:data`
1,78 % (21 von 1.181), `hc_workbook:scoring-probability-by-situation-2023-2026:data` **4,74 %**
(267 von 5.635), `legacy` **3,92 %** (145 von 3.701). Die beiden fett markierten Quellen liegen über
der intern verabredeten 2-%-Eskalationsschwelle — das wird hier als Befund gemeldet, nicht selbst
entschieden: ob ein zusätzlicher Filter vor dem Training sinnvoll ist, gehört auf die Agenda des
nächsten Freigabe-Checkpoints (M3-02-08), nicht in dieses Dokument.

## Offene Fragen

Vollständiger Wortlaut in `docs/hc-rueckfragen-2026-09.md`. Für den Oktober-Sync konkret relevant:

- **Frage 1** (beantwortet): der EC-2025-`Data`-Tab ist bewusst leer, Scoring Probability ist die
  Grundlage für alle drei Workbooks.
- **Frage 2** (teilweise beantwortet): die Blocksegmentierung ist geklärt, die Spaltenverschiebung
  ab `RECEIVED BY`/das komplette Spaltenlayout von `Copy of Data` bleibt offen.
- **Zusatzfrage A** (neu, dieses Update): gibt es Halbzeit-Marker für deine Workbook-Spiele?
- **Zusatzfrage B** (neu, dieses Update): wie sollen wir deine Spiele einordnen — Camp, Scrimmage
  oder Länderspiel?

## Reproduzierbarkeit

```
ffep ingest
uv run python scripts/hc_games_refill.py
uv run python scripts/hc_corpus_ablation.py --model both
uv run python scripts/hc_sp_snapshot.py
uv run python scripts/epa_comparison.py
```

MLflow-Run-IDs: siehe Tabelle in `## Methode`. `training_data_sha256` je Arm (aus
`data/reference/epa_refinement/ablation_summary.csv`): EP ohne HC
`5a71cb29a90f030dd353cb8cb1b421314f8475c3b87126c780aa6f0341acce0d`, EP mit HC
`c095135cbf70091bd6a76ddd3b096170696803a3ed4996276143baf23f86e62a`, WP ohne HC
`d5e65d3e319c84662df1cbd4174fd8e9b31313429b4ed049d6442970ca4feac9`, WP mit HC
`73a6f7d66fbd14d6b51f6b2cd3572c79f4bb3078631c9dcf2406435f4e6263c1`.

Jede Zahl in diesem Dokument kommt aus genau einer dieser committeten Dateien:
`data/reference/epa_refinement/ablation_summary.csv`,
`data/reference/epa_refinement/per_source_metrics_ep.csv`,
`data/reference/epa_refinement/per_source_metrics_wp.csv`,
`data/reference/epa_refinement/per_tier_metrics_ep.csv`,
`data/reference/epa_refinement/per_tier_metrics_wp.csv`,
`data/reference/epa_refinement/corpus_arms.csv`,
`data/reference/epa_refinement/no_play_rows.csv`,
`data/reference/epa_refinement/comparison_by_dd.csv`,
`data/reference/epa_refinement/comparison_clustered.csv`,
`data/reference/epa_refinement/comparison_coverage.csv`, sowie
`data/reference/hc_sp_tables/*.csv` für deine eigenen Tabellen. `tests/test_m3_epa_docs.py`
prüft das automatisch bei jedem Testlauf.

Stand: 2026-09-04 (Review abgeschlossen; Champion-Entscheidung offen, siehe Status oben).
