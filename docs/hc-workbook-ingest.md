# HC-Workbook-Ingest

Dieses Dokument berichtet den realen `ffep ingest --source hc_workbook`-Lauf über die drei
Workbooks des Cheftrainers: was gelesen wurde, was doppelt zu unseren Daten ist, was die
Validierung ergab und was für den Cheftrainer noch offen ist. Alle Zahlen unten stammen aus dem
Lauf vom 2026-09-03 (Contract v1.2) und aus `data/processed/validation-report-latest.md`, nicht
aus Schätzungen.

## Was eingelesen wurde

| Workbook | Sheet | Zeilen gelesen | Zeilen übernommen | Spiele erkannt | davon provisorisch |
|---|---|---:|---:|---:|---:|
| Germany Analytics Stats EC 2025 vs WC Nations.xlsx | Data | 269 | 269 | 3 | 3 |
| Offense Analytics 2026 Camps and Competitions.xlsx | Data | 1.926 | 1.926 | 35 | 35 |
| Scoring Probability by Situation 2023-2026.xlsx | Data | 13.811 | 13.108 | 289 | 280 |
| Scoring Probability by Situation 2023-2026.xlsx | Copy of Data | 3.895 | 3.895 | 1.801 | 1.801 |
| **Summe** | | **19.901** | **19.198** | **2.128** | **2.119** |

"Zeilen übernommen" ist die Zeilenzahl nach dem Dedupe-Schritt unten (Ausschlüsse nur bei
erklärter Paarung); "Zeilen gelesen" ist, was `read_sheet_rows` als echte Datenzeile zählt (leere
Zeilen im benutzten Bereich sind bereits herausgerechnet, siehe die einzelnen `#N/A`- und
Leerzeilen-Meldungen im Validierungsbericht).

**Germany Analytics Stats EC 2025 vs WC Nations.xlsx, Tab `Data`:** gilt weiterhin als **leer,
Antwort ausstehend** (Frage 1, `docs/hc-rueckfragen-2026-09.md`) — der Cheftrainer beschreibt,
dort immer die komplette Play-by-Play einzutragen, aber praktisch alle 2.506 Datenzeilen sind in
den Kernspalten leer. Die 269 nicht-leeren Zeilen, die dieser Lauf trotzdem fand (Zeilen 201-580),
liegen unter einem Header, der an dieser Stelle `O`/`D` statt `PLAY #`/`ODK` trägt, und tragen
bereits gefüllte `EP`/`EP After`/`EPA`-Spalten — das sieht nach eingefügten Resten einer
EP-Analyse-Tabelle aus, nicht nach roher Play-by-Play-Charting. Diese 3 Spiele bleiben deshalb
bewusst provisorisch; sie zählen hier nicht als "die aktuellen EC-2025-Daten".

**Scoring Probability, `Data` und `Copy of Data`:** beide Tabs mischen zwei Zeilenarten unter
einem Header (Frage 2, offen) — ein numerischer Block mit echter `PLAY #`/`ODK` und ein
Team-Namenspaar-Block, bei dem `PLAY #`/`ODK` fehlen. Für den Team-Namenspaar-Block bestätigt
dieser Lauf zusätzlich einen bislang nicht dokumentierten Befund: die Spielsegmentierung
(`segment_games`) trennt bei *jedem* Wechsel des Team-Paars, also auch bei einem reinen
Possession-Wechsel innerhalb desselben echten Spiels (`Germany, Ireland` → `Ireland, Germany` →
`Germany, Ireland` ...). Ein einzelnes reales Spiel zerfällt dadurch in dutzende bis über hundert
Ein-bis-Zehn-Zeilen-"Spiele" (`Copy of Data` allein: 1.801 solcher Fragmente aus 3.895 Zeilen).
Diese Fragmente sind keine echten, eigenständigen Spiele im Sinne von `hc_games.csv` — ihnen
manuell eine `game_id` zuzuweisen würde eine Genauigkeit vortäuschen, die die Daten nicht hergeben.
Sie bleiben deshalb alle provisorisch; siehe `## Wartung` für den Verweis auf diesen Befund als
Voraussetzung für eine spätere Bereinigung.

## Duplikate

**Erklärte Paarung, echter Fund:** der numerische Block der `Scoring-Probability`-`Data`-Tabelle
enthält 94 saubere, PLAY#-basierte Spiele (Zeilen 662-8.748). Ein Fingerprint-Abgleich der ersten
neun davon gegen den bestehenden `legacy`-Korpus (`data_raw.csv`, 47 handgechartete Spiele) ergab
eine fast vollständige Übereinstimmung mit `legacy-39` bis `legacy-47` — den letzten neun Spielen
dieses Korpus:

| HC-Block | Ziel (`corpus_game_id`) | HC-Zeilen | übereinstimmend | Übereinstimmung |
|---|---|---:|---:|---:|
| b01-g00 | legacy-39 | 90 | 87 | 96,7 % |
| b01-g01 | legacy-40 | 85 | 83 | 97,6 % |
| b01-g02 | legacy-41 | 81 | 79 | 97,5 % |
| b01-g03 | legacy-42 | 75 | 75 | 100 % |
| b01-g04 | legacy-43 | 70 | 67 | 95,7 % |
| b01-g05 | legacy-44 | 83 | 81 | 97,6 % |
| b01-g06 | legacy-45 | 87 | 85 | 97,7 % |
| b01-g07 | legacy-46 | 73 | 70 | 95,9 % |
| b01-g08 | legacy-47 | 78 | 76 | 97,4 % |

Diese neun Paarungen sind in `data/reference/hc_games.csv` als `corpus_game_id` eingetragen (HC-D03:
"Duplikate bei ihm erkennen und nicht berücksichtigen"). Der reale Lauf schließt dadurch **703
HC-Zeilen** aus (Summe der "übereinstimmend"-Spalte); die restlichen 2-3 Zeilen je Spiel sind
Chartingabweichungen (z. B. eine zusätzliche oder fehlende Zeile) und bleiben im Datensatz. Die
übrigen 85 Spiele desselben numerischen Blocks (`b01-g09` bis `b01-g93`) sowie beide `Data`-Blöcke
mit Team-Namenspaaren zeigen **keine** vergleichbar starke Übereinstimmung mit irgendeinem
bestehenden Spiel und bleiben unverändert.

**Undeklarierte Überschneidungen (nur gemeldet, nicht ausgeschlossen):** 134.040 Fingerprint-Treffer
über 1.724 HC-Spiele hinweg (732.407 übereinstimmende Zeilen in Summe), keiner davon ausgeschlossen,
da keine `corpus_game_id` erklärt ist. Die überwältigende Mehrheit davon ist Rauschen: der
Durchschnitt liegt bei ca. 5-6 übereinstimmenden Zeilen pro gemeldetem Paar — eine kurze,
generische Fingerprint-Kombination (z. B. `DN=1, DIST=10, RESULT=Complete` ohne Empfänger) trifft
zufällig auf viele unabhängige Spiele. Nur die neun oben genannten Paare mit 70+ übereinstimmenden
Zeilen sind belastbare Duplikat-Kandidaten; alles andere braucht eine manuelle Prüfung, bevor es in
`hc_games.csv` als `corpus_game_id` eingetragen wird (siehe `## Wartung`).

**Data / Copy of Data (Scoring Probability):** die Fragmentierung des Team-Namenspaar-Blocks (siehe
oben) verhindert eine belastbare `corpus_game_id`-Paarung zwischen den beiden Tabs für dieses Sync.
Der ursprüngliche Verdacht (`Germany`/`Ireland`-Block, `Data`-Zeilen 2-660 vs. `Copy of
Data`-Zeilen 461-573) ist qualitativ bestätigt — beide Bereiche zeigen dasselbe Team-Paar-Muster —
aber nicht auf Zeilenebene verifizierbar, solange jede Possession-Wechsel-Zeile als eigenes
"Spiel" segmentiert wird.

**HC ↔ Hudl:** nicht messbar. Der aktuell committete `games.parquet`-Snapshot enthält **kein
einziges `hudl`-Quellspiel** (die beiden Dateien unter `data/raw/hudl/` scheitern an
`UnmappedTeamError` für `AUT`/`UKR`, unabhängig von diesem Plan) — ein HC↔Hudl-Abgleich wäre daher
per Definition leer und würde fälschlich suggerieren, geprüft und nichts gefunden zu haben.

**HC ↔ IFAF (WM 2026):** der IFAF-Korpus enthält sechs GER-Spiele der WM 2026 (gegen USA, PAN, AUT,
MEX, ITA, SLO). Keines davon ist Irland — die `Germany`/`Ireland`-Paarung in der
Scoring-Probability-Tabelle stammt also aus einem anderen Wettbewerb oder einer anderen Saison.
Ein Abgleich der übrigen Team-Paare (u. a. `Germany`/`Austria`, was es real in der WM 2026 gibt)
wurde aus demselben Grund wie oben (Fragmentierung) nicht in `hc_games.csv` übernommen.

## Validierung

| Quelle | akzeptiert | akzeptiert (mit Warnung) | quarantänisiert | Spiele gesamt |
|---|---:|---:|---:|---:|
| hudl | 0 | 0 | 0 | 0 (Quelle komplett abgewiesen, `UnmappedTeamError`, außerhalb dieses Plans) |
| legacy | 46 | 1 | 0 | 47 |
| legacy-sportapp | 163 | 5 | 0 | 168 |
| ifaf | 32 | 0 | 10 | 42 |
| hc_workbook | 0 | 0 | **2.128** | 2.128 |

**Jede HC-Zeile landet in diesem Lauf in Quarantäne** — 100 % der 2.128 Spiele. Das ist keine
Fehlfunktion, sondern die ehrliche Konsequenz aus HC-D05 (`hc_workbook` steht bewusst nicht in
`_WARN_ONLY_SOURCES`, damit ein fehlschlagender Check nie stillschweigend durchgewunken wird) plus
einer strukturellen Lücke: `ingest_workbook` setzt `half` für jede HC-Zeile auf `null` (die
Workbooks kennen keine Halbzeitgrenzen), und `half_assigned` verlangt zwingend `half ∈ {1, 2}`.
Fehlgeschlagene Checks über alle 2.128 HC-Spiele:

| Check | fehlgeschlagene Spiele |
|---|---:|
| `half_assigned` | 2.128 (100 %) |
| `gapless_play_ids` | 1.279 |
| `downs_range` | 172 |
| `monotonic_drive_ids` | 8 |

**706 der 2.128 Spiele scheitern ausschließlich an `half_assigned`** — wären also ohne die fehlende
Halbzeitinformation akzeptabel. Die restlichen 1.422 Spiele haben zusätzlich strukturelle Probleme
(meist `gapless_play_ids`, plausibel für die stark fragmentierten Team-Namenspaar-"Spiele", deren
`PLAY #` synthetisiert wird und bei sehr kurzen Fragmenten leicht lückenhaft wirkt, sowie für den
numerischen `Offense-Analytics`-Block, dessen reale `PLAY #`-Nummerierung streckenweise Lücken
hat). `score_reconstruction` wird für kein einziges HC-Spiel ausgewertet — es existiert keine
`final_scores.csv`-Referenzzeile für eine provisorische oder neu vergebene `hc-`-ID; das ist HC-D05s
"Camp-Spiele dürfen legitim an der Score-Rekonstruktion scheitern" in seiner mildesten Form (hier:
gar nicht erst geprüft, mangels Referenz).

Die neun deklarierten Duplikate (`hc-scoring-probability-dup-legacy-39` … `-47`) quarantänisieren
ebenfalls (`half_assigned` plus, je nach Spiel, `downs_range`/`monotonic_drive_ids`/
`gapless_play_ids` an den nach dem Dedupe verbliebenen 2-3 Restzeilen) — das Dedupe-Ergebnis ist
unabhängig vom Validierungsergebnis: erst werden Duplikate ausgeschlossen, danach validiert der
Rest wie jede andere HC-Zeile auch.

**Gesamtlauf** (`ffep ingest`, alle fünf Quellen): 2.385 Spiele, 21.437 akzeptierte Plays,
2.138 quarantänisiert, 6 mit Warnung (alle `legacy`/`legacy-sportapp`, unabhängig von diesem Plan).

## RESULT-Vokabular

Token-Zählung über alle vier Sheets (19.901 gelesene HC-Zeilen), unabhängig vom Validierungsstatus:

| Contract-v1.2-Token (neu, aus den HC-Workbooks) | Anzahl |
|---|---:|
| `Block` | 37 |
| `Blocked` (Schreibvariante, nur in `Blocked, Def TD`) | 1 |
| `Batted Down` | 9 |
| `Dropped` | 94 |
| `Timeout` | 17 |
| `Offsetting Penalties` | 8 |

Bestehendes 13-Token-Vokabular (Auszug, größte Zähler): `Complete` 9.356, `Incomplete` 4.651,
`TD` 1.797, `Rush` 1.031, `Good` 976, `Penalty` 766, `No Good` 585, `Interception` 455, `Sack` 246,
`Def TD` 54, `Fumble` 44, `Safety` 20. `KNEEL` kommt in keiner einzigen HC-Zeile vor (deckt sich mit
dem bestehenden Legacy-Korpus).

**Noch unbekannt (`tok_unknown`):** `-5.0` — 2 Vorkommen, exakt der in `M3-01-RESEARCH.md` als
Sichtungsfund dokumentierte Zahlenwert an einer `RESULT`-Stelle (offensichtlicher Charting-Fehler).
Kein weiteres außerhalb des Contract-Vokabulars liegendes Token wurde gefunden.

## Spielerzuordnung

122 verschiedene, nicht zugeordnete Spieler-Label über alle vier Sheets (33 zahlenförmig — vermutlich
Rückennummern —, 89 namensförmig). `data/reference/player_mapping.csv` erhielt in diesem Lauf
**keine neue Zeile**: keines der 89 namensförmigen Label stimmt exakt mit genau einem
`player_name` in `data/reference/roster.csv` überein, und eine Rückennummer lässt sich ohne
bekanntes Team (siehe oben — praktisch alle 2.128 HC-Spiele sind provisorisch, `home_team`/
`away_team` bleiben `null`) nicht eindeutig einem Roster zuordnen. Die vollständige Rohliste liegt
ausschließlich unter `data/raw/hc_files/unmapped_players_<run_id>.txt` (gitignored) — nie
committet, nie in diesem Dokument zitiert.

## Offene Fragen

**Frage 1** (`docs/hc-rueckfragen-2026-09.md`): ist der `Data`-Tab in "Germany Analytics Stats EC
2025 vs WC Nations.xlsx" leer gemeint? **Status: offen, unbeantwortet.** Dieser Lauf ändert daran
nichts — die 269 gefundenen Zeilen sehen nach einer eingefügten Analyse-Tabelle aus, nicht nach der
vom Cheftrainer beschriebenen Play-by-Play, und bleiben provisorisch (siehe oben).

**Frage 2** (`docs/hc-rueckfragen-2026-09.md`): welche Spaltenbedeutung haben die
Team-Namenspaar-Zeilen in "Scoring Probability by Situation 2023-2026.xlsx"? **Status: offen,
unbeantwortet.** Dieser Lauf fügt einen konkreten Folgebefund hinzu: die aktuelle
Spielsegmentierung zerlegt einen echten Team-Namenspaar-Abschnitt an jedem Possession-Wechsel in
viele kleine Fragmente (siehe `## Was eingelesen wurde`) — eine Neusegmentierung nach echten
Spielgrenzen setzt eine Antwort auf Frage 2 voraus (die wahre Spaltenbedeutung ab `RECEIVED BY`
muss zuerst geklärt sein, sonst würde jede Grenzheuristik auf denselben unbekannten Spalten raten).

**Frage 3** (`docs/hc-rueckfragen-2026-09.md`): stimmen unsere Bedeutungen für die sechs neuen
`RESULT`-Werte? **Status: offen, unbeantwortet** (Antwort-Abschnitt in
`docs/hc-rueckfragen-2026-09.md` noch leer). Alle sechs Werte sind bereits als Contract v1.2
implementiert (siehe `## RESULT-Vokabular`) und laufen produktiv mit unserer vorgeschlagenen
Semantik — eine Korrektur durch den Cheftrainer würde `src/flag_football_ep/ingest/hudl.py`s
`derive_outcome_columns` betreffen, nicht dieses Plans Dateien.

**Neu, dieser Lauf — Frage 4 (Vorschlag, noch nicht an den Cheftrainer gestellt):** sollen `Timeout`-
und `Offsetting Penalties`-Zeilen (zusammen 25 Vorkommen) vor dem EP-/WP-Training herausgefiltert
werden? Beide werden bereits als `play_type = "no_play"` behandelt (kein trainierbarer Play), die
Entscheidung, ob sie überhaupt in `plays.parquet` verbleiben oder vor dem Training zusätzlich
gefiltert werden, liegt bei M3-2 (siehe `## Wartung` und die M3-01-04-SUMMARY.md).

## Wartung

**Eine Zeile zu `data/reference/hc_games.csv` hinzufügen:** ein erneuter `ffep ingest --source
hc_workbook`-Lauf meldet für jedes noch unbekannte `(workbook, sheet, block_key)` genau eine Zeile
im Report ("Unbekanntes Spiel ... provisorische game_id ... vergeben") mit Zeilenbereich, Play-Zahl
und (bei einem Team-Namenspaar-Block) den rohen Team-Labels. `workbook`/`sheet` sind die
slugifizierten Datei-/Tab-Namen (`flag_football_ep.ingest.hc_workbook.slugify`); `block_key` steht
in der Meldung. `game_id` muss mit `hc-` beginnen; `corpus_game_id` bleibt leer, außer das Spiel
dupliziert nachweislich ein bereits bekanntes Spiel (siehe `## Duplikate` für den Beleg-Maßstab: nur
Übereinstimmungen im Bereich von 90+ % der Zeilen einer Seite rechtfertigen eine Paarung; ein
`corpus_game_id`-Eintrag ist sicher in dem Sinne, dass nur inhaltlich passende Zeilen ausgeschlossen
werden — ein falscher Eintrag verliert also höchstens die Chance auf eine Zusammenführung, löscht
aber nie eine Zeile, die nicht wirklich übereinstimmt).

**Eine Zeile zu `data/reference/player_mapping.csv` hinzufügen:** `source` = `hc_workbook`
(bewusst grob, gilt für alle drei Workbooks), `source_player` = das rohe Label aus
`unmapped_players_<run_id>.txt`, `canonical_player` = der volle Name aus `roster.csv`. Nur
eintragen, wenn das Label eindeutig einem einzigen Roster-Eintrag entspricht — niemals die
gitignorete Rohliste unverändert einfügen.

**Nach jeder Änderung:** ein erneuter `ffep ingest --source hc_workbook` (oder `ffep ingest` für
den vollen Lauf) liest die aktualisierten Referenz-CSVs automatisch ein; keine weitere manuelle
Schritte nötig.
