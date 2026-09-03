# HC-Workbook-Ingest

Dieses Dokument berichtet den realen `ffep ingest --source hc_workbook`-Lauf über die drei
Workbooks des Cheftrainers: was gelesen wurde, was doppelt zu unseren Daten ist, was die
Validierung ergab und was für den Cheftrainer noch offen ist. Die Zahlen im Abschnitt "Was
eingelesen wurde"/"Validierung" stammen aus dem M3-02-04-Lauf vom 2026-09-03 (Contract v1.2, nach
der Segmentierungs- und `half`-Korrektur aus M3-02-01 und der `hc_games.csv`-Neubefüllung durch
`scripts/hc_games_refill.py`) und aus `data/processed/validation-report-latest.md`; die
vorangestellten "vorher"-Zahlen sind der ursprüngliche M3-01-04-Lauf, unverändert dokumentiert,
damit der Effekt der Korrektur sichtbar bleibt statt überschrieben zu werden.

## Was eingelesen wurde

| Workbook | Sheet | Zeilen gelesen | Spiele erkannt: vorher → nachher | davon trainierbar (deklariert, ≥1 Zeile nach Dedupe) |
|---|---|---:|---:|---:|
| Germany Analytics Stats EC 2025 vs WC Nations.xlsx | Data | 269 | 3 → 3 | 0 (bewusst nicht eingelesen, siehe unten) |
| Offense Analytics 2026 Camps and Competitions.xlsx | Data | 1.926 | 35 → 35 | 25 |
| Scoring Probability by Situation 2023-2026.xlsx | Data | 13.811 | 289 → 174 | 10 |
| Scoring Probability by Situation 2023-2026.xlsx | Copy of Data | 3.895 | 1.801 → 1.645 | 0 (bewusst nicht eingelesen, siehe unten) |
| **Summe** | | **19.901** | **2.128 → 1.857** | **35** |

Die Verschiebung der Gesamt-Spielzahl (2.128 → 1.857) kommt fast vollständig aus der
Segmentierungskorrektur (M3-02-01): `Data`s Team-Namenspaar-Block fällt von 137 auf 22 echte
Spiele (unten erklärt), `Copy of Data`s über 150 verwandte Namenspaar-Teilblöcke von 1.801 auf
1.645 Fragmente — die Zahl ist kleiner, aber **nicht** deshalb schon trainierbar (siehe
`## Nicht eingelesen (bewusst)`). Die aussagekräftigere Zahl für den Cheftrainer-Sync ist die
**trainierbare** Spielzahl, also wie viele Spiele tatsächlich `data/processed/plays.parquet`
erreichen: **35** (25 aus Offense Analytics, 10 aus Scoring Probability), alle mit `half=2`
(Sentinel, siehe `## Validierung`) und beide mit `posteam`/`defteam`, weil beide numerisch
segmentierte Blöcke sind (echtes `ODK`). Das ist der Wert, der in `docs/epa-refinement-2026-10.md`
zitiert werden sollte, nicht "2.128 Spiele gefunden" oder "1.857 Spiele gefunden".

**Germany Analytics Stats EC 2025 vs WC Nations.xlsx, Tab `Data`:** Frage 1 ist beantwortet
(`docs/hc-rueckfragen-2026-09.md`, Antwort 2026-09-03 durch den Cheftrainer, Jona Winkel): der
`Data`-Tab dieser Datei ist **bewusst leer** — die Scoring-Probability-Datei ist die alleinige
play-by-play-Grundlage für alle drei Workbooks. Diese Datei trägt damit **dauerhaft, nicht nur
vorläufig**, keine Spiele zum Korpus bei; die 3 hier weiterhin gefundenen, nicht-leeren Zeilen
(eingefügte Reste einer EP-Analyse-Tabelle, siehe `## Nicht eingelesen (bewusst)`) bleiben aus
demselben Grund unberücksichtigt.

**Scoring Probability, `Data` und `Copy of Data`:** beide Tabs mischen zwei Zeilenarten unter
einem Header — ein numerischer Block mit echter `PLAY #`/`ODK` und ein Team-Namenspaar-Block. Für
den Team-Namenspaar-Block bestätigte M3-01-04 einen Segmentierungsfehler: die Spielsegmentierung
trennte bei *jedem* Wechsel des Team-Paars, also auch bei einem reinen Possession-Wechsel
innerhalb desselben echten Spiels (`Germany, Ireland` → `Ireland, Germany` → `Germany, Ireland`
...) — ein einzelnes reales Spiel zerfiel dadurch in dutzende Ein-bis-Zehn-Zeilen-"Spiele". M3-02-01
behebt das (ungeordneter Team-Paar-Schlüssel statt geordnetem), M3-02-04 ergänzt eine zweite,
vom Cheftrainer am 2026-09-03 bestätigte Konvention: er hat zeitweise nur noch `O`/`D`/`S` in
Spalte A eingetragen (Offense/Defense/kein Play) statt eines Team-Namenspaars, und später wieder
angefangen, den Namen nur in der ersten Zeile eines Blocks zu schreiben — alles darunter bis zur
nächsten Namenszeile gehört zu diesem Team-Paar. `flag_football_ep.ingest.hc_workbook` implementiert
beide Konventionen (`_split_pair_block`); **im echten `Data`-Tab-Block kommt die zweite Konvention
aber kein einziges Mal vor** (verifiziert 2026-09-03: 0 O/D/S-Marker-Zeilen im Block) — jede Zeile
trägt dort weiterhin ein (teils abgekürztes) Team-Namenspaar, das reale posteam/defteam bleibt
also für diesen Block weiterhin unbestimmbar, ohne zu raten. Siehe `## Nicht eingelesen (bewusst)`
für die Konsequenz.

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

**M3-02-04-Update:** diese neun Paarungen leben im *numerischen* Block 1 der `Data`-Tabelle, den
die M3-02-01-Segmentierungskorrektur nicht umnummeriert (nur der Team-Namenspaar-Block ändert
seine `block_key`s). `scripts/hc_games_refill.py` bestätigt das explizit, nicht nur der Annahme
nach: alle neun Zeilen in `hc_games.csv` sind nach der Neubefüllung byte-identisch mit vorher
(`git diff` zeigt ausschließlich neue Zeilen), und der reale `ffep ingest`-Lauf reproduziert exakt
dieselben neun Fingerprint-Übereinstimmungen (90/85/81/75/70/83/87/73/78 HC-Zeilen, 87/83/79/75/
67/81/85/70/76 übereinstimmend) wie oben dokumentiert — RESEARCH Pitfall 2 verifiziert, nicht nur
angenommen.

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

**M3-02-04-Update:** `half` ist seit M3-02-01/M3-02-04 kein pauschales `null` mehr für jede
HC-Zeile. Für ein in `hc_games.csv` deklariertes, nicht-`Copy of Data`-Spiel setzt `ingest_workbook`
den Sentinel-Wert `half = 2` — konstant fürs ganze Spiel, damit sowohl `half_assigned`
(`half ∈ {1, 2}`) echt erfüllt ist als auch `game_end`/WP-`Winner`/EP-`No_Score` korrekt einmal am
echten letzten Zeileneintrag feuern. Die Entscheidungstabelle, der verworfene Play-Count-Mittelwert
und der benannte Label-Qualitäts-Preis (keine echte Halbzeitgrenze, eine torlose erste-Halbzeit-Drive
erbt den nächsten tatsächlichen Score) stehen in `docs/data-contract.md` Abschnitt
"`half` für hc_workbook-Zeilen" — hier nicht wiederholt. Undeklarierte Spiele und jede `Copy of
Data`-Zeile behalten `half = null` und bleiben in Quarantäne, unverändert.

| Quelle | akzeptiert | akzeptiert (mit Warnung) | quarantänisiert | Spiele gesamt |
|---|---:|---:|---:|---:|
| hudl | 0 | 0 | 0 | 0 (Quelle komplett abgewiesen, `UnmappedTeamError`, außerhalb dieses Plans) |
| legacy | 46 | 1 | 0 | 47 |
| legacy-sportapp | 163 | 5 | 0 | 168 |
| ifaf | 32 | 0 | 10 | 42 |
| hc_workbook | **35** | 0 | **1.822** | **1.857** |

**35 der 1.857 HC-Spiele sind jetzt vollständig `OK`** (0/6 Checks fehlgeschlagen) — vorher (M3-01-04)
waren es 0 von 2.128. Fehlgeschlagene Checks über alle 1.857 HC-Spiele nach der Korrektur, mit dem
Vergleich zum M3-01-04-Vorher-Stand:

| Check | fehlgeschlagene Spiele (vorher, 2.128 Spiele) | fehlgeschlagene Spiele (nachher, 1.857 Spiele) | Status |
|---|---:|---:|---|
| `half_assigned` | 2.128 (100 %) | 1.673 | **behoben für 184 deklarierte Spiele** — der Sentinel wirkt genau wie vorgesehen |
| `gapless_play_ids` | 1.279 | 1.279 | **unverändert — erwartet.** Echte Lücken in der Charting-`PLAY #`-Nummerierung (Offense Analytics) bzw. in der synthetisierten Nummerierung sehr kurzer `Copy of Data`-Fragmente; das ist der Check, der seine Aufgabe erfüllt, kein Ziel für diese Korrektur |
| `downs_range` | 172 | 172 | **unverändert — erwartet.** Echte `null`-`DN`-Zellen im Charting (kein Wert eingetragen), unabhängig von Segmentierung/`half` |
| `monotonic_drive_ids` | 8 | 8 | **unverändert — erwartet.** Kleine, nicht mit dieser Korrektur zusammenhängende Restmenge |

Die drei unveränderten Zeilen sind der Beleg, dass die Segmentierungs-/`half`-Korrektur gezielt nur
`half_assigned` behoben hat, ohne irgendeinen anderen Check zu berühren — kein Check wurde
abgeschwächt, keine Quelle wurde zu `_WARN_ONLY_SOURCES` hinzugefügt (Plan-Vorgabe). `184` ist die
Zahl der deklarierten Spiele mit mindestens einer nach dem Dedupe verbliebenen Zeile (185 in
`hc_games.csv`, minus `legacy-42`, das nach Dedupe 0 Zeilen behält, siehe `## Duplikate`) — sie
erfüllen jetzt `half_assigned`, aber nur 35 davon bestehen auch die übrigen fünf Checks. Die
restlichen 149 haben zusätzlich einen strukturellen Befund, meist `downs_range` (echte
`null`-`DN`-Lücken, auch in ansonsten sauber numerisch segmentierten `Scoring-Probability`-Spielen —
ein neuer, nicht in M3-01-04 dokumentierter Befund: 138 der 164 quarantänisierten
`Scoring-Probability`-`Data`-Spiele scheitern an `downs_range`, deutlich mehr als ursprünglich für
`Copy of Data` vermutet). `score_reconstruction` wird weiterhin für kein einziges HC-Spiel
ausgewertet — es existiert keine `final_scores.csv`-Referenzzeile für eine provisorische oder neu
vergebene `hc-`-ID; das ist HC-D05s "Camp-Spiele dürfen legitim an der Score-Rekonstruktion
scheitern" in seiner mildesten Form (hier: gar nicht erst geprüft, mangels Referenz).

Die neun deklarierten Duplikate (`hc-scoring-probability-dup-legacy-39` … `-47`) quarantänisieren
weiterhin (`half_assigned` besteht jetzt für 8 von ihnen — `legacy-42` hat nach Dedupe 0 Zeilen —,
aber `downs_range`/`monotonic_drive_ids`/`gapless_play_ids` an den 2-3 nach dem Dedupe verbliebenen
Restzeilen bleiben bestehen) — das Dedupe-Ergebnis ist unabhängig vom Validierungsergebnis: erst
werden Duplikate ausgeschlossen, danach validiert der Rest wie jede andere HC-Zeile auch.

**Gesamtlauf** (`ffep ingest`, alle fünf Quellen): 2.114 Spiele, **23.401** akzeptierte Plays
(vorher 21.437, **+1.964** — exakt die Zeilenzahl der 35 jetzt trainierbaren HC-Spiele),
1.832 quarantänisiert, 6 mit Warnung (alle `legacy`/`legacy-sportapp`, unabhängig von diesem Plan).

## Nicht eingelesen (bewusst)

Vier Quellen/Blöcke bleiben absichtlich außerhalb des trainierbaren Korpus, jeweils mit dem Grund
und was ihn auflösen würde:

| Block | Grund | Was ihn auflösen würde |
|---|---|---|
| Scoring Probability `Data`, Team-Namenspaar-Block (22 Spiele nach der Segmentierungskorrektur) | `posteam`/`defteam` bleiben unbestimmt: jede Zeile trägt ein Team-Namenspaar statt eines echten `ODK`, und die im selben Plan implementierte Kopfzeilen-/O-D-S-Marker-Konvention (Frage 2, Antwort 2026-09-03) kommt in diesem realen Block nicht ein einziges Mal vor (verifiziert 2026-09-03) | Wenn eine künftige Workbook-Version dieselben Spiele mit der Kopfzeilen-/Marker-Konvention (nicht mit vollem Namenspaar pro Zeile) chartet, löst der bereits implementierte Code das automatisch; sonst nur eine vom Cheftrainer bestätigte, explizite Zuordnung pro Spiel |
| Scoring Probability `Data`, zwei Ein-Zeilen-Blöcke (`"CC 25"`, `"Mark"`) | Einzelzeilen-Notizen, keine Spiele — unterhalb von `MIN_PLAYS=5` in `scripts/hc_games_refill.py` | Keine — echtes Rauschen, kein Datenverlust |
| Scoring Probability `Copy of Data` (alle 1.645 Fragmente) | Ein anderes, undokumentiertes Spaltenlayout als `Data` (M3-02-RESEARCH.md Sec 1.3: 14 statt 15 Spalten, `YARD LN`/`Drive Success` vertauscht, zusätzliche Spalte `FH`, `Thrown By`/`YAC` fehlen) — eine Deklaration würde Passgeber/Empfänger/Raumgewinn erraten | Eine vom Cheftrainer bestätigte Spaltenzuordnung für diesen Tab (offene Zusatzfrage, siehe `## Offene Fragen`) |
| Germany Analytics Stats EC 2025 vs WC Nations.xlsx, `Data` (alle 3 Spiele) | Der Tab ist laut Cheftrainer bewusst leer — Scoring Probability ist die alleinige Play-by-Play-Quelle für alle drei Workbooks (Frage 1, beantwortet 2026-09-03) | Nichts — dauerhafter Ausschluss, keine offene Frage mehr |

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
2025 vs WC Nations.xlsx" leer gemeint? **Status: beantwortet, 2026-09-03** (Jona Winkel, HC): Ja,
der `Data`-Tab dieser Datei ist bewusst leer, die Scoring-Probability-Datei ist die alleinige
Play-by-Play-Grundlage für alle drei Workbooks. Konsequenz umgesetzt in M3-02-04: diese Datei
bleibt dauerhaft ohne Spiele im trainierbaren Korpus (`## Nicht eingelesen (bewusst)`), nicht mehr
"Antwort ausstehend".

**Frage 2** (`docs/hc-rueckfragen-2026-09.md`): welche Spaltenbedeutung haben die
Team-Namenspaar-Zeilen in "Scoring Probability by Situation 2023-2026.xlsx"? **Status: teilweise
beantwortet, 2026-09-03.** Der Cheftrainer bestätigte die Blocksegmentierung: eine Zeile mit
Team-Namen öffnet einen Block, `O`/`D`/`S` in Spalte A (Offense/Defense/kein Play) markiert die
folgenden Zeilen desselben Blocks, eine neue Namenszeile oder eine leere Zeile schließt ihn.
M3-02-04 implementiert das (`flag_football_ep.ingest.hc_workbook._split_pair_block`,
`_pair_row_marker`) — im echten `Data`-Tab-Block kommt die Marker-Konvention aber kein einziges
Mal vor (0 O/D/S-Zeilen, verifiziert), das Blockergebnis bleibt daher unverändert bei den bereits
bekannten 22 Spielen. **Weiterhin offen:** die eigentliche Frage-2-Spaltenverschiebung ab
`RECEIVED BY` ist damit NICHT beantwortet — der Cheftrainer hat die Segmentierung erklärt, nicht
die Tail-Spalten. Ein neuer, konkreter Befund aus M3-02-RESEARCH.md Sec 1.3 macht das für
`Copy of Data` greifbar: dieser Tab hat ein anderes Spaltenlayout als `Data` (14 statt 15 Spalten,
`YARD LN`/`Drive Success` vertauscht, eine zusätzliche Spalte `FH`, `Thrown By`/`YAC` fehlen
komplett) — die Antwort auf Frage 2 muss also getrennt für `Data`s Tail-Spalten UND für
`Copy of Data`s komplett anderen Spaltenaufbau erfolgen, nicht mit einer einzigen Zuordnung für
beide Tabs.

**Frage 3** (`docs/hc-rueckfragen-2026-09.md`): stimmen unsere Bedeutungen für die sechs neuen
`RESULT`-Werte? **Status: beantwortet, 2026-09-03** (Jona Winkel, HC): Ja. Alle sechs Werte laufen
bereits als Contract v1.2 implementiert (siehe `## RESULT-Vokabular`) mit exakt der bestätigten
Semantik — keine Code-Änderung nötig.

**Neu, dieser Lauf — Frage 4 (Vorschlag, noch nicht an den Cheftrainer gestellt):** sollen `Timeout`-
und `Offsetting Penalties`-Zeilen (zusammen 25 Vorkommen) vor dem EP-/WP-Training herausgefiltert
werden? Beide werden bereits als `play_type = "no_play"` behandelt (kein trainierbarer Play), die
Entscheidung, ob sie überhaupt in `plays.parquet` verbleiben oder vor dem Training zusätzlich
gefiltert werden, liegt bei M3-2 (siehe `## Wartung` und die M3-01-04-SUMMARY.md).

## Wartung

**`scripts/hc_games_refill.py` (M3-02-04):** ein deterministisches Skript statt handgetippter
Zeilen für die beiden numerisch segmentierten Quellen (`Offense Analytics 2026 Camps and
Competitions.xlsx`/`Data`, `Scoring Probability by Situation 2023-2026.xlsx`/`Data`s numerische
Blöcke). Es deklariert nur Spiele mit mindestens `MIN_PLAYS=5` Plays, überspringt Team-Namenspaar-
und `Copy of Data`-Blöcke bewusst (siehe `## Nicht eingelesen (bewusst)`), lässt jede bereits
deklarierte Zeile byte-identisch stehen und bricht mit einer benannten Fehlermeldung ab, falls ein
bereits deklarierter `block_key` nach einer Segmentierungsänderung nicht mehr auftaucht (statt
ihn stillschweigend zu verlieren). `--dry-run` zeigt die Zahlen, ohne zu schreiben. Eine von Hand
hinzugefügte Zeile (siehe unten) funktioniert weiterhin unverändert — das Skript überschreibt
nichts, es ergänzt nur.

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
