# Rückfragen zu den drei Workbooks aus dem Sync vom 2026-09-03

**Was das ist:** Drei kurze Rückfragen zu deinen Workbooks (`Germany Analytics Stats EC 2025 vs WC Nations.xlsx`, `Scoring Probability by Situation 2023-2026.xlsx`, `Offense Analytics 2026 Camps and Competitions.xlsx`), die wir beim Einlesen in unsere Pipeline nicht aus den Daten selbst beantworten konnten.

**Warum das wichtig ist:** Die Antworten entscheiden, ob zwei der drei Workbooks korrekt oder gar nicht in die kanonischen Daten kommen.

**Wie antworten:** Formlos, ein Satz pro Frage reicht.

## Frage 1 — Ist der `Data`-Tab in "Germany Analytics Stats EC 2025 vs WC Nations.xlsx" leer gemeint?

Der Header ist vollständig und passt zu deiner Beschreibung, aber alle 2.506 Datenzeilen sind in allen Kernspalten leer. Die Auswertungs-Tabs (`Player Stats All Games`, die Gegner-Tabs) zeigen weiterhin echte Zahlen — die stammen aber aus früheren Zeilen, nicht aus dem aktuellen (leeren) `Data`-Tab.

Drei Möglichkeiten, die du einfach auswählen kannst:

- (a) bewusst geleert vor der Weitergabe
- (b) versehentlich gelöscht
- (c) die aktuellen EC-2025-Daten liegen inzwischen in einem der anderen beiden Workbooks

Konkrete Bitte: bei (b) oder (c) ein Re-Export bzw. der Hinweis, welche Datei die aktuellen Zeilen hält.

Ohne Antwort: Die Datei wird als "leer, Antwort ausstehend" gemeldet und nicht als Quelle mit null Spielen verbucht.

## Frage 2 — Welche Spaltenbedeutung haben die Zeilen mit Team-Namen in "Scoring Probability by Situation 2023-2026.xlsx"?

In `Data` und `Copy of Data` stehen in rund 653 von 3.878 Zeilen in den ersten beiden Spalten zwei Team-Namen (z. B. Germany / Ireland) statt `PLAY #`/`ODK`; ab etwa Zeile 662 stehen dort wieder Zahlen. Bis einschließlich `TARGET ROUTE` passen die Spalten in beiden Zeilenarten zum Header, ab `RECEIVED BY` erkennbar nicht mehr (Namen und Zahlen gemischt).

Bitte um die wahre Spaltenreihenfolge für diese älteren Zeilen — oder die Bestätigung, dass sie aus einer älteren Charting-Ära stammen, die du nicht mehr verwendest.

Ohne Antwort: Alle Spalten ab `RECEIVED BY` bleiben für diese Zeilen leer, mit sichtbarem Hinweis im Report — geraten wird nicht, weil ein falscher Versatz Passgeber, Empfänger und Raumgewinn stillschweigend vertauschen würde.

## Frage 3 — Stimmen unsere Bedeutungen für sechs RESULT-Werte?

Diese sechs Werte sind bereits Teil des festen Vokabulars (Data Contract v1.2, 2026-09-03), also wird beim Einlesen nichts verworfen oder geraten — nur die Interpretation braucht noch dein Ja/Nein.

| RESULT-Wert | Unsere Bedeutung | passt / passt nicht |
|---|---|---|
| `Block` | Passversuch ohne Completion, abgeblockt | |
| `Blocked, Def TD` | geblockt mit anschließendem Defensiv-Touchdown | |
| `Batted Down` | Passversuch ohne Completion, abgefälscht | |
| `Dropped` | Passversuch ohne Completion, vom Empfänger fallengelassen | |
| `Timeout` | kein echter Play, kein Down verbraucht, wie Penalty behandelt | |
| `Offsetting Penalties` | kein echter Play, kein Down verbraucht, wie Penalty behandelt | |

Ein offener Punkt zusätzlich: Sollen `Timeout` und `Offsetting Penalties` aus EP-/EPA-Berechnungen ganz herausfallen (unsere Annahme: ja, sie sind keine Plays)?

## Frage 4 — Ist die Yards-only-Formel für "Explosive %" so gewollt?

Deine Workbook-Formel (`Player Analysis All Camps!R2:S2`) prüft ausschließlich `Yards > 12`
-- keine EPA-Bedingung irgendwo in der Formelkette. Deine Beschreibung im Sync war aber "12
Yards und/oder positive EPA". Auf unserem Korpus (14.991 Pass-Attempts) ergeben die beiden
Regeln sehr unterschiedliche Zahlen:

- Workbook-Formel (nur Yards): 15,8 % (2.365/14.991)
- Mündliche Regel (Yards oder EPA): 48,6 % (7.290/14.991)

Drei Möglichkeiten, die du einfach auswählen kannst:

- (a) Yards-only ist die eigentlich gewollte Regel, die mündliche Beschreibung war ungenau
- (b) die EPA-Klausel soll nachträglich in die Tabellenformel
- (c) es sind zwei unterschiedliche Kennzahlen, die du getrennt haben möchtest

Ohne Antwort: Wir führen beide Regeln getrennt und beide korrekt beschriftet weiter (wie in
`docs/explosiveness-vorschlag.md`), ohne uns für eine als "die richtige" zu entscheiden.

## Frage 5 — Was genau steht in der Spalte "Efficiency" (Data!O)?

Deine `Efficiency`-Formel (`SUMIF(Data!O)/(Attempts+Drops)`) haben wir wörtlich
nachgebaut, aber die Spalte `Data!O` selbst konnten wir nicht aus Down/Distance/Yards
herleiten: drei plausible Formeln (direkte Umrechnung, halbe College-Success-Rate-Regel,
reines "Yards > 0") erklären jeweils unter 80 % der von dir gecharteten Werte. Wir
vermuten, dass die Spalte eine manuelle Charting-Einschätzung ist (z. B. Wurfqualität oder
Drop-Zurechnung), keine reine Formel. Bitte um deine Faustregel dafür, was du dort einträgst.

Zusätzlich, konkret zur Denominator-Frage: dein `Efficiency`-Nenner ist
`Attempts + Drops`. In unserem Vokabular ist ein `Dropped`-Play aber bereits ein
unvollständiger Pass-Versuch (`Incs`) -- wir können aus den Daten allein nicht
unterscheiden, ob deine `Incs`-Zählung `Drops` schon enthält oder nicht. Der Code
unterstützt beide Lesarten über ein explizites Argument (`drops_flag`), ohne Umbau --
wir brauchen nur deine Antwort, welche Lesart stimmt.

Ohne Antwort: `hc_efficiency_table` bleibt im Code fertig vorbereitet, wird aber nicht auf
echten Zeilen gezeigt, solange weder die Spalte `Data!O` noch die Denominator-Frage geklärt
sind (die Spalte fehlt ohnehin noch im Korpus, siehe `docs/explosiveness-vorschlag.md`
§ Datengrundlage).

## Frage 6 — Sollen Läufe in "Explosive %" und "Efficiency" mitzählen?

Dein per-QB-Nenner (`Attempts = Comps+Incs+Sacks`) ist reiner Pass-Nenner -- Läufe fließen
in deine "Explosive %"-Zeile gar nicht ein. Auf unserem Korpus, mit derselben
Yards-Klausel wie deiner Workbook-Formel:

- Pass (dieselbe Zeile wie in `docs/explosiveness-vorschlag.md`): 15,8 % (2.365/14.991)
- Lauf: 12,9 % (139/1.076)

Sollen wir (a) eine gemeinsame Offense-Zahl aus Lauf und Pass bilden, (b) beide getrennt
ausweisen, oder (c) es beim reinen Pass-Nenner belassen, wie es heute ist?

Ohne Antwort: Wir zeigen beide Zahlen weiterhin getrennt (Lauf und Pass), so wie oben, und
bilden keine ungefragte Kombination.

## Was wir ohne Antwort liefern

Das strukturell saubere Workbook (`Offense Analytics 2026 Camps and Competitions.xlsx`) wird vollständig eingelesen; die beiden offenen Punkte (Frage 1, Frage 2) erscheinen namentlich im Ingest-Report.

## Antworten

### Frage 1 — Ist der `Data`-Tab in "Germany Analytics Stats EC 2025 vs WC Nations.xlsx" leer gemeint?
Ja, der Data Tab kommt aus der Datei zur Scoring Probability und ist für alle folgenden Dateien als Grundlage zu nehmen.

### Frage 2 — Welche Spaltenbedeutung haben die Zeilen mit Team-Namen in "Scoring Probability by Situation 2023-2026.xlsx"?
Ich habe irgendwann aufgehört die Teamnamen aufzuschreiben und nur noch O für Offense, D für Defense und S für no-play genommen. Dann wiederum irgendwann wieder angefangen zumindest in der ersten Zeile wieder die Teamnamen niederzuschreiben. Alles was darunter kommt, bis zu einer leeren Zeile bzw. einer neuen Zeile mit Teamnamen soll diese Teams darstellen.

### Frage 3 — Stimmen unsere Bedeutungen für sechs RESULT-Werte?
Ja.

### Frage 4 — Ist die Yards-only-Formel für "Explosive %" so gewollt?

### Frage 5 — Was genau steht in der Spalte "Efficiency" (Data!O)?

### Frage 6 — Sollen Läufe in "Explosive %" und "Efficiency" mitzählen?