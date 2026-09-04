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

## Zusatzfragen (M3-2, EPA-Update)

Diese zwei Zusatzfragen kamen aus dem EPA-Update für den Oktober-Sync
(`docs/epa-refinement-2026-10.md`) — beide lassen sich nicht aus den Daten selbst beantworten.

### Zusatzfrage A — Gibt es Halbzeit-Marker für deine Workbook-Spiele?

Deine Workbooks tragen keine Halbzeit-Information. Für die Spiele, die wir trainierbar
verwenden können, setzen wir deshalb einen dokumentierten Ersatzwert (konstant `half = 2`
fürs ganze Spiel). Der Preis: eine torlose Drive vor der (nicht existierenden) Halbzeitgrenze
erbt im Label den nächsten tatsächlichen Score des Spiels, statt an der echten Halbzeit als
"kein Score" zu enden. Kannst du markieren, welche Zeile in einem Spiel der erste Play nach der
Halbzeit ist — eine Zahl pro Spiel reicht — oder uns bestätigen, dass Camp- und
Scrimmage-Segmente gar keine formale Zwei-Halbzeiten-Struktur haben? Ehrlich dazugesagt, was wir
NICHT gemacht haben: wir haben getestet, die Halbzeit aus der reinen Play-Anzahl zu schätzen —
das landete bei den einzigen zwei Spielen mit einem echten Marker innerhalb von vier
Prozentpunkten der Spielmitte, aber `n = 2` reicht nicht, um das zu übernehmen.

Ohne Antwort: der Ersatzwert bleibt, dokumentiert in `docs/data-contract.md`, und die
Einschränkung steht im Oktober-Dokument.

### Zusatzfrage B — Wie sollen wir deine Spiele einordnen: Camp, Scrimmage oder Länderspiel?

Das Modell nutzt das Wettbewerbs-Niveau als Eingabe, und wir klassifizieren aktuell alle deine
Spiele als `mixed-other` — dieselbe Kategorie wie die älteren Legacy-Spiele, eine Annahme, keine
Tatsache. Kannst du uns eine kurze Liste je Workbook geben (`Offense Analytics 2026 Camps and
Competitions`, `Scoring Probability by Situation 2023-2026`): Länderspiel, Turnier,
Camp/Scrimmage, gemischt?

Ohne Antwort: `mixed-other` bleibt der dokumentierte Default, und die Annahme wird im
Oktober-Dokument als solche markiert.

## Was wir ohne Antwort liefern

Das strukturell saubere Workbook (`Offense Analytics 2026 Camps and Competitions.xlsx`) wird vollständig eingelesen; die beiden offenen Punkte (Frage 1, Frage 2) erscheinen namentlich im Ingest-Report.

## Antworten

_Beantwortet 2026-09-03 per E-Mail durch den HC (Jona Winkel) — Fragen 1-3._

### Frage 1 — Ist der `Data`-Tab in "Germany Analytics Stats EC 2025 vs WC Nations.xlsx" leer gemeint?
Ja, der Data Tab kommt aus der Datei zur Scoring Probability und ist für alle folgenden Dateien als Grundlage zu nehmen.

### Frage 2 — Welche Spaltenbedeutung haben die Zeilen mit Team-Namen in "Scoring Probability by Situation 2023-2026.xlsx"?
Ich habe irgendwann aufgehört die Teamnamen aufzuschreiben und nur noch O für Offense, D für Defense und S für no-play genommen. Dann wiederum irgendwann wieder angefangen zumindest in der ersten Zeile wieder die Teamnamen niederzuschreiben. Alles was darunter kommt, bis zu einer leeren Zeile bzw. einer neuen Zeile mit Teamnamen soll diese Teams darstellen.

### Frage 3 — Stimmen unsere Bedeutungen für sechs RESULT-Werte?
Ja.

### Frage 4 — Ist die Yards-only-Formel für "Explosive %" so gewollt?

### Frage 5 — Was genau steht in der Spalte "Efficiency" (Data!O)?

### Frage 6 — Sollen Läufe in "Explosive %" und "Efficiency" mitzählen?

### Zusatzfrage A — Gibt es Halbzeit-Marker für deine Workbook-Spiele?

### Zusatzfrage B — Wie sollen wir deine Spiele einordnen: Camp, Scrimmage oder Länderspiel?

## Zusatzfragen (M3-4, Report)

_Ergänzt 2026-09-04 beim Bau des automatisierten Player-Analysis-Reports. Die Nummerierung
setzt die Fragen 1-6 oben fort; die Struktur bleibt bewusst getrennt, damit die bestehenden
Abschnitte unverändert bleiben._

#### Frage 7 — Camp IV oder Camp VI?

Deine Zeilen 3001-4000 im `Data`-Tab tauchen unter zwei verschiedenen Tab-Namen auf: `Set
Analysis Camp IV` und `Player Analysis Camp VI` — beide über exakt denselben Zeilenbereich,
beide Namen direkt aus deinen eigenen Formelzellen gelesen (kein Übertragungsfehler unserer
Seite). Welcher Name ist der richtige, und soll künftig jeder Set-Analysis-Camp (I, III, IV, V)
auch einen eigenen Player-Analysis-Tab bekommen, oder bleibt das Paar nur für diesen einen Camp
bestehen?

Ohne Antwort: Der Report zeigt diesen Abschnitt als "Camp IV/VI (Zeilen 3001-4000, Name
unklar)" mit einem sichtbaren Konflikt-Hinweis, statt sich für einen Namen zu entscheiden.

#### Frage 8 — Was bedeutet die Spalte `Data!Y` (Kopf "B")?

Deine Air-Yards-Formel (`Player Analysis All Camps!K2`) zieht einen Term ab:
`SUMIF(Data!L, <QB-Name>, Data!Y)` — summiert über alle Zeilen, in denen `RECEIVED BY` (Spalte
L) der Name des Quarterbacks selbst ist, also Zeilen, in denen der QB als eigener Passempfänger
auftaucht. Die Spalte `Data!Y` selbst trägt als Kopfzeile nur den einzelnen Buchstaben `"B"`,
ohne erkennbare Bedeutung anderswo im Workbook (vier weitere einbuchstabige Spalten -- `X`, `S`,
`C`, `Q` -- haben dasselbe Problem, aber nur `Y` fließt in eine Formel ein).

Ohne Antwort: Unsere Air-Yards-Zahl lässt diesen Subtraktionsterm weg und liegt dadurch
tendenziell etwas höher als deine -- sichtbar als Fußnote im Report, nicht stillschweigend.

#### Frage 9 — Wie wird die Drop-Spalte (`Data!W`) gefüllt?

Deine `Adj Comp %`-, `adj Pass Yards`- und `adj YPA`-Formeln hängen alle an derselben Spalte
(`Data!W`, Kopf "Drop"). Zwei Teilfragen, die wir aus den Daten allein nicht beantworten
können:

Erstens, wie trägst du einen Drop ein -- als Text (ein `x`, ein Wort) oder als Zahl? Dein
eigenes Formel-Kriterium (`COUNTIFS(..., "*")`) ist ein Excel-Wildcard und zählt ausschließlich
Text -- eine numerische Markierung (z. B. `1`) würde von deiner eigenen Tabelle stillschweigend
NICHT gezählt. Wir speichern die Spalte deshalb als Text (nie als Zahl), aber wenn du Drops
zahlenmäßig markierst, zählt deine eigene Formel sie schon heute nicht mit -- unabhängig von
uns.

Zweitens, zählt ein Drop bei dir zusätzlich als Incompletion (`Incs`)? Das ist dieselbe
Unklarheit wie schon bei Frage 5 (`Attempts + Drops` als Efficiency-Nenner) -- wir können aus
den Zeilen allein nicht sehen, ob deine `Incs`-Zählung Drops schon enthält oder nicht.

Ohne Antwort: Unser Report markiert einen Play als "dropped", sobald `Data!W` einen
nicht-leeren Wert trägt (Text oder Zahl) -- bewusst großzügiger als dein eigenes
Text-only-Wildcard-Kriterium, und wir zählen einen Drop zusätzlich als Incompletion, bis du uns
sagst, dass das falsch ist.