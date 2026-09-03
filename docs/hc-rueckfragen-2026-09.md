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

## Was wir ohne Antwort liefern

Das strukturell saubere Workbook (`Offense Analytics 2026 Camps and Competitions.xlsx`) wird vollständig eingelesen; die beiden offenen Punkte (Frage 1, Frage 2) erscheinen namentlich im Ingest-Report.

## Antworten

### Frage 1 — Ist der `Data`-Tab in "Germany Analytics Stats EC 2025 vs WC Nations.xlsx" leer gemeint?
Ja, der Data Tab kommt aus der Datei zur Scoring Probability und ist für alle folgenden Dateien als Grundlage zu nehmen.

### Frage 2 — Welche Spaltenbedeutung haben die Zeilen mit Team-Namen in "Scoring Probability by Situation 2023-2026.xlsx"?
Ich habe irgendwann aufgehört die Teamnamen aufzuschreiben und nur noch O für Offense, D für Defense und S für no-play genommen. Dann wiederum irgendwann wieder angefangen zumindest in der ersten Zeile wieder die Teamnamen niederzuschreiben. Alles was darunter kommt, bis zu einer leeren Zeile bzw. einer neuen Zeile mit Teamnamen soll diese Teams darstellen.

### Frage 3 — Stimmen unsere Bedeutungen für sechs RESULT-Werte?
Ja.