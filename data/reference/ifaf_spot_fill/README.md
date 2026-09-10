# Manuelles Nachspotten fehlender Ballpositionen (IFAF Frauen)

Stand: 2026-09-07. Hintergrund und alle drei gescheiterten Rekonstruktionsversuche stehen in
`docs/ifaf-field-mapping.md` (Nachtrag 2026-09-07, insbesondere Teil 3 und Teil 10). Kurzfassung:
bei 12 der 29 `/plays`-primären Frauen-Spiele fehlt bei mindestens einem echten Spielzug die
Ballposition (`ballOn = null` im Reviewer-Feed) — bei manchen Spielen nur bei ein bis zwei
Zeilen, bei anderen bei fast allen. Alle drei automatisierten Rekonstruktionsversuche aus dem
Events-Log lagen weit unter der für diesen Datensatz verlangten 95-%-Trefferquote und wurden
deshalb bewusst nicht übernommen — keine Ballposition wird hier je erfunden. Stattdessen spottet
der Projektinhaber (Flag-Football-Expertin/-Experte) diese Lücken von Hand aus dem
Broadcast-Video nach, mit dem Video-Zeitstempel, den der Reviewer-Feed pro Spielzug ohnehin schon
mitliefert.

Diese Datei-Sammlung (`data/reference/ifaf_spot_fill/<game_id>.csv`, eine pro betroffenem Spiel)
ist die einzige Stelle, an der dieser manuelle Wert eingetragen wird. Sie enthält **keine
Spielernamen** und ist deshalb — anders als die Arbeits-Übersichten unter
`data/raw/ifaf/spot_fill_worksheets/` (lokal, nicht eingecheckt, mit Spielernamen für den
Kontext) — Teil des Repos.

## Die `ballOn`-Konvention

`ballOn` ist die Yard-Linie **von der eigenen Torlinie der Offense aus gezählt**, als Zahl
zwischen 0 und 50:

- eigene 5-Yard-Linie → `5`
- Mittellinie → `25`
- gegnerische 5-Yard-Linie → `45`

Das ist exakt dieselbe Konvention, die der Reviewer-Feed selbst für ein echt gespottetes
`ballOn` verwendet (`yardline_50` in `plays.parquet`) — ein manuell nachgetragener Wert wird
downstream identisch behandelt wie ein echter, inklusive der Yards-gained/Yards-to-go-Ableitung.

## Welche Zeilen einen Wert brauchen

Nur Zeilen für Spielzüge, deren `ballOn` im Reviewer-Feed tatsächlich fehlt (`spot_status =
missing` in der zugehörigen Arbeits-Übersicht). Ein Spielzug, der schon eine echte Position hat,
braucht hier nie einen Eintrag — und ein bereits vorhandener echter Wert wird beim Einlesen
(`ifaf.apply_spot_fill`) nie überschrieben, selbst wenn versehentlich eine Zeile dafür angelegt
wird (das erzeugt nur einen harmlosen Hinweis im Ingest-Log, keinen Fehler).

## Spaltenformat

```
game_id,sequence,ballOn,note
```

- `game_id`: die kanonische Spiel-ID (`ifaf-<uuid>` bzw. `ifaf-ffwc26-w<x><n>`, identisch zu
  `plays.parquet`s eigener `game_id`-Spalte und zum Dateinamen dieser CSV). Kann pro Zeile auch
  leer bleiben — eine leere `game_id` wird automatisch als "dieses Spiel selbst" behandelt, nur
  eine abweichende `game_id` erzeugt einen Hinweis.
- `sequence`: die rohe `sequence`-Nummer des `/plays`-Datensatzes (steht in der Arbeits-Übersicht
  in der gleichnamigen Spalte, meist die einzige Zahl, die zum Nachschlagen nötig ist).
- `ballOn`: der abgelesene Wert (0–50), leer solange noch nicht bearbeitet.
- `note`: freier Text, optional (z. B. "Spot unsicher, Kamera verdeckt").

## Zwei gleichwertige Wege, den Wert einzutragen

Es ist egal, ob die Yard-Linie direkt in eine Datei hier oder in die zugehörige
Arbeits-Übersicht (`data/raw/ifaf/spot_fill_worksheets/<game_id>.csv`) getippt wird — beide
Wege werden gelesen:

- **Direkt hier eintragen** (Workflow unten): der Wert steht sofort in der committeten Datei,
  ohne einen weiteren Schritt.
- **In der Arbeits-Übersicht eintragen, dann `ffep ifaf-spot-fill-worksheets --collect` laufen
  lassen**: kopiert jeden dort bereits eingetragenen `ballOn`/`note`-Wert (nur Zeilen mit
  `spot_status != real`) automatisch in die passende Fill-Datei — praktisch, wenn beim
  Video-Schauen ohnehin schon die Arbeits-Übersicht offen ist. Idempotent, überschreibt nie
  einen bereits eingetragenen, abweichenden Fill-Wert (nur ein Hinweis, kein Fehler).

## Dateinamen sind frei

Eine Fill-Datei wird **nicht** über ihren Dateinamen einem Spiel zugeordnet, sondern über die
`game_id`-Spalte in jeder Zeile. Eine Datei darf also umbenannt werden (z. B. beim Bearbeiten
versehentlich mit einem Präfix versehen) — sie wird beim nächsten Ingest-Lauf trotzdem gefunden.
Ein Spiel darf seine Zeilen auch auf mehrere Dateien verteilt haben (z. B. eine Datei pro
Bearbeitungs-Sitzung); alle `*.csv`-Dateien in diesem Verzeichnis werden gelesen und nach
`game_id` zusammengeführt. Tragen zwei Dateien für dieselbe Sequenz unterschiedliche `ballOn`-Werte
ein, gewinnt die zuerst (alphabetisch nach Dateiname) gelesene Datei — der Widerspruch erscheint
als benannter Hinweis in der Ingest-Zusammenfassung, nie stillschweigend.

## Workflow

1. Arbeits-Übersicht für das Spiel öffnen (`data/raw/ifaf/spot_fill_worksheets/<game_id>.csv`,
   lokal erzeugt via `ffep ifaf-spot-fill-worksheets`) — sie zeigt pro fehlender Zeile
   `video_url`, `video_time_s`/`video_time_mmss` (wo im Video der Spielzug beginnt), Down,
   Offense-Team, Passer/Receiver und die letzte bekannte echte Position (`prev_ballOn`) zur
   Orientierung.
2. Video an der angegebenen Zeit öffnen, die Line of Scrimmage ablesen.
3. Die Yard-Linie (0–50, siehe Konvention oben) entweder direkt in eine Datei in diesem
   Verzeichnis eintragen — eine Zeile mit `game_id,sequence,ballOn,note` je Spielzug (die ersten
   drei Spalten lassen sich direkt aus der Arbeits-Übersicht übernehmen, effektiv wird pro
   Spielzug nur die eine `ballOn`-Zahl neu getippt) — oder in die `ballOn`/`note`-Spalte der
   Arbeits-Übersicht selbst eintragen und danach `ffep ifaf-spot-fill-worksheets --collect`
   laufen lassen.
4. Beim nächsten Ingest-Lauf (`ffep ingest` bzw. `pipeline.run_ingest`) liest
   `ifaf.apply_spot_fill` jede Fill-Datei automatisch mit ein, sofern in `ffep.toml`
   `[reference] ifaf_spot_fill` auf dieses Verzeichnis zeigt (Standard). Jede angewandte Zeile
   bekommt `spot_source = "manual"` in `plays.parquet` — so bleibt jederzeit erkennbar, welche
   Position echt vom Reviewer stammt und welche von Hand nachgetragen wurde.

Ungültige Zeilen (unbekannte `sequence`, `ballOn` außerhalb 0–50, oder eine Zeile für einen
Spielzug, der längst eine echte Position hat) brechen den Ingest nie ab — sie erzeugen nur einen
benannten Hinweis in der Ingest-Zusammenfassung, die betroffene Zeile wird ignoriert.

## Encoding/Trennzeichen-Toleranz (2026-09-08)

Eine in Excel bearbeitete und gespeicherte Datei kommt regelmäßig **Semikolon-getrennt**,
**nicht als UTF-8** (z. B. `mac_roman` bei einem Export von Excel für Mac) und mit einer
**leeren Spalte am Zeilenende** zurück (ein trailing Trennzeichen vor dem Zeilenumbruch, z. B.
`game_id;sequence;ballOn;note;`) — genau das Format, in dem der Projektinhaber die Datei für
das ESP-MEX-Viertelfinale der Frauen (`ifaf-019ffff1-a8db-...`) gespeichert hatte, inklusive
eines Umlauts in der Notiz zu Sequenz 710 ("überflüssiges play").

`ifaf.load_spot_fill` erkennt das automatisch: das Trennzeichen wird an der Kopfzeile
gesniffet (`;` nur wenn dort kein `,` vorkommt), die Bytes werden zuerst als UTF-8 versucht,
dann als `cp1252`, und — falls das auf ein für `cp1252` typisches Mojibake-Zeichen wie `Ÿ`
trifft (ein Hinweis, dass die Datei eigentlich `mac_roman` ist, nicht `cp1252`) — als
`mac_roman` neu decodiert. Jeder Fallback erzeugt einen benannten Hinweis in der
Ingest-Zusammenfassung, nie einen Fehler. Die im Repo committete Datei selbst ist trotzdem
immer die normalisierte Form (Komma, LF, UTF-8, exakt vier Spalten) — dieser Toleranz-Layer
ist ein Sicherheitsnetz für die *nächste* Excel-Bearbeitung, kein Ersatz für die Normalisierung
vor dem Commit.

## BOM (2026-09-10)

Diese Toleranz gilt jetzt auch für eine führende UTF-8-BOM (`EF BB BF`): Excel für macOS
schreibt beim erneuten Speichern einer als UTF-8 erkannten Datei automatisch eine BOM, und
ohne diese BOM rät Excel die Kodierung oft falsch — Umlaute erscheinen dann als Mojibake (z. B.
"Nühse" wird zu "NÃ¼hse" angezeigt). Die BOM wird beim Einlesen automatisch entfernt, bevor die
erste Spalte (`game_id`) gelesen wird — sonst würde die Kopfzeile fälschlich `"﻿game_id"`
heißen und jede `game_id`-Zelle stillschweigend leer bleiben. Dateien, die dieses Projekt dem
Projektinhaber direkt zum Öffnen in Excel gibt (z. B. die Arbeits-Übersichten unter
`data/raw/ifaf/spot_fill_worksheets/`), werden deshalb jetzt selbst mit BOM geschrieben
(`utf-8-sig`) — Dateien können direkt in Excel geöffnet und gespeichert werden, ohne dass
Umlaute verstümmelt werden. Die hier committeten Fill-Dateien selbst bleiben bewusst BOM-frei
(reine Pipeline-Eingabe, nicht zum direkten Öffnen in Excel gedacht).
