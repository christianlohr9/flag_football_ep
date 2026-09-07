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

## Workflow

1. Arbeits-Übersicht für das Spiel öffnen (`data/raw/ifaf/spot_fill_worksheets/<game_id>.csv`,
   lokal erzeugt via `ffep ifaf spot-fill-worksheets`) — sie zeigt pro fehlender Zeile
   `video_url`, `video_time_s`/`video_time_mmss` (wo im Video der Spielzug beginnt), Down,
   Offense-Team, Passer/Receiver und die letzte bekannte echte Position (`prev_ballOn`) zur
   Orientierung.
2. Video an der angegebenen Zeit öffnen, die Line of Scrimmage ablesen.
3. Die Yard-Linie (0–50, siehe Konvention oben) in diese Datei eintragen — eine Zeile mit
   `game_id,sequence,ballOn,note` je Spielzug (die ersten drei Spalten lassen sich direkt aus der
   Arbeits-Übersicht übernehmen, effektiv wird pro Spielzug nur die eine `ballOn`-Zahl neu
   getippt).
4. Beim nächsten Ingest-Lauf (`ffep ingest` bzw. `pipeline.run_ingest`) liest
   `ifaf.apply_spot_fill` diese Datei automatisch mit ein, sofern in `ffep.toml`
   `[reference] ifaf_spot_fill` auf dieses Verzeichnis zeigt (Standard). Jede angewandte Zeile
   bekommt `spot_source = "manual"` in `plays.parquet` — so bleibt jederzeit erkennbar, welche
   Position echt vom Reviewer stammt und welche von Hand nachgetragen wurde.

Ungültige Zeilen (unbekannte `sequence`, `ballOn` außerhalb 0–50, oder eine Zeile für einen
Spielzug, der längst eine echte Position hat) brechen den Ingest nie ab — sie erzeugen nur einen
benannten Hinweis in der Ingest-Zusammenfassung, die betroffene Zeile wird ignoriert.
