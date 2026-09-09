# Manuelle Korrekturen fehlerhafter Reviewer-Feed-Felder (IFAF)

Stand: 2026-09-08. Ergänzt `data/reference/ifaf_spot_fill/` (siehe dessen README) um einen
zweiten, verwandten aber unterschiedlichen Mechanismus: ein Spot-Fill trägt nur eine
**fehlende** `ballOn`-Position nach; eine Korrektur hier überschreibt ein Feld, das der
Reviewer-Feed schlicht **falsch** erfasst hat. Beide Fälle passieren beim selben
Video-Review durch den Projektinhaber (Flag-Football-Expertin/-Experte) und tauchen im selben
Datensatz auf — deshalb ein eigenes, kleines, wiederverwendbares Verzeichnis statt eines
Einmal-Patches im Code.

Bekanntes Beispiel: das ESP-MEX-Viertelfinale der Frauen (`ifaf-019ffff1-a8db-...`), Sequenzen
720/730 — der Reviewer-Feed trägt dort `offenseTeamId = w-esp` ein, obwohl sowohl der
Down-Verlauf der `/plays`-Datensätze selbst (1st@5, 2nd@12) als auch der Events-Feed
(`POSSESSION_CHANGE` auf `w-mex` unmittelbar davor) übereinstimmend zeigen, dass Mexiko den
Ball bereits hat. Sequenz 740 trägt bereits korrekt `w-mex`.

Diese Datei-Sammlung (`data/reference/ifaf_corrections/<anything>.csv`) enthält **keine
Spielernamen** und ist deshalb Teil des Repos.

## Spaltenformat

```
game_id,sequence,field,value,note
```

- `game_id`: die kanonische Spiel-ID (identisch zur Konvention in
  `data/reference/ifaf_spot_fill/`). Kann pro Zeile leer bleiben — wird dann als "dieses Spiel
  selbst" behandelt.
- `sequence`: die rohe `sequence`-Nummer des betroffenen `/plays`-Datensatzes.
- `field`: eines von genau fünf erlaubten Werten (siehe unten).
- `value`: der korrigierte Wert, als Text.
- `note`: freier Text, optional.

## Erlaubte `field`-Werte

| `field`         | Wertebereich                              | Wirkung                                                                 |
| --------------- | ------------------------------------------ | ------------------------------------------------------------------------ |
| `offense_team`  | eine der beiden Team-IDs dieses Spiels (z. B. `w-mex`) | überschreibt `posteam`, leitet `defteam` neu ab; löst einen kompletten `drive_id`-Neuaufbau für dieses Spiel aus |
| `down`          | Ganzzahl 0–4                                | überschreibt `down`                                                     |
| `half`          | `1` oder `2`                                | überschreibt `half`                                                     |
| `nullified`     | `0` oder `1`                                | überschreibt `nullified`; bei `1` (und kein Extra-Point-Play) wird `play_type` auf `"no_play"` gesetzt und jedes Scoring-/Turnover-Flag auf 0 |
| `drop_record`   | `0` oder `1`                                | `1` entfernt den Datensatz vollständig, `play_id` wird danach lückenlos neu durchnummeriert |

**`insert_after` wird nicht unterstützt.** Ein fehlender, vom Reviewer-Feed nie erfasster
Spielzug (z. B. die im selben Viertelfinale fehlende Strafe bei Sequenz 610, die alle
folgenden Video-Zeiten verschiebt) wird hier **nie erfunden** — dieselbe Regel, die
`data/reference/ifaf_spot_fill/README.md` schon für `ballOn` festlegt. Eine Zeile mit
`field=insert_after` wird abgelehnt (Hinweis im Ingest-Log, kein Fehler); solche Lücken gehören
stattdessen als bekannte, offen dokumentierte Einschränkung in
`docs/ifaf-wm2026-daten.md`s Nachtrag.

Eine leere `value`-Zelle wird stillschweigend übersprungen (noch nicht bearbeitete Zeile,
keine Fehlermeldung) — dieselbe Konvention wie ein leeres `ballOn` in
`data/reference/ifaf_spot_fill/`.

## Reihenfolge im Ingest

`ifaf.apply_corrections` läuft unmittelbar nach `flatten_plays_records`, **vor**
`apply_spot_fill`, `apply_events_ledger` und jeder Yardage-Ableitung — ein `drop_record`
entfernt einen Datensatz, bevor irgendetwas Nachgelagertes ihn referenzieren kann, und eine
`offense_team`-Korrektur muss für `apply_events_ledger`s eigenen Team-Abgleich bereits sichtbar
sein.

## Dateinamen sind frei

Wie bei `data/reference/ifaf_spot_fill/` entscheidet allein die `game_id`-Spalte, nicht der
Dateiname — alle `*.csv`-Dateien in diesem Verzeichnis werden gelesen und nach `game_id`
zusammengeführt. Tragen zwei Dateien für dieselbe `(sequence, field)`-Kombination
unterschiedliche Werte ein, gewinnt die zuerst (alphabetisch nach Dateiname) gelesene Datei —
der Widerspruch erscheint als benannter Hinweis in der Ingest-Zusammenfassung.

## Herkunfts-Spalte

Jede erfolgreich angewandte Korrektur setzt `correction_source = "manual"` in `plays.parquet`
auf genau der betroffenen Zeile (nie auf einer entfernten `drop_record`-Zeile, die es danach
nicht mehr gibt) — analog zu `spot_source` für Spot-Fills.

Ungültige Zeilen (unbekanntes `field`, unbekannte `sequence`, Wert außerhalb des erlaubten
Bereichs) brechen den Ingest nie ab — sie erzeugen nur einen benannten Hinweis in der
Ingest-Zusammenfassung, die betroffene Zeile wird ignoriert.

## Encoding/Trennzeichen-Toleranz

`load_corrections` (wie `load_spot_fill`) toleriert eine aus Excel gespeicherte,
Semikolon-getrennte, nicht-UTF-8-Datei (z. B. `mac_roman` mit einer trailing leeren Spalte) und
erzeugt dabei nur einen Hinweis, keinen Fehler — siehe
`data/reference/ifaf_spot_fill/README.md`s eigenes Encoding-Beispiel für den Hintergrund.
