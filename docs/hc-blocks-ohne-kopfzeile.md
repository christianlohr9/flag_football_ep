# Pair-Block-Gruppen ohne Team-Namenspaar-Kopfzeile

Diese Tabelle listet jede Gruppe von O/D/S-Marker-Zeilen (Offense/Defense/kein Play, Spalte A) in
einem der beiden Scoring-Probability-Tabs, für die **keine** vorausgehende Team-Namenszeile im
selben Block gefunden wurde — also Zeilen, denen `flag_football_ep.ingest.hc_workbook` (Frage 2,
Antwort 2026-09-03) kein Team-Paar zuordnen kann. Für jede Zeile: Sheet, Zeilenbereich, Play-Zahl
und alles an Datums-/Gegner-Hinweisen, das in den benachbarten Zellen erkennbar ist — keine
Spielernamen. Die letzte Spalte ist leer: dort trägt der Cheftrainer das Spiel ein, dem die Gruppe
gehört (Datum, Gegner oder eine sonstige eindeutige Kennung reicht).

**Ergebnis dieses Laufs (2026-09-03, `Scoring Probability by Situation 2023-2026.xlsx`, Tabs
`Data` und `Copy of Data`): 0 Gruppen.** Beide Tabs enthalten aktuell keine einzige Zeile mit einem
reinen `O`/`D`/`S`-Marker in Spalte A ohne eine Team-Namenszeile davor im selben Block — jede
Zeile trägt entweder eine (teils abgekürzte) Team-Namenspaar-Kopfzeile oder gehört zu einem bereits
korrekt zugeordneten Marker-Block. Diese Tabelle ist deshalb aktuell leer; sie bleibt hier stehen,
damit ein künftiger Workbook-Import (falls der Cheftrainer wieder auf die Kopfzeilen-/Marker-
Charting-Konvention zurückgreift) sofort etwas zum Ausfüllen hat, statt den ganzen Tab durchsuchen
zu müssen.

| Workbook | Sheet | Block-Key | Zeilen | Plays | Datums-/Gegner-Hinweis | Spiel (vom Cheftrainer auszufüllen) |
|---|---|---|---|---|---|---|
| _(keine Gruppe gefunden)_ | | | | | | |

## Wie diese Tabelle erneut erzeugt wird

```
uv run python -c "
from pathlib import Path
from flag_football_ep.ingest import hc_workbook

p = Path('data/raw/hc_files/Scoring Probability by Situation 2023-2026.xlsx')
for sheet in ['Data', 'Copy of Data']:
    header, rows, _ = hc_workbook.read_sheet_rows(p, sheet)
    blocks, _ = hc_workbook.segment_blocks(header, rows)
    for block in blocks:
        if block.kind != 'pair':
            continue
        slices, _ = hc_workbook.segment_games(block)
        for s in slices:
            if s.source_team1 is None:
                print(sheet, block.index, s.block_key, s.first_row, s.last_row, len(s.rows))
"
```

Eine Zeile in der Ausgabe entspricht einer Zeile in der Tabelle oben. `data/raw/hc_files/` ist
gitignored — dieser Befehl muss lokal, mit den echten Workbooks, ausgeführt werden; kein Test
öffnet diese Datei.
