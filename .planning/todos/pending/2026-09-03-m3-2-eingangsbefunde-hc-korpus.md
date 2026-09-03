# Eingangsbefunde für M3-2 (EPA-Refinement) aus dem echten HC-Ingest-Lauf (M3-01-04, 2026-09-03)

Quelle: `.planning/phases/M3-01-hc-workbook-ingest/M3-01-04-SUMMARY.md`, `docs/hc-workbook-ingest.md`.
Beide Punkte MÜSSEN vor dem ersten M3-2-Training geklärt sein — sonst trainiert M3-2 auf null HC-Zeilen.

## 1. 100 % der HC-Spiele in Quarantäne wegen `half_assigned`

Die HC-Workbooks tragen keine Halbzeit-Information; unsere Ableitung von `half` läuft über
`data/reference/half_boundaries.csv` je Spiel, die für HC-Spiele fehlt. Ergebnis: 0 HC-Zeilen
erreichen `plays.parquet`. Optionen für M3-2 (Entscheid Nutzer/Planner):
- (a) `half` für HC-Zeilen als unbekannt zulassen und im EP-Modell imputieren/als eigene Kategorie
  führen (das `half`-Feature wurde in 1.3 nur für EP adoptiert),
- (b) Halbzeitgrenzen aus der Spielzugfolge heuristisch schätzen (Kickoff-/Drive-Muster, PLAY #-Sprünge)
  und als provisorisch markieren,
- (c) den HC nach Halbzeitmarkern fragen (Frage 4 für `docs/hc-rueckfragen-2026-09.md`).
Empfehlung: (a) für den Oktober-Sync, (c) parallel anstoßen.

## 2. 2.128 "Spiele" aus 19.901 Zeilen — Über-Segmentierung prüfen

~9 Zeilen pro "Spiel" sind implausibel (eine Saison hat ~10–20 Spiele, Camps einige Dutzend
Scrimmage-Abschnitte). Die Spiel-Segmentierung (`segment_games`/`resolve_game_identity`) trennt
vermutlich bei jedem PLAY #-Neustart (Drive/Serie) statt bei Spielwechseln. Vor M3-2:
Segmentierungsregel gegen die Teamnamen-/Datumsspalten und Sheet-Struktur prüfen und die
Spielzahl auf einen plausiblen Wert bringen; `hc_games.csv` danach neu füllen. Die 9 bestätigten
Duplikate gegen `legacy-39..47` (703 Zeilen) bleiben gültig, solange die Fingerprints stimmen.

## 3. Unverändert offen
Frage 1 (leerer EC-2025-`Data`-Tab) und Frage 2 (Spaltenlayout der Teamnamen-Zeilen) an den HC.
