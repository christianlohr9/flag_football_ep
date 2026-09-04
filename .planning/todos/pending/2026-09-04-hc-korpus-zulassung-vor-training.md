# HC-Korpus: Zulassungsregeln vor dem ersten Training (Eingang M3-02-05, 2026-09-04)

Befund (Nutzer-Sichtung Zeile ~3450 im SP-`Data`-Tab + Ingest-Report M3-02-04): Neue Spiele beginnen
mit `PLAY # = 1` (Segmentierung greift, keine Leerzeile/Teamzeile nötig), aber die ersten Zeilen
eines Spiels sind oft **Platzhalter** (PLAY # 1–2 ohne ODK/DN/DIST/RESULT). Diese Zeilen lassen den
`downs_range`-Check fehlschlagen und **quarantänieren das ganze Spiel** — so gehen 164 von 174
SP-Spielen verloren (nur 10 erreichen `plays.parquet`). Dazu `gapless_play_ids`-Fails (1.279) durch
reale PLAY-#-Lücken.

Regeln für M3-02-05 (erste Task, vor dem Training; kein Aufweichen der Checks, sondern Klassifikation):
1. **Platzhalter-Zeilen** (ODK null UND DN null UND RESULT null) werden vor der Validierung als
   Nicht-Plays entfernt (gezählt, im Report ausgewiesen); Zeilen mit gültigem RESULT aber null DN
   bleiben und werden wie bisher geprüft.
2. **HC-Play-IDs:** `play_id` für `hc_workbook:`-Quellen aus der Zeilenreihenfolge innerhalb des
   Spiels neu vergeben (Original-`PLAY #` als Extra `hc_play_no` behalten), damit `gapless_play_ids`
   die reale Reihenfolge prüft statt seine Nummerierung.
3. **Perspektive statt Teamnamen:** Für EP ist `posteam` aus `ODK` (O/D) ausreichend; Teamnamen sind
   nur für Dedupe/Splits relevant. Spiele ohne Teamnamen bleiben trainierbar (provisorische
   `game_id`), aber aus dem Dedupe-Fingerprint-Vergleich nicht ausgenommen.
4. Trainierbare Spiele vorher/nachher ausweisen (Erwartung: von 35 auf deutlich über 100).
5. `tok_unknown`-Stand bleibt: nur `-5.0` (2 Vorkommen, Datenrutscher) — keine neuen Tokens nötig
   (`No Good` ist im Vokabular).
