# HC-Korpus: 22 Spiele des Pair-Blocks noch quarantäniert (Eingang M3-02-05, 2026-09-04)

Befund aus dem Ingest-Lauf nach den Zulassungsregeln (trainierbare HC-Spiele 35 → 92): Der
`Scoring Probability … Data`-Pair-Block (22 Spiele) bleibt auf `half_assigned` quarantäniert.
Ursache: diese Blöcke haben keine O/D/S-Markerzeilen; `posteam` müsste aus der Reihenfolge der
Teamnamen-Spalte abgeleitet werden, nicht aus `ODK`. `DECLARED_BLOCK_KINDS` schließt `pair`
weiterhin aus (bewusst, M3-02-04).

Offen:
- Regel für Pair-Blöcke: posteam aus Teamnamen-Reihenfolge (erstes Team = Offense?) — vorher
  an einem Spiel gegen die SP-Tabellen-Snapshots (M3-02-03) verifizieren, nicht raten.
- Erwarteter Gewinn: +22 Spiele (Größenordnung +1.500 Zeilen); erst nach M3-02-08-Review
  entscheiden, ob das für den Oktober-Sync noch nötig ist.
- Zweiter Befund derselben Messung: No-Play-Anteil (Timeout/Offsetting/Penalty) `legacy` 3,92 %,
  HC-SP-Data 4,74 % — beide über der 2 %-Schwelle → Eskalation im M3-02-08-Checkpoint.
