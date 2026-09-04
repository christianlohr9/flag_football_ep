# Explosiveness-Kalibrierung: HC-Zeilen waren beim M3-3-Lauf nicht im gescorten Korpus (2026-09-04)

Befund: `data/processed/plays_scored.parquet` enthielt bis 2026-09-04 vormittags 21.437 Zeilen
und **0 HC-Zeilen** (nie nach dem HC-Ingest neu gescort). Die M3-3-Kalibrierung
(`data/reference/explosiveness/*`, n = 14.739 nach der M3-04-01-Korrektur) und die
Vergleichstabellen in `docs/explosiveness-vorschlag.md` beruhen daher ausschließlich auf dem
Nicht-HC-Bestand; der `baseline_hc_workbook`-Vergleich (seine >12-yd-Regel auf seinen Zeilen)
lief auf 0 Zeilen.

Seit dem Re-Scoring (Champion unverändert, `ffep ingest` + `ffep score` nach M3-02-05 und
M3-04-06) liegen 28.255 Zeilen vor, davon 6.818 HC (5.840 Scrimmage-Plays, 61 Drop-Markierungen).

Offen (Entscheidung Nutzer, weil die Zahlen unter Review stehen):
- Kalibrierung mit dem vergrößerten Korpus neu rechnen (`corpus_fingerprint` ändert sich, der
  p80-Schwellwert vermutlich leicht) und die Vorschlags-/Recherche-Dokumente regenerieren —
  sinnvollerweise **vor** der Freigabe "vorschlag ok" oder als expliziter Nachtrag danach.
- Nicht still nachziehen: `docs/explosiveness-vorschlag.md` wird gerade vom Nutzer gelesen.

---

**Erledigt (2026-09-04, M3-03-03):** Neu kalibriert als Nachtrag nach Freigabe von Kandidat B
(EPA-Magnitude, Perzentil-Schwelle) -- Korpus 16.067 -> 21.907 Plays, Schwellenwert 2,69 -> 2,66
EPA, `data/reference/explosiveness/*` regeneriert, Nachtrag in `docs/explosiveness-vorschlag.md`
und `docs/explosiveness-recherche.md` dokumentiert, Doc-Guards grün.
