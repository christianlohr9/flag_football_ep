# Datenvertrag — Hudl-Export Flag Football Nationalteam

Version 1.0-draft. Maschinenlesbares Gegenstück: `data-contract.schema.json` (wird vom Phase-1.2-Ingest-Validator direkt konsumiert — Spec und Validierung können nicht auseinanderlaufen). Offene Punkte tragen die Marker `PENDING-ANALYST` (Klärung im Analysten-Gespräch) bzw. `PENDING-SIGHTING` (Klärung nach Sichtung echter Export-Werte).

## Zweck & Geltungsbereich — forward-only

Dieser Vertrag definiert das Format, in dem ab sofort jedes Spiel als Hudl-Export in die Pipeline kommt. Er gilt **forward-only**: Die 47 Bestandsspiele (3.701 Plays) in `data_raw.csv` sind grandfathered — kein Re-Export, kein Re-Charting. Konsequenz für Phase 1.2: zwei Ingest-Pfade (Legacy-Format aus `data_raw.csv` + neues Vertragsformat).

Das Export-Preset gehört und pflegt der Nutzer selbst (eigener Hudl-Zugang, exportiert alles selbst). Das Gespräch mit dem Videoanalysten behandelt ausschließlich das **Charting** (was pro Play getaggt wird und mit welchem Vokabular) — niemals Export-Mechanik.

## Export-Baseline — 19 Spalten, ein File pro Spiel

Das reale Export-Preset liefert genau diese 19 Spalten:

`PLAY #`, `ODK`, `DN`, `DIST`, `HASH`, `YARD LN`, `PLAY TYPE`, `RESULT`, `GN/LS`, `OFF FORM`, `OFF PLAY`, `OFF STR`, `PLAY DIR`, `GAP`, `PASS ZONE`, `DEF FRONT`, `COVERAGE`, `BLITZ`, `QTR`

Ein Export-File pro Spiel. CSV-Dialekt der Exporte: Semikolon-Delimiter (`;`) mit `utf-8-sig`-Encoding (UTF-8 mit BOM) — der deutsche Excel-Dialekt, verifiziert an `data_raw.csv`. **Achtung:** Excels CSV-Ausgabe hängt von den OS-Locale-Einstellungen ab; ändern sich die, parst der Ingest Müll (einspaltige Ergebnisse, kaputte Umlaute). Delimiter und Encoding sind deshalb Vertragsbestandteil, kein Detail.

Bewusste Dialekt-Dualität: Handgepflegte Repo-CSVs (`data/half_boundaries.csv`, `team_roster.csv`) verwenden schlichtes Komma + UTF-8 ohne BOM; **nur** Hudl-Exporte verwenden `;` + BOM. Phase 1.2 darf den Export-Dialekt nicht auf die handgepflegten Dateien anwenden.

## Dateinamenskonvention

Muster: `YYYY-MM-DD_{TEAM1}-vs-{TEAM2}_{COMP}.csv`

Beispiel: `2026-06-14_GER-vs-AUT_EM-QUALI.csv`

- ISO-Datum zuerst → Dateien sortieren chronologisch im Ingest-Ordner.
- `_` trennt Felder, `-` verbindet Tokens innerhalb eines Feldes → eindeutiger Split auf `_`.
- Teamcodes: 3-Buchstaben-IFAF-Codes in Großbuchstaben (`GER`, `AUT`, …).
- `TEAM1` = Charting-Perspektive-Team ist ein **Vorschlag** — PENDING-ANALYST (siehe Gesprächsagenda, Punkt a).

Aus dem Dateinamen leitet der Ingest `game_id` und Metadaten (Datum, Teams, Wettbewerb) ab.

## `RESULT`-Vokabular & Grammatik

Kanonisches Vokabular: 13 Tokens, aufgeteilt in Basis- und Modifier-Tokens.

- **Basis-Tokens:** `Rush`, `KNEEL`, `Sack`, `Interception`, `Complete`, `Incomplete`, `Good`, `No Good`, `Fumble`, `Penalty`
- **Modifier-Tokens:** `TD`, `Def TD`, `Safety`, `Penalty`

**Grammatik:** Ein `RESULT`-Wert ist ein Basis-Token plus optionale Modifier, verbunden mit dem Separator `", "` (Komma + Leerzeichen), z. B. `Complete, TD` oder `Sack, Safety`. Matching ist **exakter Token-Vergleich nach Split auf den Separator, case-sensitiv**. Das ersetzt die fragile Substring-Semantik der bisherigen Pipeline (`str.contains` in `helper_add_hudl_mutations.py`), die nur durch Zufälle funktioniert: `Incomplete` matcht `contains("Complete")` nur deshalb nicht, weil polars case-sensitiv vergleicht; `TD` ⊂ `Def TD` wird per explizitem `Def`-Ausschluss abgefangen.

**C-07-Amendment (explizite Entscheidung, nicht stillschweigend):** `No Good` (97 Legacy-Vorkommen) und `Fumble` (9 Vorkommen) werden der C-07-Liste hinzugefügt. `KNEEL` bleibt im Vokabular, obwohl es in 3.701 Legacy-Plays 0-mal vorkommt. Die Fumble-Semantik (Possession-Wechsel? Dead Ball?) ist PENDING-ANALYST.

**`RESULT` ist verpflichtend (nicht-leer), forward-only.** Legacy-Defekt zur Einordnung: 846 von 3.701 Plays (23 %) haben ein leeres `RESULT` und werden von `helper_add_hudl_mutations.py` per `.otherwise(pl.lit("pass"))` stillschweigend als Pass klassifiziert. Fehlgeformte Legacy-Varianten (`Penalty (declined)`, `Complete Penalty` ohne Komma) sind grandfathered, forward ungültig.

**PAT-Semantik:** `DN` = 0 (`down == 0`) markiert einen PAT-Play. `Good` ist Exact-Match. `yardline_50` = 45 ist der 1-Punkt-Spot, 40 der 2-Punkt-Spot. Die aktuellen Baselines (50 % / 46 %) sind in `helper_add_ep_wp.py` hartkodiert und werden in Phase 1.3 empirisch ersetzt.

## Abgeleitete Felder

Die sechs bisher manuell gepflegten Spalten werden ab sofort deterministisch abgeleitet — niemand chartet zusätzliche Per-Play-ID-Spalten. Der Ableitungs-CODE ist Phase-1.2-Scope; dieser Vertrag dokumentiert nur die Regeln.

| Feld | Quelle | Regel | Randfälle | Legacy-Verifikation |
|------|--------|-------|-----------|---------------------|
| `play_id` | `PLAY #` | Identität | — | lückenlos 1..N in allen 47 Spielen |
| `yardline_50` | `YARD LN` | `-x if x < 0 else 50 - x` (negativ = eigene Hälfte, positiv = Yards zum gegnerischen Endzone-Ziel; Ergebnis = Yards von der eigenen Goalline, 0–50) | 2 Goalline-Zeilen wurden manuell auf 50 gesetzt | 3.656/3.658 Zeilen matchen (99,95 %) |
| `drive_id` | `ODK` + `RESULT` | Inkrement bei O/D-Possession-Flip; Scoring-/Turnover-`RESULT` (`TD`, `Def TD`, `Safety`, `Interception`, `Fumble` falls Turnover) schließt ebenfalls einen Drive | benötigt sauberes `ODK` ∈ {`O`, `D`, `K`}; Legacy-`ODK` ist verschmutzt (Teamnamen, Formationen) → Grandfathered-Pfad liest die manuelle `drive_id`-Spalte | Regel dokumentiert, Implementierung 1.2 |
| `half` | `data/half_boundaries.csv` | `1 if PLAY # < half2_first_play else 2` | eine Zeile pro Spiel, Pflege durch den Nutzer | Legacy-`half` bestätigt 2-Halbzeit-Struktur |
| `game_id` | Dateiname | geparst aus der Dateinamenskonvention; vom Ingest vergeben | Int-oder-str-Wahl liegt bei Phase 1.2 | — |
| `posteam` | Dateiname + `ODK`-Perspektive | PENDING-ANALYST — Vorschlag: `TEAM1` im Dateinamen = Perspektive-Team | Caveat aus `get_games()`: Das Team mit dem ersten Drive wird aktuell als "home_team" angenommen | offen bis Gespräch |

## Zeitdaten (REQ-S1-02) — explizit: nicht verfügbar

Der Export enthält **keine Zeitinformation**. `QTR` existiert als Spalte, ist aber faktisch nie gefüllt und wird von der Pipeline nicht konsumiert. Diese Nichtverfügbarkeit ist hiermit explizit festgehalten.

Konsequenz: `half_seconds_remaining` bleibt synthetisch gemäß C-08 — `play_time = 1200 / max(play_id_half)` pro (Spiel, Halbzeit), kumulativ abgezogen von 1200 Sekunden pro Halbzeit. Quellcode: `prepare_wp_data` in `helper_add_hudl_mutations.py`, Zeilen 384–407 (nicht `helper_add_ep_wp.py`, wie früher notiert). Phase-1.4-WP-Charts müssen explizit als "synthetic time" geflaggt werden.

**Einziger manueller Zeit-Input ist die Halbzeitgrenze:** `data/half_boundaries.csv` — schlichtes Komma-CSV (UTF-8 ohne BOM), Header `filename,half2_first_play`, eine Zeile pro Spiel, gekeyt über den Export-Dateinamen (funktioniert damit vor der `game_id`-Vergabe). Beispielzeile:

```csv
2026-06-14_GER-vs-AUT_EM-QUALI.csv,34
```

Regel: `half = 1 if PLAY # < half2_first_play else 2`.

## Defense-Felder (REQ-S1-03)

`DEF FRONT`, `COVERAGE` und `BLITZ` existieren im Export und sind teilweise gefüllt. Das kanonische Vokabular ist PENDING-SIGHTING: Es wird **zuerst** aus echten Distinct-Werten von Sample-Exporten abgeleitet (kein Repo-File enthält diese Spalten), dann im Vertrag fixiert. Kein vorschnelles Schema (keine Cover-0/1/2/3-vs-man/zone-Entscheidung vor Sichtung der Daten).

**Flag-Pull-Verursacher: bewusst übersprungen.** Diese Entscheidung ist Teil des Charting-Protokolls: Das Feld wird in dieser Phase absichtlich nicht eingeführt. Revisit später, falls Defense-Scouting auf Spielerinnen-Ebene Priorität bekommt.

## Charting-Protokoll & Gesprächsagenda

Genau drei Punkte gehören ins Analysten-Gespräch — nicht mehr:

**a) `ODK`-Perspektive bei getauschtem Gegner-Film** → `posteam`-Regel + `TEAM1`-Semantik im Dateinamen. Vorschlag: Der Dateiname trägt beide Teams, `TEAM1` ist das Team, aus dessen Perspektive `ODK` gechartet wird. [PENDING-ANALYST]
*Ergebnis:* ___ (nach Gespräch eintragen)

**b) `RESULT`-Charting:** verpflichtend nicht-leer für jeden Play; Fumble-Semantik (Possession-Wechsel?); Bestätigung des 13-Token-Vokabulars inkl. C-07-Amendment. [PENDING-ANALYST]
*Ergebnis:* ___ (nach Gespräch eintragen)

**c) Defense-Spalten:** Lücken/Konsistenz forward; Bestätigung des gesichteten Vokabulars für `DEF FRONT`, `COVERAGE`, `BLITZ`. [Ergebnis wird nach Sichtung + Gespräch eingetragen]
*Ergebnis:* ___ (nach Sichtung und Gespräch eintragen)

## Nicht-Ziele

- Kein Re-Export / Re-Charting der 47 Legacy-Spiele (grandfathered).
- Keine Verhandlung von Export-Mechanik mit dem Analysten — das Preset ist nutzereigene Realität.
- Flag-Pull-Verursacher wird nicht gechartet (bewusster Skip, siehe oben).
- Kein Ableitungs- oder Validierungscode in dieser Phase — der landet in Phase 1.2 (`src/flag_football_ep`).

## Datenschutz-Hinweis

Der Export enthält Spalten mit Spielerinnen-Namen (`RECEIVED BY`, `Thrown By`, `QB`). Die DSGVO-Behandlung ist auf Strang 2 (REQ-S2-01) gescopet; hier nur der Hinweis, dass diese personenbezogenen Spalten existieren und beim Teilen von Exporten mitgedacht werden müssen.
