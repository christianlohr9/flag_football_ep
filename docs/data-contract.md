# Datenvertrag — Hudl-Export Flag Football Nationalteam

Version 1.0. Maschinenlesbares Gegenstück: `data-contract.schema.json` (wird vom Phase-1.2-Ingest-Validator direkt konsumiert — Spec und Validierung können nicht auseinanderlaufen). Die Sichtung echter Export-Werte für die Defense-Spalten ist erfolgt (2026-08-17, drei Sample-Exporte).

**Status: v1.0 einseitig festgelegt am 2026-08-17 — Analysten-Ratifizierung ausstehend (siehe DEFERRED-ANALYST-Block in §8 „Charting-Protokoll & Gesprächsagenda").** Das Analysten-Gespräch ist auf unbestimmte Zeit verschoben; alle Vertragsinhalte gelten ab sofort als einseitige Arbeitsentscheidungen des Nutzers.

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
- **Perspektiv-Regel (einseitig festgelegt am 2026-08-17):** TEAM1 = Charting-Perspektive-Team im Dateinamen; posteam = TEAM1 wenn ODK == 'O', sonst TEAM2. Analysten-Ratifizierung ausstehend (siehe DEFERRED-ANALYST-Block in §8).

Aus dem Dateinamen leitet der Ingest `game_id` und Metadaten (Datum, Teams, Wettbewerb) ab.

## `RESULT`-Vokabular & Grammatik

Kanonisches Vokabular: 13 Tokens, aufgeteilt in Basis- und Modifier-Tokens.

- **Basis-Tokens:** `Rush`, `KNEEL`, `Sack`, `Interception`, `Complete`, `Incomplete`, `Good`, `No Good`, `Fumble`, `Penalty`
- **Modifier-Tokens:** `TD`, `Def TD`, `Safety`, `Penalty`

**Grammatik:** Ein `RESULT`-Wert ist ein Basis-Token plus optionale Modifier, verbunden mit dem Separator `", "` (Komma + Leerzeichen), z. B. `Complete, TD` oder `Sack, Safety`. Matching ist **exakter Token-Vergleich nach Split auf den Separator, case-sensitiv**. Das ersetzt die fragile Substring-Semantik der bisherigen Pipeline (`str.contains` in `helper_add_hudl_mutations.py`), die nur durch Zufälle funktioniert: `Incomplete` matcht `contains("Complete")` nur deshalb nicht, weil polars case-sensitiv vergleicht; `TD` ⊂ `Def TD` wird per explizitem `Def`-Ausschluss abgefangen.

**C-07-Amendment (explizite Entscheidung, nicht stillschweigend):** `No Good` (97 Legacy-Vorkommen) und `Fumble` (9 Vorkommen) werden der C-07-Liste hinzugefügt. `KNEEL` bleibt im Vokabular, obwohl es in 3.701 Legacy-Plays 0-mal vorkommt. Das 13-Token-Vokabular inkl. C-07-Amendment ist als einseitige Arbeitsentscheidung bestätigt (2026-08-17). **Fumble-Arbeitssemantik:** Fumble = gecharteter Ballverlust-Tag; die Possession-Wechsel-Semantik wird bei der Analysten-Ratifizierung bestätigt (siehe DEFERRED-ANALYST-Block in §8).

**`RESULT` ist verpflichtend (nicht-leer), forward-only** (einseitige Arbeitsentscheidung 2026-08-17, Ratifizierung ausstehend — §8). Legacy-Defekt zur Einordnung: 846 von 3.701 Plays (23 %) haben ein leeres `RESULT` und werden von `helper_add_hudl_mutations.py` per `.otherwise(pl.lit("pass"))` stillschweigend als Pass klassifiziert. Fehlgeformte Legacy-Varianten (`Penalty (declined)`, `Complete Penalty` ohne Komma) sind grandfathered, forward ungültig.

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
| `posteam` | Dateiname + `ODK` | TEAM1 = Charting-Perspektive-Team im Dateinamen; posteam = TEAM1 wenn ODK == 'O', sonst TEAM2 (einseitig festgelegt 2026-08-17, Ratifizierung ausstehend — §8) | Caveat aus `get_games()`: Das Team mit dem ersten Drive wird aktuell als "home_team" angenommen (nur für Scores/Differential relevant) | Regel konkret; Implementierung 1.2 |

## Zeitdaten (REQ-S1-02) — explizit: nicht verfügbar

Der Export enthält **keine Zeitinformation**. `QTR` existiert als Spalte, ist aber faktisch nie gefüllt und wird von der Pipeline nicht konsumiert. Diese Nichtverfügbarkeit ist hiermit explizit festgehalten.

Konsequenz: `half_seconds_remaining` bleibt synthetisch gemäß C-08 — `play_time = 1200 / max(play_id_half)` pro (Spiel, Halbzeit), kumulativ abgezogen von 1200 Sekunden pro Halbzeit. Quellcode: `prepare_wp_data` in `helper_add_hudl_mutations.py`, Zeilen 384–407 (nicht `helper_add_ep_wp.py`, wie früher notiert). Phase-1.4-WP-Charts müssen explizit als "synthetic time" geflaggt werden.

**Einziger manueller Zeit-Input ist die Halbzeitgrenze:** `data/half_boundaries.csv` — schlichtes Komma-CSV (UTF-8 ohne BOM), Header `filename,half2_first_play`, eine Zeile pro Spiel, gekeyt über den Export-Dateinamen (funktioniert damit vor der `game_id`-Vergabe). Beispielzeile:

```csv
2026-06-14_GER-vs-AUT_EM-QUALI.csv,34
```

Regel: `half = 1 if PLAY # < half2_first_play else 2`.

## Defense-Felder (REQ-S1-03)

`DEF FRONT`, `COVERAGE` und `BLITZ` existieren im Export und sind teilweise gefüllt. Das kanonische Vokabular unten ist — wie in der Phase entschieden: Daten zuerst, Schema danach — aus echten Distinct-Werten von drei Sample-Exporten abgeleitet (Sichtung 2026-08-17): zwei eigene Spiele (GER beteiligt: GER vs. SLO, MEX vs. GER; reiches Preset mit 43 bzw. 45 Spalten) und ein Scouting-Export (Gegner-Film GB vs. CAN; 12-Spalten-Preset). Es wurde bewusst **kein** Lehrbuch-Schema (Cover 0/1/2/3, man/zone) übergestülpt — der Vertrag fixiert die tatsächlich verwendete Terminologie. Die Samples liegen unter `data/samples/`, enthalten Spielerinnen-Namen (PII) und sind deshalb per `.gitignore` dauerhaft von der Versionskontrolle ausgeschlossen.

### Preset-Asymmetrie & Fill-Rates (Sichtung 2026-08-17)

Die drei Defense-Spalten sind nicht in jedem Preset vorhanden und werden je nach Film-Typ sehr unterschiedlich konsequent gefüllt:

| Sample | Typ | Preset | `DEF FRONT` | `COVERAGE` | `BLITZ` |
|---|---|---|---|---|---|
| GER vs. SLO | eigenes Spiel | 43 Spalten | **Spalte fehlt** | 76/81 (94 %) | 36/81 (44 %) |
| MEX vs. GER | eigenes Spiel | 45 Spalten | 88/94 (94 %) | 69/94 (73 %) | 89/94 (95 %) |
| GB vs. CAN | Scouting (Gegner-Film) | 12 Spalten | 12/86 (14 %) | 15/86 (17 %) | 8/86 (9 %) |

Befunde:

1. **Eigene Spiele sind deutlich reicher gefüllt als Scouting-Filme** (73–95 % vs. 9–17 % Fill-Rate) — bestätigt die Vermutung des Nutzers.
2. **Die Preset-Landschaft ist instabil:** Keines der drei Samples entspricht dem 19-Spalten-Baseline-Preset dieses Vertrags; selbst die beiden eigenen Spiele unterscheiden sich (43 vs. 45 Spalten). Konsequenz forward: Der Nutzer legt das 19-Spalten-Vertrags-Preset als eigenes Hudl-Export-Preset an und verwendet es für alle künftigen Exporte.
3. **`DEF FRONT`-Verfügbarkeit ist asymmetrisch — und nicht "own = mehr":** Das 12-Spalten-Scouting-Preset enthält `DEF FRONT`, das 43-Spalten-Own-Preset dagegen gar nicht (das 45-Spalten-Own-Preset wieder schon). `COVERAGE` und `BLITZ` sind in allen drei Presets vorhanden.

### Kanonisches Vokabular

Das gesichtete kanonische Vokabular für `DEF FRONT`, `COVERAGE` und `BLITZ` wird **einseitig festgelegt** (2026-08-17) und gilt ab sofort as-is; die Analysten-Ratifizierung ist ausstehend (siehe DEFERRED-ANALYST-Block in §8).

**`DEF FRONT`** — zwei disjunkte Teil-Vokabulare je nach Film-Typ:

- Scouting-Notation: `LINE 5`, `LINE 7` — plus eine einmalige Übergangsnotation `LINE 5 --> 7` (Front-Wechsel innerhalb eines Plays/Abschnitts; Semantik bei der Ratifizierung zu klären).
- Own-Game-Notation: numerische Vierer-Tupel (Muster `^\d+(-\d+){3}$`), gesichtet: `5-5-7-6`, `5-6-6-5`, `5-6-6-6`, `5-6-7-6`, `5-7-7-5`, `6-6-6-5`, `6-6-6-6`, `6-7-7-6`, `7-7-7-7`, `7-7-13-7`, `7-8-8-7`, `8-8-8-8`, `10-10-13-10` — mutmaßlich eine Tiefen-/Ausrichtungs-Staffelung der Front in Yards (Bedeutung bei der Ratifizierung bestätigen) — plus die Sammel-Calls `ALL GL` (Goal Line) und `ALL FIRST`.

Die Tupel-Aufzählung ist **offen** (kein geschlossenes Enum): Das Schema führt die gesichteten Werte, neue Tupel nach demselben Muster sind vertragskonform.

**`COVERAGE`** — drei gemischte Namensräume:

- Team-Call-Farben/-Namen: `WHITE`, `BLACK`, `BLUE`, `GRAY`, `YELLOW`, `MONEY`, `POOR`
- Numerische Zonen-Shells: `0-4`, `1-2`, `1-3`, `3-1`, `F 1-2-1`, `SC 1-2-1`
- Lehrbuch-Begriff: `QUARTERS` (1× im Scouting-Film)

**`BLITZ`** — Befund statt Vokabular: Die beobachteten Werte sind **durchgängig Personennamen** (in einem Teil der Filme Vornamen, in einem anderen Nachnamen, gemischte Groß-/Kleinschreibung). Die Spalte chartet also *wer* geblitzt hat, nicht *ob/wie* — es ist kein taktisches Vokabular. Die Namen werden hier bewusst **nicht** als kanonische Werte aufgenommen: Sie sind personenbezogene Daten und keine Team-Taktik-Begriffe. Das Schema führt `BLITZ` deshalb als Freitext-Personenreferenz mit dem semantischen Platzhalter `<BLITZERIN-NAME>` (kein Literal-Wert). Solange das Name-Charting beibehalten wird, gilt `BLITZ` als PII-Spalte im Sinne des Datenschutz-Hinweises (wie `RECEIVED BY`, `Thrown By`, `QB`).

### Varianten → Kanonisch (Normalisierungs-Mapping)

| Beobachtet | Kanonisch | Regel |
|---|---|---|
| `Line 5 --> 7` | `LINE 5 --> 7` | Großschreibung vereinheitlichen (`LINE`) |
| `F 1 -2-1` | `F 1-2-1` | Leerzeichen um Bindestriche entfernen |
| `SC 1 - 2 -1` | `SC 1-2-1` | Leerzeichen um Bindestriche entfernen |

Generelle Normalisierungsregeln für die Defense-Spalten: Großschreibung, Leerzeichen um Bindestriche in numerischen Mustern entfernen. Personennamen in `BLITZ` werden nicht normalisiert (kein kanonisches Vokabular, s. o.).

### Offene Sichtungs-Fragen (zur Ratifizierung, Agenda-Punkt c)

Die Sichtung hinterlässt konkrete offene Fragen. Sie sind **nicht** durch Erfindung aufgelöst, sondern bleiben als Ratifizierungs-Punkte offen (siehe DEFERRED-ANALYST-Block in §8):

- `DEF FRONT` fehlt im 43-Spalten-Own-Preset komplett — Preset vereinheitlichen (Vertrags-Preset nutzt der Nutzer selbst) und klären, ob die Spalte in eigenen Spielen forward gechartet wird.
- Zwei disjunkte `DEF FRONT`-Notationen (`LINE n` im Scouting vs. Vierer-Tupel in eigenen Spielen) — eine Notation forward festlegen; Bedeutung der Vierer-Tupel bestätigen.
- Übergangsnotation `LINE 5 --> 7` (1×): Sollen Front-Wechsel innerhalb eines Plays gechartet werden, und in welcher Form?
- `COVERAGE` mischt Farb-Calls, numerische Shells und den Lehrbuch-Begriff `QUARTERS`: Sind Farb-Calls und Shells zwei Dimensionen desselben Calls oder Synonyme? Was bedeuten die Präfixe `F` und `SC`? Verhältnis von `1-2` zu `F 1-2-1`?
- `BLITZ`-Semantik forward entscheiden: (i) Name-Charting beibehalten (dann bleibt `BLITZ` eine PII-Spalte und braucht eine einheitliche Namensform — Vor- oder Nachname) oder (ii) auf ein taktisches Vokabular umstellen (z. B. Rush-Anzahl oder ja/nein), Blitzerin ggf. separat charten.
- Scouting-Filme sind kaum gefüllt (9–17 %): Anspruch an Scouting-Charting forward definieren (bewusst dünn lassen vs. nachziehen).

**Flag-Pull-Verursacher: bewusst übersprungen.** Diese Entscheidung ist Teil des Charting-Protokolls: Das Feld wird in dieser Phase absichtlich nicht eingeführt. Revisit später, falls Defense-Scouting auf Spielerinnen-Ebene Priorität bekommt.

## Charting-Protokoll & Gesprächsagenda

Das Analysten-Gespräch wurde auf unbestimmte Zeit verschoben (User-Entscheid 2026-08-17, Analyst derzeit nicht verfügbar). Die drei Agenda-Punkte sind als **einseitige Arbeitsentscheidungen** festgelegt und gelten ab sofort; die Ratifizierung ist als ein konsolidierter Block unten festgehalten.

**a) `ODK`-Perspektive bei getauschtem Gegner-Film** → *Festgelegt (2026-08-17):* TEAM1 = Charting-Perspektive-Team im Dateinamen; posteam = TEAM1 wenn ODK == 'O', sonst TEAM2 (siehe §3 Dateinamenskonvention und §5 Abgeleitete Felder). Caveat aus `get_games()` bleibt festgehalten: Das Team mit dem ersten Drive wird aktuell als "home_team" angenommen (nur für Scores/Differential relevant).

**b) `RESULT`-Charting** → *Festgelegt (2026-08-17):* `RESULT` verpflichtend nicht-leer für jeden Play; 13-Token-Vokabular inkl. C-07-Amendment bestätigt. Fumble-Arbeitssemantik: Fumble = gecharteter Ballverlust-Tag; die Possession-Wechsel-Semantik wird bei der Ratifizierung bestätigt (siehe §4).

**c) Defense-Spalten** → *Festgelegt (2026-08-17):* Das gesichtete kanonische Vokabular für `DEF FRONT`, `COVERAGE`, `BLITZ` aus der Sichtung wird as-is übernommen; Flag-Pull-Verursacher bleibt bewusst übersprungen (entschieden). Die sechs Sichtungs-Fragen (siehe §7 „Offene Sichtungs-Fragen“) bleiben offen und gehören zur Ratifizierung.

### Ratifizierungs-Block

> DEFERRED-ANALYST: Gespräch auf unbestimmte Zeit verschoben (User-Entscheid 2026-08-17). Owner: Nutzer. Follow-up-Trigger: sobald der Videoanalyst wieder verfügbar ist, spätestens vor dem nächsten Filmtausch. Zu ratifizieren: (a) TEAM1/posteam-Regel, (b) RESULT-Pflicht + Fumble-Semantik, (c) Defense-Vokabular inkl. der sechs Sichtungs-Fragen.

## Nicht-Ziele

- Kein Re-Export / Re-Charting der 47 Legacy-Spiele (grandfathered).
- Keine Verhandlung von Export-Mechanik mit dem Analysten — das Preset ist nutzereigene Realität.
- Flag-Pull-Verursacher wird nicht gechartet (bewusster Skip, siehe oben).
- Kein Ableitungs- oder Validierungscode in dieser Phase — der landet in Phase 1.2 (`src/flag_football_ep`).

## Datenschutz-Hinweis

Der Export enthält Spalten mit Spielerinnen-Namen (`RECEIVED BY`, `Thrown By`, `QB`). Die DSGVO-Behandlung ist auf Strang 2 (REQ-S2-01) gescopet; hier nur der Hinweis, dass diese personenbezogenen Spalten existieren und beim Teilen von Exporten mitgedacht werden müssen.
