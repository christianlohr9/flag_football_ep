# Plan 1: Hudl-Jahresexport, EP/WP-Retraining und Scouting-Produkte

Strang 1 von 2 (siehe `plan-2-cv-tracking.md`). Ziel: Aus dem gepflegten Hudl-Jahresbestand eine reproduzierbare Daten-Pipeline machen, die Modelle neu trainieren und daraus Produkte bauen, die der HC sofort nutzen kann. Dieser Strang liefert kurzfristig Wert und ist unabhängig vom CV-Strang; das CV-Tracking dockt später an dieselbe Play-by-Play-Basis an.

## Warum mehr als „Modell neu trainieren"

Der vorhandene Hudl-Export (`data_raw.csv`, 47 Spiele / ~3.700 Plays) enthält deutlich mehr als das WM-PBP-Schema: `OFF FORM`, `Off Str`, `OFF PLAY`, `TARGET ROUTE`, `RECEIVED BY`, `Thrown By`, `YAC`, `GN/LS`. Formation, Play-Call und Route pro Play sind Scouting-Gold — das WM-Datenset (`pbp_wc24_static.csv`) hat das nicht. Das eigentliche Produkt aus diesem Strang sind Tendenz- und Spielerinnen-Reports, EP-Retraining ist die Grundlage darunter.

## Phase 1.1 — Datenvertrag mit dem Videoanalysten (1 Gespräch + 1 Abend)

- Vollständigen Jahresexport definieren: welche Spiele/Turniere, welche Spalten, ein Export-Preset in Hudl, damit alle Spiele dieselben Spaltennamen und dasselbe `RESULT`-Vokabular haben. Die bestehende Pipeline parst `RESULT`-Strings (`Rush`, `Penalty`, `KNEEL`, `Sack`, `Interception`, `Complete`, `Incomplete`, `TD`, `Def TD`, `Good`, `Safety`) — jede Abweichung im Tagging-Vokabular bricht stillschweigend die Feature-Konstruktion.
- Klären, welche Felder heute manuell ergänzt werden (`game_id`, `play_id`, `drive_id`, `half`, `posteam`, `yardline_50`) und wie sie beim Export mitkommen oder deterministisch ableitbar sind.
- **Zeitdaten anfragen:** Das WP-Modell krückt aktuell mit gleichverteilter Play-Time pro Halbzeit (`1200 / max(play_id)`). Hudl-Clips haben Timestamps; wenn der Analyst Clip-Zeiten oder die Spieluhr mit exportieren kann, wird `half_seconds_remaining` echt statt synthetisch — der größte einzelne Qualitätshebel für das WP-Modell (im README selbst als „flawful bc of missing times" markiert).
- Optional fürs Charting-Protokoll: 2–3 Defense-Felder ergänzen (Coverage-Shell, Blitz ja/nein, Pull-Verursacherin). Kostet den Analysten Sekunden pro Play, eröffnet Defense-Scouting ohne jede CV.

## Phase 1.2 — Repo zur Pipeline machen (1–2 Wochenenden)

- Notebook-Logik in das begonnene `src/flag_football_ep`-Package ziehen (Hudl-Ingest, sportapp.fi-Ingest, Feature-Mutationen, Training, Scoring). Die Helper in `Python/` sind schon fast modulfertig.
- Ein Ingest-CLI: Ordner mit Hudl-Exporten rein → kanonisches Parquet raus, mit Validierungsreport pro Spiel (Downs 0–4, `yardline_50` in [0, 50], Drive-IDs monoton, Play-Sequenzen lückenlos, Score-Rekonstruktion == Endstand laut Spielbericht). Ohne diese Checks fallen Tagging-Fehler erst im EPA-Chart auf.
- Beide Quellen (Hudl eigene Spiele + sportapp.fi/IFAF-Turniere) in ein Schema mergen; `data_raw.csv`/`games_plays.csv`-Wildwuchs im Repo-Root nach `data/` konsolidieren.

## Phase 1.3 — Modelle neu trainieren, methodisch sauber (1–2 Wochenenden)

- **Split-Fix (wichtigster Punkt):** Aktuell `train_test_split` über Plays — Plays aus demselben Spiel/Drive landen in Train und Test, die Metriken sind zu optimistisch. Umstellen auf GroupKFold über `game_id` (das nflfastR-Vorbild macht LOSO über Seasons).
- Kalibrierung prüfen (Reliability-Kurven pro Klasse), Log-Loss gegen naive Baseline berichten. EP-Werte sind nur so gut wie die Kalibrierung der Klassenwahrscheinlichkeiten.
- Feature-Kandidaten mit dem größeren Datensatz erneut testen: `half`, echtes `half_seconds_remaining` (falls Phase 1.1 liefert), Wettbewerbslevel/Geschlecht als Kovariate, Recency-Gewichtung.
- **PAT-Baselines empirisch machen:** Die hartkodierten 50 % (1 Pt) / 46 % (2 Pt) aus `helper_add_ep_wp.py` aus dem Gesamtdatensatz neu schätzen und als Break-even-Chart fürs Coaching aufbereiten (ab welchem Spielstand/Restzeit lohnt 2?).
- Modelle versionieren (Datum + Trainingsdaten-Hash im Dateinamen statt `ep_model.pkl` überschreiben).

## Phase 1.4 — Coaching-Produkte (fortlaufend, erster Wurf 1–2 Wochenenden)

Priorisiert nach dem, was ein HC vor einem Turnier tatsächlich aufschlägt:

1. **Gegner-Tendenz-Report** (pro Team, automatisch generiert, HTML/PDF): Formation × Down & Distance × Feldzone, Ziel-Routen-Verteilung, Play-Call-Tendenzen nach Score-State, 4th-Down- und PAT-Verhalten. Quelle: getauschtes Hudl-Film-Charting + IFAF-Turnierdaten.
2. **Eigene Effizienz:** EPA/Play nach Formation/Play-Call/Route, EPA pro QB/Receiverin (`Thrown By`/`RECEIVED BY` liegen vor), YAC-Anteile, Drive Success.
3. **Entscheidungs-Charts:** PAT-Break-even, 4th-Down-Conversion-Raten nach Distance.
4. **Win-Probability-Charts pro Spiel** als Review-Tool (nach dem Zeitdaten-Fix).

Erfolgskriterium für den ganzen Strang: Vor dem nächsten Lehrgang/Turnier liegt für jeden Gruppengegner ein automatisch generierter Report, dessen Erzeugung aus rohen Exporten < 10 Minuten dauert.

## Nicht-Ziele

- Kein Live-/In-Game-Tooling.
- Keine CV-Abhängigkeit — dieser Strang funktioniert komplett ohne Plan 2.
