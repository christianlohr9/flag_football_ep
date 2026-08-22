# Sync-Konvention — Video-Play ↔ Hudl-PBP-Play

Maschinenlesbares Gegenstück: `data/reference/video_sync.csv`.

**Status: Variante B einseitig festgelegt am 2026-08-22; Variante A ist ein Wunsch — Analysten-Ratifizierung ausstehend (siehe Ratifizierungs-Block).**

## Zweck & Abgrenzung

Diese Konvention hängt Videopositionen an Plays, die in Hudl **schon existieren**; sie definiert niemals einen Play neu (D-10). Ohne diesen Join bleiben Tracking-Daten von Strang 1 entkoppelt (Roadmap-Erfolgskriterium 4) — eine XY-Spur ohne `game_id`/`play_id`-Anker ist für Coaching-Metriken wertlos.

Nicht in Scope: automatische Snap-Erkennung (Phase 2.3 baut darauf auf, ersetzt hier aber nichts), XY-Extraktion aus dem Video (Phase 2.1). Diese Konvention beschreibt ausschließlich das Format des Ankers, nicht wie er entsteht.

## Referenzseite: was in Hudl schon existiert

`PLAY #` wird beim Ingest zu `play_id`; `game_id` kommt aus der Export-Dateinamens-Konvention `YYYY-MM-DD_{TEAM1}-vs-{TEAM2}_{COMP}` (siehe `docs/data-contract.md` §Dateinamenskonvention). `ODK`, `DN`, `DIST` und `RESULT` dienen als Plausibilitätscheck, dass beim manuellen Markieren der richtige Play getroffen wurde (D-10): nach dem Setzen eines Ankers werden die `DN`/`DIST`/`RESULT`-Werte des Ziel-Plays gegen das gehalten, was im Video tatsächlich passiert — stimmt z. B. `DN=3, DIST=7, RESULT=Complete` nicht mit einem im Video sichtbaren Turnover überein, ist der Anker falsch gesetzt, nicht der Hudl-Eintrag falsch gechartet.

## Was von Staff verlangt wird: nichts

Kein Board, kein Klatschen, kein Timestamp-Overlay ist Voraussetzung für diese Konvention (D-09). Ein sichtbares Drive-Signal steht in `docs/capture-protocol.md` ausschließlich als Wunsch mit Begründung, nie als Blocker. Die Sync-Konvention funktioniert bei null Kooperation von Feldstaff — das gesamte Ankern passiert nachträglich am Schnittplatz, nicht während des Spiels.

## Variante A — Upload nach Hudl (Wunsch, DEFERRED-ANALYST)

Das Drohnenmaterial wird wie Spielfilm nach Hudl hochgeladen und vom Analysten ohnehin pro Play geschnitten; dann sind die Hudl-Clipgrenzen die Anker und es entfällt jedes manuelle Markieren im Rohvideo. Das ist als Bitte formuliert, nicht als Erwartung — der Analyst ist aktuell nicht verfügbar (DEFERRED-ANALYST), und diese Konvention hängt ausdrücklich **nicht** davon ab, dass Variante A jemals eintritt.

## Variante B — manuelle Snap-Anker (immer verfügbar)

Die Rohvideodatei liegt außerhalb von Hudl; der Nutzer markiert jeden Snap selbst. **Ein Anker pro Play, nie pro Drive** — mit der Begründung aus D-11: robust gegen ungefilmte Plays, Akku-Lücken und Penalties, die eine Drive-relative Zeitrechnung sofort auseinanderlaufen lassen würden. Es wird kein verlässlicher Timestamp im Material erwartet; Anker sind Framenummern bzw. Sekunden-Offsets **innerhalb einer benannten Videodatei**, nicht absolute Uhrzeiten.

Struktureller Präzedenzfall ist `data/reference/half_boundaries.csv`: eine kleine, handgepflegte Anker-CSV, die eine Lücke schließt, die der Hudl-Export nicht liefert (dort die Halbzeitgrenze, hier die Video-Play-Zuordnung). Gleiche Bauart: ein Zeilen-pro-Fall-Prinzip, gepflegt vom Nutzer, kein automatisierter Ableitungscode.

## Spaltenmodell von data/reference/video_sync.csv

| Feld | Quelle | Regel | Randfälle |
|------|--------|-------|-----------|
| `game_id` | Hudl-Exportdateiname | muss exakt dem `plays.parquet`-Wert entsprechen | — |
| `play_id` | Hudl `PLAY #` | ganzzahlig, ohne führende Nullen und ohne Leerzeichen | — |
| `video_file` | `local_path`-Wert aus `video_inventory.csv` | zeigt auf genau eine registrierte Videodatei | Play über zwei Videodateien verteilt → Anker auf die Datei setzen, die den Snap enthält |
| `snap_frame` | manuell markiert | Framenummer des Snaps in dieser Datei | — |
| `snap_seconds` | manuell markiert | Sekunden-Offset des Snaps; mindestens eine der beiden Spalten `snap_frame`/`snap_seconds` muss gefüllt sein | — |
| `end_seconds` | manuell markiert, optional | Ende des relevanten Ausschnitts | Play im Video nicht vorhanden (Akku-Wechsel, Kamera aus) → keine Zeile statt Zeile mit Nullwerten |
| `notes` | Freitext | ohne Personennamen | — |

Dtype-Tabelle, identisch zu `SYNC_SCHEMA` in `tests/test_capture_artifacts.py`:

| Spalte | Dtype |
|---|---|
| `game_id` | `Utf8` |
| `play_id` | `Int32` |
| `video_file` | `Utf8` |
| `snap_frame` | `Int64` |
| `snap_seconds` | `Float64` |
| `end_seconds` | `Float64` |
| `notes` | `Utf8` |

Bewusste Dialekt-Dualität: `data/reference/video_sync.csv` ist handgepflegt und verwendet Komma + UTF-8 **ohne** BOM; der Hudl-Export-Dialekt (`;` + `utf-8-sig`) gilt hier ausdrücklich nicht — identisch zur Regel in `docs/material-inventory.md`.

## Join gegen data/processed/plays.parquet

Der Join läuft als inner join auf `["game_id", "play_id"]`. Deutliche Warnung: bei Dtype-Mismatch (etwa `play_id` als String `"03"` statt Integer `3`) liefert der Join **null Zeilen ohne Fehlermeldung** — das sieht aus wie fehlendes Material, ist aber ein Typfehler. Deshalb wird beim Lesen immer explizit `schema_overrides` gesetzt, und `tests/test_capture_artifacts.py::test_video_sync_schema_joins_plays_shaped_frame` bewacht genau diesen Fall mit einem inline gebauten, `plays.parquet`-förmigen Fixture.

`data/processed/*` ist gitignored: `plays.parquet` existiert in keinem frischen Clone und wird in Tests durch dieses Fixture ersetzt, nie durch eine echte Datei gelesen.

Wer `video_sync.csv` außerhalb der Tests gegen ein echtes `plays.parquet` prüfen will, muss die eigene Pipeline vorher lokal laufen lassen — diese Konvention selbst erzeugt kein `plays.parquet`.

## Kompatibilität mit automatischer Snap-Erkennung (Phase 2.3)

Dieselbe Zeilenform (`game_id`, `play_id`, `video_file`, `snap_frame`/`snap_seconds`) ist das, was eine spätere automatische Snap-Erkennung vorschlagen würde; automatische Vorschläge ersetzen also nur die Befüllung, nicht das Format. Empfehlung: maschinell erzeugte Zeilen in `notes` kennzeichnen (z. B. `auto-detected, unreviewed`), damit sie von handgesetzten Ankern unterscheidbar bleiben, bis sie manuell bestätigt sind.

## Ratifizierungs-Block

> DEFERRED-ANALYST: Gespräch auf unbestimmte Zeit verschoben (User-Entscheid 2026-08-17). Owner: Nutzer. Follow-up-Trigger: sobald der Videoanalyst wieder verfügbar ist, spätestens vor dem nächsten Filmtausch. Zu ratifizieren: (a) Bereitschaft, Drohnenmaterial wie Spielfilm nach Hudl hochzuladen und pro Play zu schneiden (Variante A), (b) ob die Hudl-Clipgrenzen exportierbar/auslesbar sind, (c) ob `PLAY #` im Drohnen-Charting identisch zum bestehenden Charting vergeben wird.
