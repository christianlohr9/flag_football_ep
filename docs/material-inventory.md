# Materialinventur — Video-Bestand pro Kameradomäne

Maschinenlesbares Gegenstück: `data/reference/video_inventory.csv`.

**Status: einseitig erstellt am 2026-08-22 — Analysten-Ergänzung ausstehend (siehe Ratifizierungs-Block).**

## Zweck & Abgrenzung

Diese Inventur listet, welches Videomaterial pro Kameradomäne existiert, wer es hält und welcher Teil davon bereits gesichert (gehasht, verifizierbar) ist. Phase 2.1 wählt aus dem gesicherten Bestand das Pilotmaterial für den Go/No-Go-Test aus (Roadmap-Erfolgskriterium 1: 2–3 Beispielclips pro Domäne).

Nicht Teil dieser Inventur: CV-Verarbeitung, Ingest-Code, ein vollständiger Archiv-Durchlauf über sämtliches je aufgenommenes Material. Gescopet ist ausschließlich das, was tatsächlich als hashbare, verifizierbare Zeile in `data/reference/video_inventory.csv` landet — plus die ehrliche Prosa-Auflistung dessen, was noch nicht gesichert ist.

Dieses Dokument ist die Übergabe an zwei spätere Phasen: Phase 2.1 liest den gesicherten Bestand, um Pilotmaterial auszuwählen; Phase 2.3 joint XY-Tracks über `docs/sync-convention.md` an `data/processed/plays.parquet`, was voraussetzt, dass der hier dokumentierte `game_id`-Wert exakt übereinstimmt.

## Domänen & Bestand

| domain (CSV-Wert) | Beschreibung | Wer hält das Material | Stand 2026-08-22 |
|---|---|---|---|
| `drone` | Drohne in fester Hover-Position, Primärdomäne (D-03); bei offiziellen Spielen verboten (C-01) | unbekannt — beim Videoanalysten/Staff, nicht gesichtet | unbekannt — beim Videoanalysten/Staff, nicht gesichtet, siehe Ratifizierungs-Block |
| `sideline` | erhöhte Seitenkamera / Stativ / GoPro-Längsseite / Veo-Klasse, Zweitdomäne für offizielle Spiele | unbekannt — beim Videoanalysten/Staff, nicht gesichtet | unbekannt — beim Videoanalysten/Staff, nicht gesichtet, siehe Ratifizierungs-Block |
| `broadcast` | TV-/Seitenansicht Fremdmaterial; nur registriert, nicht verarbeitet (Phase 2.5, zurückgestellt) | unbekannt — beim Videoanalysten/Staff, nicht gesichtet | unbekannt — beim Videoanalysten/Staff, nicht gesichtet, siehe Ratifizierungs-Block |

Diese drei Werte sind das vollständige `domain`-Vokabular von `data/reference/video_inventory.csv`; es wird hier nichts erfunden, nichts geschätzt. Die tatsächlichen Bestandszahlen entstehen erst, wenn Material gesichtet und gesichert wurde (siehe „Clip registrieren" unten).

## Spaltenmodell von data/reference/video_inventory.csv

| Spalte | Typ | Pflicht | Hinweise |
|---|---|---|---|
| `domain` | string | Pflicht | nur `drone`, `sideline` oder `broadcast` |
| `session_id` | string | Pflicht | Kennung der Session im Stil `YYYY-MM-DD_{LABEL}`, z. B. `2026-05-12_TRAINING-DUS` |
| `game_id` | string | nur wenn ein Hudl-Spiel existiert | muss exakt dem `game_id` in `data/processed/plays.parquet` entsprechen (Dateinamens-Konvention aus `docs/data-contract.md` §Dateinamenskonvention) |
| `capture_date` | string | Pflicht | ISO-Format `YYYY-MM-DD` |
| `resolution` | string | Pflicht | Form `BREITExHÖHE`, aus `ffprobe` |
| `fps` | float | Pflicht | aus `r_frame_rate` ausgerechnet, z. B. `30000/1001` → `29.97` |
| `duration_seconds` | float | Pflicht | aus `format.duration` |
| `local_path` | string | Pflicht | repo-relativ unter `data/video/`, nie ein absoluter Benutzerpfad |
| `content_sha256` | string | Pflicht wenn `local_path` gesetzt | 64 Kleinbuchstaben-Hex-Zeichen |
| `notes` | string | optional | Freitext **ohne Personennamen** |

Bewusste Dialekt-Dualität: `data/reference/video_inventory.csv` ist handgepflegt und verwendet Komma + UTF-8 **ohne** BOM; der Hudl-Export-Dialekt (`;` + `utf-8-sig`) gilt hier ausdrücklich nicht.

## Clip registrieren — Schritt für Schritt

1. Clip nach `data/video/` legen (oder einen repo-relativen Pfad dorthin vergeben).
2. `ffprobe -v quiet -print_format json -show_format -show_streams <datei>` ausführen und `width`, `height`, `r_frame_rate` sowie `format.duration` ablesen.
3. `shasum -a 256 <datei>` (macOS) bzw. `sha256sum <datei>` (Linux) ausführen.
4. Eine Zeile an `data/reference/video_inventory.csv` anhängen — Spaltenreihenfolge wie oben.
5. `uv run pytest tests/test_capture_artifacts.py -q` laufen lassen — das Gate prüft Domänen-Vokabular, Hash-Form und Pfad-Form.

Zeit-Erwartung: SHA-256 über einen mehrere GB großen 4K-Clip dauert wenige Minuten je nach Speichermedium — das ist normal, nicht hängend. Eine Zeile mit gefülltem `local_path`, aber leerem `content_sha256` gilt als unvollständig, nicht als akzeptabel.

## Wo die Clips liegen

`data/video/` ist per `.gitignore` (`data/video/*`, `!data/video/.gitkeep`) von Git ausgeschlossen — gleiche PII-Policy wie `data/raw/hudl/*`, denn die Aufnahmen zeigen Gesichter von Spielerinnen.

Die CSV ist die Zeiger- und Verifikationsschicht: `local_path` + `content_sha256` machen einen verschobenen oder umbenannten Clip wiederauffindbar und einen beschädigten erkennbar. Clips außerhalb des Repos sind zulässig, brauchen aber trotzdem einen repo-relativen Symlink unter `data/video/`, damit `local_path` keinen Benutzerpfad enthält.

`data/video/` ist absichtlich leer im frischen Clone (nur `.gitkeep`); jeder, der dieses Repo klont, muss Clips selbst besorgen oder verlinken — genau wie bei `data/raw/hudl/*` gilt: kein Rohmaterial mit Personenbezug landet in der Versionskontrolle.

## Was nicht in die CSV kommt

Material, das nur der Analyst hält und das der Nutzer nie gesehen hat, bekommt **keine** CSV-Zeile — jede Zeile muss hashbar und damit verifizierbar sein, eine erfundene oder aus zweiter Hand berichtete Zeile würde dieses Prinzip brechen. Solches Material wird stattdessen in der Tabelle unter „Domänen & Bestand" in Prosa geführt (aktuell: „unbekannt — beim Videoanalysten/Staff, nicht gesichtet") und im Ratifizierungs-Block adressiert.

Das gilt auch für Gerüchte oder mündliche Zusagen über vorhandenes Material („der Analyst hat bestimmt was von der letzten Session") — ohne `ffprobe`-Lauf und Hash bleibt es außerhalb der CSV, egal wie plausibel die Quelle.

Diese Regel ist bewusst strenger als die Kern+Optional-Logik von `docs/data-contract.md`: dort dürfen optionale Spalten leer bleiben, hier darf die Zeile selbst gar nicht erst entstehen, solange sie nicht verifizierbar ist.

## Ratifizierungs-Block

> DEFERRED-ANALYST: Gespräch auf unbestimmte Zeit verschoben (User-Entscheid 2026-08-17). Owner: Nutzer. Follow-up-Trigger: sobald der Videoanalyst wieder verfügbar ist, spätestens vor dem nächsten Filmtausch. Zu ratifizieren: (a) welches Material pro Domäne tatsächlich existiert (Spalte „Stand"), (b) Übergabe von 2–3 Beispielclips pro Domäne zur Sicherung und Hashung, (c) ob `broadcast`-Material überhaupt zugänglich ist.
