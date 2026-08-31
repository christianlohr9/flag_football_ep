# Hackathon-Benchmark-Labels — Session-Arbeitsblatt (Stand: 2026-08-31)

**Status: Labelling ausstehend (A + B offen, 0/41 bzw. 0/61 erledigt)**

Arbeitsblatt für die "wichtigste Stunde" der Hackathon-Vorbereitung
(`docs/hackathon-challenge-prep.md` §2): die restlichen 41 Kontinuitäts-Urteile (A) und die
Flag-Pull-Ereignis-Tabelle (B) für die 61-Clip-Pilot-Session
(`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`). Ohne A werden die 20 beim Gate schon
bewerteten Clips faktisch zum Testset, auf das jedes Hackathon-Team optimiert
(`docs/hackathon-challenge-prep.md` §2, "Wichtigste Stunde").

Maschinenlesbarer Gate über beide Tabellen: `tests/test_cv_benchmark_labels.py`
(`uv run pytest tests/test_cv_benchmark_labels.py -q`).

---

## Pass-Kriterium (A)

Identisch zur Gate-Definition in `docs/pilot-gate-decision.md`, Kriterium 1 der
Gate-Kriterien-Tabelle ("Track-Kontinuität | >= 90 % der Plays ohne ID-Switch"):

> **"≥ 90 % des Plays ohne Identitätswechsel"**

Frage beim Bewerten eines Clips: läuft der Spielzug zu mindestens 90 % seiner Dauer, ohne dass
eine Spielerin ihre Track-Nummer verliert oder mit einer anderen tauscht (ID-Switch)? Wenn ja
→ `pass`. Wenn nein → `fail` + Kurznotiz.

Dieselbe Frage wurde beim Gate für die ersten 20 Clips beantwortet
(`data/reference/continuity_review.csv`, Plan 02.1-14) — für Konsistenz bitte denselben Maßstab
anlegen, nicht strenger oder lockerer werden.

---

## A — Kontinuitäts-Urteile (~1 h, Pflicht)

**Bereits bewertet:** Clips 1–20 (6 `pass`, 14 `fail`).

**Noch zu bewerten (41 Clips):** 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61

### Overlay-Pfad

```
data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/overlays/clip_XXX.mp4
```

(`XXX` = dreistellige, nullgepolsterte Clip-Nummer, z. B. `clip_021.mp4` für Clip 21.) v2 =
BoT-SORT-Tracker mit Torso-Crop-Team-Zuordnung — derselbe Overlay-Stand wie beim Gate-Review der
ersten 20 Clips.

**Verifikationsstatus dieser Ausführungsumgebung:** Der Overlay-Ordner existiert in diesem
Ausführungskontext (Git-Worktree) nicht — Videos sind PII, gitignored und liegen laut
`docs/capture-legal.md` ausschließlich lokal auf Nutzer-Rechnern. Keiner der 61 Clip-Pfade
(`clip_001.mp4` … `clip_061.mp4`) konnte von hier aus verifiziert werden. **Vor Beginn der
Session bitte lokal prüfen**, dass alle 61 Dateien unter obigem Pfad vorhanden sind; fehlende
Clips hier oder in der SUMMARY nachtragen, statt stillschweigend zu überspringen.

### Spalten in `data/reference/continuity_review.csv`

| Spalte | Herkunft | Bedeutung |
|---|---|---|
| `clip_number` | Maschine | Clip-Index 1–61, nicht bearbeiten |
| `n_tracks` | Maschine | Anzahl distinkter Tracks im Clip, nicht bearbeiten |
| `longest_track_frac` | Maschine | Abdeckungsanteil des längsten Tracks, nicht bearbeiten |
| `n_fragments` | Maschine | Anzahl Track-Fragmente, nicht bearbeiten |
| `auto_flag` | Maschine | `ok` / `fragmented` / `few-tracks` / `no-tracks`, nicht bearbeiten |
| `verdict` | **Nutzer** | `pass` oder `fail` (leer = noch nicht bewertet) |
| `id_switches` | **Nutzer** | Anzahl gezählter ID-Switches, wenn zählbar (sonst leer) |
| `reviewer_note` | **Nutzer** | Kurznotiz im etablierten Stil, siehe unten |

**Notiz-Stil (Beispiele aus den ersten 20 Clips):**
- `"Verdeckungen: blau1->blau18->blau21 (parallel zu rot9), rot9->rot15, grau4/rot10-Tausch, grau4 muesste blau sein"`
- `"blau8 und rot5 switchen nach kurzer Ueberlagerung"`
- `"Kameraschnitt im Clip laesst Boxen verlieren (Capture-Artefakt)"`

Format: Team-Farbe + Track-Nummer (nie Spielerinnen-Namen — siehe Datenschutz-Hinweis unten),
knapp, auf Deutsch, ohne Umlaute (ASCII-Notizen wie die bestehenden 20).

**Wichtig — keine Spielerinnen-Namen in `reviewer_note` oder `notes`.** Nur Track-Nummern und
Team-Farben verwenden (`rot 3`, `blau 7`), niemals Namen aus `data/reference/roster.csv`. Ein
Test (`tests/test_cv_benchmark_labels.py`) prüft das automatisch (T-2.2-07).

---

## B — Flag-Pull-Ereignisse (~1 h, nur nötig falls der Bonus im Challenge-Scope bleibt)

Vorlage: `data/reference/flag_pull_events.csv` (61 Zeilen vorbefüllt, alle Werte leer).

### Outcome-Vokabular

| Wert | Bedeutung |
|---|---|
| `pull` | Flag erfolgreich gezogen, Spielzug endet regulär |
| `incomplete` | Unvollständiger Pass, Spielzug endet ohne Flag-Pull |
| `out_of_bounds` | Ballträgerin verlässt das Spielfeld |
| `touchdown` | Spielzug endet mit Touchdown (kein Pull nötig) |
| `other` | Keiner der obigen Fälle (z. B. Turnover, abgebrochener Spielzug) |

### Spalten in `data/reference/flag_pull_events.csv`

| Spalte | Bedeutung |
|---|---|
| `clip_number` | Clip-Index 1–61, bereits vorbefüllt |
| `outcome` | Ein Wert aus obigem Vokabular |
| `pull_time_s` | Nur bei `outcome = pull`: Timecode im Player in Sekunden, ±0,5 s reicht |
| `carrier_track_id` | Track-Nummer der Ballträgerin aus dem Overlay-Video |
| `puller_track_id` | Track-Nummer der ziehenden Spielerin aus dem Overlay-Video |
| `notes` | Optionale Kurznotiz (gleicher Stil wie `reviewer_note`, keine Namen) |

**Bonus-Entscheidung:** Falls der Aufwand für B zu groß wird, kann der Flag-Pull-Bonus aus der
Challenge gestrichen werden — dann bleibt `flag_pull_events.csv` leer und die Bonus-Metrik wird
aus dem Bundle-Scoring-Skript entfernt, statt halb gefüllt ausgeliefert zu werden. Diese
Entscheidung wird hier und in der Plan-SUMMARY festgehalten, sobald getroffen.

---

## Datenschutz

Overlay-Videos zeigen identifizierbare Personen (Gesichter, Rückennummern) und sind PII —
`docs/capture-legal.md`: nie in Versionskontrolle, verlassen nie den Projektkontext. In die
beiden CSVs dürfen ausschließlich Track-Nummern, Team-Farben und die oben beschriebenen
Kurznotizen — niemals Spielerinnen-Namen.

---

## Ergebnis

_Wird von Plan 02.2-03 Task 3 nach Abschluss der Labelling-Session ergänzt (Pass-Rate `k/61`,
per-Verdikt-Zählung, dominante Fehlermodi)._
