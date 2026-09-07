# Hackathon-Bundles — Inhalt, Aufbau, Reproduktion (Stand: 2026-09-07)

**Status: Alle drei Bundles gebaut und gehasht. Transfer-Set unverändert seit Plan 02.2-12
(2026-09-01). Dev-Set und Test-Set neu gebaut durch Plan 02.2-21 (2026-09-07): das
Test-Set nutzt jetzt das echte zweite Drohnenspiel (GER vs. Puerto Rico, DATA-04) statt
des früheren D-07-Fallbacks (18 zurückgehaltene Pilotspiel-Clips); der Dev-Set-Umfang ist
entsprechend auf alle 61 Pilotspiel-Clips gewachsen (siehe `## Zweites Drohnenspiel —
gelöst 2026-09-02/07` unten).**

Dieses Dokument beschreibt, was jedes `ffep cv bundle --kind <kind>`-Archiv enthält, wie es
aufgebaut ist, wie es reproduziert wird und unter welchen Regeln es an die Hackathon-Teams
geht (D-08/D-09). Der Builder selbst ist `src/flag_football_ep/cv/bundle.py::build_bundle`
(Plan 02.2-10); die Scoring-Referenz ist `scripts/hackathon/score_tracks.py` (Plan 02.2-10).

---

## Dev-Set (öffentlich, fertig)

**Archiv:** `data/bundles/dev-set_2026-09-07_08a55bd95b06.zip` (~2,10 GB, 17.191 Dateien,
gitignored unter `data/bundles/`).

**Content-Hash (`content_sha256`, aus `manifest.json`):**
`08a55bd95b066f8850e36624963a120a416f9072533dd84b5d0419f7885e00c9`

**Eingefrorener Detektor-Lauf:** `87a8a5222f7a472787875e974d089c44` (aus
`data/reference/hackathon_freeze.json`, Dataset-Hash
`ab3a9673d61bc348d37ce298ba12d18b76395d1ade82a735c5b3d82d2e46aec0`).

### Inhalt

Alle Clips mit `hackathon_role = dev` in `data/reference/hackathon_split.csv` — **61 von
61** Drohnen-Clips des Pilotspiels GER vs. Panama Rojo (16.05.2026), vollständig. Seit
Plan 02.2-21 gibt es keine Pool-Beschränkung mehr im Dev-Set: die vormalige `role =
pool`-Einschränkung (43 der 61 Clips) betraf ausschließlich unseren eigenen
Detektor-Trainings-/Eval-Split (`data/reference/frozen_eval_clips.csv`), nie die
ReID-Aufgabe der Teams. Mit dem Test-Set jetzt in einem ANDEREN Spiel (Puerto Rico, siehe
unten) gibt es keinen Grund mehr, Pilotspiel-Clips vor den Teams zurückzuhalten.
Verifiziert (siehe `## Verifikation` unten).

| Artefakt | Inhalt | Herkunft |
|---|---|---|
| `data/clips/clip_NNN.mp4` | 61 rohe Drohnen-Clips | `data/video/…` (Plan 02.0) |
| `data/overlays/clip_NNN.mp4` | Boxen + Track-Nummern zur Sichtprüfung | Plan 02.1-14 |
| `data/detections.parquet` | Pro-Frame-Detektionen des eingefrorenen Detektors | Plan 02.2-08 |
| `data/tracks.parquet` | Baseline-Tracks (BoT-SORT), Team-Zuordnung, Feldkoordinaten | Phase 2.1 |
| `data/crops/` (+`index.csv`, `crops_meta.json`) | Oberkörper-Crops je Track | Plan 02.2-08 |
| `data/continuity_review.csv` | Human-Urteile pass/fail je Clip (61/61 vollständig) | Plan 02.2-03 |
| `data/flag_pull_events.csv` | Flag-Pull-Ereignisse je Clip (Bonus) | Plan 02.2-03 |
| `data/gt_positions.csv` | Hand-markierte Fußpositionen | Phase 2.1 |
| `data/homography_calibration.csv` | Landmarken je Hover-Position | Phase 2.1 |
| `README.md` | vom Builder generiert, deutsch | — |
| `manifest.json` | Datei-für-Datei-Hashes + Gesamt-Content-Hash | — |

**Bekannte Lücke:** Radar-Renderings (Top-Down-Feldansicht) sind noch nicht Teil dieses
Bundles — nur die Overlay-Videos. Kein Pro-Clip-Radar-Rendering-Lauf existiert bisher;
`cv/radar.py` kann das technisch, ein solcher Lauf war nicht Teil dieses Plans. Wird
nachgereicht.

### Verzeichnisstruktur (im Archiv)

```
dev-set/
  README.md
  manifest.json
  data/
    clips/clip_001.mp4 … (61 Dateien)
    overlays/clip_001.mp4 … (61 Dateien)
    detections.parquet
    tracks.parquet
    crops/clip_NNN/track_YYYY/frame_ZZZZZ.jpg …, index.csv, crops_meta.json
    continuity_review.csv
    flag_pull_events.csv
    gt_positions.csv
    homography_calibration.csv
```

### Schemas

- **`detections.parquet`**: `cv.schema.DETECTION_COLUMNS` (session_id, clip_number,
  frame_index, timestamp_s, det_index, class_name, confidence, bbox_x1..y2,
  detector_run_id, detected_at).
- **`tracks.parquet`**: `cv.schema.TRACKING_COLUMNS` (session_id, clip_number,
  frame_index, timestamp_s, track_id, class_name, confidence, bbox_x1..y2, foot_x_px,
  foot_y_px, team_id, hover_position_id, x_yards, y_yards, game_id, play_id,
  detector_run_id, tracked_at).
- **`crops/index.csv`**: session_id, clip_number, track_id, frame_index, team_id,
  class_name, file.
- **`continuity_review.csv`** / **`flag_pull_events.csv`**: siehe
  `docs/hackathon-benchmark-labels.md` für das vollständige Vokabular.

### Reproduktionskommando

```
uv run --extra cv ffep cv bundle --kind dev --out data/bundles
```

Zwei Läufe desselben Freeze-Pins über unveränderte Eingaben liefern denselben
`content_sha256` (verifiziert in `tests/test_cv_bundle.py`, u. a.
`test_build_bundle_content_hash_deterministic_across_two_builds`).

### Baseline-Zahl dieses Bundles (alle 61 Clips)

**Kontinuität (BoT-SORT-Baseline, menschlich bewertet): 15/61 = 24,59 %.** Reproduziert
über:

```
uv run python scripts/hackathon/score_tracks.py \
  --tracks data/bundles/dev-set/data/tracks.parquet \
  --review data/bundles/dev-set/data/continuity_review.csv
```

Ausgabe: `Referenz-Baseline (Human-Urteile, aus --review): 15/61 (24.59%)` — exakt die oben
genannte Zahl, und exakt dieselbe Zahl (dieselbe 61-Clip-Vollspiel-Population, derselbe
Denominator) wie in `docs/hackathon-challenge-reid.md` §Baseline-Zahlen. Seit Plan 02.2-21
gibt es keinen kleineren Pool-only-Denominator mehr — die vormalige 43-Clip-Zahl (10/43 =
23,26 %) galt nur, solange 18 Pilotspiel-Clips als Test-Set-Fallback aus dem Dev-Bundle
zurückgehalten wurden.

### Delivery-Regeln

- Bereitstellung über Open Telekom Cloud OBS (D-08), sobald Plan 02.2-14 den Bucket
  provisioniert — bis dahin bleibt das Archiv lokal unter `data/bundles/`.
- Keine anderweitige Verteilung (kein Cloud-Upload durch Teams, keine Weitergabe außerhalb
  des Hackathon-Kontexts).
- Löschung/Rückgabe nach dem Event (Verbandsfreigabe vom 2026-08-31,
  `docs/capture-legal.md`).
- Bewertung ausschließlich über `scripts/hackathon/score_tracks.py`, damit alle Teams
  dieselbe Zahl messen.

### Verifikation

```
uv run pytest tests/test_cv_bundle.py -q
```

Alle 61 Clip-Dateinamen im Archiv sind exakt die `hackathon_role = dev`-Zeilen von
`data/reference/hackathon_split.csv` für die Session
`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE` (geprüft per `unzip -l`); keine Clip-Nummer
des Puerto-Rico-Testspiels erscheint im Dev-Archiv (verschiedene Session-IDs, keine
Namenskollision möglich).

---

## Test-Set (privat, fertig)

**Archiv:** `data/bundles/test-set_2026-09-07_b455b642b951.zip` (~2,48 GB, 126 Dateien,
gitignored unter `data/bundles/`).

**Content-Hash (`content_sha256`, aus `manifest.json`):**
`b455b642b95144598c9c15ee3dc2d84892d687a19b733983790565b3a547c4e5`

**Eingefrorener Detektor-Lauf:** `87a8a5222f7a472787875e974d089c44` (derselbe Freeze-Pin
wie das Dev-Set).

**Zweites Drohnenspiel:** eingetroffen und registriert am 2026-09-02
(`2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE`, GER vs. Puerto Rico, 16.05.2026, 61
WIDE-Clips). Plan 02.2-21 löst den bisherigen D-07-Fallback damit ab: das Test-Set ist ab
sofort dieses ANDERE Spiel, nicht mehr eine Clip-Teilmenge des Pilotspiels. Dev und Test
sind seit diesem Plan durch das SPIEL getrennt (DATA-04), nicht durch eine
Clip-Zurückhaltung innerhalb desselben Spiels — Gegner, Trikotsatz, Lichtverhältnisse und
Hover-Geometrie unterscheiden sich jetzt zwischen den beiden Sets, wodurch ein auf
Erscheinungsbild memorisiertes Modell nicht mehr als "generalisiert" durchgeht (siehe
`## Zweites Drohnenspiel — gelöst 2026-09-02/07` unten für die volle Begründung).

### Inhalt

Alle Clips mit `hackathon_role = private_test` in `data/reference/hackathon_split.csv` —
**61 von 61** Drohnen-Clips der Puerto-Rico-Session, Clip-Nummern 1–56 und 59–63 (NICHT
durchgehend 1..61 — nie eine fest codierte Liste, sondern immer aus der Split-Datei
aufgelöst, `cv/bundle.py::_test_clip_numbers`).

| Artefakt | Inhalt | Herkunft |
|---|---|---|
| `data/clips/clip_NNN.mp4` | 61 rohe Drohnen-Clips | `data/video/…` (Plan 02.0) |
| `data/overlays/clip_NNN.mp4` | Boxen + Track-Nummern zur Sichtprüfung | Plan 02.1-14 |
| `data/detections.parquet` | Pro-Frame-Detektionen des eingefrorenen Detektors | Plan 02.2-21 |
| `data/tracks.parquet` | Baseline-Tracks (BoT-SORT), Team-Zuordnung | Plan 02.2-21 |
| `README.md` | vom Builder generiert, deutsch | — |
| `manifest.json` | Datei-für-Datei-Hashes + Gesamt-Content-Hash | — |

**Was bewusst fehlt:** keine `continuity_review.csv`, keine `flag_pull_events.csv`, keine
`gt_positions.csv`, keine Crops, kein `homography_calibration.csv`. Kontinuitäts- und
Flag-Pull-Urteile für genau diese 61 Clips sind die Endwertungs-Grundlage und dürfen
niemals mit dem Bundle mitgehen; sie liegen stattdessen im lokalen, nicht versionierten
Label-Tresor (siehe `### Label-Tresor` unten). Crops sind nicht Teil des Test-Sets
(Re-ID-Trainingsmaterial kommt ausschließlich aus dem Dev-Set — Teams entwickeln ihr
Erscheinungsmodell dort, das Test-Set dient nur der Endwertung).
`homography_calibration.csv` fehlt aus einem anderen Grund als die Labels: die
Kalibrierung ist per Hover-Position des PILOTEN-Spiels gemessen und wäre für dieses andere
Spiel falsche Daten. Entsprechend werden für Puerto Rico keine Feldkoordinaten
(`x_yards`/`y_yards`) erzeugt — Kontinuitäts- und Flag-Pull-Wertung bleiben für dieses
Set im Pixel-Raum, und die Flag-Pull-Bonusmetrik fällt auf ihr dokumentiertes
zeit-only-Fenster zurück (`scripts/hackathon/score_tracks.py` druckt den
"ortsblinden"-Hinweis bereits, wenn `x_yards`/`y_yards` fehlen). Festgehalten als
bekannte, bewusste Lücke, nicht als stille Auslassung.

### Verzeichnisstruktur (im Archiv)

```
test-set/
  README.md
  manifest.json
  data/
    clips/clip_001.mp4 … (61 Dateien, Nummern 1-56 und 59-63)
    overlays/clip_001.mp4 … (61 Dateien)
    detections.parquet
    tracks.parquet
```

### Schemas

Identisch zum Dev-Set: `detections.parquet` folgt `cv.schema.DETECTION_COLUMNS`,
`tracks.parquet` folgt `cv.schema.TRACKING_COLUMNS` (siehe `## Dev-Set` oben).

### Label-Tresor (nicht Teil des Bundles, nicht in git)

Die Kontinuitäts- und Flag-Pull-Urteile für genau die 61 Test-Set-Clips liegen unter
`data/private/test-labels/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE/`
(`continuity_review.csv`, `flag_pull_events.csv`, je 61 Datenzeilen) — lokal, gitignored
(`data/private/*` in `.gitignore`, T-2.2-28), für die Endwertung nach dem Event. Seit Plan
02.2-21 ist der Tresor per Session unterverzeichnet (nicht mehr zwei flache Dateien) und
wird vom NUTZER von Hand befüllt, nicht mehr aus einer öffentlichen Tabelle abgeleitet
(`cv/bundle.py::_vault_withheld_labels`, das die Panama-Rojo-Zurückhaltung schrieb, ist
entfernt). Skelette (automatische Spalten vorbefüllt, Human-Spalten leer) erzeugt
`ffep cv test-labels --session <id> --tracks <pfad>`; ein `--kind test`-Build VERIFIZIERT
den Tresor nur noch (`cv/bundle.py::_assert_test_labels_vaulted`, ruft
`cv.testset.validate_test_labels`) und schlägt fehl, wenn er fehlt, unvollständig ist oder
das falsche Clip-Set abdeckt — er schreibt ihn nicht mehr. Gemessene Baseline auf dem
Test-Set (Plan 02.2-21, 2026-09-07): **Kontinuität (BoT-SORT-Baseline, menschlich
bewertet) 12/61 = 19,67 %.**

### Leak-Schutz (T-2.2-28)

Zwei unabhängige Prüfungen, keine allein:

1. **Namensbasiert, vor der Zusammenstellung:** `_assert_no_test_kind_label_leak` verweigert
   jeden Bau, dessen Content-Tabelle eine Datei mit dem Namen `continuity_review.csv` oder
   `flag_pull_events.csv` enthält.
2. **Spaltenbasiert, nach der Zusammenstellung:** `_assert_no_label_leak_in_tree` liest jede
   CSV-/Parquet-Datei im fertig zusammengestellten Baum selbst ein und verweigert den Bau,
   sobald eine Spalte namens `verdict`, `id_switches`, `reviewer_note`, `pull_time_s`,
   `carrier_track_id` oder `puller_track_id` auftaucht — unabhängig vom Dateinamen. Eine
   umbenannte Label-Datei (z. B. `notes.csv`) würde die reine Namensprüfung durchrutschen;
   die Spaltenprüfung fängt sie trotzdem
   (`tests/test_cv_bundle.py::test_assert_no_label_leak_in_tree_catches_renamed_label_file`).

Eine dritte, vorgelagerte Prüfung seit Plan 02.2-21: **`_assert_test_labels_vaulted`**
verifiziert vor jedem `--kind test`-Build, dass der Tresor für die aktuelle Test-Session
existiert, vollständig ist (61/61 Urteile und 61/61 Outcomes) und exakt das von
`hackathon_split.csv` deklarierte Clip-Set abdeckt — ein Build bricht ab, statt mit einem
leeren oder falschen Tresor fortzufahren. Das ist keine Leak-Prüfung im selben Sinn wie die
beiden obigen (sie prüft Vollständigkeit/Korrektheit, nicht Auslecken), schließt aber die
verwandte Gefahr, versehentlich gegen das falsche Spiel zu werten.

Verifiziert für das reale Archiv: `unzip`+`grep` über alle CSV-Dateien und
`pl.read_parquet_schema` über alle Parquet-Dateien im entpackten Archiv finden `verdict` in
keiner Datei (siehe `### Verifikation` unten).

### Reproduktionskommando

```
uv run --extra cv ffep cv bundle --kind test --out data/bundles
```

### Delivery-Regeln

Wie das Dev-Set (siehe oben), zusätzlich: **nur für die Endwertung**, nicht zum Tuning
freigeben. Bewertung ausschließlich mit `scripts/hackathon/score_tracks.py` gegen die
vertraulich gehaltenen Urteile im Label-Tresor.

### Verifikation

```
uv run pytest tests/test_cv_bundle.py -q
```

Entpackt und geprüft (Plan 02.2-21, 2026-09-07): `grep -r verdict` über alle CSV-Dateien im
entpackten Archiv liefert keinen Treffer (es gibt ohnehin keine CSV-Datei im Archiv);
`pl.read_parquet_schema` über `detections.parquet`/`tracks.parquet` enthält keine der
sechs Leak-Spalten; kein `homography_calibration.csv` im entpackten Baum; die 61
Clip-Nummern im Archiv (1–56, 59–63) sind exakt die `hackathon_role = private_test`-Zeilen
von `data/reference/hackathon_split.csv`; der Label-Tresor enthält exakt dieselben 61
Clip-Nummern, validiert über `ffep cv test-labels --session
2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE --validate` (61/61 Urteile, 61/61
Outcomes).

---

## Transfer-Set (fertig)

**Archiv:** `data/bundles/transfer-set_2026-09-01_82c955898fe4.zip` (~543 MB, 115 Dateien,
gitignored unter `data/bundles/`).

**Content-Hash (`content_sha256`, aus `manifest.json`):**
`82c955898fe4fddac50557fac6e11537783b62e19156c1d5819cd5eba853bdc1`

**Eingefrorener Detektor-Lauf:** `87a8a5222f7a472787875e974d089c44` (derselbe Freeze-Pin
wie Dev-Set und Test-Set).

### Inhalt

Zwei Domänen, alle Clips (keine Pool-/Test-Aufteilung — das Transfer-Set ist nicht Teil der
D-07-Zurückhaltung, siehe `docs/hackathon-challenge-reid.md` §Benchmark-Design):

| Domäne | Session | Clips | Detektionen (Zeilen) | Herkunft |
|---|---|---:|---:|---|
| `sideline` (GoPro) | `2026-08-14_WC-GER-vs-MEX-GOPRO` | 60 | 155.732 | WM GER–MEX |
| `broadcast` (TV) | `2026-08-14_WC-USA-vs-AUS-TV` | 51 | 275.539 | WM USA–AUS |

Beide Detektions-Läufe wurden für diesen Plan frisch erzeugt
(`ffep cv detections --domain sideline|broadcast`), pro Domäne mit den Einstellungen aus
`docs/dataset-plan.md ## 4` (siehe Tabelle unten) statt der Drohnen-Defaults blind
zu übernehmen — auch wenn beide numerisch identisch mit dem bereits in `ffep.toml`
eingetragenen Wert sind, war das eine gemessene Koinzidenz, kein Automatismus (siehe
`docs/material-sighting.md`). Beide Läufe tragen `detector_run_id =
87a8a5222f7a472787875e974d089c44`, geprüft gegen den Freeze-Pin vor dem Bundle-Bau
(T-2.2-24).

**Domänen-Details (gemessen, aus `docs/material-sighting.md`):**

| Domäne | p50 (px) | p10 (px) | Stufe | `resolution` | `sahi` |
|---|---:|---:|---|---:|---|
| Seitenkamera (`sideline`) | 27,0 | 16,5 | Brauchbar | 896 | false |
| Broadcast (`broadcast`) | 23,0 | 14,0 | Brauchbar | 896 | false |

Beide Domänen landen im selben 20–40-px-Band wie die Piloten-Drohnensession (p50 = 30,0 px)
und wurden technisch durchgehend als `Brauchbar` gesichtet (kein Clip fällt unter die
20-px-Schwelle in beiden Domänen, siehe `docs/material-sighting.md ## Tier-Verteilung`).

| Artefakt | Inhalt |
|---|---|
| `data/sideline/clips/clip_NNN.mp4` | 60 GoPro-Seitenlinien-Clips |
| `data/sideline/detections.parquet` | Pro-Frame-Detektionen, sideline-Einstellungen |
| `data/broadcast/clips/clip_NNN.mp4` | 51 TV-Ausschnitte |
| `data/broadcast/detections.parquet` | Pro-Frame-Detektionen, broadcast-Einstellungen |
| `README.md` | vom Builder generiert, deutsch |
| `manifest.json` | Datei-für-Datei-Hashes + Gesamt-Content-Hash |

**Was bewusst fehlt:** keine Baseline-Tracks, keine Overlays, keine Kontinuitäts-Urteile.
`docs/hackathon-challenge-reid.md` §Benchmark-Design nennt für dieses Set ausdrücklich nur
"Kontinuitäts-Urteile auf einer Stichprobe (optional, falls Zeit)" — nicht Teil dieses
Bundles, mögliche spätere Ergänzung.

### Reproduktionskommando

```
uv run --extra cv ffep cv detections --session 2026-08-14_WC-GER-vs-MEX-GOPRO --domain sideline \
  --out data/labels/2026-08-14_WC-GER-vs-MEX-GOPRO/bundle-inputs/detections.parquet
uv run --extra cv ffep cv detections --session 2026-08-14_WC-USA-vs-AUS-TV --domain broadcast \
  --out data/labels/2026-08-14_WC-USA-vs-AUS-TV/bundle-inputs/detections.parquet
uv run --extra cv ffep cv bundle --kind transfer --out data/bundles
```

Gemessene Laufzeit auf der Primärmaschine (Apple M5 Max, `mps`-Backend, automatische
Geräteauswahl durch `RFDETRSmall`): sideline (60 Clips, ~798 s Rohmaterial) ~10 min,
broadcast (51 Clips, ~633 s Rohmaterial) ~9 min — beide zusammen ~19 min, deutlich unter der
C-09-Laufzeitschwelle von einer Stunde pro Spiel.

### Delivery-Regeln

Wie das Dev-Set (siehe oben). Verbandsfreigabe vom 2026-08-31 deckt ausdrücklich auch die
Weitergabe des TV-Sendematerials ab (`docs/capture-legal.md ## Nachtrag 2026-08-31`).

### Verifikation

```
uv run pytest tests/test_cv_bundle.py -q
```

Clip-Zahlen im Archiv geprüft: 60 `sideline/clips/*.mp4`, 51 `broadcast/clips/*.mp4`.
`detections.parquet` je Domäne enthält ausschließlich Clip-Nummern der jeweiligen Domäne
und trägt durchgehend den eingefrorenen `detector_run_id`.

---

## Zwei neue Referenztabellen (Plan 02.2-21)

Der Hackathon-Rollen-Split ist seit Plan 02.2-21 von Grund auf von unserem eigenen
Detektor-Eval-Split getrennt — zwei Tabellen mit zwei unterschiedlichen Aufgaben:

| Datei | Aufgabe | Zeilen |
|---|---|---|
| `data/reference/frozen_eval_clips.csv` | **UNVERÄNDERT.** Steuert nur unser eigenes Detektor-Training/-Eval: `role = pool` ist der AL-Kandidatenpool, `role = frozen_eval` misst mAP (Pläne 02.2-15/18). Ihre `private_test`-Spalte ist als Hackathon-Signal seit diesem Plan SUPERSEDIERT und wird von `cv/bundle.py` nicht mehr gelesen. | 61 (Pilotspiel) |
| `data/reference/hackathon_split.csv` | **NEU.** Steuert nur die Hackathon-Bundles: Spalten `domain,session_id,clip_number,hackathon_role,frozen_at,note`; `hackathon_role` ∈ {`dev`, `private_test`}. | 122 (61 dev + 61 private_test) |
| `data/reference/al_excluded_sessions.csv` | **NEU.** Session-weiter Trainingspool-Ausschluss (nicht clip-weise, weil Clip-Nummern zwischen den beiden Drohnenspielen kollidieren): eine Zeile, die Puerto-Rico-Session, `requirement = DATA-04`. `cv/active_learning.py::select_al_frames` liest sie vor jedem Detektor-Load und verweigert eine ausgeschlossene Session. | 1 |

Reproduktionskommando für beide neuen Tabellen (nie von Hand editieren):

```
uv run --extra cv ffep cv hackathon-split --test-session 2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE
```

## Zweites Drohnenspiel — gelöst 2026-09-02/07

Der Fallback aus `## Zweites Drohnenspiel — Stand 2026-09-01` (18 zurückgehaltene
Pilotspiel-Clips als D-07-Behelfslösung) ist abgelöst. Das echte zweite Drohnenspiel
(`2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE`, GER vs. Puerto Rico, 16.05.2026, 61
WIDE-Clips) traf am **2026-09-02** ein und wurde gegen `data/reference/video_inventory.csv`
verifiziert. Plan 02.2-21 hat daraufhin (2026-09-02 Split-Entscheidung, 2026-09-07
Pipeline-Lauf + Label-Vault + Bundle-Rebuild):

1. den Hackathon-Split neu geschnitten (Dev = alle 61 Pilotspiel-Clips, Test =
   alle 61 Puerto-Rico-Clips) — Dev und Test sind jetzt durch das SPIEL getrennt
   (DATA-04), nicht mehr durch eine Clip-Zurückhaltung innerhalb desselben Spiels;
2. die Puerto-Rico-Session dauerhaft von der aktiven-Lern-Trainingspool ausgeschlossen
   (`al_excluded_sessions.csv`);
3. den eingefrorenen Detektor, BoT-SORT-Baseline-Tracking und Overlays für alle 61
   Puerto-Rico-Clips erzeugt;
4. die menschlichen Kontinuitäts- und Flag-Pull-Urteile für alle 61 Clips im
   gitignored Label-Tresor erhoben und validiert (12/61 = 19,67 % Kontinuitäts-Baseline);
5. beide Bundles (Dev, Test) neu gebaut und gehasht (siehe oben).

`data/reference/frozen_eval_clips.csv` — unser eigener Detektor-Eval-Split — bleibt davon
unberührt: diese Datei war nie die Quelle der Wahrheit für die Hackathon-Rollen, das war
schon vor diesem Plan eine implizite Vermischung zweier unterschiedlicher Aufgaben, die
`hackathon_split.csv` jetzt explizit auflöst.

---

## Auslieferung — Stand 2026-09-07 (Plan 02.2-14)

**Status: `deliver_bundle` implementiert und getestet (`src/flag_football_ep/cv/bundle.py`,
S3-kompatibel über `s3fs`/`dvc-s3`, Zugangsdaten ausschließlich über `config.secret()`
aufgelöst, T-2.2-42). Der eigentliche Upload in die Open Telekom Cloud steht noch aus —
`OTC_OBS_ACCESS_KEY_ID`/`OTC_OBS_SECRET_ACCESS_KEY` liegen in dieser Umgebung nicht vor
und werden es voraussichtlich für längere Zeit nicht. Alle drei Bundles sind stattdessen
lokal vollständig für die Auslieferung vorbereitet ("gestaged"), ohne Netzwerkzugriff.**

### Lokale Staging (bereits erledigt)

```
uv run --extra cv ffep cv stage-delivery --bundles-dir data/bundles
```

Erzeugt `data/processed/hackathon-delivery/<datum>/` (gitignored) mit einem Hardlink auf
jedes der drei Archive, einem `manifest.json` (Größe + voller SHA-256 je Archiv, plus dem
geplanten Objekt-Schlüssel) und einem deutschen `README.md` für die Teams. Kein
zusätzlicher Plattenplatz für die mehrere-GB-Archive (`os.link`, Fallback: Kopie über
Dateisystemgrenzen hinweg). Für die "test"-Art wird die Datei-Namensliste des gestageten
Archivs zusätzlich gegen die vier Label-/GT-/Homographie-Dateinamen geprüft
(defense-in-depth zu `build_bundle`s eigenem Leak-Schutz, siehe `### Leak-Schutz` oben).

### Objekt-Schlüssel-Schema

`<bucket-prefix>/<kind>-set/<archiv-dateiname>` — deterministisch aus dem Archivnamen,
nie ein literaler Pfad je Bundle-Art. Konkret (Bucket-Präfix noch der Platzhalter
`ffep-datasets-PLACEHOLDER`, siehe unten):

| Bundle | Objekt-Schlüssel |
|---|---|
| Dev-Set | `flag-football-datasets/dev-set/dev-set_2026-09-07_08a55bd95b06.zip` |
| Test-Set | `flag-football-datasets/test-set/test-set_2026-09-07_b455b642b951.zip` |
| Transfer-Set | `flag-football-datasets/transfer-set/transfer-set_2026-09-01_82c955898fe4.zip` |

### Sobald Zugangsdaten vorliegen

Vollständiger, ausführbarer Ablauf (beide Wege: Projekt-CLI und AWS-CLI/obsutil-Fallback,
inklusive Post-Upload-Verifikation und was danach an die Teams geht):
**`docs/hackathon-otc-upload.md`**. Kurzfassung des Projekt-Wegs:

```
# Beide Zugangsdaten-Variablen zuvor exportiert (Namen und der volle Befehl in
# docs/hackathon-otc-upload.md, niemals ein Wert in diesem Dokument):
uv run --extra cv --extra versioning ffep cv deliver \
  --archive data/processed/hackathon-delivery/<datum>/dev-set/<archiv>.zip \
  --remote s3://<BUCKET>/flag-football-datasets
```

`deliver_bundle` verifiziert nach jedem Upload die Objekt-Größe gegen die lokale Datei und
bricht mit `BundleError` ab, statt eine unvollständige Übertragung stillschweigend als
Erfolg zu melden. Kein Zugangsdatenwert erscheint jemals in Log, Fehlermeldung oder
Rückgabewert.

### Verifikation für Teilnehmende

Nach dem Download: `sha256sum <archiv>.zip` muss exakt dem vollen SHA-256 entsprechen, der
zum jeweiligen Bundle veröffentlicht wird (aus dem Staging-`manifest.json`s Feld
`archive_sha256` — der Hash der ZIP-Datei selbst, nicht zu verwechseln mit dem kürzeren,
den Inhalt hashenden `content_sha256` weiter oben in diesem Dokument).

### Zugriffsregeln

- Zugriff nur für registrierte Hackathon-Teams, zweckgebunden (Verbandsfreigabe vom
  2026-08-31, `docs/capture-legal.md`); keine Weitergabe außerhalb des Event-Kontexts.
- Die privaten Test-Set-Labels (`continuity_review.csv`, `flag_pull_events.csv` für die
  Puerto-Rico-Session) sind in KEINEM Objekt enthalten — sie verlassen die lokale Maschine
  nie (siehe `### Label-Tresor` oben).
- Der Bucket muss vor dem ersten Upload als privat bestätigt sein (kein öffentliches
  Lese-/Schreibrecht) — Teil des Runbooks in `docs/hackathon-otc-upload.md`.
- Löschung/Rückgabe nach dem Event.

---

*Zuletzt aktualisiert: 2026-09-07 (Pläne 02.2-21, 02.2-14)*
