# Hackathon-Bundles — Inhalt, Aufbau, Reproduktion (Stand: 2026-09-01)

**Status: Alle drei Bundles gebaut und gehasht (Dev-Set Plan 02.2-10; Test-Set und
Transfer-Set Plan 02.2-12, 2026-09-01). Kein zweites Drohnenspiel lag zum Build-Zeitpunkt
vor — das Test-Set nutzt den D-07-Fallback (18 zurückgehaltene Pilotspiel-Clips).**

Dieses Dokument beschreibt, was jedes `ffep cv bundle --kind <kind>`-Archiv enthält, wie es
aufgebaut ist, wie es reproduziert wird und unter welchen Regeln es an die Hackathon-Teams
geht (D-08/D-09). Der Builder selbst ist `src/flag_football_ep/cv/bundle.py::build_bundle`
(Plan 02.2-10); die Scoring-Referenz ist `scripts/hackathon/score_tracks.py` (Plan 02.2-10).

---

## Dev-Set (öffentlich, fertig)

**Archiv:** `data/bundles/dev-set_2026-09-01_ad412c5cffb9.zip` (~1,5 GB, 11.834 Dateien,
gitignored unter `data/bundles/`).

**Content-Hash (`content_sha256`, aus `manifest.json`):**
`ad412c5cffb9a9b54a49e4e7c0b3075c0e013304cf8d1b7fb45d7ed5e5db2a56`

**Eingefrorener Detektor-Lauf:** `87a8a5222f7a472787875e974d089c44` (aus
`data/reference/hackathon_freeze.json`, Dataset-Hash
`ab3a9673d61bc348d37ce298ba12d18b76395d1ade82a735c5b3d82d2e46aec0`).

### Inhalt

Nur die Clips mit `role = pool` in `data/reference/frozen_eval_clips.csv` — **43 von 61**
Drohnen-Clips des Pilotspiels GER vs. Panama Rojo (16.05.2026). Die restlichen 18 Clips
(`role = frozen_eval`, `private_test = true`) sind das private Hackathon-Testset (D-07) und
erscheinen in KEINEM öffentlichen Bundle. Verifiziert (siehe `## Verifikation` unten).

| Artefakt | Inhalt | Herkunft |
|---|---|---|
| `data/clips/clip_NNN.mp4` | 43 rohe Drohnen-Clips | `data/video/…` (Plan 02.0) |
| `data/overlays/clip_NNN.mp4` | Boxen + Track-Nummern zur Sichtprüfung | Plan 02.1-14 |
| `data/detections.parquet` | Pro-Frame-Detektionen des eingefrorenen Detektors | Plan 02.2-08 |
| `data/tracks.parquet` | Baseline-Tracks (BoT-SORT), Team-Zuordnung, Feldkoordinaten | Phase 2.1 |
| `data/crops/` (+`index.csv`, `crops_meta.json`) | Oberkörper-Crops je Track | Plan 02.2-08 |
| `data/continuity_review.csv` | Human-Urteile pass/fail je Clip | Plan 02.2-03 |
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
    clips/clip_001.mp4 … (43 Dateien)
    overlays/clip_001.mp4 … (43 Dateien)
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

### Baseline-Zahl dieses Bundles (nur die 43 Pool-Clips)

**Kontinuität (BoT-SORT-Baseline, menschlich bewertet): 10/43 = 23,26 %.** Reproduziert
über:

```
uv run python scripts/hackathon/score_tracks.py \
  --tracks data/bundles/dev-set/data/tracks.parquet \
  --review data/bundles/dev-set/data/continuity_review.csv
```

Ausgabe: `Referenz-Baseline (Human-Urteile, aus --review): 10/43 (23.26%)` — exakt die oben
genannte Zahl. Diese Pool-only-Zahl ist NICHT identisch mit der Vollspiel-Zahl in
`docs/hackathon-challenge-reid.md` §Baseline-Zahlen (dort über alle 61 Clips inklusive der
18 privaten Testset-Clips: 15/61 = 24,59 %) — beide sind korrekt, sie messen über
unterschiedliche Denominatoren (43 vs. 61 Clips) aus demselben Grund: das private Testset
ist im Dev-Bundle nicht enthalten.

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

Kein Clip-Dateiname der 18 `private_test = true`-Clips erscheint im Archiv (geprüft per
`unzip -l` gegen `data/reference/frozen_eval_clips.csv`s `private_test = true`-Zeilen für
die Session `2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE`).

---

## Test-Set (privat, fertig)

**Archiv:** `data/bundles/test-set_2026-09-01_448c681c6e5c.zip` (~607 MB, 41 Dateien,
gitignored unter `data/bundles/`).

**Content-Hash (`content_sha256`, aus `manifest.json`):**
`448c681c6e5c6945e67e78d3007642dc32e2fc42990525ab87fe51d0ec342811`

**Eingefrorener Detektor-Lauf:** `87a8a5222f7a472787875e974d089c44` (derselbe Freeze-Pin
wie das Dev-Set).

**Zweites Drohnenspiel:** lag zum Build-Zeitpunkt (2026-09-01) nicht vor. Das Test-Set
nutzt daher den in D-07 festgelegten Fallback: die 18 `private_test = true`-Clips des
Pilotspiels GER vs. Panama Rojo, dieselben, die seit Plan 02.2-06 aus dem Dev-Set
ausgeschlossen sind. Sollte ein zweites Drohnenspiel später eintreffen, ersetzt es diesen
Fallback; die 18 Clips würden dann in den Pool zurückfallen (`data/reference/
frozen_eval_clips.csv` neu einfrieren) und das Test-Set aus dem neuen Spiel neu gebaut.

### Inhalt

Die 18 Clips mit `private_test = true` in `data/reference/frozen_eval_clips.csv`
(Clip-Nummern 5, 6, 7, 11, 15, 16, 21, 22, 28, 33, 36, 40, 43, 49, 52, 54, 55, 56) — nie
eine fest codierte Liste, sondern immer aus der eingefrorenen Split-Datei aufgelöst
(`cv/bundle.py::_private_test_clip_numbers`).

| Artefakt | Inhalt | Herkunft |
|---|---|---|
| `data/clips/clip_NNN.mp4` | 18 rohe Drohnen-Clips | `data/video/…` (Plan 02.0) |
| `data/overlays/clip_NNN.mp4` | Boxen + Track-Nummern zur Sichtprüfung | Plan 02.1-14 |
| `data/detections.parquet` | Pro-Frame-Detektionen des eingefrorenen Detektors | Plan 02.2-08 |
| `data/tracks.parquet` | Baseline-Tracks (BoT-SORT), Team-Zuordnung, Feldkoordinaten | Phase 2.1 |
| `data/homography_calibration.csv` | Landmarken je Hover-Position | Phase 2.1 |
| `README.md` | vom Builder generiert, deutsch | — |
| `manifest.json` | Datei-für-Datei-Hashes + Gesamt-Content-Hash | — |

**Was bewusst fehlt:** keine `continuity_review.csv`, keine `flag_pull_events.csv`, keine
`gt_positions.csv`, keine Crops. Kontinuitäts- und Flag-Pull-Urteile für genau diese 18
Clips sind die Endwertungs-Grundlage und dürfen niemals mit dem Bundle mitgehen; sie liegen
stattdessen im lokalen, nicht versionierten Label-Tresor (siehe `### Label-Tresor` unten).
Ground-Truth-Fußpositionen für diese Clips existieren in keinem Bundle, aus demselben Grund
wie die Kontinuitäts-Urteile: sie wären genau das, was eine Positions-Genauigkeits-Wertung
bräuchte. Crops sind nicht Teil des Test-Sets (Re-ID-Trainingsmaterial kommt ausschließlich
aus dem Dev-Set — Teams entwickeln ihr Erscheinungsmodell dort, das Test-Set dient nur der
Endwertung).

### Verzeichnisstruktur (im Archiv)

```
test-set/
  README.md
  manifest.json
  data/
    clips/clip_005.mp4 … (18 Dateien)
    overlays/clip_005.mp4 … (18 Dateien)
    detections.parquet
    tracks.parquet
    homography_calibration.csv
```

### Schemas

Identisch zum Dev-Set: `detections.parquet` folgt `cv.schema.DETECTION_COLUMNS`,
`tracks.parquet` folgt `cv.schema.TRACKING_COLUMNS` (siehe `## Dev-Set` oben).

### Label-Tresor (nicht Teil des Bundles, nicht in git)

Die zurückgehaltenen Kontinuitäts- und Flag-Pull-Zeilen für genau die 18 Test-Set-Clips
liegen unter `data/private/test-labels/` (`continuity_review.csv`, `flag_pull_events.csv`,
je 18 Datenzeilen) — lokal, gitignored (`data/private/*` in `.gitignore`, T-2.2-28), für
die Endwertung nach dem Event. Geschrieben von `cv/bundle.py::_vault_withheld_labels` bei
jedem `--kind test`-Build, atomar, außerhalb jedes Bundle-Baums.

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

Entpackt und geprüft: `grep -r verdict` über alle CSV-Dateien im entpackten Archiv liefert
keinen Treffer; `pl.read_parquet_schema` über `detections.parquet`/`tracks.parquet` enthält
keine der sechs Leak-Spalten; die 18 Clip-Nummern im Archiv sind exakt die
`private_test = true`-Zeilen; der Label-Tresor enthält exakt dieselben 18 Clip-Nummern und
keine Pool-Clip-Zeile.

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

## Zweites Drohnenspiel — Stand 2026-09-01

Kein zweites Drohnenspiel ist zum Build-Zeitpunkt dieses Dokuments eingetroffen. Das
Test-Set nutzt daher, wie in D-07 als Fallback vorgesehen, die 18 zurückgehaltenen Clips des
Pilotspiels. Diese Entscheidung ist nicht endgültig: sollte vor dem Hackathon (23.–27.
November 2026) ein zweites Drohnenspiel des Teams zustande kommen, wird das Test-Set daraus
neu gebaut und die 18 Pilotspiel-Clips fallen in den Dev-Pool zurück (`data/reference/
frozen_eval_clips.csv` wird für dieses Szenario neu eingefroren, nicht von Hand editiert).

---

*Zuletzt aktualisiert: 2026-09-01 (Plan 02.2-12)*
