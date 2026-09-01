# Hackathon-Bundles — Inhalt, Aufbau, Reproduktion (Stand: 2026-09-01)

**Status: Dev-Set gebaut und gehasht. Test-Set und Transfer-Set stehen aus (Plan 02.2-12 —
zweites Drohnenspiel bzw. Seitenlinien-/TV-Detektionen existieren noch nicht).**

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

## Test-Set (privat) — AUSSTEHEND

Ein zweites Drohnenspiel (oder, falls keins zustande kommt, die 18 zurückgehaltenen Clips
des Pilotspiels, D-07) existiert noch nicht identisch aufbereitet. Plan 02.2-12 füllt diesen
Abschnitt. Der Leak-Schutz dafür ist bereits implementiert und getestet:
`build_bundle(config, "test", pin, out_dir)` verweigert jeden Bau, dessen Content-Tabelle
`continuity_review.csv` oder `flag_pull_events.csv` enthält (T-2.2-28,
`tests/test_cv_bundle.py::test_build_bundle_test_kind_refuses_label_bearing_file`).

## Transfer-Set — AUSSTEHEND

60 GoPro-Seitenlinien-Clips (WM GER–MEX) und 51 TV-Clips (WM USA–AUS) mit Detektionen unter
den jeweiligen Domänen-Inferenz-Einstellungen. Plan 02.2-12 füllt diesen Abschnitt.
`build_bundle(config, "transfer", …)` schlägt bis dahin bewusst mit `BundleError` fehl statt
ein leeres oder falsches Archiv zu bauen.

---

*Zuletzt aktualisiert: 2026-09-01 (Plan 02.2-10)*
