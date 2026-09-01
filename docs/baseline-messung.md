# Ehrliche Baseline: fertige Verfahren auf dem 61-Clip-Prüfsatz (Stand: 2026-09-01)

## Zweck und Geltungsbereich

Diese Messung deckt BASE-01 bis BASE-04 ab: BASE-01 verlangt, dass jedes verfügbare fertige
Tracking-/ReID-Verfahren einmal auf dem Pilot-Benchmark gemessen wird, statt eine einzelne
Baseline-Zahl aus der Erinnerung zu behaupten; BASE-02 verlangt, dass diese Zahlen in die
Dokumente wandern, die die Hackathon-Teams tatsächlich lesen; BASE-03 verlangt einen lauffähigen
Startbefehl pro Verfahren; BASE-04 verlangt eine begründete Entscheidung, ob die eingereichte
90-Prozent-Zielmarke gegen die gemessene Baseline noch sinnvoll ist. Klare Grenze: hier werden
**fertige Verfahren gemessen**, es wird **keins gebaut** — das Lösen der ReID-Aufgabe selbst ist
die Aufgabe der Hackathon-Teams, nicht dieses Projekts.

## Versuchsaufbau

Alle Messungen liefen unter identischen Bedingungen:

- **Dieselben eingefrorenen Detektionen:** `data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet`,
  `detector_run_id 87a8a5222f7a472787875e974d089c44`, 384.689 Zeilen, 61 Clips. Kein Verfahren
  bekommt eigene, neu erzeugte Detektionen — sonst wäre ein Unterschied in den Ergebnissen nicht
  dem Tracking-/ReID-Schritt zuzuschreiben.
- **Dasselbe unveränderte Wertungsskript:** `scripts/hackathon/score_tracks.py`, byte-identisch
  vor und nach dieser Messreihe (per Diff gegen den Stand vor Phase M2-2 geprüft).
- **Derselbe Prüfsatz:** `data/reference/continuity_review.csv` (Human-Urteile, 61/61 bewertet)
  und `data/reference/frozen_eval_clips.csv` (Voll-61/Dev-Pool-43-Split, `private_test`-Spalte).
- **Ergebnisdateien:** `data/reference/baseline-methods/summary.csv` (eine Zeile pro
  Verfahren/Konfiguration) und `data/reference/baseline-methods/per_clip.csv` (eine Zeile pro
  Verfahren/Konfiguration/Clip) — Letzteres ist der Eingang für die stetige Kennzahl in M2-4.
- **Hardware und Laufzeiten:** Apple M5 Max, motion-only-Tracking (ByteTrack, CBIoU, BoT-SORT-
  Rescoring) läuft auf CPU ohne Video-Dekodierung; die GTA-Erscheinungs-Einbettung nutzt MPS
  (torch 2.13.0, `mps available: True`). Laufzeiten stehen pro Verfahren in der Tabelle unten und
  im `runtime_s`-Feld von `summary.csv`.

## Wie die Zahlen zu lesen sind

1. `score_tracks.py` gibt zwei Werte aus. Die **automatische Kontinuität** (`auto_flag=ok`)
   reagiert auf die eingereichten Tracks — sie ist für jedes Verfahren unterschiedlich und leicht
   nachzumessen. Die **Referenz-Baseline aus Human-Urteilen** wird aus `--review` gelesen und ist
   für jede Einreichung im Prinzip dieselbe Zahl, weil sie an fest gerenderten Overlays gefällt
   wurde.
2. Daher: 15/61 (24,59 %) ist BoT-SORTs Wert, weil die menschlichen Urteile an BoT-SORT-Overlays
   gefällt wurden. Für jedes andere Verfahren steht in der Human-Urteile-Spalte `keine Review`,
   und das ist kein Versehen — es existiert schlicht keine menschliche Bewertung der Tracks dieser
   anderen Verfahren.
3. Die automatische Kennzahl ist als Vergleichsmaß schwach: BoT-SORT erreicht darauf 57/61 (93,44 %),
   während sein menschlicher Referenzwert bei 15/61 (24,59 %) liegt. Sie misst Abdeckung und
   Fragmentierung, nicht Identitätswechsel — genau den Fehlermodus, der 39 von 46 Fails ausmacht.
   Konsequenz: eine belastbare Rangfolge der Verfahren auf der Human-Skala verlangt entweder neue
   menschliche Urteile auf den Overlays des jeweiligen Verfahrens (M2-3) oder die stetige Kennzahl
   (M2-4, METR-01). Diese Messung ersetzt beides nicht und behauptet es auch nicht.
4. Die beschreibenden Statistiken (Median der Spielerinnen-Tracks pro Clip, Anteil der Clips im
   Idealband 10–14, Median der Fragmente) sind vergleichbar zwischen den Verfahren und stammen aus
   derselben Auswertung; sie sind Beschreibung, keine neue Zielmetrik.
5. Fairness der Konfigurationen: BoT-SORT lief hand-getunt (`lost_track_buffer=90`,
   `minimum_iou_threshold_first_assoc=0.1`, `minimum_consecutive_frames=5`, Plan 02.1-12), deshalb
   sind ByteTrack und CBIoU je einmal mit Bibliotheks-Defaults und einmal mit denselben Parametern
   gemessen — ein Vergleich nur gegen die hand-getunte BoT-SORT-Baseline, oder nur gegen
   ungetunte Bibliotheks-Defaults, wäre je einseitig.

## Verfahren und Messwerte

| Verfahren | Konfiguration | Automatische Kontinuität (voll 61) | Automatische Kontinuität (Dev-Pool 43) | Median Spielerinnen-Tracks/Clip | Clips im Idealband 10–14 | Human-Urteile | Lizenz | Laufzeit |
|---|---|---|---|---|---|---|---|---|
| BoT-SORT | bestehend, neu bewertet (nicht neu gelaufen) | 57/61 (93,44 %) | 40/43 (93,02 %) | 23,0 | 1/61 (1,64 %) | 15/61 (24,59 %) | Apache-2.0 (trackers 2.6.0) | 0,133 s (nur Scoring) |
| ByteTrack | baseline-matched | 57/61 (93,44 %) | 40/43 (93,02 %) | 23,0 | 0/61 (0,00 %) | keine Review | Apache-2.0 (trackers 2.6.0) | 11,43 s |
| ByteTrack | defaults | 55/61 (90,16 %) | 39/43 (90,70 %) | 25,0 | 0/61 (0,00 %) | keine Review | Apache-2.0 (trackers 2.6.0) | 10,91 s |
| CBIoU | baseline-matched | 58/61 (95,08 %) | 41/43 (95,35 %) | 22,0 | 2/61 (3,28 %) | keine Review | Apache-2.0 (trackers 2.6.0) | 16,18 s |
| CBIoU | defaults | 49/61 (80,33 %) | 35/43 (81,40 %) | 25,0 | 0/61 (0,00 %) | keine Review | Apache-2.0 (trackers 2.6.0) | 15,76 s |
| GTA | gta-link@e4d5cc40+osnet_x1_0-generic | 61/61 (100,00 %) | 43/43 (100,00 %) | 18,0 | 10/61 (16,39 %) | keine Review | MIT (gta-link) + MIT (deep-person-reid/osnet_x1_0) | 18,42 s |
| Deep-EIoU | — | nicht gemessen — Referenz-Implementierung ohne LICENSE-Datei (GitHub-API license: null, geprüft 2026-09-01), damit keine Nutzungserlaubnis; D-02-Gate | — | — | — | — | keine (kein LICENSE-Datei) | nicht gemessen |

**Caveats pro Verfahren:**

- **CBIoU ist ausdrücklich nicht Deep-EIoU** — nächstverwandtes permissiv lizenziertes Verfahren
  (gepufferte IoU-Assoziation), zum Vergleich mitgemessen, weil Deep-EIoU selbst am Lizenz-Gate
  scheitert (siehe `## Deep-EIoU: warum nicht gemessen`).
- **GTA lief mit einem generischen, nicht sportspezifisch feingetunten OSNet-Checkpoint**
  (Market-1501, kein Flag-Football-Finetuning) auf gesampelten Crops (median 12 Crops/Track,
  gedeckelt bei `max_crops_per_track=12`, nicht jeder Frame eingebettet). Der Split/Merge-Schritt
  führte über alle 61 Clips 0 Split- und 364 Merge-Operationen aus, die Track-Partition blieb in
  nur 3/61 Clips unverändert. **Wichtiger Vorbehalt:** die automatische Kontinuitäts-Rate von
  61/61 (100 %) ist NICHT durch eine menschliche Review bestätigt (`human_pass_k`/`n` bewusst leer) und
  misst nur Track-Länge, nicht Identitätskorrektheit — bei median nur 12 Crops/Track und einem
  generischen Embedding könnten einige der 364 Merge-Operationen Tracks verschiedener Spielerinnen
  fälschlich zusammengeführt haben, ohne dass dies verifiziert wäre. Ein hoher Auto-Wert ist hier
  **kein Beleg** für eine tatsächliche Verbesserung gegenüber der 15/61-Referenz.

## Startbefehle

Jeder Befehl läuft aus einem frischen Klon des Repositorys mit den Bundle-Inputs vor Ort
(`data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/`,
`data/processed/tracking/`, `data/reference/`).

**BoT-SORT** (Voraussetzung: die bestehende Tracks-Datei aus Phase 2.1 liegt vor, es wird kein
Tracker neu ausgeführt, nur neu bewertet):

```bash
uv run python scripts/hackathon/run_baseline_trackers.py \
  --method botsort-existing \
  --tracks data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet \
  --review data/reference/continuity_review.csv \
  --split data/reference/frozen_eval_clips.csv \
  --freeze data/reference/hackathon_freeze.json
```

**ByteTrack** (Voraussetzung: `trackers==2.6.0`, bereits Teil der `cv`-Extra-Installation):

```bash
uv run python scripts/hackathon/run_baseline_trackers.py \
  --method bytetrack --config baseline-matched \
  --detections data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet \
  --review data/reference/continuity_review.csv \
  --split data/reference/frozen_eval_clips.csv \
  --freeze data/reference/hackathon_freeze.json \
  --out-dir data/processed/baseline-methods \
  --results-dir data/reference/baseline-methods
```

Mit `--config defaults` statt `--config baseline-matched` für die ungetunte Bibliotheks-Variante.

**CBIoU** (dieselbe Voraussetzung wie ByteTrack — nicht Deep-EIoU, siehe Caveat oben):

```bash
uv run python scripts/hackathon/run_baseline_trackers.py \
  --method cbiou --config baseline-matched \
  --detections data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet \
  --review data/reference/continuity_review.csv \
  --split data/reference/frozen_eval_clips.csv \
  --freeze data/reference/hackathon_freeze.json \
  --out-dir data/processed/baseline-methods \
  --results-dir data/reference/baseline-methods
```

Mit `--config defaults` statt `--config baseline-matched` für die ungetunte Bibliotheks-Variante.

**GTA** (Voraussetzung: der Vendoring-Schritt aus `vendor/README.md` — `gta-link` bei
`e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca` geklont, das MIT-lizenzierte `osnet_x1_0`-Checkpoint
mit SHA-256 `2809d3227f7d078f6045f7feb874a34d0684f0e0057b264b99adccf7d4519154` unter
`data/processed/baseline-methods/checkpoints/osnet_x1_0_market1501.pth` bezogen):

```bash
mkdir -p vendor
git clone https://github.com/sjc042/gta-link.git vendor/gta-link
cd vendor/gta-link && git checkout e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca && cd -

uv run --with gdown python -m gdown \
  "https://drive.google.com/uc?id=1vduhq5DpN2q1g4fYEZfPI17MJeh9qyrA" \
  -O data/processed/baseline-methods/checkpoints/osnet_x1_0_market1501.pth

uv run python scripts/hackathon/measure_gta.py \
  --tracks data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet \
  --crops data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/crops \
  --vendor vendor/gta-link \
  --checkpoint data/processed/baseline-methods/checkpoints/osnet_x1_0_market1501.pth \
  --checkpoint-sha256 2809d3227f7d078f6045f7feb874a34d0684f0e0057b264b99adccf7d4519154 \
  --review data/reference/continuity_review.csv \
  --split data/reference/frozen_eval_clips.csv \
  --out-dir data/processed/baseline-methods \
  --results-dir data/reference/baseline-methods
```

## Deep-EIoU: warum nicht gemessen

`gh api repos/hsiangwei0903/Deep-EIoU --jq '.license'` liefert `null` (geprüft 2026-09-01); im
Repository-Root existiert keine `LICENSE`- oder `COPYING`-Datei. Unter Default-Copyright bedeutet
das "alle Rechte vorbehalten" — es gibt keine Erlaubnis, den Code zu installieren, zu vendoren
oder auszuführen, auch nicht rein lokal für eine interne Messung. Ein permissiv lizenzierter
Nachbau existiert nicht: `boxmot`, das einzige andere Tracker-Toolkit mit sportnahen Methoden, ist
selbst AGPL-3.0 lizenziert und enthält Deep-EIoU ohnehin nicht (nur DeepOCSORT, BoTSORT,
StrongSORT, OCSORT, ByteTrack).

Das ist auch ein Ergebnis für die Hackathon-Teams, nicht nur eine Lücke in dieser Messung: sie
stoßen unter derselben Lizenzregel an dieselbe Wand, sollten sie Deep-EIoU in Betracht ziehen (vgl.
`docs/hackathon-challenge-reid.md` `## Teil 4`, das dieselbe Lizenz-Warnung an die Teams
weitergibt).

## Lizenzen der gemessenen Verfahren

| Komponente | Lizenz (SPDX) | Bezug | Quelle |
|---|---|---|---|
| `trackers` 2.6.0 | Apache-2.0 | bereits installiert (`cv`-Extra); liefert BoT-SORT, ByteTrack, CBIoU | `importlib.metadata`, siehe `docs/lizenz-inventur.md` |
| `gta-link` | MIT | gepinnt auf `e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca` (kein Tag vorhanden, daher `main`-HEAD zum Prüfzeitpunkt) | `gh api repos/sjc042/gta-link --jq '.license.spdx_id'` → `MIT`, geprüft 2026-09-01T14:42:53Z |
| `deep-person-reid` (OSNet, `KaiyangZhou/deep-person-reid`) | MIT | in `vendor/gta-link/reid/` enthalten (strukturell identisch zum offiziellen Repo, kein separater Klon nötig) | `gh api repos/KaiyangZhou/deep-person-reid --jq '.license.spdx_id'` → `MIT`, geprüft 2026-09-01T14:42:53Z |
| Checkpoint `osnet_x1_0_market1501.pth` | MIT (Modellcode); Market-1501-Trainingsdaten separat lizenziert, hier nur der Checkpoint verwendet | Personen-ReID-Backbone für die GTA-Erscheinungs-Einbettung, SHA-256 `2809d3227f7d078f6045f7feb874a34d0684f0e0057b264b99adccf7d4519154` | `vendor/gta-link/reid/docs/MODEL_ZOO.md` §"Same-domain ReID", Google-Drive-ID `1vduhq5DpN2q1g4fYEZfPI17MJeh9qyrA` |
| `hsiangwei0903/Deep-EIoU` (ausgeschlossen) | keine — Default-Copyright, kein LICENSE-File | nicht verwendet | `gh api repos/hsiangwei0903/Deep-EIoU --jq '.license'` → `null`, geprüft 2026-09-01 |
| `sports_model.pth.tar-60` (ausgeschlossen) | keine nachvollziehbare Lizenz/Herkunft (informeller Google-Drive-Link, identischer Dateiname in Deep-EIoU und gta-link) | nicht verwendet, aus dem lokalen `vendor/gta-link`-Klon entfernt | `vendor/README.md` `## Bekannte Probleme` |

## Grenzen dieser Messung

Keine Beschönigung: die automatische Kennzahl ist gesättigt (BoT-SORTs eigene Referenzzeile
beweist das: auto=57/61 = 93,44 % vs. Human-Referenz=15/61 = 24,59 %).
Für neue Verfahren fehlen menschliche Urteile — ByteTrack,
CBIoU und GTA haben alle keine Human-Review, ihre automatischen Werte sind daher nicht direkt mit
der 15/61-Referenz vergleichbar. GTA lief mit einem generischen statt einem sportspezifischen
Erscheinungsmodell und auf gesampelten Crops, nicht auf jedem Frame. Die Transferdomänen (GoPro-
Seitenlinie, TV) sind nicht Teil dieser Messung (v2, TRANS-01) — die hier gemessenen Zahlen gelten
nur für das Drohnenmaterial des Pilotspiels. Die Reihenfolge der Verfahren nach automatischer
Kennzahl (GTA > CBIoU/baseline-matched > BoT-SORT ≈ ByteTrack/baseline-matched > ByteTrack/
defaults > CBIoU/defaults) ist **keine** Reihenfolge nach Identitätsstabilität — dafür fehlt
genau die menschliche Bestätigung, die nur bei BoT-SORT vorliegt.

## Pflege

`tests/test_m2_baseline_docs.py` hält diese Tabelle und `data/reference/baseline-methods/summary.csv`
synchron: eine neue Messung heißt konkret — Skript laufen lassen (die passende Zeile unter
`## Startbefehle`), die CSV aktualisiert sich automatisch über `append_results`, die Tabelle hier
von Hand nachziehen, dann `uv run pytest tests/test_m2_baseline_docs.py -q` grün bekommen, bevor
der Stand als aktuell gilt.
