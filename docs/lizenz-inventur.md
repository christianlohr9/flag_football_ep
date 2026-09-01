# Lizenz-Inventur der ausgelieferten Komponenten (Stand: 2026-09-01)

## Zweck und Geltungsbereich

RECHT-04 verlangt, dass jede Komponente, die eine Hackathon-Mannschaft erhält oder installiert,
eine benannte Lizenz mit einer nachprüfbaren Quelle trägt, nicht eine, die aus dem Gedächtnis
behauptet wird. Diese Inventur deckt vier Klassen ab: das Code-Paket `flag-football-ep` samt
Wertungsskript, jede in `pyproject.toml` deklarierte Distribution, die Modellartefakte hinter den
ausgelieferten Detektionen und die Datenartefakte in den drei Bundles (Dev/Test/Transfer, siehe
`docs/hackathon-bundles.md`). Ausdrücklich außen vor: transitive Abhängigkeiten werden nicht
zeilenweise aufgeführt. Der `rfdetr[train]`-Unterbaum (`pytorch-lightning`, `torchmetrics`,
`faster-coco-eval`, `pycocotools`, `roboflow`, `rf100vl`) wurde einzeln in Plan 02.1-10 geprüft
(Ergebnis: Apache-2.0/BSD/MIT) und wird hier zitiert, nicht wiederholt.

## Ergebnis in einem Satz

Kein AGPL-Bestandteil irgendwo in der ausgelieferten Kette. Die einzige nicht-permissive Lizenz
in der Auslieferung ist die des Repositories selbst, GPL-3.0 (siehe `## Befunde`).

## Komponenten

Erhoben in dieser Reihenfolge: (1) installierte Paket-Metadaten der laufenden Umgebung über
`importlib.metadata` (`.venv/bin/python`), Präzedenz `License-Expression` vor `Classifier: License
:: …` vor dem `License`-Feld; (2) für nicht installierte Distributionen (die `versioning`-Extras
`dvc`/`dvc-s3`, die in dieser Umgebung bewusst nicht installiert sind) `https://pypi.org/pypi/
<name>/json`; (3) für dieses Repository selbst die `LICENSE`-Datei direkt. Es wurde nichts
installiert, um Schritt 1 zu ermöglichen.

| Komponente | Version | Lizenz | Rolle in der Auslieferung | Quelle |
|---|---|---|---|---|
| **Eigenes Paket** | | | | |
| `flag-football-ep` | 0.1.0 | GPL-3.0 | ausgeliefertes Code-Paket (`ffep`-CLI, `src/flag_football_ep/**`), Wertungsskript `scripts/hackathon/score_tracks.py` | LICENSE-Datei |
| **Kern-Abhängigkeiten (`project.dependencies`)** | | | | |
| `requests` | 2.32.3 | Apache-2.0 | Installationsabhängigkeit des Kernpakets | importlib.metadata |
| `pandas` | 2.2.2 | BSD-3-Clause | Installationsabhängigkeit des Kernpakets | importlib.metadata |
| `ipykernel` | 6.29.5 | BSD-3-Clause | Installationsabhängigkeit des Kernpakets (Notebook-Unterstützung) | importlib.metadata |
| `polars` | 1.5.0 | MIT | Wertungsskript `score_tracks.py`, Parquet-Verarbeitung | importlib.metadata |
| `numpy` | 2.1.0 | BSD-3-Clause | Installationsabhängigkeit des Kernpakets | importlib.metadata |
| `seaborn` | 0.13.2 | BSD (Variante laut Metadaten nicht spezifiziert) | Installationsabhängigkeit des Kernpakets (interne Visualisierung) | importlib.metadata |
| `matplotlib` | 3.9.2 | Matplotlib-Lizenz (PSF-basiert) | Installationsabhängigkeit des Kernpakets (interne Visualisierung) | importlib.metadata |
| `scikit-learn` | 1.9.0 | BSD-3-Clause | Installationsabhängigkeit des Kernpakets | importlib.metadata |
| `hyperopt` | 0.2.7 | BSD (Variante laut Metadaten nicht spezifiziert) | Installationsabhängigkeit des Kernpakets (Hyperparameter-Suche) | importlib.metadata |
| `xgboost` | 3.4.1 | Apache-2.0 | Installationsabhängigkeit des Kernpakets | importlib.metadata |
| `setuptools` | 80.10.2 | MIT | Installationsabhängigkeit des Kernpakets (Build-Toolchain) | importlib.metadata |
| `pyarrow` | 17.0.0 | Apache License (Versionsangabe fehlt in den Paket-Metadaten) | Installationsabhängigkeit des Kernpakets (Parquet-Backend) | importlib.metadata |
| `typer` | 0.27.1 | MIT | CLI-Framework für `ffep` | importlib.metadata |
| `mlflow` | 3.15.1 | Apache-2.0 | Installationsabhängigkeit des Kernpakets (Experiment-Tracking, intern) | importlib.metadata |
| `scipy` | 1.14.1 | BSD-3-Clause | Installationsabhängigkeit des Kernpakets | importlib.metadata |
| `jinja2` | 3.1.6 | BSD (Variante laut Metadaten nicht spezifiziert) | Installationsabhängigkeit des Kernpakets (Templating, z. B. generierte READMEs) | importlib.metadata |
| **CV-Erweiterung (`optional-dependencies.cv`)** | | | | |
| `rfdetr` | 1.9.3 | Apache-2.0 | Detektor-Training/-Inferenz (`cv/detect.py`), Basis des eingefrorenen Modells | importlib.metadata |
| `trackers` | 2.6.0 | Apache-2.0 | Tracking-Baseline (BoT-SORT/OC-SORT), Bundle-Prozessierung | importlib.metadata |
| `supervision` | 0.30.0 | MIT | Detektions-/Box-Datenmodell, Bundle-Prozessierung | importlib.metadata |
| `sahi` | 0.12.6 | MIT | Slicing-Inferenz für Detektion, Bundle-Prozessierung | importlib.metadata |
| `transformers` | 5.15.1 | Apache-2.0 | Zero-Shot-Vorlabeling (Grounding DINO), Erscheinungs-Encoder | importlib.metadata |
| `umap-learn` | 0.5.12 | BSD (Variante laut Metadaten nicht spezifiziert) | Embedding-Visualisierung, interne Analyse | importlib.metadata |
| `opencv-python` | 5.0.0.93 | Apache-2.0 | Klassische CV-Bausteine (Homographie, Crops), Bundle-Prozessierung | importlib.metadata |
| `torch` | 2.13.0 | Apache-2.0 AND Apache-2.0 WITH LLVM-exception AND BSD-2-Clause AND BSD-3-Clause AND BSL-1.0 AND MIT | Trainings-/Inferenz-Backend für `rfdetr`/`transformers` | importlib.metadata |
| `cvat-sdk` | 2.73.0 | MIT | Anbindung an die self-hosted CVAT-Labeling-Oberfläche | importlib.metadata |
| **Versionierung (`optional-dependencies.versioning`)** | | | | |
| `dvc` | 3.67.1 | Apache-2.0 | `dvc pull` des Datensatzes | PyPI (2026-09-01) |
| `dvc-s3` | 3.3.0 | Apache-2.0 | S3-kompatibles Backend (OTC OBS) für `dvc pull` | PyPI (2026-09-01) |
| **Testsuite (`dependency-groups.dev`)** | | | | |
| `pytest` | 9.1.1 | MIT | ausgelieferte Testsuite | importlib.metadata |
| **Vendorierte Forschungscode-Kopien (M2-2, nicht in `pyproject.toml`)** | | | | |
| `gta-link` | kein Release-Tag | MIT | Tracklet-Split/Merge (GTA-Baseline-Messung), vendoriert unter `vendor/gta-link`, gepinnt auf Commit `e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca` | GitHub |
| `deep-person-reid` (OSNet) | kein separater Klon, in `vendor/gta-link/reid/` enthalten | MIT | Erscheinungs-Encoder-Code für die GTA-Baseline-Messung, gepinnt über denselben `gta-link`-Commit `e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca` | GitHub |
| OSNet-Checkpoint `osnet_x1_0_market1501.pth` | Market-1501, generisch (nicht sportspezifisch feingetunt) | MIT (Modellcode; Trainingsdaten separat lizenziert) | GTA-Erscheinungs-Backbone, SHA-256 `2809d3227f7d078f6045f7feb874a34d0684f0e0057b264b99adccf7d4519154` | GitHub (Google-Drive-Link aus der MIT-lizenzierten `deep-person-reid`-Quelle) |

## Modell- und Datenartefakte

| Artefakt | Lizenz-/Rechtslage | Quelle |
|---|---|---|
| Eingefrorener Detektor (RF-DETR-Small, nachtrainiert) | Basisgewichte und Code Apache-2.0 (über `rfdetr`); die feingetunten Gewichte sind ein eigenes Artefakt des Projekts, kein OSS-Lizenztext | importlib.metadata (`rfdetr`), Plan 02.2-08 |
| Pro-Frame-Detektionen und Baseline-Tracks (`data/detections.parquet`, `data/tracks.parquet`) | abgeleitetes Werk des Materials, keine OSS-Lizenz | `docs/capture-legal.md` |
| Clips, Overlays und Oberkörper-Crops in den drei Bundles | keine OSS-Lizenz | `docs/capture-legal.md` |

„Keine OSS-Lizenz" heißt hier „nur unter der Verbandsfreigabe nutzbar", nicht „frei nutzbar":
die Bundle-Delivery-Regeln (`docs/hackathon-bundles.md ### Delivery-Regeln`) und die Zweckbindung
aus der Verbandsfreigabe (`docs/capture-legal.md`, RECHT-01) gelten unabhängig von jeder
Software-Lizenz in der Kette.

## Befunde

1. **`LICENSE` dieses Repositories ist GPL-3.0.** Das ist Copyleft: eine Mannschaft, die auf dem
   ausgelieferten Code aufbaut, erzeugt ein GPL-3.0-Derivat. Es ist nicht AGPL, Netzwerknutzung
   allein ist also keine Distribution. Aber die Adoptionsfrage des Verbands aus
   `docs/hackathon-challenge-reid.md ### Technische oder organisatorische Einschränkungen` gilt
   damit auch für unseren eigenen Code, nicht nur für die Bibliothekswahl der Teams.
2. **`pyproject.toml` deklariert kein `license`-Feld.** Die gebaute Distributions-Metadatendatei
   trägt damit keine Lizenzangabe, während `LICENSE` GPL-3.0 sagt. Diese Diskrepanz wird hier nur
   benannt, nicht behoben: `pyproject.toml` ist in diesem Plan read-only.
3. **Kein AGPL-Bestandteil in der ausgelieferten Kette.** D-02 hat durchgehend gehalten: kein
   Ultralytics, kein boxmot. Alle 29 Zeilen der `## Komponenten`-Tabelle wurden geprüft.
4. **`mlflow` und `numpy` liefern im `License`-Feld zuerst einen Copyright-Vermerk statt einer
   reinen Lizenzkennung** (`mlflow`: „Copyright 2018 Databricks, Inc. …" vor dem eingebetteten
   Apache-2.0-Volltext; `numpy`/`scipy`: „Copyright (c) … NumPy/SciPy Developers." vor dem
   eingebetteten BSD-3-Clause-Muster mit Namensnennungs- und No-Endorsement-Klausel). Die
   verlässliche Kennung kommt in beiden Fällen aus dem `Classifier`- bzw. dem übrigen Textmuster,
   nicht aus einer sauberen SPDX-Zeile.
5. **`numpys` binäres Wheel bündelt zusätzlich Laufzeitbibliotheken unter anderen Lizenzen**
   (`libgfortran`/`libquadmath` unter GPL-3.0-with-GCC-exception bzw. LGPL-2.1, `OpenBLAS`/`LAPACK`
   unter BSD-3-Clause): das betrifft nur die mitgelieferten kompilierten Laufzeitkomponenten,
   nicht die Lizenz von NumPy selbst (BSD-3-Clause, siehe oben), wird hier aber der Vollständigkeit
   halber festgehalten, da GPL-3.0-with-GCC-exception textuell im `License`-Feld auftaucht.
6. **`seaborn`, `jinja2`, `hyperopt` und `umap-learn` melden nur „BSD" ohne Klausel-Variante**
   (Classifier `License :: OSI Approved :: BSD License` bzw. `License`-Feld-Wert `BSD`). Die
   Inventur übernimmt genau das, was die Paket-Metadaten hergeben, statt eine Klausel-Zahl zu
   erraten.

## Nicht ausgeliefert, nur empfohlen

Die in `docs/hackathon-challenge-prep.md ## 4. Bibliotheken und Ressourcen für die Teams` und
`docs/hackathon-challenge-reid.md ## Teil 4` genannten Kandidatenverfahren (`torchreid`,
`gta-link`, MOTIP, OC-SORT/Hybrid-SORT als Alternative, SigLIP/CLIP/DINOv2-Encoder) sind
Empfehlungen an die Teams, keine ausgelieferten Komponenten dieses Projekts. Sie erscheinen daher
nicht in `## Komponenten` und werden hier nicht erneut geprüft; die Lizenzhinweise (inklusive der
Vermeiden-Liste: Ultralytics YOLO, boxmot, Deep-EIoU, GTATrack, PRTreID/BPBreID, sn-gamestate/
sn-reid, SportsMOT) stehen in den genannten Dokumenten. Phase M2-2 hängt jeden Tracker-Kandidaten,
den sie tatsächlich misst, vor dessen Installation an `## Komponenten` an.

## Pflege

Diese Inventur wird von `tests/test_m2_lizenz_inventur.py` geprüft: eine neue Distribution in
`pyproject.toml` ohne passende Zeile in `## Komponenten` lässt den Test fehlschlagen. Wer eine
Abhängigkeit hinzufügt, ergänzt im selben Schritt eine Zeile mit Version, Lizenz und Quelle nach
dem oben beschriebenen Erhebungsverfahren.
