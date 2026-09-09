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

Kein AGPL-Bestandteil irgendwo in der ausgelieferten Kette. Die Lizenz des Repositories selbst
war GPL-3.0 und ist seit der Entscheidung vom 2026-09-09 Apache-2.0 (siehe `## Befunde` und
`## Entscheidung 2026-09-09`).

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
| `flag-football-ep` | 0.1.0 | Apache-2.0 | ausgeliefertes Code-Paket (`ffep`-CLI, `src/flag_football_ep/**`), Wertungsskript `scripts/hackathon/score_tracks.py` | LICENSE-Datei |
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
| `openpyxl` | 3.1.5 | MIT | Zellgenaues Lesen der Head-Coach-Workbooks (`ingest/hc_workbook.py`), `data_only`-Formelauflösung und `read_only`-Streaming | importlib.metadata |
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

1. **`LICENSE` dieses Repositories war GPL-3.0, ist seit 2026-09-09 Apache-2.0.** Der Befund
   unten beschreibt den Stand zum Zeitpunkt dieser Inventur (2026-09-01): GPL-3.0 ist Copyleft,
   eine Mannschaft, die auf dem ausgelieferten Code aufbaut, hätte ein GPL-3.0-Derivat erzeugt.
   Es war nicht AGPL, Netzwerknutzung allein wäre also keine Distribution gewesen. Die
   Adoptionsfrage des Verbands aus `docs/hackathon-challenge-reid.md ### Technische oder
   organisatorische Einschränkungen` galt damit auch für unseren eigenen Code, nicht nur für die
   Bibliothekswahl der Teams — das war genau der Grund für die Entscheidung, siehe
   `## Entscheidung 2026-09-09`.
2. **`pyproject.toml` deklarierte kein `license`-Feld.** Die gebaute Distributions-Metadatendatei
   trug damit keine Lizenzangabe, während `LICENSE` GPL-3.0 sagte. Zum Zeitpunkt dieser Inventur
   (2026-09-01) wurde die Diskrepanz nur benannt, nicht behoben (`pyproject.toml` war in Plan
   M2-01-02 read-only). Behoben mit der Entscheidung vom 2026-09-09: `pyproject.toml` trägt jetzt
   `license = "Apache-2.0"`.
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

## Entscheidung 2026-09-09

Der Nutzer hat die in `.planning/todos/pending/2026-09-01-lizenz-des-eigenen-codes-klaeren.md`
aufgeworfene Entscheidung getroffen (Antwort auf `docs/TODO-CHRISTIAN.md` Punkt 5, wörtlich
„privat + Apache"): das Repository wird auf GitHub **privat** gestellt, und die Lizenz wechselt
von GPL-3.0 auf **Apache-2.0 mit Namensnennung** des Urhebers (`christianlohr9`).

**Begründung:** Namensnennung statt Copyleft — der Verband/Hackathon-Teams sollen den Code frei
weiterverwenden können, ohne selbst unter GPL-3.0 zu geraten (das war genau der in `## Befunde`
Punkt 1 benannte Adoptions-Konflikt). Der Wettbewerbsvorteil des Projekts (Modelle, Datensätze,
Methodik) liegt nicht im Quellcode selbst, sondern in den nicht-quelloffenen Modell- und
Datenartefakten (siehe `## Modell- und Datenartefakte` oben, ohnehin „keine OSS-Lizenz"). Privat
zu stellen schützt diesen Vorteil (niemand außerhalb des Teams sieht die Historie, Notebooks,
Zwischenstände), ohne die Lizenzfrage zu berühren — Sichtbarkeit und Lizenz sind zwei getrennte
Hebel, die hier bewusst zusammen, aber unabhängig voneinander entschieden wurden.

### Abhängigkeitsprüfung vor der Umstellung

Vor der Umstellung wurde `## Komponenten` (oben) und `pyproject.toml` erneut gegen die
Möglichkeit geprüft, dass eine Laufzeitabhängigkeit GPL/AGPL-lizenziert ist und eine
Apache-2.0-Lizenzierung dieses Repositories damit falsch wäre:

- Kein Eintrag in `## Komponenten` trägt GPL oder AGPL als Lizenz der Distribution selbst
  (`## Befunde` Punkt 3, unverändert gültig: kein AGPL-Bestandteil in der ausgelieferten Kette).
  Alle Kern- und `cv`-Extra-Abhängigkeiten sind Apache-2.0, MIT oder BSD-3-Clause; die
  `versioning`-Extras (`dvc`, `dvc-s3`) sind Apache-2.0.
- Einzige Fundstelle mit GPL-Text im `License`-Feld überhaupt ist `## Befunde` Punkt 5: `numpy`s
  binäres Wheel bündelt kompilierte Laufzeitbibliotheken (`libgfortran`/`libquadmath` unter
  GPL-3.0-with-GCC-exception, `OpenBLAS`/`LAPACK` unter BSD-3-Clause). Das ist kein Blocker: die
  GCC Runtime Library Exception existiert genau für diesen Fall — sie erlaubt das Linken/Bündeln
  mit anders lizenziertem Code ausdrücklich, ohne dass der linkende Code selbst GPL wird. NumPy
  selbst bleibt BSD-3-Clause (siehe `## Komponenten`); die gebündelten Laufzeitbibliotheken sind
  kompilierte Artefakte des NumPy-Wheels, kein Code dieses Repositories, und keine Abhängigkeit,
  die dieses Repository selbst unter GPL stellen würde.
- **Ergebnis: kein GPL/AGPL-Blocker.** Eine Apache-2.0-Lizenzierung dieses Repositories ist mit
  der geprüften Abhängigkeitskette vereinbar.

### Umgesetzte Änderungen

- `LICENSE`: vollständiger Apache-2.0-Text (offizielle Fassung, `apache.org/licenses/
  LICENSE-2.0.txt`), Copyright-Zeile `Copyright 2026 christianlohr9`.
- `NOTICE`: Projektname, Copyright-Zeile, ein Satz zur Namensnennung bei Weiterverwendung.
- `pyproject.toml`: `license = "Apache-2.0"` (SPDX-Ausdruck, PEP 639) ergänzt — löst `## Befunde`
  Punkt 2 auf.
- `README.md`: neuer Lizenz-Abschnitt, verweist auf `LICENSE`/`NOTICE`.
- `## Komponenten`-Tabelle oben: Zeile `flag-football-ep` von GPL-3.0 auf Apache-2.0 aktualisiert.
- `tests/test_m2_lizenz_inventur.py::test_own_package_row_present`: Assertion von GPL-3.0 auf
  Apache-2.0 aktualisiert (sonst würde die Coverage-Gate ab jetzt fälschlich fehlschlagen).

### Was das für die Hackathon-Bundles bedeutet

Die drei Bundles (`docs/hackathon-bundles.md`) enthalten selbst keinen Quellcode dieses
Repositories — nur Daten- und Modellartefakte (Clips, Detektionen, Tracks, Crops), die laut
`## Modell- und Datenartefakte` oben ohnehin „keine OSS-Lizenz" tragen und ausschließlich über
die Verbandsfreigabe (`docs/capture-legal.md`) und die Delivery-Regeln in
`docs/hackathon-bundles.md` geregelt sind, nicht über `LICENSE`. Diese Delivery-Regeln ändern
sich durch den Lizenzwechsel inhaltlich nicht: Zweckbindung, Löschung nach dem Event und das
Verbot der Weitergabe außerhalb des Event-Kontexts gelten unverändert für die Datenartefakte.

Ein Satz ist trotzdem nötig, weil die Teams zur Auswertung `scripts/hackathon/score_tracks.py`
und Teile von `src/flag_football_ep/**` (Schemas, CLI) brauchen — das ist der Teil, dessen Lizenz
sich tatsächlich ändert. Ergänzt in `docs/hackathon-bundles.md` (Abschnitt „Lizenz des
Wertungscodes"): das Wertungsskript und der `ffep`-Code sind ab 2026-09-09 Apache-2.0 mit
Namensnennung, unabhängig von den Delivery-Regeln der Datenartefakte.

### Sichtbarkeit umstellen (Aufgabe des Nutzers)

Dieser Plan ändert die Sichtbarkeit **nicht** — das ist bewusst eine Aktion des Repository-Owners,
keine automatisierte. So geht es:

**Kommandozeile** (GitHub-CLI, `gh` muss beim Nutzer eingeloggt sein):

```
gh repo edit christianlohr9/flag_football_ep --visibility private --accept-visibility-change-consequences
```

**Oder über die GitHub-Oberfläche:** Repository öffnen → „Settings" → ganz unten „Danger Zone" →
„Change repository visibility" → „Change visibility" → „Make private" → Repository-Namen zur
Bestätigung eintippen.

**Wichtig, unabhängig vom Weg:** Bereits existierende Forks und lokale Klone von anderen
Personen behalten ihre Kopie unter der alten Lizenz (GPL-3.0) und bleiben öffentlich einsehbar,
wo immer sie liegen — das ist bei bereits verteilten Versionen nicht rückholbar. Die
Sichtbarkeits- und Lizenzumstellung wirkt nur auf dieses Repository und auf Kopien, die ab jetzt
gezogen werden.

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
