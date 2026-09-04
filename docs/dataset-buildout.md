# Datensatz-Aufbau — Laufendes Protokoll (Phase 2.2)

**Status: Iteration 1 abgeschlossen — Korrektursitzung, Merge und DVC-Versionierung am
2026-09-02 (Plan 02.2-13), am selben Tag per Korrektur auf Datensatz v1.1 berichtigt
(D-17-Verstoss, siehe `### Korrektur 2026-09-02` unten), am 2026-09-04 per GoPro-Nachsitzung
auf Datensatz v1.2 erweitert (siehe `### Nachtrag 2026-09-04` unten). Datensatz v1.2 liegt
unter `data/labels/dataset/`, DVC-getrackt, 572 Bilder über drei Domänen (Drohne 450,
TV/Broadcast 100, GoPro/Hinterfeld 22) — jedes Bild tatsächlich von der Nutzerin in CVAT
gesichtet und/oder korrigiert, per Datei-Diff gegen die Vorlabels verifiziert, nicht nur
gemeldet. Iteration-1-Detektor auf v1.2 trainiert und per Domäne evaluiert (Plan 02.2-15,
MLflow Run `be854a1adebf4eb4b01d98dc39022ee1`) — Drohne verschlechtert sich gegenüber dem
Champion auf den eingefrorenen Eval-Clips (`mAP_50_95` -0,0476), GoPro nicht messbar (keine
Ground Truth für die eingefrorenen Clips), daher **nicht promoviert**, siehe
`## Iteration-1-Detektor: Training und Per-Domain-Evaluation (Plan 02.2-15)` unten. Noch
offen: Iteration 2 (Plan 02.2-17) und der echte OTC-OBS-`dvc push` (Plan 02.2-20). Am
2026-09-04 zusätzlich: Ground-Truth-Sampling + Vorlabel-Push für die eingefrorenen Eval-Clips
vorbereitet (CVAT-Aufgaben `eval-gt-drone`/`eval-gt-sideline`), geprüft, Held-out-Ergebnis
bestätigt die Nicht-Promotion (`### Nachtrag 2026-09-04 (abends)`). Ursachen-Diagnose
abgeschlossen (drei Ablationsläufe): der ursprünglich berichtete Drohnen-Abstand war
grossteils (rund zwei Drittel bis drei Viertel, je nach Metrik) ein Messartefakt — der
Champion selbst war auf den Eval-Clips nicht sauber held-out (88+25 Leck-Frames im eigenen
Pilotentraining). Ein "sauberer Champion" (Ablation D, Eval-Clips vollständig entfernt) landet
bei `mAP_50` 0,9030/`mAP_50_95` 0,7847 statt der ursprünglichen 0,9550/0,9423. Ein kleinerer,
aber echter Rest bleibt (~0,015/~0,077 gegenüber Iteration-1) und ist am ehesten der
AL-1-Frame-Diversität pro Clip zuzuschreiben, siehe `### Nachtrag 2026-09-04 (Diagnose)` und
`### Nachtrag 2026-09-04 (Diagnose, Korrektur)` unten.**

### Korrektur 2026-09-04 (Koordinator): Der Drohnen-Vergleich ist kein Held-out-Vergleich

**Befund:** Die 76 Ground-Truth-Bilder der obigen Tabelle stammen aus dem korrigierten
Pilot-Datensatz (`data/labels/frames/manifest.json`, 404 Frames über alle 61 Clips). Der
Phase-2.1-Champion (`87a8a522…`) wurde genau auf diesem Datensatz trainiert: von den 113
Pilot-Frames in den 18 eingefrorenen Drohnen-Eval-Clips lagen 88 im `train`- und 25 im
`val`-Split des Champion-Laufs. Der Champion wurde also auf seinen eigenen Trainingsbildern
gemessen (0,6259), der Iteration-1-Lauf (`be854a1a…`) dagegen auf echt ungesehenen Bildern —
Datensatz v1.2 enthält keinen einzigen der 18 Eval-Clips (43 Pool-Clips, 450 Drohnen-Frames,
0 Bild-Überschneidung mit dem Pilot-Manifest). Trainingskonfiguration beider Läufe identisch
(30 Epochen, 896 px, Batch 4, Grad-Accum 4, MPS).

**Folge:** Der Unterschied von −0,0476 mAP_50_95 ist als „Rückgang" nicht belegt; die
Stoppregel-Bewertung für die Drohnen-Domäne lautet nicht `nein`, sondern **nicht messbar**
(wie bereits für GoPro/Hinterfeld). Die Nicht-Beförderung bleibt richtig, aber aus dem
Grund „kein gültiger Vergleich", nicht „Regression".

**Was den Vergleich gültig macht (Nutzer-Gate):** Ground Truth direkt aus den eingefrorenen
Eval-Clips — 18 Drohnen- und 12 GoPro/Hinterfeld-Clips, Vorschlag 5–6 Frames je Clip
(≈ 90 + 70 Frames), als eigene CVAT-Aufgabe mit Vorlabels des aktuellen Champions, 100 % geprüft
(D-15). Beide Läufe werden dann auf denselben, von keinem Modell gesehenen Bildern gemessen;
erst danach ist die Stoppregel (+0,010 mAP_50_95) überhaupt anwendbar. Bis dahin gilt für
beide Domänen: Iteration 2 läuft wegen des 1.500-Frame-Floors ohnehin weiter.

### Nachtrag 2026-09-04 (abends): Held-out-Auswertung auf der geprüften Eval-Ground-Truth

Die Nutzerin hat die beiden Eval-Aufgaben (CVAT 6 `eval-gt-drone`, 7 `eval-gt-sideline`) zu 100 %
geprüft; die Labels liegen unter `data/labels/eval/<domain>/corrected/` (Drohne 90 Bilder,
1.834 Boxen; GoPro/Hinterfeld 72 Bilder, davon 66 mit Boxen, 623 Boxen). Beide Läufe wurden mit
`ffep cv eval-domains --split data/reference/frozen_eval_clips.csv` auf exakt dieser Ground Truth
gemessen (`data/reports/eval_domains_champion.json`, `data/reports/eval_domains_iteration1.json`):

| Lauf | Domäne | n Bilder | n Boxen | mAP_50 | mAP_50_95 | AP_player | AP_referee |
|---|---|---:|---:|---:|---:|---:|---:|
| Champion 2.1 (`87a8a522…`) | Drohne | 90 | 1834 | 0,955 | 0,942 | 0,952 | 0,933 |
| Iteration-1 v1.2 (`be854a1a…`) | Drohne | 90 | 1834 | 0,888 | 0,707 | 0,713 | 0,701 |
| Champion 2.1 | GoPro/Hinterfeld | 72 | 623 | 0,797 | 0,781 | 0,702 | 0,861 |
| Iteration-1 v1.2 | GoPro/Hinterfeld | 72 | 623 | 0,725 | 0,529 | 0,595 | 0,463 |

**Vorbehalt Vorlabel-Bias:** Die Ground Truth wurde aus Champion-Vorlabels heraus geprüft. Auf der
Drohne blieben 1.736 von 1.834 Boxen (95 %) unverändert (IoU ≥ 0,95 zum Vorlabel), 55 wurden
nachjustiert, 43 ergänzt, 14 gelöscht; auf GoPro/Hinterfeld blieben 467 von 623 unverändert,
141 wurden ergänzt (Spielerinnen, die der Champion nicht gefunden hatte), 15 nachjustiert,
14 gelöscht. Die Champion-Werte — besonders mAP_50_95, das die Box-Genauigkeit belohnt — sind
dadurch nach oben verzerrt; 0,942 auf der Drohne ist überwiegend Selbstübereinstimmung. Ein
Drohnen-only-Modell mit 0,781 mAP_50_95 auf GoPro ist aus demselben Grund nicht als echte
GoPro-Leistung zu lesen.

**Was trotzdem trägt:** Bei IoU 0,5 (mAP_50), wo die Box-Genauigkeit kaum zählt, liegt
Iteration-1 auf der Drohne um 0,067 und auf GoPro um 0,072 hinter dem Champion. Die Stoppregel
(+0,010 mAP_50_95 auf der Drohne, ohne Rückgang in der zweiten Domäne) ist damit **nicht erfüllt**;
der Champion bleibt, der Iteration-1-Lauf wird nicht befördert. Anders als am Vormittag ist das
jetzt ein Held-out-Befund, wenn auch mit dem genannten Bias zugunsten des Champions.

**Offene Diagnose (vor Iteration 2 zu klären):** Warum ist ein auf 450 Drohnen-Frames (plus TV
100, GoPro 22) trainiertes Modell auf der Drohne schwächer als das Pilot-Modell auf 404 Frames?
Kandidaten, jeweils prüfbar ohne neue Labels: (1) kein Val-Split in v1.2 — der Lauf nutzte den
EMA-Fallback statt einer Best-Checkpoint-Auswahl (`checkpoint_source =
best_ema_fallback_no_val_split`); (2) Multi-Domain-Mix (TV/GoPro) im selben Lauf; (3) andere
Frame-Verteilung der AL-1-Auswahl (gezielt unsichere Frames) gegenüber der Pilot-Stichprobe;
(4) Startgewichte (COCO-Pretrain vs. Champion-Feintuning). Vorschlag: eine kleine Ablation
(Drohne-only v1.2 mit 10 % Val-Split; Champion-Feintuning auf v1.2) auf denselben Eval-Labels,
bevor Plan 02.2-16 weitere Labels anfordert.

### Nachtrag 2026-09-04 (Ausführung): Ground-Truth-Sampling für die eingefrorenen Eval-Clips vorbereitet

Der Tooling-Teil der Korrektur oben ist ausgeführt — Frame-Sampling + Vorlabel-Push in eigene
CVAT-Aufgaben, ohne selbst Labels zu erzeugen (das bleibt Aufgabe der Nutzerin, D-15/D-19).

**Sampling** (`ffep cv eval-gt-sample`, neu: `cv/frames.py::sample_eval_gt_frames`): zieht pro
eingefrorenem Eval-Clip eine feste, seed-deterministische Frame-Anzahl (kein
dauer-proportionales Budget wie `sample_training_frames`, sondern N Frames je Clip), verteilt
über die Clip-Zeitachse per Grid+Jitter (derselbe Mechanismus, andere Allokation). Seed
`20260516` (derselbe wie `frozen_eval_clips.csv`, aus Nachvollziehbarkeit wiederverwendet).

| Domäne | Clips | Frames/Clip | Frames gesamt | Session | Manifest |
|---|---:|---:|---:|---|---|
| Drohne (`drone`) | 5, 6, 7, 11, 15, 16, 21, 22, 28, 33, 36, 40, 43, 49, 52, 54, 55, 56 (18) | 5 | **90** | `2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE` | `data/labels/eval/drone/frames/manifest.json` |
| GoPro/Hinterfeld (`sideline`) | 8, 11, 16, 19, 24, 27, 34, 36, 37, 38, 50, 52 (12) | 6 | **72** | `2026-08-14_WC-GER-vs-MEX-GOPRO` | `data/labels/eval/sideline/frames/manifest.json` |

**Vorlabel** (`ffep cv prelabel --backend finetuned`): ohne explizite `--run-id` löst
`prelabel._load_finetuned_backend` die aktuelle `champion`-Alias auf — da Iteration 1 nicht
promoviert wurde (Plan 02.2-15), bleibt das der Phase-2.1-Champion
`87a8a5222f7a472787875e974d089c44`, exakt der Lauf, gegen den auch Iteration 1 gemessen werden
soll. Inferenz-Einstellungen `resolution=896`/`sahi=false` (`ffep.toml`) sind für beide Domänen
identisch mit den in `docs/dataset-plan.md` `## 4` gemessenen Pro-Domäne-Empfehlungen (zufällige
Koinzidenz, dort dokumentiert) — keine Config-Änderung nötig.

- Drohne: 90/90 Frames mit ≥1 Box, 1805 Boxen gesamt, 0 Frames ohne Erkennung.
- GoPro/Hinterfeld: 66/72 Frames mit ≥1 Box, 496 Boxen gesamt, **6 Frames ohne Erkennung**
  (Clip 24: 4 von 6 Frames, Clip 27: 2 von 6 Frames). Nach der Fernfeld-Vereinbarung vom
  2026-09-02 (`### Nachtrag 2026-09-02` oben) mutmasslich echtes, unberührtes Fernfeld, kein
  Detektor-Fehlschlag — **für die Nutzerin:** diese 6 Frames dürfen mit 0 Boxen bestehen
  bleiben, wenn tatsächlich keine Spielerin im Nah-/Mittelfeld sichtbar ist; nur bei
  sichtbaren, aber unmarkierten Spielerinnen im Nah-/Mittelfeld nachtragen. Betroffene Dateien:
  `Wide - Clip 024_f00028.jpg`, `..._f00176.jpg`, `..._f00461.jpg`, `..._f00595.jpg`,
  `Wide - Clip 027_f00048.jpg`, `..._f00244.jpg`.

**CVAT-Aufgaben** (self-hosted, Loopback, getrennt von den `al-1-*`/`al-2-*`-Aufgaben):

| Task-ID | Name | Frames |
|---:|---|---:|
| 6 | `eval-gt-drone` | 90 |
| 7 | `eval-gt-sideline` | 72 |

**DVC:** `data/labels/eval/` (Frames + Vorlabel-COCO beider Domänen, 328 Dateien) per
`dvc add data/labels/eval` getrackt (`data/labels/eval.dvc`, md5
`a5c6e47a93c76b44b9068edb918e4d21.dir`), gegen den lokalen Rückfall-Remote `local-fallback`
gepusht (167 Dateien, dedupliziert über die hartverlinkten `frames/`/`prelabel/`-Kopien) —
derselbe Mechanismus wie `data/labels/dataset.dvc` (Plan 02.2-13), nie der
OTC-OBS-Platzhalter-Remote. `data/labels/eval/` bleibt strikt getrennt von
`data/labels/dataset/` (eigener DVC-Pointer, eigener Verzeichnisbaum).

**D-19-Schutz jetzt Code, nicht mehr nur Konvention:** `dataset.assert_no_frozen_eval_clips`,
neu über einen `eval_split_path`-Parameter in `validate_coco` verdrahtet — sowohl
`ffep cv dataset` als auch `detect.train_detector`s eigene Datensatz-Vorbereitung übergeben ab
sofort unbedingt `data/reference/frozen_eval_clips.csv`. Ein Merge-Versuch, der einen
eingefrorenen Eval-Clip enthält, schlägt jetzt mit einem benannten `DatasetError` fehl, statt
sich (wie bislang bei jedem AL-Iterations-Merge, siehe Plan 02.2-13s "0 Überschneidung"
-Assertion) auf eine manuelle Post-hoc-Prüfung zu verlassen. Test:
`tests/test_cv_dataset.py::test_assert_no_frozen_eval_clips_raises_naming_offending_clip` und
`test_validate_coco_rejects_frozen_eval_frame_when_eval_split_path_given`.

**Lade-Reihenfolge für `evaluate_per_domain`:** `detect._load_domain_ground_truth` bevorzugt ab
sofort `data/labels/eval/<domain>/corrected/instances.json` (diese neue, garantiert
held-out Konvention) vor der alten `data/labels/<session_id>/corrected/`-Konvention, sobald
Ersteres existiert — nie beide gemischt für dieselbe Domäne. Test:
`test_evaluate_per_domain_prefers_eval_gt_directory_over_session_corrected`.

**Nach der Prüfung durch die Nutzerin:** korrigierte Exporte herunterladen —

```
ffep cv cvat-pull --task 6 --out data/labels/eval/drone/corrected
ffep cv cvat-pull --task 7 --out data/labels/eval/sideline/corrected
```

— dann beide Läufe auf denselben, garantiert ungesehenen 90 (Drohne) + 72
(GoPro/Hinterfeld) Bildern messen:

```
ffep cv eval-domains --run 87a8a5222f7a472787875e974d089c44 \
  --split data/reference/frozen_eval_clips.csv --out data/reports/eval_domains_champion.json
ffep cv eval-domains --run be854a1adebf4eb4b01d98dc39022ee1 \
  --split data/reference/frozen_eval_clips.csv --out data/reports/eval_domains_iteration1.json
```

Erst danach ist die Stoppregel (+0,010 mAP_50_95, `docs/dataset-plan.md` `## 3`) für beide
Domänen tatsächlich anwendbar — nicht länger "nicht messbar".

### Nachtrag 2026-09-04 (Diagnose): Warum Iteration 1 auf der Drohne schwächer ist als der Champion

Die vier Kandidaten aus dem vorigen Nachtrag wurden geprüft — zwei ohne GPU an den
vorhandenen Daten/MLflow-Params, zwei per kleiner Ablation (max. drei Trainingsläufe laut
Vorgabe, zwei genügten für eine Entscheidung).

**Günstige Prüfung ohne neues Training:**

- **Manifest/Frames bestätigt korrekt.** `be854a1a…`s Param `dataset_content_sha256` ==
  `d4528a99…` == Datensatz v1.2s eigener Hash exakt; der Lauf startete 2026-09-04 13:45,
  nach Commit `53ba641` (11:26), der `_resolve_manifest_path` fixte. Iteration 1 trainierte
  nachweislich auf den richtigen 572 v1.2-Frames, nicht auf dem alten 404-Frame-Pilot-Manifest.
- **Keine Label-Konventions-Drift.** Boxen/Bild (Pilot 21,76 vs. v1.2-Drohne 19,69),
  Referee-Anteil (9,86 % vs. 10,23 %), Box-Fläche (Median 3537 vs. 3585 px²), Box-Breite/Höhe
  (44,6×86,2 vs. 43,8×88,5) — alle nahezu identisch zwischen Piloten-Korrektur und
  AL-1-Korrektur. Die Korrektursitzungen labeln konsistent.
- **Frame-Dichte pro Clip deutlich unterschiedlich.** Der Champion trainierte auf 322
  Trainings-Frames über 46 Clips (Median 7 Frames/Clip, Ziehung duration-proportional über
  alle 60 Session-Clips). v1.2-Drohne zieht 450 Frames über nur 43 Pool-Clips (Median 12
  Frames/Clip — am `_MAX_CANDIDATES_PER_CLIP`-Deckel von 12, denselben, den auch
  `sample_training_frames` für reguläres Sampling verwendet). Trotz *mehr* Rohframes ist die
  effektive Clip-Diversität in v1.2 niedriger: bis zu 12 zeitlich nahe, stark korrelierte
  Frames aus demselben Clip liefern deutlich weniger unabhängiges Trainingssignal als
  duration-proportional über mehr Clips verteilte Frames.

**Ablation A — Drohne-only-Teilmenge von v1.2 mit gesätem 10 %-Val-Split** (Kandidaten 1
"kein Val-Split/EMA-Fallback" und 2 "Multi-Domain-Mix" zusammen getestet): 450 Drohnen-Frames
unverändert aus v1.2, Clip-Level-Val-Split neu zugewiesen (Seed `20260905`, 5 von 43 Clips,
45/450 Frames = 10,0 %), sonst exakt die Champion-Einstellungen (30 Epochen, 896 px, Batch 4,
Grad-Accum 4, MPS). MLflow-Lauf `702ab0b5baaf422fa0c21e3988daa4a4`
(`checkpoint_source = best_total` — der Val-Split griff, echte Best-Checkpoint-Auswahl über
30 Epochen, nicht der EMA-Fallback). **Ergebnis: praktisch identisch zu Iteration 1**
(Drohne `mAP_50` 0,8884 vs. 0,8881, `mAP_50_95` 0,7076 vs. 0,7073). Damit sind Kandidaten 1
und 2 beide **widerlegt**: weder ein echter Val-Split mit Best-Checkpoint-Auswahl noch die
Entfernung des Multi-Domain-Mix verändert die Drohnen-Leistung messbar. Da A die Lücke nicht
schliesst, entfällt Lauf C (der laut Plan nur zur Trennung von 1 und 2 nötig gewesen wäre,
wenn A sie geschlossen hätte).

**Ablation B — v1.2 vollständig, Start von den Champion-Gewichten** (Kandidat 4
"COCO-Pretrain vs. Champion-Feintuning"): dieselben 572 v1.2-Frames wie Iteration 1
(0 Val-Frames, `checkpoint_source = best_ema_fallback_no_val_split`, unverändert gegenüber
Iteration 1), aber `RFDETRSmall(pretrain_weights=…)` mit dem Champion-Checkpoint
(`checkpoint_best_total.pth`, Modellregistry-Artefakt des Champion-Laufs) statt dem
RF-DETR-Standard-COCO-Pretrain initialisiert — dafür `train_detector` um einen minimalen
`init_weights`-Parameter erweitert (`RFDETRSmall(pretrain_weights=…)`, dieselbe Mechanik, die
`RFDETRWrapper.load_context` bereits für das Laden registrierter Checkpoints nutzt; neuer
`--init-weights`-CLI-Flag, ein Test, Commit `94f0572`). MLflow-Lauf
`689d5f1dc2c2450785be0f1a1bac9491`. **Ergebnis: kleine, echte, aber die Lücke nicht
schliessende Verbesserung.** Drohne `mAP_50` 0,8926 (+0,0044 ggü. Iteration 1), `mAP_50_95`
0,7283 (+0,0210); GoPro/Hinterfeld `mAP_50` 0,7644 (+0,0396), `mAP_50_95` 0,5655 (+0,0363).
Kandidat 4 trägt real bei — spürbarer bei GoPro/Hinterfeld als bei der Drohne — bleibt aber
weit vom Champion entfernt (Drohne `mAP_50_95` 0,7283 vs. 0,9423).

**Ergebnistabelle** (alle Läufe auf derselben geprüften Ground Truth, `n` überall identisch):

| Lauf | Domäne | n Bilder | mAP_50 | mAP_50_95 | AP_player | AP_referee |
|---|---|---:|---:|---:|---:|---:|
| Champion 2.1 (`87a8a522…`) | Drohne | 90 | 0,9550 | 0,9423 | 0,9520 | 0,9325 |
| Iteration-1 v1.2 (`be854a1a…`) | Drohne | 90 | 0,8881 | 0,7073 | 0,7134 | 0,7013 |
| Ablation A (`702ab0b5…`, Drohne-only + Val-Split) | Drohne | 90 | 0,8884 | 0,7076 | 0,7223 | 0,6929 |
| Ablation B (`689d5f1d…`, Champion-Feintuning) | Drohne | 90 | 0,8926 | 0,7283 | 0,7208 | 0,7359 |
| Champion 2.1 | GoPro/Hinterfeld | 72 | 0,7971 | 0,7813 | 0,7016 | 0,8610 |
| Iteration-1 v1.2 | GoPro/Hinterfeld | 72 | 0,7248 | 0,5292 | 0,5952 | 0,4633 |
| Ablation A (Drohne-only, sah nie GoPro) | GoPro/Hinterfeld | 72 | 0,6510 | 0,4838 | 0,5410 | 0,4265 |
| Ablation B (Champion-Feintuning, v1.2 voll) | GoPro/Hinterfeld | 72 | 0,7644 | 0,5655 | 0,6046 | 0,5265 |

Ablation A's GoPro/Hinterfeld-Zeile ist erwartungsgemäss die schwächste (das Modell trainierte
nie auf GoPro-Frames) — sie steht nur zur Vollständigkeit in der Tabelle, nicht als
Domain-Vergleich.

**Schlussfolgerung.** Weder der fehlende Val-Split (Kandidat 1) noch der Multi-Domain-Mix
(Kandidat 2) erklärt die Drohnen-Lücke — Ablation A widerlegt beide direkt. Die
Startgewichte (Kandidat 4) tragen real, aber nur klein bei. Die tragfähigste verbleibende
Erklärung ist die **Frame-Diversität der AL-1-Auswahl** (Kandidat 3, ergänzt um den
Diversitäts-Aspekt, nicht nur "andere Verteilung"): die aktive Lernauswahl zieht bis zu
12 Frames pro Clip aus nur 43 Pool-Clips (nahe am Cap), während der Champion mit 322
Trainings-Frames aus 46 Clips (Median 7/Clip, duration-proportional über den ganzen
60-Clip-Pool) bei geringerer Rohframe-Zahl eine deutlich diversere Stichprobe erhielt. Mehr
korrelierte Frames aus wenigen Clips scheinen weniger effektives Trainingssignal zu liefern
als weniger, aber breiter gestreute Frames — eine Größenordnung, die die 30-Epochen/
Feintuning-Differenz und die Startgewichte-Differenz beide überschattet.

**Offener Nebenbefund (nicht Teil dieser Ablation, für Iteration 2 relevant):** der Champion
— nie auf einem einzigen GoPro/Hinterfeld-Frame trainiert, reines Zero-Shot-Domain-Transfer
von der Drohne — schlägt Iteration 1 (200 echte GoPro-Trainings-Frames) auf GoPro/Hinterfeld
in jeder Metrik (`mAP_50` 0,7971 vs. 0,7248, `mAP_50_95` 0,7813 vs. 0,5292). Ablation B
(Champion-Feintuning, volles v1.2) verbessert GoPro/Hinterfeld spürbar (0,7644/0,5655),
bleibt aber unter dem reinen Zero-Shot-Champion. Das spricht dafür, dass echtes
GoPro-Trainingsmaterial in der aktuellen Rezept-Form (kein Val-Split, Multi-Domain-Batch,
COCO-Pretrain-Start) die Zero-Shot-Generalisierung des Champions nicht zuverlässig übertrifft
— nicht innerhalb dieser Ablation aufgelöst (keine GoPro-only-Ablation im Drei-Läufe-Budget),
als offene Frage für Plan 02.2-16/17 markiert.

**Konsequenz für Plan 02.2-16 (Iteration 2):** die Iteration-2-Auswahl sollte die
Frame-Diversität pro Clip explizit erhöhen statt nur mehr Frames zu ziehen — z. B. mehr
distinkte Clips pro Domäne einbeziehen (falls der Pool das hergibt) oder den Per-Clip-Cap für
die Uncertainty-Selektion senken, damit AL-2 nicht erneut nahe am 12-Frame-Deckel pro Clip
landet. Die Startgewichte-Frage (Kandidat 4) ist ebenfalls real: `train_detector`s neue
`--init-weights`-Option (Commit `94f0572`) steht für Iteration 2 zur Verfügung, sollte aber
nicht als alleinige Lösung erwartet werden. `champion` und `hackathon-frozen` bleiben beide
unverändert auf `87a8a5222f7a472787875e974d089c44` — kein Alias wurde bewegt. Vollständiger
Bericht: `.planning/phases/02.2-dataset-buildout/02.2-DIAG-SUMMARY.md`.

### Nachtrag 2026-09-04 (Diagnose, Korrektur): Der Champion selbst ist nicht sauber Held-out — Ablation D

**Befund des Koordinators:** Der obige Vergleich hat ein Loch. Der Champion (`87a8a522…`)
ist auf den eingefrorenen Eval-Clips selbst nicht held-out: sein Pilot-Trainingsset enthält
88 Train- und 25 Val-Frames aus genau den 18 eingefrorenen Drohnen-Clips (`### Korrektur
2026-09-04 (Koordinator)` oben) — die Eval-GT-Frames sind andere Frame-Indizes derselben
Clips, also zeitlich nahe Near-Duplikate dessen, worauf der Champion trainiert hat. v1.2/
Ablation A/B enthalten dagegen keinen einzigen dieser 18 Clips. Das, plus der dokumentierte
Vorlabel-Bias (95 % der geprüften Drohnen-Boxen sind unveränderte Champion-Vorlabels), kann
die gesamte Drohnen-Lücke erklären, ganz ohne einen Frame-Dichte-Effekt.

**Ablation D — "sauberer Champion":** exakt das Piloten-Rezept (korrigierter Piloten-Datensatz,
304 Bilder, 30 Epochen, 896 px, Batch 4, Grad-Accum 4, MPS, dieselbe Clip-Level-Split-Logik wie
beim Champion — Split-Zuordnung für überlebende Clips unverändert aus dem Original-Manifest
übernommen, nicht neu gewürfelt), aber mit jedem Frame aus den 18 eingefrorenen Drohnen-Clips
aus Train UND Val entfernt. Tatsächlich entfernt: 76 von 304 korrigierten Bildern (nicht die
zuvor grob geschätzten ~291/43 Clips — diese Zahl war die Schätzung auf Manifest-Sample-Ebene
vor Korrektur-Filterung, nicht die tatsächlich korrigierten Bilder). Verbleibend: **228 Bilder,
33 Clips**, Split 181 Train / 47 Val (20,6 % — nahe am ursprünglichen 20 %-Design). Per
`assert_no_frozen_eval_clips` verifiziert: 0 Überschneidung mit den eingefrorenen Eval-Clips.
Da 228 Bilder unter `validate_coco`s Standard-Untergrenze `_MIN_IMAGES = 250` liegt (die der
Champion mit 304 Bildern noch komfortabel erfüllte), wurde `dataset._MIN_IMAGES` für diesen
einen Diagnoselauf per `monkeypatch.setattr`-Mechanismus auf 200 gesenkt — genau der
Mechanismus, den `dataset.py`s eigener Docstring für exakt diesen Fall vorsieht (Laufzeit-Lookup
des Moduls-Attributs statt Def-Time-Default), keine Änderung an `train_detector`s öffentlicher
Signatur für einen einzelnen Ablationslauf. MLflow-Lauf `a6d53662e6fa4df88d10debd1551de6b`
(`checkpoint_source = best_total` — der Val-Split griff).

**Ergebnis auf der geprüften Held-out-Ground-Truth:**

| Lauf | Domäne | n Bilder | mAP_50 | mAP_50_95 | AP_player | AP_referee |
|---|---|---:|---:|---:|---:|---:|
| Champion 2.1 (`87a8a522…`, **nicht sauber held-out**) | Drohne | 90 | 0,9550 | 0,9423 | 0,9520 | 0,9325 |
| **Ablation D (`a6d53662…`, sauberer Champion)** | Drohne | 90 | **0,9030** | **0,7847** | 0,8110 | 0,7583 |
| Iteration-1 v1.2 (`be854a1a…`) | Drohne | 90 | 0,8881 | 0,7073 | 0,7134 | 0,7013 |
| Ablation A (Drohne-only + Val-Split) | Drohne | 90 | 0,8884 | 0,7076 | 0,7223 | 0,6929 |
| Ablation B (Champion-Feintuning, v1.2 voll) | Drohne | 90 | 0,8926 | 0,7283 | 0,7208 | 0,7359 |
| Champion 2.1 (Zero-Shot, nie auf GoPro trainiert) | GoPro/Hinterfeld | 72 | 0,7971 | 0,7813 | 0,7016 | 0,8610 |
| Ablation D (Drohne-only, Zero-Shot auf GoPro) | GoPro/Hinterfeld | 72 | 0,6255 | 0,5264 | 0,5394 | 0,5135 |
| Iteration-1 v1.2 | GoPro/Hinterfeld | 72 | 0,7248 | 0,5292 | 0,5952 | 0,4633 |
| Ablation B (v1.2 voll, Champion-Feintuning) | GoPro/Hinterfeld | 72 | 0,7644 | 0,5655 | 0,6046 | 0,5265 |

**Frame-Dichte pro Clip, D vs. v1.2-Drohne (derselbe Vergleich wie oben, jetzt für D statt für
den vollen Piloten):**

| Datensatz | n Bilder | n Clips | Frames/Clip Median | Min | Max |
|---|---:|---:|---:|---:|---:|
| Ablation D (Piloten-Rezept ohne die 18 Eval-Clips) | 228 | 33 | 7 | 3 | 12 |
| v1.2-Drohne (AL-1) | 450 | 43 | 12 | 4 | 12 |
| Piloten-Datensatz voll (zum Vergleich, inkl. Leck) | 304 | 46 | 7 | 3 | 12 |

D behält das Dichte-Profil des vollen Piloten fast unverändert (Median 7 vs. 7, nur die 18
Eval-Clips fehlen) — der Dichte-Kontrast zu v1.2 (Median 12, nahe am Cap) bleibt also bestehen,
unabhängig von der Leckage-Frage.

**Ehrliche, revidierte Schlussfolgerung — beide Effekte sind real, in unterschiedlichem
Ausmaß:**

Die Rechnung mit den jetzt vorliegenden Zahlen: von der ursprünglich berichteten Drohnen-Lücke
(Champion 0,9550/0,9423 vs. Iteration-1 0,8881/0,7073 → Δ 0,0669 mAP_50 / 0,2350 mAP_50_95)
schliesst Ablation D (der saubere Champion, 0,9030/0,7847) **77,7 % der `mAP_50`-Lücke und
67,1 % der `mAP_50_95`-Lücke** — das ist die Leckage- plus Vorlabel-Bias-Erklärung des
Koordinators, und sie trägt tatsächlich den **überwiegenden Teil** der ursprünglich berichteten
Differenz. Das war kein kleiner Vorbehalt, sondern der Hauptfaktor.

Aber selbst nach vollständiger Entfernung der Leckage bleibt ein echter, sauber
held-out-gemessener Rest: Ablation D (0,9030/0,7847) liegt weiterhin **über** Iteration-1
(0,8881/0,7073, Δ 0,0149 mAP_50 / 0,0774 mAP_50_95) und über Ablation A (0,8884/0,7076, Δ
0,0146/0,0771) — bei nur 228 statt 450 bzw. 450 Trainingsbildern. Da Ablation A bereits
sowohl "kein Val-Split" als auch "Multi-Domain-Mix" als Erklärung für diesen Rest widerlegt
hat (A hat beides behoben und blieb bei Iteration-1s Niveau), und D denselben Val-Split-Mechanismus
wie der Champion nutzt (`checkpoint_source = best_total`) ohne den Rest zu schliessen, bleibt
die **Frame-Diversität pro Clip** die plausibelste Erklärung für diesen kleineren, aber realen
Rest: D zieht 228 Bilder aus 33 Clips bei Median 7/Clip (fast identisch mit dem ungekürzten
Piloten), v1.2/Iteration-1/A ziehen 450 Bilder aus nur 43 Clips bei Median 12/Clip (nahe am
Cap) — mehr Rohbilder, aber dichter konzentriert auf weniger, dafür stärker korrelierte Clips.

**Zusammengefasst:** Die Leckage im Champion-Vergleich erklärt den **überwiegenden Teil**
(rund zwei Drittel bis drei Viertel, je nach Metrik) der ursprünglich berichteten Drohnen-Lücke
— es handelt sich nicht um eine echte Modell-Regression in diesem Ausmass. Ein kleinerer, aber
auf sauberer Held-out-Basis real gemessener Rest (~0,015 `mAP_50`, ~0,077 `mAP_50_95`) bleibt
bestehen und ist am ehesten der Frame-Dichte/-Diversität pro Clip zuzuschreiben, nicht dem
Val-Split, nicht dem Multi-Domain-Mix und nur geringfügig den Startgewichten (Ablation B). Für
GoPro/Hinterfeld liefert D einen zusätzlichen, nicht überbewerteten Datenpunkt: Ds
Zero-Shot-Transfer auf GoPro (0,6255/0,5264) ist sogar schwächer als der des (leckenden)
Champions (0,7971/0,7813) — vermutlich schlicht wegen der kleineren Trainingsmenge (228 vs.
304 Bilder), nicht weiter aufgelöst innerhalb dieser Diagnose.

**Konsequenz für Plan 02.2-16 (Iteration 2), revidiert:** Iteration 1 ist **keine dramatische
Regression** gegenüber einem sauber gemessenen Champion-Äquivalent — der ursprünglich
berichtete Abstand war grossteils ein Messartefakt der eigenen Leckage des Champions. Die
Frame-Diversitäts-Empfehlung von oben (mehr distinkte Clips statt mehr Frames pro Clip, Cap
für die Uncertainty-Selektion prüfen) bleibt gültig, aber als Verbesserung im kleinen,
einstelligen-Prozentpunkt-Bereich einzuordnen, nicht als Behebung eines grossen Defekts. Kein
Alias wurde bewegt; `champion`/`hackathon-frozen` bleiben auf `87a8a5222f7a472787875e974d089c44`.

## Zweck & Abgrenzung

Dieses Dokument ist der laufende Ausführungs-Nachweis der Active-Learning-Iterationen, die
`docs/dataset-plan.md` vorschreibt: was pro Iteration tatsächlich gezogen, vorgelabelt und in
CVAT hochgeladen wurde, mit welchem Seed, mit welchen Zahlen. `docs/dataset-plan.md` fixiert die
Zielwerte vor der Ausführung; dieses Dokument berichtet, was die Ausführung davon eingehalten hat
— dieselbe Trennung von Plan und Ausführungsprotokoll wie zwischen `docs/pilot-gate-decision.md`s
Kriterien und `docs/cv-setup.md`s Umgebungsnachweis.

Nicht Teil dieses Dokuments: die eigentliche CVAT-Korrektursitzung, die DVC-Versionierung und die
Datensatz-Validierung (Plan 02.2-13, ergänzt diesen Abschnitt nach der Sitzung) sowie die zweite
AL-Iteration (Plan 02.2-17, eigener Abschnitt unten in einer späteren Fassung).

## Iteration 1

### Ziel-Ableitung

Aus `docs/dataset-plan.md` (`## 1`/`## 2`) und der `<interfaces>`-Formel des ausführenden Plans:

```
target_iteration_1 = ceil((floor_total - seed_frames) / 2)
                    = ceil((1500 - 0) / 2)
                    = 750
```

`floor_total = 1500` ist der verbindliche REQ-S2-03-Floor. `seed_frames = 0`, weil der
Piloten-Seed (304 Frames, Drohne) laut `docs/dataset-plan.md`s Seed-Set-Prüfung (`## 6`, Verdikt
`nicht übernommen`) nicht in die Drohnen-Frame-Zielzahl eingerechnet wird — die 900 Drohnen-Frames
dort sind bereits vollständig neu zu labelnde AL-Frames, keine Korrektur war nötig.

Die 750 Iteration-1-Frames verteilen sich auf die drei Domänen exakt nach dem in
`docs/dataset-plan.md` `## 1` fixierten Mix (Drohne 60 % / GoPro-Hinterfeld 26,7 % / TV-Broadcast
13,3 %), was — da der Gesamt-Floor pro Domäne glatt durch 2 teilbar ist (900/400/200) — exakt der
halben Domänen-Zielzahl entspricht:

| Domäne | Domänen-Floor (`docs/dataset-plan.md`) | Iteration-1-Ziel | Tatsächlich gezogen |
|---|---:|---:|---:|
| Drohne (`drone`) | 900 | 450 | 450 |
| GoPro/Hinterfeld (`sideline`) | 400 | 200 | 200 |
| TV/Broadcast (`broadcast`) | 200 | 100 | 100 |
| **Summe** | **1500** | **750** | **750** |

**Seed:** `20260516` (derselbe Seed wie `data/reference/frozen_eval_clips.csv`, aus
Nachvollziehbarkeitsgründen wiederverwendet, nicht weil ein technischer Zwang dazu bestünde —
`select_al_frames` akzeptiert jeden Integer-Seed unabhängig vom Eval-Split-Seed).

### Ausführung: `ffep cv active-learn`

Jede Domäne wurde als eigener CLI-Aufruf gezogen (eine Domäne = eine Session in
`video_inventory.csv`), wie in der `<interfaces>`-Vorgabe des ausführenden Plans festgehalten —
ein einzelner Mehr-Domänen-Aufruf hätte die Domänen-Aufteilung der proportionalen
Diversitäts-Allokation überlassen, statt dem hier fixierten Mix zu folgen:

```bash
ffep cv active-learn --iteration 1 --target 450 --seed 20260516 \
  --session 2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE \
  --out-dir data/labels/al-iteration-1/drone

ffep cv active-learn --iteration 1 --target 200 --seed 20260516 \
  --session 2026-08-14_WC-GER-vs-MEX-GOPRO \
  --out-dir data/labels/al-iteration-1/sideline

ffep cv active-learn --iteration 1 --target 100 --seed 20260516 \
  --session 2026-08-14_WC-USA-vs-AUS-TV \
  --out-dir data/labels/al-iteration-1/broadcast
```

Jeder Aufruf schreibt `<out-dir>/selection_manifest.json` (das `ALSelection`-Format:
Sessions, Iteration, Ziel, Seed, Frames mit Uncertainty-Score und Diversity-Key) sowie neu in
dieser Ausführung `<out-dir>/manifest.json` — eine `FrameSampleManifest`-Brücke
(`active_learning.py::selection_to_frame_manifest`), die für `ffep cv prelabel`/`ffep cv dataset`
nötig ist, da diese Befehle das `FrameSampleManifest`-Format erwarten, nicht `ALSelection`s
eigenes Schema. Beide Dateien liegen unter `data/labels/` (git-ignoriert, PII).

### Pool-Sicherheit (T-2.2-32)

Verifikation nach jeder Ziehung: die Schnittmenge zwischen den gezogenen Clip-Nummern und den
`role = frozen_eval`-Zeilen von `data/reference/frozen_eval_clips.csv` ist für jede Domäne leer.

```python
selected_clips = {f['clip_number'] for f in selection['frames']}
frozen = {row clip_number for row in frozen_eval_clips.csv if domain matches and role == 'frozen_eval'}
selected_clips & frozen
```

| Domäne | Gezogene Clips (Schnittmenge mit `frozen_eval`) |
|---|---|
| Drohne | `set()` — leer |
| GoPro/Hinterfeld | `set()` — leer |
| TV/Broadcast | `set()` — leer (TV hat noch keinen eingefrorenen Eval-Split, `## 8` in `docs/dataset-plan.md`; der gesamte Pool ist `role = pool`) |

### Ergebnis pro Domäne

| Domäne | Frames gezogen | Distinkte Clips (von Pool-Clips) | Max Frames/Clip | Uncertainty min / median / max |
|---|---:|---:|---:|---|
| Drohne | 450 | 43 (von 43) | 12 | 0.232 / 0.323 / 0.620 |
| GoPro/Hinterfeld | 200 | 40 (von 48) | 11 | 0.597 / 0.724 / 1.000 |
| TV/Broadcast | 100 | 41 (von 51) | 9 | 0.422 / 0.463 / 0.878 |

Drohne deckt jeden einzigen Pool-Clip mindestens einmal ab (43/43) — plausibel bei
`target / per_clip_cap = 450 / 12 = 37,5`, mit der Zwei-Hover-Positionen-Stratifizierung, die
jede Gruppe mindestens einmal bedient. GoPro/Hinterfeld und TV/Broadcast liegen beide deutlich
höher in der Uncertainty-Verteilung als Drohne (Median 0,72 bzw. 0,46 gegenüber 0,32) — der
erwartete Domain-Shift-Effekt: der champion-Detektor wurde bisher ausschließlich auf Drohnen-Frames
feingetunt, sieht die beiden neuen Domänen also zum ersten Mal. GoPro/Hinterfeld erreicht sogar
den theoretischen Maximalwert 1,0 (leere Detektion) bei mindestens einem gezogenen Frame — exakt
das Signal, das die Uncertainty-Auswahl in einer neuen Domäne suchen soll, kein Fehlschlag.

### Vorlabeln mit dem feingetunten Detektor

```bash
ffep cv prelabel --frames data/labels/al-iteration-1/<domain> \
  --out data/labels/al-iteration-1/<domain>-prelabel --backend finetuned
```

`--backend` existierte an der CLI-Oberfläche vorher nicht (`prelabel_frames` akzeptierte den
Parameter bereits seit Plan 02.2-09, `cv/commands.py`s `prelabel`-Befehl exponierte ihn aber
nicht) — als blockierender Fund während dieser Ausführung ergänzt (siehe SUMMARY). Jeder Lauf
bestätigt im Log `backend=finetuned`, nie eine `transformers`/Grounding-DINO-Zeile — kein
Zero-Shot-Fallback möglich, da `--backend` das explizit erzwingt.

| Domäne | Frames | Boxen gesamt | Boxen/Frame | `player` | `referee` | Frames ohne Detektion | Laufzeit |
|---|---:|---:|---:|---:|---:|---:|---:|
| Drohne | 450 | 8650 | 19,22 | 7795 | 855 | 0/450 | 13,6 s |
| GoPro/Hinterfeld | 200 | 816 | 4,08 | 498 | 318 | 11/200 | 6,3 s |
| TV/Broadcast | 100 | 1375 | 13,75 | 1291 | 84 | 0/100 | 3,4 s |

GoPro/Hinterfeld fällt gegenüber den beiden anderen Domänen deutlich ab (4,08 Boxen/Frame
gegenüber 19,22 bzw. 13,75) und zeigt einen für diese Domäne auffällig hohen `referee`-Anteil
(318 von 816 Boxen, ca. 39 % — gegenüber ca. 10 % bei Drohne). Beides ist konsistent mit
Domain-Shift auf einen Detektor, der nur Drohnen-Perspektiven kennt: die Hinterfeld-/
Endzone-Ansicht (siehe `docs/material-sighting.md`s Geometrie-Korrektur — `sideline` ist keine
Seitenansicht) unterscheidet sich am stärksten vom Trainingswinkel, und 11 von 200 Frames liefern
gar keine Detektion. Das ist genau der Bestätigungs-statt-Neuzeichnen-Fall, den die Vorlabelung
abfangen soll — bei GoPro/Hinterfeld wird die Nutzerin spürbar mehr Boxen selbst nachtragen müssen
als bei Drohne oder TV.

### CVAT-Aufgaben

Push via `ffep cv cvat-push --coco <domain>-prelabel --name al-1-<domain> --max-images 300` gegen
den lokalen, ausschließlich auf Loopback erreichbaren CVAT-Stack (`config.cv.cvat_host =
http://localhost:8080`, T-2.2-33). `--max-images` existierte vorher nicht (`create_cvat_task`
pusht immer genau ein Verzeichnis als eine Aufgabe) — als blockierender Fund ergänzt
(`dataset.py::split_coco_for_task_upload`, siehe SUMMARY), notwendig für die
"höchstens 300 Frames pro Aufgabe"-Vorgabe aus der Phase-2.1-Erfahrung (eine einzelne
404-Frame-Aufgabe überschritt eine Sitzung; die Nutzerin stoppte bei 304, siehe
`docs/cv-setup.md`s Labeling-Konvention-Abschnitt).

| CVAT-Task-ID | Name | Frames |
|---:|---|---:|
| 2 | `al-1-drone-1` | 300 |
| 3 | `al-1-drone-2` | 150 |
| 4 | `al-1-sideline-1` | 200 |
| 5 | `al-1-broadcast-1` | 100 |

Alle vier Aufgaben in CVAT bestätigt (`GET /api/tasks`, Status `annotation`) mit exakt
übereinstimmenden Annotations-Zahlen auf der CVAT-Seite (Task 2: 5917 Shapes, Task 3: 2733,
zusammen 8650 — deckungsgleich mit der Drohnen-Vorlabel-Box-Zahl oben; Task 4: 816; Task 5: 1375).
Keine Zugangsdaten wurden in irgendeiner Befehlsausgabe dieser Sitzung ausgegeben
(`secret()`-Auflösung, nie ein Literal in `ffep.toml` oder Log).

## Labelling-Anleitung Iteration 1

Verbindliche Konvention, wörtlich aus `docs/cv-setup.md`s `### Datensatz` →
"Labeling-Konvention" übernommen (galt für den Piloten, gilt unverändert für alle drei
Iteration-1-Domänen):

1. Jede klar sichtbare Person wird geboxt.
2. Nur Personen mit einer aktiven Schiedsrichterrolle auf dem Feld erhalten das Label `referee`.
3. **Alle** anderen Personen — inklusive Trainerstab, Ersatzspielerinnen und
   Seitenlinien-Personal — erhalten `player`, nicht etwa eine dritte Klasse oder werden
   ausgelassen. Die räumliche Filterung (wer tatsächlich auf dem Feld ist) passiert
   stromabwärts in Feldkoordinaten, nicht bereits beim Boxen.
4. Boxen umschliessen den vollständig sichtbaren Körper inklusive Gliedmassen.
5. Die Boxen-Unterkante sitzt eng an den Füssen, der Schatten wird ausgeschlossen — der
   Fusspunkt ist der Punkt, der später per Homographie in Feldkoordinaten projiziert wird.

**Domänen-spezifische Ergänzungen aus der Sichtung** (`docs/material-sighting.md`):

- **Kleinere scheinbare Spielergrösse:** GoPro/Hinterfeld misst p50 = 27,0 px, TV/Broadcast
  p50 = 23,0 px, gegenüber der Drohnen-Domäne mit p50 = 30,0 px. Alle drei bleiben über der
  20-px-Schwelle (`resolution = 896`, `sahi = false` gilt unverändert für alle drei), aber Boxen
  sind entsprechend kleiner und die Fuss-/Schatten-Trennung (Regel 5) braucht bei GoPro/Hinterfeld
  und TV/Broadcast eine Spur mehr Sorgfalt als bei Drohne.
- **GoPro/Hinterfeld zeigt das eigene Team (GER vs. MEX)** aus einer Hinterfeld-/
  Endzone-Perspektive (keine echte Seitenansicht, siehe Geometrie-Korrektur in
  `docs/material-sighting.md`) — Trainerstab und Ersatzspielerinnen am Spielfeldrand sind in
  dieser Kameraposition sichtbar dichter im Bild als bei der Drohnen-Aufsicht; Regel 3 (alles
  `player`, keine dritte Klasse) trägt das bereits ab, verdient hier aber die ausdrückliche
  Erinnerung, weil es in dieser Domäne häufiger vorkommt als bei Drohne.
- **TV/Broadcast zeigt ein fremdes Spiel (USA vs. AUS)** aus der tatsächlichen Seitenansicht —
  keine eigene Trainerstab-/Spielerinnen-Wiedererkennung nötig oder möglich, reine
  Objekt-Geometrie-Aufgabe wie bei den anderen beiden Domänen.
- Frames dieser Iteration sind durchgehend als Rechteck zu boxen (kein Polygon-Modus) — der
  Polygon-zu-Rechteck-Übergang aus der Piloten-Sitzung (`docs/cv-setup.md`, Frame ~103) betraf
  nur die damalige CVAT-Konfiguration, nicht diese Aufgaben.

**Sitzungsbudget (D-16):** ~1 Wochenende pro AL-Iteration. 750 Frames über vier Aufgaben (300 +
150 + 200 + 100), jede einzelne Aufgabe innerhalb der aus der Piloten-Sitzung gelernten
Aufmerksamkeits-Obergrenze (die Nutzerin stoppte damals bei 304 von 404 Frames in einer einzigen
Aufgabe) — die Aufteilung ist bewusst so gewählt, dass jede einzelne Aufgabe für sich in einem
Sitzungsblock abschliessbar ist, nicht nur die Summe über das Wochenende.

**Verifikationsgrad (D-17):** 100 % der 750 Frames werden von der Nutzerin in CVAT gesichtet und
korrigiert/bestätigt — die Vorlabels aus dem feingetunten Drohnen-Detektor (überwiegend hohe
Trefferquote bei Drohne und TV/Broadcast, spürbar mehr Korrekturaufwand bei GoPro/Hinterfeld,
siehe oben) machen das überwiegend zu Bestätigungsarbeit, ersetzen aber nicht die Prüfung jedes
einzelnen Frames — kein Frame gilt als "verifiziert", ohne dass ein Mensch es tatsächlich gesehen
hat.

### Nachtrag 2026-09-02: GoPro-Fernfeld wird übersprungen (Nutzerentscheid während der Sitzung)

Nach den ersten 8 GoPro-Frames meldete die Nutzerin: Vorlabels leer, Fernfeld stark verpixelt.
Die Sichtung der neuen Sessions (`docs/material-sighting.md`, Abschnitt 2026-09-02) bestätigt den
Mechanismus: GoPro-Spielerinnen sind im nahen/mittleren Feld so gross wie auf der Drohne
(p50 28–32 px), nur das Fernfeld fällt auf 25–27 px ab. Vereinbarung:

- Die Aufgabe `al-1-sideline-1` wird **nur für Frames mit Spielerinnen im nahen/mittleren
  Feldbereich** korrigiert; Fernfeld-Frames bleiben **unberührt** (kein Kasten, kein Tag).
  Zielgrösse ~50–80 saubere GoPro-Frames statt 200 — genug, damit Iteration 2 brauchbare
  Vorlabels liefert.
- **Merge-Regel für Plan 02.2-13:** ein Sideline-Frame mit 0 Boxen ist ein *übersprungener*
  Frame, kein echtes Negativ (jeder AL-Frame stammt aus einem laufenden Spielzug mit 10
  Spielerinnen auf dem Feld). Solche Frames werden vor der Validierung getrimmt, nicht als leere
  Frames ins Dataset übernommen. Die 8 bereits korrigierten Frames bleiben.
- **Iteration 2 (Plan 02.2-16):** die GoPro-Auswahl zieht nur aus nahen/mittleren Feldzonen
  (`field_zone_bucket`), das Fernfeld wird ausgeschlossen. Die GoPro-Domäne bleibt vorerst
  Trainingsdomäne; die Ablation aus dem D-11-Verdikt entscheidet weiterhin über ihren Verbleib.

### Korrektursitzung: Ergebnis (Plan 02.2-13, Task 1)

Die Nutzerin meldete am 2026-09-02 alle fünf CVAT-Aufgaben (`al-1-drone-1`, `al-1-drone-2`,
`al-1-sideline-1`, `al-1-broadcast-1`) als "gelabelt" gemäss D-16/D-17 — jede Aufgabe wurde
vollständig gesichtet, mit der oben stehenden Konvention korrigiert oder bestätigt, ausser dem
oben dokumentierten, ausdrücklich vereinbarten Fernfeld-Überspringen bei GoPro/Hinterfeld.

Vergleich Vorlabel → korrigierte CVAT-Aufgabe (Box-Zahlen), gepullt via `ffep cv cvat-pull`:

| Aufgabe | Bilder | Boxen Vorlabel | Boxen korrigiert | `player` | `referee` |
|---|---:|---:|---:|---:|---:|
| `al-1-drone-1` | 300 | — | 6064 | 5457 | 607 |
| `al-1-drone-2` | 150 | — | 2795 | 2496 | 299 |
| Drohne gesamt | 450 | 8650 | 8859 | 7953 | 906 |
| `al-1-sideline-1` | 200 | 816 | 903 | 576 | 327 |
| `al-1-broadcast-1` | 100 | 1375 | 1408 | 1270 | 138 |

Die Box-Zunahme bei allen drei Domänen (Drohne +209, GoPro/Hinterfeld +87, TV/Broadcast +33)
bestätigt, dass tatsächlich korrigiert und nicht nur unverändert bestätigt wurde. Ein
Datei-für-Datei-Diff gegen die jeweiligen Vorlabel-`instances.json` (gleicher Dateiname pro
Frame) zeigt zusätzlich: bei GoPro/Hinterfeld unterscheiden sich 104 von 200 Frames von ihrem
Vorlabel (echte Korrektur, nicht nur Bestätigung), 96 sind unverändert — konsistent mit einer
deutlich über die anfänglich gemeldeten "8 Frames" hinausgehenden Sitzung, wie unten in der
Trim-Tabelle sichtbar.

### Merge & Validierung (Plan 02.2-13, Task 2)

Alle vier Aufgaben per `ffep cv cvat-pull --task <id> --out <dir>` gezogen (CVAT-Task-IDs 2-5,
siehe `## Iteration 1` → `### CVAT-Aufgaben` oben), dann domänenweise (Drohne: Aufgabe 2+3
zusammengeführt) mit den jeweiligen `manifest.json`-Dateien aus `data/labels/al-iteration-1/
<domain>/` abgeglichen und in das eine wachsende Verzeichnis `data/labels/dataset/` gemergt
(Bild- und Annotations-IDs neu durchnummeriert, Bilddateien domänen-präfixiert, um
Dateinamens-Kollisionen zwischen Sessions zu vermeiden — z. B. taucht `Wide - Clip 001_f00000.jpg`
sowohl in der Drohnen- als auch in der GoPro-Session auf).

**Piloten-Seed (304 Frames) nicht übernommen:** gemäss dem in `docs/dataset-plan.md` `## 6`
festgehaltenen Verdikt `nicht übernommen` (die 1.500-Frame-Zielzahl ist bereits ohne den
Piloten-Seed geschrieben) fliesst der Piloten-Datensatz nicht in `data/labels/dataset/` ein —
Iteration 1 startet die wachsende Datei bei null.

**Merge-Regel angewendet (GoPro-Fernfeld-Trim, siehe Nachtrag oben):**

| Domäne | Frames im Manifest | Übersprungen (0 Boxen) | Übernommen |
|---|---:|---:|---:|
| Drohne | 450 | 0 | 450 |
| GoPro/Hinterfeld | 200 | 11 | 189 |
| TV/Broadcast | 100 | 0 | 100 |
| **Summe** | **750** | **11** | **739** |

Die 11 übersprungenen GoPro-Frames sind exakt dieselben 11 Frames, die bereits beim Vorlabeln
keine Detektion hatten (Mengenvergleich vor/nach Korrektur: identische Dateimenge) — das
bestätigt, dass es sich tatsächlich um das unberührte Fernfeld handelt, nicht um vom Menschen
bewusst leer bestätigte Frames (die Regel "0 Boxen = übersprungen, nie echtes Negativ" trifft
also exakt die richtige Teilmenge). Die 189 übernommenen GoPro-Frames liegen über der in der
Nachtrags-Vereinbarung genannten Zielgrösse "~50–80 saubere Frames" — die Sitzung ging nach dem
ursprünglich gemeldeten Stopp-Punkt bei 8 Frames erkennbar weiter (104 von 189 übernommenen
Frames unterscheiden sich von ihrem Vorlabel, siehe Tabelle oben); dies wird hier ehrlich
berichtet, nicht nachträglich auf die ursprüngliche Schätzung zurechtgestutzt. Der volle,
ungetrimmte Export bleibt unverändert unter `data/labels/al-iteration-1/cvat-export/sideline/
instances.json` (200 Bilder, git-ignoriert) erhalten, analog zum `instances.full-404.json`-
Präzedenzfall der Piloten-Sitzung (`docs/cv-setup.md` → `### Datensatz`).

**Ausschluss-Assertionen (T-2.2-Bezug, per Skript geprüft):**

- Puerto Rico (`2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE`, die private Testpartie) taucht
  in keinem der drei Iteration-1-Sessions auf (Drohne: Panama Rojo, GoPro: GER-MEX, Broadcast:
  USA-AUS) — 0 Treffer bei einer direkten Pfad-Suche über alle 739 gemergten Frames.
  Puerto Rico war nie Teil der AL-1-Auswahl (`docs/dataset-buildout.md` `## Iteration 1` →
  `### Ausführung`).
  Puerto Rico Ausschluss zusätzlich sessionscharf gegen `data/reference/frozen_eval_clips.csv`
  geprüft: keine Schnittmenge.
- `data/reference/frozen_eval_clips.csv` (`role = frozen_eval`, 30 Zeilen über die drei
  Domänen): sessionscharfer Abgleich (Domäne + `session_id` + `clip_number`) gegen alle 739
  gemergten Frames ergibt eine leere Schnittmenge — kein eingefrorener Eval-Clip ist im
  Trainingsdatensatz gelandet.

**Validierung** (`ffep cv dataset --coco data/labels/dataset --manifest
data/labels/dataset/manifest.json --min-images 1 --max-images 3000`):

| Kennzahl | Wert |
|---|---|
| `n_images` | 739 |
| `player`-Boxen | 9799 |
| `referee`-Boxen | 1371 |
| Bilder ohne Annotation (`_empty_images`) | 0 |
| Split `train` | 739 Bilder (kein `val` — AL-Iterationen liefern ausschliesslich Trainingsmaterial, die Evaluierung läuft separat über den eingefrorenen Eval-Split, `02.2-11-SUMMARY.md`s Entscheidungsabschnitt) |
| `content_sha256` | `e27c1b60d60e240d8f6bc9d4b6b2cd276b135776cb2cd812ff36ff6661fabb8b` |

`boxes_by_domain` (jede Domäne trägt mindestens eine `player`-Box, von `validate_coco` erzwungen):

| Domäne | `player` | `referee` | Bilder ohne Annotation |
|---|---:|---:|---:|
| Drohne | 7953 | 906 | 0 |
| GoPro/Hinterfeld | 576 | 327 | 0 |
| TV/Broadcast | 1270 | 138 | 0 |

**Ehrlicher Stand gegen den 1.500-Floor:** 739 von 1.500 (49 %) — **erwartet unterhalb des
Floors**, kein Fehlschlag. `docs/dataset-plan.md` `## 1`/`## 2` legt den 1.500-Floor über beide
AL-Iterationen zusammen fest (750 pro Iteration bei zwei Iterationen); Iteration 1 allein war nie
als floor-erreichend geplant. Die GoPro-Domäne liegt mit 189 von den ursprünglich geplanten 200
Iteration-1-Frames deutlich unter ihrem Anteil (94 % statt 100 %, wegen des Fernfeld-Trims) — der
Domänen-Mix nach Iteration 1 ist damit Drohne 450/900 (50 %), GoPro/Hinterfeld 189/400 (47 %),
TV/Broadcast 100/200 (50 %). Iteration 2 (Plan 02.2-16/17) muss den GoPro-Rückstand nicht
zwingend proportional aufholen, seit die Nachtrags-Vereinbarung die Zielgrösse dieser Domäne
bereits auf "~50–80 saubere Frames pro Sitzung" statt auf den vollen 200er-Anteil abgesenkt hat.

**CLI-Lücke gefunden und geschlossen (Abweichung, siehe SUMMARY):** `ffep cv dataset` exponierte
bislang keinen Weg, `validate_coco`s `min_images`/`max_images` zu überschreiben — der Befehl
validierte immer gegen das feste Phase-2.1-Einzeldomänen-Band `[250, 600]`, das ein 739-Bilder-
Multi-Domänen-Paket immer als Ceiling-Verstoss abgelehnt hätte. `cv/commands.py`s `dataset`-Befehl
bekam `--min-images`/`--max-images` (Default `None`, bestehendes Verhalten unverändert). Für
diesen Iteration-1-Lauf wurde bewusst `--min-images 1` statt des vollen Multi-Domänen-Floors
`1500` übergeben — die 3000er-Decke gilt sofort und uneingeschränkt (sie darf über keine
Iteration hinweg überschritten werden), der 1.500er-Floor ist dagegen ein kumulatives
Phase-Ziel über beide Iterationen und würde, als hartes Gate auf einen einzelnen
Iteration-1-Lauf angewendet, den Befehl grundlos mit Exit-Code ≠ 0 scheitern lassen, obwohl das
Ergebnis exakt dem Plan entspricht.

### DVC-Versionierung (Plan 02.2-13, Task 3)

`data/labels/dataset/` per `uv run --extra versioning dvc add data/labels/dataset` getrackt.

| Kennzahl | Wert |
|---|---|
| DVC-MD5 (`.dvc`-Datei, `outs[0].md5`) | `b0a33db5bb3269c8fdd594e198dcab9f.dir` |
| `nfiles` (DVC) | 741 (739 Bilder + `instances.json` + `manifest.json`) |
| Projekt-`content_sha256` (`dataset_hash()`) | `e27c1b60d60e240d8f6bc9d4b6b2cd276b135776cb2cd812ff36ff6661fabb8b` |
| Git-Commit des Pointers (`data/labels/dataset.dvc`) | `7b528cd` |

**Zwei Hashes, zwei Zwecke (RESEARCH Pattern 2, siehe auch `docs/cv-setup.md` → `##
Dataset-Versionierung`):** der DVC-MD5 ist DVCs eigene Content-Adressierung für
Push/Pull/Cache — er identifiziert das Verzeichnis für DVCs Datenbewegung. Der
`content_sha256` ist die projekt-interne Reproduzierbarkeits-Prüfsumme, die ein künftiger
Trainingslauf (Plan 02.2-15) als MLflow-Parameter loggt. Beide bleiben nebeneinander bestehen,
keiner ersetzt den anderen.

**`git check-ignore -q data/labels/dataset`** bestätigt: die Nutzdaten bleiben git-ignoriert,
nur `data/labels/dataset.dvc` erscheint in `git status`. `.gitignore` brauchte dafür eine
gezielte Ausnahme (`!data/labels/dataset.dvc`) zur bestehenden `data/labels/*`-Regel — ohne sie
verweigert `dvc add` selbst das Schreiben des Pointers ("bad DVC file name ... is git-ignored"),
da DVC keinen Pointer erzeugt, der von der eigenen Git-Konfiguration sofort wieder verschluckt
würde (Abweichung, siehe SUMMARY; vom Plan selbst vorweggenommen: "commit ... die
`.gitignore`-Ergänzung, die DVC schreibt").

**`dvc push` gegen den echten OTC-OBS-Endpunkt: versucht, wie erwartet fehlgeschlagen.** Der
Platzhalter-Bucket (`ffep-datasets-PLACEHOLDER`) ist nicht bereitgestellt — `403 Forbidden` auf
den ersten `HeadObject`-Aufruf, keine Zugangsdaten hinterlegt. Als lokaler Rückfall (per Plan
so vorgesehen, `.dvc/config.local` — git-ignoriert, kein Teil des Commits) wurde ein
lokal-Verzeichnis-Remote `local-fallback` unter `~/.dvc-local-remote/flag-football-datasets`
konfiguriert und `dvc push -r local-fallback` erfolgreich ausgeführt (742 Dateien, inkl. der
`.dir`-Cache-Datei) — beweist den Push/Pull-Mechanismus gegen das reale 739-Bilder-Datenset,
nicht nur gegen `tests/test_dvc_layout.py`s Wegwerf-Verzeichnis, und legt bereits eine echte
lokale Sicherungskopie der Korrektursitzung an. Der eigentliche `dvc push` gegen den
provisionierten OTC-OBS-Bucket bleibt Plan 02.2-20 vorbehalten (Bucket-Bereitstellung: Plan
02.2-14) — dieser Aufschub blockiert diesen Plan nicht, wie in Task 3s eigenem `<action>`-Block
vorgesehen.

> **Korrigiert am 2026-09-02, siehe `### Korrektur 2026-09-02` unten:** die 189
> GoPro/Hinterfeld-Frames, die die obigen Tabellen als "übernommen" ausweisen, waren zum
> Zeitpunkt dieses Merges tatsächlich überwiegend ungeprüfte Vorlabels, keine
> nutzerin-gesichteten Frames. `data/labels/dataset/` ist inzwischen auf Datensatz v1.1
> berichtigt (558 Bilder). Die Zahlen und Hashes oben beschreiben, was am Merge-Tag
> tatsächlich gebaut wurde (historischer Ausführungsnachweis) — für den aktuellen,
> D-17-konformen Stand gilt ausschliesslich die Korrektur-Sektion.

### Korrektur 2026-09-02: D-17-Verstoss behoben — Datensatz v1.1

**Befund:** Die Nutzerin stellte am 2026-09-02 klar, dass von den 200 GoPro/Hinterfeld-Frames
der Aufgabe `al-1-sideline-1` tatsächlich nur ca. 8 in CVAT gesichtet und gelabelt wurden — die
übrigen Frames tragen weiterhin ungeprüfte Vorlabels des feingetunten Detektors (die 11
Fernfeld-Frames mit 0 Boxen waren bereits vor diesem Befund korrekt als übersprungen getrimmt,
siehe `### Merge & Validierung` oben). Drohne (450) und TV/Broadcast (100) wurden von der
Nutzerin durchgesehen und bleiben unverändert. Der oben unter `### Korrektursitzung: Ergebnis`
dokumentierte Datei-Diff (104/200 abweichend von den Vorlabel-Boxzahlen) hatte Box*zahl*-
Änderungen gezählt, nicht pro Frame verifiziert, ob die Boxen selbst tatsächlich neu gesetzt
wurden — das verdeckte, dass ein grosser Teil der scheinbar "veränderten" Zahl aus wenigen stark
bearbeiteten Frames stammt, während die meisten der 200 Frames unangetastet blieben. Das
verletzt D-17 (100 % Nutzerin-Verifikation): ein Datensatz darf keine Frames enthalten, die nur
ein Vorlabel-Modell, aber nie ein Mensch gesehen hat.

**Diff-Methodik (dieser Korrekturlauf):** pro Domäne wurde jedes Bild aus dem korrigierten
CVAT-Export gegen dasselbe Bild im Vorlabel-COCO (gleicher Dateiname) verglichen — Boxenzahl,
`category_id` und `bbox`-Koordinaten (Toleranz 1,0 px, deckt CVATs eigene
Rundung/Polygon-zu-Rechteck-Konvertierung an unveränderten Boxen ab). Ein Frame gilt als
"berührt", wenn die Boxenzahl abweicht oder mindestens eine Box aus dem Vorlabel keine
Entsprechung im Export findet; sonst als "unberührt" (= identisch zum Vorlabel = nie tatsächlich
bearbeitet, auch wenn die Aufgabe in CVAT als abgeschlossen markiert wurde).

| Domäne | Bilder | Berührt (≠ Vorlabel) | Unberührt (= Vorlabel) | Davon 0-Boxen (Fernfeld, bereits getrimmt) | Behandlung |
|---|---:|---:|---:|---:|---|
| Drohne | 450 | 172 | 278 | 0 | alle 450 bleiben — von der Nutzerin durchgesehen, "unberührt" heisst hier bestätigt, nicht ungesehen |
| TV/Broadcast | 100 | 94 | 6 | 0 | alle 100 bleiben — von der Nutzerin durchgesehen |
| GoPro/Hinterfeld | 200 | **8** | 192 | 11 | nur die 8 berührten Frames bleiben; die 181 unberührten, nicht-Fernfeld Frames werden ausgeschlossen (D-17) |

Die 8 berührten GoPro-Frames sind exakt die ersten acht in Aufnahmereihenfolge (`Wide - Clip
001_f00000` bis `Wide - Clip 002_f00071`) — deckungsgleich mit der im Nachtrag vom 2026-09-02
festgehaltenen Beobachtung "nach den ersten 8 GoPro-Frames meldete die Nutzerin: Vorlabels leer,
Fernfeld stark verpixelt". Bei Drohne und TV/Broadcast bestätigt die hohe Berührt-Quote (38 %
bzw. 94 %) zusammen mit der ausdrücklichen Aussage der Nutzerin, dass diese beiden Domänen
tatsächlich durchgesehen wurden — der niedrigere Drohnen-Anteil ist plausibel, weil der
Drohnen-Detektor (worauf er feingetunt ist) dort die höchste Vorlabel-Trefferquote hatte und
entsprechend am wenigsten Korrektur brauchte.

**Datensatz v1 → v1.1:**

| Kennzahl | v1 (Plan 02.2-13, fehlerhaft) | v1.1 (diese Korrektur) |
|---|---:|---:|
| Bilder gesamt | 739 | 558 |
| Drohne | 450 | 450 (unverändert) |
| TV/Broadcast | 100 | 100 (unverändert) |
| GoPro/Hinterfeld | 189 | **8** |
| `player`-Boxen | 9799 | 9305 |
| `referee`-Boxen | 1371 | 1063 |
| DVC-MD5 (`.dvc`-Datei) | `b0a33db5bb3269c8fdd594e198dcab9f.dir` (741 Dateien) | `1659e351c063750eea94b536eb9f10e1.dir` (560 Dateien) |
| `content_sha256` | `e27c1b60d60e240d8f6bc9d4b6b2cd276b135776cb2cd812ff36ff6661fabb8b` | `82f0feb7c4d678a44bdc7e90be416561bb2e27fabb5a657eb0dc005dbc54fa92` |

181 GoPro/Hinterfeld-Frames wurden per D-17 aus `data/labels/dataset/` entfernt (Bilddateien
gelöscht, `instances.json`/`manifest.json` neu durchnummeriert). Der volle, ungetrimmte
200-Bilder-Export bleibt unverändert und git-ignoriert unter
`data/labels/al-iteration-1/cvat-export/sideline/instances.json` erhalten — eine künftige
GoPro-Nachsitzung kann daraus jeden noch unbearbeiteten Frame nachträglich korrigieren und
mergen, ohne die Ziehung erneut zu machen.

**Ausschluss-Assertionen erneut geprüft (Teilmenge von v1, daher trivial erwartet, aber nicht
angenommen):** Puerto Rico 0 Treffer über alle 558 Frames; `frozen_eval_clips.csv`
(`role = frozen_eval`) 0 Schnittmenge (Domäne + Clip-Nummer) gegen alle 558 Frames.

**Validierung** (`ffep cv dataset --coco data/labels/dataset --manifest
data/labels/dataset/manifest.json --min-images 1 --max-images 3000`): exit 0, 558 Bilder, 9305
`player` + 1063 `referee` Boxen, 0 Bilder ohne Annotation, jede Domäne mit
mindestens einer `player`-Box (Drohne 7953, GoPro/Hinterfeld 82, TV/Broadcast 1270).

**Ehrlicher Stand gegen den 1.500-Floor (korrigiert):** 558 von 1.500 (37 %) — spürbar unter dem
zuvor gemeldeten 739/1500 (49 %), weil dieser Wert vorher auf 181 nicht tatsächlich verifizierten
Frames beruhte. Iteration 2 (Plan 02.2-16/17) muss den GoPro-Rückstand entsprechend deutlicher
aufholen als im ursprünglichen Plan-13-Stand angenommen — die Nachtrags-Zielgrösse "~50–80
saubere Frames pro Sitzung" bleibt die realistische Grundlage dafür, nicht die volle
200er-Ausschreibung.

**DVC:** `dvc add data/labels/dataset` erneut ausgeführt (neuer Pointer, `git status --porcelain
data/labels` zeigt weiterhin nur `data/labels/dataset.dvc`, `git check-ignore -q
data/labels/dataset` bestätigt weiterhin den Ausschluss der Nutzdaten). `dvc push -r
local-fallback` erneut gegen den bestehenden lokalen Rückfall-Remote ausgeführt (3 neue Dateien
— Drohne/Broadcast-Bildinhalte waren bereits im Cache, nur die geänderten JSON-Dateien und der
neue `.dir`-Eintrag sind neu). Der reale OTC-OBS-Push bleibt weiterhin Plan 02.2-20 vorbehalten,
unverändert gegenüber Plan 02.2-13s Stand.

### Nachtrag 2026-09-04: GoPro-Nachsitzung — Datensatz v1.2

**Befund:** Die Nutzerin meldete am 2026-09-04, sie habe "zwischenzeitlich GoPro
nachgelabelt... einige nahe Aufnahmen gelabelt und den Rest so gelassen" — eine weitere,
freiwillige Korrektursitzung auf derselben CVAT-Aufgabe `al-1-sideline-1` (Task-ID 4),
zusätzlich zu den bereits in `### Korrektur 2026-09-02` festgestellten 8 berührten Frames.
Kein Frame gilt als verifiziert, ohne per Datei-Diff bestätigt zu sein — dieselbe Methodik
wie in der Korrektur vom 2026-09-02 wurde erneut angewendet, diesmal gegen einen frischen
`ffep cv cvat-pull --task 4` (statt gegen den zwei Tage alten, zwischenzeitlich veralteten
Export).

**Diff-Methodik (unverändert gegenüber `### Korrektur 2026-09-02`):** pro Bild wird die
Boxenzahl, `category_id` und `bbox` (Toleranz 1,0 px) des frischen CVAT-Exports gegen dasselbe
Bild im Vorlabel-COCO (`data/labels/al-iteration-1/sideline-prelabel/instances.json`)
verglichen. "Berührt" = Boxenzahl weicht ab oder mindestens eine Box hat keine Entsprechung im
Vorlabel; sonst "unberührt".

| Kennzahl | 2026-09-02 (Korrektur) | 2026-09-04 (dieser Nachtrag) |
|---|---:|---:|
| Berührte (verifizierte) GoPro-Frames | 8 | **22** |
| Neu berührt seit 2026-09-02 | — | **14** |
| Fernfeld-Skip (0 Boxen, bereits vor 2026-09-02 getrimmt) | 11 | 11 (unverändert) |
| Unberührt/unverifiziert (ausgeschlossen) | 192 | 178 |
| `player`-Boxen (berührte Frames) | 82 | 218 |
| `referee`-Boxen (berührte Frames) | 19 | 57 |

Die 14 neu berührten Frames sind:

```
Wide - Clip 017_f00051.jpg   Wide - Clip 017_f00076.jpg   Wide - Clip 017_f00178.jpg
Wide - Clip 018_f00277.jpg   Wide - Clip 020_f00525.jpg   Wide - Clip 021_f00023.jpg
Wide - Clip 021_f00046.jpg   Wide - Clip 021_f00068.jpg   Wide - Clip 021_f00091.jpg
Wide - Clip 031_f00028.jpg   Wide - Clip 033_f00188.jpg   Wide - Clip 033_f00375.jpg
Wide - Clip 033_f00563.jpg   Wide - Clip 035_f00000.jpg
```

Keiner der 14 neuen Frames hat 0 Boxen — die Nutzerin hat konsequent nur Frames mit
Spielerinnen im nahen/mittleren Feldbereich bearbeitet, exakt wie im Nachtrag vom 2026-09-02
vereinbart (Fernfeld bleibt unberührt). Kein bereits berührter Frame verlor seinen
"berührt"-Status (0 Regressionen). Die verbleibenden 178 unberührten Frames (11 bestätigtes
Fernfeld-Skip + 167 noch nicht gesichtete Frames mit weiterhin ungeprüften Vorlabels) bleiben
per Merge-Regel ausgeschlossen — kein Frame gilt als Teil des Datensatzes, ohne dass ein
Mensch ihn tatsächlich gesehen hat (D-17).

**Nebenbefund (nicht Teil der gemeldeten Sitzung, aber real und vor dem Merge geprüft):**
Ein Kontroll-Pull von Task 5 (`al-1-broadcast-1`) zeigte ebenfalls eine Änderung seit der
letzten Korrektur — Shape-Zahl 1408 → 1419, zuletzt aktualisiert 2026-09-04 08:36 UTC (vs.
2026-09-01 für die Drohnen-Aufgaben, die byte-identisch geblieben sind, per direktem
Vorlabel-Vergleich bestätigt: `al-1-drone-1`/`al-1-drone-2` unverändert). Ein Datei-Diff
(gleiche Methodik) zeigt 26 von 100 Broadcast-Frames mit abweichenden Boxen gegenüber dem
09-02-Export, davon 3 neu berührt (vorher exakt Vorlabel-identisch: `Wide - Clip 031_f00050`,
`Wide - Clip 032_f00354`, `Wide - Clip 040_f00325`) — 97 von 100 Frames jetzt berührt (vorher
94). Da die TV/Broadcast-Domäne bereits mit "alle 100 bleiben — von der Nutzerin durchgesehen"
vollständig in den Datensatz übernommen ist (siehe `### Korrektur 2026-09-02`), ändert dieser
Fund keine Ein-/Ausschluss-Entscheidung; die Box-Koordinaten wurden dennoch aus dem frischeren
Export übernommen, um den Datensatz auf dem aktuellsten von der Nutzerin bestätigten Stand zu
halten statt auf einem zwei Tage alten Zwischenstand. Aufgenommen unter
`.planning/phases/02.2-dataset-buildout/deferred-items.md` als Beobachtung für eine künftige
Sitzung, da die Nutzerin dies nicht ausdrücklich berichtet hatte.

**Merge (Datensatz v1.1 → v1.2):**

| Kennzahl | v1.1 | v1.2 |
|---|---:|---:|
| Bilder gesamt | 558 | **572** |
| Drohne | 450 (unverändert) | 450 (unverändert, byte-identisch gegen Vorlabel geprüft) |
| TV/Broadcast | 100 | 100 (Bild-Auswahl unverändert, Annotationen aktualisiert) |
| GoPro/Hinterfeld | 8 | **22** |
| `player`-Boxen | 9305 | **9444** |
| `referee`-Boxen | 1063 | **1109** |
| DVC-MD5 (`.dvc`-Datei) | `1659e351c063750eea94b536eb9f10e1.dir` (560 Dateien) | `b39db72109a25376fe50628405ab6e48.dir` (574 Dateien) |
| `content_sha256` | `82f0feb7c4d678a44bdc7e90be416561bb2e27fabb5a657eb0dc005dbc54fa92` | `d4528a9958305c267e6257be26c07466fe78e286d4777108c29d9476003b56b1` |

**Ausschluss-Assertionen erneut geprüft:** Puerto Rico 0 Treffer (kein Session-Bezug zur
GoPro/Broadcast/Drohne-Auswahl dieser Iteration); `frozen_eval_clips.csv`
(`role = frozen_eval`) 0 Schnittmenge gegen alle 572 Frames (die 14 neuen GoPro-Frames stammen
aus Clips 17/18/20/21/31/33/35, keiner davon in der 30-zeiligen `frozen_eval`-Liste).

**Validierung** (`ffep cv dataset --coco data/labels/dataset --manifest
data/labels/dataset/manifest.json --min-images 1 --max-images 3000`): exit 0, 572 Bilder,
9444 `player` + 1109 `referee` Boxen, 0 Bilder ohne Annotation. Domänen-Aufschlüsselung:

| Domäne | Bilder | `player` | `referee` | Bilder ohne Annotation |
|---|---:|---:|---:|---:|
| Drohne | 450 | 7953 | 906 | 0 |
| GoPro/Hinterfeld | 22 | 218 | 57 | 0 |
| TV/Broadcast | 100 | 1273 | 146 | 0 |

`uv run pytest tests/test_cv_dataset.py tests/test_dvc_layout.py -x -q` — 23 passed (grün
gegen v1.2).

**Ehrlicher Stand gegen den 1.500-Floor:** 572 von 1.500 (38 %) — weiterhin deutlich unter dem
Floor, wie erwartet für einen Stand zwischen den beiden geplanten AL-Iterationen. GoPro liegt
jetzt bei 22/400 (5,5 %) statt vorher 8/400 (2 %) — die Nachtrags-Zielgrösse "~50–80 saubere
Frames" ist noch nicht erreicht, Iteration 2 (Plan 02.2-16/17) bleibt der Ort, an dem der
GoPro-Rückstand strukturiert aufgeholt wird, nicht diese Ad-hoc-Nachsitzung.

**DVC:** `dvc add data/labels/dataset` erneut ausgeführt (neuer Pointer,
`git status --porcelain data/labels` zeigt weiterhin nur `data/labels/dataset.dvc`,
`git check-ignore -q data/labels/dataset` bestätigt weiterhin den Ausschluss der Nutzdaten).
`dvc push -r local-fallback` erneut gegen den bestehenden lokalen Rückfall-Remote ausgeführt;
`dvc status -r local-fallback -c` bestätigt "Cache and remote 'local-fallback' are in sync"
danach. Der reale OTC-OBS-Push bleibt weiterhin Plan 02.2-20 vorbehalten.

## Iteration-1-Detektor: Training und Per-Domain-Evaluation (Plan 02.2-15)

### Training

`evaluate_per_domain` implementiert (`src/flag_football_ep/cv/detect.py`): läuft den geladenen
Detektor über jede Domäne der eingefrorenen Eval-Clips, scored die Vorhersagen mit
`torchmetrics.detection.MeanAveragePrecision` gegen menschlich korrigierte Ground-Truth-Frames
aus `data/labels/<session_id>/corrected/instances.json` (gefiltert auf die dieser Domäne
zugeordneten `frozen_eval`-Clip-Nummern). Eine Domäne ohne verfügbare Ground-Truth-Frames für
ihre eingefrorenen Clips löst `EvalGroundTruthMissing` mit Domänennamen aus, statt eine
Kennzahl über eine leere Menge zu berichten.

**Gefundener und behobener Blocker (Rule 3):** `train_detector` löste sein Manifest bislang
immer aus dem festen Piloten-Pfad (`data/labels/frames/manifest.json`, 404 reine
Drohnen-Frames) auf, unabhängig vom übergebenen `--dataset`-Pfad — ein Training gegen
`data/labels/dataset` (das wachsende Multi-Domänen-Datenset) hätte still die falschen Frames
verwendet. `_resolve_manifest_path` bevorzugt jetzt `<dataset_dir>/manifest.json`, wenn
vorhanden (die 2.2-Konvention, siehe `### Merge & Validierung` oben), und fällt nur für den
Piloten-Datenpfad auf den alten Ort zurück.

**Zweiter gefundener und behobener Blocker (Rule 1):** Der echte Trainingslauf (unten) endete
nach allen 30 Epochen mit `WeightsNotFound: expected checkpoint not found ... checkpoint_best_
total.pth`. Ursache: RF-DETRs eigener `BestModelCallback.on_fit_end` schreibt diese Datei nur,
wenn mindestens eine Validierungs-Epoche die überwachte Metrik tatsächlich verbessert hat — die
AL-Iterations-Datensätze dieser Phase tragen aber überhaupt keinen `val`-Split (jeder gemergte
Frame trägt `split: "train"`; die Evaluierung läuft bewusst getrennt über die eingefrorenen
Eval-Clips, nicht über `train_detector`s internen Val-Split), sodass nie eine verbessernde
Validierungs-Epoche stattfindet. RF-DETR selbst schreibt in diesem Fall stattdessen zuverlässig
`checkpoint_best_ema.pth` ("EMA metric never improved; saved final EMA weights..." im Log).
`train_detector`/`_register_from_artifacts` akzeptieren jetzt beide Dateinamen und tragen die
tatsächlich verwendete Quelle in `params["checkpoint_source"]` ein
(`best_ema_fallback_no_val_split` für diesen Lauf) — ein Blocker, der jeden künftigen
AL-Iterations-Trainingslauf gegen dieses Datenset ebenso getroffen hätte, nicht nur diesen.

Trainiert auf der Primärmaschine (Apple M5 Max, `--device mps`, D-21-Fallback — kein CUDA auf
dieser Maschine verfügbar), nach demselben chunk-freien Verfahren wie die 2.1-Baseline
dokumentiert (`## Detector-Training` in `docs/cv-setup.md`), diesmal in einem durchgehenden
Hintergrundlauf statt manuell segmentiert:

| Setting | Wert |
|---|---|
| `resolution` | 896 |
| `epochs` | 30 |
| `batch_size` | 4 |
| `grad_accum_steps` | 4 |
| `device` | `mps` |
| `dataset_content_sha256` | `d4528a9958305c267e6257be26c07466fe78e286d4777108c29d9476003b56b1` (== Datensatz v1.2) |
| `checkpoint_source` | `best_ema_fallback_no_val_split` |
| `machine` | `MacBook-Pro-2.local` (Apple M5 Max, 128 GB) |
| **MLflow Run-ID** | `be854a1adebf4eb4b01d98dc39022ee1` |
| Wall-Clock | ~2 h 19 min (11:26:35–13:45:12 Uhr, 2026-09-04, ein durchgehender Lauf) |
| Registrierte Modellversion | `cv_detector_model` Version 2 |

### Per-Domain-Evaluation

Beide Detektoren — der Phase-2.1-Champion (`87a8a5222f7a472787875e974d089c44`) und der neue
Iteration-1-Lauf (`be854a1adebf4eb4b01d98dc39022ee1`) — wurden mit `ffep cv eval-domains` gegen
`data/reference/frozen_eval_clips.csv` gemessen.

**Drohne:** Ground Truth kommt aus dem Piloten-Korrektur-Datensatz
(`data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/corrected/`, 304 Frames, vor der
2.2-Eval-Split-Einfrierung gezogen). 13 der 18 eingefrorenen Drohnen-Clips (5, 6, 7, 11, 15, 16,
21, 22, 28, 33, 36, 40, 43) überschneiden sich mit den vom Piloten gelabelten Clip-Nummern
(1–46); die übrigen 5 (49, 52, 54, 55, 56) liegen ausserhalb des Piloten-Materials und tragen
keine Ground Truth. 76 Bilder / 1635 Boxen stehen damit für die Drohnen-Domäne zur Verfügung —
ein Teilsatz der 18 Clips, aber real von der Nutzerin gesichtete Boxen, keine erfundene Zahl.

**GoPro/Hinterfeld:** Für keinen der 12 eingefrorenen `sideline`-Clips existiert ein
menschlich-korrigiertes COCO-Paket (`data/labels/2026-08-14_WC-GER-vs-MEX-GOPRO/` enthält nur
`bundle-inputs/`, kein `corrected/`) — jede AL-1-Korrektur betraf ausschliesslich `role = pool`-
Clips (`Pool-Sicherheit` oben), wie von Konstruktion beabsichtigt. Ein realer
`ffep cv eval-domains`-Lauf gegen den vollen Split bestätigt das:
`EvalGroundTruthMissing: domain 'sideline' has zero ground-truth-labeled frames overlapping its
12 frozen_eval clip(s)`. **Die GoPro-Domäne ist diese Iteration nicht messbar** — kein
Ausweichen auf eine erfundene Zahl, sondern eine ehrlich dokumentierte Lücke: eine eigene
Eval-Ground-Truth-Sitzung für die eingefrorenen GoPro-Clips ist bislang von keinem Plan
vorgesehen und hat nicht stattgefunden.

**Vergleichstabelle (identischer 76-Bilder-Drohnen-Eval-Satz für beide Läufe):**

| Detektor | Domäne | n Bilder | n Boxen | `mAP_50` | `mAP_50_95` | `AP_player` | `AP_referee` |
|---|---|---:|---:|---:|---:|---:|---:|
| Phase-2.1-Champion (`87a8a522...`) | Drohne | 76 | 1635 | 0,8669 | 0,6259 | 0,6163 | 0,6355 |
| Iteration-1 v1.2 (`be854a1a...`) | Drohne | 76 | 1635 | 0,8315 | 0,5783 | 0,5475 | 0,6091 |
| **Delta** | Drohne | — | — | **-0,0354** | **-0,0476** | -0,0688 | -0,0264 |
| Phase-2.1-Champion | GoPro/Hinterfeld | — | — | nicht messbar (keine Ground Truth) | | | |
| Iteration-1 v1.2 | GoPro/Hinterfeld | — | — | nicht messbar (keine Ground Truth) | | | |

**Wichtiger Methodenhinweis:** Diese 76-Bild-Zahl (`mAP_50_95 = 0,6259` für den Champion) ist
*nicht* dieselbe Messung wie die in `docs/dataset-plan.md` `## 3` als Referenz-Ausgangspunkt
zitierte `mAP_50_95 = 0,8112` — jene Zahl stammt aus dem internen 20-%-Val-Split des
304-Bild-Piloten-Datensatzes (Phase 2.1, vor Existenz der eingefrorenen Eval-Clips). Die hier
gemessene Zahl verwendet dieselben eingefrorenen Clips für beide Detektoren, exakt die
Vergleichsbasis, die das Abbruchkriterium verlangt ("gegenüber dem zuletzt registrierten
Detektor ... auf den eingefrorenen Pro-Domäne-Eval-Clips") — beide Zahlen sind real gemessen,
beschreiben aber unterschiedliche Testsätze und dürfen nicht gegeneinander verglichen werden.

### Abbruchkriterium-Verdikt

**Drohne:** Delta `mAP_50_95` = -0,0476 (Verschlechterung, nicht Verbesserung), `mAP_50`
bewegt sich in dieselbe (negative) Richtung. Klar unterhalb der +0,010-Verbesserungsschwelle —
**Iteration 2 für Drohne (Metrik-Verdikt): nein.** Plausible Ursache: das Multi-Domänen-Training
(572 Bilder über drei Domänen, davon nur 450 Drohne) ohne jeden Val-Split (siehe
`checkpoint_source`-Fund oben) hat keine früh-stoppende, validierungsbasierte
Checkpoint-Auswahl — es zählen die letzten EMA-Gewichte nach exakt 30 Epochen, nicht die besten
gegen eine Kontrollmenge. Eine Verschlechterung ist damit ein plausibles, ehrliches Ergebnis
dieser Iteration, kein Messfehler.

**GoPro/Hinterfeld:** Kein registrierter Vorgänger-Lauf für diese Domäne UND keine
Ground-Truth-Frames für ihre eingefrorenen Clips diese Iteration — das Abbruchkriterium ist für
diese Domäne (noch) nicht anwendbar (`docs/dataset-plan.md` `## 3`: "für GoPro und TV ...
labelt daher immer bis mindestens zum Floor, unabhängig vom Abbruchkriterium").

**1.500-Floor-Status (bindend, unabhängig vom Metrik-Verdikt):** 572 von 1.500 (38 %) —
deutlich unter dem Floor. **Iteration 2 läuft für alle Domänen zum Erreichen des Floors, auch
für die Drohne trotz ihres negativen Metrik-Verdikts** — exakt der von Task 3 selbst
vorweggenommene Fall ("Note the 1,500-frame floor separately: it is mandatory regardless of the
metric verdict").

### Promotion-Entscheidung

**Nicht promoviert.** Task 3s eigene Regel ("Promote ... if it improves the drone domain
without regressing the second domain") ist hier zweifach nicht erfüllt: die Drohnen-Domäne hat
sich verschlechtert (nicht verbessert), und die zweite Domäne (GoPro) ist diese Iteration gar
nicht messbar, kann eine fehlende Regression also nicht bestätigen. `champion` bleibt auf dem
Phase-2.1-Lauf (`87a8a5222f7a472787875e974d089c44`) — verifiziert nach der Evaluation
unverändert: `resolve_champion('cv_detector_model', config) == '87a8a5222f7a472787875e974d089c44'`.
`hackathon-frozen` ebenfalls unverändert und identisch geprüft
(`resolve_frozen('cv_detector_model', config) == '87a8a5222f7a472787875e974d089c44'`) — beide
Aliase wurden von diesem Plan nur gelesen, nie geschrieben.

Der neue Iteration-1-Lauf (`be854a1adebf4eb4b01d98dc39022ee1`) bleibt als registrierte
`cv_detector_model`-Version 2 im MLflow-Store erhalten (Provenienz-Nachweis, kein Alias) — für
einen künftigen Vergleich (z. B. nach Iteration 2, wenn ein Val-Split-fähiger Trainingslauf
existiert) verfügbar, ohne den aktuellen Produktionsstand zu beeinflussen.

## Iteration 2

Noch nicht gezogen — folgt in Plan 02.2-17, nach Abschluss der Iteration-1-Korrektursitzung
(Plan 02.2-13) und ihrer Auswirkung auf das Abbruchkriterium (`docs/dataset-plan.md` `## 3`).
