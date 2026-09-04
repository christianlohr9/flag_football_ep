# Ground Truth für die eingefrorenen Eval-Clips (Nutzer-Gate, 2026-09-04)

Befund aus Plan 02.2-15: Die einzige vorhandene Drohnen-Ground-Truth (76 Bilder) stammt aus dem
Pilot-Datensatz, auf dem der Champion trainiert wurde (88 train + 25 val der 113 Pilot-Frames in
den 18 Eval-Clips). Der Vergleich Champion 0,6259 vs. Iteration-1 0,5783 mAP_50_95 ist daher
kein Held-out-Vergleich; die Stoppregel ist für beide Domänen **nicht messbar**.
GoPro/Hinterfeld hat für seine 12 Eval-Clips gar keine Ground Truth.

Was fehlt (Labeling durch die Nutzerin):
- 18 Drohnen-Eval-Clips (5, 6, 7, 11, 15, 16, 21, 22, 28, 33, 36, 40, 43, 49, 52, 54, 55, 56) und
  12 GoPro-Eval-Clips: je 5–6 Frames (≈ 90 + 70), Vorlabels vom aktuellen Champion, 100 % geprüft.
- Diese Frames werden **nie** Trainingsdaten (D-19); eigener DVC-Pfad (`data/labels/eval/`),
  eigene CVAT-Aufgabe.

Danach: `ffep cv eval-domains --run <run> --split data/reference/frozen_eval_clips.csv` für
Champion und `be854a1a…` auf derselben GT; erst dann die Stoppregel (+0,010 mAP_50_95) anwenden
und über die Beförderung entscheiden.

Tooling-Vorbereitung (autonom möglich, ohne Labels): Frame-Sampling aus den Eval-Clips +
Prelabel-Push nach CVAT als eigene Aufgabe (analog Plan 02.2-16), damit die Nutzerin nur noch
prüft.
