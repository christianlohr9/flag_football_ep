# `ffep cv cvat-pull` ohne `--force` (Eingang 2026-09-04)

Befund: Ein zweiter `cvat-pull --task 6 --out data/labels/eval/drone/corrected` bricht mit
`DatasetError: CVAT export failed … (FileExistsError)` ab, weil `export_cvat_task` ein vorhandenes
Zielverzeichnis nicht überschreibt. Die Fehlermeldung verschleiert die Ursache (klingt nach
CVAT-/HTTP-Problem).

Vorschlag (klein, für Plan 02.2-16/17 oder als Nebenfix):
- `--force` (oder `--overwrite`) am CLI, das das Zielverzeichnis vorher leert;
- ohne Flag eine klare Meldung „Zielverzeichnis existiert bereits: <pfad> — `--force` oder anderes
  `--out`“ statt der generischen Export-Fehlermeldung.
