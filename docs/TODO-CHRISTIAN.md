# Deine To-do-Liste

Stand: 2026-09-09. Diese Liste pflege ich (Claude) bei jedem Schritt. Kurze Sätze, ein Punkt pro Aufgabe.
Erledigtes wandert nach unten. Wenn du etwas fertig hast: einfach sagen, ich hake ab.

## Jetzt

1. **Ballpositionen für 4 WM-Spiele** (wie beim Viertelfinale).
   PAN–BRA (69 Plays), JPN–FRA (4), GER–SLO (1), USA–ESP (1).
   Arbeitsblätter: `data/raw/ifaf/spot_fill_worksheets/`. Werte in `data/reference/ifaf_spot_fill/<Spiel>.csv`.
   Dann sagen „Spots fertig“.

2. **GTA-Spot-Check.** Ein paar Clips ansehen. Anleitung: `docs/gta-spotcheck.md`. Dauer etwa eine Stunde.
   Dann sagen „GTA ok“ oder was nicht passt.

3. **Repo auf privat stellen.** Lizenz ist schon auf Apache-2.0 umgestellt, jetzt fehlt nur noch die Sichtbarkeit: `gh repo edit christianlohr9/flag_football_ep --visibility private --accept-visibility-change-consequences` ausführen (oder auf GitHub unter Settings → Danger Zone → Change visibility).

4. **CVAT-Aufgabe 11 `eval-bias-test` labeln** (30 Bilder, keine Vorlabels, alles neu zeichnen, etwa eine Stunde) — Anleitung in `docs/dataset-buildout.md` unter „Vorlabel-Bias-Test (Nutzerentscheid 2026-09-11)".

## Warten auf andere

5. **OTC-Zugangsdaten.** Kommen später. Dann: `docs/hackathon-otc-upload.md`, ein Befehl.
6. **IFAF-Statistiker.** Jona kümmert sich. Fragen stehen in `docs/ifaf-wm2026-daten.md`.

## Erledigt

- CVAT-Aufgaben 8, 9, 10 geprüft (2026-09-10). Daten werden jetzt zusammengeführt und das Modell neu trainiert.

- Spielerinnen zugeordnet: deine 14 Einträge übernommen, Unbekannte bleiben leer (2026-09-10).

- Lizenz-Entscheidung „privat + Apache“ umgesetzt: Lizenz von GPL-3.0 auf Apache-2.0 mit Namensnennung umgestellt (2026-09-09). Sichtbarkeit steht noch aus, siehe „Jetzt“ oben.
- Jona hat den Hackathon schriftlich bestätigt (2026-09-09). Freigabe erledigt.
- GPS-Export über Titan/Hudl: geht nicht. Thema geschlossen (2026-09-09).
- Viertelfinale MEX–ESP: Ballpositionen eingetragen (2026-09-08).
- Reviews: Explosiveness, EPA-Dokument, Oktober-Handout (2026-09-04).
- Beförderung der Modelle: `both` (2026-09-09), zweite Runde `none` (2026-09-09).
- Plattform-Entscheidung unterschrieben: Option ii, gestaffelt (2026-09-09).
- Eval-Bilder in CVAT geprüft (Tasks 6 und 7) (2026-09-04).
- Testset-Labels der 61 Puerto-Rico-Clips (2026-09-07).
