# Deine To-do-Liste

Stand: 2026-09-09. Diese Liste pflege ich (Claude) bei jedem Schritt. Kurze Sätze, ein Punkt pro Aufgabe.
Erledigtes wandert nach unten. Wenn du etwas fertig hast: einfach sagen, ich hake ab.

## Jetzt

1. **Spielerinnen zuordnen (alle Datenquellen).**
   Jetzt deckt die Zuordnung auch die alten Spiele und die WM-Daten ab, nicht nur die HC-Tabelle.
   Alles Eindeutige ist schon übernommen.
   Es bleiben 46 Einträge offen.
   Datei: `data/raw/hc_files/player_mapping_template.csv` (nur noch die offenen Fälle).
   Neue Spalte `source` zeigt, woher der Eintrag kommt. Nicht ändern.
   Spalte `source_player` = sein Eintrag. Nicht ändern.
   Spalte `canonical_player` = voller Name aus `data/reference/roster.csv`.
   Kennst du die Spielerin nicht? Leer lassen. Das ist ok.
   Fertig? Sag „Zuordnung fertig“.

2. **CVAT-Aufgaben 8, 9, 10 prüfen** (Bilder für die nächste Trainingsrunde).
   Task 8 = Drohne (183 Bilder), Task 9 = GoPro (150 Bilder, nur nahe Bilder, ferne überspringen), Task 10 = TV (101 Bilder).
   Dann sagen „CVAT fertig“.

3. **Ballpositionen für 4 WM-Spiele** (wie beim Viertelfinale).
   PAN–BRA (69 Plays), JPN–FRA (4), GER–SLO (1), USA–ESP (1).
   Arbeitsblätter: `data/raw/ifaf/spot_fill_worksheets/`. Werte in `data/reference/ifaf_spot_fill/<Spiel>.csv`.
   Dann sagen „Spots fertig“.

4. **GTA-Spot-Check.** Ein paar Clips ansehen. Anleitung: `docs/gta-spotcheck.md`. Dauer etwa eine Stunde.
   Dann sagen „GTA ok“ oder was nicht passt.

5. **Repo auf privat stellen.** Lizenz ist schon auf Apache-2.0 umgestellt, jetzt fehlt nur noch die Sichtbarkeit: `gh repo edit christianlohr9/flag_football_ep --visibility private --accept-visibility-change-consequences` ausführen (oder auf GitHub unter Settings → Danger Zone → Change visibility).

## Warten auf andere

6. **OTC-Zugangsdaten.** Kommen später. Dann: `docs/hackathon-otc-upload.md`, ein Befehl.
7. **IFAF-Statistiker.** Jona kümmert sich. Fragen stehen in `docs/ifaf-wm2026-daten.md`.

## Erledigt

- Lizenz-Entscheidung „privat + Apache“ umgesetzt: Lizenz von GPL-3.0 auf Apache-2.0 mit Namensnennung umgestellt (2026-09-09). Sichtbarkeit steht noch aus, siehe „Jetzt“ oben.
- Jona hat den Hackathon schriftlich bestätigt (2026-09-09). Freigabe erledigt.
- GPS-Export über Titan/Hudl: geht nicht. Thema geschlossen (2026-09-09).
- Viertelfinale MEX–ESP: Ballpositionen eingetragen (2026-09-08).
- Reviews: Explosiveness, EPA-Dokument, Oktober-Handout (2026-09-04).
- Beförderung der Modelle: `both` (2026-09-09), zweite Runde `none` (2026-09-09).
- Plattform-Entscheidung unterschrieben: Option ii, gestaffelt (2026-09-09).
- Eval-Bilder in CVAT geprüft (Tasks 6 und 7) (2026-09-04).
- Testset-Labels der 61 Puerto-Rico-Clips (2026-09-07).
