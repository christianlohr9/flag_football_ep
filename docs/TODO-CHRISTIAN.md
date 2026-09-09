# Deine To-do-Liste

Stand: 2026-09-09. Diese Liste pflege ich (Claude) bei jedem Schritt. Kurze Sätze, ein Punkt pro Aufgabe.
Erledigtes wandert nach unten. Wenn du etwas fertig hast: einfach sagen, ich hake ab.

## Jetzt

1. **Spielerinnen zuordnen (HC-Daten).**
   Roster auf die aktuellen 2026er-Kader aktualisiert (u.a. Deutschland) und alle eindeutigen
   Rückennummer-/Nachname-Vorschläge automatisch übernommen (2026-09-09). Rest ist absichtlich
   offen: fast alles sind einzelne Vornamen ohne Nachnamen oder Nachnamen von Camp-Gegnerinnen
   anderer Nationen -- die kann ich ohne Verwechslungsrisiko nicht automatisch raten.
   Datei öffnen: `data/raw/hc_files/player_mapping_hc_template.csv` (frisch, nur noch die
   offenen Fälle).
   Spalte `source_player` = so hat der Head Coach die Spielerin in seiner Excel geschrieben (Nummer, Vorname oder Nachname).
   Spalte `canonical_player` = der volle Name aus dem Roster (`data/reference/roster.csv`, Spalte `player_name`).
   Wo schon ein Vorschlag steht: prüfen. Wo leer: Namen eintragen. Wenn keine deutsche Spielerin: leer lassen.
   Dann sagen „Zuordnung fertig“, ich übernehme sie ins Repo.

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
