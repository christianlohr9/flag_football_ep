# Einreichung BWI Data Analytics Hackathon 2026 — Formulartext (Stand 2026-08-31)

Copy-and-paste-fertig für https://hackathon.bwi.de/challenges/. Hintergrund, Benchmark-Design,
Labelling-Plan und Stand der Technik: `docs/hackathon-challenge-reid.md`. Vor dem Absenden:
Verbands-Freigabe für das Material einholen (Feld „Verfügbare Daten").

---

## Titel

Wer ist wer nach der Verdeckung? Re-Identification für automatisches Spielerinnen-Tracking im Flag Football

## Beschreibung

Die deutsche Flag-Football-Frauennationalmannschaft bereitet sich auf die Olympischen Spiele 2028 vor. Ein ehrenamtliches Analytics-Projekt hat eine Pipeline gebaut, die aus Drohnenaufnahmen automatisch die Positionen aller Spielerinnen auf dem Feld berechnet: nachtrainierter Detektor, Tracker, Team-Zuordnung, Homographie in Feldkoordinaten, Radar-Ansicht. Positionsgenauigkeit (~15 cm) und Laufzeit (ein Spiel in unter einer Stunde) erfüllen die Zielkriterien.

Ein Kriterium scheitert: die Identitäts-Kontinuität. Sobald sich zwei Spielerinnen im Bild überschneiden – Kreuzungsrouten, Coverage, Flag-Pull, im Flag Football ständig – verliert der Tracker die Zuordnung und vergibt neue Nummern oder vertauscht sie. Von 61 Spielzügen eines Pilotspiels bestehen 15 von 61 (24,59 %) das Kriterium „mindestens 90 % des Spielzugs ohne Identitätswechsel" – gemessen über alle 61 Clips, nicht hochgerechnet; Ziel sind 90 %. Ohne stabile Identitäten sind alle darauf aufbauenden Coaching-Kennzahlen (Routentiefen, Separation, Spacing) wertlos.

Das Problem ist offen: Alle Spielerinnen eines Teams tragen dasselbe Trikot, ein Körper ist im Drohnenbild nur etwa 30 Pixel hoch, Rückennummern sind nicht lesbar, und es gibt keine Identitäts-Labels – nur die Tracklets des Trackers und die Spielregeln (5 gegen 5, Team bekannt, niemand erscheint mitten im Feld). Eine Recherche über SoccerNet, die NFL-Wettbewerbe und kommerzielle Anbieter zeigt: Bestehende Systeme lösen Identität über Rückennummern, Sensordaten oder Menschen im Loop – nichts davon ist hier verfügbar.

Gesucht ist ein Verfahren, das nach einer Verdeckung dieselbe Spielerin wieder derselben Nummer zuordnet: gelerntes Erscheinungsbild, globale Tracklet-Verknüpfung, Bewegungsmodelle oder eine Kombination. Pipeline, Baseline, ein von Hand bewerteter Benchmark und die Zielmetrik liegen bereit – das Team beginnt bei „Problem verstanden, Fortschritt messbar".

## Ziel

Kernziel: ein Tracking-Verfahren (Modell, Nachverarbeitung oder beides), das auf dem Drohnen-Benchmark den Anteil der Spielzüge ohne Identitätswechsel gegenüber der Baseline (BoT-SORT, 15/61 = 24,59 % gemessen) messbar erhöht – Zielmarke 90 %. Ergebnis: lauffähiger Prototyp, der die bereitgestellten Detektionen einliest und Tracks im vorgegebenen Schema ausgibt, plus Kurzbericht mit der Messung auf Dev- und Test-Set.

Transfer-Wertung: dieselbe Methode auf Seitenlinien- (GoPro) und TV-Ausschnitte angewendet – wie viel der Verbesserung überlebt den Kamerawechsel? Offizielle Spiele sind nur über TV/Seitenlinie zugänglich (Drohnen sind dort verboten); diese Frage entscheidet über den Praxisnutzen.

Bonus (optional, separat bewertet): Erkennung des Flag-Pulls als Ereignis aus den Trajektorien – Zeitpunkt (±0,5 s), Ort (~2 Yards) und beteiligte Spielerinnen. Der Bonus belohnt gute Kontinuität und liefert direkt Coaching-Wert (Wo enden Plays? Yards nach dem Catch?).

Ausdrücklich nicht Ziel: Ball-Erkennung – aus Drohnenhöhe wenige Pixel und fast immer verdeckt, ein eigenes Forschungsproblem.

## Verfügbare Daten

Dev-Set (für die Teams): Pilotspiel vom Mai 2026, 61 Drohnen-Clips (je ein Spielzug, 8–11 s, 1920×1080, 30 fps, ca. 10,5 min, ca. 250 MB) mit: Detektionen pro Frame (Boxen, Klasse, Konfidenz, Parquet – kein Detektor-Training nötig), Baseline-Tracks mit Team-Zuordnung und Feldkoordinaten (ca. 354.000 Zeilen), ca. 17.000 Spielerinnen-Ausschnitte als Trainingsmaterial für Erscheinungsmodelle, von Hand bewertete Kontinuitäts-Urteile pro Clip mit Fehlerbeschreibung, 250 hand-markierte Fußpositionen, Homographie-Kalibrierung, Flag-Pull-Ereignisse, Overlay-Videos und Radar-Renderings zur Sichtprüfung.

Test-Set (privat, Labels zurückgehalten): ein zweites Drohnenspiel derselben Mannschaft, identisch aufbereitet; Endwertung findet dort statt.

Transfer-Material: 60 Seitenlinien-Clips (GoPro, WM-Spiel) und 51 TV-Clips (WM-Spiel) mit Detektionen.

Formate: MP4, Parquet, CSV, JPEG; Python-Paket mit Kommandozeilenwerkzeugen und Tests; läuft auf einem Laptop, GPU beschleunigt Training und Inferenz. Öffentliche Analog-Datensätze als Zusatz: TeamTrack (Drohnen-Top-View, CC BY 4.0), SoccerTrack v2 (CC BY 4.0).

Datenschutz: Das Material zeigt identifizierbare Personen (Nationalspielerinnen). Bereitstellung nur mit Freigabe des Verbands für eine interne, zweckgebundene Nutzung im Hackathon, ohne Weitergabe, mit Nutzungsvereinbarung der Teilnehmenden und Löschung nach dem Event. [Freigabe liegt vor / wird bis <Datum> eingeholt – vor dem Absenden anpassen.]

## Technische oder organisatorische Einschränkungen

Python-Pipeline mit definierten Schnittstellen (Detektionen rein, Tracks im festen Schema raus). Der Detektor ist vorgegeben – kein YOLO nötig. Für Tracking-/ReID-Komponenten bitte permissive Lizenzen (Apache/MIT/BSD) bevorzugen; verfügbare Bausteine: trackers (Apache), supervision (MIT), torchreid (MIT), gta-link (MIT), MOTIP (Apache). AGPL-Komponenten (Ultralytics, boxmot) sind kein Ausschlusskriterium für die Bewertung, aber ein Ergebnis damit könnte der Verband nicht übernehmen. Keine Cloud-Uploads des Materials; Arbeit auf bereitgestellter Infrastruktur. Bewertung ausschließlich mit den bereitgestellten Skripten, damit alle Teams dieselbe Zahl messen.

## Zielgruppe der Lösung

Trainerinnen, Trainer und Videoanalyse der Nationalmannschaft – Nutzer ohne ML-Kenntnisse, die Radar-Clips und Kennzahlen im Videostudium einsetzen. Technisch übernimmt das ehrenamtliche Analytics-Projekt das Ergebnis in seine Pipeline; das Team liefert an Entwickler, nicht an Endnutzer.

## Weitere Hinweise an das Team

Sprache: Deutsch oder Englisch (Code englisch dokumentiert, Messprotokolle deutsch). Einstieg am ersten Tag: Baseline laufen lassen (Minuten), Overlay eines gescheiterten Clips anschauen, Fehlerbeschreibung lesen – das Problem ist in einer Viertelstunde verstanden. Schwierigkeit: forschungsnah; erfolgversprechende Richtungen sind bekannt (Erscheinungslernen aus den eigenen Tracklets ohne Labels, Tracklet-Verknüpfung als Optimierungsproblem mit 5-gegen-5-Priors, gelernte Assoziation wie bei DanceTrack/MOTIP), aber keine ist für diesen Fall erprobt. Teams mit ML-, Optimierungs- und Evaluationsprofil ergänzen sich. Ein Ansprechpartner aus dem Projekt ist während der Woche erreichbar.

Möglicher Nutzen über den Sport hinaus (kein Muss): Mehrere sich ähnlich sehende Personen in Luftaufnahmen über Verdeckungen hinweg eindeutig zu verfolgen, ist nicht sportspezifisch – Leistungsdiagnostik in den Sportfördergruppen der Bundeswehr (Flag Football ist ab 2028 olympisch), Auswertung von Gruppenbewegungen in Drohnenaufnahmen von Übungsplätzen (uniforme Kleidung, Verdeckung), Such- und Rettungsszenarien, und methodisch das Lernen ohne Labels aus reichlich vorhandenem, aber unannotiertem Material.

## Kontakt

Name: [Christian Lohr]
E-Mail: [ausfüllen]
Telefon (optional): [ausfüllen]
Organisation (optional): [ausfüllen]
