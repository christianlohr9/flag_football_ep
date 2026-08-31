# Hackathon-Challenge: Wer ist wer nach der Verdeckung? — Re-Identification im Flag-Football-Tracking

**Status: Entwurf vom 2026-08-31 für den BWI Data Analytics Hackathon (23.–27. November 2026,
Einreichung über https://hackathon.bwi.de/challenges/). Voraussetzung vor Einreichung: Freigabe
des Verbands/Teams für die Nutzung des Spielmaterials (siehe `## Datenschutz`). Owner: Nutzer.**

Dieses Dokument ist die Arbeitsgrundlage: Teil 1 folgt den sieben Formularfeldern der
BWI-Einreichung, Teil 2 hält das Benchmark-Design und den Labelling-Plan fest, Teil 3 die
offenen Punkte. Hintergrund und Zahlen stammen aus Phase 2.1 (`docs/pilot-gate-decision.md`,
`docs/pilot-accuracy.md`, `docs/cv-setup.md`, `docs/homography-calibration.md`).

---

## Teil 1 — Formularfelder

### Titel

Wer ist wer nach der Verdeckung? Re-Identification für automatisches Spielerinnen-Tracking im
Flag Football (Drohnen- und TV-Material)

### Beschreibung (150–300 Wörter)

Die deutsche Flag-Football-Frauennationalmannschaft bereitet sich auf die Olympischen Spiele 2028
vor. Ein ehrenamtliches Analytics-Projekt hat dafür eine Pipeline gebaut, die aus Drohnenaufnahmen
automatisch die Positionen aller Spielerinnen auf dem Feld berechnet: Detektor (RF-DETR,
selbst nachtrainiert), Tracker (BoT-SORT), Team-Zuordnung, Homographie auf Feldkoordinaten,
Radar-Ansicht. Positionsgenauigkeit (~15 cm) und Laufzeit (ein Spiel in unter einer Stunde)
erfüllen die Zielkriterien.

Ein Kriterium scheitert: die **Identitäts-Kontinuität**. Sobald sich zwei Spielerinnen im Bild
überschneiden — Kreuzungsrouten, Coverage, Flag-Pull, im Flag Football ständig — verliert der
Tracker die Zuordnung und vergibt neue Nummern oder vertauscht sie. Von 61 Spielzügen eines
Pilotspiels bestehen im besten Fall 77 % das Kriterium „≥ 90 % des Spielzugs ohne
Identitätswechsel"; Ziel sind 90 %. Ohne stabile Identitäten sind alle darauf aufbauenden
Coaching-Kennzahlen (Routentiefen, Separation, Spacing) wertlos.

Das Problem ist offen, weil klassische Re-Identification hier versagt: Alle Spielerinnen eines
Teams tragen dasselbe Trikot, ein Körper ist auf dem Drohnenbild nur ~30 Pixel hoch, und es gibt
keine Identitäts-Labels — nur die Tracklets des Trackers und die Spielregeln (5 gegen 5,
Team bekannt, niemand erscheint mitten im Feld). Gesucht ist ein Verfahren, das nach einer
Verdeckung dieselbe Spielerin wieder derselben Nummer zuordnet — per gelerntem
Erscheinungsbild, per globaler Tracklet-Verknüpfung, per Bewegungsmodell oder einer
Kombination. Die Pipeline, eine Baseline, ein von Hand bewerteter Benchmark und die Zielmetrik
liegen bereit; das Team beginnt bei „Problem verstanden, Fortschritt messbar", nicht beim
Datenputzen.

### Ziel

- **Kernziel (Pflicht):** Ein Tracking-Verfahren (Modell, Nachverarbeitung oder beides), das auf
  dem Drohnen-Benchmark den Anteil der Spielzüge ohne Identitätswechsel gegenüber der Baseline
  (BoT-SORT, 77 % Obergrenze) messbar erhöht — Zielmarke 90 %. Ergebnis: lauffähiger Prototyp,
  der die bereitgestellten Detektionen einliest und Tracks ausgibt, plus ein Kurzbericht mit der
  Messung auf Dev- und Test-Set.
- **Transfer-Wertung:** Dieselbe Methode, angewendet auf Seitenlinien-(GoPro-) und TV-Ausschnitte
  aus dem Inventar — wie viel der Verbesserung überlebt den Kamerawechsel? (Offizielle Spiele sind
  nur über TV/Seitenlinie zugänglich; Drohnen sind dort verboten. Diese Frage entscheidet über den
  Praxisnutzen.)
- **Bonus (optional, separat bewertet):** Erkennung des Flag-Pulls als Ereignis aus den
  Trajektorien — Zeitpunkt (±0,5 s), Ort (~2 Yards) und beteiligte Spielerinnen. Der Bonus
  belohnt gute Kontinuität, statt von ihr abzulenken, und liefert direkt Coaching-Wert
  (Wo enden Plays? Pursuit-Winkel, Yards nach dem Catch).
- **Ausdrücklich nicht Ziel:** Ball-Erkennung. Aus Drohnenhöhe wenige Pixel und fast immer verdeckt,
  im TV-Bild bewegungsunscharf — ein eigenes Forschungsproblem, das die Challenge nur verwässern
  würde.

### Verfügbare Daten

- **Dev-Set (öffentlich für die Teams):** Pilotspiel GER vs. Panama Rojo, 16.05.2026, 61 Drohnen-Clips
  (je ein Spielzug, 8–11 s, 1920×1080, 30 fps, ~10,5 min gesamt, ~250 MB), dazu:
  - Detektionen des nachtrainierten Detektors pro Frame (Boxen, Klasse Spielerin/Schiedsrichterin,
    Konfidenz) — als Parquet; Teams müssen keinen Detektor trainieren.
  - Baseline-Tracks (BoT-SORT) mit Team-Zuordnung und Feldkoordinaten (Yards), ~354.000 Zeilen.
  - ~17.000 Spielerinnen-Crops (Oberkörper) als Trainingsmaterial für Erscheinungsmodelle.
  - Human-Benchmark: pro Clip das Urteil `pass`/`fail` für „≥ 90 % ohne Identitätswechsel" samt
    Fehlerbeschreibung (z. B. „rot 3 wird nach Kreuzen mit rot 7 zu rot 30"); 250 hand-markierte
    Fußpositionen; Homographie-Kalibrierung; Flag-Pull-Ereignisse (Zeitpunkt, beteiligte Nummern).
  - Overlay-Videos (Boxen + Nummern) und Radar-Renderings zur Sichtprüfung.
- **Test-Set (privat, Labels zurückgehalten):** ein zweites Drohnen-Spiel derselben Mannschaft,
  identisch aufbereitet; Endwertung findet dort statt.
- **Transfer-Material:** 60 GoPro-Seitenlinien-Clips (WM GER–MEX) und 51 TV-Clips (WM USA–AUS)
  aus dem Materialinventar, mit Detektionen.
- **Formate:** MP4, Parquet, CSV, JPEG; Python-Paket mit CLI (`ffep cv …`) und Tests; alles läuft
  auf einem Laptop, GPU beschleunigt Training/Inferenz.
- **Datenschutz:** siehe `## Datenschutz` unten — das Material zeigt identifizierbare Personen
  (Nationalspielerinnen, Rückennummern). Bereitstellung nur mit Freigabe des Verbands, intern,
  zweckgebunden, ohne Weitergabe.

### Technische oder organisatorische Einschränkungen

- Keine AGPL-Komponenten (z. B. Ultralytics YOLO, boxmot) — die Ergebnisse sollen im
  Verbandsumfeld nutzbar bleiben. Erlaubt: alles unter Apache/MIT/BSD (RF-DETR, `trackers`,
  `supervision`, torch, transformers …).
- Python-Pipeline; Schnittstellen sind definiert (Detektionen rein, Tracks im festen Schema raus).
- Keine Cloud-Uploads des Materials; Arbeit auf bereitgestellter Infrastruktur/Laptops.
- Bewertung ausschließlich mit den bereitgestellten Skripten (Kontinuitäts-Metrik, Flag-Pull-Metrik),
  damit alle Teams dieselbe Zahl messen.

### Zielgruppe der Lösung

Die Trainerinnen und Trainer sowie die Videoanalyse der Nationalmannschaft — Nutzer ohne
ML-Kenntnisse, die Radar-Clips und Kennzahlen im Videostudium einsetzen. Technisch übernimmt das
Ergebnis das ehrenamtliche Analytics-Projekt in seine Pipeline (Phase „Tracks zu
Coaching-Kennzahlen").

### Weitere Hinweise an das Team

- Sprache: Deutsch oder Englisch. Die Code-Basis ist englisch dokumentiert, die Messprotokolle
  deutsch.
- Startpunkt am ersten Tag: Baseline laufen lassen (Minuten), Overlay eines `fail`-Clips
  anschauen, die Fehlerbeschreibung lesen — das Problem ist in einer Viertelstunde verstanden.
- Erwartete Schwierigkeit: forschungsnah. Erfolgversprechende Richtungen sind bekannt
  (selbstüberwachtes Erscheinungslernen aus Tracklets, Tracklet-Verknüpfung als
  Optimierungsproblem mit Spielregel-Priors, Bewegungsmodelle), aber keine ist erprobt. Teams mit
  ML-, Optimierungs- und Evaluations-Profil ergänzen sich.
- Ein Ansprechpartner aus dem Projekt ist während der Woche erreichbar (Pipeline, Daten,
  Fachfragen).

### Nice to have: Möglicher Nutzen für BWI und Bundeswehr

Die Kernaufgabe — mehrere sich ähnlich sehende Objekte in Luftaufnahmen über Verdeckungen hinweg
eindeutig zu verfolgen — ist nicht sportspezifisch. Naheliegende Bezüge, ohne dass die Challenge
daran hängt:

- **Sportförderung:** Die Bundeswehr fördert über die Sportfördergruppen Spitzensportlerinnen und
  -sportler; Flag Football ist ab 2028 olympisch. Leistungsdiagnostik aus Videomaterial (Lauf- und
  Abstandsdaten ohne Sensoren) ist dort unmittelbar anschlussfähig.
- **Übungs- und Ausbildungsauswertung:** Bewegungsmuster von Gruppen in Drohnenaufnahmen von
  Übungsplätzen auswerten (Formationen, Abstände, Laufwege), inklusive Sicherheitsaspekten wie
  Abstandsregeln — dasselbe Problem mit uniformer Kleidung und Verdeckung.
- **Such- und Rettungsszenarien / Lageübersicht:** Robustes Multi-Objekt-Tracking aus
  Drohnenbildern bei Verdeckung durch Gelände und Vegetation.
- **Methodisch:** Selbstüberwachtes Lernen ohne Identitäts-Labels ist genau die Situation, in der
  reale Einsatzdaten meist vorliegen — viel Material, keine Annotation.

---

## Teil 2 — Benchmark-Design und Labelling-Plan

### Benchmark-Design

| Set | Material | Labels | Zweck |
|---|---|---|---|
| Dev-Set (öffentlich) | Pilotspiel, 61 Clips, alle Artefakte der Phase 2.1 | Kontinuitäts-Urteile für alle 61 Clips, 250 GT-Fußpunkte, Flag-Pull-Ereignisse | Entwicklung, Tuning, Zwischenstände |
| Test-Set (privat) | zweites Drohnenspiel, identisch aufbereitet (Detektionen, Baseline-Tracks, Overlays) | Kontinuitäts-Urteile + Flag-Pull-Ereignisse, **zurückgehalten** | Endwertung, verhindert Overfitting auf die 61 bekannten Clips |
| Transfer-Set | 60 GoPro- + 51 TV-Clips mit Detektionen | Kontinuitäts-Urteile auf einer Stichprobe (optional, falls Zeit) | Transfer-Wertung |

**Kern-Metrik:** Anteil der Spielzüge mit `pass` („≥ 90 % des Plays ohne Identitätswechsel"),
gemessen von Menschen an gerenderten Overlays — dieselbe Definition wie im Gate-Dokument
(`docs/pilot-gate-decision.md`, C-09 Kriterium 1). Ergänzend automatisch: Anzahl Spielerinnen-Tracks
pro Clip (Ideal 10–14), verspätete Track-Starts, Fragmentzahl (`ffep cv continuity`). Für ein
vollautomatisches Scoring während der Woche können aus den Human-Urteilen IDF1-artige Proxys
abgeleitet werden — Design offen (siehe Teil 3).

**Bonus-Metrik (Flag-Pull):** Treffer, wenn der erkannte Pull-Zeitpunkt innerhalb ±0,5 s und der
Ort innerhalb ~2 Yards des gelabelten Ereignisses liegt; Precision/Recall über alle Clips mit
`outcome = pull`.

**Baseline-Zahlen (Pilot, Stand 2026-08-31):** BoT-SORT mit CMC, Torso-Crop-Team-Zuordnung, pro-Clip-
Drift-Korrektur; 20/61 Clips bewertet, 6 `pass`, obere Schranke 47/61 = 77 %. Dominanter
Fehlermodus: Identitätswechsel bei Verdeckung; sekundär vereinzelte Team-Fehlzuordnungen.

### Labelling-Plan (Nutzer, ~3,5 h gesamt)

- **A — Kontinuitäts-Urteile Clips 21–61 auf den v2-Overlays (~1 h):** `pass`/`fail` + Kurznotiz,
  Eintrag in `data/reference/continuity_review.csv` (Spalten `verdict`, `id_switches`,
  `reviewer_note`). Wichtigste Stunde — ohne sie werden die 20 bewerteten Clips zum faktischen
  Testset.
- **B — Flag-Pull-Ereignisse Pilot-Session (~1 h):** Vorlage `data/reference/flag_pull_events.csv`
  (61 Zeilen vorbefüllt). Pro Clip: `outcome` (`pull` / `incomplete` / `out_of_bounds` /
  `touchdown` / `other`), `pull_time_s` (Timecode im Player, ±0,5 s reicht), `carrier_track_id`,
  `puller_track_id` (Nummern aus dem Overlay-Video), `notes`.
- **C — Zweites Spiel als Test-Set (~1,5 h, vor November):** Pipeline vorher vom Projekt auf die
  zweite Session anwenden (Detektor, Baseline-Tracking, Overlays; prüft nebenbei die
  Generalisierung des Detektors — relevant für Phase 2.2), dann A + B dort wiederholen. Labels
  bleiben unter Verschluss.

### Datenschutz

Das Material zeigt identifizierbare Personen (Gesichter, Rückennummern). Projektregel
(`docs/capture-legal.md`): Rohmaterial und Crops sind PII, liegen nie in Versionskontrolle und
verlassen den Projektkontext nicht. Für die Challenge nötig, **bevor** eingereicht wird:
Freigabe des Verbands/Teams für eine interne, zweckgebundene Nutzung im Hackathon ohne
Weitergabe; Teilnehmende akzeptieren eine Nutzungsvereinbarung; Rückgabe/Löschung nach dem
Event. Gesichter sind bei ~30 px Körperhöhe nicht erkennbar, die Rechtslage hängt aber nicht an
der Pixelzahl. Das Formular fordert den Ausschluss sensibler/fremder personenbezogener Daten —
die Freigabe muss das ausdrücklich abdecken.

---

## Teil 3 — Offene Punkte

- Verbands-OK einholen (Nutzer; blockiert die Einreichung).
- Zweites Drohnenspiel auswählen und aufbereiten (Projekt), danach Labelling C (Nutzer).
- Scoring-Skript: Human-Urteile bleiben die Referenz; ob während der Woche ein automatischer
  Proxy (IDF1 gegen die bewerteten Clips, oder gegen eine kleine Voll-GT von 3–5 Clips) sinnvoll
  ist, entscheiden wir beim Aufbereiten der Datasets.
- Umfang des Transfer-Sets (nur Detektionen, oder auch Urteile auf einer Stichprobe).
- Infrastruktur beim Hackathon (GPU-Zugang, Datenablage ohne Cloud-Upload).
- Einordnung in die Roadmap: Challenge **vor** Phase 2.3 (Coaching-Kennzahlen brauchen stabile
  Identitäten); Phase 2.2 (Dataset Buildout) läuft unabhängig davon.
