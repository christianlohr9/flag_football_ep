# Hackathon-Challenge: Wer ist wer nach der Verdeckung? — Re-Identification im Flag-Football-Tracking

**Status: Entwurf vom 2026-08-31 für den BWI Data Analytics Hackathon (23.–27. November 2026,
Einreichung über https://hackathon.bwi.de/challenges/). Freigabe des Verbands/Teams für die
Nutzung des Spielmaterials am 2026-08-31 mündlich erteilt, schriftlich fixiert in
`docs/freigabe-vorlage.md` (Signaturdatum: SIGNATUR-DATUM-TBD, siehe `## Datenschutz`).
Owner: Nutzer.**

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
Pilotspiels bestehen 15 von 61 (24,59 %) das Kriterium „≥ 90 % des Spielzugs ohne
Identitätswechsel" — gemessen über alle 61 Clips, nicht hochgerechnet; Ziel sind 90 %. Ohne
stabile Identitäten sind alle darauf aufbauenden
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
  (BoT-SORT, 15/61 = 24,59 % gemessen) messbar erhöht — Zielmarke 90 %. Ergebnis: lauffähiger Prototyp,
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

- Der Detektor ist vorgegeben und seine Detektionen liegen bereit — ihr braucht kein YOLO. Für
  Tracking-/ReID-Komponenten bitte permissive Lizenzen (Apache/MIT/BSD) bevorzugen; `trackers`,
  `supervision`, `torchreid` und `gta-link` decken alles ab, was `boxmot`/Ultralytics könnten.
  AGPL-Komponenten sind kein Ausschlusskriterium für die Bewertung, aber ein Ergebnis damit
  könnte der Verband nicht übernehmen — und darum geht es. (Lizenzhinweise zu einzelnen
  Forschungs-Repos in `## Teil 4`.)
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
| Transfer-Set 2 (geplant, verdeckt) | Bundeswehr-Inszenierung: 5 Uniformierte, Drohne schräg von oben (30–60 m Hover, Personen ~30–50 px), 10–20 Takes à 20–40 s | Ground Truth per Drehbuch (Reihenfolge rein/raus je Take) + diskretes, aus der Luft unsichtbares Merkmal zum Nachprüfen | Beweis der Übertragbarkeit auf den eigentlichen Anwendungsfall; als Überraschungs-Testset am letzten Tag |

**Szenenliste Transfer-Set 2 (bewusst schwer):** (1) fünf gehen als A-B-C-D-E hinter ein Gebäude
und kommen in vertauschter Reihenfolge wieder heraus, Verdeckungsdauern 2 s / 10 s / 30 s;
(2) Teilverdeckung und Kreuzung zweier Gruppen hinter Fahrzeugen/Vegetation; (3) „5 rein, 5 raus,
aber andere 5": einer bleibt zurück, ein anderer kommt dazu; (4) gleiche Perspektive wie das
Sportmaterial, damit nur die Domäne wechselt, nicht die Geometrie. Voraussetzungen: Freigabe
der Bundeswehr für Drohnenflug auf dem Übungsplatz und Verwendung des Materials
(Persönlichkeitsrechte der Soldaten, Geheimschutz — bei gestellten Szenen voraussichtlich
unkritisch, vorher klären). Ankündigung erst nach der Einreichung, damit niemand darauf
optimiert.

**Kern-Metrik:** Anteil der Spielzüge mit `pass` („≥ 90 % des Plays ohne Identitätswechsel"),
gemessen von Menschen an gerenderten Overlays — dieselbe Definition wie im Gate-Dokument
(`docs/pilot-gate-decision.md`, C-09 Kriterium 1). Ergänzend automatisch: Anzahl Spielerinnen-Tracks
pro Clip (Ideal 10–14), verspätete Track-Starts, Fragmentzahl (`ffep cv continuity`). Für ein
vollautomatisches Scoring während der Woche können aus den Human-Urteilen IDF1-artige Proxys
abgeleitet werden — Design offen (siehe Teil 3).

**Bonus-Metrik (Flag-Pull):** Treffer, wenn der erkannte Pull-Zeitpunkt innerhalb ±0,5 s und der
Ort innerhalb ~2 Yards des gelabelten Ereignisses liegt; Precision/Recall über alle Clips mit
`outcome = pull`.

**Baseline-Zahlen (Pilot, gemessen 2026-09-01, ersetzt die vormalige 77-%-Obergrenze über
n=20):** BoT-SORT mit CMC, Torso-Crop-Team-Zuordnung, pro-Clip-Drift-Korrektur; alle 61
Clips bewertet, **15 `pass` = 15/61 = 24,59 %** — der ungünstigere, aber gemessene (nicht
mehr hochgerechnete) Wert (`docs/hackathon-benchmark-labels.md` `## Ergebnis 2026-09-01`).
Dominanter Fehlermodus: Identitätswechsel bei Spieler-Überlagerung (39/46 Fails, ~85 %);
sekundär vereinzelte Team-Fehlzuordnungen (6/46 Fails, ~13 %).

**Vier fertige Verfahren wurden zusätzlich unter identischen Bedingungen gemessen (Phase M2-2,
`docs/baseline-messung.md`):** die Human-Zahl 15/61 (24,59 %) gilt ausschließlich für BoT-SORT,
weil die Urteile an BoT-SORT-Overlays gefällt wurden; die automatische Kontinuitäts-Kennzahl ist
dagegen gesättigt (misst Trackdauer, keine Identitätswechsel) und daher kein direkter Ersatz für
eine Human-Skala.

| Verfahren | Automatische Kontinuität (voll 61) | Human-Urteile | Lizenz |
|---|---|---|---|
| BoT-SORT | 57/61 (93,44 %) | 15/61 (24,59 %) | Apache-2.0 |
| ByteTrack (baseline-matched) | 57/61 (93,44 %) | keine Review | Apache-2.0 |
| CBIoU (baseline-matched, nicht Deep-EIoU) | 58/61 (95,08 %) | keine Review | Apache-2.0 |
| GTA (generisches Erscheinungsmodell, Überlagerungs-Vorbehalt) | 61/61 (100,00 %) | keine Review | MIT |
| Deep-EIoU | nicht gemessen — kein LICENSE-File im Referenz-Repo (D-02-Gate) | — | — |

Vollständiges Protokoll, Startbefehle und alle Vorbehalte: `docs/baseline-messung.md`.

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
verlassen den Projektkontext nicht. Der Verband hat die Freigabe für eine interne,
zweckgebundene Nutzung im Hackathon am 2026-08-31 erteilt (Nutzer-Aussage, wörtlich: "die
Anfrage ist positiv, wir haben alle Befugnisse"; datierte Nutzer-Zusicherung, vollständige
Fassung in `docs/capture-legal.md` Nachtrag 2026-08-31). Die schriftliche, unterschriebene
Freigabe, die Dev-, Test- und Transfer-Set einzeln benennt und den Löschweg festlegt, ist
`docs/freigabe-vorlage.md`, signiert am SIGNATUR-DATUM-TBD. Die Löschfrist für alle drei
Materialklassen ist 2026-12-11; die team-seitige Löschung bestätigt BWI in Textform. Die
operativen Regeln gelten unverändert: keine Cloud-Uploads des Materials (Arbeit auf
bereitgestellter Infrastruktur/Laptops, siehe
`## Teil 1 — Technische oder organisatorische Einschränkungen`); Teilnehmende akzeptieren eine
Nutzungsvereinbarung; Rückgabe/Löschung nach dem Event. Gesichter sind bei ~30 px Körperhöhe
nicht erkennbar, die Rechtslage hängt aber nicht an der Pixelzahl. Das Formular fordert den
Ausschluss sensibler/fremder personenbezogener Daten — die Freigabe deckt das ausdrücklich ab.

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
- ~~Prüfen, welche `trackers`-Version installiert ist~~ — geklärt (Phase M2-2): `trackers==2.6.0`,
  enthält SORT, ByteTrack, BoT-SORT, OC-SORT, CBIoU und McByte; BoT-SORT läuft ohne
  Erscheinungs-/ReID-Zweig (nur Bewegung + Kamerakompensation), siehe `docs/baseline-messung.md`.

---

## Teil 4 — Stand der Technik (Recherche 2026-08-31): existiert das schon?

Geprüft: SoccerNet 2022–2026, SoccerTrack/TeamTrack, SportsMOT, DanceTrack, die NFL-Kaggle-
Wettbewerbe und der Big Data Bowl, Sport-ReID- und tracklet-selbstüberwachte ReID-Literatur,
Luftbild-ReID, kommerzielle Systeme, Flag Football. Lizenzen wo möglich per GitHub-API verifiziert.

**Urteil: nicht gelöst.** Kein gefundenes System hält Identitäten für gleich gekleidete,
nummernlose, ~30-px-Spielerinnen aus Drohnen- oder TV-Sicht ohne Identitäts-Labels stabil.
Jedes starke Sportsystem löst Identität über **Rückennummern** auf Broadcast-Crops (SoccerNet-
GSR-Sieger 2024/2025 per Jersey-OCR bzw. Vision-LLM, TrackID3x3, SoccerTrack-Challenge 2025)
plus **überwachtes** ReID auf gelabelten Datensätzen. American Football: die einzige
videobasierte Identitätsarbeit (Kaggle Helmet Assignment 2021, Player Contact 2023) beruht auf
**RFID-Sensordaten** (Next Gen Stats); der Big Data Bowl ist Sensor-, nicht Videodaten. Hudl IQ
sagt öffentlich, dass Menschen im Loop die Tracks „bei den richtigen Spielern halten". Flag
Football: keinerlei Datensatz, Paper oder Produkt gefunden (Pixellot/Veo filmen nur automatisch).
Direkter Beleg für die Lücke: ein MMSports'25-Beitrag mit **exakt unserem Stack** (RF-DETR +
BoT-SORT + GTA + CLIP/PRT-ReID) erreichte auf statischem Vollfeld-Fußball HOTA 0,55 bei einer
Assoziationsgüte (AssA) von nur 0,43.

**Startpunkte, die die Challenge den Teams nennen sollte:**

| Ressource | Was | Lizenz | Nutzen |
|---|---|---|---|
| TeamTrack (CVPRW 2024) | Drohnen-Top-View 4K Fußball/Basketball + Fisheye, 4,37 M Boxen, persistente IDs; ByteTrack HOTA nur 53,7 aus der Luft | CC BY 4.0 | bestes Eval-/Pretraining-Analogon zu unserem Material |
| gta-link (GTA, Nov 2024) | Tracklet-Splitter (DBSCAN auf Embeddings) + -Connector (Clustering + Raum-Zeit-Constraints), +6,7 IDF1 auf SportsMOT | MIT | direkt hinter BoT-SORT einsteckbar |
| Kalisteo (SoccerNet-Tracking-Sieger 2023) | ReID-Modell per Triplet-Loss auf **eigenen Tracklets** nachtrainiert, dann Tracklet-Merging | Paper | genau die „ohne Labels"-Rezeptur |
| TSSL / UTAL / SSR-C | tracklet-basiertes unüberwachtes ReID (Fußgänger) | Paper / Code | Methode übertragbar |
| DanceTrack + MOTIP | gleich gekleidete Ziele, gelernte In-Context-ID-Zuordnung (HOTA 69,6) | Daten NC / Code Apache | Assoziationsideen |
| OC-SORT, Hybrid-SORT, torchreid (OSNet) | bewegungsbasierte Assoziation, ReID-Backbones | MIT | Bausteine |
| Golovkin 2025 / lianyou 2025 / Mori 2025 | 4-stufige Tracklet-Nachverarbeitung; Eindeutigkeits-Constraint (max. N Identitäten — bei uns 5 pro Team); Uniformfarben/GK-Merkmale zur Switch-Korrektur | Paper | Priors für 5v5 |
| PRAI-1581 | Luftbild-Personen-ReID bei 30–150 px (verschiedene Kleidung) | Forschung | belegt: ReID bei 30 px ist möglich, wenn Kleidung differiert |

**Lizenz-Warnungen für die Teams:** Deep-EIoU und GTATrack haben **keine LICENSE-Datei**
(GTATrack bündelt zudem AGPL-Ultralytics); PRTreID/BPBreID stehen unter Hippocratic License;
sn-gamestate/sn-reid unter GPL-3; SportsMOT, DanceTrack und der Handheld-AF-MOT-Datensatz sind
non-commercial (nur zur Evaluation nutzbar).

**Genuin offen (das ist die Challenge):** Erscheinungs-ReID für identische Trikots bei ~30 px
aus schräger Drohnensicht, selbstüberwacht aus den Tracklets des Trackers; Kombination mit
Bewegungs-/Formations-Priors und dem harten 5v5-Constraint; Identität über Kamerasichten
(Drohne + TV desselben Plays) ohne Labels; Evaluation der Kontinuität ohne
Identitäts-Ground-Truth.

---

## Teil 5 — Vorbereitungs-Checkliste bis November

### Organisatorisch (Nutzer)

- [ ] Verbands-Freigabe für das Spielmaterial (blockiert die Einreichung)
- [ ] Formular absenden (`docs/hackathon-challenge-reid-formular.md`; Datenschutz-Satz und Kontakt ausfüllen)
- [ ] Bundeswehr-Freigabe für Transfer-Set 2 (optional; Drohnenflug Übungsplatz + Materialnutzung)
- [ ] Mit BWI klären: eine GPU pro Team (24 GB-Klasse reicht, L40/48 GB komfortabel), lokale NVMe (~100 GB), ≥8 Kerne / 32 GB RAM, und ob Hugging-Face-Zugriff aus dem Netz möglich ist (sonst Weights-Cache ins Image — siehe Projekt-Aufgaben)

### Labelling (Nutzer, gesamt ~4 h — es ist WENIGER, als es aussieht)

Ausdrücklich NICHT nötig: neue Spieler-Boxen (der Detektor ist fertig und wird nicht Teil der
Challenge), neue Trainingslabels jeglicher Art, Ball,
