# Sichtungsprotokoll — Domänen Seitenkamera & Broadcast (2026-08-14)

Maschinenlesbare Gegenstücke: `data/reference/sighting_2026-08-14_WC-GER-vs-MEX-GOPRO.csv`
(Seitenkamera) und `data/reference/sighting_2026-08-14_WC-USA-vs-AUS-TV.csv` (Broadcast),
beide erzeugt mit `ffep cv sight --domain <sideline|broadcast> --session <session_id>`.

**Status: automatisiert erzeugt am 2026-09-01 (`uv run --extra cv ffep cv sight`,
Korrelationsschwelle 0.05) — am 2026-09-01 durch Nutzer-Freigabe (D-11) ratifiziert, siehe
Ratifizierungs-Block. Geometrie-Korrektur ebenfalls am 2026-09-01 eingearbeitet (siehe unten):
die `sideline`-Domäne (GoPro) ist keine Seitenansicht, sondern eine Hinterfeld-/Endzone-Ansicht;
die `broadcast`-Domäne (TV) ist die tatsächliche Seitenansicht.**

## Zweck & Abgrenzung

Dieses Dokument beantwortet D-10 (Domänen-Mix) mit Messung statt Annahme für die beiden bislang
nie gesichteten Domänen: die 60 GoPro-Clips der Seitenkamera-Session
`2026-08-14_WC-GER-vs-MEX-GOPRO` und die 51 TV-Broadcast-Clips der Session
`2026-08-14_WC-USA-vs-AUS-TV`. Beide Sessions lagen bis zu diesem Durchlauf unter der Notiz
`(quarantined Stand 2026-08-24)` in `data/reference/video_inventory.csv` — die Freigabe des
Verbands vom 2026-08-31 hebt diesen Status auf; die Sichtung hier ist der erste inhaltliche
Blick auf das Material.

Nicht Teil dieses Dokuments: die Mix-Entscheidung selbst (Plan 02.2-06 rechnet mit dem hier
gemessenen Domänen-Set weiter) und jede Homographie-/Trainingsentscheidung. Dieses Dokument
liefert die Messgrundlage; die verbindliche D-11-Entscheidung, ob die Seitenkamera-Domäne
nutzbar ist, folgt am Ende dieses Dokuments als Ratifizierungs-Block, sobald der Nutzer geurteilt
hat.

**Geometrie-Korrektur (2026-09-01):** Die Domänen-Bezeichnung `sideline` (GoPro) ist irreführend
benannt und wurde erst bei der menschlichen Sichtung im Rahmen der D-11-Prüfung als solche
erkannt. Die GoPro-Session ist **keine** Seitenansicht — sie filmt von hinter dem Spielfeld, aus
einer Hinterfeld-/Endzone-Position (die Kamera steht hinter einer Endzone und blickt das Feld
entlang). Die TV-Broadcast-Session ist dagegen geometrisch die tatsächliche Seitenansicht — eine
erhöhte Kamera seitlich des Spielfelds. Damit entspricht `docs/capture-protocol.md`s "Domäne 2
(erhöhte Seitenkamera)" geometrisch der **TV**-Session, nicht der GoPro-Session, obwohl der
Inventar-Domänenwert der GoPro-Session `sideline` heißt. Der `domain`-Spaltenwert in
`data/reference/video_inventory.csv` und der `--domain`-CLI-Parameter bleiben bei `sideline` für
die GoPro-Session unverändert — nachgelagerter Code (Sichtungs-Pipeline, Mix-Rechnung Plan
02.2-06) verwendet diesen Wert als Schlüssel, eine Umbenennung wäre eine Breaking Change ohne
funktionalen Nutzen. Die Korrektur lebt ausschließlich hier in der Dokumentation und in den
`notes`-Spalten der betroffenen Inventar-Zeilen (Suffix "Perspektive: Hinterfeld-/Endzone-Ansicht
von hinten, nicht Seitenansicht"). Wo unten von "Seitenkamera (GoPro)" die Rede ist, ist damit die
Hinterfeld-/Endzone-Ansicht der GoPro-Session gemeint, nicht die Domäne-2-Geometrie aus
`docs/capture-protocol.md`.

Wichtiger Unterschied zwischen den beiden Domänen, der die Einordnung unten prägt: die
Seitenkamera-Session (GoPro, tatsächlich Hinterfeld-/Endzone-Ansicht, siehe Geometrie-Korrektur
oben) zeigt das eigene Team (GER vs. MEX). Die Broadcast-Session (TV, geometrisch die
tatsächliche Seitenansicht) zeigt dagegen ein fremdes Spiel (USA vs. AUS), also weder das eigene
Team noch einen Gegner aus dem eigenen Turnierplan — reines Fremdmaterial. `docs/capture-protocol.md`
definiert für diese Domäne keine eigene Stufentabelle (nur Domäne 1 Drohne und Domäne 2
Seitenkamera); außerdem hält `.planning/PROJECT.md` explizit fest, dass CV auf Gegner-/
Fremdmaterial kein Fundament dieser Phase ist (D-01: Gegneranalyse bleibt PBP-Charting) und dass
Broadcast-Material als Stretch-Ziel auf Phase 2.5 verschoben ist (REQ-S2-06). Die Messung unten
behandelt Broadcast trotzdem vollständig — die Sichtungs-Pipeline ist domänen-agnostisch — aber
die Einordnung in `## Konsequenzen` benennt diesen Scope-Konflikt ausdrücklich, statt ihn zu
verschweigen.

## Kamera-Positionen

`ffep cv sight` gruppiert Clips wie in der Piloten-Sichtung (`docs/pilot-sighting.md`) über eine
normalisierte Kreuzkorrelation eines Framing-Fingerprints (8 über den Clip verteilte Frames,
grayscale, stark geglättet, auf 64x36 herunterskaliert, gemittelt), Schwelle 0.05.

### Seitenkamera (GoPro, `2026-08-14_WC-GER-vs-MEX-GOPRO`, Domänenwert `sideline`)

**Perspektive: Hinterfeld-/Endzone-Ansicht von hinten, nicht Seitenansicht** (siehe
Geometrie-Korrektur oben) — der Domänenwert `sideline` ist eine irreführende Bezeichnung, die aus
Code-/Inventar-Kompatibilitätsgründen unverändert bleibt.

**1 Kamera-Position** über alle 60 Clips (`hp-01`, Clip-Bereich 001–060, lückenlos). Das ist
konsistent mit einer fest montierten Kamera, die über das gesamte Spiel nicht bewegt wurde —
anders als die Piloten-Drohnensession (2 Positionen) oder die Broadcast-Session unten.

### Broadcast (TV, `2026-08-14_WC-USA-vs-AUS-TV`, Domänenwert `broadcast`)

**Perspektive: Seitenansicht** — geometrisch die tatsächliche Entsprechung zu
`docs/capture-protocol.md`s "Domäne 2 (erhöhte Seitenkamera)", trotz des Domänenwerts
`broadcast` (siehe Geometrie-Korrektur oben).

**2 Kamera-Positionen**, verschachtelt über die Session (kein sauberer Block "erste Hälfte
Position A, zweite Hälfte Position B"):

| Position | Clip-Nummern | n Clips |
|---|---|---|
| hp-01 | 1–8, 12–19, 21, 23–24, 30–37, 40–47 | 35 |
| hp-02 | 9–11, 20, 22, 25–29, 38–39, 48–51 | 16 |

Die Verschachtelung ist bei TV-Broadcast plausibel als redaktioneller Kameraschnitt zwischen zwei
Einstellungen pro Play (Übersicht/Nahaufnahme), nicht als physische Repositionierung einer
einzelnen Kamera zwischen Drives wie bei der Piloten-Drohnensession. Das ist ein struktureller
Unterschied zur Seitenkamera-Domäne, die während des gesamten Spiels bei einer einzigen Position
blieb.

## Gemessene Spielergroesse

Methode: identisch zur Piloten-Sichtung — `cv2.createBackgroundSubtractorMOG2` über (nahezu) den
gesamten Clip laufen lassen (sequenzielle Frames, nicht über den Clip verstreute Einzelbilder),
erste ~60 Frames als Aufwärmphase verwerfen, dann bewegte Vordergrund-Komponenten sammeln:
Flächen-Mindestgröße, Füllgrad- und Seitenverhältnis-Filter gegen dünne Feldlinien-/
Kompressionskanten-Artefakte, Höhen-Deckel gegen Tribünen-/Anzeigetafel-Artefakte, Rand-
Ausschluss. Ergebnis pro Clip: `apparent_player_px_p10`/`_p50` (10.-Perzentil / Median der
erkannten Blob-Höhe in Pixeln). Beide Läufe schlossen ohne einen einzigen "no moving-blob
samples recovered"-Hinweis ab (siehe Notices in der jeweiligen CSV-Erzeugung) — jeder Clip in
beiden Domänen lieferte auswertbare Blob-Messungen.

**Ausdrücklicher Hinweis: Richtwert, kein Messprotokoll.** MOG2-Hintergrundsubtraktion ist eine
Bewegungsheuristik, kein kalibriertes Messinstrument — sie erkennt vor allem Spielerinnen, die
sich im analysierten Fenster tatsächlich bewegen, und ihre Trennschärfe gegen Kompressionsrauschen
ist begrenzt. Alle Zahlen unten sind aus genau diesem Grund als Median/Perzentil über hunderte
Einzelmessungen pro Clip gebildet, nicht als Einzelwert — exakt dieselbe Einschränkung, die
`docs/pilot-sighting.md` für die Drohnen-Domäne bereits festhält.

| Domäne | n Clips | p10 Median | p50 Median | p50 Spanne |
|---|---|---|---|---|
| Seitenkamera (gesamt) | 60 | 16.5 px | 27.0 px | 22.0–33.0 px |
| Broadcast (gesamt) | 51 | 14.0 px | 23.0 px | 21.0–25.0 px |
| Broadcast hp-01 | 35 | 14.0 px | 23.0 px | 21.0–25.0 px |
| Broadcast hp-02 | 16 | 14.0 px | 23.5 px | 22.0–25.0 px |

Zum Vergleich, aus `docs/pilot-sighting.md`: die Piloten-Drohnensession maß p50 = 30.0 px
(Median über 61 Clips). Beide hier gemessenen Domänen liegen etwas darunter (Seitenkamera 27.0
px, Broadcast 23.0 px), aber deutlich über der 20-px-Schwelle, an der `_classify_tier` und
`recommend_inference_settings` beide auf `Unbrauchbar` bzw. SAHI-Kachelung umschalten. Beide
Kamera-Positionen der Broadcast-Session liegen praktisch gleichauf (23.0 px vs. 23.5 px
Median) — kein relevanter Größenunterschied zwischen den zwei Einstellungen.

## Tier-Verteilung

Alle 111 gesichteten Clips (60 Seitenkamera + 51 Broadcast) sind mit `1920x1080` in
`data/reference/video_inventory.csv` registriert. `_classify_tier` entscheidet unterhalb von
2560x1440 über die gemessene Spielergröße: `< 20 px` → `Unbrauchbar`, sonst `Brauchbar`. Da der
kleinste gemessene `apparent_player_px_p50`-Wert in beiden Domänen bei 21.0 px liegt (Broadcast),
fällt **jeder einzelne Clip in beiden Domänen auf `Brauchbar`** — keine Streuung, kein Clip
erreicht `Ideal` (das würde 4K voraussetzen) oder `Unbrauchbar`.

Einschränkung, die hier ausdrücklich benannt wird: `docs/capture-protocol.md` definiert eine
eigene Stufentabelle nur für Domäne 1 (Drohne) und Domäne 2 (Seitenkamera). Für die
Seitenkamera-Domäne trifft die generische `_classify_tier`-Logik zufällig dieselbe Aussage wie
die domänenspezifische Tabelle (1080p ist dort explizit als `Brauchbar` benannt, weil "der
Kleinobjekt-Malus der Höhe hier nicht greift"). Für Broadcast existiert **keine** eigene Tabelle
in `docs/capture-protocol.md` — die `Brauchbar`-Einstufung hier ist eine Erweiterung der
generischen Auflösungs-/Größen-Heuristik auf eine Domäne, die das Protokoll nie vorgesehen hat,
kein Abgleich gegen einen dokumentierten Zielwert.

## Ableitung Inferenz-Einstellungen

`recommend_inference_settings` bildet den Median von `apparent_player_px_p50`/`_p10` über alle
Zeilen der jeweiligen Sichtungs-CSV und wendet die in Plan 02.1-03 festgelegten Bänder an:

> Seitenkamera: measured apparent player height p50=27.0px, p10=16.5px across 60 clips; band
> 20-40 px -> resolution=896, sahi=False

> Broadcast: measured apparent player height p50=23.0px, p10=14.0px across 51 clips; band
> 20-40 px -> resolution=896, sahi=False

Beide Domänen landen im selben Band wie die Piloten-Drohnensession (p50=30.0px, ebenfalls
20–40 px) und empfehlen dieselben Einstellungen, die `ffep.toml` bereits seit der
Piloten-Sichtung trägt: **`resolution = 896`**, **`sahi = false`**. Keine `ffep.toml`-Änderung
nötig — der aktuell eingetragene Wert deckt beide neu gesichteten Domänen bereits ab.

## Konsequenzen

**Seitenkamera:** misst als eigenes Team (GER vs. MEX), eine einzige durchgehende
Kamera-Position über das ganze Spiel, alle 60 Clips `Brauchbar`, gemessene Spielergröße (p50 =
27.0 px) nahe an der Piloten-Drohnensession (p50 = 30.0 px), die im CV-Piloten bereits Position
und Laufzeit bestand (nur Kontinuität nicht). Vorschlag aus der Messung: die Domäne ist als
zweite Trainingsdomäne technisch tragfähig. Was diese Sichtung **nicht** prüft: Verdeckungshäufigkeit,
Kamerastabilität (Stativ vs. handgehalten lässt sich aus der Einzelbild-Fingerprint-Gruppierung
nicht ablesen, nur dass die Framing über die ganze Session stabil genug für eine einzelne Gruppe
war) und Trikotlesbarkeit — das sind genau die Punkte, die der menschliche Blick in `## how-to-
verify` unten prüfen soll.

**Broadcast:** misst technisch ebenfalls durchgehend `Brauchbar` (p50 = 23.0 px, 51/51 Clips),
aber das Material zeigt ein fremdes Spiel (USA vs. AUS) und fällt damit unter D-01
("Gegneranalyse bleibt PBP-Charting") und REQ-S2-06 ("Broadcast-Material ist Stretch-Ziel Phase
2.5, zurückgestellt"). Vorschlag aus der Messung: Broadcast bleibt vorerst **Transfer-Material**,
nicht zweite Trainingsdomäne dieser Phase — unabhängig von der guten gemessenen Bildqualität, weil
der Scope-Konflikt mit D-01/REQ-S2-06 unabhängig von der Pixelgröße besteht. Der verschachtelte
Zwei-Positionen-Befund (redaktionelle Kameraschnitte zwischen Einstellungen) ist ein zusätzlicher
Praktikabilitäts-Hinweis, aber nicht der Hauptgrund für diesen Vorschlag.

Diese Einschätzung ist ein Vorschlag aus der Messung, keine Entscheidung — die verbindliche
Entscheidung (D-11) folgt unten im Ratifizierungs-Block, sobald der Nutzer geurteilt hat.

## Ratifizierungs-Block

> Ratifiziert am 2026-09-01 durch den Nutzer (D-11, Checkpoint Plan 02.2-02 Task 3). Verbatim-Verdikt:
>
> "Grundsätzlich 'mehr ist immer besser' — beide Domänen nehmen. Leidet darunter die Qualität der
> Modelle, dann nur GoPro. Sollten wir uns damit schlechter stellen, bitte nur GoPro. Sonst
> beides."
>
> **Operationalisierung (mit dem Nutzer abgestimmt):** Beide Domänen — GoPro-Hinterfeld
> (Domänenwert `sideline`) und TV-Seitenansicht (Domänenwert `broadcast`) — werden als
> Trainingsdomänen zugelassen. Die Bedingung des Nutzers wird messbar gemacht durch eine
> Ablationsstudie in den Trainingsläufen: Pro-Domäne-mAP mit vs. ohne den TV-Anteil, gemessen auf
> den eingefrorenen Pro-Domäne-Eval-Splits (D-04/D-13). Verschlechtert die Aufnahme von TV die
> Pro-Domäne-mAP von Drohne oder GoPro messbar, fällt TV zurück auf Transfer-Material-Status ("nur
> GoPro") — exakt der Nutzer-Fallback. Der Worst Case ist damit identisch zum reinen
> GoPro-Ergebnis, nur mit Evidenz statt Annahme belegt. Nachgelagerte Pläne (Plan 02.2-06
> Mix/Split, Plan 02.2-09 Active Learning, Plan 02.2-15/02.2-18 Training) konsumieren dieses
> Ergebnis über diesen Ratifizierungs-Block als bindenden Input.
>
> **Scope-Hinweis:** Die Zulassung von TV als Trainingsdomäne betrifft ausschließlich die
> **Objekt-Detektion** (Domänen-Mix D-10, siehe `## Konsequenzen` oben zu D-01/REQ-S2-06). Der
> Nutzer lässt TV bewusst als Detektions-Trainingsdomäne unter der oben genannten Ablations-
> Bedingung zu; Tracking auf bewegten TV-Kameras bleibt unverändert Phase 2.5 (REQ-S2-06,
> zurückgestellt) — diese Ratifizierung ändert daran nichts.
>
> **Geometrie-Korrektur zur Kenntnis genommen:** Die GoPro-Session (`sideline`) ist eine
> Hinterfeld-/Endzone-Ansicht, keine Seitenansicht; die TV-Session (`broadcast`) ist die
> tatsächliche Seitenansicht und entspricht geometrisch `docs/capture-protocol.md`s Domäne 2. Die
> Domänenwerte in Code und Inventar bleiben unverändert (`sideline`/`broadcast`), die Korrektur ist
> rein dokumentarisch.

## Nachtrag 2026-09-02 — Fünf neue Sessions (Vorarbeit AL-Iteration 2 / Milestone-2 M2-3)

**Einordnung:** Der Nutzer hat fünf neue Aufnahme-Sessions unter `data/video/` abgelegt. Dieser
Nachtrag registriert sie in `data/reference/video_inventory.csv`, sichtet sie mit derselben
`ffep cv sight`-Pipeline wie oben und hält die Messwerte fest. Er ist doppelt zweckgebunden:
erstens als Vorarbeit für **Phase 2.2 Active-Learning-Iteration 2** (die neuen Sessions sind
Kandidaten für eine spätere Dataset-Version, sobald AL-Iteration 2 ansteht), zweitens als
Materialgrundlage für **Milestone-2 Phase M2-3** ("Labels und Prüfsatz" — Materialauswahl für
den Hackathon-Prüfsatz, `.planning/ROADMAP.md` §Milestone 2, DATA-01..05). Die fünf Sessions:

| Session | Ordner | Domäne (registriert) | Clips | Spielzüge lt. Breakdown |
|---|---|---|---|---|
| Trainingslager GER vs GER (2026-01-03) | `2026-01-03_TrainingCamp_GERvsGER_Drone_9711289` | `drone` (Wide) + `sideline` (End Zone, siehe unten) | 30 + 30 | 30 |
| Freundschaftsspiel GER vs Puerto Rico (2026-05-16) | `2026-05-16_Friendly_GERvsPuertoRico_Drone_9711283` | `drone` (Wide) + `sideline` (End Zone, siehe unten) | 61 + 10 | 66 |
| Freundschaftsspiel GER vs MEX (2026-03-01) | `2026-03-01_Friendly_GERvsMEX_GoPro_9711282` | `sideline` | 80 | 80 |
| WC MEX vs ESP (2026-08-14) | `2026-08-14_WC_MEXvsESP_TV_9711284` | `broadcast` | 88 | 88 |
| WC USA vs MEX (2024-08-31) | `2024-08-31_WC_USAvsMEX_TV_9711287` | `broadcast` | 95 | 95 |

Alle fünf Ordner enthalten zusätzlich ein `breakdown.xlsx` (Hudl-PBP-Export, ein Sheet
`Sheet1`), das **nicht** als Clip registriert wurde (siehe `docs/material-inventory.md` §Was
nicht in die CSV kommt) — die Zeilenzahl abzüglich Kopfzeile liefert aber die Spielzüge-Spalte
oben (per `openpyxl`, read-only, keine Personennamen ausgelesen). Hudl-Breakdown vorhanden →
PBP-Join möglich für alle fünf Sessions.

Die Clip-Zahlen in der obigen Tabelle sind die tatsächlich auf der Platte gefundenen — die
Ordner enthalten je einen Clip weniger, als der Nutzer beim Ablegen genannt hatte (61/72/81/89/96
statt 60/71/80/88/95); die genannten Zahlen zählten offenbar `breakdown.xlsx` mit. Harmlos, aber
der Vollständigkeit halber hier festgehalten, weil die Registrierung strikt von der Platte liest,
nicht von der Nutzer-Angabe.

### Ordner-Befund: zwei Kamera-Feeds pro Drohnen-Ordner, und eine Domänen-Korrektur

Beide Drohnen-Ordner (Trainingslager, Puerto Rico) enthalten **zwei Hudl-Winkel-Label**, `Wide`
und `End Zone`, mit teilweise identischer Clip-Nummerierung — beim Trainingslager decken beide
Label exakt dieselben 30 Spielzüge ab (volle 1:1-Überlappung, zwei simultane Kamera-Feeds),
bei Puerto Rico deckt `Wide` die Plays 001–056 und 059–063 ab (61 Clips), `End Zone` die Plays
057–066 (10 Clips), mit Teil-Überlappung bei 057–063 (vermutlich Kamera-Repositionierung Richtung
Spielende). Da `sight_session`/`clip_paths` Clips pro Domäne+`session_id` über ein
`clip_number`-keyed Dict indizieren, hätte eine gemeinsame `session_id` für beide Label bei jeder
Kollision einen Clip stillschweigend verschluckt — beide Ordner wurden deshalb in je zwei
`session_id`-Gruppen aufgeteilt (Suffix `-WIDE` bzw. eigene `-SIDELINE`-`session_id`, siehe unten).

Beim Sichten der `End Zone`-Clips stellte sich zusätzlich heraus, dass sie **nicht** dieselbe
Aufnahmecharakteristik wie ihre `Wide`-Geschwister zeigen: die Puerto-Rico-`End Zone`-Clips
weisen eine deutliche Fischaugen-/Weitwinkel-Verzerrung auf (gekrümmte Torraum- und Seitenlinien,
Bodennähe) — optisch identisch zur Signatur der beiden bestätigten GoPro-Sessions
(2026-08-14 und die neue 2026-03-01-Session, siehe Vergleichsbilder unten), nicht zur glatten,
unverzerrten Luftaufnahme der `Wide`-Clips derselben Session. Die Trainingslager-`End
Zone`-Clips zeigen eine sehr breite, konstant hohe Rahmung mit einem sichtbaren Mast-/
Stativ-Element im Bild — ebenfalls untypisch für eine frei hovernde Drohne. Beide `End
Zone`-Gruppen wurden daher **von `domain=drone` auf `domain=sideline` umregistriert**
(`fix(inventory)`-Commit dieser Session, siehe Git-Historie) und in
`2026-01-03_TRAININGCAMP-GER-vs-GER-SIDELINE` (30 Clips) bzw.
`2026-05-16_FRIENDLY-GER-vs-PUERTORICO-SIDELINE` (10 Clips) umbenannt. Diese Zuordnung stützt
sich auf visuelle Auswertung einzelner Frames, nicht auf ein kalibriertes Verfahren — eine
menschliche Bestätigung (welches physische Gerät die `End Zone`-Clips tatsächlich aufgenommen
hat) steht noch aus, ist aber für die hier gezogenen Schlüsse nicht entscheidend, weil beide
Domänen ohnehin als Trainingsdomänen zugelassen sind (D-11, siehe Ratifizierungs-Block oben).

Die verbleibenden `Wide`-Clips beider Drohnen-Ordner bleiben `domain=drone`:
`2026-01-03_TRAININGCAMP-GER-vs-GER-DRONE-WIDE` (30 Clips) und
`2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE` (61 Clips).

### Kamera-Positionen

| Session | domain | n Clips | Hover-/Kamera-Positionen |
|---|---|---|---|
| Trainingslager `-DRONE-WIDE` | drone | 30 | 2 (hp-01: 17, hp-02: 13) |
| Trainingslager `-SIDELINE` | sideline | 30 | 1 (hp-01: 30, lückenlos) |
| Puerto Rico `-DRONE-WIDE` | drone | 61 | 2 (hp-01: 33, hp-02: 28) |
| Puerto Rico `-SIDELINE` | sideline | 10 | 1 (hp-01: 10, lückenlos) |
| GoPro GER vs MEX (neu) | sideline | 80 | 1 (hp-01: 80, lückenlos) |
| TV MEX vs ESP | broadcast | 88 | 1 (hp-01: 88, lückenlos) |
| TV USA vs MEX | broadcast | 95 | 1 (hp-01: 95, lückenlos) |

Beide Drohnen-`-WIDE`-Sessions zeigen wie die Piloten-Session zwei Hover-Positionen (vermutlich
ein Positionswechsel zur Halbzeit oder zwischen Drives); alle übrigen fünf Sessions liegen bei
einer einzigen, durchgehenden Kameraposition über die gesamte Session.

### Gemessene Spielergröße

Methode identisch zu oben (MOG2-Hintergrundsubtraktion, Median/Perzentil über hunderte
Einzelmessungen pro Clip). Kein Clip in keiner der sieben Sichtungen lieferte ein
"no moving-blob samples recovered"-Notice — jeder Clip lieferte auswertbare Messungen.

| Session | n Clips | p10 Median | p50 Median | p50-Spanne | Tier |
|---|---|---|---|---|---|
| Pilot (Referenz, drone) | 61 | 16.0 px | 30.0 px | 25.0–61.0 px | 61/61 Brauchbar |
| Trainingslager `-DRONE-WIDE` | 30 | 17.0 px | 32.5 px | 22.0–67.0 px | 30/30 Brauchbar |
| Trainingslager `-SIDELINE` | 30 | 19.0 px | 34.5 px | 22.0–53.0 px | 30/30 Brauchbar |
| Puerto Rico `-DRONE-WIDE` | 61 | 17.0 px | 32.0 px | 19.0–62.0 px | 60/61 Brauchbar, 1 Unbrauchbar (Clip 25, p50=19.0) |
| Puerto Rico `-SIDELINE` | 10 | 17.0 px | 26.5 px | 23.0–41.0 px | 10/10 Brauchbar |
| GoPro GER vs MEX (neu) | 80 | 14.5 px | 28.0 px | 19.0–35.0 px | 79/80 Brauchbar, 1 Unbrauchbar (Clip 45, p50=19.0) |
| TV MEX vs ESP | 88 | 14.0 px | 22.0 px | 17.0–25.0 px | 87/88 Brauchbar, 1 Unbrauchbar (Clip 13, p50=17.0) |
| TV USA vs MEX | 95 | 13.0 px | 24.0 px | 19.0–33.0 px | 94/95 Brauchbar, 1 Unbrauchbar (Clip 87, p50=19.0) |

Zur Einordnung: beide TV-Sessions liegen bei `1280x720`, nicht `1920x1080` wie die bestehende
Broadcast-Session — niedrigere Auflösung als die bisherige Broadcast-Session, aber die gemessene
Spielergröße bleibt im selben Band. Alle sieben Sessions bestehen fast vollständig aus
`Brauchbar`-Clips; die vier vereinzelten `Unbrauchbar`-Ausreißer liegen jeweils nur 1 px unter
der 20-px-Schwelle (17.0–19.0 px) und sind Einzelclip-Rauschen, kein Domänen-Befund.
`recommend_inference_settings` bestätigt für alle sieben Sessions Band `20-40 px` →
`resolution=896`, `sahi=false` — identisch zum bereits eingetragenen `ffep.toml`-Wert, keine
Config-Änderung nötig.

### GoPro: Spielergröße nach Bildzone (nah/mittel/fern)

Zusätzliche Analyse für beide GoPro-Sessions (bestehende 2026-08-14 und neue 2026-03-01), um die
Nutzerfrage "sind weit entfernte Spielerinnen unbrauchbar klein?" mit Zahlen statt Vermutung zu
beantworten. Methode: dieselbe MOG2-Blob-Erkennung wie oben, zusätzlich wird pro akzeptiertem
Blob die vertikale Bildposition (`y + h/2`, normiert auf Bildhöhe) mitgeführt und über alle Blobs
einer Session in Terzile geteilt (unteres Drittel = "nah", mittleres = "mittel", oberes Drittel =
"fern" der Y-Verteilung). Kein produktiver Pipeline-Code wurde dafür geändert — die Analyse
importiert die privaten MOG2-Konstanten/Filter aus `cv/sighting.py` direkt, um exakt dieselbe
Filterkaskade zu verwenden, läuft aber als eigenständiges Analyseskript (nicht Teil dieses
Commits).

| Session | Zone | n Blobs | p10 | p50 |
|---|---|---|---|---|
| GoPro 2026-08-14 (bestehend) | nah (unteres Drittel) | 38.040 | 15.0 px | 28.0 px |
| GoPro 2026-08-14 (bestehend) | mittel | 38.239 | 17.0 px | 28.0 px |
| GoPro 2026-08-14 (bestehend) | fern (oberes Drittel) | 37.994 | 17.0 px | 25.0 px |
| GoPro 2026-03-01 (neu) | nah (unteres Drittel) | 50.339 | 15.0 px | 29.0 px |
| GoPro 2026-03-01 (neu) | mittel | 50.842 | 13.0 px | 32.0 px |
| GoPro 2026-03-01 (neu) | fern (oberes Drittel) | 50.292 | 16.0 px | 27.0 px |

**Ausdrücklicher Hinweis, härter als der Richtwert-Hinweis oben:** die Bild-Y-Position ist ein
Näherungs-Proxy für Tiefe, keine kalibrierte Entfernung — ohne Homographie ist "oberes Drittel"
nicht zwangsläufig "am weitesten entfernt", nur "am weitesten oben im Bild" (bei einer schräg
geneigten Kamera korrelieren beide meist, aber nicht linear). Der Höhen-Deckel
(`h > frame_height/4`, filtert Tribünen-/Anzeigetafel-Artefakte) kann zudem gerade die größten
kameranahen Spieler-Blobs mit-herausfiltern, was die "nah"-Zone systematisch nach unten
verzerren könnte. Mit diesem Vorbehalt: **der Trend geht in die erwartete Richtung** — die
"fern"-Zone misst in beiden Sessions den niedrigsten `p50` (25.0 px bzw. 27.0 px), 3–5 px unter
"nah"/"mittel" —, aber die Spanne ist schmal (kein Kollaps auf ein "unbrauchbar klein"-Niveau; alle
sechs Zonen-Werte bleiben deutlich über der 20-px-`Unbrauchbar`-Schwelle).

### Drohnen-Winkel vs. Pilot

Sichtprobe (Einzelframes, keine Homographie-Messung): die `-DRONE-WIDE`-Clips beider neuen
Sessions (Trainingslager, Puerto Rico) zeigen einen zur Piloten-Session vergleichbaren, mäßig
schrägen Hover-Winkel — keine erkennbar stärkere Aufsicht/Überkopf-Perspektive. Das
Trainingslager wurde zudem in einer Multisport-Halle ohne Football-Markierungen aufgenommen
(sichtbare Mittelkreis-Linie eines Hallenfußballfelds statt Yard-Linien), nicht auf einem
regulären Football-Feld. Die neu als `sideline` reklassifizierten `End Zone`-Clips zeigen, wo
beurteilbar (Puerto Rico), eher einen **flacheren**, boden­näheren Blickwinkel mit sichtbarer
Weitwinkel-Verzerrung als die `Wide`-Drohnen-Clips derselben Session — das Gegenteil der im
Piloten-Gate (`docs/capture-protocol.md` §Nachtrag 2026-08-31) gewünschten "steileren,
überkopfigeren" Ausrichtung.

## Die drei Einschätzungen (2026-09-02)

**(a) Gibt es eine GoPro-Konfiguration mit p50 ≥ ~28 px in den relevanten Zonen
(→ labelwürdig)?** Auf Session-Ebene: ja, knapp — die neue GoPro-Session misst p50 = 28.0 px
gesamt (bestehende: 27.0 px), beide über der ~28-px-Hausmarke bzw. unmittelbar daran. Auf
Zonen-Ebene ist das Bild gemischt: "nah" und "mittel" liegen bei beiden Sessions bei 28–32 px
(klar labelwürdig), "fern" liegt bei 25.0 px (bestehend) bzw. 27.0 px (neu) — knapp unter bzw. an
der Hausmarke. Es gibt also **keine** GoPro-Konfiguration, bei der auch die "fern"-Zone die
~28-px-Marke sicher erreicht; die Gesamt-Session-Werte werden von den nahen/mittleren Zonen nach
oben gezogen. Für Labeling-Zwecke heißt das: GoPro-Material ist insgesamt brauchbar, aber
Clips/Abschnitte mit überwiegend weit entfernten Spielerinnen sollten mit reduzierter Erwartung
an Box-Präzision eingeplant werden, nicht pauschal ausgeschlossen (kein Wert fällt unter die
20-px-`Unbrauchbar`-Schwelle).

**(b) Welche Drohnen-Session qualifiziert sich als REALES zweites Spiel für das private Test-Set
(DATA-04), und erfüllen die Drohnen-Spiele jetzt DATA-01 (≥3 Spiele à ≥40 Spielzüge)?** Die
Puerto-Rico-Session (`-DRONE-WIDE`, 61 Clips, 66 Spielzüge lt. Breakdown, andere Gegnerin als der
Pilot, selber Aufnahmetag) ist ein echtes zweites Spiel gegen einen realen Gegner mit ≥40
Spielzügen — sie qualifiziert sich für DATA-04 (Dev/Test-Trennung nach Spiel) und erfüllt für
sich genommen DATA-02 (unterscheidet sich vom Piloten-Spiel im Gegner). Die
Trainingslager-Session (`-DRONE-WIDE`, 30 Clips, 30 Spielzüge lt. Breakdown) qualifiziert sich
**nicht**: GER gegen GER ist kein Gegner-Spiel, sondern ein internes Trainingslager, und 30 < 40
Spielzüge unterschreitet ohnehin die DATA-01-Schwelle. Damit gibt es nach dieser Registrierung
**zwei** echte Drohnen-Spiele mit ≥40 Spielzügen (Pilot: 61, Puerto Rico: 66), nicht drei — DATA-01
(≥3 Spiele) ist für die Drohnen-Domäne **weiterhin nicht erfüllt**, ein drittes echtes
Drohnen-Spiel gegen einen Gegner fehlt noch. Das korrigiert die in `ABGLEICH.md` festgehaltene
Annahme "genau EIN Drohnen-Spiel registriert" auf **zwei** — ein Fortschritt, aber noch nicht die
Zielmarke drei. (`docs/material-sighting.md` ist dafür der richtige Ort für die Korrektur; die
DATA-01-Zählung selbst lebt in `.planning/imported/challenge-haertung/ABGLEICH.md`, das dieser
Nachtrag nicht editiert, weil es außerhalb des Scopes dieser Sitzung liegt.)

**(c) Zeigt eine Drohnen-Session einen steileren/überkopfigeren Winkel als der Pilot
(Gate-Retrigger-Kandidat)?** Nein. Die Sichtprobe (siehe `### Drohnen-Winkel vs. Pilot` oben)
zeigt für beide `-DRONE-WIDE`-Sessions einen zum Piloten vergleichbaren, nicht erkennbar
steileren Winkel; die als `sideline` reklassifizierten `End Zone`-Clips liegen, wo beurteilbar,
sogar flacher als der Pilot. Der im Piloten-Gate-Nachtrag (`docs/capture-protocol.md`) geäußerte
Wunsch nach einem steileren Hover-Winkel für die nächste Session ist mit diesem Material also
noch nicht erfüllt — kein Grund, das Piloten-Gate erneut aufzurollen, aber auch kein Fortschritt
in die gewünschte Richtung. Diese Einschätzung stützt sich auf Einzelframe-Sichtprobe, nicht auf
eine Winkel-Messung (keine Homographie für diese Sessions vorhanden) und ist entsprechend
vorläufig.

**Aktualisierte Bestandsaussage:** die drei oben unter "Domänen & Bestand" (`docs/material-
inventory.md`) und in `ABGLEICH.md` festgehaltenen Stand-Aussagen zum Drohnen-Bestand ("genau EIN
Drohnen-Spiel") sind mit dieser Registrierung überholt — es sind jetzt zwei echte Drohnen-Spiele
(Pilot + Puerto Rico) plus eine Trainingslager-Session ohne Gegner-Charakter. Diese Zahlen sind
hier in `docs/material-sighting.md` verbindlich festgehalten; `docs/material-inventory.md`s
eigene "Domänen & Bestand"-Tabelle trägt weiterhin den Stand 2026-08-24 als historischen
Schnappschuss (siehe deren Kopfzeile) und wird von diesem Nachtrag nicht überschrieben.
