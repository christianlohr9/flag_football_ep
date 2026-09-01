# Sichtungsprotokoll — Domänen Seitenkamera & Broadcast (2026-08-14)

Maschinenlesbare Gegenstücke: `data/reference/sighting_2026-08-14_WC-GER-vs-MEX-GOPRO.csv`
(Seitenkamera) und `data/reference/sighting_2026-08-14_WC-USA-vs-AUS-TV.csv` (Broadcast),
beide erzeugt mit `ffep cv sight --domain <sideline|broadcast> --session <session_id>`.

**Status: automatisiert erzeugt am 2026-09-01 (`uv run --extra cv ffep cv sight`,
Korrelationsschwelle 0.05) — Nutzer-Freigabe (D-11) ausstehend, siehe Ratifizierungs-Block.**

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

Wichtiger Unterschied zwischen den beiden Domänen, der die Einordnung unten prägt: die
Seitenkamera-Session zeigt das eigene Team (GER vs. MEX) aus einer erhöhten Seitenposition —
das ist exakt `docs/capture-protocol.md`s Domäne 2. Die Broadcast-Session zeigt dagegen ein
fremdes Spiel (USA vs. AUS), also weder das eigene Team noch einen Gegner aus dem eigenen
Turnierplan — reines Fremdmaterial. `docs/capture-protocol.md` definiert für diese Domäne keine
eigene Stufentabelle (nur Domäne 1 Drohne und Domäne 2 Seitenkamera); außerdem hält
`.planning/PROJECT.md` explizit fest, dass CV auf Gegner-/Fremdmaterial kein Fundament dieser
Phase ist (D-01: Gegneranalyse bleibt PBP-Charting) und dass Broadcast-Material als
Stretch-Ziel auf Phase 2.5 verschoben ist (REQ-S2-06). Die Messung unten behandelt Broadcast
trotzdem vollständig — die Sichtungs-Pipeline ist domänen-agnostisch — aber die Einordnung in
`## Konsequenzen` benennt diesen Scope-Konflikt ausdrücklich, statt ihn zu verschweigen.

## Kamera-Positionen

`ffep cv sight` gruppiert Clips wie in der Piloten-Sichtung (`docs/pilot-sighting.md`) über eine
normalisierte Kreuzkorrelation eines Framing-Fingerprints (8 über den Clip verteilte Frames,
grayscale, stark geglättet, auf 64x36 herunterskaliert, gemittelt), Schwelle 0.05.

### Seitenkamera (GoPro, `2026-08-14_WC-GER-vs-MEX-GOPRO`)

**1 Kamera-Position** über alle 60 Clips (`hp-01`, Clip-Bereich 001–060, lückenlos). Das ist
konsistent mit einer fest montierten Kamera, die über das gesamte Spiel nicht bewegt wurde —
anders als die Piloten-Drohnensession (2 Positionen) oder die Broadcast-Session unten.

### Broadcast (TV, `2026-08-14_WC-USA-vs-AUS-TV`)

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
