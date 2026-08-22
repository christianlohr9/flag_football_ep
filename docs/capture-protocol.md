# Capture-Wunschzettel — Drohne & Zweitkamera

**Status: einseitig als Wunschliste verfasst am 2026-08-22 — Analysten- und Staff-Zustimmung ausstehend (siehe Ratifizierungs-Block).**

## Zweck & Ton — Wunschliste, kein Pflichtenheft

Der Verfasser dieses Dokuments fliegt die Drohne nicht selbst und hat gegenüber Staff keine Weisungsbefugnis. Alles Folgende ist eine Bitte mit Begründung, kein Anforderungskatalog — Formulierungsregister durchgehend "so wäre es für die Analyse ideal, falls machbar". Wer die Aufnahme macht, entscheidet vor Ort, was praktikabel ist.

Nichts in diesem Dokument ist Voraussetzung dafür, dass mit dem Material gearbeitet werden kann. Die drei Stufen pro Parameter existieren, damit der Empfänger eingehendes Material in Minuten einordnen kann, nicht um etwas zu verhandeln oder abzunicken.

## Wie die Stufen zu lesen sind

**Ideal** heißt: optimal für Detection, Tracking und Homographie, so wie es sich aus der Projekt-Evidenzbasis ableiten lässt. **Brauchbar** heißt: Pilotmaterial ist damit noch verwertbar, mit etwas Mehraufwand bei Vorverarbeitung oder Modellwahl. **Unbrauchbar** heißt: für Tracking nicht verwertbar — für alles andere, etwa Coaching-Review per Auge, bleibt das Material natürlich trotzdem nützlich.

Ausdrücklicher Hinweis: alle Zahlen unten sind Richtwerte, kein Messprotokoll. Sie stammen aus der Projekt-Evidenzbasis (`docs/research-notes.md`) und aus den in `.planning/PROJECT.md` festgelegten Zielwerten, nicht aus einer flag-football-spezifischen Messreihe. Und sie sind geräteneutral formuliert: Zielwerte statt Menüpfade eines bestimmten Drohnenmodells.

## Domäne 1 — Drohne (Primärdomäne)

Die Drohne ist die Primärdomäne für Trainings und Testspiele. Bei offiziellen Spielen ist sie verboten, dort trägt ausschließlich die Zweitdomäne unten.

### Hover-Position & Winkel

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | Fester Hover hinter/über der Endzone, deutlich schräg (grob 30–45° gegen die Senkrechte), Position pro Drive konstant | Beste Grundlage für Detection und Tracking |
| Brauchbar | Fester Hover mit anderem Winkel oder leichter Positionsdrift, solange die Kamera nicht aktiv geschwenkt wird | Homographie bleibt als einmalige Kalibrierung machbar |
| Unbrauchbar | Exakt senkrechte Top-Down-Sicht oder aktiv mitfliegende/schwenkende Kamera | Bewegte Kamera erzwingt ein eigenes Feld-Keypoint-Modell, das bewusst zurückgestellt ist |

Begründung: TeamTrack misst Seitenansicht mAP 52.7 gegen Top-View mAP 23.5 — Top-Down macht Detection messbar schwerer (kleine Objekte, untypische Posen), leicht schräg ist besser als exakt senkrecht und Trikotnummern bleiben teilweise sichtbar. Ein statischer Hover macht die Homographie zu einer einmaligen manuellen 4–8-Punkt-Kalibrierung pro Setup.

### Flughöhe

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | 30–60 m [Richtwert] | Ganzes Feld im Bild bei noch ausreichender Objektgröße |
| Brauchbar | 20–30 m oder 60–80 m, solange das ganze Feld im Bild bleibt bzw. die Spielerinnen groß genug abgebildet sind | Detection funktioniert noch mit etwas Mehraufwand |
| Unbrauchbar | Über 80 m oder unter 15 m | Unten schneidet der Bildausschnitt Spielerinnen ab, oben landen sie im Kleinobjekt-Regime |

Begründung: der Richtwert-Korridor balanciert Feldabdeckung gegen Objektgröße — kleine Objekte aus großer Höhe brauchen höhere Auflösung bzw. Kachel-Inferenz (SAHI), was den Piloteinsatz teurer macht.

### Auflösung

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | 3840×2160 (4K) oder höher | Genug Pixel pro Spielerin auch aus 50 m Höhe |
| Brauchbar | 2560×1440 | Detection bleibt handhabbar, mit etwas Qualitätsverlust bei kleinen Objekten |
| Unbrauchbar | 1920×1080 oder weniger bei Flughöhen über 40 m | Eine Spielerin fällt unter die übliche Kleinobjekt-Grenze, Detection bricht ein |

Begründung [Richtwert]: aus 50 m Höhe ist eine Spielerin in 4K noch wenige Dutzend Pixel hoch; in 1080p aus derselben Höhe ist sie deutlich kleiner und damit für den Detektor schwerer zu greifen.

### Bildrate

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | Konstante 50 fps oder mehr | Glatte Bewegungsschätzung für Tracking |
| Brauchbar | Konstante 25–30 fps | Tracking funktioniert noch, gröber |
| Unbrauchbar | Variable Bildrate (VFR) oder unter 24 fps | Framenummer-zu-Sekunden-Umrechnung stimmt nicht mehr |

Begründung: die Snap-Erkennung und die Sync-Anker rechnen Framenummern in Sekunden um — bei variabler Bildrate bricht diese Umrechnung, siehe `docs/sync-convention.md`.

### Belichtung & Weißabgleich

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | Manuell fest für die gesamte Aufnahme, kein Auto-ISO, kein HDR, kein automatischer Weißabgleich | Stabile Farb-Embeddings über die ganze Aufnahme |
| Brauchbar | Automatik ohne sichtbare Sprünge | Farb-Embeddings bleiben nutzbar |
| Unbrauchbar | Sichtbares Pumpen/Flackern der Belichtung, etwa bei Gegenlicht oder Wolkenwechsel | Verschiebt die Farb-Embeddings, an denen die Team-Zuordnung hängt |

Begründung: Team-Zuordnung ohne Labels läuft über Farb-Embeddings (SigLIP + UMAP + KMeans) — springende Belichtung verschiebt diese Embeddings mitten in der Aufnahme.

### Feldabdeckung

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | Ganzes Feld inklusive beider Endzonen durchgehend im Bild, mit etwas Rand | Kein Play verlässt den Bildausschnitt |
| Brauchbar | Eine feste Position pro Half-Feld-Drive, sodass der jeweils bespielte Feldteil vollständig drin ist | Tracking bleibt innerhalb des bespielten Feldteils vollständig |
| Unbrauchbar | Spielerinnen verlassen regelmäßig den Bildrand | Was außerhalb des Bildes passiert, existiert für Tracking und Homographie nicht |

Begründung: Homographie und Tracking können nur mit dem arbeiten, was im Bild ist — ein abgeschnittener Feldrand kostet keine Kalibrierungsgenauigkeit, aber jeden Play, der über den Rand hinausläuft.

### Aufnahmedauer & Akku-Wechsel

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | Ein durchgehender Clip pro Drive-Block, Akkuwechsel in der Pause zwischen zwei Drives, eine Datei pro Akkuzyklus (~20–25 min) | Klare Datei-zu-Drive-Zuordnung ohne Lücken |
| Brauchbar | Wechsel mitten in einem Drive mit kurzer Lücke, sofern nachvollziehbar bleibt, welcher Clip zu welchem Spielabschnitt gehört | Sync bleibt möglich, mit etwas Mehraufwand bei der Zuordnung |
| Unbrauchbar | Lücken mitten in einem Play oder Clips, deren Zuordnung zum Spielverlauf nicht mehr rekonstruierbar ist | Sync-Anker liegen datei-relativ — eine nicht zuordenbare Datei ist nicht synchronisierbar |

Begründung: die Akku-Laufzeit liegt bei ~20–25 min, ein Wechselprotokoll zwischen den Drives statt mittendrin hält jede Datei eindeutig einem Spielabschnitt zuordenbar.

### Sync-Signal (reiner Wunsch)

| Stufe | Zielwert | Warum |
|---|---|---|
| Ideal | Entweder Upload wie Spielfilm nach Hudl (Variante A, siehe `docs/sync-convention.md`) oder ein sichtbares Signal zu Drive-Beginn (Board, Klatschen) | Spart dem Empfänger nachträgliche manuelle Markierung |
| Brauchbar | Nichts davon — die Snaps werden nachträglich manuell markiert (Variante B) | Sync funktioniert trotzdem vollständig, nur mit mehr Handarbeit beim Nutzer |
| Unbrauchbar | entfällt | Es gibt hier bewusst keine unbrauchbare Stufe |

Ausdrücklicher Satz: es gibt hier bewusst keine unbrauchbare Stufe, weil die Sync-Konvention (`docs/sync-convention.md`) bei null Kooperation über Variante B funktioniert. Dieser Punkt spart dem Empfänger Arbeit, er verlangt keine.

## Domäne 2 — Erhöhte Seitenkamera (Zweitdomäne)

Bei offiziellen Spielen ist die Drohne verboten — dort ist diese Domäne die einzige Quelle, und für den angestrebten Dataset-Mix (~60 % Drohne / ~40 % Zweitdomäne) wird sie ohnehin von Anfang an gebraucht.

| Parameter | Ideal | Brauchbar | Unbrauchbar |
|---|---|---|---|
| Position & Winkel | Erhöhter, fester Standpunkt an der Seitenlinie auf Höhe der Mittellinie, grob 4–10 m über Feldniveau, Stativ [ASSUMED] | Niedriger (2–4 m) oder aus einer Endzonen-Ecke [ASSUMED] | Handgeführt oder mitschwenkend ohne festen Punkt |
| Auflösung | 3840×2160 (4K) oder höher | 1920×1080 — der Kleinobjekt-Malus der Höhe greift hier nicht, daher ist 1080p in dieser Domäne noch brauchbar statt unbrauchbar | Deutlich unter 1080p |
| Bildrate | Konstante 50 fps oder mehr | Konstante 25–30 fps | Variable Bildrate (VFR) oder unter 24 fps |
| Belichtung | Manuell fest für die gesamte Aufnahme | Automatik ohne sichtbare Sprünge | Sichtbares Pumpen/Flackern der Belichtung |
| Feldabdeckung | Ganzes Feld inklusive beider Endzonen durchgehend im Bild | Eine feste Position pro Half-Feld-Drive | Spielerinnen verlassen regelmäßig den Bildrand |
| Aufnahmedauer | Ein durchgehender Clip pro Drive-Block | Kurze Lücke bei nachvollziehbarer Zuordnung | Lücken mitten in einem Play |
| Sync-Signal | Sichtbares Signal zu Drive-Beginn (Board, Klatschen) | Nachträgliche manuelle Markierung (Variante B, `docs/sync-convention.md`) | entfällt |

Die Höhen- und Abstandswerte dieser Domäne sind ausdrücklich als [ASSUMED] gekennzeichnet — sie sind nirgends im Projekt festgelegt und laden zur Korrektur durch den Analysten ein.

## Material, das keine Stufe trifft

Material, das keiner der drei Stufen sauber zuzuordnen ist, wird trotzdem in `data/reference/video_inventory.csv` registriert, mit einem entsprechenden Hinweis im `notes`-Feld, damit der Bestand vollständig bleibt. Es wird nur nicht automatisch als Pilotmaterial für Phase 2.1 ausgewählt.

## Ratifizierungs-Block

> DEFERRED-ANALYST: Gespräch auf unbestimmte Zeit verschoben. Owner: Nutzer. Follow-up-Trigger: sobald der Videoanalyst wieder verfügbar ist, spätestens vor dem nächsten Filmtausch. Zu ratifizieren: (a) ob der Wunschzettel insgesamt praktikabel ist, (b) Machbarkeit des Akku-Wechsel-Timings zwischen Drives, (c) die Zielwerte der Zweitdomäne (Höhe/Standpunkt), die bislang geschätzt sind.
