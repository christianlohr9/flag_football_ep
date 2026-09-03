# Entwurf: Mail an Jona Winkel — Informationen und Bitte um Genehmigung (Hackathon-Challenge)

**Status: Entwurf vom 2026-09-03, zum Versand durch den Nutzer. Anhänge: `docs/freigabe-vorlage.md`
(als PDF), optional `docs/hackathon-challenge-reid-formular.md` (die eingereichte Beschreibung).**
Platzhalter in eckigen Klammern vor dem Versand füllen.

---

**Betreff:** Hackathon-Challenge „Wer ist wer nach der Verdeckung?" — Informationen und Bitte um Freigabe

Hallo Jona,

vielen Dank für das Gespräch heute — hier wie besprochen alle Informationen zur Hackathon-Challenge
gebündelt, zusammen mit der Bitte um deine Genehmigung für die Nutzung des Spielmaterials.

**Worum es geht.** Beim BWI Data Analytics Hackathon (23.–27. November 2026) arbeiten Teams eine
Woche lang an dem Problem, das unsere Drohnen-Tracking-Pipeline heute noch hat: Nach Überschneidungen
zweier Spielerinnen verliert das System die Zuordnung. Ehrlich gemessen laufen aktuell 15 von 61
Spielzügen (24,6 %) ohne Nummernwechsel durch; Ziel sind 90 %. Positionsgenauigkeit (~15 cm) und
Laufzeit (ein Spiel in unter einer Stunde) passen bereits. Die Challenge ist bei der BWI angenommen.

**Welches Material die Teams bekommen** (nur Drohnen-/Seitenlinien-/TV-Aufnahmen, keine
Personendaten darüber hinaus):
- Dev-Set: Freundschaftsspiel GER – Panama Rojo (16.05.2026), 61 Drohnen-Clips mit Detektionen,
  Baseline-Tracks und unserem von Hand bewerteten Prüfsatz.
- Test-Set (Labels bleiben bei uns): Freundschaftsspiel GER – Puerto Rico (16.05.2026), 61 Drohnen-Clips.
- Transfer-Material: WM-Seitenlinien- (GoPro, GER – MEX) und TV-Ausschnitte (USA – AUS).

**Wie das Material geschützt ist.** Nutzung ausschließlich für die Challenge, intern und
zweckgebunden; Arbeit nur auf der Hackathon-Infrastruktur (Open Telekom Cloud, deutscher Anbieter),
keine Weitergabe, keine Veröffentlichung ohne gesonderte Zustimmung des Verbands. Löschung durch
die Teams und die BWI bis zum 11.12.2026 mit schriftlicher Bestätigung. Alle Details stehen in der
angehängten Freigabe-Vereinbarung.

**Was der Verband davon hat.** Jede Lösung entsteht unter Lizenzen, die der Verband übernehmen
darf (Apache/MIT). Direkt danach kann ich auf unseren Spielen die Kennzahlen aufsetzen, über die wir
gesprochen haben — allen voran „wie offen ist eine Spielerin" (Separation), dazu Routen, Spacing.

**Meine Bitte an dich:**
1. Die angehängte Freigabe-Vereinbarung prüfen und unterschreiben (Datum, Name, Funktion) — sie
   benennt Dev-, Test- und Transfer-Material einzeln sowie Löschweg und Bestätigung.
2. Ein kurzes Statement des Verbands für die Challenge-Seite (zwei bis drei Sätze), damit die Teams
   sehen, dass ein Nationalteam hinter der Aufgabe steht.
3. Falls möglich: die GPS-Tracking-Daten der Spiele gegen Panama und Puerto Rico dürfen wir als
   Referenz für die Challenge nutzen — das würde die Aufgabe deutlich aufwerten.

Für die Aufnahmen bei künftigen Spielen/Camps hänge ich unseren Wunschzettel an (kein Muss):
Drohne möglichst steil über dem Feld, beide Endzonen im Bild, keine Schnitte innerhalb eines
Spielzugs; GoPro erhöht und halbfeldweise; und weiterhin die Hudl-Breakdown-Exporte dazu.

Zum nächsten Treffen im Oktober bringe ich den Stand der EPA-Berechnung mit den neuen Daten und einen
ersten Vorschlag zur Explosiveness-Kennzahl mit.

Viele Grüße
[Name]

---

*Anhänge:* Freigabe-Vereinbarung (PDF aus `docs/freigabe-vorlage.md`), Challenge-Beschreibung
(`docs/hackathon-challenge-reid-formular.md`), Wunschzettel (`docs/capture-protocol.md`, Drohnen-Teil).
