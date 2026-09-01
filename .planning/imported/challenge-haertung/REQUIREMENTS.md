# Requirements: Challenge-Härtung Re-Identifikation Flag Football

**Defined:** 2026-09-01
**Core Value:** Am Montagmorgen liegen freigegebene Daten, eine ehrliche Baseline und eine Messvorschrift bereit, die auf einem fremden Spiel noch stimmt.

## v1 Requirements

### Recht und Freigabe

- [ ] **RECHT-01**: Die schriftliche Datenfreigabe des Verbands liegt vor und nennt ausdrücklich Dev-, Test- und Transfermaterial
- [ ] **RECHT-02**: Der Platzhalter für das Freigabedatum in der Challenge-Beschreibung ist durch das echte Datum ersetzt
- [ ] **RECHT-03**: Es steht schriftlich, wie und bis wann das Material nach dem Hackathon gelöscht wird, und wer das bestätigt
- [ ] **RECHT-04**: Die Lizenz jeder bereitgestellten Komponente ist benannt; AGPL-Anteile sind als solche markiert

### Baseline

- [ ] **BASE-01**: BoT-SORT, ByteTrack, Deep-EIoU und Global Tracklet Association sind je einmal auf dem Prüfsatz gemessen
- [ ] **BASE-02**: Die Messwerte stehen in der Challenge-Beschreibung, nicht nur im Repository
- [ ] **BASE-03**: Der Startbefehl je Verfahren ist dokumentiert und läuft auf der bereitgestellten Infrastruktur durch
- [ ] **BASE-04**: Wenn ein fertiges Verfahren die 77 % deutlich schlägt, ist die Zielmarke der Challenge angepasst

### Labels und Prüfsatz

- [ ] **DATA-01**: Der Prüfsatz umfasst mindestens drei verschiedene Spiele mit je 40 oder mehr Spielzügen
- [ ] **DATA-02**: Die Spiele unterscheiden sich in Tageszeit, Flughöhe oder Gegner — nicht drei Aufnahmen desselben Nachmittags
- [ ] **DATA-03**: Jeder Spielzug im Prüfsatz trägt Identitäts-Labels je Spielerin, nicht nur Markierungen der Spurwechsel
- [ ] **DATA-04**: Dev und Test sind nach Spiel getrennt; kein Spiel kommt in beiden vor
- [ ] **DATA-05**: Das Labeln läuft als Korrektur bestehender Spurstücke, nicht als Neuzeichnen von Kästen

### Messvorschrift

- [ ] **METR-01**: Neben der Schwellenmetrik wird eine stetige Kennzahl ausgewiesen (Identitätswechsel je Spielzug oder ein Standardmaß der Zuordnungsgüte)
- [ ] **METR-02**: Das Evaluationsskript gibt beide Kennzahlen in einem Lauf aus, getrennt nach Dev und Test
- [ ] **METR-03**: Die Challenge sagt, welche Kennzahl das Abnahmekriterium ist und welche die Zielrichtung
- [ ] **METR-04**: Ein Vergleichslauf zweier Verfahren zeigt, dass die stetige Kennzahl Unterschiede sichtbar macht, die die Schwellenmetrik verschluckt

### Einstiegspaket

- [ ] **PACK-01**: Ein Clip von etwa 30 Sekunden zeigt den Fehler: zwei Spielerinnen kreuzen, die Zuordnung springt
- [ ] **PACK-02**: Daneben liegt die Baseline-Ausgabe zum selben Clip, damit der Unterschied sichtbar ist
- [ ] **PACK-03**: Die nutzbaren Randbedingungen stehen als Liste, nicht im Fließtext (5 gegen 5, geschlossene Menge, kein Eintritt in der Feldmitte, Feldkoordinaten, Snap-Zeitpunkt)
- [ ] **PACK-04**: Die Challenge sagt, was nach dem Hackathon mit dem Ergebnis geschieht
- [ ] **PACK-05**: Ein Team kommt vom geklonten Repository zur ersten eigenen Messung in unter 30 Minuten, an einer fremden Person nachgewiesen

## v2 Requirements

Nach dem Hackathon, nicht davor.

### Übertragung

- **TRANS-01**: Dasselbe Verfahren auf Seitenlinien- und TV-Material gemessen
- **TRANS-02**: Flag-Pull-Erkennung aus Trajektorien

### Betrieb

- **OPS-01**: Auswertekette ohne AGPL-Komponenten, damit der Verband sie übernehmen kann
- **OPS-02**: Labeln als wiederholbarer Ablauf für kommende Spiele

## Out of Scope

| Feature | Reason |
|---------|--------|
| Das Tracking-Verfahren selbst bauen | Das ist die Aufgabe des Hackathon-Teams — hier wird nur die Aufgabe vorbereitet |
| Sensorik am Spieler (GNSS, RFID) | Löst das Problem, umgeht aber die Frage; die Challenge ist die Frage |
| Rückennummern lesbar machen | Andere Kameraposition, eigenes Projekt |
| Ballverfolgung | Steht in der Challenge als Nicht-Ziel |
| Mehr Spielzüge aus dem Pilotspiel | Menge ohne Breite misst dasselbe Spiel noch einmal |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| RECHT-01 … RECHT-04 | Phase 1 | Pending |
| BASE-01 … BASE-04 | Phase 2 | Pending |
| DATA-01 … DATA-05 | Phase 3 | Pending |
| METR-01 … METR-04 | Phase 4 | Pending |
| PACK-01 … PACK-05 | Phase 5 | Pending |
