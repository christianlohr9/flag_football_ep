# Roadmap: Challenge-Härtung Re-Identifikation Flag Football

## Overview

Fünf Phasen von der Freigabe bis zum Einstiegspaket. Phase 1 ist ein Riegel:
Ohne unterschriebene Datenfreigabe wird die Challenge zurückgezogen, dann ist
alles Weitere umsonst. Phase 2 stellt fest, wo man wirklich steht — die 77 %
sind bisher der Wert des ersten ausprobierten Werkzeugs. Phase 3 ist die
eigentliche Arbeit am Material, Phase 4 macht Fortschritt messbar, Phase 5
sorgt dafür, dass ein fremdes Team am Montag um zehn schon rechnet und nicht
noch liest.

## Phases

- [ ] **Phase 1: Freigabe und Lizenzlage** - Riegel: ohne das hier findet nichts statt
- [ ] **Phase 2: Ehrliche Baseline** - fertige Verfahren messen, bevor jemand sie nachbaut
- [ ] **Phase 3: Labels und Prüfsatz** - Identitäten über mehrere Spiele, getrennt nach Spiel
- [ ] **Phase 4: Messvorschrift** - eine stetige Kennzahl neben der Schwelle
- [ ] **Phase 5: Einstiegspaket** - Fehlerclip, Randbedingungen, erste Messung in 30 Minuten

## Phase Details

### Phase 1: Freigabe und Lizenzlage
**Goal**: Die rechtliche Grundlage steht schriftlich, und jede gelieferte Komponente hat eine benannte Lizenz.
**Depends on**: Nothing (first phase)
**Requirements**: RECHT-01, RECHT-02, RECHT-03, RECHT-04
**Success Criteria** (what must be TRUE):
  1. Eine unterschriebene Freigabe des Verbands liegt vor und nennt Dev-, Test- und Transfermaterial einzeln
  2. In der Challenge-Beschreibung steht kein Platzhalter mehr, sondern ein Datum
  3. Es steht schriftlich, wie und bis wann gelöscht wird und wer das bestätigt
  4. Zu jeder bereitgestellten Komponente ist die Lizenz notiert, AGPL-Anteile sind markiert
**Plans**: 2 plans

Plans:
- [ ] 01-01: Freigabe einholen und die Beschreibung darauf umstellen
- [ ] 01-02: Lizenzen der Auswertekette aufnehmen, AGPL-Anteile markieren

### Phase 2: Ehrliche Baseline
**Goal**: Die Ausgangslage ist gemessen statt geschätzt — inklusive der Möglichkeit, dass ein fertiges Verfahren die Zielmarke schon fast erreicht.
**Depends on**: Phase 1
**Requirements**: BASE-01, BASE-02, BASE-03, BASE-04
**Success Criteria** (what must be TRUE):
  1. BoT-SORT, ByteTrack, Deep-EIoU und Global Tracklet Association sind je einmal auf dem Prüfsatz gemessen
  2. Die Werte stehen in der Challenge-Beschreibung
  3. Jedes Verfahren lässt sich mit einem dokumentierten Befehl wiederholen
  4. Falls ein Verfahren die 77 % deutlich schlägt, ist die Zielmarke angepasst und die Anpassung begründet
**Plans**: 2 plans

Plans:
- [ ] 02-01: Die vier Verfahren lauffähig machen und messen
- [ ] 02-02: Ergebnisse in die Beschreibung ziehen, Zielmarke prüfen

### Phase 3: Labels und Prüfsatz
**Goal**: Der Prüfsatz misst das Problem und nicht ein Spiel — mit Identitäts-Labels, die überwachtes Lernen überhaupt erst möglich machen.
**Depends on**: Phase 2
**Requirements**: DATA-01, DATA-02, DATA-03, DATA-04, DATA-05
**Success Criteria** (what must be TRUE):
  1. Mindestens drei Spiele mit je 40 oder mehr Spielzügen sind gelabelt
  2. Die Spiele unterscheiden sich in Tageszeit, Flughöhe oder Gegner
  3. Jeder Spielzug trägt Identitäten je Spielerin, nicht nur Markierungen der Spurwechsel
  4. Dev und Test enthalten disjunkte Spiele
  5. Das Labeln lief als Korrektur bestehender Spurstücke; der Zeitaufwand je Spielzug ist notiert
**Plans**: 3 plans

Plans:
- [ ] 03-01: Material sichten und drei bis vier Spiele auswählen, Auswahl begründen
- [ ] 03-02: Korrekturoberfläche für Spurstücke bauen oder eine vorhandene einrichten
- [ ] 03-03: Labeln, Trennung nach Spiel festschreiben, Prüfsatz veröffentlichen

### Phase 4: Messvorschrift
**Goal**: Fortschritt wird sichtbar, auch wenn er die Schwelle nicht überspringt.
**Depends on**: Phase 3
**Requirements**: METR-01, METR-02, METR-03, METR-04
**Success Criteria** (what must be TRUE):
  1. Das Evaluationsskript gibt Schwellenmetrik und eine stetige Kennzahl in einem Lauf aus, getrennt nach Dev und Test
  2. Die Challenge benennt, welche Kennzahl das Abnahmekriterium ist
  3. Ein Vergleich zweier Verfahren aus Phase 2 zeigt einen Unterschied, den die Schwellenmetrik verschluckt
  4. Die Skripte laufen auf dem neuen Prüfsatz ohne Anpassung durch das Team
**Plans**: 2 plans

Plans:
- [ ] 04-01: Zweite Kennzahl im Evaluationsskript ergänzen
- [ ] 04-02: Beide Kennzahlen gegen die Phase-2-Läufe stellen und die Beschreibung schärfen

### Phase 5: Einstiegspaket
**Goal**: Ein fremdes Team rechnet am Montag um zehn, statt zu lesen.
**Depends on**: Phase 4
**Requirements**: PACK-01, PACK-02, PACK-03, PACK-04, PACK-05
**Success Criteria** (what must be TRUE):
  1. Ein Clip von etwa 30 Sekunden zeigt den Identitätssprung, daneben die Baseline-Ausgabe
  2. Die nutzbaren Randbedingungen stehen als Liste in der Challenge
  3. Die Challenge sagt, was nach dem Hackathon mit dem Ergebnis geschieht
  4. Eine Person, die das Projekt nicht kennt, kommt vom Klonen zur ersten eigenen Messung in unter 30 Minuten
**Plans**: 2 plans

Plans:
- [ ] 05-01: Fehlerclip und Gegenüberstellung schneiden
- [ ] 05-02: Beschreibung, Randbedingungen und Kurzanleitung; Trockenlauf mit einer fremden Person

## Progress

**Execution Order:**
Phasen laufen in numerischer Reihenfolge: 1 → 2 → 3 → 4 → 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Freigabe und Lizenzlage | 0/2 | Not started | - |
| 2. Ehrliche Baseline | 0/2 | Not started | - |
| 3. Labels und Prüfsatz | 0/3 | Not started | - |
| 4. Messvorschrift | 0/2 | Not started | - |
| 5. Einstiegspaket | 0/2 | Not started | - |
