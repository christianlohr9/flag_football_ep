# Challenge-Härtung: Re-Identifikation Flag Football

## What This Is

Die Challenge „Wer ist wer nach der Verdeckung?" ist beim BWI Hackathon 2026
eingereicht und angenommen. Dieses Projekt bringt sie bis zum Hackathon in den
Zustand, in dem ein fremdes Team am Montagmorgen ohne Rückfragen anfangen und
am Freitag ein belastbares Ergebnis vorlegen kann. Es geht nicht darum, das
Tracking-Problem selbst zu lösen — das ist die Aufgabe des Teams.

Adressat ist der Challenge-Geber (das ehrenamtliche Analytics-Projekt der
Flag-Football-Frauennationalmannschaft), nicht die Hackathon-Orga.

## Core Value

Am Montagmorgen liegen freigegebene Daten, eine ehrliche Baseline und eine
Messvorschrift bereit, die auf einem **fremden Spiel** noch stimmt.

## Requirements

### Validated

<!-- Was schon steht und trägt. -->

- ✓ Auswertekette von Drohnenvideo zu Feldkoordinaten (~15 cm, ein Spiel < 1 h)
- ✓ Baseline BoT-SORT mit Messwert 77 % bestandener Spielzüge
- ✓ Handbewerteter Prüfsatz über 61 Spielzüge eines Pilotspiels
- ✓ Evaluationsskripte und festes Ein-/Ausgabeschema (Parquet)

### Active

- [ ] Datenfreigabe des Verbands liegt unterschrieben vor, mit Löschzusage
- [ ] Identitäts-Labels statt nur Spurwechsel-Markierungen
- [ ] Prüfsatz über mehrere Spiele, getrennt nach Spiel statt nach Spielzug
- [ ] Zweite, stetige Kennzahl neben der Ja/Nein-Schwelle
- [ ] Ehrliche Baseline: veröffentlichte Nachbearbeiter einmal durchgemessen
- [ ] Einstiegspaket: Fehlerclip, Randbedingungen, Lizenzlage, Verwertung

### Out of Scope

- Das Tracking-Verfahren selbst bauen — das ist die Aufgabe des Hackathon-Teams
- Sensorik am Spieler (GNSS, RFID) — löst das Problem, aber nicht die Frage
- Rückennummern lesbar machen (andere Kameraposition, anderes Projekt)
- Ballverfolgung — steht in der Challenge ausdrücklich als Nicht-Ziel
- Produktreife oder Übernahme in den Regelbetrieb des Verbands

## Context

Eine Recherche zum Stand der Technik (31.08.2026) hat drei Dinge belegt, die
die Arbeit hier begründen:

- **Die Aufgabe ist offen.** Jedes bestehende System kauft Identität aus einer
  Quelle, die hier fehlt: mehrere kalibrierte Kameras, Sensorik am Spieler,
  lesbare Rückennummern oder ein Mensch, der nachkorrigiert.
- **Erscheinungsbild trägt hier nicht.** Wiedererkennung verliert rund die
  Hälfte ihrer Leistung, wenn Ausschnitte von 64×128 auf 32×32 Pixel
  schrumpfen; bei identischer Kleidung bricht sie zusätzlich ein. Beides trifft
  hier zusammen (~30 Pixel Körperhöhe, gleiche Trikots).
- **Fertige Werkzeuge helfen begrenzt.** Das führende Nachbearbeitungsverfahren
  (Global Tracklet Association) verknüpft Spurstücke über gelernte
  Erscheinungsmerkmale — genau das Signal, das fehlt. Es gehört trotzdem
  gemessen, sonst kennt niemand die echte Ausgangslage.

Die Randbedingungen des Sports sind der Ersatz für das fehlende
Erscheinungsbild: fünf gegen fünf, geschlossene Menge, niemand betritt das Feld
in der Mitte, Feldkoordinaten liegen durch die Homographie vor.

Mehrere Terabyte Rohmaterial sind vorhanden. Der Prüfsatz ist deshalb eine
Auswahlfrage, keine Aufwandsfrage.

## Constraints

- **Termin**: Der Hackathon läuft 23.–27.11.2026. Alles hier muss eine Woche
  vorher fertig sein — die Teams bekommen das Material vorab.
- **Recht**: Verbandsfreigabe für Spielaufnahmen; keine Cloud-Uploads; Arbeit
  ausschließlich auf bereitgestellter Infrastruktur; Löschung nach dem Event.
- **Lizenzen**: Die bestehende Kette hängt an AGPL-Komponenten (Ultralytics,
  boxmot). Der Verband kann AGPL-Code nicht ohne Weiteres übernehmen.
- **Aufwand**: Ehrenamtliches Projekt. Labeln muss in Stunden passen, nicht in
  Wochen — deshalb Spurkorrektur statt Kästen zeichnen.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Identitäten labeln statt nur mehr Spielzüge | Ohne Labels ist die Aufgabe unüberwacht; mit Labels verdoppelt sich die Zahl gangbarer Ansätze | — Pending |
| Breite statt Menge im Prüfsatz | 61 Spielzüge aus einem Spiel sind eine Stichprobe von einem Spiel, nicht vom Problem | — Pending |
| Trennung nach Spiel, nicht nach Spielzug | Spielzüge desselben Spiels teilen Licht, Flughöhe und Personen — sonst misst der Test Auswendiglernen | — Pending |
| Fertige Verfahren vorab messen | Die 77 % sind der Wert des ersten ausprobierten Werkzeugs, nicht belegt der beste erreichbare | — Pending |
| Zweite stetige Kennzahl | Die Schwellenmetrik verbirgt echte Verbesserungen und belohnt Zufallstreffer | — Pending |

---
*Last updated: 2026-09-01 nach der Annahme der Challenge*
