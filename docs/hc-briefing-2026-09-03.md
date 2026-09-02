# Briefing: Gespräch mit dem Head Coach am 2026-09-03

**Zweck:** Den HC über den Stand des Analytics-Projekts informieren und ihn als Unterstützer
(Schirmherr/Sponsor aus dem Verband) für die BWI-Hackathon-Challenge gewinnen.
**Dauer-Empfehlung:** 10 Minuten Vortrag, Rest Gespräch. Keine Pipeline-Interna zeigen.

---

## 1. Die Geschichte in drei Sätzen

1. **Was schon geht:** Aus einem Drohnenvideo berechnet die Pipeline automatisch die Position
   jeder Spielerin auf dem Feld — auf etwa 15 cm genau, ein ganzes Spiel in unter einer Stunde.
   (Der HC kennt den Showcase-Reel vom 31.08.)
2. **Was noch fehlt:** Wenn sich zwei Spielerinnen im Bild überschneiden — Kreuzungsrouten,
   Coverage, Flag-Pull — verliert das System die Zuordnung. Ehrlich gemessen: **nur 15 von 61
   Spielzügen (24,6 %)** laufen ohne Nummernwechsel durch. Ohne stabile Identitäten gibt es
   keine Routen-, Separation- oder Spacing-Kennzahlen.
3. **Wie wir das lösen:** Genau dieses Problem ist als Challenge beim **BWI Data Analytics
   Hackathon (23.–27.11.2026) angenommen**. Teams mit viel GPU-Rechenleistung arbeiten eine
   Woche daran — kostenlos für den Verband. Alles, was entsteht, ist lizenzrechtlich so gebaut,
   dass der Verband es übernehmen kann (Apache/MIT, kein AGPL).

## 2. Was er davon hat (in seiner Sprache)

- **Für die Vorbereitung:** Routen-Overlays und Routen-Klassifikation je Spielzug, Separation
  beim Catch, Time-to-Throw, Spacing der Defense — automatisch aus eigenen Drohnenaufnahmen
  (Phase 2.3, direkt nach dem Hackathon).
- **Scouting bleibt wie bisher** (Hudl-Charting + Tendenzreports in < 10 Minuten pro Gegner) —
  die Videoanalyse ergänzt, sie ersetzt nichts.
- **Horizont:** LA28. Kein anderes Flag-Football-Programm hat eigene Tracking-Daten. Das ist ein
  echter Vorsprung, den man sich jetzt erarbeitet.

## 3. Was ich konkret von ihm/vom Verband brauche (die Bitte klar machen)

1. **Unterschrift auf der Datenfreigabe** — `docs/freigabe-vorlage.md` (ausgedruckt mitnehmen).
   Nennt Dev-/Test-/Transfer-Material einzeln, Zweckbindung, Löschfrist 11.12.2026, Bestätigung.
   Das ist der Riegel: ohne Unterschrift wird die Challenge zurückgezogen.
2. **Jemand vom Verband, der hinter der Challenge steht** — Statement/Logo für die
   Challenge-Seite, idealerweise eine kurze Grußbotschaft oder Präsenz beim Hackathon.
   Wichtig für die Teams: „Da steht ein Nationalteam dahinter, nicht ein Hobbyprojekt."
3. **Aufnahme-Wünsche an die Staff** (Wunschzettel, kein Muss — `docs/capture-protocol.md`):
   Drohne steiler/senkrechter über dem Feld (weniger Verdeckungen), beide Endzonen im Bild,
   keine Schnitte innerhalb eines Spielzugs, GoPro erhöht und halbfeldweise statt Weitwinkel.
   Und: die Hudl-Breakdown-Exporte (`breakdown.xlsx`) weiter mitliefern — die machen den
   Video↔Spielzug-Abgleich möglich.
4. **Ein drittes Drohnen-Spiel** irgendwann bis November wäre Gold: der Prüfsatz soll drei
   verschiedene Spiele umfassen; aktuell haben wir zwei (Panama Rojo, Puerto Rico).

## 4. Worauf bei der Präsentation achten

- **Erst zeigen, dann erklären.** Reel (30 s) → ein Fehler-Clip (10 s: „hier springt die
  Nummer") → die eine Zahl (24,6 %) → die Lösung (Hackathon) → die Bitte.
- **Ehrlich mit der Zahl bleiben.** 24,6 % klingt schlecht, ist aber die Stärke des Vortrags:
  „Wir wissen genau, wo das Problem liegt, und wir haben es messbar gemacht."
- **Datenschutz proaktiv ansprechen:** Material zeigt identifizierbare Spielerinnen. Nutzung nur
  intern und zweckgebunden, keine Cloud außerhalb der Hackathon-Infrastruktur (Open Telekom
  Cloud, deutscher Anbieter), Löschung nach dem Event mit schriftlicher Bestätigung.
- **Nichts versprechen, was nicht steht:** kein Live-Tool am Spielfeldrand, keine Produktreife,
  Drohne bleibt bei offiziellen Spielen verboten (deshalb die zweite Kamera-Domäne).
- **Eine offene Frage nur nennen, nicht lösen:** Die Lizenz des eigenen Codes (GPL-3.0) muss
  vor der Übergabe an die Teams entschieden werden — Verbandsübernahme wäre mit einer
  permissiven Lizenz einfacher.

## 5. Demo-Ablauf (alles lokal, nichts hochladen)

| Schritt | Datei | Was sagen |
|---|---|---|
| 1. Reel (0:30) | `data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/showcase/showcase_h264_1080p.mp4` | „Das läuft heute: Video links, Radar rechts, jede Spielerin ein Punkt." |
| 2. Fehler-Reel (~0:35) | `data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/showcase/hc-demo-fehler-2026-09-03.mp4` | „Und das ist das Problem: Kreuzung, Verdeckung, Nummer springt. In 3 von 4 Spielzügen." |
| 3. Neues Spiel (0:10) | `data/labels/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE/overlays/clip_003.mp4` | „Funktioniert auch auf dem Puerto-Rico-Spiel — Ihr Material ist direkt nutzbar." |
| 4. Die Zahl | `docs/baseline-messung.md` (nur die Tabelle) | „24,6 % ohne Nummernwechsel. Fertige Verfahren haben wir gemessen — keins löst es." |
| 5. Die Bitte | `docs/freigabe-vorlage.md` (Ausdruck) | Unterschrift + Rückendeckung. |

Die Datei aus Schritt 2 wird in der Nacht zum 03.09. erzeugt (drei Fehler-Clips mit
Titelkarten hintereinander, H.264). Falls sie fehlt: Overlays `clip_009.mp4`, `clip_019.mp4`,
`clip_027.mp4` aus `…/PANAMA-ROJO-DRONE/overlays/` einzeln zeigen — dort sitzt jeweils ein
dokumentierter Nummernwechsel bei Überlappung.

## 6. Mögliche Fragen des HC — kurze Antworten

- *„Was kostet das?"* — Nichts. Ehrenamt plus Hackathon-Rechenleistung der BWI.
- *„Wann sehe ich was?"* — Nach dem Hackathon (Dezember): Routen und Separation auf den
  vorhandenen Spielen; bei jedem neuen Drohnenspiel innerhalb eines Tages.
- *„Können das die Gegner auch?"* — Öffentlich gibt es weder Datensatz noch Werkzeug dafür.
  Ob wir den Datensatz später veröffentlichen, entscheidet der Verband mit (Stand: nach dem
  Hackathon, volle Frames, Lizenz offen).
- *„Was ist mit den Spielerinnen/Datenschutz?"* — Siehe oben; Freigabe-Dokument regelt es
  schriftlich, Löschung wird bestätigt.
