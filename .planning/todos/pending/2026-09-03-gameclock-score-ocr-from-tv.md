# Backlog BL-01: Gameclock und Score per CV/OCR aus TV-Aufnahmen

**Erfasst:** 2026-09-03 (HC-Gespräch, `docs/hc-notes-2026-09-03.md`)
**Entscheid Nutzer:** eigene Backlog-Phase, NICHT Teil der Hackathon-Challenge (würde sie verwässern).

**Problem:** Das WP-Modell rechnet mit synthetischer Spielzeit (C-08); die echte Gameclock ist der
größte Qualitätshebel. TV-Aufnahmen tragen Scorebug (Uhr + Score) im Bild.

**Idee:** OCR/Template-Erkennung des Scorebugs pro Frame → Uhr und Score je Spielzug →
`half_seconds_remaining` real statt `1200 / max(play_id)`.

**Voraussetzungen / offene Fragen:**
- TV-Aufnahmen von GER-Spielen (aktuell im Inventar nur USA–AUS, MEX–ESP, USA–MEX) — für WP der
  eigenen Spiele braucht es GER-Übertragungen mit Scorebug.
- Zeit-Sync Video ↔ Hudl-Play (`docs/sync-convention.md`) muss für TV-Clips vorliegen.
- Kleiner OCR-Spike (Tesseract/PaddleOCR-Lizenz prüfen, D-02) auf 2–3 Clips vor jeder Planung.

**Einordnung:** Milestone 3 Backlog; frühestens nach dem Oktober-Sync.
