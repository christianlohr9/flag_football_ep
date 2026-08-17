---
created: 2026-08-17T06:20:00.000Z
title: Ratify data contract v1.0 with the Videoanalyst
area: docs
files:
  - docs/data-contract.md
  - docs/data-contract.schema.json
---

## Problem

Der Data Contract wurde am 2026-08-17 als v1.0 **einseitig festgelegt** (User-Entscheid: Analysten-Gespräch auf unbestimmte Zeit verschoben, Analyst aktuell nicht verfügbar). Beide Artefakte tragen einen konsolidierten `DEFERRED-ANALYST`-Block. Phase-1.1-Erfolgskriterium 1 („Preset mit Analyst vereinbart") ist damit nur PARTIAL erfüllt.

Zu ratifizieren (Agenda steht fertig in `docs/data-contract.md` §8):
- (a) TEAM1/posteam-Regel (TEAM1 = Charting-Perspektive-Team; posteam = TEAM1 wenn ODK == 'O', sonst TEAM2)
- (b) RESULT-Pflicht (nicht-leer) + Fumble-Semantik (Arbeitsannahme: Ballverlust-Tag, Possession-Details offen)
- (c) Defense-Vokabular aus der Sichtung inkl. der sechs offenen Sichtungs-Fragen (Preset-Vereinheitlichung, DEF-FRONT-Notation/Tupel, Front-Wechsel, COVERAGE-Namensräume/Präfixe, BLITZ Name-vs-Taktik, Scouting-Fill-Anspruch)

## Solution

Owner: Nutzer. Follow-up-Trigger: sobald der Videoanalyst wieder verfügbar ist, spätestens vor dem nächsten Filmtausch. Nach dem Gespräch: Ergebnisse in §8 eintragen, `DEFERRED-ANALYST`-Block auflösen, Schema-`status` von "provisional" auf "agreed" heben, Statuszeile der Spec auf „vereinbart am …" ändern. Weicht der Analyst von einer Arbeitsentscheidung ab: Contract-Version bumpen und Phase-1.2-Validator auf Folgeänderungen prüfen.
