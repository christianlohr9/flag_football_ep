---
created: 2026-08-17T05:50:34.878Z
title: Tap IFAF/cpx.studio WM API as additional data source
area: general
files:
  - docs/data-contract.md
---

## Problem

Die Flag-Football-WM 2026 ist über die IFAF/cpx.studio-API abrufbar — ein neuer, vom User am 2026-08-17 eingebrachter Datenschatz, der bisher nirgends im Projekt eingeplant ist. Beispiel-Endpoint (Spiel GER–USA):

`https://us.cpx.studio/v1/games/019ffff1-a8f8-7656-aaca-5f8856c4c8a4/unified-plays`

Verifizierte Struktur (Abruf 2026-08-17): flaches JSON-Array mit 90 Play-Objekten. Pro Play:
- `context`: gameClockMs, half, down, ballOn, possessionTeamId, score (home/away)
- `sequence`: Aktionsliste (PASS, COMPLETE, FLAG_PULL, …) mit playerIds/Nummern
- `outcome`: type, scoringPlay, turnover, penalty, pointsScored
- `description`: Spielernamen, Trikotnummern, Foto-URLs
- `sources`, `corrected`, `createdAt`/`updatedAt`, `id`

Wichtig: KEINE Defense-Scheme-Felder (kein COVERAGE, BLITZ oder DEF FRONT) — als Quelle für das Defense-Vokabular des Data Contracts (Phase 1.1) ungeeignet, daher dort bewusst nicht eingearbeitet.

## Solution

TBD — Kandidaten-Nutzen:
- Internationales Benchmarking / EP-Modell-Kalibrierung auf WM-Niveau (Play-by-play mit Down/Distance/Ballposition/Score/Clock reicht für EP-Zwecke)
- Gegner-Scouting (USA, MEX, …) über deren WM-Spiele
- Abgleich eigener Hudl-Daten (GER-Spiele) mit der offiziellen WM-Erfassung

Offene Fragen: Game-ID-Discovery (Listen-Endpoint?), Nutzungsbedingungen/Rate Limits, Abdeckung weiterer Turniere (EM Paris?). Als eigene Phase oder Erweiterung der Datenpipeline (Phase 1.2+) einplanen.
