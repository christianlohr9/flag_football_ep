# Abgleich: Challenge-Härtung gegen den Projektstand (Import 2026-09-01)

Der Nutzer-Entwurf (dieses Verzeichnis, Original unter `.claude/worktrees/.planning/`,
Stand 2026-09-01 14:04) wurde als **Milestone 2** ins Hauptprojekt übernommen
(`.planning/ROADMAP.md` §Milestone 2, `.planning/REQUIREMENTS.md` §Milestone 2).
Dieser Abgleich hält fest, was beim Import korrigiert, bestätigt oder als offener
Konflikt markiert wurde.

## Überholte Zahlen und Annahmen (beim Import korrigiert)

| Entwurf sagt | Tatsächlicher Stand (2026-09-01) |
|---|---|
| "Baseline BoT-SORT mit Messwert 77 %" | 77 % war die optimistische Obergrenze bei 20/61 geprüften Clips. Nach dem vollständigen Review: **15/61 = 24,6 %** (voller Prüfsatz), **10/43 = 23,3 %** (Dev-Pool). Quelle: `02.2-03-SUMMARY.md`, `docs/hackathon-benchmark-labels.md`. BASE-04s Referenzwert ist entsprechend niedriger. |
| "Die bestehende Kette hängt an AGPL-Komponenten (Ultralytics, boxmot)" | **Falsch (erfreulich):** Die Kette nutzt `rfdetr` (Apache-2.0), `trackers` (Roboflow, Apache-2.0), `supervision` (MIT) — kein boxmot, kein Ultralytics (D-02 wurde von Anfang an durchgehalten; `pyproject.toml`). v2-Requirement OPS-01 ist damit bereits erfüllt; RECHT-04 wird zur reinen Lizenz-Inventur ohne erwartete AGPL-Funde in der eigenen Kette. |
| "Evaluationsskripte und festes Schema" als vorhandene Grundlage | Bestätigt und konkreter: `scripts/hackathon/score_tracks.py` (gemeinsame Wertung), Parquet-Schemata, drei gebaute Bundles (Dev/Test/Transfer) mit Content-Hashes, Label-Vault `data/private/test-labels/`. Quelle: `docs/hackathon-bundles.md`. |
| Freigabe als offener Riegel (RECHT-01) | Die mündliche/pauschale Freigabe liegt laut Nutzer vor (dokumentiert in `docs/capture-legal.md`, Nachtrag 2026-08-31). Der Entwurf verlangt mehr: **schriftlich, unterschrieben, Materialklassen einzeln benannt, mit Löschzusage** — das bleibt als Härtungs-Aufgabe bestehen (Nutzer-seitig). Kein Widerspruch, eine Verschärfung. |

## Echte Konflikte / Entscheidungen, die Milestone 2 treffen muss

1. **DATA-04 (Dev/Test disjunkt nach Spiel) vs. aktuelles Test-Set:** Das in 2.2
   gebaute private Test-Set (18 zurückgehaltene Clips) stammt aus DEMSELBEN Spiel
   wie das Dev-Set (D-07-Fallback, bewusst dokumentiert). DATA-04 verlangt Trennung
   nach Spiel — dafür braucht es mindestens ein zweites gelabeltes Spiel (Phase M2-3).
   Bis dahin bleibt das Clip-Split-Test-Set der ehrliche Platzhalter; bei Erfolg von
   M2-3 wird `test-set`-Bundle und `frozen_eval_clips.csv` neu geschnitten
   (Re-Bundle nötig, Leak-Vault wandert mit).
2. **DATA-01 (≥3 Spiele à ≥40 Spielzüge):** Es ist genau EIN Drohnen-Spiel
   registriert (`video_inventory.csv`). Der Entwurf nennt "mehrere Terabyte
   Rohmaterial" — die Sichtung/Registrierung dieses Materials ist damit der erste
   Arbeitsschritt von M2-3 (Auswahlfrage, keine Aufwandsfrage, aber Registrierung
   + Hashing nach `docs/material-inventory.md`-Prozedur gehört dazu).
3. **Identitäts-Labels (DATA-03) vs. Hauptprojekt-Linie "ReID bleibt draußen":**
   Kein Widerspruch — die Abgrenzung des Hauptprojekts betraf das LÖSEN des
   ReID-Problems (Aufgabe der Teams). Identitäts-LABELS sind Prüfsatz-Arbeit des
   Challenge-Gebers und machen überwachte Ansätze erst bewertbar. Gleiches gilt
   für M2-2 (fertige Verfahren MESSEN ≠ Verfahren bauen).
4. **Zielmarke 90 % unter Vorbehalt (BASE-04):** Steht im Entwurf und bleibt —
   mit dem korrigierten Ausgangswert (24,6 %) ist ein "deutliches Schlagen" durch
   fertige Nachbearbeiter wahrscheinlicher als bei 77 %; die Messung (M2-2)
   entscheidet, ob die 90-%-Marke der Challenge angepasst werden muss.

## Bereits vorhandene Bausteine, die der Entwurf noch nicht kannte

- Gemeinsames Scoring: `scripts/hackathon/score_tracks.py` (M2-4 erweitert es um die
  stetige Kennzahl, statt neu zu bauen).
- Spurkorrektur-Oberfläche (M2-3, Plan 03-02): CVAT (self-hosted, aus 2.1) kann
  Track-Korrektur — Kandidat, bevor etwas Neues gebaut wird.
- Transfer-Detektionen für GoPro/TV existieren bereits (431.271 Zeilen) — Grundlage
  für v2-TRANS-01.
- Fehlerclip-Kandidaten (PACK-01): die 14 dokumentierten Continuity-Fails samt
  `reviewer_note` in `data/reference/continuity_review.csv` sind die Auswahlliste.

## Termin-Anker

**16.11.2026**: Material an die Teams (eine Woche vor Hackathon 23.–27.11.).
Milestone-2-Phasen werden zwischen die verbleibenden 2.2-Wellen geschoben, wo keine
Datei-Kollisionen bestehen; 2.2-Wellen 7–11 (Auslieferung, Retrains, Abschluss)
behalten Vorrang auf gemeinsamen Dateien.

## ID-Konvention

Die Requirement-IDs des Entwurfs (RECHT-, BASE-, DATA-, METR-, PACK-, TRANS-, OPS-)
werden unverändert übernommen; sie kollidieren nicht mit REQ-S1-/REQ-S2-.
Phasen heißen im Hauptprojekt M2-1 … M2-5 (Entwurf: Phase 1–5, project_code `reid`).
