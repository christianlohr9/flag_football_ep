---
created: 2026-09-01T00:00:00.000Z
title: Lizenz des eigenen Codes vor der Auslieferung klären
area: docs
files:
  - LICENSE
  - pyproject.toml
  - docs/lizenz-inventur.md
---

## Problem

Der ausgelieferte Code ist GPL-3.0 (`LICENSE`), während `pyproject.toml` gar kein `license`-Feld
deklariert und die gesamte Abhängigkeitskette permissiv ist (Apache-2.0/MIT/BSD, siehe
`docs/lizenz-inventur.md`). Mannschaften, die auf `ffep` aufbauen, erzeugen damit GPL-3.0-Derivate.
Das berührt genau die Adoptionsfrage des Verbands, um die die Challenge gebaut ist
(`docs/hackathon-challenge-reid.md ### Technische oder organisatorische Einschränkungen`): ein
Ergebnis, das der Verband wegen seiner Lizenz nicht übernehmen kann, verfehlt den Zweck der
Challenge auch dann, wenn es technisch stark ist.

## Solution

Die Entscheidung liegt beim Nutzer: entweder GPL-3.0 beibehalten und das in der
Challenge-Beschreibung klar benennen, oder die ausgelieferte Oberfläche (mindestens
`scripts/hackathon/score_tracks.py` und die Schemata) vor der Auslieferung permissiv
relizenzieren. In beiden Fällen gehört ein passendes `license`-Feld in `pyproject.toml`, damit die
gebaute Distributions-Metadatendatei nicht länger lizenzlos ist. Das ist eine Entscheidung, keine
Aufgabe, die dieser Plan treffen darf, und sie sollte vor dem 2026-11-16 (Materialübergabe an die
Teams) geklärt sein.

---

**Erledigt (2026-09-09):** Nutzer-Entscheidung „privat + Apache" (`docs/TODO-CHRISTIAN.md` Punkt
5). Repository-Lizenz von GPL-3.0 auf Apache-2.0 mit Namensnennung umgestellt (`LICENSE`, neue
`NOTICE`-Datei, `pyproject.toml license`-Feld ergänzt), Repository-Sichtbarkeit wird zusätzlich
auf privat gestellt (Nutzer-Aktion, `gh repo edit ... --visibility private`, noch offen).
Abhängigkeitsprüfung vor der Umstellung: kein GPL/AGPL-Laufzeitpaket, kein Blocker. Details und
Begründung: `docs/lizenz-inventur.md ## Entscheidung 2026-09-09`.
