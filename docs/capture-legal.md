# Rechtliches zur Videoaufnahme — Zuständigkeiten

## Zweck & Abgrenzung

Dieses Dokument hält fest, wer wofür zuständig ist. Es schafft keine Compliance-Artefakte: keine Einverständniserklärung, keine Betreiber-Checkliste, kein Nachweis-Register. Register und Länge orientieren sich am "Datenschutz-Hinweis" in `docs/data-contract.md`.

## EU-Drohnenverordnung — Zuständigkeit liegt beim Betreiber

Kategorie/Unterkategorie, Registrierung des Betreibers, Kompetenznachweis der Fernpilotin und Versicherung liegen vollständig bei der Person bzw. Stelle, die die Drohne betreibt (Staff/Analyst). Der Verfasser dieses Projekts prüft das nicht, fordert keine Nachweise an und archiviert keine.

Als reine Orientierung: der Betreiber klopft dabei typischerweise Kategorie/Registrierung, Kompetenznachweis und Versicherungsschutz ab — [ASSUMED], diese Aufzählung stammt aus Sekundärquellen und ist weder vollständig noch rechtsverbindlich.

Aufnahmen sind ohnehin auf Trainings/Testspiele beschränkt, weil die Drohne bei offiziellen Spielen nicht erlaubt ist (C-01).

## DSGVO — Einverständnis liegt beim Verband

Der Verband hält die Einverständniserklärungen; sie sind von jeder betroffenen Person erteilt und decken die Nutzung zu Analysezwecken ab. Dieses Projekt erhebt nichts Neues und erstellt keine eigene Erklärung.

Verwendungs-Scope: interne Analyse, keine Veröffentlichung — Aufnahme und Auswertung sind etwas anderes als Publikation.

Praktisch im Repo: Rohmaterial liegt unter `data/video/` und ist per `.gitignore` von der Versionskontrolle ausgeschlossen, gleiche Policy wie die personenbezogenen Hudl-Exporte unter `data/raw/hudl/*`. In `data/reference/video_inventory.csv` und `data/reference/video_sync.csv` stehen ausdrücklich keine Personennamen.

## Nicht-Ziele

- Keine Betreiber-Checkliste (bewusst zurückgestellt, könnte Staff später als Serviceleistung angeboten werden)
- Keine eigene Einverständniserklärung
- Keine namentliche Erfassung einzelner Einverständnisse
- Keine Rechtsberatung
