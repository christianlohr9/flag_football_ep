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

## Nachtrag 2026-08-31 — Freigabe des Verbands

Der Verband hat am 2026-08-31 gegenüber dem Nutzer die Freigabe bestätigt (Nutzer-Aussage,
wörtlich): "die Anfrage ist positiv, wir haben alle Befugnisse." Diese Freigabe deckt drei
Nutzungen ab: die Hackathon-Challenge (`docs/hackathon-challenge-reid.md`), die Weitergabe des
TV-Sendematerials, und die Veröffentlichung des Datensatzes.

Diese Angabe ist eine datierte Nutzer-Zusicherung, keine eigene Rechtseinschätzung dieses
Projekts — die Zuständigkeit für die DSGVO-Einwilligung bleibt beim Verband (siehe
`## DSGVO — Einverständnis liegt beim Verband` oben). Der Satz „Verwendungs-Scope: interne
Analyse, keine Veröffentlichung" oben bleibt als bisherige Grundregel bestehen; mit dieser
Freigabe ist Veröffentlichung nun grundsätzlich abgedeckt, wird aber bewusst bis nach dem
Hackathon zurückgestellt (D-19, `.planning/phases/02.2-dataset-buildout/02.2-CONTEXT.md`).

## Nachtrag 2026-09-01 — Schriftliche Freigabe (Vorlage)

Die mündliche, pauschale Freigabe vom 2026-08-31 (siehe Nachtrag oben) war eine allgemeine
Zusicherung ohne Aufschlüsselung nach Materialklasse. Für die Weitergabe von Spielmaterial an
die Teams des BWI Data Analytics Hackathons wird sie deshalb auf eine schriftliche,
unterschriebene Freigabe aufgestuft, die Dev-, Test- und Transfer-Set einzeln benennt und
einen Löschweg festlegt (RECHT-01, RECHT-03). Das signierbare Dokument ist
`docs/freigabe-vorlage.md`.

Bis zur Unterschrift gilt das Material als nicht ausgeliefert; das Signaturdatum wird bis
dahin als der wörtliche Platzhalter-Token `SIGNATUR-DATUM-TBD` geführt (siehe Marker-Zeile in
`docs/freigabe-vorlage.md`). Die Zuständigkeit für die DSGVO-Einwilligung bleibt unverändert
beim Verband (siehe `## DSGVO — Einverständnis liegt beim Verband` oben).
