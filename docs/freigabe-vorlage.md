# Datenfreigabe für den BWI Data Analytics Hackathon (23.–27.11.2026)

Dieses Dokument ist die schriftliche Freigabe des Verbands für die Weitergabe von
Spielmaterial an die Teams des BWI Data Analytics Hackathons. Es löst die mündliche,
pauschale Zusage vom 2026-08-31 (siehe `docs/capture-legal.md`, Nachtrag) für diesen
konkreten Zweck ab und benennt Materialklassen, Zweckbindung, Löschweg und Löschfrist
einzeln. Es ersetzt nicht die DSGVO-Einwilligungen der betroffenen Personen; diese bleiben
beim Verband (siehe `## Geltungsdauer`).

## Parteien

**Freigebende Stelle:** der Verband (Deutscher Flag-Football-Verband bzw. das
Nationalteam-Umfeld, das die Trainings-/Testspielaufnahmen ermöglicht hat).
Name, Funktion: ____________________

**Empfänger der Freigabe:** der Projektverantwortliche (Nutzer), der das Spielmaterial im
Rahmen des BWI Data Analytics Hackathons an die teilnehmenden Teams weitergibt.

## Gegenstand der Freigabe

Freigegeben werden ausschließlich die folgenden drei Materialklassen. Jede Klasse wird als
Klasse beschrieben, nicht als Aufzählung einzelner Personen.

| Materialklasse | Inhalt | Umfang | Empfänger |
|---|---|---|---|
| Dev-Set | 43 Drohnen-Clips des Pilotspiels, Detektionen, Baseline-Tracks, Oberkörper-Crops, Human-Urteile, Overlays | 43 Clips (öffentlicher Anteil des Pilotspiels) | alle Hackathon-Teams |
| Test-Set | 18 zurückgehaltene Drohnen-Clips desselben Spiels, Detektionen, Baseline-Tracks, Overlays, ohne Labels | 18 Clips (private Wertungs-Zurückhaltung) | alle Hackathon-Teams, Wertungsgrundlage |
| Transfer-Set | 60 GoPro-Seitenlinien-Clips WM GER–MEX und 51 TV-Clips WM USA–AUS mit Detektionen | 111 Clips (60 Seitenlinie, 51 TV) | alle Hackathon-Teams |

## Zweckbindung

Die Nutzung ist auf den Hackathon-Kontext beschränkt: Entwicklung und Messung von
Re-Identification-Verfahren für die drei oben genannten Materialklassen. Keine
Veröffentlichung des Materials durch die Teams, keine Weitergabe außerhalb des
Hackathon-Kontexts, kein Cloud-Upload des Materials durch die Teams (Arbeit ausschließlich
auf der bereitgestellten Infrastruktur). Diese Regeln entsprechen den operativen Vorgaben in
`docs/hackathon-challenge-reid.md ## Datenschutz` und den `### Delivery-Regeln` der drei
Bundles in `docs/hackathon-bundles.md`.

## Löschweg und Löschfrist

Der Hackathon endet am 2026-11-27. Ab diesem Datum gilt:

- Jedes teilnehmende Team löscht innerhalb von 14 Tagen, also bis spätestens `2026-12-11`,
  alle lokalen Kopien aller drei Materialklassen (Dev-Set, Test-Set, Transfer-Set) von allen
  eigenen Geräten und Speichern.
- Der Projektverantwortliche löscht die Bundle-Objekte aus dem Auslieferungs-Speicher (Open
  Telekom Cloud OBS) bis zum selben Datum, `2026-12-11`.
- Der Label-Tresor (Human-Urteile, Kontinuitäts- und Flag-Pull-Labels für das Test-Set) bleibt
  ausschließlich beim Projekt; er wurde nie an Teams weitergegeben und ist von dieser
  Löschfrist nicht betroffen, weil er sie nie verlassen hat.

## Bestätigung der Löschung

Der Hackathon-Veranstalter (BWI) bestätigt dem Projektverantwortlichen die team-seitige
Löschung in Textform (E-Mail genügt) nach Ablauf der Löschfrist. Der Projektverantwortliche
hält diese Bestätigung als datierten Nachtrag in `docs/capture-legal.md` fest.

Bestätigende Stelle/Person: ____________________

## Geltungsdauer

Diese Freigabe gilt ab dem Datum der Unterschrift bis zur dokumentierten Bestätigung der
Löschung (siehe `## Bestätigung der Löschung`). Sie ersetzt nicht die DSGVO-Einwilligungen
der betroffenen Personen; diese bleiben, wie in `docs/capture-legal.md` unter
`## DSGVO — Einverständnis liegt beim Verband` festgehalten, in der Zuständigkeit des
Verbands.

## Unterschrift

Ort, Datum: ____________________

Unterschrift: ____________________

<!-- signatur-datum: SIGNATUR-DATUM-TBD -->

Diese Marker-Zeile ist die einzige Stelle, an der das Signaturdatum gepflegt wird;
`tests/test_m2_legal_docs.py` prüft jedes andere Dokument gegen diesen Wert.
