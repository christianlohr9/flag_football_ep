# Datensatz-Veröffentlichung — Bewertung (Phase 2.2, Plan 02.2-19)

**Status: verfasst am 2026-09-11, nach Abschluss beider Active-Learning-Iterationen und des
Vorlabel-Bias-Tests.** Dieses Dokument bewertet die Veröffentlichungsoption, die REQ-S2-03s
drittes Erfolgskriterium ("die Veröffentlichungsoption wurde bewertet") verlangt. Es entscheidet
**nicht** über den Zeitpunkt — der ist mit D-19 bereits festgelegt (nach dem Hackathon) — und
wählt auch nicht eigenmächtig die Lizenz: RESEARCH-Annahme A5 markiert die Lizenzwahl
ausdrücklich als Rechte-Entscheidung, die der Nutzerin vorbehalten bleibt, nicht dem Forscher.
Dieses Dokument ist keine Rechtsberatung, sondern eine Projektentscheidung, dokumentiert mit
ihrer Begründung.

## 1. Was veröffentlicht würde

**Umfang, falls veröffentlicht wird (nach dem Hackathon, D-19):**

- **Vollbilder** (keine Unschärfe, keine Ausschnitte — D-20) aus drei Domänen: Drohne,
  GoPro/Hinterfeld, TV/Broadcast, zusammen mit den COCO-Annotationen (`player`/`referee`-Boxen)
  im Format, das `docs/dataset-card.md` beschreibt.
- **Datensatz v2**, DVC-getrackt unter `data/labels/dataset.dvc` (md5
  `4b1652c97f6ca4e2f0032a6a7e2334a3.dir`, `content_sha256`
  `d87dd04cb7ed53cc3436e02596233937971df0edfbcf9cff628192a9d8963dce`), 755 Bilder über die drei
  Domänen (Drohne 514, TV/Broadcast 184, GoPro/Hinterfeld 57).
- Die Split-Definition (`data/reference/frozen_eval_clips.csv`, clip-level, siehe
  `docs/dataset-card.md`) und die geprüfte Eval-Ground-Truth unter `data/labels/eval/`.
- Ein Dataset Card (`docs/dataset-card.md`) mit Provenienz, Konventionen und Limitierungen.

**Was ausdrücklich zurückgehalten wird:**

- Die private Hackathon-Testpartie (GER vs. Puerto Rico, `2026-05-16_FRIENDLY-GER-vs-
  PUERTORICO-DRONE-WIDE`) und ihre Labels — D-07/DATA-04 binden dieses Material an den
  Hackathon-Benchmark; eine Veröffentlichung würde das private Testset kontaminieren und den
  Zweck des Hackathons (unverfälschter Benchmark) im Nachhinein untergraben. Dasselbe gilt für
  die 18 eingefrorenen Drohnen- und 12 GoPro-Eval-Clips, die zugleich als privates Testset dienen
  (`data/reference/frozen_eval_clips.csv`, `private_test = true`) — diese bleiben bis nach dem
  Event zurückgehalten, nicht dauerhaft; nach dem Event ist ihre Zurückhaltung nur noch eine
  Frage des Veröffentlichungs-Timings, keine strukturelle Einschränkung mehr.
- Rohvideo, soweit es nicht Teil der extrahierten, gelabelten Frame-Menge ist — Rohmaterial ist
  laut `docs/capture-legal.md` PII und verlässt den Projektkontext grundsätzlich nicht.
- Die Hackathon-Bundle-Archive selbst (`data/bundles/*.zip`) sind Auslieferungsartefakte für die
  Teams (Zweckbindung, Löschfrist 2026-12-11, siehe `docs/hackathon-challenge-reid.md
  §Datenschutz`) und keine Veröffentlichungsartefakte — eine Veröffentlichung würde aus dem
  DVC-Datensatz und einem neuen, öffentlichen Export gebaut, nicht aus den Bundle-Archiven.

## 2. Rechtsgrundlage

Grundlage ist die Verbandsfreigabe vom **2026-08-31** (`docs/capture-legal.md`, Abschnitt
`## Nachtrag 2026-08-31 — Freigabe des Verbands`): Nutzeraussage, wörtlich "die Anfrage ist
positiv, wir haben alle Befugnisse." Diese Freigabe deckt laut demselben Dokument **drei**
Nutzungen ab: die Hackathon-Challenge, die Weitergabe des TV-Sendematerials, und die
**Veröffentlichung des Datensatzes**. Damit ist Veröffentlichung grundsätzlich abgedeckt, wird
aber bewusst bis nach dem Hackathon zurückgestellt (D-19).

**Was die Freigabe nicht ist:** eine eigene DSGVO-Rechtseinschätzung dieses Projekts. Die
Zuständigkeit für die Einwilligung der betroffenen Personen liegt beim Verband
(`docs/capture-legal.md §DSGVO — Einverständnis liegt beim Verband`); dieses Projekt erhebt keine
eigene Einverständniserklärung und archiviert keine. Die mündliche, pauschale Freigabe vom
2026-08-31 wurde für die Hackathon-Weitergabe inzwischen auf eine schriftliche, unterschriebene
Fassung (`docs/freigabe-vorlage.md`) aufgestuft, die Dev-, Test- und Transfer-Set einzeln benennt
und einen Löschweg festlegt — dieselbe Aufstufung wäre vor einer echten öffentlichen
Veröffentlichung sinnvoll nachzuvollziehen, ist aber nicht Teil dieser Bewertung, sondern ein
Punkt der Checkliste in `## 6` unten.

**Standardvorbehalt:** Dieses Dokument ersetzt keine anwaltliche Prüfung. Es dokumentiert eine
Projektentscheidung auf Basis einer dokumentierten Nutzerzusicherung, nicht ein rechtliches
Gutachten. Bei tatsächlicher Veröffentlichung identifizierbarer Personen (Gesichter,
Rückennummern) in einem öffentlichen, dauerhaft abrufbaren Datensatz ist eine erneute,
projektexterne Rechtsprüfung angemessen, bevor der erste Datensatz-Download live geht.

## 3. Plattformvergleich

**Hinweis zur Verlässlichkeit:** RESEARCH bewertet diesen Vergleich als **LOW confidence** —
er ist eine oberflächliche Recherche zum Planungszeitpunkt (2026-08-31), keine geprüfte
Evaluierung mit tatsächlichen Testuploads. Vor der eigentlichen Veröffentlichung sollte die
gewählte Plattform noch einmal mit einem kleinen Testdatensatz durchgespielt werden.

| Kriterium | HuggingFace Datasets | Zenodo | Roboflow Universe |
|---|---|---|---|
| Speichermodell | Git-basiert (LFS für grosse Dateien), eigenes `datasets`-Ladeformat (Parquet/Arrow empfohlen) | Dateibasiertes Archiv (ZIP/beliebiges Format), keine Versionierungs-API im DVC-Sinn, aber versionierte Records | Eigenes CV-natives Format (Bilder + Annotationen), automatische Format-Exporte (COCO, YOLO, etc.) |
| Lizenz-Handhabung | Freitext-Lizenzfeld im Dataset Card, SPDX-Vorschläge, keine Durchsetzung | Verbindliches Lizenzfeld bei der Einreichung, unterstützt CC-Familie direkt | Lizenzfeld pro Projekt, CC-Familie direkt unterstützt, Roboflow selbst wirbt mit "Public"-Projekten |
| Zitierbarkeit (DOI) | Kein eigener DOI, aber verlinkbar/zitierbar über den Repo-Pfad; DOI nur über externe Kopplung (z. B. Zenodo-Mirror) | **Nativ DOI-vergebend** pro Version — der Standard-Weg für zitierbare Forschungsdaten | Kein DOI nativ; Projekte sind über eine URL referenzierbar, nicht akademisch zitierfähig im DOI-Sinn |
| Sichtbarkeit in der CV-Community | Hoch für ML-/Detection-Datasets allgemein, starke Integration mit `transformers`/`datasets`-Ökosystem | Mittel — eher Forschungscommunity/Archivierung als aktive CV-Praktiker-Community | Hoch spezifisch für Objekterkennungs-Datensätze, direkte Integration mit gängigen Trainings-Frameworks (inkl. RF-DETR-Umfeld, da Roboflow RF-DETR massgeblich mitentwickelt) |
| Praktischer Pflegeaufwand (Solo-Projekt) | Mittel — Parquet/Arrow-Konvertierung ist zusätzlicher Schritt gegenüber dem bestehenden COCO-Layout | Niedrig — ein Upload pro Version, kein laufender Betrieb nötig, aber keine Möglichkeit für spätere inkrementelle Updates ohne neue Version | Niedrig bis mittel — COCO-Export existiert bereits (`ffep cv dataset`), Upload ist ein Format, das das Projekt schon spricht |

**Vorläufige Einschätzung (nicht final, siehe Vorbehalt oben):** Roboflow Universe passt am
nächsten an das bestehende COCO-Layout und die RF-DETR-Trainingskette dieses Projekts, hat aber
keinen DOI. Zenodo liefert die akademisch sauberste Zitierbarkeit, verlangt aber eine eigene
Formatentscheidung. HuggingFace Datasets liegt in der Mitte. Eine Kombination (z. B. primärer
Upload auf Roboflow Universe für die CV-Praktiker-Zielgruppe, zusätzlicher Zenodo-Eintrag für
einen DOI und die akademische Zitierbarkeit) ist plausibel, aber ebenfalls nicht final entschieden
— das ist eine Entscheidung für den Zeitpunkt der tatsächlichen Veröffentlichung, nicht für diese
Bewertung.

## 4. Lizenzempfehlung

Vier Optionen standen zur Wahl; die Auswahl selbst ist eine Rechte-Entscheidung über
identifizierbare Personen, nicht eine technische — RESEARCH-Annahme A5 markiert das ausdrücklich,
und die Entscheidung wird der Nutzerin in einem eigenen Checkpoint vorgelegt (Task 3 dieses
Plans), nicht hier vorweggenommen.

- **CC BY-NC 4.0 (Empfehlung des Forschers):** Forschung/Lehre bleibt frei nutzbar, kommerzielle
  Weiterverwertung der Spielerinnen-Abbildungen ist ausgeschlossen. Standard, gut verstanden.
  Nachteil: schliesst kommerzielle Forschungslabore aus; "nicht-kommerziell" ist an den Rändern
  notorisch unscharf definiert.
- **CC BY 4.0:** maximale Reichweite und Zitierfähigkeit, entspricht der Lizenzwahl von
  TeamTrack und SoccerTrack, den nächstliegenden Analogie-Datensätzen im Sport-CV-Bereich.
  Nachteil: erlaubt auch kommerzielle Nutzung identifizierbarer Personenabbildungen — ein
  grösserer Griff in die Verbandsfreigabe, als diese womöglich gemeint war.
- **Custom Research-Use-Lizenz mit Registrierung:** engste Kontrolle, Nutzung liesse sich an ein
  zu akzeptierendes Nutzungsdokument koppeln, wie es mehrere Sport-Datensätze handhaben. Nachteil:
  Individuallizenzen senken die Adoption und erzeugen laufenden Pflegeaufwand für ein
  Solo-Projekt.
- **Entscheidung auf den Veröffentlichungszeitpunkt verschieben:** nichts geht verloren, da
  Veröffentlichung ohnehin erst nach dem Hackathon stattfindet; bis dahin könnten mehr
  Informationen vorliegen. Nachteil: lässt eine offene Entscheidung im Abschlussprotokoll stehen
  und verzögert den Lizenzabschnitt der Dataset Card.

**Diese Empfehlung ist ausdrücklich vorläufig** — die endgültige Wahl trifft die Nutzerin im
Entscheidungs-Checkpoint dieses Plans; das Ergebnis wird unten in `## 7` als datierte
Entscheidung nachgetragen und im Lizenzabschnitt von `docs/dataset-card.md` gespiegelt.

## 5. Zeitpunkt

**Nach dem Hackathon (D-19).** Begründung, unverändert seit der Phase-Kontextfestlegung
(`.planning/phases/02.2-dataset-buildout/02.2-CONTEXT.md`): Datensatz und Benchmark zusammen
sind ein stärkeres Paket als der Datensatz allein — eine Veröffentlichung, die zugleich den ersten
öffentlichen Flag-Football-Detektions-Datensatz UND eine Referenz-Benchmark-Zahl (aus dem
Hackathon) liefert, ist überzeugender als ein isolierter Datensatz-Release. Zusätzlich vermeidet
der spätere Zeitpunkt jeden Konflikt mit dem privaten Testset: die 18 Drohnen-/12 GoPro-Clips
(`private_test = true`) und die komplette Puerto-Rico-Partie müssen bis zum Ende des Events
zurückgehalten bleiben; eine frühere Veröffentlichung des restlichen Datensatzes würde das Risiko
einer versehentlichen Vermischung mit dem privaten Testset unnötig erhöhen, ohne einen
Zeitgewinn zu rechtfertigen.

## 6. Checkliste für den Veröffentlichungszeitpunkt

Konkrete, ausführbare Schritte für den Moment, in dem tatsächlich veröffentlicht wird (nicht Teil
dieser Phase, hier nur festgehalten, damit nichts vergessen wird):

1. **Lizenzdatei finalisieren** — den Text der in `## 7` festgehaltenen Entscheidung als
   eigenständige `LICENSE`-Datei im Veröffentlichungs-Artefakt (nicht im Code-Repository —
   dessen Lizenz ist unabhängig davon Apache-2.0, siehe `docs/lizenz-inventur.md`) ablegen.
2. **Englische Dataset Card produzieren** — `docs/dataset-card.md` ist auf Deutsch verfasst
   (Projektsprache); die Veröffentlichung braucht eine englische Fassung für die internationale
   CV-Community.
3. **Eingefrorene/private Clips endgültig entfernen** — die 18 Drohnen- und 12 GoPro-Clips
   (`frozen_eval_clips.csv`, `private_test = true`) sowie die komplette Puerto-Rico-Partie aus
   dem zu veröffentlichenden Export streichen, per Skript geprüft (dieselbe
   `assert_no_frozen_eval_clips`-Guard, die bereits Trainings-Merges schützt, liesse sich für den
   Publikations-Export wiederverwenden).
4. **Hashes veröffentlichen** — `content_sha256` (aktuell `d87dd04c…` für Datensatz v2, siehe
   `## 1` oben) und die DVC-Pointer-Commits als nachprüfbare Provenienz neben dem Download
   angeben, wie in `docs/dataset-card.md §Provenienz` beschrieben.
5. **DOI registrieren, falls Zenodo gewählt wird** — Schritt entfällt, falls die Plattformwahl
   auf Roboflow Universe oder HuggingFace Datasets fällt (siehe `## 3`).
6. **Schriftliche Freigabe aktualisieren** — prüfen, ob die für den Hackathon aufgestufte
   schriftliche Freigabe (`docs/freigabe-vorlage.md`) für eine öffentliche, unbefristete
   Veröffentlichung ausreicht oder eine eigene, für Veröffentlichung formulierte Fassung mit dem
   Verband nötig ist — die bestehende Vorlage ist für die Hackathon-Zweckbindung geschrieben, nicht
   für unbefristete öffentliche Verfügbarkeit.
7. **Ankündigen** — kurzer Ankündigungstext für die gewählte Plattform und ggf. die
   CV-Sport-Analytics-Community (z. B. Verweis von Roboflow-Universe-Projektseite auf den
   Zenodo-DOI oder umgekehrt, falls beide genutzt werden).

## 7. Lizenzentscheidung (nachgetragen nach dem Entscheidungs-Checkpoint)

*Wird nach der Nutzerentscheidung in Task 3 dieses Plans ergänzt — Platzhalter, solange der
Checkpoint offen ist.*
