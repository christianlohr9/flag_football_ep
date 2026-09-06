# IFAF World Flag 2026 — was wir jetzt an Daten haben

Stand: 2026-09-06. Kurzfassung für dich, Details (mit allen Zahlen und Belegen) stehen im
Nachtrag 2026-09-06 in `docs/ifaf-field-mapping.md`.

## Was neu dazugekommen ist

- **Beide Turniere, nicht nur Frauen:** Die API (`cpx.studio`) listet 96 Spiele — 48 Frauen
  (`ffwc26-women`, kannten wir schon) und 48 Männer (`ffwc26-men`, komplett neu geholt). Alle
  96 Spiele sind jetzt vollständig gesichert: Play-by-Play, das Events-Log, das volle
  Spieldokument und der Reviewer-Feed pro Spiel (vier Dateien pro Spiel, 389 Dateien
  insgesamt).
- **12 echte Forfeits** (0 Spielzüge), je 6 pro Turnier, in beiden Fällen immer gegen Nigeria
  (0:1 oder 1:0). Kein Fetch-Fehler, sondern reale Nichtantritte.
- **Zwei neue, bisher nie abgefragte Endpunkte:** das volle Spieldokument
  (Kader/Spieler-Statistiken) und der Reviewer-Feed mit Video-Zeitstempel pro Spielzug.
- **Personenbezug wird jetzt automatisch geschwärzt**, bevor irgendetwas auf die Platte
  geschrieben wird: E-Mail-Adressen von Reviewern/Bearbeitern und interne User-IDs werden zu
  `null`, aber das Feld bleibt sichtbar (damit man sieht, dass da mal was war). Die
  Rohdaten selbst werden wie bisher nicht committed (nur lokal, `data/raw/` ist git-ignoriert
  im Sinne dieser Session).

## Yardage — geht jetzt, mit Einschränkung

Wir können jetzt pro Spielzug ableiten, wie viele Yards gewonnen wurden (`yards_gained`),
indem wir die Ballposition von zwei aufeinanderfolgenden Spielzügen in derselben Ballbesitz-
Serie vergleichen. Sonderregeln für Touchdown, Safety, Ballverlust (Interception etc.) und
Fouls sind eingebaut, damit da nie ein erfundener Wert rauskommt. Ergebnis: **71% aller
akzeptierten IFAF-Spielzüge** haben jetzt eine echte Yardage-Zahl (vorher: 0%).

Wir haben das gegen zwei unabhängige Quellen gegengecheckt (den neuen Reviewer-Feed und das
Events-Log) — bei den Spielen, wo beide Seiten überhaupt verwertbare Daten haben, stimmen
**98%** der Ballpositionen überein. Ein gutes Zeichen: die Ableitung ist nicht geraten,
sondern trifft, was die API selbst an anderer Stelle auch aufzeichnet.

**Die schlechte Nachricht:** "Yards to go" (wie weit bis zum ersten Down) ist in dieser API
**überall** eine feste Konstante (`10`), egal wo man hinschaut — auch im Events-Log, das wir
diese Woche neu ausgewertet haben. Das ist keine echte Zahl, sondern ein Platzhalter. Und
genau dieses Feld braucht unser EP-Modell zwingend. Ergebnis: **echte EP/WP-Werte bleiben für
IFAF-Spielzüge bei 0%** — das ändert sich nicht durch bessere Yardage-Ableitung, weil das
Modell einen anderen, fehlenden Baustein braucht. Das ist die wichtigste offene Frage an den
Datenanbieter (siehe unten).

## Play-Type — deutlich besser

Über die Aktionsliste, die jeder Spielzug ohnehin mitbringt (z. B. "Pass, gefangen" oder
"Handoff, Lauf"), können wir jetzt für viel mehr Spielzüge sagen, ob es ein Lauf- oder
Passspiel war — auch bei Touchdowns und dem häufigsten Ereignistyp überhaupt ("Flag Pull").
Abdeckung: **86%** (vorher rund 40%).

## Eine unangenehme Überraschung: die Daten haben sich verschlechtert

Zwischen unserem ersten Abruf (17. August) und heute hat sich die API-Datenlage für einen
Teil der Frauen-Spiele **verschlechtert**, nicht verbessert: 11 Spiele haben jetzt weniger
Spielzüge als vorher (vermutlich eine nachträgliche Korrektur/Bereinigung durch die Reviewer),
und genau in diesen 11 Spielen fehlt jetzt bei deutlich mehr Zeilen die Angabe, welcher Down
es war. Das führt dazu, dass unsere Qualitätsprüfung mehr Frauen-Spiele aussortiert als vorher
(21 von 48 statt 32 von 48) — nicht wegen eines Fehlers bei uns, sondern weil die Rohdaten
selbst schlechter geworden sind. Männer-Spiele laufen zum ersten Mal durch: 25 von 48
akzeptiert.

## Video

Für 5.522 Spielzüge über 62 Spiele haben wir jetzt eine kompakte Tabelle mit Video-Link und
Zeitstempel (`data/processed/ifaf_video_marks.csv`) — 70% davon mit einem auflösbaren
Video-Link. Ein Link wurde stichprobenartig geprüft (nur Kopfabfrage, kein Download): frei
erreichbar, ca. 8,3 GB, `video/mp4`. Die Rohvideos liegen offenbar auf einem öffentlichen
Cloud-Speicher ohne Login.

## Was noch offen ist / was man den Anbieter fragen sollte

1. Gibt es irgendwo echte "Yards to go"-Daten, oder kennt das IFAF-Regelwerk das Konzept gar
   nicht (z. B. Zonen-System statt Downs-und-Distanz)? Das ist der einzige Blocker für
   EP/WP bei IFAF.
2. Was ist zwischen dem 17. August und heute mit den 11 betroffenen Frauen-Spielen passiert?
   Gab es eine nachträgliche Korrektur-Runde?
3. 13 Frauen-Spiele liefern im Reviewer-Feed noch gar keine Spielzüge (Grund: "keine
   Extrapunkt-Versuche markiert" — die Review ist offenbar nicht abgeschlossen). Wird das noch
   nachgeliefert?
4. Zwei weitere Endpunkte (Team- und Spielerlisten) antworten live, wurden aber diese Woche
   nur auf Erreichbarkeit geprüft, nicht abgeholt — lohnt sich für ein späteres Mal, falls
   Kader-/Spielerdaten gebraucht werden.

Alle Zahlen, Commits und der volle technische Nachtrag stehen in
`.planning/phases/01.2-repo-to-pipeline/01.2-IFAF-FULL-SUMMARY.md` und
`docs/ifaf-field-mapping.md`.
