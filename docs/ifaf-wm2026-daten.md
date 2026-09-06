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

**Update (noch am selben Tag): Doch gelöst.** Die IFAF-5v5-Regeln geben die Antwort selbst:
die Offense hat vier Downs, um die Mittellinie zu überqueren, und danach nochmal vier Downs,
um zu punkten. Die "Distanz bis zum ersten Down" ist damit nie eine feste Zahl, sondern immer
eine von zwei festen Ziellinien (Mittellinie oder gegnerische Torlinie) — und die kennen wir
ja bereits aus der Ballposition. Wir haben das implementiert und gegen das Events-Log
gegengecheckt (dort gibt es ein Feld `marker` mit echten Werten "MIDDLE"/"GOAL", auch wenn die
zugehörige Zahl weiterhin die falsche Konstante ist) — **98% Übereinstimmung**. Eine erste,
kompliziertere Version (die sich "gemerkt" hat, ob eine Serie die Mittellinie schon mal
überquert hatte) hat sich als schlechter erwiesen als die einfache Version, die pro Spielzug
neu aus der aktuellen Ballposition berechnet — ein echter, durch die Daten belegter Befund,
keine Annahme.

**Ergebnis: echte EP/WP-Werte für IFAF liegen jetzt bei 98%** (vorher ~0%, nur ein paar feste
Konstanten bei Extrapunkt-Versuchen). Das Modell rechnet jetzt echt mit für diese Spiele.

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
es war.

**Update: gelöst, pro Spiel einzeln.** Für jedes der 42 Frauen-Spiele haben wir beide
Datenstände (17. August und 6. September) durch unsere komplette Qualitätsprüfung laufen
lassen und automatisch den nehmen, der wirklich durchkommt. Ergebnis: exakt dieselben 11
Spiele bestehen nur mit dem alten Stand (keine fehlenden Downs), die anderen 31 unverändert
mit dem neuen. Kein Spiel scheitert an beiden. Die Wahl pro Spiel steht in
`data/raw/ifaf/snapshot_manifest.json` (lokal, nicht Teil des Repos, genau wie die
Rohdaten selbst). Frauen-Akzeptanz ist jetzt wieder bei **32 von 48** — exakt wie vor dieser
Woche. Männer-Spiele laufen zum ersten Mal durch: 25 von 48 akzeptiert.

## Video

Für 5.522 Spielzüge über 62 Spiele haben wir jetzt eine kompakte Tabelle mit Video-Link und
Zeitstempel (`data/processed/ifaf_video_marks.csv`) — 70% davon mit einem auflösbaren
Video-Link. Ein Link wurde stichprobenartig geprüft (nur Kopfabfrage, kein Download): frei
erreichbar, ca. 8,3 GB, `video/mp4`. Die Rohvideos liegen offenbar auf einem öffentlichen
Cloud-Speicher ohne Login.

## Was noch offen ist / was man den Anbieter fragen sollte

1. ~~Gibt es irgendwo echte "Yards to go"-Daten?~~ Diese Woche gelöst — aus den IFAF-Regeln
   selbst ableitbar, siehe oben. Kein Blocker mehr.
2. Was ist zwischen dem 17. August und heute mit den 11 betroffenen Frauen-Spielen passiert?
   Gab es eine nachträgliche Korrektur-Runde? (Diese Woche nur umschifft, nicht geklärt —
   wäre gut zu wissen, ob sich das bei künftigen Abrufen wiederholt.)
3. 13 Frauen-Spiele liefern im Reviewer-Feed noch gar keine Spielzüge (Grund: "keine
   Extrapunkt-Versuche markiert" — die Review ist offenbar nicht abgeschlossen). Wird das noch
   nachgeliefert?
4. Zwei weitere Endpunkte (Team- und Spielerlisten) antworten live, wurden aber diese Woche
   nur auf Erreichbarkeit geprüft, nicht abgeholt — lohnt sich für ein späteres Mal, falls
   Kader-/Spielerdaten gebraucht werden.

## Wichtiger Nachtrag: Männer- und Frauen-Spiele wurden vermischt

Beim Nachrechnen ist aufgefallen: Die beiden Turnierdokumente (Frauen und Männer) tragen
intern denselben Namen ("IFAF World Flag 2026"), nur ein Zusatzfeld ("Women"/"Men")
unterscheidet sie. Unser Ingest hat bisher nur den Namen genommen — dadurch sind alle 25
akzeptierten Männer-Spiele unbemerkt unter derselben Kennung wie die Frauen-Spiele gelandet.
Zusätzlich verschärft: der deutsche Männer-Nationalteam-Code (`m-ger`) wird in unserer
Team-Tabelle auf denselben Kürzel "GER" abgebildet wie das Frauenteam — ein Bericht, der nach
Team "GER" filtert, hätte also Männer- und Frauendaten vermischt, sobald die Männer-Spiele
mal im Datensatz landen.

**Behoben:**
- Die Wettbewerbs-Kennung unterscheidet jetzt zwischen "IFAF World Flag 2026 Women" und
  "...Men" (32 Frauen-Spiele / 3.191 Zeilen, 25 Männer-Spiele / 2.305 Zeilen).
- Die Männer-Spiele bekommen eine eigene Kategorie ("mens-international") und sind ab sofort
  von jedem EP/WP-Training ausgeschlossen — sie bleiben im Datensatz und werden auch weiter
  bewertet (echte EP/WP-Werte, genau wie bei den Frauen), aber sie fließen nicht ins Training
  der Modelle ein, bis das jemand bewusst anders entscheidet.
- Der Auswertungs-Vergleichsbericht (M3, Explosiveness/Vergleich) filtert die Männer-Spiele
  jetzt explizit heraus, bevor irgendetwas berechnet wird — nicht erst nachträglich.

Für dich heißt das: nichts an den bisherigen Frauen-Auswertungen ändert sich, aber die
Männer-Daten waren zwischenzeitlich unbemerkt mit eingeflossen und sind jetzt sauber
getrennt.

Alle Zahlen, Commits und der volle technische Nachtrag stehen in
`.planning/phases/01.2-repo-to-pipeline/01.2-IFAF-FULL-SUMMARY.md` und
`docs/ifaf-field-mapping.md`.
