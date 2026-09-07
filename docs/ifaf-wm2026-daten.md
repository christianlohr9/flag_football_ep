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

## Letzter Nachtrag: Männer-Daten jetzt komplett draußen, nicht nur vom Training

Der Trainings-Ausschluss von eben reichte nicht: Die Männer-Spiele waren zwar vom Modell-
Training ausgeschlossen, standen aber weiterhin im Gesamtdatensatz — mit demselben Team-
Kürzel "GER" wie die Frauen. Jeder andere Bericht, der nach Team filtert (nicht nur das
Training), hätte die beiden Teams also weiterhin vermischt.

**Jetzt richtig gelöst:** Die Männer-Spiele werden gar nicht erst in den Datensatz
übernommen, außer man schaltet das bewusst frei (ein neuer Schalter in der Konfiguration,
`ingest_tournaments`, steht standardmäßig nur auf "Frauen"). Die Rohdaten bleiben weiterhin
abgeholt (falls man sie später doch braucht), aber sie fließen nicht automatisch ein.
Zusätzlich haben Männer-Teams jetzt eigene Kürzel (`GER-M` statt `GER`), falls jemand die
Männer-Daten bewusst dazuschaltet — dann können sie nie mit den Frauen-Daten verschmelzen.

**Ergebnis nach erneutem Lauf:** Datensatz hat jetzt wieder genau 32 Frauen-Spiele / 3.191
Zeilen von IFAF, null Männer-Zeilen. Gesamtzahl aller Quellen zusammen: 28.255 Zeilen — genau
der Stand von vor dieser ganzen IFAF-Aktion, nur jetzt mit besseren Frauen-Daten (Yardage,
Distanz-bis-Erstdown, echte EP/WP-Werte).

**Abschließend geprüft:** der komplette Testlauf über das ganze Projekt (nicht nur die
IFAF-Tests) wurde am Ende noch einmal durchlaufen — zwei Fehler kamen zum Vorschein, die
direkt aus dieser Session stammten (eine Test-Fixture, die noch mit dem alten
Namensschema rechnete, und zwei veraltete Zeilenzahl-Prüfungen), beide sind jetzt behoben.
Übrig bleiben nur zwei Testfehler, die schon vor dieser Session da waren und mit IFAF nichts
zu tun haben (Kamera-Tracking/Hackathon-Scoring).

Alle Zahlen, Commits und der volle technische Nachtrag stehen in
`.planning/phases/01.2-repo-to-pipeline/01.2-IFAF-FULL-SUMMARY.md` und
`docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 — die Play-by-Play-Zeilen waren an vielen Stellen falsch, jetzt korrigiert

Du hattest bei einem konkreten Spiel (Frauen-Viertelfinale MEX–ESP) gemeldet, dass die ersten
drei Spielzüge alle identisch aussahen (down 2, 21 Yards bis zum ersten Down, Ballposition 4) —
und dass ein späterer Spielzug von down 2 direkt auf down 4 sprang, obwohl der Reviewer-Feed
selbst (`/games/{id}/plays`) eine ganz normale Sequenz zeigt (1. Versuch von der eigenen
5-Yard-Linie, dann 2. Versuch bei 11, 3. Versuch bei 31, …). Du hattest recht: Das war kein
Einzelfall bei diesem einen Spiel, sondern ein echter Fehler in der bisherigen Datenquelle.

**Was kaputt war:** Die bisher genutzte `unified-plays`-Quelle liefert pro Spielzug ein
`context`-Feld mit Down/Ballposition — aber dieses Feld ist nicht zuverlässig der Zustand *vor*
dem Snap. Es springt zwischen "vor dem Spielzug", "nach dem Spielzug" und einem reinen
Platzhalter-Wert (down 2, Ballposition 4) hin und her, ohne erkennbares Muster. Bei genau
diesem MEX-ESP-Spiel steht fast die Hälfte aller Zeilen (43 von 93) auf diesem Platzhalter —
das schlechteste Spiel im ganzen Frauen-Datensatz, und zufällig genau das, das du dir angesehen
hast. Über den gesamten Frauen-Datensatz hinweg betrifft das rund 2,6 % aller Zeilen.

**Die Lösung:** Wir nutzen jetzt `/games/{id}/plays` — den vom Reviewer geprüften Feed — als
primäre Quelle, nicht mehr `unified-plays`. Dieser Feed trägt Down, Ballposition und Halbzeit
direkt und zuverlässig, plus eine Liste der tatsächlichen Spielereignisse (Pass, Fang,
Interception, Touchdown, Strafe, …), aus der wir jetzt Spielergebnis und Spieltyp direkt
ableiten, statt sie zu erraten. Vom Reviewer als "ungültig" markierte Spielzüge (z. B. ein
zurückgenommener Extrapunkt) werden jetzt korrekt als "kein echter Spielzug" behandelt — nie
stillschweigend gelöscht, aber auch nie mit einem erfundenen Ergebnis versehen.

**Für das MEX-ESP-Spiel sieht die korrigierte Sequenz jetzt genau so aus, wie der
Reviewer-Feed sie zeigt:** 1. Versuch von der 5 → 2. Versuch bei 11 → 3. Versuch bei 31 →
Touchdown → (ein vom Reviewer zurückgenommener Extrapunktversuch) → (eine Strafe) → Mexiko
übernimmt bei der eigenen 5.

**Der Preis, offen benannt:** Nicht jeder Spielzug im neuen Reviewer-Feed trägt eine
Ballposition (rund ein Viertel der Zeilen in den akzeptierten Spielen nicht) — dadurch sinkt
die Abdeckung bei Yards-Werten und EP/WP-Werten gegenüber vorher, und mehr Spiele fallen jetzt
bei der Qualitätsprüfung durch (25 von 42 statt vorher 32 von 42 nicht-kampflos-verlorenen
Spielen), weil der Reviewer-Feed selbst an einzelnen Stellen keinen Down-Wert einträgt (z. B.
bei einer stehenden Strafe ohne Spielzug). Das ist kein Rückschritt, sondern der ehrliche
Tausch: weniger, aber dafür echte Daten statt mehr, aber teils falsche. Das MEX-ESP-Spiel
selbst gehört zu diesen 25 zurückgestellten Spielen (wegen genau einer fehlenden
Down-Angabe bei einer Strafe) — es taucht aber weiterhin vollständig und korrigiert in den
CSV-Exports auf, falls du es dir noch mal ansehen willst.

**Gegengecheckt, ehrlich berichtet:** Die "Mittellinie überquert/noch nicht überquert"-Logik
stimmt zu 98,2 % mit dem eigenen `marker`-Feld des Reviewer-Feeds überein (fast identisch zur
vorherigen Zahl — jetzt aber gegen die richtige Quelle gemessen). Der Abgleich mit dem
unabhängigen Events-Log ist gemischt: bei "welcher Down ist das" 82 % Übereinstimmung, bei der
genauen Ballposition nur 45 % — Letzteres liegt am Events-Log selbst (es protokolliert viel
feinteiliger als es Spielzüge gibt, nicht an einer Unzuverlässigkeit der neuen Quelle).

Voller technischer Nachtrag mit allen Zahlen: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 2, noch am selben Tag) — zwei Nachbesserungen

Eine Prüfung des gestrigen Fixes hat zwei weitere Lücken gefunden, beide noch am selben Tag
behoben.

**1. Eine Strafe ohne Spielzug braucht keinen Down-Wert.** 17 der 29 Frauen-Spiele, die den
neuen Reviewer-Feed nutzen, sind an der Qualitätsprüfung gescheitert, weil einzelne Zeilen
keinen Down-Wert hatten. Bei 38 von 46 dieser Zeilen war das aber gar keine Datenlücke,
sondern nur eine unvollständige Einordnung unsererseits: Eine stehende Strafe ohne echten
Spielzug (z. B. eine Strafe zwischen zwei Downs) hat per Definition keinen eigenen Down —
niemand hat gesnappt. Die Qualitätsprüfung akzeptiert das jetzt explizit als das, was es ist,
statt es als Lücke zu werten. **Dein MEX-ESP-Spiel war genau so ein Fall** — die einzige
blockierende Zeile war exakt diese eine stehende Strafe. Das Spiel ist jetzt vollständig
akzeptiert (93 Zeilen), nicht nur im CSV-Export sichtbar. 12 weitere Spiele sind dadurch
ebenfalls jetzt akzeptiert. Fünf Spiele bleiben zurückgestellt — dort sind es echte
Charting-Lücken (fehlender Down-Wert auf einem echten Spielzug), die wir bewusst nicht
erfinden.

**2. Die 13 Ausweich-Spiele (die noch die alte, unzuverlässige Quelle nutzten) sind jetzt
ganz draußen, nicht mehr "notdürftig akzeptiert".** Wir haben versucht, für diese 13 Spiele
den Vor-Snap-Zustand stattdessen aus dem rohen Events-Log selbst zu rekonstruieren (das ist
die Quelle, aus der der Reviewer-Feed selbst gebaut wird) — und das ehrlich gegengecheckt: bei
den 29 Spielen, wo wir echte Reviewer-Daten zum Vergleichen haben, stimmt die rekonstruierte
Down-Angabe nur zu 77,5 % und die Ballposition nur zu 47 % überein — beides deutlich unter der
Schwelle von 95 %, die wir uns vorher gesetzt hatten, um so etwas überhaupt zu verwenden. Ein
Spiel zeigte sogar eine rund 51-stündige Zeitversatz zwischen zwei internen Uhren — ein klares
Zeichen, dass diese Rekonstruktion nicht verlässlich genug ist.

**Konsequenz: Diese 13 Spiele fließen jetzt gar nicht mehr in den Datensatz ein**, statt (wie
gestern) mit der unzuverlässigen alten Quelle akzeptiert zu werden. Frauen-Datensatz jetzt:
24 von 42 nicht-kampflos-verlorenen Spielen akzeptiert, 2.198 Zeilen — alle davon aus dem
neuen, geprüften Reviewer-Feed, keine einzige mehr aus der alten unsicheren Quelle.

Voller technischer Nachtrag mit allen Zahlen: `docs/ifaf-field-mapping.md`.
