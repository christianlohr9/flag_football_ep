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

## Nachtrag 2026-09-07 (Teil 3, noch am selben Tag) — ein gezielter Reparaturversuch, ehrlich gescheitert

In 8 der 24 akzeptierten Frauen-Spiele fehlt bei rund 576 echten Spielzügen die Ballposition,
obwohl der Down-Wert da ist (dein MEX-ESP-Spiel gehört dazu — allerdings bei den *späteren*
Spielzügen, nicht den ersten drei, die du geprüft hattest). Das drückt die EP/WP-Abdeckung
weiterhin auf rund 69 %.

Wir haben versucht, die fehlende Ballposition aus dem rohen Events-Log zu ergänzen — diesmal
nicht über Zeitstempel (die hatten sich als unzuverlässig erwiesen), sondern über die
*Reihenfolge* innerhalb einer Ballbesitz-Serie: der x-te Down-Schritt im Reviewer-Feed sollte
zum x-ten Down-Schritt im Events-Log passen. Ehrlich gegengecheckt an den 16 bereits
vollständigen Spielen (deren echte Ballposition wir kennen): Die einfache Version stimmt nur
zu 59 % überein, eine verfeinerte Version (die auch Neustarts der Down-Zählung nach
Mittellinien-Überquerung berücksichtigt) sogar nur zu 31 % — schlechter, nicht besser. Beides
liegt weit unter der Schwelle von 95 %, die wir uns vorher gesetzt hatten.

**Konsequenz: Kein Reparaturversuch wird übernommen.** Die 576 Zeilen bleiben ehrlich ohne
Ballposition, statt sie zu erfinden. Die EP/WP-Zahlen ändern sich dadurch nicht. Auffällig:
Genau diese 8 Spiele haben durchgehend den Down-Wert, aber nur einen Bruchteil der
Ballpositionen — das sieht nach einer unvollständigen Einspielung im Reviewer-Werkzeug für
genau diese 8 Spiele aus, nicht nach einem Zufallsmuster. Das wäre eine gute Frage an IFAF/
cpx.studio direkt.

Voller technischer Nachtrag mit allen Zahlen: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 4, noch am selben Tag) — der Punktestand war falsch, nicht nur die Vor-Snap-Daten

Du hattest gemeldet: beim Viertelfinale MEX–ESP (`ifaf-019ffff1-a8db-73ed-91ff-068fd964194c`)
zeigt unser rekonstruierter Endstand 36:30, offiziell war es 27:26. Du hattest wieder recht,
und diesmal war es kein Vor-Snap-Problem, sondern ein Fehler beim Punktezählen selbst.

**Was kaputt war:** Wir haben den Punktestand bisher aus den Spielaktionen abgeleitet — eine
`TOUCHDOWN`-Aktion galt automatisch als 6 Punkte, eine `TRY`-Aktion als 1 oder 2 (je nachdem,
was das Try-Event selbst sagte). Das ist falsch: Der Reviewer-Feed trägt pro Spielzug ein
eigenes Feld `officialScore` (`TD`/`XP1`/`XP2`/`NONE`/leer) — das eigentliche, geprüfte Urteil
des Reviewers, unabhängig von der Aktionsliste. Ein konkretes Beispiel aus genau diesem Spiel:
Spielzug 21 zeigt die Aktionen "Pass, Fang, Touchdown" — sieht nach 6 Punkten aus, ist aber
laut `officialScore` ein `XP1`, also der 1-Punkt-Extrapunktversuch nach dem Touchdown davor.
Wir haben also 6 statt 1 Punkt gebucht. Corpusweit betrifft das Hunderte Zeilen — 41-mal ein
fälschlich als Touchdown gezählter 1-Punkt-Versuch, 46-mal ein fälschlich als Touchdown
gezählter 2-Punkt-Versuch, dazu 21 Fälle, wo ein Extrapunktversuch selbst fälschlich ein
"TD"-Etikett trägt (siehe unten).

**Die Lösung:** Der Punktestand kommt jetzt ausschließlich aus `officialScore`, nie mehr aus
den Aktionsnamen. `TD` → Touchdown (6, oder 6 für die Abwehr bei einer Interception-Rückgabe),
`XP1`/`XP2` → 1/2 Punkte für den Extrapunktversuch, `NONE` oder leer → kein Punkt. Eine
Besonderheit: 21 Extrapunktversuch-Zeilen im gesamten Corpus tragen selbst ein `officialScore`
von "TD" — das kann nie stimmen (ein Extrapunktversuch ist nie 6 Punkte wert). Wir haben
geprüft: in den meisten Fällen ist das ein verrutschtes Etikett — die vorherige Touchdown-
Zeile trägt fälschlich `NONE`, und die echten 6 Punkte gehören dorthin, nicht auf den
Extrapunktversuch. Genau das ist bei diesem Spiel selbst passiert (siehe unten). In den
übrigen Fällen war die vorherige Touchdown-Zeile bereits korrekt mit `TD` markiert — dann ist
das "TD" auf dem Extrapunktversuch ein Duplikat ohne eigene Bedeutung. So oder so: die
Punkte des Extrapunktversuchs selbst kommen dann aus dem Try-Event (erfolgreich/1 Punkt,
erfolgreich/2 Punkte, oder gescheitert), nie aus dem geliehenen "TD"-Etikett.

**Für das MEX-ESP-Spiel im Detail:** Der rekonstruierte Endstand ist jetzt **26:25** (Heim
Mexiko, Auswärts Spanien) statt vorher 36:30 — viel näher am offiziellen 27:26, aber nicht
exakt. Wir haben die verbleibende Lücke von je 1 Punkt pro Team bis auf den Spielzug genau
verfolgt, und beide sind echte Datenlücken im Reviewer-Feed selbst, keine Rechenfehler mehr:
Spaniens erster Touchdown (Spielzug 4) hatte einen erfolgreichen Extrapunktversuch, der aber
von einer Strafe gegen die eigene Offense zurückgenommen wurde (`officialScore: NONE`,
`nullified: true`) — das ist ein echtes 0, keine Lücke. Mexikos Touchdown mitten im ersten
Viertel (Sequenznummer 310) hat dagegen **gar keinen** zugehörigen Extrapunktversuch im Feed —
die Zeilen direkt danach springen unerwartet zurück auf einen zweiten Versuch für Mexiko in
Folge (der bereits dokumentierte "1, 2, 3, 2"-Fehler im Down-Zähler dieses Spiels, siehe
Nachtrag Teil 1). Wir erfinden diesen fehlenden Punkt nicht.

**Zusätzlich, auf deine Rückfrage hin (mit deiner Flag-Football-Expertise geprüft):**
- Vier Zeilen im Corpus tragen `officialScore: XP2`, obwohl weder eine `TRY`- noch eine
  `TOUCHDOWN`-Aktion vorliegt — alle vier sind Safeties (`SAFETY`-Aktion, Ballposition 5). Die
  App kodiert eine Safety offenbar als "XP2"; wir buchen sie weiterhin korrekt als Safety
  (2 Punkte für die Abwehr), nie als Extrapunkt für die Offense.
- Eine Rückgabe eines Extrapunktversuchs durch die Abwehr (Interception/Flag-Pull-Rückgabe)
  wäre nach IFAF-Regeln 2 Punkte für die Abwehr wert — im aktuellen Corpus kommt diese
  Kombination (Try-Aktion + Interception/Flag-Pull + `officialScore: XP2`) kein einziges Mal
  vor, die Regel bleibt also dokumentiert, aber ungetestet an echten Daten.

**Der Punktestand wird jetzt für jedes IFAF-Spiel gegen den offiziellen Endstand aus
`games.json` geprüft** (vorher hatten wir dafür gar keine Referenz für diese Quelle — die
Qualitätsprüfung hat jedes IFAF-Spiel stillschweigend übersprungen). Ergebnis über den ganzen
Frauen-Datensatz (29 Spiele mit Daten aus dem Reviewer-Feed): **9 Spiele haben jetzt exakt den
richtigen Endstand**, 20 nicht. 16 davon waren vorher (nur nach den anderen Prüfungen)
akzeptiert und fliegen jetzt neu raus, weil der Punktestand nicht stimmt — das ist die
richtige, ehrliche Konsequenz einer Prüfung, die vorher gar nicht real lief. Manche
Abweichungen sind klein (1-2 Punkte, vermutlich derselbe Fehlertyp wie beim MEX-ESP-Spiel),
andere sind groß (bis zu 13 Punkte) und brauchen eine eigene Untersuchung — noch offen.

**Eine offene Frage bleibt bewusst an dich zurückgegeben:** Wie eine Abwehr-Rückgabe eines
Extrapunktversuchs zu werten wäre (2 Punkte laut IFAF-Regelwerk) lässt sich an den aktuellen
Daten nicht testen, weil kein einziger solcher Fall vorkommt — die Regel ist implementiert,
aber nicht durch echte Daten bestätigt.

Voller technischer Nachtrag mit allen Zahlen (inkl. der vollständigen Liste aller 20
verbleibenden Punktestand-Abweichungen): `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 5, noch am selben Tag) — ein zweites, unabhängiges Punktestand-Protokoll bestätigt die genaue Lücke, wird aber bewusst nicht in den Datensatz geschrieben

Der Events-Feed hat noch ein drittes Punktestand-Signal, unabhängig von `officialScore` und
den Spielaktionen: explizite "SCORE"-Ereignisse mit Team, Punktart (TD/XP1/XP2) und
Punktzahl. Über den ganzen Frauen-Datensatz gegengecheckt: **41 von 48 Spielen** stimmen mit
dem offiziellen Endstand exakt überein, 6 Ausnahmen sind Nichtantritte (keine Ereignisse), und
bei genau einem Spiel (`ffwc26-wd4`) widerspricht das Protokoll selbst dem offiziellen
Ergebnis — dieses eine Spiel wird als unzuverlässig markiert, nicht von Hand korrigiert.

**Für das MEX-ESP-Spiel bestätigt dieses Protokoll exakt 27:26** — und korrigiert dabei eine
kleine Ungenauigkeit aus Teil 4 oben: Die fehlenden Punkte sitzen nicht dort, wo ich zuerst
vermutet hatte (Spaniens erster Touchdown / Mexikos dritter Touchdown), sondern bei Mexikos
*erstem* Touchdown und Spaniens *drittem* Touchdown — beide Male fehlt im Reviewer-Feed der
komplette Extrapunktversuch, obwohl das unabhängige Ereignis-Protokoll bestätigt, dass er
stattfand und erfolgreich war. Die Gesamtaussage bleibt: **26:25 rekonstruiert, zwei echte,
im Reviewer-Feed fehlende Datensätze, kein Rechenfehler** — nur die genaue Zuordnung, welche
zwei Touchdowns betroffen sind, war vorher ungenau.

**Bewusste Entscheidung: Wir schreiben diese beiden fehlenden Punkte NICHT als erfundene
Zeilen in den Datensatz**, auch wenn das Protokoll sie für dieses eine Spiel eindeutig
bestätigt. Grund: Das Protokoll selbst stimmt nur bei 41 von 48 Spielen (85%) — deutlich unter
der 95%-Schwelle, die wir uns für jede rekonstruierte Quelle in dieser Session gesetzt haben
(und mit der wir bereits zwei andere Rekonstruktionsversuche verworfen haben, siehe Teil 2 und
Teil 3 oben). Für ein einzelnes Spiel mag das Protokoll stimmen — aber eine allgemeine Regel
("füge ein, was das Protokoll zeigt, aber der Reviewer-Feed nicht") müsste über den ganzen
Datensatz laufen, nicht nur beim MEX-ESP-Spiel, und würde damit auch die 15%-Fehlerquote mit
einschleppen. Eine erfundene Zeile ist außerdem etwas grundsätzlich anderes als ein leerer
Wert: Sie hätte keine echte Ballposition, keinen Video-Zeitstempel, keine echte Reviewer-
Prüfung — und nichts würde sie später von einem echten, geprüften Spielzug unterscheiden, wenn
sie einmal im Datensatz steht. Stattdessen gibt es jetzt für jedes Spiel eine reine
Diagnose-Zeile im Bericht ("Protokoll bestätigt/widerspricht den offiziellen Endstand") — hilft
beim Verstehen, verändert aber nie, welche Zeilen tatsächlich in den Datensatz kommen.

**Zusätzlich umgesetzt, auf ausdrücklichen Wunsch:** Ein durch eine nachfolgende Strafe
zurückgenommener Spielzug behält jetzt seine eigene Spielform — ein zurückgenommener
Extrapunktversuch (wie Spaniens erster Versuch im MEX-ESP-Spiel) heißt jetzt korrekt
"Extrapunkt", nicht mehr "kein Spielzug". Der Punktestand ändert sich dadurch nicht (der war
schon vorher korrekt bei 0), nur die Klassifizierung wird ehrlicher.

Voller technischer Nachtrag mit allen Zahlen: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 6, noch am selben Tag) — auf deine ausdrückliche Entscheidung hin: fehlende Extrapunkte werden jetzt ergänzt

Du hast dir Teil 5 direkt angesehen und einen Rechenfehler in meiner eigenen 95%-Schwelle
gefunden: 6 der 7 "Fehltreffer" beim Punktestand-Protokoll sind Nichtantritte ganz ohne
Protokoll-Daten — die gehören nicht in den Nenner, genau wie unsere eigene Punktestand-Prüfung
selbst zwischen "übersprungen" (keine Referenz) und "durchgefallen" unterscheidet. Auf die 42
Spiele mit echten Protokolldaten gerechnet, stimmt das Protokoll bei **41 von 42 (97,6%)** —
über der Schwelle. Auf dieser Basis hast du ausdrücklich entschieden: fehlende Extrapunkte, die
das Protokoll eindeutig bestätigt, werden jetzt als eigene (klar markierte) Zeile ergänzt, statt
nur als Kommentar im Bericht zu stehen.

**Umgesetzt:** Für jedes Spiel, dessen Protokoll-Gesamtsumme exakt zum offiziellen Endstand
passt, kommt der Punktestand jetzt ausschließlich aus dem Protokoll (nicht mehr aus
`officialScore`). Fehlt für einen bestätigten Extrapunkt die passende Zeile im Reviewer-Feed
komplett, wird sie direkt nach dem zugehörigen Touchdown ergänzt — als klar markierte
"synthetische" Zeile (eigene Spalte `score_source`, Wert `"events-ledger-synthetic"`; echte,
im Protokoll bestätigte Zeilen bekommen `"events-ledger"`). Ein fehlender ganzer Touchdown wird
weiterhin **nicht** erfunden — nur der Extrapunktversuch danach, und nur, wenn das Protokoll ihn
eindeutig bestätigt.

**Für dein MEX-ESP-Spiel: jetzt exakt 27:26.** Beide fehlenden Extrapunkte (nach Mexikos erstem
und Spaniens drittem Touchdown) sind jetzt als synthetische Zeilen ergänzt.

**Zwei echte Programmierfehler beim Abgleich gefunden und behoben** (nicht einfach ungetestet
übernommen): Erstens konnte eine fehlende PAT-Zeile fälschlich die PAT-Zeile eines *späteren*
Touchdowns "stehlen", wenn die Suche nicht an der nächsten eigenen Touchdown-Zeile derselben
Mannschaft gestoppt wurde — genau das war beim MEX-ESP-Spiel selbst der Fall. Zweitens konnte
ein gemeinsamer Fortschritts-Zeiger über beide Mannschaften hinweg eine echte, frühere
Kandidaten-Zeile der einen Mannschaft überspringen, wenn die Reihenfolge zwischen Protokoll und
Reviewer-Feed nicht 1:1 zusammenpasste (in einem echten Spiel im Datensatz beobachtet) — jetzt
zählt jede Mannschaft ihren eigenen Fortschritt getrennt. Ein verwandter, bereits vorher
bestehender Fehler in der alten (nur-`officialScore`)-Ergänzungslogik aus Teil 4 wurde beim
Testen ebenfalls gefunden und genauso behoben.

**Ergebnis über den ganzen Frauen-Datensatz:** Die Punktestand-Prüfung geht von 9 auf **18 von
29** korrekt bestätigten Spielen (die übrigen 11, inklusive deines schon bekannten `wd4`, haben
alle einen genau benannten, echten Grund — meist ein *komplett* fehlender Touchdown-Datensatz,
nicht nur ein fehlender Extrapunkt; einmal ein sichtbar fehlerhaftes Protokoll für ein Team;
Details mit Ursache pro Spiel im technischen Nachtrag).

**Wichtig geprüft, nicht nur behauptet:** Die neuen synthetischen Zeilen fließen nie ins
Modelltraining ein (EP und WP) — das war schon vorher durch bestehende Mechanismen so (leere
Spalten führen automatisch zum Ausschluss), aber ich habe das jetzt mit einem echten Test
bewiesen, nicht nur angenommen.

Auch die offene Frage von Teil 4 (die 21 Fälle, bei denen ein Extrapunktversuch selbst fälschlich
ein "TD"-Etikett trägt) ist jetzt mit echten Protokolldaten beantwortet: 16 von 21 bestätigen den
vorherigen Touchdown als den echten (4 davon zusätzlich mit einem echten Extrapunkt danach), 4
bleiben auch mit Protokolldaten ungeklärt (bewusst nicht erfunden), 1 (`wd4`) hat gar kein
verlässliches Protokoll. Volle Tabelle mit jedem Einzelfall: `docs/ifaf-field-mapping.md`.

Voller technischer Nachtrag mit allen Zahlen: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 7, noch am selben Tag) — abgelehnt: einen ganz fehlenden Touchdown als erfundene Zeile ergänzen

> **Am selben Tag noch einmal vorgelegt und diesmal anders entschieden (Teil 8, unten).** Du
> hast dieselbe Abwägung noch einmal direkt gesehen und dich für die korrekte Punktestand-Fortführung
> entschieden, nicht für das Zurückhalten der Zeile — mit derselben Kennzeichnung (eigene Spalte,
> vom Training ausgeschlossen), die unten schon beschrieben ist. Die Begründung hier bleibt
> unverändert stehen, sie war zum Zeitpunkt der Entscheidung richtig und wird nicht zurückgenommen,
> nur durch deine eigene Entscheidung überstimmt. Für das tatsächliche Verhalten gilt Teil 8.

Die nächste Anfrage kam mit derselben Begründung wie Teil 6 (eine ausdrückliche
Nutzer-Entscheidung, Abwägung gezeigt): Für die Spiele, bei denen das Protokoll einen Touchdown
zeigt, der im Reviewer-Feed gar nicht existiert, sollte jetzt auch eine erfundene Touchdown-Zeile
eingefügt werden — mit leerem Spielfeld-Wert für so gut wie jedes echte Feld (kein Down, keine
Ballposition, keine echte Spielaktion), an einer nur ungefähr geschätzten Stelle im Spiel.

**Diese Anfrage wird abgelehnt.** Der Unterschied zu Teil 6 ist real, nicht nur graduell: Ein
fehlender Extrapunkt hängt direkt an einem echten, geprüften Touchdown — nur die eine folgende
Zeile fehlt, und wo sie hingehört, ist eindeutig. Eine "erfundene Touchdown-Zeile" hätte dagegen
so gut wie keinen echten Inhalt außer der Punktzahl und dem Team — nicht mal die genaue Stelle
im Spiel ist bekannt (die eigene Platzierungsregel der Anfrage gibt das selbst zu: "wenn die
genaue Stelle unklar ist"). Das wäre keine erfundene Zeile, die eine Lücke schließt, sondern eine
Korrektur des Punktestands, als Spielzug verkleidet — und würde unsere eigene Punktestand-Prüfung
für genau die Spiele bedeutungslos machen, bei denen sie am wichtigsten wäre: Sie würde dann nicht
mehr echt nachrechnen, sondern nur noch bestätigen, was wir selbst vorher eingefügt haben.

**Umgesetzt stattdessen:** Ein zusammenfassender Hinweis pro Spiel, der ehrlich sagt, wie viele
Punkte fehlen und warum ("N Touchdown(s) und M Extrapunkt(e), vom Protokoll bestätigt, aber ohne
jede Zeile im Reviewer-Feed — nicht erfunden"). Geprüft am echten Datensatz: **10 von 29
Frauen-Spielen** betroffen — 9 davon mit mindestens einem wirklich fehlenden Touchdown-Datensatz,
1 Spiel (`01a00140-b679`, das schon aus Teil 6 bekannte Spiel mit dem sichtbar fehlerhaften
Protokoll) ohne fehlenden Touchdown, aber mit 8 nicht zuordenbaren Extrapunkt-Ereignissen.

Eine spielweite Markierung ("dieses Spiel hat eine unvollständige Datengrundlage") wurde geprüft,
aber nicht umgesetzt — dafür müsste die Spiele-Tabelle (`games.parquet`), die von allen fünf
Datenquellen gemeinsam genutzt wird, erweitert werden, was deutlich mehr Code an mehr Stellen
verändern würde als für diese eine Quelle nötig ist. Genau für diesen Fall war in der Anfrage
selbst schon "sonst nur ein Hinweis" als Rückfalloption genannt — das wird umgesetzt.

**Der Punktestand-Abgleich bleibt bei 18 von 29 bestätigten Spielen**, nicht bei den
angepeilten 28. Das ist keine unerledigte Aufgabe, sondern die ehrliche Konsequenz einer
bewussten Grenze: Die verbleibende Lücke bei diesen 10 Spielen ist eine echte, jetzt klar
benannte Lücke im Reviewer-Feed selbst, kein Programmfehler — und sie so zu schließen, würde
entweder Zeilen erfinden (abgelehnt) oder eine Nachlieferung durch IFAF/cpx.studio selbst
brauchen, die Frage gehört an den Anbieter, nicht in diese Ingest-Schicht.

Voller technischer Nachtrag mit allen Zahlen: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 8, noch am selben Tag) — deine Entscheidung: fehlende Touchdowns werden jetzt doch ergänzt

Du hast die Abwägung aus Teil 7 noch einmal direkt vorgelegt bekommen und dich diesmal anders
entschieden: eine korrekte Punktestand-Fortführung für alle späteren echten Spielzüge dieser 9
Spiele ist dir wichtiger als das Zurückhalten der Zeile — auch wenn sie so gut wie keinen echten
Spielinhalt hat (kein Down, keine Ballposition, keine Spielaktion). Die Zeile bleibt dabei auf
jeder Ebene klar erkennbar: eigene Spalte (`score_source = "events-ledger-synthetic"`), eine neue
spielweite Markierung (`plays_incomplete`, s.u.) und komplett ausgeschlossen vom Modelltraining.

**Umgesetzt:** Fehlt für einen vom Protokoll bestätigten Touchdown die passende Zeile im
Reviewer-Feed komplett, wird jetzt eine synthetische Touchdown-Zeile ergänzt — kein Down, keine
Ballposition, kein Spielzugtyp, aber `Touchdown = 1` und die richtige Mannschaft. Folgt im
Protokoll direkt danach auch noch ein fehlender Extrapunkt für dieselbe Mannschaft, wird der
genauso ergänzt wie schon in Teil 6. Die genaue Stelle im Spiel ist ehrlich gesagt nicht exakt
bekannt (dazu unten mehr) — sie wird so spät wie möglich eingefügt, aber immer noch vor dem
nächsten Spielzug, den das Protokoll bereits echt bestätigt hat.

**Zwei Spiele bleiben bewusst unangetastet:** `wd4` (dessen eigenes Protokoll dem offiziellen
Endstand schon widerspricht) und `01a00140-b679` (dessen Protokoll für ein Team sichtbar
fehlerhaft ist — hier fehlt gar kein ganzer Touchdown, nur mehrere nicht zuordenbare
Extrapunkt-Ereignisse ohne jeden Touchdown in der Nähe, die weiterhin nicht erfunden werden).

**Ergebnis über den ganzen Frauen-Datensatz:** Die Punktestand-Prüfung geht von 18 auf **26 von
29** korrekt bestätigten Spielen. Ein Spiel (`ffwc26-wc3`) bleibt trotz der neuen Zeile um genau
1 Punkt daneben — dort gibt es zusätzlich noch einen ganz anderen, schon aus Teil 5 bekannten
Protokollfehler (ein Extrapunkt-Ereignis ganz ohne zugehörigen Touchdown, an einer anderen Stelle
im Spiel), den diese Änderung bewusst nicht mit anpackt, weil er nichts mit dem hier behandelten
fehlenden Touchdown zu tun hat. `wd4` und `01a00140-b679` bleiben aus den schon genannten,
unveränderten Gründen ebenfalls daneben — macht 26 von 29, nicht 29 von 29.

**Wichtig, ehrlich eingeordnet:** Dass die Punktestand-Prüfung jetzt öfter bestätigt, bedeutet
nicht automatisch mehr Spiele im tatsächlich genutzten Datensatz — 8 dieser 9 Spiele bleiben aus
einem ganz anderen, schon vorher bekannten Grund (einzelne fehlende Down-Werte) draußen, den
diese Änderung nicht behebt. Was sich ändert, ist der korrekt fortgeführte Punktestand selbst,
nicht automatisch, welche Spiele am Ende im Trainingsdatensatz landen.

Voller technischer Nachtrag mit allen Zahlen: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 9, noch am selben Tag) — Korrektur: die neuen Zeilen waren der Grund, nicht nebensächlich

Die Aussage oben ("8 dieser 9 Spiele bleiben aus einem ganz anderen Grund draußen") war falsch: bei 6 der 9 Spiele war der fehlende Down-Wert genau die neu eingefügte synthetische Zeile selbst, kein separates Problem. Die Down-Prüfung akzeptiert jetzt zusätzlich synthetische Zeilen (`score_source = "events-ledger-synthetic"`) ohne Down-Wert — genau wie sie das schon für zurückgenommene Strafspielzüge tut. Ergebnis: **21 von 29 Spielen jetzt im Datensatz** (vorher 15), EPA-Abdeckung darauf **70,0%** (vorher 58,7%). Zwei Spiele (`wb4`, `wc1`) bleiben wegen je eines echten, unabhängigen fehlenden Down-Werts draußen; ein Spiel (`wc3`) bleibt aus dem schon in Teil 8 genannten, eigenen Punktestand-Grund draußen.

## Nachtrag 2026-09-07 (Teil 10, noch am selben Tag) — ein dritter Reparaturversuch für die fehlende Ballposition, wieder ehrlich gescheitert

Du hast das offene Problem aus Teil 3 noch einmal aufgegriffen, konkret an deinem eigenen
MEX-ESP-Viertelfinale (`ifaf-019ffff1-a8db-73ed-91ff-068fd964194c`): ab Spielzug 24 ist dort
alles Positionsbezogene leer, weil `ballOn` bei 71 von 93 Zeilen fehlt. Wir haben einen dritten,
methodisch anderen Versuch unternommen, diese Lücke aus dem Events-Log zu füllen: nicht mehr
über eine feste Position innerhalb der Ballbesitz-Serie (Teil 3), sondern über eine echte
Zustandsmaschine, die das Events-Log Schritt für Schritt nachspielt (wer hat den Ball, welcher
Down, welche Position) und das Ergebnis strukturell — über Mannschaft und Down, nicht über
Zeitstempel oder feste Position — mit den echten `/plays`-Zeilen abgleicht.

**Ehrlich gegengecheckt an 21 bereits vollständigen Spielen** (mehr als die ursprünglich
angenommenen 16 — die zusätzlichen 5 sind Spiele, die aus ganz anderen, hier nicht relevanten
Gründen ohnehin schon aussortiert sind, aber trotzdem eine vollständige eigene Ballposition
haben und sich deshalb genauso gut zur Kontrolle eignen): **16,8 % exakte Übereinstimmung**
(292 von 1.735 vergleichbaren Zeilen), der Down-Wert selbst nur 41,9 %. Das liegt nicht nur
unter der 95-%-Schwelle, sondern ist sogar schlechter als alle drei bisherigen Versuche
(59 %, 31 %, 46,8 %) — kein Fortschritt, ein weiterer, unabhängiger Beleg, dass sich das
Events-Log nicht zuverlässig auf die vom Reviewer geprüfte Zeilenstruktur abbilden lässt.

Der Grund, diesmal direkt an den echten Daten gefunden: Ein Spielzug ganz ohne Raumgewinn
(ein unvollständiger Pass, ein Sack am Anspiellinie) erzeugt im Events-Log gar kein eigenes
Positions-Update — erst der *übernächste* Positions-Eintrag wird dann versehentlich dem
falschen Down zugeordnet, und sobald das einmal passiert, verschiebt sich der Rest der ganzen
Ballbesitz-Serie mit. Ein vermuteter Seitenwechsel-Fehler in der Ballposition (manche Spiele
speichern sie spiegelverkehrt) wurde ebenfalls direkt geprüft und schließt sich als Erklärung
aus — selbst mit einer nachträglichen Korrektur in beide Richtungen bleibt die Trefferquote
exakt gleich.

**Konsequenz: Auch dieser dritte Versuch wird nicht übernommen.** Keine Ballposition wird
erfunden. Die Werkzeuge (Zustandsmaschine, Abgleich, Prüfschwelle) bleiben getestet im Code,
aber fest verdrahtet auf "nicht anwenden" — für den Fall, dass sich die Rohdaten irgendwann
ändern. Pro betroffenem Spiel gibt es jetzt eine genaue Diagnose (wie viele Zeilen fehlen, wie
viele Positions-Ereignisse das Events-Log für dieses Spiel überhaupt hat) statt eines weiteren
Rateversuchs.

Voller technischer Nachtrag mit allen Zahlen und der Spiel-für-Spiel-Tabelle: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-07 (Teil 11, noch am selben Tag) — deine Entscheidung: die fehlenden Ballpositionen werden von Hand nachgespottet

Nach drei unabhängigen, alle gescheiterten Rekonstruktionsversuchen (Teil 3, Teil 10 und den beiden
Varianten aus dem dritten technischen Nachtrag) hast du dich entschieden, die Lücke anders zu
schließen: Du spottest die fehlenden Ballpositionen der betroffenen Frauen-Spiele selbst nach, aus
dem Broadcast-Video, mit dem Video-Zeitstempel, den der Reviewer-Feed pro Spielzug ohnehin schon
mitliefert. Diese Ergänzung baut das Werkzeug dafür, damit du dabei wirklich nur eine Zahl pro
Spielzug eintippen musst.

**Programmatisch gefunden: 12 betroffene Frauen-Spiele** (mindestens ein echter Spielzug mit
fehlender `ballOn`) — das ist mehr als die 8 Spiele aus Teil 10, weil dort nur Spiele mit einer
größeren, zusammenhängenden Lücke gezählt wurden; die 4 zusätzlichen Spiele haben nur 1–4 fehlende
Zeilen. Bei 5 der 12 Spiele lässt sich für jede fehlende Zeile eine Video-URL auflösen (eigener
Video-Marker oder die im Spieldokument hinterlegte Aufnahme + der abgeleitete Zeitstempel der
Zeile selbst); bei den anderen 7 Spielen fehlt im Snapshot die Video-URL komplett — die
Arbeits-Übersicht bleibt für diese Spiele trotzdem nützlich (Down, Team, letzte bekannte Position),
nur eben ohne anklickbaren Video-Link.

**Was jetzt geht:**

1. **Arbeits-Übersicht pro Spiel** (`ffep ifaf-spot-fill-worksheets`, schreibt nach
   `data/raw/ifaf/spot_fill_worksheets/<game_id>.csv` — lokal, git-ignoriert, mit echten
   Spielernamen für den Kontext): eine Zeile pro fehlendem Spielzug, plus die jeweils
   letzte/nächste echte Zeile drumherum zur Orientierung. Spalten: Spielzug-Nummer, Down,
   Offense-Team, Passer/Receiver, letzte bekannte echte Position, Video-URL und -Zeitstempel.
   Erneutes Ausführen überschreibt nie einen bereits eingetragenen Wert.
2. **Die eigentliche Eintragung** passiert in `data/reference/ifaf_spot_fill/<game_id>.csv` —
   eine committete, PII-freie Datei pro Spiel (`game_id,sequence,ballOn,note`), aktuell leer bis
   auf die Kopfzeile. Die genaue Konvention (0–50 ab eigener Torlinie) und der Workflow stehen im
   `README.md` desselben Verzeichnisses.
3. **Beim nächsten Ingest-Lauf** liest `ingest.ifaf.apply_spot_fill` diese Datei automatisch mit
   ein und trägt den Wert in `yardline_50` ein — mit einer neuen Spalte `spot_source = "manual"`,
   damit für immer erkennbar bleibt, welche Position echt vom Reviewer stammt und welche von Hand
   nachgetragen wurde. Ein bereits echt gespotteter Wert wird dabei nie überschrieben (nur ein
   Hinweis im Log, kein Fehler), und `yards_gained`/`yards_to_go` behandeln eine nachgetragene
   Position exakt wie eine echte.

Voller technischer Nachtrag mit der Spiel-für-Spiel-Tabelle: `docs/ifaf-field-mapping.md`.

## Nachtrag 2026-09-08: Fehlende Ballpositionen = nicht reviewte Spielzüge

Befund: In jedem betroffenen Spiel fallen die Datensätze ohne `ballOn` **exakt** mit den
Datensätzen ohne `reviewedAt` zusammen (z. B. VF ESP–MEX: 62 von 82 echten Plays ohne Spot, alle
62 ohne Review, beide ab Sequenz 220; mehrere Spiele ohne jeden Review ab Sequenz 10). Die
Ballposition wird also erst im menschlichen Review-Durchgang der Reviewer-App gesetzt; wo der
Review abbricht, enden die Spots. Der Review lief für alle Spiele unter einem einzigen
Reviewer-Konto (Gmail-Plus-Alias einer Person, vermutlich der App-Entwickler bzw. ein
beauftragter Reviewer) mit menschlichem Tempo (Stunden pro Spiel), teils „marked“, teils
„derived“ Video-Zeiten; ein automatischer Abgleich (`reconciliation`: `no-tries-labelled`,
`score-mismatch`) markiert nur Probleme. Kein Hinweis auf automatische CV-Erfassung.

Konsequenz: Die Frage an den Anbieter lautet nicht „welcher Endpunkt“, sondern „wird der
Review-Durchgang für die offenen und abgebrochenen Spiele nachgeholt“. Bis dahin: manuelles
Nachtragen der Spots über die Video-Marken (Teil 11) für die Spiele mit Video-URL.
