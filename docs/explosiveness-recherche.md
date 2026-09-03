# Rechercheergebnisse: Explosiveness & Efficiency

Stand: 2026-09-03. Recherche zu Phase M3-3 (HC-04). Frage des Head Coaches: "Wie definieren
andere (NFL, PFF, College) 'explosive plays', und wie adaptieren wir das für unser 5v5 Flag
Football?" Anlass: der HC's aktuelle Regel ("mehr als 12 Yards und/oder positive EPA = explosive")
erzeugt eine harte Kante -- "was ist, wenn eine Spielerin nur 11 Yards erzielt?"

Keine Spielernamen in diesem Dokument (öffentlich, git-versioniert).

## Kernbefund: Zwei Vokabulare, eine Dichotomie

Die (US-)Football-Analytics-Literatur trennt seit Jahren zwei Fragen, die der HC's einzelne
"Explosive %"-Kennzahl vermischt:

1. **Efficiency / Success Rate** -- "Wie zuverlässig ist die Offense?" Ein Play ist ein Erfolg
   oder kein Erfolg (binär), meist kontextabhängig (Down & Distance).
2. **Explosiveness / IsoPPP** -- "Wenn ein Play erfolgreich war, wie wertvoll war er dann?" Nur
   über die *erfolgreichen* Plays gemittelt, nicht über alle.

Bill Connelly (Football Study Hall / ESPN, Begründer von S&P+) hat diese Trennung explizit
gemacht: "Success Rate is defined as gaining 50% of necessary yardage on first down, 70% on
second down and 100% on third and fourth down. IsoPPP looks at only the per-play value of a
team's successful plays." Seine spätere Erkenntnis, nach mehreren Saisons Beobachtung: "efficiency
is everything ... explosiveness is too random to rely on without efficiency" -- IsoPPP ist eine
"dramatically unstable measure", die von Saison zu Saison stark zum Mittelwert zurückkehrt,
während Success Rate stabil bleibt. In seinem S&P+-Modell fließt Success Rate mit 86% Gewicht ein,
IsoPPP nur mit 14% ([Football Study Hall](https://www.footballstudyhall.com/2014/1/27/5349762/five-factors-college-football-efficiency-explosiveness-isoppp)).

**Für uns heißt das:** Die HC-Frage "was ist Explosiveness" ist eigentlich zwei Fragen. Der
Efficiency-Teil ("wie oft ist ein Play gut genug") sollte stabil und robust definiert sein
(Success Rate/EPA>0-Stil); der Explosiveness-Teil ("wie groß ist der gute Play") ist naturgemäß
volatiler und sollte nie ohne Stichprobengröße kommuniziert werden.

## Wie die NFL "explosive plays" definiert

- **Ursprung:** Der Begriff geht auf einen Artikel des früheren Ravens-Head-Coaches Brian Billick
  (2011) zurück, der jeden Scrimmage-Play (Lauf oder Pass) von 20+ Yards als "explosive" definierte.
- **Die heute verbreitetste Konvention** ist gesplittet nach Play-Typ: Pass 20+ Yards, Lauf 10+
  Yards (teils auch 12+ Yards) -- begründet damit, dass Läufe im Schnitt deutlich kürzer sind als
  Pässe, ein einziger Flat-Cutoff also Läufe systematisch benachteiligt
  ([Sharp Football Analysis](https://www.sharpfootballanalysis.com/nfl-stats/offense/explosive-plays/team-rankings-explosive-plays-2021/), [nflanalytic.com](https://nflanalytic.com/explainer-explosive-plays.html)).
- **Es gibt keine offizielle, league-weite Definition.** Verschiedene Analysten nutzen 20+ pauschal,
  10/15-Split, 12/16-Split, oder verzichten ganz auf eine harte Grenze zugunsten einer
  kontinuierlichen EPA-Skala.
- **PFF** selbst (in ihrer eigenen Data-Study zu explosiven Plays) verwendet 20+ Yards als
  illustrativen, nicht "magischen" Schwellenwert ("20 yards is no magic number") und stellt
  explizit heraus, dass Drives mit einem explosiven Play im Schnitt fast 4x so viele Expected
  Points erzeugen wie Drives ohne (≈2.2 vs. ≈8 EP/Drive) -- das eigentliche Argument für
  "explosive Plays zählen" ist also EPA-Impact, nicht der Yard-Wert selbst
  ([PFF](https://www.pff.com/news/nfl-explosive-plays-and-re-thinking-offensive-success)).

## Die Kritik an harten Yard-Schwellen (genau der HC-Einwand)

Diese Kritik ist in der Literatur bereits ausformuliert, nicht nur unsere eigene Intuition:

- **Der "Cliff-Effekt":** Ein Cutoff bei genau 19 vs. 20 Yards (oder 11 vs. 12) erzeugt eine
  künstliche Klippe, obwohl der tatsächliche Wert eines Plays für den Spielausgang ein Kontinuum
  ist, kein Sprung. "The central issue is that using a hard cutoff ... creates an artificial
  'cliff effect' where plays are categorized differently based on arbitrary yard markers, when in
  reality the value of plays likely falls on a continuum."
- **Sam Hoppen** (NFL-Analytics-Autor) hat genau diese Kritik an seiner eigenen früheren
  15-Yard-Schwelle formuliert: "there's a bit of a flaw in wanting to measure something
  yards-based when we do have EPA available" -- weil Yards und EPA stark korreliert, aber nicht
  identisch sind (ein 15-Yard-Gain bei 4th & 20 ist kein Erfolg; ein 8-Yard-Gain bei 3rd & 7 in
  der Red Zone kann wertvoller sein als viele "explosive" 20-Yard-Gains).
  Er testet drei Alternativen und landet am Ende trotzdem wieder bei einem (datengetriebenen)
  Play-Type-Split (25+ Pass / 10+ Lauf, aus einer Scoring-Probability-Regressions-Bruchstelle
  hergeleitet) -- der Autor selbst schreibt, dass jeder Cutoff Interpretierbarkeit gegen Präzision
  eintauscht ([Sam Hoppen](https://samhoppen.substack.com/p/how-should-we-define-an-explosive)).

## nflfastR / nflverse: Success Rate als EPA > 0

Im nflverse-Ökosystem (nflfastR) ist "Success" durchgängig als `EPA > 0` definiert: ein Play ist
erfolgreich, wenn er das Team (laut EP-Modell) besser dastehen lässt als vorher. Das ist bereits
kontextsensitiv (Down, Distance, Feldposition, Uhr fließen alle ins EP-Modell ein) -- ohne dass
man selbst Down/Distance-Buckets bauen muss. Für "explosive" gibt es dort keine standardisierte
Definition; verbreitet ist z.B. `EPA > 1.0` als Zusatzkriterium (entspricht in etwa der 80.
Perzentile) ([nflanalytic.com EPA vs. DVOA vs. Success Rate](https://nflanalytic.com/explainer-epa-vs-dvoa.html)).

**Das ist die direkte Blaupause für unsere Efficiency-Seite:** unser bestehendes `epa`-Feld
(`features/mutations.py::add_ep_variables`) ist bereits exakt das nflverse-EP-Modell-Pendant --
wir müssen für "Success Rate" nichts Neues bauen, nur `epa > 0` auswerten.

## Die HC's eigenen Formeln (aus dem Workbook, `data_only=False`)

Quelle: `data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx`, Tab
`Player Analysis All Camps` (Formelzellen), referenziert auf Tab `Data`.

**Exp Plays / Explosive %** (Zeile 2, Spalten R/S, für jede QB-Zeile):

```
ExpPlays  = COUNTIFS(Data!P, <QB>, Data!J, ">12")   ' J = "GN/LS" (Yards Gained/Lost)
Explosive% = ExpPlays / Attempts                     ' Attempts = Comps+Incs+Sacks (nur Pass!)
```

Wichtiger Befund: **die Formel prüft ausschließlich `Yards > 12` -- keine EPA-Bedingung.** Die
verbale Beschreibung des HC ("12 Yards und/oder positive EPA") ist in der Tabelle nicht
implementiert; sie existiert nur mündlich/gedanklich. Das ist eine offene Frage an den HC (siehe
`docs/research-notes.md`-Pendant für M3-3), nicht etwas, das wir stillschweigend gleichsetzen
dürfen. Zusätzlich ist "Attempts" hier nur die Pass-Attempts eines QB -- Läufe fließen in diese
Kennzahl gar nicht ein.

**Efficiency** (Spalte U):

```
Efficiency = SUMIF(Data!P, <QB>, Data!O) / (Attempts + Drops)
```

Spalte `O` im `Data`-Tab heißt selbst "Efficiency" und enthält pro Play einen Wert 0/1 (vereinzelt
Ausreißer wie `9`, dazu `#N/A`-Formelreste). Wir haben versucht, diese Spalte aus Down/Distance/
Yards-Gained zu rekonstruieren (Conversion-Regel, hälftige College-Success-Rate-Regel, reines
"Yards > 0") -- keine dieser drei Kandidaten-Formeln erklärt die Spalte besser als 80%
Übereinstimmung. Das deutet darauf hin, dass Spalte `O` **manuell während des Charting vergeben**
wird (z.B. inklusive Ballwurf-Qualität, Drop-Zurechnung o.ä.), nicht rein aus Down/Distance/Yards
berechenbar ist. **Das ist eine offene Frage, die nur der HC selbst beantworten kann** -- wir
reproduzieren daher zunächst die Formel wörtlich (Summe der gecharteten O-Werte geteilt durch
Attempts+Drops), ohne die Bedeutung von Spalte O selbst zu erklären.

## Unsere eigene Yards-Verteilung (2023-2026-Korpus)

Basis: `data/processed/plays_scored.parquet`, Scrimmage-Plays (`play_type` in `{run, pass}`,
`down` 1-4, `yards_gained` nicht null). **n = 15.006 Plays.** Keine Spielernamen, nur
Aggregatzahlen.

| Kennzahl | Wert |
|---|---|
| Median Yards Gained | 6 |
| 75. Perzentile | 10 |
| 80. Perzentile | 11 |
| 90. Perzentile | 16 |
| 95. Perzentile | 23 |
| Anteil Plays > 12 Yards | 16.7% (2.504/15.006) |
| Plays in der "Klippen-Zone" 10-12 Yards | 11.5% (1.727/15.006) |

Die Klippen-Zone (10-12 Yards, also unmittelbar um den HC-Cutoff) ist mit 11,5% aller Plays keine
Randerscheinung -- mehr als jeder neunte Play liegt so nah am Cutoff, dass 1 Yard mehr oder
weniger das Etikett "explosive" kippt. Das bestätigt den Einwand des Nutzers quantitativ: der
Cutoff bei 12 trifft nicht selten, sondern trifft mitten in eine dichte Zone der Verteilung.

**EPA-Seite** (n = 14.669 Plays mit EPA, aus demselben Scrimmage-Subset):

| Kennzahl | Wert |
|---|---|
| Success Rate (EPA > 0) | 52.2% |
| HC-Regel wörtlich (Yards>12 ODER EPA>0) | 52.8% |
| ... davon nur durch "Yards>12" ausgelöst (EPA ≤ 0) | 89 Plays (0,6% aller EPA-Plays) |
| Ø EPA auf erfolgreichen Plays (IsoPPP-Analog) | +1,60 |
| Median EPA auf erfolgreichen Plays | +1,14 |

**Der zentrale Befund:** Die "und/oder"-Verknüpfung in der mündlichen HC-Regel wird fast
vollständig vom EPA-Teil dominiert -- 52,2% aller Plays haben positive EPA, aber nur 16,7% haben
mehr als 12 Yards. Wenn man beide mit "oder" verknüpft (52,8%), tragen nur 89 zusätzliche Plays
(0,6%) etwas bei, die nicht schon über positive EPA erfasst wären. **Die verbal beschriebene
"Explosive"-Regel des HC ist also faktisch fast identisch mit seiner Success Rate, nicht mit einer
Beschreibung "großer" Plays** -- genau die Efficiency/Explosiveness-Verwechslung, die die
Literatur (Connelly) als Kernfehler benennt. Das ist ein starkes Argument dafür, die beiden
Konzepte im Vorschlag klar zu trennen: eine Success-Rate-Kennzahl (Efficiency) und eine separate,
kleinere Explosiveness-Kennzahl über nur die erfolgreichen Plays.

## Vorschlags-Kandidaten (threshold-frei bzw. kalibriert)

1. **Kandidat A -- Down-konditionierte Perzentile:** "explosive" = oberste X% (z.B. 20%) der
   Yards-Verteilung *für dieses Down*, nicht global. Vermeidet den globalen Cliff, ersetzt ihn aber
   durch einen down-spezifischen Cliff -- kommunizierbar ("Play, der besser war als 4 von 5
   vergleichbaren Downs"), aber immer noch binär.
2. **Kandidat B -- EPA-Magnitude auf erfolgreichen Plays (IsoPPP-Stil):** "explosive" = Play mit
   `epa > 0` UND `epa` über der empirischen 80. Perzentile der erfolgreichen Plays (bei uns ≈ +2,3
   EPA). Vorteil: automatisch kontextsensitiv (Down, Distance, Feldposition, Uhr stecken schon im
   EP-Modell), keine manuelle Down-Bucket-Pflege nötig. Direkt an nflverse/PFF anschlussfähig.
3. **Kandidat C -- Kontinuierlicher/weicher Score:** statt einer 0/1-Flagge ein Score in [0,1]
   (z.B. eine logistische Funktion der EPA- oder Yards-Verteilung), der die 11-vs-12-Klippe
   auflöst, weil es keine Klippe mehr gibt -- ein Play mit 11 Yards bekommt einen leicht
   niedrigeren Score als einer mit 12, nicht ein hartes "nein" gegen ein hartes "ja". Für Coaches
   weiterhin auf eine griffige Kopfzahl (z.B. "Explosive %" = Anteil Plays mit Score > 0,5)
   herunterbrechbar, aber die zugrunde liegende Verteilung bleibt sichtbar und der Cliff-Einwand
   des Nutzers ist strukturell gelöst, nicht nur kosmetisch verschoben.

**Empfehlung:** Kandidat B als Hauptkennzahl (EPA-Magnitude auf erfolgreichen Plays, IsoPPP-Stil),
weil er (a) die Efficiency/Explosiveness-Trennung der Literatur direkt umsetzt, (b) ohne neue
Down-Bucket-Logik auskommt (das EP-Modell übernimmt das), und (c) sich 1:1 an die nflverse-Success-
Rate-Konvention anschließt, die wir für Efficiency ohnehin reproduzieren. Kandidat C als
Zusatzdarstellung (Verteilungs-Chart), nicht als Ersatz-Kopfzahl -- Coaches brauchen weiterhin eine
einzelne Zahl im Report.

## Kleine Stichproben

Wenige Länderspiele/Jahr bedeuten kleine Pro-Spieler-Stichproben. Der Standardansatz in der
Sport-Analytics-Literatur für dieses Problem ist Empirical-Bayes-/Beta-Binomial-Shrinkage: rohe
Raten werden zu einem datengetriebenen Gesamtmittel hin "geschrumpft", stärker bei kleiner
Stichprobe, schwächer bei großer -- "observed rates are unbiased but highly unreliable, while
Bayes estimates are biased but have lower mean squared error" ([Übersicht bei kiwidamien.github.io](https://kiwidamien.github.io/shrinkage-and-empirical-bayes-to-improve-inference.html)).
Unser Code hat mit `MUTED_MIN_N` (`reports/aggregate.py`) und Clopper-Pearson-Konfidenzintervallen
bereits eine "nie verstecken, aber ehrlich markieren"-Konvention etabliert -- diese für
Explosiveness/Efficiency weiterzuverwenden statt eine neue Konvention zu erfinden, ist konsistent
mit dem bestehenden Code.

## Offene Fragen an den HC

1. Die Workbook-Formel für "Explosive %" prüft nur Yards > 12, keine EPA -- ist die
   "und/oder EPA"-Beschreibung ein älterer Gedankenstand, oder soll das nachträglich in die Tabelle?
2. Was genau bedeutet die manuell gecharteten Werte in `Data!Efficiency` (Spalte O)? Wir konnten
   keine reine Down/Distance/Yards-Formel finden, die die Spalte zu >80% erklärt.
3. Soll "Explosive %" laufbezogene Plays einschließen (aktuell nur QB-Pass-Attempts im Nenner)?

## Quellen

- [Football Study Hall: Five Factors -- Efficiency, Explosiveness, IsoPPP](https://www.footballstudyhall.com/2014/1/27/5349762/five-factors-college-football-efficiency-explosiveness-isoppp)
- [Football Study Hall: Big plays are the 3-pointers of football](https://www.footballstudyhall.com/2017/8/22/16075050/college-football-big-plays-efficiency-five-factors)
- [PFF Data Study: Explosive plays and re-thinking offensive success](https://www.pff.com/news/nfl-explosive-plays-and-re-thinking-offensive-success)
- [Sam Hoppen: How should we define an explosive play?](https://samhoppen.substack.com/p/how-should-we-define-an-explosive)
- [Sharp Football Analysis: 2021 NFL Team Ranks -- Explosive Plays](https://www.sharpfootballanalysis.com/nfl-stats/offense/explosive-plays/team-rankings-explosive-plays-2021/)
- [nflanalytic.com: Explosive Plays -- Why Big Gains Matter](https://nflanalytic.com/explainer-explosive-plays.html)
- [nflanalytic.com: EPA vs. DVOA vs. Success Rate](https://nflanalytic.com/explainer-epa-vs-dvoa.html)
- [kiwidamien.github.io: Shrinkage and Empirical Bayes to improve inference](https://kiwidamien.github.io/shrinkage-and-empirical-bayes-to-improve-inference.html)
- `data/raw/hc_files/Offense Analytics 2026 Camps and Competitions.xlsx` (gitignored, PII) --
  Formelzellen `Player Analysis All Camps` R2/S2/U2, `Data`-Tab Spalte O
- `data/processed/plays_scored.parquet` -- eigene Verteilungszahlen (siehe Tabellen oben)
