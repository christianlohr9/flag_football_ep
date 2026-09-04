# Explosiveness-Entscheidung: Kandidat B angenommen

Stand: 2026-09-04. Entscheidungsprotokoll zu HC-04 (Phase M3-3) -- kurz und eigenständig, damit
es unabhängig von `docs/explosiveness-vorschlag.md` und `docs/explosiveness-recherche.md`
gelesen werden kann und dem Head Coach mitgeteilt werden kann, welche Überlegungen zu dieser
Kennzahl geführt haben.

Keine Spielernamen in diesem Dokument (öffentlich, git-versioniert). Der Head Coach wird hier
durchgängig als "HC"/"Head Coach" bezeichnet.

## Entscheidung

Am 2026-09-04 wurde entschieden: **Kandidat B -- EPA-Magnitude auf erfolgreichen Plays
(IsoPPP-Stil) -- wird unsere Explosiveness-Kennzahl.** Ein Play gilt als "explosive", wenn er
erfolgreich war (`epa > 0`) UND seine `epa` über einem aus unserem eigenen Korpus abgeleiteten
Schwellenwert liegt (aktuell die 80. Perzentile der EPA aller erfolgreichen Plays; siehe
`data/reference/explosiveness/calibration.json` für den jeweils aktuellen Zahlenwert).
Efficiency bleibt separat definiert als Success Rate (`epa > 0`) -- die beiden Kennzahlen werden
nicht mehr in einer einzigen "Explosive %"-Zahl vermischt.

## Welche Varianten geprüft wurden

| Variante | Beschreibung | Status |
|---|---|---|
| HC-Workbook-Formel | `Yards > 12`, nur Pass-Attempts (Zelle `Player Analysis All Camps!R2:S2`) | reproduziert als Baseline, nicht übernommen |
| HC-mündliche Regel | `Yards > 12` ODER `epa > 0`, nur Pass-Attempts | reproduziert als Baseline, nicht übernommen |
| Kandidat A -- down-konditionierte Perzentile | "explosive" = oberste X % der Yards-Verteilung *für dieses Down* | verworfen |
| **Kandidat B -- EPA-Magnitude auf erfolgreichen Plays** | `epa > 0` UND `epa` über der 80. Perzentile der erfolgreichen Plays | **angenommen** |
| Kandidat C -- stetiger Score | logistischer Score in [0,1] statt einer 0/1-Flagge | als Zusatzansicht behalten, nicht als Kopfzahl |

Vollständige Herleitung und die Literaturquellen (Connelly/Football Study Hall, PFF, Sam Hoppen,
nflverse) stehen in `docs/explosiveness-recherche.md`; die auf unserem Korpus gemessenen Zahlen
zu jeder Variante stehen in `docs/explosiveness-vorschlag.md`.

## Warum Kandidat B

- **Analytisch, nicht mündlich:** löst die Vermengung von "war der Play gut genug?" (Efficiency)
  und "wie groß war er, gegeben dass er gut war?" (Explosiveness) auf, die die Literatur
  (Connelly) als Kernfehler einer einzelnen "Explosive %"-Zahl benennt. Auf unserem Korpus zeigt
  sich das konkret: die mündliche HC-Regel liegt fast exakt bei der reinen Success Rate --
  praktisch dieselbe Zahl unter anderem Namen, nicht ein Maß für große Plays.
- **Reproduzierbar:** der Schwellenwert wird nicht geschätzt oder aus der NFL-Literatur
  übernommen, sondern direkt aus `data/processed/plays_scored.parquet` berechnet und zusammen
  mit dem Korpus, aus dem er stammt, versioniert gespeichert (siehe "Wie der Schwellenwert
  versioniert wird" unten). Jeder kann den Wert nachrechnen, niemand muss ihn glauben.
- **Kalibriert pro Korpus:** die 80. Perzentile ist relativ zu unserer eigenen
  Yards-/EPA-Verteilung (5v5, 50-Yard-Feld, 4-Down-Serien), nicht ein NFL-Cutoff (20+ Yards Pass,
  10+ Yards Lauf), der für unser Spielformat nicht direkt übertragbar ist.
- **Vergleichbar über Situationen hinweg:** `epa` ist bereits kontextsensitiv -- Down, Distance,
  Feldposition und (soweit modelliert) die Spielsituation fließen über das EP-Modell ein, ohne
  dass wir eigene Down-Bucket-Regeln pflegen müssten. Ein Play mit 11 Yards und einer mit 12
  Yards in vergleichbarer Situation haben fast dieselbe `epa` und werden deshalb fast gleich
  eingeordnet -- die Klippe bei 12 Yards, die den HC ursprünglich gestört hat
  ("was ist, wenn eine Spielerin nur 11 Yards erzielt?"), verschwindet strukturell, nicht nur
  kosmetisch.

## Was verworfen wurde und warum

- **Die HC-Workbook-Formel selbst (`Yards > 12`) als künftige Kennzahl:** erzeugt genau die
  harte Kante, die der Anlass für diese ganze Recherche war -- 10-12 % aller Plays liegen so nah
  am Cutoff, dass ein einziger Yard die Einordnung kippt. Bleibt als reproduzierte Baseline im
  Vorschlag sichtbar (Transparenz, keine Löschung), wird aber nicht die künftige Kennzahl.
- **Die mündliche "Yards oder EPA"-Regel als künftige Kennzahl:** auf unserem Korpus praktisch
  identisch mit der reinen Success Rate (nur eine verschwindend kleine Zahl an Plays wird
  ausschließlich durch die Yards-Klausel "explosive", ohne dass `epa` positiv ist) -- sie misst
  in der Praxis Efficiency, nicht Größe. Als eigenständige Explosiveness-Kennzahl irreführend.
- **Kandidat A (down-konditionierte Perzentile):** löst den globalen Cliff auf, ersetzt ihn aber
  nur durch einen down-spezifischen Cliff -- weiterhin binär, weiterhin eine harte Kante, nur an
  anderer Stelle. Zusätzlicher Pflegeaufwand für Down-Buckets ohne den strukturellen Vorteil von
  Kandidat B.
- **Kandidat C (stetiger Score) als Kopfzahl:** löst den Cliff am saubersten, aber Coaches
  brauchen weiterhin eine einzelne griffige Zahl im Report. Kandidat C bleibt als unterstützende,
  zusätzliche Ansicht (Verteilungs-Chart) erhalten, ersetzt aber nicht die Kopfzahl.

## Wie der Schwellenwert neu berechnet und versioniert wird

Der Schwellenwert lebt in `data/reference/explosiveness/calibration.json`, mit folgenden
Feldern: `epa_quantile` (aktuell 0,80), `epa_threshold` (der abgeleitete EPA-Wert),
`epa_median_success`/`epa_iqr_success` (Kontext zur Verteilung), `corpus_n`/`n_success` (Umfang
des kalibrierten Korpus), `corpus_sources` (welche Datenquellen eingeflossen sind),
`corpus_fingerprint` (Hash über den kalibrierten Korpus -- ändert sich, sobald sich die
zugrunde liegenden Plays ändern) und `calibrated_on` (Zeitstempel der Berechnung).

Eine Neuberechnung geschieht nie automatisch und nie still: `scripts/explosiveness_comparison.py`
lädt ohne `--recalibrate`-Flag den bestehenden, committeten Schwellenwert und rechnet ihn nicht
neu; nur ein expliziter `--recalibrate`-Lauf leitet einen frischen Wert aus dem aktuellen Korpus
ab. Jede Neuberechnung wird als dessen eigener, datierter Nachtrag in
`docs/explosiveness-vorschlag.md` und `docs/explosiveness-recherche.md` festgehalten, mit
Vorher/Nachher-Zahlen -- nie eine stillschweigende Änderung an einer bereits veröffentlichten
Kennzahl.

## Weiterführende Dokumente

- `docs/explosiveness-recherche.md` -- die vollständige Literaturrecherche und alle geprüften
  Formelzellen.
- `docs/explosiveness-vorschlag.md` -- der Vorschlag an den Head Coach mit allen gemessenen
  Zahlen auf unserem Korpus.
- `data/reference/explosiveness/calibration.json` -- der aktuell gültige, versionierte
  Schwellenwert.
