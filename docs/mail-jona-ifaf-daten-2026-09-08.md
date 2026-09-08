# Entwurf: Nachricht an den HC zu den WM-Daten (2026-09-08)

Kontext: Play-by-Play der IFAF World Flag 2026 (Frauen) aus der cpx.studio-API, Stand 2026-09-08.
Anhang für den HC: Abschnitt „Anhang“ unten (Liste der betroffenen Spiele, ohne Personenbezug).

---

Hey Jona,

kurzes Update zu den WM-Daten. Ich habe die Play-by-Play-Daten stichprobenartig Zeile für Zeile
geprüft, und das sieht grundsätzlich gut aus: Downs, Ballpositionen, Ergebnisse, Passerin/Fängerin
und die Spielstände stimmen dort, wo die IFAF-Statistiker ihre Arbeit zu Ende gebracht haben. Genau
da liegt aber das Problem: Bei einem Teil der Spiele haben sie mittendrin oder ganz aufgehört. Ich
habe dir das unten aufgeschlüsselt, damit du es nachvollziehen kannst.

Was ich herausgefunden habe:

1. Die Daten entstehen in zwei Schritten: Live-Erfassung am Spielfeld (Down, Spot, Score) und
   danach ein Review-Durchgang, in dem die Ballpositionen und Spielzüge bestätigt werden. Nur der
   Review-Durchgang liefert verlässliche Feldpositionen. Bei 13 Spielen wurde dieser Durchgang gar
   nicht gemacht, bei weiteren Spielen nur teilweise. Beim Viertelfinale ESP–MEX hört der Review
   nach 22 Spielzügen einfach auf; ab da fehlt jede Ballposition. Das Spiel trage ich gerade selbst
   am Video nach.
2. In 9 Spielen fehlen im Review einzelne Touchdown-Spielzüge samt Try komplett, obwohl die
   Statistiker die Punkte live korrekt eingetragen haben (das Live-Ereignisprotokoll der IFAF-App
   kennt Team, Art und Punkte). Das passiert auch in Spielen, die ansonsten vollständig reviewt
   sind, es wurde also schlicht ein Spielzug ausgelassen. Die Punkte habe ich aus dem
   Live-Protokoll ergänzt (als solche markiert), damit der Spielstand stimmt; der Spielzug selbst
   bleibt ohne Details.
3. Bei einem Spiel widersprechen sich Live-Protokoll und offizieller Endstand um 12 Punkte. Das
   Spiel lasse ich draußen, bis das geklärt ist.
4. Die API liefert keine Yards. Raumgewinn und Distanz zum nächsten First Down leite ich aus den
   Ballpositionen und den Regeln (Mittellinie, dann Endzone) ab; das stimmt in 98 % der prüfbaren
   Fälle mit den Markern der Statistiker überein.
5. Der Anbieter hat zwischen Mitte August und Anfang September bei 11 Spielen nachträglich
   Spielzüge entfernt. Für diese Spiele nutze ich den älteren, vollständigeren Stand.

Was das für dich heißt: Für gut 20 der 48 Frauen-Spiele haben wir vollständige, belastbare
Daten inklusive EPA. Die restlichen Spiele sind entweder unvollständig (Positionen fehlen) oder
ganz ohne Review. Ich bin dran, das Viertelfinale und ein weiteres Spiel per Video zu
vervollständigen; für sechs Spiele haben wir nicht einmal die Video-Marken.

Falls du einen Draht zur IFAF-Statistik hast: Die Fragen, die wir stellen müssten, sind (a) ob der
Review-Durchgang für die 13 offenen Spiele noch nachgeholt wird, (b) warum bei einigen Spielen der
Review mittendrin abbricht, und (c) ob es für die Ballpositionen eine andere Quelle gibt. Wenn du
willst, formuliere ich das als kurze Mail vor.

Viele Grüße
Christian

---

## Anhang: betroffene Spiele (Frauen, Stand 2026-09-08)

Kennungen sind die Spiel-IDs der IFAF-API; Teamkürzel aus `games.json`.

**A. Review-Durchgang nie gemacht (13 Spiele, keine verlässlichen Ballpositionen):**
Spiele, deren `/plays`-Datensatz leer ist (`reconciliation.reason = no-tries-labelled`). Liste in
`docs/ifaf-wm2026-daten.md`, Teil 3.

**B. Review-Durchgang abgebrochen oder unvollständig (Frauen, mit Anzahl fehlender Positionen):**

| Spiel | Plays ohne Position | Review endet bei Sequenz | Video-Marken vorhanden |
|---|---:|---:|---|
| `019ffff1-a8db…` (VF ESP–MEX) | 71 von 93 | 220 (nach 22 Plays) | ja |
| `ffwc26-wd5` | 69 | – | ja |
| `019ffff1-add2…` | 4 | – | ja |
| `ffwc26-wa5`, `ffwc26-wb6` | je 1 | – | ja |
| `019ffff1-a998…` | 85 | nie begonnen | nein |
| `01a00140-b679…` | 92 | nie begonnen | nein |
| `01a00140-b68c…` | 107 | nie begonnen | nein |
| `01a0062b-6706…` | 2 | – | nein |
| `01a0062b-6782…` | 47 | ab Sequenz 310 | nein |
| `ffwc26-wc6` | 88 | – | nein |
| `ffwc26-wd6` | 96 | – | nein |

**C. Fehlende Touchdown-Spielzüge (Punkte aus dem Live-Protokoll ergänzt):** 9 Spiele, 8 TD- und
19 Try-Ergänzungen; Liste in `docs/ifaf-field-mapping.md` (Nachtrag 2026-09-07, Ledger).

**D. Widerspruch Live-Protokoll vs. Endstand:** `ffwc26-wd4` (25–37 vs. 13–37), ausgeschlossen.

**E. Nachträglich veränderte Daten des Anbieters:** 11 Spiele, älterer Stand vom 17.08. genutzt
(`data/raw/ifaf/snapshot_manifest.json`).
