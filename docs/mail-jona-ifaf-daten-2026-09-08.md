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
   nicht gemacht, darunter zwei von uns: GER–PAN (15.08.) und AUT–GER (16.08.). Bei weiteren
   Spielen ist er unvollständig. Beim Viertelfinale MEX–ESP hört der Review nach 22 Spielzügen
   einfach auf; ab da fehlt jede Ballposition. Das Spiel trage ich gerade selbst am Video nach.
2. In 9 Spielen fehlen im Review einzelne Touchdown-Spielzüge samt Try komplett, obwohl die
   Statistiker die Punkte live korrekt eingetragen haben (das Live-Ereignisprotokoll der IFAF-App
   kennt Team, Art und Punkte). Das passiert auch in Spielen, die ansonsten vollständig reviewt
   sind, es wurde also schlicht ein Spielzug ausgelassen, unter anderem bei USA–GER (15.08.) und
   ITA–GER (13.08.). Die Punkte habe ich aus dem Live-Protokoll ergänzt (als solche markiert),
   damit der Spielstand stimmt; der Spielzug selbst bleibt ohne Details.
3. Bei Japan gegen Panama (13.08., Vorrunde) widersprechen sich Live-Protokoll und offizieller
   Endstand: Das Live-Protokoll ergibt 25–37, offiziell steht es 13–37. Zwei Touchdowns für Japan
   wurden also live gebucht, zählen offiziell aber nicht. Das Spiel lasse ich draußen, bis das
   geklärt ist.
4. Die API liefert keine Yards. Raumgewinn und Distanz zum nächsten First Down leite ich aus den
   Ballpositionen und den Regeln (Mittellinie, dann Endzone) ab; das stimmt in 98 % der prüfbaren
   Fälle mit den Markern der Statistiker überein.
5. Der Anbieter hat zwischen Mitte August und Anfang September bei 11 Spielen nachträglich
   Spielzüge entfernt. Für diese Spiele nutze ich den älteren, vollständigeren Stand.

Was das für dich heißt: Für gut 20 der 48 Frauen-Spiele haben wir vollständige, belastbare
Daten inklusive EPA. Die restlichen Spiele sind entweder unvollständig (Positionen fehlen) oder
ganz ohne Review. Ich bin dran, das Viertelfinale und ein weiteres Spiel per Video zu
vervollständigen; für sieben Spiele haben wir nicht einmal die Video-Marken.

Falls du einen Draht zur IFAF-Statistik hast: Die Fragen, die wir stellen müssten, sind (a) ob der
Review-Durchgang für die 13 offenen Spiele noch nachgeholt wird, (b) warum bei einigen Spielen der
Review mittendrin abbricht, und (c) ob es für die Ballpositionen eine andere Quelle gibt. Wenn du
willst, formuliere ich das als kurze Mail vor.

Viele Grüße
Christian

---

## Anhang: betroffene Spiele (Frauen, Stand 2026-09-08)

Teamkürzel und Datum aus der IFAF-API; Heimteam zuerst.

**A. Review-Durchgang nie gemacht (13 Spiele, keine verlässlichen Ballpositionen):**
MEX–ITA (14.08.), FRA–CHN (14.08.), AUS–SLO (14.08.), CHN–BRA (14.08.), ITA–JPN (15.08.),
MEX–CAN (15.08.), ESP–AUT (15.08.), **GER–PAN (15.08.)**, FRA–SLO (15.08.), JPN–CHN (16.08.),
ITA–AUS (16.08.), **AUT–GER (16.08.)**, ESP–PAN (16.08.).

**B. Review-Durchgang abgebrochen oder unvollständig (mit Anzahl fehlender Ballpositionen):**

| Spiel | Datum | Plays ohne Position | Review | Video-Marken |
|---|---|---:|---|---|
| MEX–ESP (Viertelfinale) | 15.08. | 71 von 93 | endet nach 22 Plays | ja |
| PAN–BRA | 14.08. | 69 | unvollständig | ja |
| JPN–FRA | 14.08. | 4 | fast vollständig | ja |
| GER–SLO | 14.08. | 1 | fast vollständig | ja |
| USA–ESP | 14.08. | 1 | fast vollständig | ja |
| CAN–AUT | 15.08. | 85 | nie begonnen | nein |
| AUS–CHN | 15.08. | 92 | nie begonnen | nein |
| SLO–BRA | 15.08. | 107 | nie begonnen | nein |
| CAN–USA | 16.08. | 2 | fast vollständig | nein |
| MEX–GBR | 16.08. | 47 | bricht mittendrin ab | nein |
| GBR–AUT | 14.08. | 88 | unvollständig | nein |
| CAN–JPN | 14.08. | 96 | unvollständig | nein |

**C. Fehlende Touchdown-Spielzüge (Punkte aus dem Live-Protokoll ergänzt), im akzeptierten
Korpus:** USA–GER (15.08.), SLO–MEX (13.08.), ITA–GER (13.08.), GER–SLO (14.08.), USA–ESP (14.08.),
AUT–CHN (13.08., drei Spielzüge); weitere in quarantänierten Spielen. Details in
`docs/ifaf-field-mapping.md` (Nachtrag 2026-09-07, Ledger).

**D. Widerspruch Live-Protokoll vs. Endstand:** JPN–PAN (13.08.): Live-Protokoll 25–37,
offiziell 13–37; ausgeschlossen.

**E. Nachträglich veränderte Daten des Anbieters:** 11 Spiele, älterer Stand vom 17.08. genutzt
(`data/raw/ifaf/snapshot_manifest.json`).
