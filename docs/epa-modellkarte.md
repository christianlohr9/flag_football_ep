# EPA/WP-Modellkarte

**Stand:** 2026-09-08. Eine Seite, für dich als Head Coach — keine Spielernamen, keine
Personendaten. Details, Herleitung und alle committeten Zahlen dahinter stehen in
`docs/epa-refinement-2026-10.md` (inkl. Nachtrag 2026-09-08).

## Wozu das Modell da ist

Jeder Spielzug bekommt zwei Zahlen: **EP** (Expected Points — wie viele Punkte bringt diese
Spielsituation im Erwartungswert bis zum nächsten Score) und **WP** (Win Probability — wie
wahrscheinlich ist der Sieg von hier aus). Die Differenz von EP vor und nach einem Spielzug ist
**EPA** (Expected Points Added) — die Kennzahl, mit der wir einzelne Spielzüge, Spielerinnen und
Spielsituationen vergleichbar machen, unabhängig vom Spielstand oder der Spielphase.

## Eingaben

Down, Distanz-zum-ersten-Down, Feldposition, Halbzeit, Spielstand-Differenz (nur WP), eine
Wettbewerbs-Tier-Kennung (`mixed-other` / `womens-international`) sowie — nur bei WP — eine
**synthetische** Spieluhr (gleichmäßig heruntergezählt, kein echter Spielzeit-Zeitstempel aus den
meisten Quellen). Kein Name, keine Position, keine Spielerin fließt als Eingabe ein — das Modell
kennt nur die Spielsituation, nie, wer sie ausgeführt hat.

## Trainingskorpus

| Quelle | Zeilen | Spiele | Tier |
|---|---:|---:|---|
| `legacy` (historisch) | 3.701 | 47 | mixed-other |
| `legacy-sportapp` (sportapp.fi) | 14.545 | 168 | mixed-other |
| Dein Kopftrainer-Workbook (2 nutzbare Tabs von 3) | 6.818 | 92 | mixed-other |
| IFAF World Flag 2026 (Frauen-WM, reviewt) | 1.951 | 21 | womens-international |
| **Gesamt** | **27.015** | **328** | — |

Zeitraum: mehrere Saisons, 2023 bis 2026 (Jahreszahl je nach Quelle, siehe
`docs/epa-refinement-2026-10.md`). Ausgeschlossen vom Training, mit Absicht: alle synthetischen
IFAF-Zeilen (`score_source = "events-ledger-synthetic"`, vom Protokoll bestätigte, aber im
Reviewer-Feed fehlende Zeilen — 27 Stück, geprüft nie im Training), sowie jede Zeile, deren
nächster echter Score in der Halbzeit ein *erfolgreicher* Extrapunkt-/Zwei-Punkt-Versuch ist.

## Methode

**Leave-one-game-out (LOGO):** jedes Spiel genau einmal Testspiel, nie gleichzeitig Trainings-
und Testdaten — kein Blick in die Zukunft, keine Selbstbestätigung. Kalibrierung wird
mitgemessen (Reliability-Kurven, Log-Loss gegen eine einfache Grundrate). Kein Modell wird
automatisch "Champion" — das ist immer eine bewusste, von uns gemeinsam geprüfte Entscheidung
(`docs/model-training.md` Abschnitt 3).

## Performance gegen die einfache Grundrate

Gemessen am 2026-09-08 auf dem vollständigen Korpus oben (LOGO, Arm "mit Kopftrainer-Daten"):

| Modell | Log-Loss | Grundrate | Verbesserung |
|---|---:|---:|---:|
| EP | 0,942659 | 0,994269 | 0,051610 |
| WP | 0,372350 | 0,691566 | 0,319215 |

Beide Modelle schlagen die einfache Grundrate klar — EP zum ersten Mal in der Geschichte dieses
Projekts (früher lag EP knapp *hinter* der Grundrate). Pro Quelle: IFAF schlägt seine eigene
Grundrate bei EP solide (0,881 gegen 0,914), bei WP nur ganz knapp (0,689 gegen 0,693) — die
synthetische Spieluhr ist vermutlich der Grund, aber das ist eine Vermutung, kein Beweis. Deine
Workbook-Daten sind bei beiden Modellen die stärkste oder zweitstärkste Einzelquelle im ganzen
Korpus.

## Bekannte Grenzen

- **Keine echte Spieluhr** in den meisten Quellen — WP nutzt eine gleichmäßig heruntergezählte,
  synthetische Uhr. IFAF liefert zwar einen echten Zeitstempel, wird dafür aber (noch) nicht
  genutzt.
- **IFAF-Lücken bleiben real:** 21 von 42 nicht-kampflos-verlorenen Frauen-Spielen sind aktuell
  nutzbar — der Rest hat einen unvollständigen oder nie durchgeführten Review-Durchgang beim
  Anbieter (Details: `docs/ifaf-wm2026-daten.md`).
- **Synthetische Zeilen fließen nie ins Training** — geprüft, nicht nur behauptet.
- **Ein Rand-Verhalten, seit Phase 1.3 unverändert:** gescheiterte (nicht erfolgreiche)
  Extrapunktversuche bleiben im Training, weil sie kein eigenes "Extrapunkt"-Label bekommen,
  sondern das Label des nächsten echten Scores erben — betrifft alle Quellen gleichermaßen,
  keine IFAF-Besonderheit.
- **Tier-Mix ist ungleich:** 22.808 Zeilen `mixed-other` gegen 1.286 Zeilen
  `womens-international` (nur IFAF) — die Tier-Kovariate hat auf der zweiten Gruppe deutlich
  weniger Daten zu lernen.

## Versionierung

- **Champion-Alias (aktuell produktiv):** unverändert die ursprüngliche Phase-1.3-Version (vor
  der Korpus-Erweiterung um deine Workbooks und vor IFAF) — `ep_model` Run
  `5e8ec9573e774ebaa20c9694c6ae15bb`, `wp_model` Run `f9cfe5f348244a7f99dd6817785bff6d`. Die
  oben gemessenen, verbesserten Zahlen stammen von neu registrierten, **noch nicht beförderten**
  Modellversionen — die Beförderung ist eine offene, bewusste Entscheidung (siehe
  `docs/epa-refinement-2026-10.md`, Abschnitt "Champion-Entscheidung").
- **Korpus-Fingerabdruck (2026-09-08):**
  `ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd`.
- **Neue Kandidaten-Läufe (mit HC-Daten, 2026-09-08):** EP
  `97259da7acaf43f3b2c65e59f7f11694`, WP `2c8c249d295d4ce9a2845800c459c153`.
- Jede Modellversion bleibt für immer im Registry erhalten — eine Beförderung ersetzt nie eine
  ältere Version, sie verschiebt nur, welche Version aktuell "Champion" heißt.

## Wie es genutzt wird

`ffep score` berechnet EP/WP für jeden Spielzug im Korpus mit der aktuellen Champion-Version.
Die EPA-Differenz daraus fließt in deinen Player-Analysis-Report (`ffep report`) und in jeden
Auswertungsvergleich (z. B. den Explosiveness/Efficiency-Vorschlag). Historische Auswertungen
verwenden dabei bewusst die Out-of-Fold-Vorhersage (das Modell hat das jeweilige Spiel nie
gesehen), nicht ein Rescoring mit der finalen Champion-Version — sonst würde ein Spiel von
Wissen "profitieren", das zum Zeitpunkt seiner eigenen Messung gar nicht da war.

## Nächste Schritte

1. Champion-Entscheidung treffen (Beförderung der HC/IFAF-erweiterten Läufe ja/nein) —
   `.planning/phases/M3-02-epa-refinement/M3-02-RERUN-2026-09-08-SUMMARY.md` nennt die exakten
   Befehle.
2. Zusatzfrage A/B beantworten (Halbzeit-Marker, Spielklassifizierung deiner Workbook-Spiele) —
   `docs/hc-rueckfragen-2026-09.md`.
3. Fehlende IFAF-Review-Durchgänge beim Anbieter nachfragen (21 von 42 Spielen aktuell nutzbar).
4. Echte Spieluhr für WP prüfen, sobald mehr Quellen sie liefern.
