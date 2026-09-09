# EPA/WP-Modellkarte

**Stand:** 2026-09-09. Automatisch generiert von `scripts/render_model_card.py` aus dem
MLflow-Registry-Stand und committeten Referenz-CSVs zum Generierungszeitpunkt -- nie von Hand
eingetragen. Eine Seite, für dich als Head Coach -- keine Spielernamen, keine Personendaten.
Details, Herleitung und alle committeten Zahlen dahinter stehen in
`docs/epa-refinement-2026-10.md` (inkl. Nachtrag).

## Wozu das Modell da ist

Jeder Spielzug bekommt zwei Zahlen: **EP** (Expected Points -- wie viele Punkte bringt diese
Spielsituation im Erwartungswert bis zum nächsten Score) und **WP** (Win Probability -- wie
wahrscheinlich ist der Sieg von hier aus). Die Differenz von EP vor und nach einem Spielzug ist
**EPA** (Expected Points Added) -- die Kennzahl, mit der wir einzelne Spielzüge, Spielerinnen und
Spielsituationen vergleichbar machen, unabhängig vom Spielstand oder der Spielphase.

## Eingaben

Down, Distanz-zum-ersten-Down, Feldposition, Halbzeit, Spielstand-Differenz (nur WP), eine
Wettbewerbs-Tier-Kennung (`mixed-other` / `womens-international`) sowie -- nur bei WP -- eine
**synthetische** Spieluhr (gleichmäßig heruntergezählt, kein echter Spielzeit-Zeitstempel aus den
meisten Quellen). Kein Name, keine Position, keine Spielerin fließt als Eingabe ein -- das Modell
kennt nur die Spielsituation, nie, wer sie ausgeführt hat.

## Trainingskorpus

| Modell | Champion-Run | Version | Zeilen (n_plays) | LOGO-Folds |
|---|---|---:|---:|---:|
| EP | `97259da7acaf43f3b2c65e59f7f11694` | 5 | 24.094 | 323 |
| WP | `2c8c249d295d4ce9a2845800c459c153` | 5 | 24.705 | 323 |

Referenzierter Freeze (`ffep freeze-corpus`-Manifest mit Pro-Quelle-Zeilenzahlen):

- **EP:** kein Freeze referenziert
- **WP:** kein Freeze referenziert

Ohne ein referenziertes Freeze-Manifest stehen hier keine Pro-Quelle-Zeilenzahlen -- die
Pro-Quelle-**Log-Loss**-Werte weiter unten unter "Performance" kommen trotzdem direkt aus dem
jeweiligen MLflow-Lauf, nicht aus einer Schätzung. Seit M3-05-03 schreibt `ffep freeze-corpus` bei jedem Retrain ein datiertes, fingerabdruck-basiertes Manifest nach `data/reference/corpus_freeze/` -- Champion-Läufe von vor dieser Instrumentierung referenzieren noch keins; jeder künftige Retrain zitiert automatisch das zu diesem Zeitpunkt aktuellste Manifest.

## Methode

**Leave-one-game-out (LOGO):** jedes Spiel genau einmal Testspiel, nie gleichzeitig Trainings-
und Testdaten -- kein Blick in die Zukunft, keine Selbstbestätigung. Kalibrierung wird
mitgemessen (Reliability-Kurven, Log-Loss gegen eine einfache Grundrate). Kein Modell wird
automatisch "Champion" -- das ist immer eine bewusste, gemeinsam geprüfte Entscheidung
(`docs/model-training.md` Abschnitt 3).

- **EP** (ep_model): schließt dieser Lauf Extrapunkt-/Zwei-Punkt-Zeilen vollständig vom Training aus? nein -- dieser Champion-Lauf trainierte vor dem Extrapunkt-Ausschluss-Fix (siehe "## Beförderungs-Gate: letzter Stand" unten); jeder künftige Retrain wendet den Fix automatisch an, ohne weiteren Code-Eingriff.
- **WP** (wp_model): schließt dieser Lauf Extrapunkt-/Zwei-Punkt-Zeilen vollständig vom Training aus? nein -- dieser Champion-Lauf trainierte vor dem Extrapunkt-Ausschluss-Fix (siehe "## Beförderungs-Gate: letzter Stand" unten); jeder künftige Retrain wendet den Fix automatisch an, ohne weiteren Code-Eingriff.

## Performance gegen die einfache Grundrate

| Modell | Log-Loss | Grundrate | Verbesserung | Run-ID |
|---|---:|---:|---:|---|
| EP | 0,942659 | 0,994269 | 0,051610 | `97259da7acaf43f3b2c65e59f7f11694` |
| WP | 0,372350 | 0,691566 | 0,319215 | `2c8c249d295d4ce9a2845800c459c153` |

### Pro-Quelle-Aufschlüsselung (aus dem jeweiligen Champion-Lauf selbst)

| Modell | Quelle | Log-Loss |
|---|---|---:|
| EP | `hc_workbook:offense-analytics-2026-camps-and-competitions:data` | 0,978616 |
| EP | `hc_workbook:scoring-probability-by-situation-2023-2026:data` | 0,908592 |
| EP | `ifaf` | 0,881065 |
| EP | `legacy` | 0,977325 |
| EP | `legacy-sportapp` | 0,950924 |
| WP | `hc_workbook:offense-analytics-2026-camps-and-competitions:data` | 0,306335 |
| WP | `hc_workbook:scoring-probability-by-situation-2023-2026:data` | 0,347864 |
| WP | `ifaf` | 0,689286 |
| WP | `legacy` | 0,345898 |
| WP | `legacy-sportapp` | 0,363386 |

Pro-Tier-Aufschlüsselung: nicht verfügbar für diesen Lauf (kein `per_tier_logloss_*`-Metrik auf einem der Champion-Läufe protokolliert).

## Kalibrierung

- **EP:** nicht gemessen für diesen Lauf
- **WP:** nicht gemessen für diesen Lauf

## Beförderungs-Gate: letzter Stand

Der jüngste reale Beförderungs-Gate-Lauf (2026-09-09) prüfte neue Kandidaten
nach dem Extrapunkt-Ausschluss-Fix gegen den amtierenden Champion:

| Modell | Kandidat-Run-ID | Log-Loss | Grundrate | Verbesserung | Gate-Verdikt |
|---|---|---:|---:|---:|---|
| EP | `efd9fd3dc457431d917fd6ce59788305` | 0,938953 | 0,989134 | 0,050181 | FAIL |
| WP | `3b7d571c3f004858b87729ef7b92c30c` | 0,373400 | 0,690405 | 0,317005 | FAIL |

**Owner-Entscheidung (2026-09-09):** none -- keine
Alias-Verschiebung. Der Extrapunkt-Ausschluss-Fix selbst bleibt im Trainingscode
(unabhängig von dieser Entscheidung) und gilt automatisch für den nächsten echten Retrain.
Details zu jedem einzelnen Gate-Check (beats_naive/beats_champion/calibration/no_play_share)
und die vollständige Owner-Begründung stehen im Abschnitt "Methodenänderung:
Extrapunkt-Ausschluss" in `docs/epa-refinement-2026-10.md`.

## Bekannte Grenzen

- **Keine echte Spieluhr** in den meisten Quellen -- WP nutzt eine gleichmäßig heruntergezählte,
  synthetische Uhr. IFAF liefert zwar einen echten Zeitstempel, wird dafür aber (noch) nicht
  genutzt.
- **IFAF-Lücken bleiben real:** nicht jedes Frauen-Spiel ist aktuell nutzbar -- ein Teil hat einen
  unvollständigen oder nie durchgeführten Review-Durchgang beim Anbieter (Details:
  `docs/ifaf-wm2026-daten.md`).
- **Synthetische Zeilen fließen nie ins Training** -- geprüft, nicht nur behauptet.
- **Ein Rand-Verhalten, seit Phase 1.3 unverändert:** gescheiterte (nicht erfolgreiche)
  Extrapunktversuche bleiben im Training, weil sie kein eigenes "Extrapunkt"-Label bekommen,
  sondern das Label des nächsten echten Scores erben -- betrifft jeden Champion-Lauf seit
  Phase 1.3 gleichermaßen, unabhängig davon, welche Quellen er sonst gesehen hat.

## Versionierung

- **Champion-Alias (aktuell produktiv):** `ep_model` Run `97259da7acaf43f3b2c65e59f7f11694` (Version 5, registriert am 2026-09-08), `wp_model` Run `2c8c249d295d4ce9a2845800c459c153` (Version 5, registriert am 2026-09-08).
- **Warum dieser Champion:** Champion-Entscheidung: am 2026-09-09 getroffen ("both") — Champion für EP und WP auf die 2026-09-08 `with_hc`-Läufe verschoben (siehe `## Champion-Entscheidung` im Nachtrag unten für Begründung und Run-IDs).
- **Korpus-Fingerabdruck / Commit dieses jeweiligen Laufs:** EP: Fingerabdruck ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd, Commit 82ae8cc88908283094dde036def7a883f1b5214a; WP: Fingerabdruck ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd, Commit 82ae8cc88908283094dde036def7a883f1b5214a. Zum Vergleich, der Korpus-Stand von HEUTE laut `docs/epa-refinement-2026-10.md` (nicht zwingend die Trainingsbasis der obigen Champion-Läufe): Fingerabdruck `ae1f014022b4588ed33c7f31894e96a78e87fa1ef62c4e66201162f62b1b6dcd`, Commit `82ae8cc88908283094dde036def7a883f1b5214a`.
- Jede Modellversion bleibt für immer im Registry erhalten -- eine Beförderung ersetzt nie eine
  ältere Version, sie verschiebt nur, welche Version aktuell "Champion" heißt.

## Plattform

Der lokale MLflow-Tracking-Store (`mlruns/mlflow.db`) kann optional in eine containerisierte
Variante gespiegelt werden (Postgres-Backend-Store, MinIO-S3-Artefakt-Store,
`docker-compose.mlflow.yml`) -- Betriebsanleitung inkl. Start/Stopp, Migration und
Backup/Restore: `docs/mlflow-container-platform.md`. Die MLflow-UI (lokal oder
containerisiert) zeigt registrierte Modelle, Champion-Alias/Version und die Reliability-Kurve
live -- Schritt-für-Schritt-Anleitung für die Coach-Session: `docs/mlflow-ui-howto.md`.

## Wie es genutzt wird

`ffep score` berechnet EP/WP für jeden Spielzug im Korpus mit der aktuellen Champion-Version.
Die EPA-Differenz daraus fließt in deinen Player-Analysis-Report (`ffep report`) und in jeden
Auswertungsvergleich (z. B. den Explosiveness/Efficiency-Vorschlag). Historische Auswertungen
verwenden dabei bewusst die Out-of-Fold-Vorhersage (das Modell hat das jeweilige Spiel nie
gesehen), nicht ein Rescoring mit der finalen Champion-Version -- sonst würde ein Spiel von
Wissen "profitieren", das zum Zeitpunkt seiner eigenen Messung gar nicht da war.

## Nächste Schritte

1. Champion-Entscheidung treffen (Beförderung neuerer, HC-/IFAF-erweiterter Läufe ja/nein) --
   `.planning/phases/M3-02-epa-refinement/M3-02-RERUN-2026-09-08-SUMMARY.md` nennt die exakten
   `ffep promote`-Befehle für die aktuell gemessenen Kandidaten.
2. Zusatzfrage A/B beantworten (Halbzeit-Marker, Spielklassifizierung deiner Workbook-Spiele) --
   `docs/hc-rueckfragen-2026-09.md`.
3. Fehlende IFAF-Review-Durchgänge beim Anbieter nachfragen.
4. Echte Spieluhr für WP prüfen, sobald mehr Quellen sie liefern.
