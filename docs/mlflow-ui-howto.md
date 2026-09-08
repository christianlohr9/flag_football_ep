# MLflow UI -- Anleitung für den Oktober-Sync

Diese Anleitung ist für dich als Entwickler gedacht, nicht zum direkten Weiterreichen an den
Head Coach: die MLflow-UI ist ein Demo-Werkzeug, das du live durchklickst und erklärst, während
er zuschaut -- kein Link, den er unbeaufsichtigt selbst navigiert. Der Grund: die UI zeigt pro
Lauf rohe Metrik-Tabellen und Hyperparameter-Dumps, die ohne Erklärung nur Rauschen sind, kein
Narrativ. Für die eigentliche Coach-Frage ("welches Modell ist produktiv, warum, wie gut, seit
wann") nimm stattdessen [`docs/epa-modellkarte.md`](./epa-modellkarte.md) -- die wird automatisch
aus genau demselben MLflow-Stand generiert (`scripts/render_model_card.py`), aber coach-lesbar
aufbereitet.

## Vorbereitung (einmalig, vor dem Sync)

Kein neuer Code, keine neue Konfiguration -- der Befehl steht bereits in
`docs/model-training.md` Abschnitt 2 und ist hier nur für den Sync-Kontext zusammengefasst.
Starte den Server lokal, aus dem Projektverzeichnis heraus:

```bash
mlflow ui --backend-store-uri sqlite:///$(pwd)/mlruns/mlflow.db
```

Öffne danach `http://127.0.0.1:5000` im Browser. Der Server läuft, solange das Terminal offen
bleibt -- mit `Ctrl+C` beenden, wenn der Sync vorbei ist.

## Was du live zeigen kannst (coach-geeignet)

- **Die registrierten Modelle** (`ep_model`, `wp_model` -- linke Navigation, "Models"): die
  Liste der Versionen und welche Version aktuell den `champion`-Alias trägt. Das beantwortet
  direkt "welches Modell ist produktiv".
- **Seit wann die aktuelle Version registriert ist** (Spalte "Created" auf der Modell-Detailseite
  der jeweiligen Version) -- die näherungsweise Antwort auf "seit wann" (das exakte Datum, an
  dem der `champion`-Alias zuletzt bewegt wurde, protokolliert die Registry selbst nicht separat
  -- dieselbe Einschränkung gilt auch für die Modellkarte).
- **Die Reliability-Kurve** (Artefakt `reliability_ep.png`/`reliability_wp.png` auf der
  Run-Detailseite) -- ein Bild, kein Zahlen-Dump, gut für eine kurze visuelle Erklärung von
  Kalibrierung.

## Was NICHT unbeaufsichtigt gezeigt werden sollte

- **Rohe Pro-Lauf-Metrik-Tabellen** (die "Metrics"-Spalte in der Run-Liste eines Experiments) --
  ohne Erklärung, welche Metrik was bedeutet und warum ein Lauf besser/schlechter ist als ein
  anderer, ist das nur eine Zahlenwand. Die kuratierte, erklärte Fassung dieser Zahlen steht in
  `docs/epa-modellkarte.md`.
- **Hyperparameter-Dumps** (die "Parameters"-Spalte) -- interessant für dich als Entwickler zur
  Fehlersuche, ohne Kontext für den Coach bedeutungslos.
- **Trainingsverlauf-/Tuning-Kurven** einzelner Experimente -- reine Entwicklungsdiagnostik, kein
  Coach-relevanter Inhalt.

Wenn eine dieser Ansichten während der Demo zufällig auftaucht (z. B. weil du zu einer
Experiment-Übersicht statt zur Modell-Übersicht navigierst), kurz kommentieren und zur
Modell-Übersicht oder direkt zur Modellkarte zurückwechseln -- kein Problem, nur kein Ziel der
Demo.

## Nach dem Sync

Kein Aufräumen nötig außer dem Serverstopp (`Ctrl+C`) -- der lokale Store
(`mlruns/mlflow.db`) ist git-ignoriert und bleibt unverändert, egal was in der UI angeklickt
wurde (die UI ist rein lesend für diesen Anwendungsfall; ein `champion`-Alias wird ausschließlich
über `ffep promote` bewegt, nie durch Klicks in der UI während dieser Demo).
