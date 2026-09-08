# ADR 0001 — Modell-Plattform für EP/WP: Single Machine, OTC-VM oder Kubernetes?

**Status:** Entwurf, zur Unterschrift vorgelegt
**Datum:** 2026-09-08
**Betrifft:** `flag_football_ep` EP/WP-Modelle, MLflow-Tracking/Registry, künftige
Coach-Web-App (BL-02)

## Kontext

Dieses Dokument entscheidet die Plattform-Grundlage für die Mehr-Team-Zukunft des Projekts:
U17 bis Seniors, Männer- und Frauen-Nationalmannschaften, und perspektivisch weitere Coaches,
deren heute in Excel geführte Play-by-Play-Analyse in eine Web-App überführt werden soll
(BL-02, vgl. `docs/hc-notes-2026-09-03.md`: "Er hat super viel Arbeit in seine Excel Datei
gesteckt … Dazu wäre es sehr gut eine App zu haben"). Der Kopftrainer selbst hat diese
Entscheidung explizit angestoßen, weil ein falscher Default heute teuer werden könnte, sobald
U17 bis Seniors das Tooling mitnutzen wollen.

**Aktueller Stand (verifiziert, RESEARCH Q6-Q10):** ein Programm (die deutsche Frauen-
Nationalmannschaft), ca. 30.000 Plays im kanonischen Korpus, ausschließlich Batch-Scoring
(Reports laufen in unter 10 Minuten), kein Echtzeit-Bedarf, ein einzelner Entwickler mit
alleinigem Schreibzugriff. Das MLflow-Tracking läuft lokal gegen `sqlite:///mlruns/mlflow.db`
mit einem dateibasierten Artifact-Store — das ist die heutige "Option (i)".

Wichtig für die Reihenfolge dieser Phase: die parallel in dieser Phase laufenden
Hygiene-Arbeiten (Lineage-Erfassung, Korpus-Freeze, Beförderungs-Gate, CI-Lauf auf
Fixture-Daten — Pläne M3-05-02/03/06) sind **Option-(i)-nativ und von dieser Entscheidung
unabhängig**: sie funktionieren unverändert, unabhängig davon, welche Plattform-Option hier
gewählt wird, und ein späterer Umzug auf Option (ii) oder (iii) übernimmt sie unverändert.
Diese ADR entscheidet also **wann**, nicht **ob**, sich die Grundlage ändert.

## Optionen

Die folgende Tabelle stammt unverändert aus RESEARCH Q6 (Kosten sind, wo mit
`[ASSUMED, Größenordnung]` markiert, Schätzungen aus Websuche, nicht Angebote eines
Preisrechners — die Open Telekom Cloud veröffentlicht für die relevanten SKUs (ECS/CCE/OBS)
ausdrücklich keine Festpreise, sondern verweist auf einen Preisrechner, den jede:r selbst
bedienen muss, siehe Quellen unten).

| Option | Monatliche Kosten (Größenordnung) | Operativer Aufwand (Solo-Entwickler) | Migrationspfad ab heute | Bricht zuerst bei 10x Daten / 10 Teams |
|---|---|---|---|---|
| **(i) Single Machine** — MLflow file/SQLite + DVC + geplanter Batch (**heutiger Zustand**) | 0 EUR zusätzlich | Minimal — läuft bereits | Kein Migrationsaufwand; das IST heute | SQLites Single-Writer-Modell riskiert Lock-Contention bei gleichzeitigem Mehrbenutzer-Training/-Promotion; die Registry lebt nur auf einem Laptop — keine Verfügbarkeit ohne dessen Betrieb, was eine Coach-Web-App mit eigenständiger Uptime blockiert |
| **(ii) Containerisiert auf OTC** — eine VM (docker-compose: MLflow-Server + Postgres + OBS-Artifact-Root + Scoring-Job + kleine Web-App) | Niedrige zweistellige Euro-Beträge/Monat, Größenordnung, für eine kleine ECS-VM + OBS-Storage (Cent/GB) `[ASSUMED — niedrige Konfidenz, OTC hat keinen Festpreis, nur einen Rechner]` | Moderat — eine VM patchen/absichern, Postgres-Backups, MLflow-Auth muss explizit aktiviert + Default-Admin-Passwort rotiert werden (nicht standardmäßig an), eine docker-compose-Datei pflegen; noch im Rahmen eines Solo-Entwicklers nach Ersteinrichtung | `mlflow server --backend-store-uri postgresql://... --default-artifact-root s3://...` ersetzt die `sqlite:///`-URI; `MLFLOW_S3_ENDPOINT_URL` gegen OTC OBS ist im eigenen Repo bereits erprobt (`docs/hackathon-otc-upload.md`) | Handhabt Batch-Scoring für eine Handvoll Programme komfortabel; bleibt eine Single-VM-Kapazitätsgrenze bei vielen gleichzeitigen Trainingsläufen, aber Postgres beseitigt SQLites Schreib-Concurrency-Grenze |
| **(iii) Kubernetes** (OTC CCE) + MLflow + Feature Store + Model Serving | CCE-Control-Plane kostenlos für ≤50 Non-HA-Nodes, aber die zugrundeliegenden ECS-Worker-Nodes kosten pro Node dasselbe wie bei (ii)'s VM, und ein Spielzeug-Cluster braucht mindestens 2-3 Nodes — plausibel das 2-3fache von (ii)'s Untergrenze `[ASSUMED — niedrige Konfidenz]` [ZITIERT: t-cloud-public.com/en/prices/pricing-models/computing-container, abgerufen 2026-09-08, "free for up to 50 nodes (not HA)"] | Hoch — Cluster-Upgrades, Netzwerk-Policies, K8s-Secrets, YAML/Helm-Manifeste, Ingress/TLS: eine kategorisch größere operative Fläche als (ii) für eine einzelne Person | (ii) ist strikte Voraussetzung — erst containerisieren, dann K8s-Manifeste/Helm-Charts um dieselben Images bauen | An der tatsächlichen Skala dieses Projekts bricht nichts auf eine Art, die spezifisch K8s löst; K8s rechtfertigt seine Komplexität erst bei Echtzeit-High-QPS-Serving oder vielen unabhängig skalierten Diensten — beides existiert hier nicht (Kontext: "kein Echtzeit-Bedarf heute") |

**Kostenhinweis:** Jede mit `[ASSUMED]` markierte Zahl ist eine Größenordnung, keine Zusage.
Wer vor der Unterschrift eine belastbare Zahl braucht, sollte den echten OTC-Preisrechner
selbst bedienen (siehe Entscheidungs-Checkpoint unten) — diese ADR tut das nicht für Sie.

## Feature Store (RESEARCH Q7)

Ein Feature Store löst zwei Probleme: Training-Serving-Skew (Features werden zur Trainings-
und zur Score-Zeit unterschiedlich berechnet) und Feature-Wiederverwendung über Teams/Modelle
hinweg. Der EP/WP-Feature-Satz dieses Projekts (~12 Batch-only Play-State-Spalten,
`hyperparams.EP_FEATURES`/`WP_FEATURES`) wird von **demselben** `prepare_fn`/`mutate_fn`-Paar
sowohl zur Trainingszeit (`model/train.py`) als auch zur Score-Zeit (`model/score.py` — beide
importieren aus `features/mutations.py`) berechnet — Training-Serving-Skew ist hier also
bereits strukturell durch geteilten Code verhindert, nicht durch fehlende Infrastruktur. Es
gibt keinen Echtzeit-Serving-Bedarf. Ein leichtgewichtiges Äquivalent existiert de facto
bereits: `plays.parquet`/`plays_scored.parquet` plus `training_data_sha256` (bestehend) und der
in dieser Phase eingeführte `corpus_fingerprint` (M3-05-03) sind bereits "ein versioniertes
Feature-Parquet mit Schema-Vertrag und Fingerabdruck" — nur nicht so benannt.

Nach dem Tacnode-Entscheidungsrahmen (3 von 5 Auslöse-Signalen: mehrere Modelle mit
inkonsistenten Feature-Definitionen, Training-Serving-Mismatch-Vorfälle, Echtzeit-
Aktualitätsbedarf, Feature-Engineering-Flaschenhals, doppelte teamübergreifende
Feature-Arbeit) [ZITIERT: tacnode.io/post/do-you-need-a-feature-store, abgerufen 2026-09-08]
steht dieses Projekt heute bei **0 von 5**.

**Empfehlung:** Kein Feature Store jetzt. Neu bewerten (Feast, die Standard-Open-Source-Wahl),
sobald ein zweites/drittes Programm ein Modell mit materiell abweichenden, von einer anderen
Person als dem aktuellen Entwickler gepflegten Features braucht, oder wenn jemals Echtzeit-
Scoring hinzukommt (z. B. eine Live-Win-Probability-Einblendung während einer Übertragung, was
zusätzlich das Backlog-Item "Gameclock aus Broadcast" voraussetzen würde).

## Multi-Tenant-Datenmodell (RESEARCH Q8)

Das kanonische Plays-Schema trägt bereits die Ansätze einer Programm-Skalierung:
`source`, `competition_tier` (One-Hot, seit Phase 1.3 in den Produktions-Features) und
`team_mapping.csv` (bereits um `-M`-Suffixe erweitert, um Männer- von Frauen-
Nationalmannschaften zu unterscheiden, die sich den kanonischen Code `GER` teilen — siehe
`ffep.toml`s `[sources.ifaf]`-Kommentar). Eine erstklassige `programme_id`/`season`-Scope-Spalte
würde dieses Muster erweitern, nicht ersetzen.

**Geteiltes Modell vs. Modell pro Programm:** Die Evidenz, die diese Phase selbst produziert
hat, spricht für ein geteiltes Modell mit Kovariate statt Modelle pro Programm — der erste
echte Tier-Vergleich (`per_tier_metrics_ep.csv`, `docs/epa-refinement-2026-10.md`s Nachtrag)
fand `womens-international` (IFAF) bei EP-Log-Loss 0.881 gegenüber `mixed-other` bei 0.946 —
ein realer, messbarer Tier-Effekt, der bereits durch das Pooling von Daten über den Korpus
hinweg mit einer Tier-Kovariate erfasst wird. Ein Modell, das nur auf IFAF-Daten trainiert
würde, hätte weniger Daten und würde genau diesen Pooling-Vorteil verlieren. Da das Volumen
jedes einzelnen Programms klein ist (allein die Frauen-Nationalmannschaft hat ca. 20.000 Plays;
ein neues U17-Programm würde nahe null starten), ist ein geteiltes Modell mit
Programm-/Tier-Kovariate die evidenzbasierte Empfehlung — kein Modell pro Team.

**PII-Grenzen:** `player_mapping.csv` existiert bereits als programmgebundene
Kader-Zuordnungsdatei mit einem durchgesetzten PII-Gate-Muster (die kaderbasierten Prüfungen in
`tests/test_m3_epa_docs.py`). Das Kader eines zweiten Programms braucht seine eigene
skopierte Zuordnungsdatei nach demselben Gate-Prinzip — niemals mit dem Kader eines anderen
Programms ohne ausdrückliche Zustimmung zusammengeführt.

**Auth für eine Coach-Web-App:** außerhalb des Umfangs dieser Phase (BL-02), aber die
Plattform-Wahl bestimmt, wo sie leben würde — ein dateibasierter Single-Machine-Ansatz
(Option i) hat kein natürliches Zuhause für programmgebundenen Login/Session-Zustand; eine
Postgres-gestützte Bereitstellung (Option ii) ist dort, wo Benutzer-/Session-Tabellen leben
würden, sollte BL-02 gebaut werden.

## MLflow-Betriebsrealitäten (RESEARCH Q9)

MLflow hat standardmäßig keine Authentifizierung. Mehrere CVEs wurden 2026 gegen
unauthentifizierte MLflow-Tracking-Server veröffentlicht (SSRF über Webhook-Zustellung,
CVE-2026-64849; Auth-Bypass an Job-Endpunkten, CVE-2026-0545; Default-Credential-Bypass,
CVE-2026-2635) [ZITIERT, siehe Quellen — Tertiär, nicht in voller Advisory-Tiefe gelesen].
Basic-HTTP-Auth ist verfügbar (`pip install 'mlflow[auth]'`, `mlflow server --app-name
basic-auth`), liefert aber standardmäßig das Credential `admin`/`password1234` aus, das sofort
rotiert werden muss [ZITIERT: mlflow.org/docs/latest/self-hosting/security/basic-http-auth/,
abgerufen 2026-09-08]. Dies ist relevant, sobald jemals Option (ii) oder (iii) tatsächlich
umgesetzt wird — für den heutigen lokalen Betrieb (Option i, `mlflow ui` auf Loopback) besteht
diese Angriffsfläche nicht.

SQLite ist für den heutigen Single-Writer-Betrieb ausreichend; eine echte RDBMS wird zur
Standardempfehlung, sobald gleichzeitige Mehrbenutzer-Schreibzugriffe stattfinden (Option
ii/iii) `[ASSUMED — allgemeine MLflow-Betriebsempfehlung, diese Session nicht gegen eine
spezifische MLflow-Doku-URL verifiziert]`.

**MLflow bleibt** die Wahl der Tracking-/Registry-Lösung unabhängig von dieser Entscheidung —
es ist bereits tief in `train.py`/`registry.py`/`score.py`/`experiments.py` verankert, und
keine Alternative (Weights & Biases: SaaS-Abhängigkeit, die dieses Projekt bewusst vermeidet;
ZenML/Metaflow: volle Pipeline-Orchestrierung, die dieses Projekts dreistufiger `ffep run`
nicht braucht) adressiert die tatsächlichen Schmerzpunkte (manuelle Promotion, fehlende
Lineage, kein Gate — alle innerhalb MLflows behebbar, siehe Phase M3-05 Teil A) besser.

## Empfehlung

RESEARCHs gestufte Empfehlung (Q10), hier unverändert übernommen:

**Bei Option (i) bleiben** durch diese Phase und BL-02s ersten Entwurf hindurch. Zu
**Option (ii)** migrieren, sobald ein konkreter Auslöser eintritt — ein zweiter Mensch braucht
Schreibzugriff auf die Registry, oder BL-02 muss unabhängig davon laufen, ob der Laptop des
Entwicklers eingeschaltet ist. **Kubernetes und einen Feature Store gemeinsam** zurückstellen,
nicht unabhängig voneinander — sie werden tendenziell an demselben Skalenpunkt relevant
(Echtzeit-Anforderung oder mehr als ca. 3 Programme mit materiell abweichenden Feature-
Bedürfnissen), also gemeinsam statt getrennt bewerten.

## Entscheidung (vom Nutzer zu unterschreiben)

*[Ausstehend — wird in Task 3 anhand der Checkpoint-Antwort ausgefüllt. Kein Platzhalter-Text
über diese Zeile hinaus darf als Entscheidung missverstanden werden.]*

## Quellen

- RESEARCH `M3-05-RESEARCH.md`, Fragen Q6-Q10 (Plattform-Optionen, Feature Store,
  Multi-Tenant-Datenmodell, MLflow-Betriebsrealitäten, Empfehlung) — die vollständige
  evidenzielle Grundlage dieses Dokuments.
- [MLflow Authentication with Username and Password](https://mlflow.org/docs/latest/self-hosting/security/basic-http-auth/) — abgerufen 2026-09-08
- [MLflow Artifact Stores](https://mlflow.org/docs/latest/ml/tracking/artifact-stores/) — abgerufen 2026-09-08
- [Do You Need a Feature Store? Decision Framework for ML Teams](https://tacnode.io/post/do-you-need-a-feature-store) — abgerufen 2026-09-08
- [Open Telekom Cloud / T Cloud Public — Pricing Models: Computing & Containers](https://www.t-cloud-public.com/en/prices/pricing-models/computing-container) — abgerufen 2026-09-08
- [MLflow SSRF advisory (GHSA-7gwp-5pfp-969j)](https://github.com/advisories/GHSA-7gwp-5pfp-969j) — abgerufen 2026-09-08
- [CVE-2026-0545 summary](https://www.sentinelone.com/vulnerability-database/cve-2026-0545/) — abgerufen 2026-09-08
- `docs/hackathon-otc-upload.md`, `docs/hc-notes-2026-09-03.md`, `docs/model-training.md`,
  `ffep.toml` — projektinterne Referenzen, gelesen 2026-09-08
