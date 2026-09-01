# Vendor-Verzeichnis: gepinnte Fremdcode-Kopien

## Zweck

Zwei Repositories liegen hier als gepinnte, ungepatchte Kopien statt als Paketinstallation:

- `sjc042/gta-link` (Global Tracklet Association, "GTA") hat keine PyPI-Veröffentlichung — der
  einzige Bezugsweg ist ein Git-Klon.
- Die Lizenzprüfung (D-02, `.planning/PROJECT.md`) verlangt, dass jede fremde Codequelle vor
  Verwendung live gegen die GitHub-API geprüft wird, nicht aus dem Gedächtnis behauptet — siehe
  `## Gepinnte Quellen` unten für den rohen API-Output dieser Prüfung.
- Die im Repo gemeinsam genutzte `.venv` wird von den parallel laufenden 2.2-Wellen (Wellen 7–11)
  verwendet. Dieser Plan (M2-02-02) ist so gebaut, dass er `pyproject.toml`/`uv.lock` nicht
  anfasst — ein `pip install`/`uv add` von Forschungscode ohne PyPI-Release wäre ohnehin nicht der
  saubere Weg, ein Vendor-Verzeichnis mit fixierter SHA ist reproduzierbarer und risikoärmer für
  die geteilte Umgebung.

## Gepinnte Quellen

| Komponente | Repository | Commit-SHA | Lizenz (SPDX) | geprüft am | Quelle der Lizenzangabe |
|---|---|---|---|---|---|
| GTA (Split/Merge-Algorithmus) | `github.com/sjc042/gta-link` | `e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca` | MIT | 2026-09-01T14:42:53Z | `gh api repos/sjc042/gta-link --jq '{spdx: .license.spdx_id, pushed: .pushed_at, stars: .stargazers_count}'` → `{"pushed":"2025-12-12T07:26:36Z","spdx":"MIT","stars":91}` |
| OSNet-Erscheinungs-Encoder (`torchreid`, offizielles Repo) | `github.com/KaiyangZhou/deep-person-reid` | *(kein separates Vendoring nötig — siehe unten)* | MIT | 2026-09-01T14:42:53Z | `gh api repos/KaiyangZhou/deep-person-reid --jq '{spdx: .license.spdx_id, pushed: .pushed_at, stars: .stargazers_count}'` → `{"pushed":"2026-01-09T05:23:24Z","spdx":"MIT","stars":4905}` |

`gta-link` hat keine Git-Tags (`git ls-remote --tags` liefert keine Zeilen); gepinnt wird daher auf
den aktuellen `main`-Branch-Commit zum Prüfzeitpunkt, nicht auf einen Branch-Namen.
`deep-person-reid` hat Tags bis `v1.0.6` (`cab4a342cc2acd49c1360abf9ea125cf8afc471d`).

**Kein separates Klonen von `deep-person-reid` nötig (RESEARCH-Annahme A5 bestätigt):**
`vendor/gta-link/reid/` ist bereits eine vollständige, strukturell identische Kopie des
offiziellen `deep-person-reid`-Repos (`.flake8`, `Dockerfile`, `LICENSE`, `torchreid/`,
`torchreid.egg-info`, `docs/MODEL_ZOO.md` — exakt die Dateiliste, die RESEARCH.md als Fingerabdruck
nennt). `vendor/gta-link/reid/LICENSE` beginnt mit `MIT License / Copyright (c) 2018 Kaiyang Zhou`
— derselbe Copyright-Inhaber wie im offiziellen Repo. `vendor/gta-link/reid/torchreid/models/osnet.py`
enthält dieselben Klassennamen (`OSNet`, `OSBlock`, `ChannelGate`, …) und denselben
`pretrained_urls`-Dict-Inhalt, den `docs/MODEL_ZOO.md` im selben Verzeichnis dokumentiert. Damit ist
Weg (c) aus dem Plan — "vendorierte Kopie verwenden, falls vorhanden und nicht materiell
abweichend" — der genommene Pfad; kein zweiter `git clone` von `deep-person-reid`.

## Reproduktion

```bash
mkdir -p vendor
git clone https://github.com/sjc042/gta-link.git vendor/gta-link
cd vendor/gta-link
git checkout e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca
```

Die OSNet-Implementierung liegt danach unter `vendor/gta-link/reid/torchreid/models/osnet.py` —
kein zweiter Klon nötig (siehe oben).

## Checkpoint

Gewählte Variante: `osnet_x1_0`, **Personen-ReID-trainiert auf Market-1501** (Rank-1 94.2 / mAP
82.6 laut `vendor/gta-link/reid/docs/MODEL_ZOO.md` §"Same-domain ReID"), nicht die
ImageNet-only-Variante aus demselben Dokument §"ImageNet pretrained models". Begründung: GTA nutzt
den Embedding-Vektor für Erscheinungs-Wiedererkennung (Appearance-Re-Identification) zwischen
Tracklets — ein auf Personen-Wiedererkennung trainiertes Backbone ist für genau diese Aufgabe
gebaut, ein nur-ImageNet-klassifizierendes Backbone nicht.

- **Quelle:** `vendor/gta-link/reid/docs/MODEL_ZOO.md`, Zeile `osnet_x1_0` unter "Same-domain ReID"
  → Google-Drive-Link `https://drive.google.com/file/d/1vduhq5DpN2q1g4fYEZfPI17MJeh9qyrA/view` (Drive-ID
  `1vduhq5DpN2q1g4fYEZfPI17MJeh9qyrA`), gefunden in der vendorierten MIT-Quelle, nicht per Websuche.
- **Zielpfad:** `data/processed/baseline-methods/checkpoints/osnet_x1_0_market1501.pth`
  (gitignored, kein Teil des Repos)
- **Bezug:** `uv run --with gdown python -m gdown "https://drive.google.com/uc?id=1vduhq5DpN2q1g4fYEZfPI17MJeh9qyrA" -O data/processed/baseline-methods/checkpoints/osnet_x1_0_market1501.pth`
  (ephemer über `uv run --with` — kein permanenter Eintrag in `pyproject.toml`/`uv.lock`)
- **SHA-256:** `2809d3227f7d078f6045f7feb874a34d0684f0e0057b264b99adccf7d4519154`
- **Ausdrücklich NICHT verwendet:** der sport-feingetunte Checkpoint `sports_model.pth.tar-60`,
  auf den sowohl `gta-link` als auch das unlizenzierte Deep-EIoU-Repo verweisen. Dieser Checkpoint
  hat keine nachvollziehbare Lizenz oder Herkunft (informeller Google-Drive-Link, identischer
  Dateiname in zwei nicht-lizenzierten bzw. unlizenz-angrenzenden Projekten). Konsequenz: das
  gemessene GTA-Embedding ist generisches Personen-ReID (Market-1501-trainiert), nicht
  sport-spezifisch feingetunt — dies ist eine gemessene Einschränkung, keine verschwiegene.

## Nicht verwendet

- **PyPI-Paket `torchreid`** (bare Name, z. B. `pip install torchreid`): Der Maintainer dieses
  PyPI-Pakets (`kadirnar`, verlinkt auf `github.com/goksenin-uav/torchreid-pip`) ist NICHT
  Kaiyang Zhou, der Autor des kanonischen `deep-person-reid`-Repos. Trotz `MIT`-Lizenzangabe im
  PyPI-Klassifizierer und einem sauberen `slopcheck`-Ergebnis ist dies ein Maintainer-Mismatch, den
  automatisierte Registry-Checks nicht erkennen (RESEARCH.md, Pitfall 1). Nicht installiert, nicht
  importiert.
- **`hsiangwei0903/Deep-EIoU`** (Referenzimplementierung des vierten BASE-01-Kandidaten): Hat KEINE
  `LICENSE`-Datei im Repo-Root (`gh api repos/hsiangwei0903/Deep-EIoU --jq '.license'` → `null`).
  Unter Default-Copyright bedeutet das "alle Rechte vorbehalten" — keine Erlaubnis zur
  Installation, Ausführung oder zum Vendoring, auch nicht rein lokal. Wird in
  `data/reference/baseline-methods/summary.csv` nicht gemessen; die Begründung steht in Plan
  M2-02-01/M2-02-03, nicht hier.

## Bekannte Probleme

Der gepinnte Commit von `gta-link` liefert unter `reid_checkpoints/sports_model.pth.tar-60`
denselben nicht nachvollziehbaren, sport-feingetunten Checkpoint direkt im Repository mit aus
(30.393.613 Bytes, ein echtes PyTorch-`state_dict`, kein Platzhalter — verifiziert per
`git log --oneline -- reid_checkpoints/sports_model.pth.tar-60`, Commit `1c08b3d`). Diese Datei
wurde NICHT von uns heruntergeladen — sie kam bereits im gepinnten Klon mit. Da die
Akzeptanzkriterien dieses Plans verlangen, dass die Datei nirgendwo unter `vendor/` auf der
Festplatte existiert, wurde sie nach dem Checkout aus dem lokalen (ohnehin per
`vendor/.gitignore` nicht committeten) Arbeitsbaum gelöscht: `rm vendor/gta-link/reid_checkpoints/sports_model.pth.tar-60`.
Das ändert die gepinnte Commit-SHA nicht — bei einer erneuten Reproduktion nach den obigen
`## Reproduktion`-Schritten taucht die Datei wieder auf und muss erneut gelöscht werden, falls
dasselbe Akzeptanzkriterium erneut geprüft wird. Sie wurde zu keinem Zeitpunkt geladen oder als
Checkpoint verwendet; `scripts/hackathon/measure_gta.py` referenziert ausschließlich den separat
bezogenen, oben dokumentierten `osnet_x1_0_market1501.pth`.

Ansonsten keine Probleme: beide Lizenzprüfungen bestanden live, der eigene Checkpoint-Download war
beim ersten Versuch erfolgreich, kein weiterer Vendoring-Schritt musste abgebrochen werden.
