"""Generate the coach-readable German model card (`docs/epa-modellkarte.md`) from the live
MLflow registry plus committed `data/reference/epa_refinement/` CSVs.

This is a driver, not a training path (same convention as `scripts/hc_corpus_ablation.py`/
`scripts/epa_comparison.py`): it never fits a model and never moves the `champion` alias. It
resolves whichever champion is currently live for `ep_model`/`wp_model`
(`flag_football_ep.model.registry.resolve_champion`), reads that run's own MLflow
params/metrics, and writes every number on the card straight from that data -- no numeric
literal is typed into this script's template strings.

The card must stay regenerable from day one of phase M3-05, before later plans in this phase
add lineage (`corpus_fingerprint`/`git_commit` on the champion run itself), calibration
(`calibration_max_deviation_*`), per-tier metrics (`per_tier_logloss_*`) or a corpus freeze
manifest. Every one of those fields is read defensively (`.get(...)`/key-prefix scan) -- when
absent, the card states a named "nicht verfuegbar"/"nicht gemessen"/"unbekannt"/"kein Freeze
referenziert" state for that field instead of crashing or silently omitting the section. If
`resolve_champion` raises (no champion set yet), the card says so explicitly instead of
crashing -- it must always be regenerable, even before any promotion has happened.

Usage:
    uv run python scripts/render_model_card.py [--config ffep.toml] [--out docs/epa-modellkarte.md]
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from mlflow.exceptions import MlflowException  # noqa: E402
from mlflow.tracking import MlflowClient  # noqa: E402

from flag_football_ep.config import Config, load_config  # noqa: E402
from flag_football_ep.model import mlflow_store, registry  # noqa: E402
from flag_football_ep.model.registry import CHAMPION_ALIAS, RegistryError  # noqa: E402

EPA_DIR = REPO_ROOT / "data" / "reference" / "epa_refinement"
REFINEMENT_DOC = REPO_ROOT / "docs" / "epa-refinement-2026-10.md"

# EP/WP log different pooled-metric key names by design (RESEARCH.md Pitfall 2,
# model/train.py::train_ep/train_wp's own `metric_name` argument) -- resolved per model
# prefix here, never hard-coded to a single name for both.
_POOLED_METRIC_NAME: dict[str, str] = {"ep": "logo_mlogloss", "wp": "logo_logloss"}
_MODEL_LABEL: dict[str, str] = {"ep": "EP", "wp": "WP"}

_SOURCE_METRIC_PREFIX = "logo_logloss_by_source_"
_PER_TIER_METRIC_PREFIX = "per_tier_logloss_"
_CALIBRATION_METRIC_PREFIX = "calibration_max_deviation_"

# M3-05-08: the M3-05-07 extra-point-exclusion methodology-change section in the refinement
# doc -- read defensively (never assumed present) so this script stays regenerable both before
# and after that section lands, same discipline as _champion_reason()/_current_corpus_context().
_METHOD_CHANGE_MARKER = "## Methodenaenderung: Extrapunkt-Ausschluss"
_CANDIDATE_VERDICT_RE = re.compile(r"\*\*(EP|WP)-Kandidat \(`([0-9a-f]{32})`\): (PASS|FAIL)\*\*")
_GATE_DECISION_RE = re.compile(
    r"\*\*Entschieden am (\d{4}-\d{2}-\d{2})\*\*.*?:\s*\*\*(\w+)\*\*\s*--", re.DOTALL
)

NA_NO_FREEZE = "kein Freeze referenziert"
NA_UNKNOWN = "unbekannt für diesen Lauf"
NA_MISSING = "nicht verfügbar für diesen Lauf"
NA_UNMEASURED = "nicht gemessen für diesen Lauf"
NA_NO_CHAMPION = "kein Champion gesetzt"


def _de(value: float, decimals: int = 6) -> str:
    """German-locale decimal formatting: comma, no thousands separator."""
    return f"{value:.{decimals}f}".replace(".", ",")


def _de_int(value) -> str:
    """German-locale thousands-grouped integer formatting; passes non-numeric NA strings
    through unchanged (never crashes on a defensive 'nicht verfuegbar' placeholder)."""
    try:
        return f"{int(value):,}".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


@dataclass
class ChampionSnapshot:
    prefix: str
    registered_name: str
    run_id: str | None
    version: str
    registered_on: str
    n_plays: str
    n_folds: str
    metric_name: str | None
    metric_value: float | None
    naive_metric_name: str | None
    naive_value: float | None
    improvement: float | None
    per_source: list[tuple[str, float]] = field(default_factory=list)
    per_tier: list[tuple[str, float]] = field(default_factory=list)
    calibration: list[tuple[str, float]] = field(default_factory=list)
    corpus_fingerprint: str | None = None
    git_commit: str | None = None
    freeze_manifest: str | None = None
    extra_point_excluded: str | None = None
    error: str | None = None

    @property
    def missing(self) -> bool:
        return self.error is not None


def _sanitized_source_map(params: dict) -> dict[str, str]:
    """Reverses `train.py`'s `orig->safe` metric-key sanitization notes (e.g.
    `legacy-sportapp->legacy_sportapp`) so the card can print the real source name."""
    raw = params.get("sanitized_source_metric_keys", "")
    mapping: dict[str, str] = {}
    for pair in raw.split(","):
        if "->" in pair:
            orig, safe = pair.split("->", 1)
            mapping[safe] = orig
    return mapping


def _missing_snapshot(prefix: str, registered_name: str) -> ChampionSnapshot:
    return ChampionSnapshot(
        prefix=prefix,
        registered_name=registered_name,
        run_id=None,
        version=NA_MISSING,
        registered_on=NA_MISSING,
        n_plays=NA_MISSING,
        n_folds=NA_MISSING,
        metric_name=None,
        metric_value=None,
        naive_metric_name=None,
        naive_value=None,
        improvement=None,
        error=NA_NO_CHAMPION,
    )


def load_champion_snapshot(prefix: str, config: Config, client: MlflowClient) -> ChampionSnapshot:
    """Resolve the live champion for `{prefix}_model` and read every field this card needs
    off that run's own MLflow params/metrics -- never off a training script's in-memory
    result, so this function reflects exactly what the registry says right now."""
    registered_name = registry.registered_model_name(prefix)
    try:
        run_id = registry.resolve_champion(registered_name, config)
    except RegistryError:
        return _missing_snapshot(prefix, registered_name)

    run = client.get_run(run_id)
    params = run.data.params
    metrics = run.data.metrics

    try:
        mv = client.get_model_version_by_alias(registered_name, CHAMPION_ALIAS)
        version = mv.version
        registered_on = (
            datetime.fromtimestamp(mv.creation_timestamp / 1000, tz=timezone.utc)
            .date()
            .isoformat()
        )
    except MlflowException:
        version, registered_on = NA_MISSING, NA_MISSING

    metric_name = _POOLED_METRIC_NAME[prefix]
    naive_metric_name = f"naive_{metric_name.removeprefix('logo_')}"

    safe_to_orig = _sanitized_source_map(params)
    per_source = sorted(
        (
            safe_to_orig.get(
                key[len(_SOURCE_METRIC_PREFIX) :], key[len(_SOURCE_METRIC_PREFIX) :]
            ),
            value,
        )
        for key, value in metrics.items()
        if key.startswith(_SOURCE_METRIC_PREFIX)
    )
    per_tier = sorted(
        (key[len(_PER_TIER_METRIC_PREFIX) :], value)
        for key, value in metrics.items()
        if key.startswith(_PER_TIER_METRIC_PREFIX)
    )
    calibration = sorted(
        (key[len(_CALIBRATION_METRIC_PREFIX) :], value)
        for key, value in metrics.items()
        if key.startswith(_CALIBRATION_METRIC_PREFIX)
    )

    return ChampionSnapshot(
        prefix=prefix,
        registered_name=registered_name,
        run_id=run_id,
        version=str(version),
        registered_on=registered_on,
        n_plays=params.get("n_plays", NA_MISSING),
        n_folds=params.get("n_folds", NA_MISSING),
        metric_name=metric_name,
        metric_value=metrics.get(metric_name),
        naive_metric_name=naive_metric_name,
        naive_value=metrics.get(naive_metric_name),
        improvement=metrics.get("logloss_improvement"),
        per_source=per_source,
        per_tier=per_tier,
        calibration=calibration,
        corpus_fingerprint=params.get("corpus_fingerprint"),
        git_commit=params.get("git_commit"),
        freeze_manifest=params.get("freeze_manifest_path"),
        extra_point_excluded=params.get("extra_point_rows_excluded"),
        error=None,
    )


def _champion_reason(doc_text: str) -> str:
    """Whatever promotion-status line `docs/epa-refinement-2026-10.md` carries today --
    read, never assumed. This plan has no dependency on M3-05-01's own
    '## Champion-Entscheidung' section landing first."""
    match = re.search(r"^Champion-Entscheidung:.*$", doc_text, re.MULTILINE)
    if match:
        return match.group(0).strip()
    return (
        "keine 'Champion-Entscheidung:'-Zeile in docs/epa-refinement-2026-10.md gefunden -- "
        "die Champion-Beförderung ist dort nicht (mehr) explizit dokumentiert."
    )


def _current_corpus_context(doc_text: str) -> tuple[str | None, str | None]:
    """The most recent corpus fingerprint/commit *the document* reports today -- context
    about today's overall corpus, not necessarily the champion run's own training basis
    (that distinction is spelled out explicitly wherever this is quoted on the card)."""
    fp_match = re.search(r"\*\*Korpus-Fingerabdruck:\*\*\s*`([0-9a-f]+)`", doc_text)
    commit_match = re.search(r"\*\*Commit:\*\*\s*`([0-9a-f]+)`", doc_text)
    fp = fp_match.group(1) if fp_match else None
    commit = commit_match.group(1) if commit_match else None
    return fp, commit


def _all_ablation_rows() -> dict[str, dict]:
    """Every row from every `ablation_summary.csv` under EPA_DIR -- the top-level
    (2026-09-04, undated) file plus every dated subfolder (e.g. `2026-09-08/`, `2026-09-09/`)
    -- keyed by `run_id`. Globs rather than hard-coding one date, mirroring
    `tests/test_m3_epa_docs.py::_all_ablation_rows`."""
    rows_by_run_id: dict[str, dict] = {}
    candidates = [EPA_DIR / "ablation_summary.csv", *sorted(EPA_DIR.glob("*/ablation_summary.csv"))]
    for path in candidates:
        if not path.exists():
            continue
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                rows_by_run_id[row["run_id"]] = row
    return rows_by_run_id


def _extra_point_fix_status(doc_text: str) -> dict | None:
    """Pull the dated M3-05-07 extra-point-exclusion methodology section's key facts
    (per-model candidate verdicts + the owner's promotion decision) straight out of the
    refinement doc -- never hand-typed here. Returns None if the section is not (yet)
    present, or if its shape does not match what this parser expects (degrades gracefully
    rather than rendering a malformed/partial claim)."""
    idx = doc_text.find(_METHOD_CHANGE_MARKER)
    if idx == -1:
        return None
    section = doc_text[idx:]

    candidates = {
        m.group(1): (m.group(2), m.group(3)) for m in _CANDIDATE_VERDICT_RE.finditer(section)
    }
    decision_match = _GATE_DECISION_RE.search(section)
    if not candidates or decision_match is None:
        return None

    return {
        "candidates": candidates,  # {"EP": (run_id, verdict), "WP": (run_id, verdict)}
        "decision_date": decision_match.group(1),
        "decision": decision_match.group(2),
    }


def _render_gate_status(status: dict | None) -> str | None:
    """Renders the '## Beförderungs-Gate: letzter Stand' section from `status` (see
    `_extra_point_fix_status`) plus each candidate's own measured figures, read from the
    committed `data/reference/epa_refinement/*/ablation_summary.csv` row for that run id --
    never a second, hand-typed copy of a number already measured elsewhere. Returns None
    (section omitted entirely) if there is no methodology-change section yet, or if a cited
    candidate run id has no matching CSV row."""
    if status is None:
        return None

    rows_by_id = _all_ablation_rows()
    table_rows = []
    for label in ("EP", "WP"):
        if label not in status["candidates"]:
            continue
        run_id, verdict = status["candidates"][label]
        row = rows_by_id.get(run_id)
        if row is None:
            continue
        table_rows.append(
            f"| {label} | `{run_id}` | {_de(float(row['metric_value']))} | "
            f"{_de(float(row['naive_value']))} | {_de(float(row['logloss_improvement']))} | "
            f"{verdict} |"
        )
    if not table_rows:
        return None

    table = "\n".join(table_rows)
    return f"""## Beförderungs-Gate: letzter Stand

Der jüngste reale Beförderungs-Gate-Lauf ({status['decision_date']}) prüfte neue Kandidaten
nach dem Extrapunkt-Ausschluss-Fix gegen den amtierenden Champion:

| Modell | Kandidat-Run-ID | Log-Loss | Grundrate | Verbesserung | Gate-Verdikt |
|---|---|---:|---:|---:|---|
{table}

**Owner-Entscheidung ({status['decision_date']}):** {status['decision']} -- keine
Alias-Verschiebung. Der Extrapunkt-Ausschluss-Fix selbst bleibt im Trainingscode
(unabhängig von dieser Entscheidung) und gilt automatisch für den nächsten echten Retrain.
Details zu jedem einzelnen Gate-Check (beats_naive/beats_champion/calibration/no_play_share)
und die vollständige Owner-Begründung stehen im Abschnitt "Methodenänderung:
Extrapunkt-Ausschluss" in `docs/epa-refinement-2026-10.md`."""


# ---------------------------------------------------------------------------
# Static prose (ported from the hand-written docs/epa-modellkarte.md, 2026-09-08 --
# unchanged in substance, per plan instruction; the corpus-composition-specific "Tier-Mix"
# bullet from that version is NOT ported verbatim here -- it described the 2026-09-08
# with_hc/IFAF ablation arm, not the live champion this card actually describes, which would
# misrepresent whichever champion happens to be live (see this plan's own instruction to
# never crash or silently misstate a field). Tier coverage is reported dynamically instead,
# from the champion run's own per_tier_logloss_* metrics when present.
# ---------------------------------------------------------------------------

_INTRO_PURPOSE = """## Wozu das Modell da ist

Jeder Spielzug bekommt zwei Zahlen: **EP** (Expected Points -- wie viele Punkte bringt diese
Spielsituation im Erwartungswert bis zum nächsten Score) und **WP** (Win Probability -- wie
wahrscheinlich ist der Sieg von hier aus). Die Differenz von EP vor und nach einem Spielzug ist
**EPA** (Expected Points Added) -- die Kennzahl, mit der wir einzelne Spielzüge, Spielerinnen und
Spielsituationen vergleichbar machen, unabhängig vom Spielstand oder der Spielphase."""

_INTRO_INPUTS = """## Eingaben

Down, Distanz-zum-ersten-Down, Feldposition, Halbzeit, Spielstand-Differenz (nur WP), eine
Wettbewerbs-Tier-Kennung (`mixed-other` / `womens-international`) sowie -- nur bei WP -- eine
**synthetische** Spieluhr (gleichmäßig heruntergezählt, kein echter Spielzeit-Zeitstempel aus den
meisten Quellen). Kein Name, keine Position, keine Spielerin fließt als Eingabe ein -- das Modell
kennt nur die Spielsituation, nie, wer sie ausgeführt hat."""

_KNOWN_LIMITS_STATIC = """- **Keine echte Spieluhr** in den meisten Quellen -- WP nutzt eine gleichmäßig heruntergezählte,
  synthetische Uhr. IFAF liefert zwar einen echten Zeitstempel, wird dafür aber (noch) nicht
  genutzt.
- **IFAF-Lücken bleiben real:** nicht jedes Frauen-Spiel ist aktuell nutzbar -- ein Teil hat einen
  unvollständigen oder nie durchgeführten Review-Durchgang beim Anbieter (Details:
  `docs/ifaf-wm2026-daten.md`).
- **Synthetische Zeilen fließen nie ins Training** -- geprüft, nicht nur behauptet.
- **Ein Rand-Verhalten, seit Phase 1.3 unverändert:** gescheiterte (nicht erfolgreiche)
  Extrapunktversuche bleiben im Training, weil sie kein eigenes "Extrapunkt"-Label bekommen,
  sondern das Label des nächsten echten Scores erben -- betrifft jeden Champion-Lauf seit
  Phase 1.3 gleichermaßen, unabhängig davon, welche Quellen er sonst gesehen hat."""

_USAGE_STATIC = """## Wie es genutzt wird

`ffep score` berechnet EP/WP für jeden Spielzug im Korpus mit der aktuellen Champion-Version.
Die EPA-Differenz daraus fließt in deinen Player-Analysis-Report (`ffep report`) und in jeden
Auswertungsvergleich (z. B. den Explosiveness/Efficiency-Vorschlag). Historische Auswertungen
verwenden dabei bewusst die Out-of-Fold-Vorhersage (das Modell hat das jeweilige Spiel nie
gesehen), nicht ein Rescoring mit der finalen Champion-Version -- sonst würde ein Spiel von
Wissen "profitieren", das zum Zeitpunkt seiner eigenen Messung gar nicht da war."""

_PLATFORM_STATIC = """## Plattform

Der lokale MLflow-Tracking-Store (`mlruns/mlflow.db`) kann optional in eine containerisierte
Variante gespiegelt werden (Postgres-Backend-Store, MinIO-S3-Artefakt-Store,
`docker-compose.mlflow.yml`) -- Betriebsanleitung inkl. Start/Stopp, Migration und
Backup/Restore: `docs/mlflow-container-platform.md`. Die MLflow-UI (lokal oder
containerisiert) zeigt registrierte Modelle, Champion-Alias/Version und die Reliability-Kurve
live -- Schritt-für-Schritt-Anleitung für die Coach-Session: `docs/mlflow-ui-howto.md`."""

_NEXT_STEPS_STATIC = """## Nächste Schritte

1. Champion-Entscheidung treffen (Beförderung neuerer, HC-/IFAF-erweiterter Läufe ja/nein) --
   `.planning/phases/M3-02-epa-refinement/M3-02-RERUN-2026-09-08-SUMMARY.md` nennt die exakten
   `ffep promote`-Befehle für die aktuell gemessenen Kandidaten.
2. Zusatzfrage A/B beantworten (Halbzeit-Marker, Spielklassifizierung deiner Workbook-Spiele) --
   `docs/hc-rueckfragen-2026-09.md`.
3. Fehlende IFAF-Review-Durchgänge beim Anbieter nachfragen.
4. Echte Spieluhr für WP prüfen, sobald mehr Quellen sie liefern."""


def _method_extra_point_sentence(snap: ChampionSnapshot, methodenaenderung_present: bool) -> str:
    if snap.missing:
        return NA_UNKNOWN
    if snap.extra_point_excluded is not None:
        return snap.extra_point_excluded
    if methodenaenderung_present:
        return (
            "nein -- dieser Champion-Lauf trainierte vor dem Extrapunkt-Ausschluss-Fix "
            '(siehe "## Beförderungs-Gate: letzter Stand" unten); jeder künftige Retrain '
            "wendet den Fix automatisch an, ohne weiteren Code-Eingriff"
        )
    return NA_UNKNOWN


def _render_method(
    snapshots: dict[str, ChampionSnapshot], methodenaenderung_present: bool
) -> str:
    sentences = "\n".join(
        f"- **{_MODEL_LABEL[prefix]}** ({snap.registered_name}): schließt dieser Lauf "
        f"Extrapunkt-/Zwei-Punkt-Zeilen vollständig vom Training aus? "
        f"{_method_extra_point_sentence(snap, methodenaenderung_present)}."
        for prefix, snap in snapshots.items()
    )
    return f"""## Methode

**Leave-one-game-out (LOGO):** jedes Spiel genau einmal Testspiel, nie gleichzeitig Trainings-
und Testdaten -- kein Blick in die Zukunft, keine Selbstbestätigung. Kalibrierung wird
mitgemessen (Reliability-Kurven, Log-Loss gegen eine einfache Grundrate). Kein Modell wird
automatisch "Champion" -- das ist immer eine bewusste, gemeinsam geprüfte Entscheidung
(`docs/model-training.md` Abschnitt 3).

{sentences}"""


def _render_trainingskorpus(snapshots: dict[str, ChampionSnapshot]) -> str:
    rows = []
    for prefix, snap in snapshots.items():
        run_cell = f"`{snap.run_id}`" if snap.run_id else NA_NO_CHAMPION
        rows.append(
            f"| {_MODEL_LABEL[prefix]} | {run_cell} | {snap.version} | "
            f"{_de_int(snap.n_plays)} | {_de_int(snap.n_folds)} |"
        )
    table = "\n".join(rows)
    freeze_lines = "\n".join(
        f"- **{_MODEL_LABEL[prefix]}:** {snap.freeze_manifest or NA_NO_FREEZE}"
        for prefix, snap in snapshots.items()
    )
    any_missing_freeze = any(not snap.freeze_manifest for snap in snapshots.values())
    freeze_note = (
        " Seit M3-05-03 schreibt `ffep freeze-corpus` bei jedem Retrain ein datiertes, "
        "fingerabdruck-basiertes Manifest nach `data/reference/corpus_freeze/` -- "
        "Champion-Läufe von vor dieser Instrumentierung referenzieren noch keins; jeder "
        "künftige Retrain zitiert automatisch das zu diesem Zeitpunkt aktuellste Manifest."
        if any_missing_freeze
        else ""
    )
    return f"""## Trainingskorpus

| Modell | Champion-Run | Version | Zeilen (n_plays) | LOGO-Folds |
|---|---|---:|---:|---:|
{table}

Referenzierter Freeze (`ffep freeze-corpus`-Manifest mit Pro-Quelle-Zeilenzahlen):

{freeze_lines}

Ohne ein referenziertes Freeze-Manifest stehen hier keine Pro-Quelle-Zeilenzahlen -- die
Pro-Quelle-**Log-Loss**-Werte weiter unten unter "Performance" kommen trotzdem direkt aus dem
jeweiligen MLflow-Lauf, nicht aus einer Schätzung.{freeze_note}"""


def _render_performance(snapshots: dict[str, ChampionSnapshot]) -> str:
    perf_rows = []
    source_rows = []
    tier_rows = []
    for prefix, snap in snapshots.items():
        label = _MODEL_LABEL[prefix]
        run_cell = f"`{snap.run_id}`" if snap.run_id else NA_NO_CHAMPION
        if snap.missing or snap.metric_value is None or snap.naive_value is None:
            perf_rows.append(
                f"| {label} | {NA_MISSING} | {NA_MISSING} | {NA_MISSING} | {run_cell} |"
            )
        else:
            improvement = (
                snap.improvement
                if snap.improvement is not None
                else (snap.naive_value - snap.metric_value)
            )
            perf_rows.append(
                f"| {label} | {_de(snap.metric_value)} | {_de(snap.naive_value)} | "
                f"{_de(improvement)} | {run_cell} |"
            )
        if snap.per_source:
            for source, value in snap.per_source:
                source_rows.append(f"| {label} | `{source}` | {_de(value)} |")
        else:
            source_rows.append(f"| {label} | -- | {NA_MISSING} |")

        for tier, value in snap.per_tier:
            tier_rows.append(f"| {label} | `{tier}` | {_de(value)} |")

    if tier_rows:
        tier_table = "\n".join(tier_rows)
        tier_section = f"""

### Pro-Tier-Aufschlüsselung (nur wenn dieser Lauf sie protokolliert)

| Modell | Tier | Log-Loss |
|---|---|---:|
{tier_table}"""
    else:
        tier_section = (
            f"\n\nPro-Tier-Aufschlüsselung: {NA_MISSING} (kein "
            f"`{_PER_TIER_METRIC_PREFIX}*`-Metrik auf einem der Champion-Läufe protokolliert)."
        )

    perf_table = "\n".join(perf_rows)
    source_table = "\n".join(source_rows)
    return f"""## Performance gegen die einfache Grundrate

| Modell | Log-Loss | Grundrate | Verbesserung | Run-ID |
|---|---:|---:|---:|---|
{perf_table}

### Pro-Quelle-Aufschlüsselung (aus dem jeweiligen Champion-Lauf selbst)

| Modell | Quelle | Log-Loss |
|---|---|---:|
{source_table}{tier_section}"""


def _render_calibration(snapshots: dict[str, ChampionSnapshot]) -> str:
    lines = []
    for prefix, snap in snapshots.items():
        label = _MODEL_LABEL[prefix]
        if snap.calibration:
            values = ", ".join(f"`{name}` = {_de(value)}" for name, value in snap.calibration)
            lines.append(f"- **{label}:** {values}")
        else:
            lines.append(f"- **{label}:** {NA_UNMEASURED}")
    body = "\n".join(lines)
    return f"""## Kalibrierung

{body}"""


def _render_versionierung(
    snapshots: dict[str, ChampionSnapshot],
    reason: str,
    corpus_fp: str | None,
    corpus_commit: str | None,
) -> str:
    champion_lines = []
    lineage_lines = []
    for prefix, snap in snapshots.items():
        label = _MODEL_LABEL[prefix]
        if snap.missing:
            champion_lines.append(f"`{snap.registered_name}` -> {NA_NO_CHAMPION}")
        else:
            champion_lines.append(
                f"`{snap.registered_name}` Run `{snap.run_id}` (Version {snap.version}, "
                f"registriert am {snap.registered_on})"
            )
        fp = snap.corpus_fingerprint or NA_MISSING
        commit = snap.git_commit or NA_MISSING
        lineage_lines.append(f"{label}: Fingerabdruck {fp}, Commit {commit}")
    champion_line = ", ".join(champion_lines)
    lineage_line = "; ".join(lineage_lines)

    context_sentence = ""
    if corpus_fp or corpus_commit:
        context_sentence = (
            f" Zum Vergleich, der Korpus-Stand von HEUTE laut `docs/epa-refinement-2026-10.md` "
            f"(nicht zwingend die Trainingsbasis der obigen Champion-Läufe): Fingerabdruck "
            f"`{corpus_fp or NA_MISSING}`, Commit `{corpus_commit or NA_MISSING}`."
        )

    return f"""## Versionierung

- **Champion-Alias (aktuell produktiv):** {champion_line}.
- **Warum dieser Champion:** {reason}
- **Korpus-Fingerabdruck / Commit dieses jeweiligen Laufs:** {lineage_line}.{context_sentence}
- Jede Modellversion bleibt für immer im Registry erhalten -- eine Beförderung ersetzt nie eine
  ältere Version, sie verschiebt nur, welche Version aktuell "Champion" heißt."""


def render_card(snapshots: dict[str, ChampionSnapshot], refinement_doc_text: str) -> str:
    stand = datetime.now(tz=timezone.utc).date().isoformat()
    reason = _champion_reason(refinement_doc_text)
    corpus_fp, corpus_commit = _current_corpus_context(refinement_doc_text)
    gate_status = _extra_point_fix_status(refinement_doc_text)
    gate_section = _render_gate_status(gate_status)

    header = f"""# EPA/WP-Modellkarte

**Stand:** {stand}. Automatisch generiert von `scripts/render_model_card.py` aus dem
MLflow-Registry-Stand und committeten Referenz-CSVs zum Generierungszeitpunkt -- nie von Hand
eingetragen. Eine Seite, für dich als Head Coach -- keine Spielernamen, keine Personendaten.
Details, Herleitung und alle committeten Zahlen dahinter stehen in
`docs/epa-refinement-2026-10.md` (inkl. Nachtrag)."""

    sections = [
        header,
        _INTRO_PURPOSE,
        _INTRO_INPUTS,
        _render_trainingskorpus(snapshots),
        _render_method(snapshots, gate_status is not None),
        _render_performance(snapshots),
        _render_calibration(snapshots),
    ]
    if gate_section:
        sections.append(gate_section)
    sections.extend(
        [
            f"## Bekannte Grenzen\n\n{_KNOWN_LIMITS_STATIC}",
            _render_versionierung(snapshots, reason, corpus_fp, corpus_commit),
            _PLATFORM_STATIC,
            _USAGE_STATIC,
            _NEXT_STEPS_STATIC,
        ]
    )
    return "\n\n".join(sections) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("ffep.toml"), help="Path to ffep.toml")
    parser.add_argument(
        "--out", type=Path, default=Path("docs/epa-modellkarte.md"),
        help="Output path for the generated German model card",
    )
    args = parser.parse_args(argv)

    config = load_config(args.config)
    mlflow_store.configure(config)
    client = MlflowClient()

    snapshots = {
        prefix: load_champion_snapshot(prefix, config, client) for prefix in ("ep", "wp")
    }

    refinement_doc_text = (
        REFINEMENT_DOC.read_text(encoding="utf-8") if REFINEMENT_DOC.exists() else ""
    )

    card_text = render_card(snapshots, refinement_doc_text)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(card_text, encoding="utf-8")

    for prefix, snap in snapshots.items():
        state = snap.run_id or NA_NO_CHAMPION
        print(f"{prefix}_model: champion={state}")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
