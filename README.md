# flag-football-ep

Expected-points (EP) and win-probability (WP) pipeline for a German women's flag football
national team: raw exports (Hudl, sportapp.fi, IFAF/cpx.studio) in, a canonical validated
Parquet dataset and trained/tracked EP/WP models out.

## Install

```bash
uv sync
cp .env.example .env   # fill in SPORTAPP_API_KEY (required to fetch) / CPX_API_KEY (optional)
```

Requires Python >=3.12; `uv` manages the virtualenv and dependencies from
`pyproject.toml`/`uv.lock`.

## Commands

| Command | What it does |
|---|---|
| `ffep ingest` | Ingest raw exports (Hudl, legacy, sportapp.fi, IFAF) into canonical `plays.parquet`/`games.parquet` plus a per-run validation report. |
| `ffep fetch-sportapp` | Download sportapp.fi play-by-play JSON snapshots into `data/raw/sportapp/`. |
| `ffep fetch-ifaf` | Download IFAF/cpx.studio tournament JSON snapshots into `data/raw/ifaf/`. |
| `ffep train` | Train the EP and/or WP models, logging params/metrics/artifact to MLflow (`mlruns/`). |
| `ffep score` | Score the canonical dataset with the trained models, writing `plays_scored.parquet`. |
| `ffep run` | Chain ingest -> train -> score behind one command with per-stage timing. |
| `ffep report` | Chain ingest -> score(champion) -> report, writing the full coaching-report set (opponent, own-team, decisions, WP-review) as offline HTML. |

Full reference (every option, config key, and the validation/quarantine semantics) lives in
[`docs/pipeline.md`](docs/pipeline.md).

## Coaching-Reports

```bash
ffep report
```

Generates the full REQ-S1-16 report set from raw exports in one command: opponent tendency
reports, the own-team efficiency report, the decision cheatsheet and per-game win-probability
review, each a standalone offline HTML file (PDF via the browser's print dialog, no PDF
dependency). See [`docs/coaching-reports.md`](docs/coaching-reports.md) for the full command
reference, the model-promotion discipline, the two maintained reference-file schemas and the
documented discretion decisions.

## Repository map

```
src/flag_football_ep/   # the package: cli, ingest/, features/, model/, validation/, fetch/
notebooks/              # thin demo notebooks (import the package, visualize -- no pipeline logic)
data/
  raw/                  # per-source raw inputs (hudl/ is git-ignored: PII)
  processed/            # canonical Parquet + validation report (git-ignored: rebuilt per run)
  reference/            # hand-maintained CSVs (team mapping, half boundaries, final scores, roster)
tests/                  # pytest suite
docs/
  pipeline.md           # this project's operational reference (CLI, data layout, config)
  data-contract.md       # the data contract: column model, RESULT vocabulary, filename convention
  ifaf-field-mapping.md # IFAF/cpx.studio field mapping to the canonical schema
mlruns/                 # local MLflow tracking store (git-ignored)
```

See [`docs/pipeline.md`](docs/pipeline.md), [`docs/data-contract.md`](docs/data-contract.md)
and [`docs/ifaf-field-mapping.md`](docs/ifaf-field-mapping.md) for the details.

## Development

```bash
uv run pytest tests/ -q          # full suite
uv run pytest tests/test_repo_hygiene.py -q   # guards against sprawl/import-hack regressions
```

`tests/test_repo_hygiene.py` is the automated gate that keeps the repo-root sprawl and the
old `sys.path`/`Python/` helper-import pattern from creeping back in.

## Status

Strand 1 (this repository) is mid-migration: notebook logic has moved into
`src/flag_football_ep`, the ingest/validation/train/score pipeline runs end-to-end via `ffep
run`, and MLflow tracks every training run. Methodology changes (GroupKFold evaluation,
calibration, empirical PAT baselines) land in the next phase -- see `.planning/` for the full
requirements and roadmap. Known limitations (synthetic clock, provisional data contract,
hard-coded PAT baselines) are documented in [`docs/pipeline.md`](docs/pipeline.md#8-known-limitations).
