# Pipeline

Operational reference for `flag_football_ep` (CLI name `ffep`): how to run the pipeline,
what lives where under `data/`, how to maintain the reference CSVs, and what the
validation/quarantine report means. Every command below was run successfully against real
data during phase 01.2 (see "Real-run baseline" at the end of each relevant section).

For the data contract itself (column model, `RESULT` vocabulary, filename convention), see
`docs/data-contract.md`. For the IFAF/cpx.studio field mapping, see
`docs/ifaf-field-mapping.md`.

## 1. Quickstart

```bash
# Install (uv manages the virtualenv from pyproject.toml/uv.lock)
uv sync

# Secrets: copy the template and fill in real values. .env is git-ignored; never commit it.
cp .env.example .env
# edit .env: SPORTAPP_API_KEY=..., CPX_API_KEY=... (CPX_API_KEY is optional -- see section 6)

# Fetch raw snapshots (writes JSON under data/raw/sportapp/, data/raw/ifaf/; safe to skip
# if you already have snapshots on disk -- ingest always reads from disk, never the network)
ffep fetch-sportapp
ffep fetch-ifaf

# Ingest every configured source into the canonical Parquet dataset
ffep ingest

# Train EP and/or WP (writes an MLflow run under mlruns/ for each)
ffep train --model both

# Score the canonical dataset with the most recently trained models
ffep score

# Or run the whole chain (ingest -> train -> score) in one command, skipping fetch by default
ffep run
```

All six subcommands accept `--config` to point at a config file other than the checked-in
`ffep.toml` (mainly useful for tests); every invocation below assumes the default.

**Real-run baseline (phase 01.2-17, this worktree):** `ffep run` on the sources available
locally (legacy `data_raw.csv`, the grandfathered `legacy-sportapp` WC24 corpus, real IFAF
snapshots; Hudl empty by worktree design, sportapp.fi live fetch blocked on the unrotated key
-- see section 8) ingested 257 games / 21,437 plays (10 quarantined, 6 accepted-with-warnings)
and trained + scored both models in **1.79 seconds total** (ingest 0.39s, train_ep 1.04s,
train_wp 0.26s, score 0.10s). Comfortably inside the phase-1.4 ten-minute goal already, with
Hudl and live sportapp.fi still to be added.

## 2. Data layout

```
data/
  raw/
    hudl/       # new-format Hudl exports (git-ignored: player names + BLITZ are PII)
    sportapp/   # sportapp.fi JSON snapshots (ffep fetch-sportapp writes here)
    ifaf/       # IFAF/cpx.studio JSON snapshots (ffep fetch-ifaf writes here)
    legacy/     # grandfathered data_raw.csv, wc24_pbp.csv, pbp_wc24_static.xlsx
  processed/    # canonical Parquet + validation report (git-ignored: rebuilt per ingest)
  reference/    # hand-maintained CSVs (tracked -- see section 3)
```

- **`data/raw/hudl/`** is git-ignored (`.gitignore`: `data/raw/hudl/*`, `!.gitkeep`) because
  new-format exports carry player names and `BLITZ` person names -- real PII. Only the
  directory marker is tracked; drop real exports here locally, they never get committed.
- **`data/raw/sportapp/`** and **`data/raw/ifaf/`** are tracked. `ffep fetch-*` downloads raw
  JSON here once (snapshot-first: ingest always reads from disk, never re-hits the network),
  so ingest is reproducible offline and immune to silent upstream API changes.
  `data/raw/ifaf/` already ships real WM-2026 snapshots (32 games) captured during phase 01.2.
- **`data/raw/legacy/`** is tracked: `data_raw.csv` (47 hand-charted games, the bulk of the
  training corpus, cannot be re-exported), `wc24_pbp.csv` (the WC24 PBP corpus, ingested via
  the `legacy-sportapp` fallback path), `pbp_wc24_static.xlsx` (the original static WC24
  export, kept as a raw input alongside its derived CSV).
- **`data/processed/`** is git-ignored and rebuilt atomically on every `ffep ingest`:
  `plays.parquet`, `games.parquet`, and a timestamped `validation-report-*.md` plus a
  `validation-report-latest.md` symlink-equivalent (latest copy). Regenerable output is
  never committed -- only the reference data and code that produce it are versioned.
- **`data/reference/`** is tracked: the four maintained reference CSVs (section 3) plus
  `roster.csv` (moved from the repo root in plan 01.2-17; cross-source player-identity
  mapping against it is deferred to phase 1.4, decided on real report need).
- **`mlruns/`** (repo root, git-ignored) is the local MLflow tracking store -- section 7.

## 3. Reference-file maintenance

All four files live under `data/reference/`, are comma-delimited with a header row (no
metadata rows), and are loaded by `flag_football_ep.reference` with an explicit
`schema_overrides` -- a bad hand-edit fails loudly at load time rather than silently
mistyping a column.

| File | Schema | Consequence of a missing row |
|---|---|---|
| `half_boundaries.csv` | `filename,half2_first_play` | The affected game's `half` column cannot be derived past halftime; `half_assigned` fails for it (quarantined, or WARN for a warn-only source). |
| `final_scores.csv` | `game_id,home_team,away_team,home_score,away_score,note` | `score_reconstruction` is **skipped** (not failed) for that game -- the check has no ground truth to compare against, so it neither passes nor blocks ingestion. |
| `team_mapping.csv` | `source,source_team,canonical_team` | `map_teams` raises `UnmappedTeamError` naming every unmapped label, source and affected column -- ingest **hard-fails** for the whole source, not just one game. This is the one reference gap that is never silent or partial. |
| `sportapp_games.csv` | `source_game_id,competition,season,note` | `ffep fetch-sportapp` (no `--game-ids`/`--games-file`) reads its game-id list from this file; a missing row means that game is simply never fetched -- no error, just absence. |

`roster.csv` (`data/reference/roster.csv`) is maintained but not yet wired into any loader --
cross-source player identity mapping is deferred to phase 1.4 per `01.2-CONTEXT.md`.

Workflow to add a row: open the CSV, append a line matching the schema above, save. No
regeneration step -- every loader reads the file fresh on each `ffep` invocation.
`team_mapping.csv` additionally rejects duplicate `(source, source_team)` pairs and
`final_scores.csv` rejects duplicate `game_id`s, both at load time.

## 4. Validation and quarantine semantics

`ffep ingest` runs six checks per `game_id` (`flag_football_ep.validation.checks`):

| Check | PASS condition |
|---|---|
| `downs_range` | every `down` is 0..4, never null |
| `yardline_range` | every `yardline_50` is in [0, 50] |
| `half_assigned` | every `half` is 1 or 2 (needs a `half_boundaries.csv` row) |
| `monotonic_drive_ids` | `drive_id` starts at 1 and never decreases across ascending `play_id` |
| `gapless_play_ids` | `play_id` is exactly 1..N with no gaps or duplicates |
| `score_reconstruction` | the last row's cumulative score matches `final_scores.csv` (aligned by team code, not position) |

**Quarantine** means: a game with any FAIL is excluded from `plays.parquet`/`games.parquet`
entirely, and listed under "Quarantined games" in the run's Markdown report with the failing
check(s) and a short reason. The run continues -- one bad game never aborts the whole ingest.
Fix the export (or the reference data), re-run.

**Legacy is warn-only.** `data_raw.csv`'s 47 games run through the same six checks, but any
FAIL is downgraded to WARN and the game's rows stay in the dataset -- it cannot be
re-exported and is the bulk of the training corpus, so quarantining it would silently shrink
history that cannot be recovered. WARN still shows up in the report so the finding is never
hidden, it just never blocks.

The report lands next to the Parquet: `data/processed/validation-report-<run-id>.md` (one
per run) plus `data/processed/validation-report-latest.md` (always the most recent). Format:
a header with the run id and games-ingested/quarantined/warned counts, a "Quarantined games"
section, a per-game pass/fail/skip/warn table (one row per game, one column per check), and a
"Missing reference data" section per game. `notebooks/pipeline_demo.ipynb` reads and displays
the quarantine section as a live example.

**Report sections beyond quarantine.** Two more sections are always rendered, even when
there is nothing to report, so a reader can tell "no notices" apart from "notices not
rendered": `## Source notices` covers source-level messages -- a whole source directory
missing, an exception raised during a source's dispatch, a grandfathered snapshot fallback
used -- and prints `None.` when empty. `## Skipped files` covers a file that produced zero
rows at all -- a bad filename, a wrong delimiter, a missing core column -- naming the file
and the reason, including the raised exception's class name. Both sections land in the same
Markdown report next to the per-game sections, are echoed to the console during the run
(`notice: {text}`), and show up in `ffep ingest`/`ffep run`'s stdout.

**Per-file containment.** One bad export never removes the rest of its source. A non-numeric
charted cell in `DN`, `DIST`, `YARD LN` or `PLAY #` is cast to a null with `strict=False`
rather than raising, so the game still ingests: the bad cell becomes a null
`down`/`yards_to_go`/`yardline_50`/`play_id` plus a domain notice, and the game then fails
the relevant per-game check (`downs_range` for a null `down`, `gapless_play_ids` for a null
`play_id`) exactly like any other bad value -- it never disappears from the report as if the
whole source were missing.

**The one deliberate exception.** An unmapped team code still aborts the whole source
loudly instead of degrading into a per-file notice: it signals a gap in the reference data
(`data/reference/team_mapping.csv`), not a per-export data-quality issue, and the operator
must add the mapping there before the source can ingest at all.

**Cross-source `play_type`.** Every source converges on one canonical `play_type`
vocabulary -- `run`, `pass`, `no_play`, `qb_kneel`, `extra_point`, `kickoff`, or null for an
unparsed play -- so a downstream filter can rely on the column without knowing which source
a row came from. Each source's own wording (e.g. sportapp.fi's raw "rush") stays in
`result_raw`, never in `play_type`.

**Real-run baseline:** the phase 01.2-17 run above quarantined 10 games, all IFAF, all
`downs_range` failures from null `down` values on penalty/PAT plays -- a real property of
that feed, confirmed not a validation bug. 6 games (5 `legacy-sportapp`, 1 `legacy`) carried
warnings on a warn-only source and were still accepted.

## 5. Adding a new export

New-format Hudl exports follow the filename contract from `docs/data-contract.md`:
`YYYY-MM-DD_{TEAM1}-vs-{TEAM2}_{COMP}.csv` (fallback: `YYYY_{TEAM1}-vs-{TEAM2}[_{COMP}][_n].csv`).
Core columns (always required): `PLAY #`, `ODK`, `DN`, `DIST`, `YARD LN`, `PLAY TYPE`,
`RESULT`, `GN/LS`. Everything else is an optional charting field (formation, route, blitz,
etc.) that lands as a nullable extra in the canonical schema if recognized, or is dropped
(reported as `dropped_unknown`) if not.

First-run checklist for a new export:
1. Drop the file(s) under `data/raw/hudl/` (git-ignored -- never committed).
2. Confirm both teams have a `team_mapping.csv` row for `source=hudl` (unmapped teams hard-fail).
3. If the game crosses halftime, add its `half_boundaries.csv` row before running ingest, or
   expect `half_assigned` to fail for it.
4. Run `ffep ingest --source hudl` (or the default, which ingests every source) and read the
   validation report for that game.
5. If it quarantines, the report names the exact check and offending rows/values -- fix the
   export or the reference data, not the pipeline.

## 6. Configuration reference (`ffep.toml`)

| Key | Meaning |
|---|---|
| `[paths] data_root` | Base directory all other `raw_*`/`processed`/`reference` paths resolve under (currently `data`, but each path is independently resolvable). |
| `[paths] raw_hudl`, `raw_sportapp`, `raw_ifaf`, `raw_legacy` | Per-source raw input directories (section 2). |
| `[paths] processed` | Where `ffep ingest`/`ffep score` write `plays.parquet`, `games.parquet`, `plays_scored.parquet`, and the validation report. |
| `[paths] reference` | Directory holding the reference CSVs (section 3). |
| `[paths] models` | Directory for `--export-pkl`'s dated `.pkl` export (MLflow, not this directory, is the primary artifact store -- section 7). |
| `[paths] mlruns` | Local MLflow tracking-store directory. |
| `[paths] contract` | Path to `docs/data-contract.schema.json`, the machine-readable contract the validator tolerates a version bump on. |
| `[reference] half_boundaries`, `final_scores`, `team_mapping`, `sportapp_games` | Paths to the four reference CSVs (section 3). |
| `[sources.sportapp] base_url` | sportapp.fi API base URL. |
| `[sources.sportapp] api_key_env` | Name of the environment variable `ffep fetch-sportapp` reads the API key from (`.env`, never the TOML -- secrets are never committed). |
| `[sources.ifaf] base_url` | cpx.studio API base URL. |
| `[sources.ifaf] tournament` | Default tournament identifier for `ffep fetch-ifaf` (overridable with `--tournament`). |
| `[sources.ifaf] api_key_env` | Environment variable name for the optional cpx.studio API key -- the discovered endpoints have not required one so far. |
| `[train] ep_experiment`, `wp_experiment` | MLflow experiment names `ffep train`/`ffep score` log to and resolve runs from. |
| `[train] exclude_games_ep`, `exclude_games_wp` | Canonical `game_id`s excluded from training, ported verbatim from the notebook's `game_id != 37` / `!= 35` filters (`models/ep_model.ipynb` cell 3, `models/wp_model.ipynb` cell 3) -- these were single-game holdouts in the original notebook, not a methodology choice; phase 1.3 revisits holdout strategy under GroupKFold. |

Paths are resolved relative to `ffep.toml`'s own directory, not the process's working
directory, so `ffep` behaves the same regardless of where it is invoked from.

## 7. Model tracking (MLflow)

Every `ffep train` run (EP and WP separately) logs params, metrics and the fitted XGBoost
model as its own MLflow run under `mlruns/` (local `file:` store; `MLFLOW_ALLOW_FILE_STORE`
is set automatically since mlflow>=3.15 deprecates the file store by default). Nothing ever
overwrites a previous run's artifact the way the notebook's fixed `ep_model.pkl` dump did.

Browse runs with:

```bash
uv run mlflow ui --backend-store-uri file:./mlruns
```

`ffep score` resolves which model to use via `flag_football_ep.model.score.resolve_run`: an
explicit `--ep-run`/`--wp-run` (validated as a plain hex run id) wins, otherwise the most
recent `FINISHED` run of the configured experiment is used. This "most recent finished run"
lookup is the interim equivalent of a model registry; the actual MLflow registry (a friendlier
"latest model" API, plus REQ-S1-11's versioning-by-date-and-training-data-hash) is phase 1.3's
job, built on this foundation.

`--export-pkl` on `ffep train` additionally writes a dated, hash-suffixed `.pkl` under
`[paths] models` for compatibility with existing consumers of the notebook's
`{ep,wp}_model.pkl` convention -- MLflow is the primary store; this export is a placeholder
phase 1.3 replaces with the full versioning scheme.

## 8. Known limitations

- **`half_seconds_remaining` is synthetic**, not a real game clock: it is derived as
  `1200 / max(play_id_half)` per half (`flag_football_ep.features.mutations.prepare_wp_data`)
  because real Hudl clock data has not been delivered (REQ-S1-02 pending). It is not part of
  the canonical `plays.parquet` schema at all -- it exists only transiently inside WP
  training/scoring. `notebooks/wp_model_demo.ipynb` plots WP against play sequence, not real
  time, and says so explicitly; treat any WP chart the same way until REQ-S1-02 lands.
- **The data contract is provisional.** `docs/data-contract.schema.json` (v1.1) was fixed
  unilaterally because the Videoanalyst has been unavailable (`DEFERRED-ANALYST`, tracked as
  a pending todo); the validator is built to tolerate a contract version bump once it is
  ratified, but every derivation rule in this document reflects the unratified v1.1.
- **PAT baselines are hard-coded.** `PAT_BASELINE_ONE_POINT = 0.5` and
  `PAT_BASELINE_TWO_POINT = 0.92` (`flag_football_ep.features.mutations`) are the notebook's
  original fixed assumptions (1-pt try from the 5, 2-pt try from the 10), ported as-is.
  REQ-S1-10 (phase 1.3) replaces them with empirical break-even estimates computed from the
  data.
- **`SPORTAPP_API_KEY` rotation is still pending** (STATE.md blocker, unchanged by this plan):
  the key that was previously committed in plaintext remains compromised in git history even
  after plan 01.2-17 deleted the two notebooks (`api_call.ipynb`, `api_fuzzing.ipynb`) that
  still contained it in plaintext -- deleting them is repo hygiene, not remediation. `ffep
  fetch-sportapp`/`ffep run --no-skip-fetch` fail cleanly with a clear "environment variable
  not set" error rather than silently using the old key, until the key is rotated with the
  provider and `.env` is updated.
