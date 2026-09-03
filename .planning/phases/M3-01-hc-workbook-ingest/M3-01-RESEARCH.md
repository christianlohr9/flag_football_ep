# Phase M3-1: HC-Workbook-Ingest - Research

**Researched:** 2026-09-03
**Domain:** Excel-to-canonical ingest (openpyxl/polars), Hudl-shaped `RESULT` grammar reuse, cross-source dedupe, PII-safe player-identity mapping
**Confidence:** MEDIUM — the ingest/contract/validation architecture to reuse is HIGH confidence (read directly from the codebase); the three HC workbooks' actual row-level structure is HIGH confidence for two of three `Data` tabs and LOW/contested for the third (see Open Questions #1). No official docs apply here (this is bespoke spreadsheet archaeology), so every workbook-structure claim below is `[VERIFIED: openpyxl inspection this session]`, not from any external source.

## Summary

The three HC workbooks are **not** a single, clean tabular export each — they are years of copy-pasted Hudl-shaped charting sessions accumulated inside hand-maintained Excel files, and one of the three (`Germany Analytics Stats EC 2025 vs WC Nations.xlsx`) has an entirely **empty** `Data` tab in the copy currently on disk, despite the HC's own notes saying he always charts play-by-play there. The other two workbooks (`Offense Analytics 2026 Camps and Competitions.xlsx`, `Scoring Probability by Situation 2023-2026.xlsx`) do contain real play rows, and their columns overlap substantially with the existing Hudl contract's core vocabulary (`DN`, `DIST`, `YARD LN`, `RESULT`, `GN/LS`, `ODK`) — confirming HC-D01's "Hudl-export-shaped" premise. But `Scoring Probability`'s `Data`/`Copy of Data` tabs are internally inconsistent: roughly 1 in 6 rows have **team names instead of `PLAY #`/`ODK`** in columns 1–2 (a different charting-era layout pasted into the same sheet under one header row), and the player-identity columns (`RECEIVED BY`, `Thrown By`) mix player **names** and **jersey numbers** in the same column across different blocks. This is materially messier than what a Hudl-CSV-shaped `ingest/hudl.py`-style reader can handle unmodified; it needs a dedicated cell-level reader (openpyxl, not a bulk `pl.read_excel(engine="calamine")` call) plus explicit block-boundary and dtype-based validation.

`openpyxl` (MIT) is the right reader for the `Data`/`Copy of Data` tabs specifically, because the phase needs `data_only=True` formula resolution, `#N/A`-residue detection, and row-by-row type inspection that a fast rectangular-table reader (`fastexcel`/calamine) is not designed to expose cleanly. `openpyxl` and `fastexcel` both passed the slopcheck legitimacy gate (`[OK]`) and are absent from `pyproject.toml` today — the project has never read `.xlsx` programmatically before (the one existing raw `.xlsx`, `data/raw/legacy/pbp_wc24_static.xlsx`, is not parsed in code; only its hand-derived `wc24_pbp.csv` sibling is ingested).

**Primary recommendation:** build `ingest/hc_workbook.py` following the existing `ingest/hudl.py`/`ingest/legacy.py` pattern (own module, own mutation chain, converges only at `canonical.conform_to_canonical`), using `openpyxl.load_workbook(path, data_only=True, read_only=True)` to read each `Data`/`Copy of Data` tab into row dicts, segmenting rows into blocks by column-1 dtype (numeric `PLAY #` vs. string team-name pair) before applying any column mapping, and routing every RESULT/player-identity ambiguity into `IngestNotices` (never a silent guess) — with a `checkpoint:human-verify` for the HC on the specific structural ambiguities this research could not resolve from data alone (Open Questions #1–#3). Treat the `Germany Analytics` workbook's `Data` tab as **empty pending HC clarification**, not as a source with zero games (those are different failure modes for the report).

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Excel cell-level parsing (openpyxl) | Ingest module (`ingest/hc_workbook.py`) | — | Mirrors `ingest/hudl.py`'s ownership of its raw vocabulary; no other tier touches raw Excel cells |
| Column mapping to contract v1.1 core/extras | Ingest module | `canonical.py` (`conform_to_canonical` convergence) | Source-specific renaming stays in the source module; only the final `select` onto `CANONICAL_COLUMNS` is shared |
| Game-identity resolution (`hc_games.csv`) | Reference data (`data/reference/`) | Ingest module (lookup) | New maintained CSV, same pattern as `half_boundaries.csv`/`team_mapping.csv` — data, not code, drives the mapping |
| Dedupe against existing Hudl/IFAF plays | Ingest orchestration (`pipeline.py` or a dedicated pre-write step) | Ingest module (fingerprint columns) | Cross-source concern — must see both HC rows and already-ingested rows, so it cannot live inside the single-source `ingest/hc_workbook.py` module |
| Player-identity mapping (name/jersey → canonical) | `reference.py` (`map_players`, extended `player_mapping.csv`) | `data/reference/roster.csv` (jersey lookup seed) | Existing machinery already tolerates unmapped labels gracefully (report-only, never raises) — reuse, do not fork |
| Per-source validation / quarantine | `validation/checks.py` + `pipeline.run_ingest`'s `partition_games` | Ingest module (notices) | The six checks are source-agnostic; HC-typed games just need `hc_workbook` added to (or kept out of) `warn_only_sources` per HC-D05 |
| PII discipline (never printed/committed) | Every tier touching player columns | — | Cross-cutting; enforced by convention (testing.py synthetic factories, .gitignore) not a single component |

## User Constraints (from CONTEXT.md)

<user_constraints>

### Locked Decisions

- **HC-D01 Reuse, don't fork:** the HC `Data` tabs are Hudl-export-shaped; the ingest goes through `flag_football_ep.ingest.hudl`-style contract mapping (column aliases: `YARD LN`→`yardline_50` derivation, `AIR YARDS`/`Air Yards`, `BF Action`, `Hand`, `Efficiency` as optional extras), with a dedicated `ingest/hc_workbook.py` reader (openpyxl or polars-excel) that yields one frame per workbook+sheet and records `source = hc_workbook:<file>:<sheet>`.
- **HC-D02 PII discipline:** player names in the position/target columns are mapped through `data/reference/roster.csv` / `player_mapping.csv` (extend the maintained CSVs where names are new); raw names never appear in reports, docs, tests or commits. Fixtures use the `testing.py` frame factories with synthetic names. The workbooks stay gitignored (done 2026-09-03).
- **HC-D03 Dedupe preference (HC's own words):** "wir reichern eher unsere Daten um seine an … Duplikate bei ihm erkennen und nicht berücksichtigen; sonst mit der Doppelung leben". Detect duplicates by (game identity → `game_id` mapping via team names + date/competition; `PLAY #`/`play_id`) first, content fingerprint (DN, DIST, YARD LN, RESULT, GN/LS, RECEIVED BY, Thrown By) second; report overlap counts per game; exclude HC duplicates of our Hudl plays; keep HC-only games (EC 2025, camps, 2023–2024 history) as new games with validation.
- **HC-D04 Game identity:** HC data lacks our `game_id` filename convention; build a `data/reference/hc_games.csv` mapping (workbook, sheet, team pair, date/competition → `game_id`, competition tier) as a maintained CSV; unknown games get provisional ids and a loud warning (existing reference-CSV pattern).
- **HC-D05 Validation honesty:** run the existing six per-game checks; HC-typed camps/scrimmages may legitimately fail score reconstruction — quarantine partition + report, not silent drop.
- **HC-D06 Deliverable for M3-2:** one canonical Parquet (existing `plays.parquet` path/versioning) with a `source` column, plus a German `docs/hc-workbook-ingest.md` recording per-source counts, duplicates found, games added, and open mapping questions for the HC.

### Claude's Discretion

- Reader library choice (openpyxl vs polars/calamine), sheet-detection heuristics, handling of the "Scoring Probability" header offset, fingerprint tolerance.
- Whether the HC analysis tabs (`EP`, `SP by D&D`) are read now for M3-2's comparison (recommended: extract them read-only into `data/reference/hc_sp_tables/` as CSV snapshots for M3-2) or left to M3-2.

### Deferred Ideas (OUT OF SCOPE)

- Web app replacing the Excel (BL-02); automated stat collection from CV (BL-03); game clock OCR (BL-01).

</user_constraints>

## Phase Requirements

<phase_requirements>

| ID | Description | Research Support |
|----|-------------|------------------|
| HC-01 | Parse the HC's Hudl-like `Data` tabs and map them to data contract v1.1 | Confirmed which HC columns match contract core/optional columns exactly (case-sensitive) vs. need extras-renaming vs. are genuinely new (§ "Mapping to Contract"); confirmed the three workbooks are structurally inconsistent with each other and, in one case, internally inconsistent — a single fixed column-index mapping will not work for `Scoring Probability`'s `Data`/`Copy of Data` tabs (§ "Workbook Structure Findings", Pitfalls 1–4) |
| HC-02 | Dedupe HC rows against existing Hudl/IFAF plays per HC's stated preference, validate per source | Confirmed current `plays.parquet` has **zero rows from the `hudl` source** (only 2 files exist under `data/raw/hudl/`, contributing nothing to the committed `games.parquet` snapshot at research time) — so today's HC↔Hudl overlap is empty by definition; the real overlap to detect is HC-workbook-internal duplication (the two `Scoring Probability` tabs, `Data` and `Copy of Data`, both contain a `GER vs. IRE`-shaped block spanning similar row ranges — see Pitfall 4) and HC↔IFAF overlap (WC 2026 games); documented the six validation checks' expected pass/fail shape for camp/scrimmage-typed HC games (§ "Validation") |

</phase_requirements>

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `openpyxl` | 3.1.x (`[OK]` slopcheck; MIT license) | Cell-level `.xlsx` reading with `data_only=True` formula resolution, `read_only=True` streaming for large sheets (SP workbook's `Data`/`Copy of Data` run to ~6,000/8,000 physical rows) | Only library in the polars/pandas ecosystem giving per-cell dtype + formula-vs-cached-value control needed to detect the block-boundary and formula-residue issues found in this research; `pl.read_excel`'s own `openpyxl` engine wraps the same library when finer control is needed later |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `polars` | already a dependency (>=1.5.0) | Assemble the row-dicts openpyxl yields into a DataFrame, then run the same `conform_to_canonical` / `map_teams` / `map_players` pipeline every other source uses | Always — openpyxl is only the file-reading boundary, never a DataFrame library on its own |
| `fastexcel` | 0.21.x (`[OK]` slopcheck; MIT license, Rust `calamine` bindings) | NOT recommended for the `Data`/`Copy of Data` tabs (see rationale below); could be used later for the ~28 read-only analysis-tab snapshots (`EP`, `SP by D&D`, `Player Analysis All Camps`) if those turn out to be clean rectangular tables, since it is dramatically faster than openpyxl for that shape | Only for tabs confirmed clean/rectangular; verify per-tab before switching |

**Installation:**
```bash
uv add openpyxl
```
Do not add `fastexcel` unless a specific analysis-tab snapshot step needs its speed — keep the core dependency surface minimal per the project's existing `cv`/`versioning` extras-group discipline (pyproject.toml comments already document this philosophy for other additions).

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| openpyxl cell-level read | `pl.read_excel(engine="calamine")` (bulk rectangular read) | Much faster, but calamine assumes one consistent header/column layout per sheet; it cannot express the row-dtype-based block segmentation this phase needs for `Scoring Probability`'s `Data`/`Copy of Data` tabs (confirmed: same header row, but rows ~2–661 use team-name-pair columns 1–2 while rows ~662+ use numeric `PLAY #`/`ODK` — a single schema-inference pass over the whole sheet cannot represent this) |
| openpyxl cell-level read | `pl.read_excel(engine="xlsx2csv")` | Converts to CSV first; would silently collapse the `#N/A` formula-residue cells and the ragged row widths in ways that are harder to inspect than raw cell objects |
| Manual pre-cleaning in Excel (HC re-exports clean CSVs per game) | — | Would solve the structural mess at the source, but is out of this phase's scope (this phase must ingest the workbooks as they exist today) and depends on the HC's time; worth raising as a longer-term recommendation in the delivered `docs/hc-workbook-ingest.md`, not a blocker for M3-1 |

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `openpyxl` | PyPI | long-established (10+ years; used across the Python ecosystem as the reference `.xlsx` read/write library) | very high | `foss.heptapod.net/openpyxl/openpyxl` (mirrored on GitHub/PyPI) | `[OK]` | Approved |
| `fastexcel` | PyPI | actively maintained (`ToucanToco/fastexcel`), version installed this session: 0.21.0 | high | `github.com/ToucanToco/fastexcel` | `[OK]` | Approved (not required for this phase's core scope; documented as a future option only) |

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

Both packages were checked with `slopcheck install openpyxl fastexcel` this session; the scan itself completed and reported `2 OK` before the tool's own post-scan `pip install` step failed locally (no `pip` binary on this machine's `PATH` — `uv`-only environment). The scan result is unaffected by that unrelated failure. Package name provenance: `openpyxl` and `fastexcel` are both training-data-known names, additionally confirmed installable and importable in this session (`uv run --with openpyxl` / `--with fastexcel` succeeded, `fastexcel.read_excel(...).sheet_names` returned real sheet names) — tag as `[VERIFIED: this session's install + import + slopcheck]` rather than `[ASSUMED]`, since both official-source confirmation (successful import against the real files) and slopcheck concur.

## Architecture Patterns

### System Architecture Diagram

```
data/raw/hc_files/*.xlsx (3 workbooks, gitignored, PII)
        |
        v
ingest/hc_workbook.py
  1. openpyxl.load_workbook(path, data_only=True, read_only=True)
  2. per (workbook, "Data"|"Copy of Data" sheet):
       a. read header row -> compare against contract core/optional/unknown
       b. row-by-row: classify block by col-1 dtype
            numeric  -> PLAY#/ODK block (Offense Analytics shape, SP rows ~662+)
            string   -> team-name-pair block (SP rows ~2-661; NOT PLAY#/ODK)
            all-None -> empty tab (Germany Analytics Data tab -- flag, do not silently skip)
       c. per block: apply the block's own column-index mapping (validated by
          dtype, not header text alone) -> row dicts
  3. assemble polars DataFrame from row dicts
        |
        v
Column mapping to contract v1.1 (mirrors ingest/hudl.py's _CHARTING_RENAME)
  DN, DIST, YARD LN, RESULT, GN/LS, ODK  -> contract core columns (name match)
  OFF FORM, OFF PLAY, OFF STR, TARGET ROUTE, HASH -> optional/extras (existing renames)
  BF Action, Hand, Efficiency, Air Yards, Drive Success,
  Blitz*, Tackle, Pass Breakup, D Target -> new extras (not yet in canonical.NULLABLE_EXTRAS)
        |
        v
Player-identity resolution (reference.map_players)
  RECEIVED BY / Thrown By / QB columns hold BOTH text names and jersey numbers
  (confirmed in the same header-labeled column across different pasted blocks)
  -> extend player_mapping.csv with source="hc_workbook" entries keyed by
     BOTH name-string and jersey-number-string, seeded from roster.csv's
     player_jersey column
        |
        v
Game-identity resolution (data/reference/hc_games.csv, new maintained CSV)
  (workbook, sheet, team1, team2, date/competition) -> game_id, competition_tier
  unmapped game -> provisional id + loud warning (existing reference-CSV pattern)
        |
        v
canonical.conform_to_canonical(df, source="hc_workbook")
        |
        v
Dedupe (new step, cross-source: needs the already-ingested hudl/ifaf frames too)
  1. game-identity match against existing games.parquet (team pair + date/competition)
  2. within matched games: content fingerprint (DN, DIST, YARD LN, RESULT, GN/LS,
     RECEIVED BY, Thrown By) -> exclude HC rows that duplicate existing rows
  3. HC-only games (no game-identity match) -> kept as new games
        |
        v
validation.checks.run_checks + partition_games (existing six checks, per HC-D05
HC-typed camps/scrimmages may legitimately FAIL score_reconstruction -- decide
warn_only_sources membership for "hc_workbook" per HC-D05, do not silently drop)
        |
        v
plays.parquet / games.parquet (existing atomic-write path, source column
distinguishes hc_workbook:<file>:<sheet> per HC-D01) + docs/hc-workbook-ingest.md
(German-language report per HC-D06)
```

### Recommended Project Structure

```
src/flag_football_ep/ingest/
├── hc_workbook.py         # new: openpyxl read, block segmentation, column mapping, own IngestNotices
data/reference/
├── hc_games.csv            # new: (workbook, sheet, team pair, date/competition) -> game_id, tier
├── player_mapping.csv       # extended: source="hc_workbook" rows, name AND jersey-number keys
data/reference/hc_sp_tables/ # new (Claude's Discretion, recommended): read-only CSV snapshots of
                              # the HC's own EP/SP-by-D&D tabs, for M3-2's comparison
docs/
├── hc-workbook-ingest.md   # new (German): per-source counts, duplicates found, games added, open questions
```

### Pattern 1: Block segmentation by column-1 dtype, not by header text

**What:** Before applying any header-driven column mapping, classify every data row of a `Data`/`Copy of Data` tab by whether column 1 holds a number (a real `PLAY #`) or a string (a team name, meaning the row belongs to a different, older charting-era layout).
**When to use:** Any time a sheet's header row cannot be trusted to describe every row beneath it — confirmed necessary for `Scoring Probability by Situation 2023-2026.xlsx`'s both `Data` tabs.
**Example:**
```python
# Source: this session's inspection (data/raw/hc_files/Scoring Probability by
# Situation 2023-2026.xlsx :: Data, data_only=True read)
for row in ws.iter_rows(min_row=2, values_only=True):
    col1 = row[0]
    if isinstance(col1, (int, float)):
        # PLAY#/ODK block -- header columns 1-2 match the data here
        play_num, odk = col1, row[1]
    elif isinstance(col1, str):
        # team-name-pair block -- header says "PLAY #"/"ODK" but the cells
        # actually hold (team1, team2); there is no PLAY # in this block at
        # all -- synthesize play_id from row order within the block instead
        team1, team2 = col1, row[1]
    else:
        # blank row inside the used range -- skip, do not treat as data
        continue
```

### Pattern 2: dtype-validated column mapping, never header-text-only

**What:** After segmenting by block, validate that a mapped column's values actually look like what the header claims (e.g. `RESULT` values must be in the extended token vocabulary; `RECEIVED BY`/`Thrown By` values must be either a plausible name string or an integer matching a `roster.csv` `player_jersey`) before trusting the mapping for that block.
**When to use:** Every block of every HC `Data`/`Copy of Data` tab — this is the direct mitigation for the header/data misalignment found in `Scoring Probability`'s `Data` tab (§ Pitfall 2).
**Example:**
```python
# Sketch -- not yet implemented. Mirrors validation/schema.py's
# check_column_domains, applied per-block instead of per-file.
def looks_like_result(value: str) -> bool:
    tokens = value.split(", ")
    return all(t in EXTENDED_RESULT_VOCAB for t in tokens)
```

### Pattern 3: reuse `reference.map_players`/`map_teams` unmodified

**What:** Do not write a new player/team mapping function. `reference.map_players` already tolerates unmapped labels by leaving them in place and returning them in `PlayerMappingResult.unmapped` for the caller to warn on (deliberately different from `map_teams`, which raises) — this is exactly the HC-D02 "never breaks, always reports" behavior this phase needs.
**When to use:** For `RECEIVED BY`, `Thrown By`, `QB`, `BLITZ`, `D Target`, `Tackle`, `Pass Breakup` columns once renamed onto their canonical extras names.
**Example:**
```python
# Source: src/flag_football_ep/reference.py (already in the codebase)
result = map_players(df, player_mapping, source="hc_workbook", columns=["received_by", "thrown_by", "qb"])
df = result.frame
if result.unmapped:
    notices.append(f"unmapped HC player label(s): {result.unmapped}")
```

### Anti-Patterns to Avoid

- **Trusting the header row as ground truth for the whole sheet:** confirmed wrong for `Scoring Probability`'s `Data` tab — the header text and the actual cell contents diverge for roughly 17% of rows. A single `pl.read_excel(...)` call with `has_header=True` would silently produce a corrupted frame (team names cast into a `PLAY #` int column, `RECEIVED BY` and `GN/LS`/`Thrown By`/`YAC` swapped for those rows) without raising anything.
- **Assuming `values_only=True` (data_only) results are always current:** `Germany Analytics Stats EC 2025 vs WC Nations.xlsx`'s aggregate tabs (`Player Stats All Games`, `Player Stats vs USA`, etc.) show real cached numbers computed by formulas referencing the `Data` tab, but the `Data` tab's referenced cells are now blank — the cached values are stale, not derived from anything currently in the file. Do not treat those aggregate tabs as validation ground truth for a currently-populated `Data` tab; they prove the tab **used to** hold data, not that it does now.
- **Silently coercing jersey numbers into the same column as names without a typed union:** `RECEIVED BY`/`Thrown By` hold both types across different blocks of the same workbook. Cast to `Utf8` early (openpyxl gives Python `int`/`float`/`str`/`None` per cell) and let `map_players`'s lookup key be the string form (`"25"` for jersey 25, `"<SURNAME>"` for a name), not a premature int/string branch that could raise on mixed input.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Reading `.xlsx` cell values, formula vs. cached value, `#N/A` detection | A custom zip/XML parser for `.xlsx` | `openpyxl` | `.xlsx` is a zipped OOXML format with a real spec; openpyxl already handles shared-string tables, styles, formula caching, and `read_only` streaming correctly |
| RESULT-token parsing, drive/scoring derivation, score reconstruction | A second, independent implementation for HC rows | `ingest/hudl.py`'s existing `parse_result_tokens`/`derive_outcome_columns`/`add_scoring_play_team`/`add_score_columns` (import and reuse, extending `_BASE_TOKENS`/`_MODIFIER_TOKENS` if the contract is amended — see Pitfall 3) | These functions are already tested against the 13-token contract grammar; forking them for HC rows would create two RESULT-parsing implementations to keep in sync forever |
| Player/team label normalization | A new fuzzy-matching or ML-based name-resolution library | `reference.map_players`/`map_teams` + hand-maintained CSVs | The project's whole reference-data philosophy (§ `docs/pipeline.md` section 3) is explicit, auditable CSV mappings over automatic fuzzy matching — a new dependency here would contradict that established pattern for no proven benefit at this data volume (a few hundred distinct player labels) |
| Cross-source duplicate detection | A generic dedupe/record-linkage library (e.g. `recordlinkage`, `dedupe`) | The HC-D03-specified two-stage match (game-identity first, then a fixed content fingerprint on `DN`/`DIST`/`YARD LN`/`RESULT`/`GN/LS`/`RECEIVED BY`/`Thrown By`) | HC-D03 already locks the exact matching strategy; the corpus is small (hundreds of games, tens of thousands of plays) and the match keys are well-defined, so a general-purpose record-linkage library would add a heavy, unnecessary dependency (several of these pull in scikit-learn-adjacent stacks) for a problem this project has already specified deterministically |

**Key insight:** every "don't hand-roll" here already exists in this codebase. This phase's actual engineering risk is not missing tooling — it is the workbook structure itself (§ Pitfalls). Budget planning time there, not on picking libraries.

## Common Pitfalls

### Pitfall 1: `Germany Analytics Stats EC 2025 vs WC Nations.xlsx`'s `Data` tab is empty

**What goes wrong:** The tab has the right header (`O`, `D`, `OFF FORM`, `BF Action`, `OFF PLAY`, `DN`, `DIST`, `YARD LN`, `RESULT`, `TARGET ROUTE`, `RECEIVED BY`, `GN/LS`, …) but every one of its 2,506 physical data rows has `None` in every one of those columns (verified at rows 2, 100, 500, 1000, 1500, 2000, 2500, 2506). Only three formula columns (`EP`/`EP After`/`EPA`, added at columns AE–AG, referencing a `VLOOKUP` keyed on the blank `DN`/`DIST`/`YARD LN` cells) still evaluate, and they all resolve to `#N/A` because their lookup key is built from blank cells. Meanwhile the workbook's aggregate tabs (`Player Stats All Games`, `Player Stats vs USA`, etc.) still show real, non-zero cached numbers — proving the `Data` tab held real rows at some point.
**Why it happens:** Unknown from data alone — either (a) the copy handed off for this phase had its `Data` tab intentionally cleared before sharing (plausible, given how seriously this project treats player PII), (b) an accidental deletion, or (c) this specific file is stale/superseded and the real current EC2025 play-by-play now lives consolidated in one of the other two workbooks instead.
**How to avoid:** Do not silently ingest 0 rows from this file and call it done — the plan must include a `checkpoint:human-verify` asking the HC directly whether this file's `Data` tab should have rows, and if so, requesting a re-export. Do not attempt to "reconstruct" play-by-play from the cached aggregate numbers; those are lossy summaries, not row-level data.
**Warning signs:** Any ingest run reporting 0 plays for `hc_workbook:Germany Analytics...:Data` alongside non-zero counts for the other two workbooks.

### Pitfall 2: `Scoring Probability`'s `Data`/`Copy of Data` tabs mix two incompatible row layouts under one header

**What goes wrong:** Roughly 653 of ~3,878 (`Data`) and 653 of ~4,000 (`Copy of Data`) data rows have a team name (e.g. `"Germany"`, `"Ireland"`) in the column the header labels `PLAY #`, and the opposing team name in the column labeled `ODK`. In those rows, columns 3–11 (`OFF FORM` through `TARGET ROUTE`) still line up correctly with the header, but from column 12 onward (`RECEIVED BY`, `GN/LS`, `Thrown By`, `YAC`, `QB`) the values show a mix of names and numbers inconsistent with a single, fixed shift — e.g. a `Thrown By`-labeled cell holding `25.0` (plausibly a jersey number or air-yards value) next to a `YAC`-labeled cell holding a player surname. The remaining rows (e.g. verified from row 662 onward in `Data`) have a real numeric `PLAY #`/`ODK` in columns 1–2 that matches the header, but `RECEIVED BY` in those rows sometimes holds a small integer (a jersey number, consistent with `roster.csv`'s `player_jersey` column) rather than a name string.
**Why it happens:** This is years (2023–2026) of Hudl-shaped charting sessions pasted into one sheet under one static header row, across charting eras that used different column layouts and different player-identity conventions (name vs. jersey number) — the same root cause `docs/data-contract.md` already documents for raw Hudl exports ("no fixed export preset... seven real exports, seven different headers"), just pasted into Excel instead of kept as separate CSV files.
**How to avoid:** Never treat this sheet as one schema. Segment rows by column-1 dtype (Pattern 1), and within each block, validate every mapped column by dtype/domain before trusting it (Pattern 2) — e.g. a `RESULT`-mapped column must contain only extended-vocabulary tokens; a `DN`-mapped column must be an int 0–4. Flag any block where dtype-validation fails for `checkpoint:human-verify` with the HC rather than guessing the shift.
**Warning signs:** `RESULT` domain-check failures concentrated in specific row ranges rather than scattered randomly; `DN`/`DIST`/`YARD LN` casts producing an unusually high null rate in some sub-range of one tab.

### Pitfall 3: HC `RESULT` vocabulary has at least 6 tokens beyond the current 13-token contract

**What goes wrong:** Distinct `RESULT` values observed across `Offense Analytics` and both `Scoring Probability` tabs include `Block`, `Blocked, Def TD`, `Dropped`, `Timeout`, `Batted Down`, `Offsetting Penalties`, and one clearly corrupt cell (`-5.0`, a number where a RESULT string is expected). None of `Block`/`Dropped`/`Timeout`/`Batted Down`/`Offsetting Penalties` are in the contract's 13-token vocabulary (`Rush`, `KNEEL`, `Sack`, `Interception`, `Complete`, `Incomplete`, `Good`, `No Good`, `Fumble`, `Penalty` + modifiers `TD`, `Def TD`, `Safety`, `Penalty`). `KNEEL` itself was never observed in any HC row (consistent with the existing legacy corpus, where it also appears 0 times). The already-valid multi-token combos (`Complete, TD`, `Rush, TD`, `Interception, Def TD`, `Sack, Safety`) all parse correctly against the existing exact-token grammar.
**Why it happens:** The HC's own charting vocabulary evolved independently of the Hudl-export contract this project ratified from seven *Hudl* sample exports — his workbooks were never in scope for that August 2026 sighting.
**How to avoid:** `ingest/hudl.py`'s `parse_result_tokens` already handles unknown tokens gracefully (records them in `tok_unknown`, never raises) — reuse it as-is for a first pass, but do not let the new tokens silently fall into `play_type = None`/`pass` catch-alls unnoticed. Given the C-07 precedent (the project's own process for RESULT-vocabulary amendments), treat this as requiring an explicit contract v1.2 amendment decision, not a silent extension — surface the six new tokens' row counts prominently in `docs/hc-workbook-ingest.md` for HC-D06.
**Warning signs:** `IngestNotices.messages` containing "unknown RESULT token(s)" for `hc_workbook` at a materially higher rate than the near-zero rate seen from real Hudl exports.

### Pitfall 4: the two `Scoring Probability` tabs (`Data` and `Copy of Data`) overlap

**What goes wrong:** Both tabs contain rows for the same `Germany vs. Ireland`-shaped block (confirmed: `Data` rows 2–~30 and `Copy of Data` rows ~461–475+ both show `Germany`/`Ireland` team-name pairs with matching-looking `OFF FORM` sequences like `DOG`, `SPREAD`, `TRIPS`). The sheet name `"Copy of Data"` itself signals this — it is very likely a literal Excel-generated duplicate of (part of) `Data`, not an independent charting session.
**Why it happens:** Manual Excel workflow — copying a sheet to preserve a snapshot before editing, then never fully reconciling the two.
**How to avoid:** Treat `Data` and `Copy of Data` as two candidate sources within the *same workbook* and run the HC-D03 dedupe logic (game-identity + content fingerprint) between them too, not only against the existing Hudl/IFAF corpus — HC-02's per-source dedupe requirement applies within this workbook, not just across workbooks.
**Warning signs:** A large fraction of `Copy of Data` rows fingerprint-matching `Data` rows for games in the row-2-through-~30 / row-461-through-~480 ranges specifically.

### Pitfall 5: `PLAY #` and other "core" numeric columns arrive as Python `float`, not `int`

**What goes wrong:** openpyxl returns numeric Excel cells as Python `float` (e.g. `PLAY # = 1.0`, `DN = 1.0`), not `int` — confirmed throughout this session's row dumps. A naive `isinstance(x, int)` dtype check anywhere in the block-segmentation or validation logic will silently miscategorize every real numeric cell.
**Why it happens:** Excel stores all numbers as IEEE 754 doubles internally; openpyxl surfaces that faithfully rather than inferring intent.
**How to avoid:** Use `isinstance(x, (int, float))` for "is this cell numeric" checks (as done throughout this research's probe scripts), and cast to `Int32` non-strictly downstream via polars (mirroring `hudl.derive_identity_columns`'s `strict=False` pattern) rather than relying on Python-level int/float distinction.
**Warning signs:** A block-segmentation heuristic that classifies 100% of numeric rows as "string/team-name" rows (the opposite of the real ~83%/17% split found this session) — a strong sign the numeric check used `isinstance(x, int)` alone.

## Code Examples

### Reading a `Data` tab with formula resolution and error detection

```python
# Source: this session's inspection scripts (verified against the real files)
import openpyxl

wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
ws = wb["Data"]
header = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
for row in ws.iter_rows(min_row=2, values_only=True):
    if all(v is None for v in row):
        continue  # blank row inside the used range -- do not treat as data
    # row[i] is None (empty), int/float (numeric or a resolved formula
    # result), str (text -- including the literal string '#N/A' for an
    # unresolved formula error), or datetime.datetime (seen in the SP
    # workbook's "SP by D&D Clustered" analysis tab, not the Data tabs)
```

### Extending `player_mapping.csv` with jersey-number keys

```python
# Sketch, mirroring the existing player_mapping.csv schema
# (source, source_player, canonical_player) -- source_player becomes the
# stringified jersey number for rows where the HC charted a number instead
# of a name, seeded from roster.csv's player_jersey column per team.
# hc_workbook,25,<canonical name for jersey 25 on the relevant roster team>
# hc_workbook,<SURNAME>,<canonical name>
```

## State of the Art

Not applicable in the usual "library X replaced library Y" sense — this is a one-off bespoke-spreadsheet ingest, not a domain with an evolving standard tool. The one relevant precedent inside this codebase: every prior source (`hudl`, `legacy`, `sportapp`, `ifaf`) converges only at `canonical.conform_to_canonical`, never shares mutation code — this phase should not be the first to break that pattern by trying to unify with `ingest/hudl.py` beyond importable helper functions (`parse_result_tokens`, `derive_outcome_columns`, `add_scoring_play_team`, `add_score_columns`).

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The `Germany Analytics` workbook's empty `Data` tab is a deliberate PII-scrub or a stale/superseded file, rather than evidence the HC never actually charted this workbook the way his notes describe | Pitfall 1, Open Questions | If the real answer is "the HC expects us to reconstruct plays from the aggregate `Player Stats...` tabs," the plan would need an entirely different (and much lossier) ingest strategy for this file |
| A2 | The team-name-pair rows in `Scoring Probability`'s `Data`/`Copy of Data` tabs represent an older, different Hudl-export-era layout (not, e.g., a deliberate different charting protocol the HC still actively uses) | Pitfall 2, Architecture Pattern 1/2 | If wrong, the "numeric PLAY# block = current, string block = legacy" framing in the plan could misprioritize which block's semantics to nail down first |
| A3 | Numeric values in `RECEIVED BY`/`Thrown By`-labeled cells are jersey numbers matching `roster.csv`'s `player_jersey` column, not some other numeric code (e.g. a play-clock value, a formation slot number) | Pitfall 2, Pattern 3 | If wrong, seeding `player_mapping.csv` with jersey-number keys would silently attribute plays to the wrong player |
| A4 | `Data!O` ("Efficiency", Offense Analytics workbook) is a manually charted binary success flag (0/1, with 2 anomalous `9.0` values and ~18% `#N/A` residue), not a formula-derived value | Code Examples, M3-4 handoff note below | Confirmed this session via `data_only=False` inspection (cell value is literal `0.0`/`1.0`, no formula) — HIGH confidence, listed here only because its exact success-rate *definition* (which down/distance thresholds) is still unknown and would need HC confirmation for M3-3/M3-4, not this phase |

**Note for M3-3/M3-4 handoff (not this phase's scope, but discovered incidentally):** the `Player Analysis All Camps` "Adj Comp %" formula credits a dropped pass (non-blank `Drop` column) as a completion; "Exp Plays"/"Explosive %" in that sheet threshold strictly on `GN/LS > 12` for **passing plays only** (filtered by `Thrown By` match) — it does **not** implement the "OR positive EPA" half of the explosiveness rule the HC described verbally in `docs/hc-notes-2026-09-03.md`. This is a real discrepancy between the spreadsheet's current formula and the HC's stated intent, worth flagging to M3-3's research/planning.

## Open Questions

1. **Is `Germany Analytics Stats EC 2025 vs WC Nations.xlsx`'s `Data` tab supposed to have rows?**
   - What we know: the header is correct and matches the HC's own description; the tab is 100% empty across 2,506 physical rows (checked at 8 sample points spanning the full range); the workbook's aggregate stat tabs (`Player Stats All Games`, per-opponent tabs) show real, non-zero cached numbers proving the tab held data at some point.
   - What's unclear: whether the copy on disk was intentionally stripped before being shared with this project (privacy), whether this is stale/accidental, or whether the real current data lives elsewhere.
   - Recommendation: `checkpoint:human-verify` — ask the HC directly before writing any ingest code against this specific file's `Data` tab; do not spend implementation time guessing.

2. **What is the exact column semantics for `Scoring Probability`'s team-name-pair block rows, from `RECEIVED BY` onward?**
   - What we know: columns 1–11 (through `TARGET ROUTE`) are internally consistent with the header even in team-name-pair rows; from column 12 (`RECEIVED BY`) onward, values show a pattern inconsistent with a single fixed shift (mixed names/numbers in ways that don't cleanly resolve to "shifted by N columns").
   - What's unclear: the true column order for that block — this could not be resolved from data alone within this research session without materially higher confidence of introducing a wrong guess into training data.
   - Recommendation: `checkpoint:human-verify` with the HC, or (cheaper) treat this block's post-`TARGET ROUTE` columns as unmapped/null with a loud notice rather than guessing, accepting a smaller extras surface for those older rows in exchange for not silently corrupting `GN/LS`/passer-identity data.

3. **Should the 6 new RESULT tokens (`Block`, `Dropped`, `Timeout`, `Batted Down`, `Offsetting Penalties`, `Blocked, Def TD`) become a contract v1.2 amendment, or stay as `tok_unknown`?**
   - What we know: they parse safely today via the existing `tok_unknown` fallback (no crash, no silent misclassification — just `play_type = None`/no matching flag); `Block`/`Batted Down` look like pass-breakup-adjacent outcomes, `Timeout` and `Offsetting Penalties` look like non-plays that probably should not carry EP/WP training weight at all.
   - What's unclear: whether the HC (or the project's own C-07 process) wants these formally added to the vocabulary before M3-2 trains on this corpus.
   - Recommendation: surface the six tokens and their row counts explicitly in `docs/hc-workbook-ingest.md` (HC-D06 already requires "open mapping questions for the HC" — this is exactly that); do not block M3-1 on resolving it, since `tok_unknown` already degrades safely.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `openpyxl` | Reading the three `.xlsx` workbooks | ✗ (not in `pyproject.toml` or the synced venv) | — (confirmed installable: `uv run --with openpyxl` succeeded this session, importable, all inspection scripts ran) | Add via `uv add openpyxl` as a core dependency (not `--with`, not a dev-only group) — the ingest module needs it at runtime, same tier as `polars`/`typer` |
| `fastexcel` | Not required for this phase's core scope | ✗ | — (confirmed installable this session: `fastexcel 0.21.0`, `sheet_names` worked) | Not needed unless the analysis-tab CSV-snapshot step (Claude's Discretion) is done via `pl.read_excel(engine="calamine")` instead of openpyxl |
| `data/raw/hc_files/*.xlsx` | The entire phase | ✓ (present, gitignored, verified this session) | 3 files, ~1.4–2.9 MB each | — |

**Missing dependencies with no fallback:** none — `openpyxl` just needs to be added to `pyproject.toml`.
**Missing dependencies with fallback:** `fastexcel` (optional, has a viable openpyxl-based fallback for every use case identified so far).

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest >= 8 (`dependency-groups.dev` in `pyproject.toml`) |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `addopts = "-q"`) |
| Quick run command | `uv run pytest tests/test_ingest_hc_workbook.py -x` (new file, does not exist yet — Wave 0 gap) |
| Full suite command | `uv run pytest` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| HC-01 | Header validated against contract core/optional/unknown, per-block column mapping applied correctly on a synthetic `.xlsx` fixture (built with `openpyxl.Workbook()` in the test, not a real HC file — never commit real workbook bytes or derived fixtures containing real names) | unit | `uv run pytest tests/test_ingest_hc_workbook.py -k header_and_block_mapping -x` | ❌ Wave 0 |
| HC-01 | Block segmentation correctly separates a numeric-`PLAY#` block from a team-name-pair block within one synthetic sheet | unit | `uv run pytest tests/test_ingest_hc_workbook.py -k block_segmentation -x` | ❌ Wave 0 |
| HC-01 | RESULT tokens outside the 13-token contract vocabulary are recorded in notices, never crash, never silently misclassify | unit | `uv run pytest tests/test_ingest_hc_workbook.py -k unknown_result_tokens -x` | ❌ Wave 0 |
| HC-01 | Player-identity columns accept both name-string and jersey-number-string values without raising | unit | `uv run pytest tests/test_ingest_hc_workbook.py -k player_identity_mixed_type -x` | ❌ Wave 0 |
| HC-02 | Game-identity dedupe: an HC row matching an existing game+content fingerprint is excluded; an HC-only game is kept | unit | `uv run pytest tests/test_ingest_hc_workbook.py -k dedupe -x` | ❌ Wave 0 |
| HC-02 | `run_checks`/`partition_games` correctly quarantines or warns HC-typed camp games per HC-D05's `warn_only_sources` decision | integration | `uv run pytest tests/test_pipeline_ingest.py -k hc_workbook -x` (extend existing file) | ❌ Wave 0 (existing file, new cases) |
| HC-01/HC-02 | End-to-end: `ingest_hc_workbook` on a small synthetic multi-block workbook produces a canonical-schema-conformant frame, `IngestResult` includes an `hc_workbook` source, `docs/hc-workbook-ingest.md`-shaped report data is derivable | integration | `uv run pytest tests/test_ingest_hc_workbook.py -k end_to_end -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `uv run pytest tests/test_ingest_hc_workbook.py -x`
- **Per wave merge:** `uv run pytest`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `tests/test_ingest_hc_workbook.py` — new file, covers HC-01/HC-02 unit + integration cases above; build fixture workbooks with `openpyxl.Workbook()` inside the test (synthetic team/player names via `testing.py`-style factories — never load or reference the real gitignored files from a test)
- [ ] `data/reference/hc_games.csv` — new maintained CSV (HC-D04), needs at least one seeded row for a real test fixture to reference by shape (synthetic values only)
- [ ] `player_mapping.csv` test-fixture rows — extend the existing `contract`-style pytest fixture pattern (see `tests/test_ingest_hudl.py`'s `contract` fixture) with a small `source="hc_workbook"` mapping fixture, synthetic names/jerseys only
- [ ] `openpyxl` install: `uv add openpyxl` — currently absent from the synced environment entirely

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | Offline batch pipeline, no auth surface touched by this phase |
| V3 Session Management | no | N/A |
| V4 Access Control | no | N/A |
| V5 Input Validation | yes | Contract-driven header/domain validation (`validation/schema.py`'s `validate_header`/`check_column_domains`, extended per Pattern 2's per-block dtype validation) — untrusted spreadsheet input from a hand-maintained file must never be trusted to match its own header claims (this is the whole substance of Pitfall 2) |
| V6 Cryptography | no | N/A — no secrets/crypto touched by this phase |

### Known Threat Patterns for this phase's stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| PII leakage (player names/jersey identities into git, logs, docs, error messages) | Information Disclosure | Existing `.gitignore` entry for `data/raw/hc_files/`; `testing.py` synthetic-name fixture pattern already established; this research report itself contains zero real player names (redacted at inspection time — see probe script `A4`/`<REDACTED-NAME>` pattern used during this session) — the plan must hold every future artifact (docs, test fixtures, commit messages, quarantine reports) to the same bar |
| Malformed/corrupted input silently accepted (e.g. the `-5.0` RESULT-column data-entry error found this session) | Tampering (of data quality, not adversarial) | `validation/checks.py`'s existing six checks + this phase's own per-block dtype validation (Pattern 2); never let a domain violation pass silently — route to `IngestNotices` per the established project convention |
| Zip-bomb / malicious `.xlsx` payload | Denial of Service | Not a realistic threat model here — these are three specific, known files from a trusted source (the HC), not untrusted user uploads; `openpyxl`'s `read_only=True` mode already streams rather than loading the whole workbook into memory, which is a reasonable proportional mitigation without adding a dedicated scanning step |

## Sources

### Primary (HIGH confidence)

- This session's direct `openpyxl` inspection of all three files under `data/raw/hc_files/` (sheet lists, header rows, row counts, RESULT vocabulary, DN/DIST/YARD LN ranges, formula residue, block-boundary detection) — no external documentation exists for these bespoke files; every structural claim in this document traces to a specific probe script run in this session
- `src/flag_football_ep/ingest/hudl.py`, `ingest/legacy.py`, `ingest/ifaf.py`, `ingest/__init__.py` — existing ingest patterns
- `src/flag_football_ep/canonical.py`, `validation/checks.py`, `validation/schema.py`, `reference.py`, `pipeline.py`, `testing.py` — shared convergence points, validation gate, reference-CSV loaders, test factory conventions
- `docs/data-contract.md`, `docs/data-contract.schema.json` — contract v1.1
- `docs/hc-notes-2026-09-03.md`, `.planning/phases/M3-01-hc-workbook-ingest/M3-01-CONTEXT.md` — HC's stated preferences and locked decisions
- `pyproject.toml` — confirmed `openpyxl`/`fastexcel`/any Excel reader currently absent from dependencies
- `data/processed/games.parquet` (read this session) — confirmed 0 `hudl`-source games in the current committed snapshot (only `legacy`, `legacy-sportapp`, `ifaf`)

### Secondary (MEDIUM confidence)

- `polars.read_excel` docstring (`uv run python -c "help(pl.read_excel)"`) — confirms the `calamine`/`xlsx2csv`/`openpyxl` engine options and their tradeoffs directly from the installed library, not from external web docs

### Tertiary (LOW confidence)

- None used — no WebSearch/WebFetch was needed for this phase; every claim traces to direct inspection of the codebase or the actual HC files.

## Metadata

**Confidence breakdown:**
- Standard stack (openpyxl/fastexcel choice, legitimacy): HIGH — both packages installed, imported, and slopcheck-verified this session
- Architecture (reuse of `ingest/hudl.py` patterns, `canonical.py` convergence, `reference.py` mapping functions): HIGH — read directly from the existing, working codebase
- Workbook structure (`Offense Analytics`, `Scoring Probability` `Data` tab from row ~662 onward): HIGH — directly verified, internally consistent with the header
- Workbook structure (`Scoring Probability`'s team-name-pair block column semantics beyond `TARGET ROUTE`): LOW — confirmed the divergence exists and is not resolvable from data alone; explicitly flagged for `checkpoint:human-verify` rather than guessed
- Workbook structure (`Germany Analytics`'s empty `Data` tab): HIGH confidence that it is empty; LOW confidence on *why*, explicitly flagged as Open Question #1
- Pitfalls/Don't-Hand-Roll: HIGH — derived directly from this session's inspection plus the existing codebase's established patterns

**Research date:** 2026-09-03
**Valid until:** Until the HC's `checkpoint:human-verify` responses arrive (Open Questions #1–#3) — the workbook-structure findings are a snapshot of the specific files on disk today, not a stable external API; re-verify if the HC provides updated/re-exported files before implementation starts.
