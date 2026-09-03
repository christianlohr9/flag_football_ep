---
phase: M3-01-hc-workbook-ingest
plan: 03
subsystem: ingest
tags: [polars, hc-workbook, game-identity, dedupe-prep, canonical-schema, pii]

# Dependency graph
requires:
  - phase: M3-01-hc-workbook-ingest plan 02
    provides: "ingest/hc_workbook.py reader (read_sheet_rows, segment_blocks, map_block_to_frame, HcBlock, HcIngestNotices, hc_source_label, slugify)"
provides:
  - "data/reference/hc_games.csv + reference.load_hc_games: maintained (workbook, sheet, block_key) -> game_id/teams/competition/season/tier mapping, hc- prefixed ids, duplicate/tier/prefix validation"
  - "ingest/hc_workbook.py: HcGameSlice, HcGameIdentity, segment_games, resolve_game_identity, count_result_tokens, ingest_workbook -- the full canonical derivation chain for one (workbook, sheet)"
affects: [M3-01-04 (dedupe, pipeline/CLI wiring, real hc_games.csv rows from the first real run)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Game segmentation within a block: numeric splits on a non-increasing PLAY #, pair splits on a team-pair change; block_key = b{block_index:02d}-g{game_index:02d}, stable and block-scoped"
    - "Identity resolution never raises: a (workbook, sheet, block_key) miss degrades to a provisional hc-<workbook>-<sheet>-<block_key> id plus one actionable notice, never an abort (map_teams deliberately not reused here)"
    - "Reuse, don't fork: ingest_workbook imports and calls ingest.hudl's derive_identity_columns/parse_result_tokens/derive_outcome_columns/derive_drive_id/derive_yards_gained_first_down unchanged, in hudl's own order"
    - "PII discipline via a dedicated notices field: HcIngestNotices.unmapped_players carries raw labels with a never-render docstring; every human-readable message reports only counts"

key-files:
  created: []
  modified:
    - data/reference/hc_games.csv
    - src/flag_football_ep/reference.py
    - src/flag_football_ep/ingest/hc_workbook.py
    - tests/test_reference.py
    - tests/test_ingest_hc_workbook.py

key-decisions:
  - "segment_games returns (list[HcGameSlice], list[str]) rather than the plan's abbreviated list[HcGameSlice] -- matches every other function in this module (read_sheet_rows, segment_blocks, map_block_to_frame all return messages alongside data) and is required to honor the plan's own instruction to record an unparseable-PLAY#-forces-boundary finding in a message; ingest_workbook aggregates it into HcIngestNotices.messages exactly like every other segmentation-time finding."
  - "A pair block's frame carries two synthetic columns (hc_pair_team1/hc_pair_team2) that a numeric block's frame never has; ingest_workbook drops them per game slice before concatenation so every game frame in a sheet shares one schema (the plan's own concat invariant) -- the raw labels are still available via HcGameSlice.source_team1/source_team2 for identity resolution and stay out of the canonical frame entirely."
  - "posteam/defteam for a numeric-block row follow hudl's own ODK convention (posteam = home_team when ODK=='O', away_team for every other non-null ODK including 'K', null when ODK itself is null) rather than a new rule, so the kickoff override's play_type-only semantics stay consistent with the reused hudl chain."

requirements-completed: [HC-01]

# Metrics
duration: 95min
completed: 2026-09-03
---

# Phase M3-01 Plan 03: Game Segmentation, Identity Resolution, Canonical Derivation Summary

**`data/reference/hc_games.csv` + `load_hc_games`, `segment_games`/`resolve_game_identity` for provisional-id-safe game identity, and `ingest_workbook` converging one HC sheet onto the canonical schema by reusing `ingest.hudl`'s RESULT/drive/scoring chain unchanged.**

## Performance

- **Duration:** 95 min
- **Started:** 2026-09-03T~18:40Z
- **Completed:** 2026-09-03T~20:15Z
- **Tasks:** 3 (all TDD RED/GREEN)
- **Files modified:** 5 (0 created new files beyond the plan's own `hc_games.csv`; all five are the plan's owned files)

## Accomplishments
- `data/reference/hc_games.csv` (14-column header, no data rows yet -- plan M3-01-04 fills it after the first real run) + `load_hc_games`, following `load_competition_tier` line by line: rejects duplicate `(workbook, sheet, block_key)` triples, duplicate `game_id`s, tiers outside `COMPETITION_TIERS`, and `game_id`s not prefixed `hc-`, each naming the offending value(s)
- `segment_games`: splits a numeric block wherever `PLAY #` does not increase (reset, decrease, or an unparseable/null cell, which always forces a boundary rather than being compared against a stale reference); splits a pair block wherever the case-insensitive, whitespace-trimmed `(team1, team2)` pair changes; produces stable `b{block_index:02d}-g{game_index:02d}` block keys
- `resolve_game_identity`: filters the maintained `hc_games` frame on `(workbook, sheet, block_key)`; a hit returns the mapped identity, a miss returns a provisional `hc-<workbook>-<sheet>-<block_key>` id plus exactly one notice naming the block key, physical row range, play count, and (for a pair block) the raw team labels -- never raises, `map_teams` deliberately not reused here per HC-D04
- `ingest_workbook(path, sheet, contract, hc_games, player_mapping) -> tuple[pl.DataFrame, HcIngestNotices]`: reads, segments, resolves identity, stamps constants (`source`, `competition`, `season`, `game_id`, `game_date`, `home_team`, `away_team`, `result_raw`), synthesizes `PLAY #` for pair-block rows (1-based position within the game), derives `posteam`/`defteam` (null for pair-block rows pending Frage 2), then runs the **unmodified** `ingest.hudl` chain (`derive_identity_columns` -> `parse_result_tokens` -> `derive_outcome_columns` -> ODK `"K"` override -> `derive_drive_id` -> `half`=null -> `add_scoring_play_team` -> `add_score_columns` -> `derive_yards_gained_first_down`), then `map_players` under the coarse `"hc_workbook"` source key, `count_result_tokens`, and `conform_to_canonical` -- returns a `CANONICAL_COLUMNS` frame with `source = hc_workbook:<file>:<sheet>`
- `HcIngestNotices` extended with `unmapped_players: list[str]` (PII, never-render docstring, only its length may appear in a message) and `result_token_counts: dict[str, int]` (every token observed in `RESULT`, contract vocabulary or not)
- An entirely empty sheet (or one with no usable blocks) returns a zero-row `CANONICAL_COLUMNS` frame plus a notice instead of raising, built directly from `CORE_COLUMNS`/`NULLABLE_EXTRAS`
- 43 new tests (15 `test_reference.py`, 28 `test_ingest_hc_workbook.py`), all against synthetic in-process `pl.DataFrame`/`openpyxl.Workbook()` fixtures with synthetic names (`Spieler A`/`B`/`C`, jersey `25`, `Alphaland`/`Betaland`) -- no real workbook opened, no real player/team name referenced

## Task Commits

Each task followed TDD RED/GREEN:

1. **Task 1 RED: failing hc_games.csv loader tests** - `bf72cd4` (test)
1. **Task 1 GREEN: hc_games.csv + load_hc_games** - `ec19a71` (feat)
2. **Task 2 RED: failing game-segmentation/identity-resolution tests** - `36dbb41` (test)
2. **Task 2 GREEN: segment_games + resolve_game_identity** - `cc31282` (feat)
3. **Task 3 RED: failing ingest_workbook tests** - `db20d73` (test)
3. **Task 3 GREEN: ingest_workbook canonical derivation chain** - `a46445c` (feat)

## Files Created/Modified
- `data/reference/hc_games.csv` - 14-column header (`workbook,sheet,block_key,source_team1,source_team2,game_id,home_team,away_team,competition,season,game_date,tier,corpus_game_id,note`), no rows yet
- `src/flag_football_ep/reference.py` - `_HC_GAMES_SCHEMA`, `load_hc_games`
- `src/flag_football_ep/ingest/hc_workbook.py` - `HcGameSlice`, `HcGameIdentity`, `segment_games`, `resolve_game_identity`, `count_result_tokens`, `ingest_workbook`, plus `HcIngestNotices.unmapped_players`/`result_token_counts`
- `tests/test_reference.py` - 15 `load_hc_games` tests (`-k hc_games`)
- `tests/test_ingest_hc_workbook.py` - 28 new tests: `-k game_segmentation` (7), `-k provisional_game` (5, plus one CSV round-trip), `count_result_tokens` (2), `ingest_workbook` end-to-end/`player_identity_mixed_type`/`synthesized_play_id` and others (13)

## Decisions Made
- **`segment_games` returns `(list[HcGameSlice], list[str])`, not the plan text's abbreviated `list[HcGameSlice]`.** Every other function in this module returns messages alongside data (`read_sheet_rows`, `segment_blocks`, `map_block_to_frame`), and the plan's own action text requires "a null/unparseable `PLAY #`... record that in a message" -- there is no other place for that message to originate. `ingest_workbook` folds it into `HcIngestNotices.messages` like every other finding.
- **Synthetic pair-block columns (`hc_pair_team1`/`hc_pair_team2`) are dropped per game slice before concatenation.** A pair block's frame has two more columns than a numeric block's frame (added by `map_block_to_frame`'s `_null_pair_block_tail`); without dropping them, `pl.concat(..., how="vertical")` raises a `ShapeError` the first time a sheet mixes both kinds -- exactly the scenario the plan's own end-to-end test exercises. The raw team labels remain available via `HcGameSlice.source_team1`/`source_team2` for identity resolution, so nothing is lost.
- **`posteam`/`defteam` for numeric-block rows follow hudl's exact ODK convention** (`posteam = home_team` when `ODK == "O"`, `away_team` for every other non-null `ODK` including `"K"`) rather than inventing a new rule, so the `"K"`-kickoff override (which only changes `play_type`) stays semantically consistent with the reused hudl derivation chain.

## Deviations from Plan

None beyond the two documented "Key Decisions" above, both Rule 1/Rule 3 in character (a literal reading of the plan's abbreviated `segment_games` signature would have silently dropped a required finding; the un-dropped synthetic columns would have made the plan's own end-to-end scenario raise). No architectural changes, no new files beyond the plan's own `data/reference/hc_games.csv`, no scope creep.

**Total deviations:** 0 formal deviations (Rule 1-3 auto-fixes only, both documented as key decisions since they concern public function signatures/schema shape rather than internal bugs).
**Impact:** Both auto-fixes were necessary for correctness (message reporting completeness, concat schema-invariant) and are internal implementation details -- the plan's documented public interface (`ingest_workbook(path, sheet, contract, hc_games, player_mapping) -> tuple[pl.DataFrame, HcIngestNotices]`) is unchanged.

## Issues Encountered
- The full, unrestricted `uv run pytest tests -q` run (as opposed to the plan's scoped `test_ingest_hc_workbook.py`/`test_reference.py`/`test_canonical.py` verification, which is green) is heavy and slow in this worktree: it ran past 10 minutes of CPU time with no failures observed through ~53% of the suite (all passes/skips) before being terminated to avoid blocking phase completion -- consistent with plan M3-01-02's SUMMARY, which documented the same collection failures for `cv2`/`supervision`/`torch` (the `cv` extras group, not installed here per this plan's `file_collision_guard`) and a long-running M2 model-training test tail, both unrelated to this plan's five owned files.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Public surface plan M3-01-04 wires: `ingest_workbook(path, sheet, contract, hc_games, player_mapping) -> tuple[pl.DataFrame, HcIngestNotices]`; `HcIngestNotices` now also carries `unmapped_players` (PII, never-render) and `result_token_counts`; `load_hc_games(path) -> pl.DataFrame` with the 14-column schema documented in its docstring; `block_key` format `b{block_index:02d}-g{game_index:02d}`.
- `data/reference/hc_games.csv` has zero data rows -- plan M3-01-04's real run against `data/raw/hc_files/` will surface the actual `(workbook, sheet, block_key)` triples via `resolve_game_identity`'s provisional-id notices, which a maintainer then transcribes into the CSV.
- Dedupe (HC-D03, `corpus_game_id`) and `ffep ingest`/CLI wiring are explicitly out of scope here and remain plan M3-01-04's job, as the plan's objective states.
- No blockers for M3-01-04.

---
*Phase: M3-01-hc-workbook-ingest*
*Completed: 2026-09-03*

## Self-Check: PASSED

- All 5 modified files confirmed present on disk (`data/reference/hc_games.csv`, `src/flag_football_ep/reference.py`, `src/flag_football_ep/ingest/hc_workbook.py`, `tests/test_reference.py`, `tests/test_ingest_hc_workbook.py`).
- All 6 task commits (`bf72cd4`, `ec19a71`, `36dbb41`, `cc31282`, `db20d73`, `a46445c`) confirmed in `git log`.
- Re-ran `uv run pytest tests/test_ingest_hc_workbook.py tests/test_reference.py tests/test_canonical.py -q`: all pass (0 failures).
- Re-ran `-k "end_to_end or player_identity_mixed_type or synthesized_play_id"`: 3/3 pass.
- `grep -rn "hc_files" tests/test_ingest_hc_workbook.py` returns nothing.
- `grep -c "unmapped_players" src/flag_football_ep/ingest/hc_workbook.py` is 7 (non-zero); the field's docstring on `HcIngestNotices` states the never-render rule.
- `git diff --name-only` against the plan's base commit lists exactly the five owned files, no more.
