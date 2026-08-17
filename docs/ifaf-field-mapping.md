# IFAF / cpx.studio field mapping

## Provenance

- **Endpoint:** `https://us.cpx.studio/v1` — `/games`, `/tournaments/{slug}`, `/tournaments/{slug}/teams`, `/games/{id}/unified-plays`, `/games/{id}/events`.
- **Snapshot files:** `data/raw/ifaf/*.json` (99 files), fetched by plan 01.2-07's live discovery run against tournament `ffwc26-women`.
- **Fetch date:** 2026-08-17 (see `.planning/phases/01.2-repo-to-pipeline/01.2-07-SUMMARY.md`).
- **Games observed:** 96 games total on `/games`; 48 matched `tournamentId == "ffwc26-women"`. 42 of the 48 have at least one play; 6 (all involving Nigeria) are `status: "FINAL"` with a genuinely empty `unified-plays` array — real forfeits, not a fetch bug.
- **Plays observed:** 4,057 play objects across the 42 non-empty games (this document's per-field counts are taken over exactly this corpus).
- **Auth:** none required — every request in the live run succeeded with no `Authorization` header and no `CPX_API_KEY`. `CPX_API_KEY` stays optional in `fetch/ifaf.py`.
- **Two distinct `gameId`/game-`id` shapes observed in the same tournament:** a 36-character UUID (21 of 48 games) and a short slug like `ffwc26-wc5` (21 of 48 games) plus 6 forfeits. Both are used verbatim as `source_game_id`; the ingest parser must not assume either shape.

## Mapping table

Evidence tags: `observed` (seen in the snapshot, example value given), `documented` (from CONTEXT.md/the folded todo but not directly seen), `absent` (no IFAF source; canonical column stays null).

| cpx.studio path | canonical column | transform | evidence |
|---|---|---|---|
| `context.gameClockMs` | `game_clock_ms` | direct copy, cast Int64 | `observed` — e.g. `1200000` at the first play of a half (20:00 in ms), decreasing toward `0` |
| `context.half` | `half` | direct copy, cast Int32 | `observed` — only `1` or `2` seen in 4,057/4,057 plays, never missing |
| `context.down` | `down` | direct copy, cast Int32; `0` on PAT plays | `observed` — `{1: 1118, 2: 844, 3: 893, 4: 736, 0: 392}`; missing on 74/4,057 plays (1.8%), counted as a notice, left null |
| `context.yardsToGo` (or `games.json currentContext.yardsToGo`) | `yards_to_go` | **not used** — see below | `observed`, but the observed value is a hardcoded constant `10` on every single occurrence (3,191/4,057 plays where the key is present; the parallel `games.json` `currentContext.yardsToGo` field is also always `10` across all 48 games). This is not real per-play distance-to-go data, so `yards_to_go` is filled null for this source rather than trusting a constant that would silently distort every down/distance-conditioned feature. This directly extends plan 01.2-07's finding (which only checked one game and reported the key as absent) — the key exists but carries no information. |
| `context.ballOn` | `yardline_50` | direct copy, cast Int32, asserted 0..50 | `observed` — see the ballOn-semantics section below |
| `context.possessionTeamId` | `posteam` | `map_teams(df, mapping, source="ifaf", columns=["posteam"])` | `observed` — e.g. `"w-usa"`, `"w-ger"`; matches `homeTeam.id`/`awayTeam.id` in `games.json` for the same game |
| (the other team of `homeTeam`/`awayTeam` in `games.json`) | `defteam` | whichever of `home_team`/`away_team` is not `posteam` | `observed` (derived) |
| `games.json[].homeTeam.id` (mapped via game lookup, joined by `id`) | `home_team` | `map_teams` | `observed` — `games.json` is a flat top-level list; joined to `unified-plays` by matching `id`/`gameId` |
| `games.json[].awayTeam.id` | `away_team` | `map_teams` | `observed` |
| `playNumber` (top-level, not inside `context`) | `play_id` | contiguous 1..N per game, assigned by sorting plays on `playNumber` then enumerating (not by trusting `playNumber` verbatim — gaps exist, e.g. penalty-only entries can share a `playNumber` cluster) | `observed` — every sampled play carries `playNumber`; used as the ordering key, not the stored value |
| (possession change vs. the previous row) | `drive_id` | increments by 1 within a game whenever `context.possessionTeamId` differs from the previous play's, starting at 1 | `observed` (derived) — verified against a real drive: plays 1-3 (USA), play 4 onward (GER) after an interception |
| `outcome.type` | `result_raw`, plus `touchdown`/`def_touchdown`/`safety`/`interception`/`complete_pass`/`incomplete_pass`/`sack`/`one_point_conv_success`/`two_point_conv_success` flags | `OUTCOME_MAP` lookup — see outcome-vocabulary section | `observed` |
| `description.text` (fallback: `description.detail`, `description.label`) | `description` (nullable extra) | direct copy | `observed`; `null` on 726/4,057 plays (17.9%) — some plays (mostly `MIDDLE_LINE`/plain penalty entries) carry no `description` object at all |
| `sequence` | not mapped to a canonical column | dropped after use for context only (no canonical "sequence" column) | `observed` — internal action list, e.g. `PASS`→`COMPLETE`→`FLAG_PULL`; empty (`[]`) on 547/4,057 plays |
| `id` (top-level) | `source_game_id` is the *game* id; the *play*'s own `id` is not separately preserved as a canonical column (no canonical "source_play_id" column exists) | not mapped | `observed` |
| `game_id` (this ingest module's own construction) | `game_id` | `make_game_id("ifaf", source_game_id)` → `"ifaf-<id>"` | `observed` (derived) |
| `tournament.id` (`ffwc26-women`) or its `shortCode` | `competition` | `tournament.name` (`"IFAF World Flag 2026"`) preferred; falls back to the tournament slug if `tournament_path` is not supplied | `observed` — `tournament_ffwc26-women.json` has `name: "IFAF World Flag 2026"`, `shortCode: "FFWC26W"` |
| `tournament.startDate` | `season` | year parsed from `startDate` (`"2026-08-13T..."` → `2026`) | `observed` |
| `tournament.divisions[0]` | `gender` | lowercased (`"Women"` → `"women"`) | `observed` |
| `context.score.home` / `context.score.away` | not copied directly — `home_team_points`/`away_team_points`/`*_score` are re-derived by `canonical.add_score_columns` from the outcome flags, same as every other source | cross-check only | `observed` — the payload's own running score is compared against the reconstructed score per play when both exist, and a mismatch is recorded as a notice (see Task 2 rules); confirmed the final play's `context.score` equals `games.json`'s `currentScore` for a sampled game (`{"home": 35, "away": 34}` both places) |

## Canonical CORE columns with no IFAF source

| column | how filled | validation consequence |
|---|---|---|
| `yards_to_go` | null (see mapping table — the only IFAF field for this is a hardcoded constant, not real data) | any per-game check requiring non-null `yards_to_go` (a `NON_NULL_COLUMNS` entry) fails for `source == "ifaf"` rows; downstream validation (plan 06) must treat `ifaf` as a documented exception or this source cannot pass the core-column completeness gate as currently specified |
| `posteam_after` | left null; nothing in `unified-plays` records the resulting possession team for the *next* play as a same-row field (it is derivable from the next row's `possessionTeamId`, which is out of scope for `flatten_unified_plays`'s per-play mapping) | none — this column is not in `NON_NULL_COLUMNS` |
| `play_type` | left null; IFAF has no explicit rush/pass/kick classification comparable to Hudl's `OFF PLAY` or sportapp's `action_title` (the closest proxy, `outcome.type`, is already captured in `result_raw` and the flag columns) | none — not in `NON_NULL_COLUMNS` |
| `yardline`, `yardline_50_after`, `yardline_50_simple`, `yards_to_go_simple` | left null; no post-play field position and no simplified-bucket fields exist in `unified-plays` | none — none of these are in `NON_NULL_COLUMNS` |

No defense-scheme fields exist in this source (confirmed by the folded todo and the live snapshots: no `COVERAGE`, `DEF FRONT`, or `BLITZ`-equivalent key anywhere in `unified-plays`), so every `NULLABLE_EXTRAS` defense-charting column (`def_front`, `coverage`, `blitz`, etc.) stays null for `source == "ifaf"`, same as it would for sportapp.fi.

## Outcome vocabulary

Distinct `outcome.type` values observed across the 4,057-play corpus, with counts, and the canonical flags each maps to (`OUTCOME_MAP` in `ingest/ifaf.py`):

| `outcome.type` | count | canonical flags set | notes |
|---|---:|---|---|
| `FLAG_PULL` | 1,289 | none (no dedicated flag; the tackle-equivalent event, not a play-ending result on its own in this schema — most `FLAG_PULL` outcomes co-occur with a preceding `COMPLETE`/`RUSH` in `sequence`) | most common value; carries no scoring/turnover signal by itself |
| `INCOMPLETE_PASS` | 924 | `incomplete_pass = 1` | |
| `TOUCHDOWN` | 375 | `touchdown = 1`; additionally `def_touchdown = 1` instead of `touchdown` when `outcome.turnover == True` (14/375 occurrences — a defensive/pick-six touchdown) | |
| `COMPLETE_PASS` | 433 | `complete_pass = 1` | |
| `TURNOVER` | 290 | none of the scoring flags; recorded only in `result_raw` (generic turnover, distinct from `INTERCEPTION`) | `outcome.turnover == True` on all observed instances |
| `None` (key absent from `outcome`) | 262 | no flags set | `outcome` object present but without a `type` key — mid-sequence bookkeeping rows (snap/QB-set-only entries, sequence often empty) |
| `TRY` | 164 | `one_point_conv_success = 1` when `outcome.pointsScored == 1`; no flag set (recorded only in `result_raw`) when the try fails (`description.kind == "TRY_NO_GOOD"`, `pointsScored` absent) | PAT attempt (`down == 0`); 1-point distance |
| `MIDDLE_LINE` | 145 | none | observed only as a mid-sequence marker, no scoring/turnover semantics found in the sampled data |
| `INTERCEPTION` | 57 | `interception = 1` | `outcome.turnover == True` on all observed instances |
| `SACK` | 41 | `sack = 1` | |
| `XP1` | 34 | `one_point_conv_success = 1` | alternate 1-point-try vocabulary, coexists with `TRY`/`pointsScored==1` in the same corpus (not a per-game split — see the provenance note on the two `gameId` shapes) |
| `TD` | 18 | `touchdown = 1` | alternate vocabulary for `TOUCHDOWN`; every observed `TD` play has `turnover == False` and an empty `sequence`/`null description` (summary-only rows), so `TD` is never treated as a defensive touchdown |
| `XP2` | 12 | `two_point_conv_success = 1` | |
| `RUN` | 12 | none | rush play, no scoring/turnover signal by itself |
| `SAFETY` | 1 | `safety = 1` | single observed instance; `outcome.type` was `SAFETY` with `turnover == True` |

**Unmapped values:** none observed outside the table above. Any future `outcome.type` value not present in `OUTCOME_MAP` is recorded as a notice (`unmapped_outcomes` count) and leaves every outcome flag at `0` rather than raising or guessing.

**Expected-but-unseen values:** none flagged by CONTEXT.md or the folded todo beyond what appears above.

## ballOn semantics

**Transform: `yardline_50 = context.ballOn` (identity, no sign flip or mirroring), clamped to the documented 0..50 range.**

Evidence, all `observed` against the live snapshots:

1. Range: `ballOn` values across the full 4,057-play corpus fall strictly within `[1, 49]` — never negative, never above 50 — matching `docs/data-contract.md`'s existing `yardline_50` definition ("Yards von der eigenen Goalline, 0-50": yards from the possessing team's own goal line, 0 = own goal, 50 = opponent's goal).
2. Same-possession drive progression: a traced USA drive (`unified-plays_019ffff1-a8f8-...`, plays 1-3) starts at `ballOn = 5`, advances to `16` then `24` as the same team keeps possession — monotonically increasing toward the opponent's goal, consistent with "distance already covered from own goal line," not a fixed absolute field coordinate.
3. Scoring proximity: touchdown plays consistently start from a `ballOn` value close to 50 (observed: `46` immediately before a `TOUCHDOWN` outcome) — consistent with 50 representing the opponent's goal line.
4. PAT distance: `TRY`/`XP1` (1-point) plays were observed starting near `ballOn = 40`-`45`, matching `docs/data-contract.md`'s existing note that "yardline_50 = 45 is the 1-point spot, 40 the 2-point spot" for this project's field convention.
5. Possession-change discontinuity: when possession changes mid-drive (interception at USA's `ballOn = 24`, next play's context shows GER's `ballOn = 36`), the new team's `ballOn` does **not** mirror the previous team's value (`50 - 24 = 26 != 36`). This confirms `ballOn` already resets to the new possessing team's own-goal-relative frame on every possession change — exactly the same convention `yardline_50` uses elsewhere in this project — rather than needing a `50 - x` conversion. The gap between the mirrored value and the observed value (`36` vs `26`) is consistent with return yardage gained during the interception return itself, not a semantic error.

No conservative fallback transform is needed: the evidence above settles the semantics as directly compatible with the canonical `yardline_50` definition already in use.
