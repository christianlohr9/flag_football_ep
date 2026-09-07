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
| `penalty` (top-level boolean) | `penalty` | direct copy, cast Int32 (`True`→`1`, absent/`False`→`0`) | `observed` — `penalty: true` with a `penaltyCount` on 225/4,057 plays; used directly rather than inferred from `outcome.type`, since penalties co-occur with many different outcome types (including `None`) |
| `description.text` (fallback: `description.detail`, `description.label`) | `description` (nullable extra) | direct copy | `observed`; `null` on 726/4,057 plays (17.9%) — some plays (mostly `MIDDLE_LINE`/plain penalty entries) carry no `description` object at all |
| `sequence` | not mapped to a canonical column | dropped after use for context only (no canonical "sequence" column) | `observed` — internal action list, e.g. `PASS`→`COMPLETE`→`FLAG_PULL`; empty (`[]`) on 547/4,057 plays |
| `id` (top-level) | `source_game_id` is the *game* id; the *play*'s own `id` is not separately preserved as a canonical column (no canonical "source_play_id" column exists) | not mapped | `observed` |
| `game_id` (this ingest module's own construction) | `game_id` | `make_game_id("ifaf", source_game_id)` → `"ifaf-<id>"` | `observed` (derived) |
| `tournament.id` (`ffwc26-women`/`ffwc26-men`) + `tournament.divisions[0]` | `competition` | `f"{tournament.name} {divisions[0]}"` (`"IFAF World Flag 2026 Women"`/`"...Men"`) — **not** `tournament.name` alone (see 2026-09-06 third follow-up below: both tournaments share the identical name) | `observed` — `tournament_ffwc26-women.json` and `tournament_ffwc26-men.json` both have `name: "IFAF World Flag 2026"`, disambiguated only by `divisions: ["Women"]`/`["Men"]` |
| `games.json[].tournamentId` (falls back to `tournament.id`) | `tournament_id` (nullable extra) | direct copy | `observed` — kept as its own canonical column alongside the human-readable `competition` string, so downstream code can key on the stable machine identifier |
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
| `yards_gained` | left null; `unified-plays` gives only the pre-play `ballOn`, never a paired start/end yard line for the same play, so a per-play yardage delta cannot be derived without assuming adjacent rows share a drive (unsafe across turnovers/penalties) | none — not in `NON_NULL_COLUMNS` |
| `first_down` | left null; no explicit first-down marker exists in `context` or `outcome`, and deriving it from a `down` reset would conflate genuine first downs with PAT-line down resets (`down == 0`) and turnover-driven resets | none — not in `NON_NULL_COLUMNS` |

No defense-scheme fields exist in this source (confirmed by the folded todo and the live snapshots: no `COVERAGE`, `DEF FRONT`, or `BLITZ`-equivalent key anywhere in `unified-plays`), so every `NULLABLE_EXTRAS` defense-charting column (`def_front`, `coverage`, `blitz`, etc.) stays null for `source == "ifaf"`, same as it would for sportapp.fi.

## Outcome vocabulary

Distinct `outcome.type` values observed across the 4,057-play corpus, with counts, and the canonical flags each maps to (`OUTCOME_MAP` in `ingest/ifaf.py`):

| `outcome.type` | count | canonical flags set | notes |
|---|---:|---|---|
| `FLAG_PULL` | 1,289 | none (no dedicated flag; the tackle-equivalent event, not a play-ending result on its own in this schema — most `FLAG_PULL` outcomes co-occur with a preceding `COMPLETE`/`RUSH` in `sequence`) | most common value; carries no scoring/turnover signal by itself |
| `INCOMPLETE_PASS` | 924 | `incomplete_pass = 1` | |
| `TOUCHDOWN` | 375 | see the scoring-types note below — `outcome.pointsScored`, not the type string, decides the flag | 316 have `pointsScored == 6, turnover == False` (`touchdown = 1`); 30 have `pointsScored` absent (`turnover == False`, no flag set — an apparent overturned/nullified TD, since `context.score` never changes across these rows); 14 have `pointsScored == 1` (`turnover == False` — this is actually a 1-point conversion mislabeled with type `"TOUCHDOWN"`, confirmed by `description.kind == "TRY"` on those exact rows; `one_point_conv_success = 1`); 13 have `pointsScored` absent and `turnover == True` (no flag — same nullified pattern as above); 1 has `pointsScored == 6, turnover == True` (a genuine defensive/pick-six touchdown, `def_touchdown = 1`); 1 has `pointsScored == 2, turnover == False` (`two_point_conv_success = 1`, same mislabeling as the `pointsScored == 1` case) |
| `COMPLETE_PASS` | 433 | `complete_pass = 1` | |
| `TURNOVER` | 290 | none of the scoring flags; recorded only in `result_raw` (generic turnover, distinct from `INTERCEPTION`) | `outcome.turnover == True` on all observed instances |
| `None` (key absent from `outcome`) | 262 | no flags set | `outcome` object present but without a `type` key — mid-sequence bookkeeping rows (snap/QB-set-only entries, sequence often empty) |
| `TRY` | 164 | see the scoring-types note below | 102 have `pointsScored == 1` (`one_point_conv_success = 1`); 28 have `pointsScored == 2` (`two_point_conv_success = 1`); 34 have `pointsScored` absent (no flag — the try failed, `description.kind == "TRY_NO_GOOD"`) |
| `MIDDLE_LINE` | 145 | none | observed only as a mid-sequence marker, no scoring/turnover semantics found in the sampled data |
| `INTERCEPTION` | 57 | `interception = 1` | `outcome.turnover == True` on all observed instances |
| `SACK` | 41 | `sack = 1` | |
| `XP1` | 34 | `one_point_conv_success = 1` | every observed instance has `pointsScored == 1`; coexists with `TRY`/`pointsScored==1` in the same corpus (not a per-game split — see the provenance note on the two `gameId` shapes) |
| `TD` | 18 | `touchdown = 1` | every observed instance has `pointsScored == 6, turnover == False`; alternate vocabulary for `TOUCHDOWN`, same empty-`sequence`/`null description` summary-only shape |
| `XP2` | 12 | `two_point_conv_success = 1` | every observed instance has `pointsScored == 2` |
| `RUN` | 12 | none | rush play, no scoring/turnover signal by itself |
| `SAFETY` | 1 | `safety = 1` | single observed instance; `outcome.type` was `SAFETY` with `turnover == True` and no `pointsScored` key at all (unlike the five scoring-shaped types above, `SAFETY` carries no points field in the one observed instance, so its flag stays unconditional on the type string) |

**Scoring-shaped types (`TOUCHDOWN`, `TD`, `TRY`, `XP1`, `XP2`) are keyed by `outcome.pointsScored`, not by the type string.** This is a live-data finding, not an assumption: the type string alone is unreliable — some `"TOUCHDOWN"`-typed rows are really 1- or 2-point conversions (confirmed by `description.kind == "TRY"` on those exact rows), and some carry no `pointsScored` at all despite a scoring-shaped type, which corresponds to zero actual movement in `context.score` across that row (an apparent overturned/nullified play, not a real score). `pointsScored == 6` → `touchdown` (or `def_touchdown` when `outcome.turnover == True`); `== 2` → `two_point_conv_success`; `== 1` → `one_point_conv_success`; anything else (including absent) → no flag, `result_raw` still records the raw type string. This finding came from the score-reconstruction cross-check (see the mapping table's `context.score` row): trusting the type string alone produced score mismatches on roughly 40% of plays in the full corpus; keying on `pointsScored` instead reduces that substantially (the residual mismatches are a separate, smaller-magnitude pattern not further decomposed in this plan — see `IngestNotices.score_mismatches`).

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

## Nachtrag 2026-09-06 — full snapshot (both tournaments), redaction, yardage derivation, video marks

**Scope of this addendum:** a full re-snapshot of every game cpx.studio exposes (not just `ffwc26-women`), two never-before-fetched endpoints (`/games/{id}`, `/games/{id}/plays`), PII redaction at the fetch layer, `yards_gained` derivation, and a per-play video-mark table. The 2026-08-17 snapshot (99 files, `ffwc26-women` only) is preserved unchanged at `data/raw/ifaf-snapshot-20260817/` (gitignored, reproducibility baseline) — every number below that says "before" refers to that snapshot; every "after" number refers to the 2026-09-06 refresh.

### New endpoints and full-corpus snapshot

`/games` exposes **96 games total across two tournaments**, not one: 48 `ffwc26-women` (already known) and 48 `ffwc26-men` (new — same competition structure, same schema, never previously fetched). Both are now snapshotted end to end: `unified-plays_{id}.json`, `events_{id}.json`, and the two new endpoints `game_{id}.json` (`GET /games/{id}` — full game document: rosters, per-player/per-team stat aggregates, current context) and `plays_{id}.json` (`GET /games/{id}/plays` — the reviewer-facing per-play feed: `ballOn`, `down`, `half`, `offenseTeamId`, `events[]` with `action`/`penaltyType`/`playerId`, `videoMark`, `nullified`, `officialScore`, `reconciliation`). All 96 games have all four files present (389 files total under `data/raw/ifaf/`, including the two tournaments' metadata docs). Fetched sequentially with a 0.2s pause between games and up to 3 retries on 429/5xx (none observed live — every request succeeded on the first attempt).

**12 games are genuine zero-play forfeits**, all involving Nigeria, split evenly across both brackets: 6 women's (already known from the 2026-08-17 run) and 6 men's (new finding — same pattern: Nigeria lost every game 0-1 or 1-0 with an empty `unified-plays` array, `status: "FINAL"`). Not a fetch bug in either bracket.

**A handful of additional plausible endpoints were probed for status only (no body parsed, no write):** `/tournaments` → 200, `/tournaments/{id}/games` → 404, `/games/{id}/unified-plays?includeSequence=true` → 200 (the `includeSequence` query param is accepted but the corpus already carries `sequence` on ~86% of plays without it), `/teams/{id}` → 200, `/players?teamId={id}` → 200. The last two are new, real, unauthenticated 200s worth a follow-up fetch in a future plan if per-player/roster data becomes useful — deliberately not fetched or parsed in this session (status-only probe, per scope).

### PII redaction

`/games/{id}/plays` and `/games/{id}/events` both carry real person-identifying fields, confirmed live: `lastEditedByEmail`/`reviewedByEmail` hold real operator email addresses (e.g. a `@gmail.com` address observed on a `plays` row), `lastEditedBy`/`reviewedBy` hold Firebase-style uids, and the events feed's `recordedByUserId` holds either the literal string `"venue-console"` (a system actor, not a person) or a Firebase-style uid (a real reviewer). `fetch/ifaf.py`'s `_write_json` now runs every payload through `redact_pii` before it touches disk — nulls `lastEditedBy(Email)`, `reviewedBy(Email)`, `recordedByUserId`, and any key ending in `Email`/`UserId` (defensive suffix match, in case a future endpoint adds a new person-identifying field), keeping the key present but nulled rather than deleting it. `videoMark`/`videoUrl`/`videoTimeSec` are untouched (they name a video asset, not a person). Player names inside `sequence`/`description`/`players` are deliberately **not** redacted — those live only in the gitignored `data/raw/` tree and are never committed, same policy `docs/ifaf-field-mapping.md`'s original mapping already relied on for the committed fixture (which was hand-trimmed and redacted separately).

### Corpus data-quality regression, 2026-08-17 → 2026-09-06

Comparing `unified-plays` play counts for the same 48 `ffwc26-women` games across the two snapshot dates: **37 games unchanged, 11 games changed** — 10 of those 11 *shrank* (e.g. `ffwc26-wc3`: 161 → 82 plays, `ffwc26-wb4`: 138 → 98, `ffwc26-wa3`: 129 → 103) and 1 grew (`ffwc26-wb6`: 100 → 104). This is consistent with a server-side "corrected"/review-consolidation pass merging or removing play fragments between the two fetch dates, not a fetch bug on our side (every request in both runs returned 200 with a well-formed payload).

**More importantly, exactly those same 11 games newly show null `down` values that were not null before**: games with at least one null `down` value went from **10 (old snapshot) to 21 (new snapshot)**, out of 42 non-forfeit women's games — e.g. `ffwc26-wc3` had 0 null downs in the old snapshot and 9 in the new one; `ffwc26-wa3` went from 0 to 13. This directly drives a lower game-acceptance rate for the women's bracket in this session's re-ingest (see the ingest re-run section below) via the existing `downs_range` validation check (any null `down` value quarantines the whole game) — **this is a live-corpus regression on the provider's side, not a regression introduced by this session's code.** Flagged as the top open question for the provider (see "Open questions" below).

### Yardage derivation (`yards_gained`)

`ingest/ifaf.py::derive_yardage_columns` (run immediately after `derive_outcome_columns`, per game) diffs consecutive `yardline_50` (`ballOn`) values within a drive, with explicit priority rules — full docstring in the module, summarized here:

1. A play carrying the top-level `penalty` flag → null (never a fabricated gain across a penalty).
2. An offensive touchdown (`touchdown == 1`) → `50 - yardline_50` (distance from the snap to the opponent goal line).
3. A safety (`safety == 1`) → `-yardline_50` (tackled at the offense's own goal line).
4. A turnover-shaped play (`interception`, `def_touchdown`, `defensive_two_point_conv`, `result_raw == "TURNOVER"`, or `outcome.turnover`) → null (the next row's `ballOn` belongs to the new possession, not this offense's gain).
5. Otherwise, if the next row shares this row's `drive_id` → `next.yardline_50 - yardline_50`.
6. Otherwise (last play of a drive/game, no following same-drive row) → null.

Live coverage on the accepted post-re-ingest corpus (46 games, both tournaments, 4,218 rows): **71.1% non-null `yards_gained`** (3,001/4,218).

**Cross-check against the new `/plays` endpoint's own `ballOn`:** of the 42 non-forfeit women's games, 13 have a `/plays` response with **zero usable plays** (`reconciliation.reason: "no-tries-labelled"` — the reviewer never finished labeling that game) and 5 more have entries but every `ballOn` is `null`. Restricting to the 24 games where both sides have real, non-null `ballOn` values (1,924 comparable rows), the multiset overlap between `unified-plays`' `ballOn` and `/plays`' `ballOn` is **97.8% (1,881/1,924)** — most games agree at 100%, two show meaningfully lower agreement (`01a0062b-6782-...`: 38%, `ffwc26-wd5`: 46%), worth a closer per-play look in a future plan.

**Cross-check against the `events` feed's `LOS_UPDATE` payload:** multiset overlap of `unified-plays`' `ballOn` against every non-reverted `LOS_UPDATE.payload.ballOn` in the same game is **54.7% (1,769/3,233, 42 games)** — meaningfully lower than the `/plays` comparison. This is expected, not concerning: `LOS_UPDATE` is a finer-grained bookkeeping stream (mid-drive spot corrections, marker resets) with many more events per game than `unified-plays` has rows, so a raw multiset comparison undercounts true agreement. The `/plays` endpoint's own `ballOn` (97.8%) is the stronger corroborating signal.

**`yards_to_go` stays null — now confirmed across every field that could carry it.** `context.yardsToGo`, `games.json`'s `currentContext.yardsToGo`, and (new finding this session) the `events` feed's `DISTANCE_CHANGE.payload.yardsToGo` are **all** a hardcoded constant `10` — checked across all 1,632 `DISTANCE_CHANGE` events in the full 99-file 2026-08-17 snapshot, every single one reads `10`. There is no field anywhere in this API that carries real per-play distance-to-go data. **This blocks EP/WP scoring for this source structurally**, not just cosmetically: `yards_to_go` is a required input to `EP_FEATURES` (`model/hyperparams.py`), so every IFAF row's `ep`/`wp` model prediction is null (0% non-null, confirmed on the 4,218-row post-re-ingest corpus). The only IFAF rows with a non-null `epa` (217/4,218 = 5.1%) are successful 1-/2-point conversions, whose `epa` formula uses a fixed empirical constant (`pat_baselines`) and never touches the null `ep`/`yards_to_go` at all — this is not real EP-model output, and should not be read as "IFAF has 5% EPA coverage" so much as "IFAF has 0% real EP/WP coverage, plus a handful of conversion-attempt constants."

**Play type coverage improved via the play's own `sequence`:** where the direct `outcome.type → play_type` mapping already left a play null (`TOUCHDOWN`/`TD`, `FLAG_PULL`, `TURNOVER`, `MIDDLE_LINE`, `SAFETY`, penalty-only, or no `outcome.type` at all), `_play_type_from_sequence` classifies the play's own `sequence` action list when it names an unambiguous run/pass form (`PASS`/`COMPLETE`/`INCOMPLETE_PASS`/`INTERCEPTION`/`SACK` → `pass`; `RUSH`/`HAND_OFF` → `run`; pass-shaped tokens checked first so yards-after-catch running doesn't misclassify a completed pass as a run). `FLAG_PULL` is the single most common outcome value (1,289/4,057 in the original corpus) and had no `play_type` at all before this addendum. Only tokens already in `canonical.PLAY_TYPE_VOCABULARY` are ever produced — no contract change. Coverage on the post-re-ingest corpus: **86.0% non-null `play_type`** (3,626/4,218), up from ~39.8% (1,268/3,191) before this session (direct outcome-mapping only).

### Ingest/score re-run

| | before (2026-08-17 snapshot, `ffwc26-women` only) | after (2026-09-06, both tournaments) |
|---|---:|---:|
| IFAF rows accepted | 3,191 | 4,218 |
| IFAF games accepted / total non-forfeit | 32 / 42 | 46 / 84 (21 women + 25 men) |
| non-null `yards_gained` | 0 (0%) | 3,001 (71.1%) |
| non-null `play_type` | 1,268 (39.8%) | 3,626 (86.0%) |
| non-null `epa` | not measured (structurally ~0% either way — see above) | 217 (5.1%, conversion-constant rows only) |

Women's-bracket acceptance dropped from 32/48 to 21/48 games — entirely attributable to the corpus data-quality regression described above (more null `down` values in the refreshed snapshot), not to this session's derivation code (no new validation check was added; `downs_range` is unchanged). Men's bracket ingested for the first time: 25/48 accepted, 17/48 quarantined (same `downs_range` pattern) + 6 forfeits. Full-pipeline totals (all five sources): `plays.parquet` 29,282 rows, `games.parquet` 511 games (158 quarantined, mostly `hc_workbook` `half_assigned`/`downs_range` — unrelated to this session). Champion EP/WP models unchanged; scored via `ffep score` with no `--ep-run`/`--wp-run` override (resolves the existing `champion` MLflow alias, no promotion).

### Video marks

`ingest/ifaf_video_marks.py::build_video_marks_table` + the `ifaf-video-marks` CLI command build one row per play from the redacted `plays_{id}.json` snapshots: game/team/half/down/spot context, a compact `events[].action` join as the outcome label, and the video URL + timestamp (the play's own `videoMark` when present, else the game's single source recording + the play's own derived `videoTimeSec`). Live run: **5,522 plays across 62 games** (the 34 games with a zero-length `/plays` response — 12 forfeits + reconciliation gaps — contribute nothing), **70.0% with a resolvable `video_url`** (3,867/5,522). One sampled URL HEAD-checked (no download): `200`, `Content-Type: video/mp4`, `Content-Length: ~8.3GB` — the source recordings are hosted on a public Nextcloud/ownCloud share (`cloud.spontent.pro`) and reachable without authentication.

### Update (same day, follow-up) — `yards_to_go` is derivable after all, and the corpus regression is worked around

Both open questions above were resolved (partially) within the same session, prompted by a closer look at the IFAF 5v5 ruleset itself rather than trusting the API's own `yardsToGo` field.

**`yards_to_go` derivation (`derive_yards_to_go`).** IFAF 5v5 flag rules give the offense four downs to advance the ball past midfield, then a fresh four downs to score — the "line to gain" is therefore always one of exactly two fixed field landmarks (midfield, or the opponent's goal line), never a constant `+10`. Given the already-verified `yardline_50` convention (own-goal-line origin, 0..50, midfield == 25), `yards_to_go` is fully determined by field position alone:

- A down-0 (PAT/TRY) row: `yards_to_go = 50 - yardline_50` (every PAT attempt is inherently goal-to-go — a team only reaches a PAT by having already scored, deep in opponent territory).
- Otherwise: `yards_to_go = 50 - yardline_50` once `yardline_50 >= 25` (already past midfield), else `yards_to_go = 25 - yardline_50` (still trying to reach midfield).

**This turned out to need one live-data correction to the obvious design.** The first draft made "crossed midfield" *sticky* for the rest of a drive (`cum_max` over `drive_id`, mirroring how an American-football first down persists even if a later sack loses yards) — the coordinator's own framing anticipated this ("reset the crossed flag" on a possession change implies persistence within one). Cross-checked against the `events` feed's own `DISTANCE_CHANGE.payload.marker` (which carries exactly two real values, `MIDDLE`/`GOAL` — the payload's `yardsToGo` number is the already-documented hardcoded `10` and carries no signal, but `marker` is real and meaningful), the sticky version agreed on only **74.3%** of 3,527 comparable `(game, ballOn)` pairs. A simple, non-sticky, per-play recompute — no drive memory at all, just "is *this* row's own spot past midfield?" — agreed on **98.2%** of the same pairs. Adding the explicit `MIDDLE_LINE` outcome/sequence marker as an extra OR-signal (computed as `_outcome_middle_line`/`_sequence_middle_line`, still available on the frame) made it slightly *worse* (97.5%), so it is deliberately left unused. **Live-data finding, not an assumption: IFAF's own engine does not persist a "crossed midfield" achievement across a drive the way an American-football first down would — the MIDDLE/GOAL phase is just a function of the current spot**, full stop. The residual ~1.8% disagreement is concentrated on a couple of specific `ballOn` values (`5` and `45` account for most of it) and is most likely asynchronous referee-console state updates across the separate `DOWN_UPDATE`/`LOS_UPDATE`/`DISTANCE_CHANGE` event types (which fire independently, not atomically) rather than a semantic gap in this rule — not further decomposed given the small residual.

Coverage: **100% non-null `yards_to_go`** on the accepted post-restore corpus (every accepted row has a real `yardline_50`).

**Real EP/WP scoring, unlocked.** Re-scored via `ffep score` against the unchanged champion models: `ep`/`epa` are now non-null on **97.9%** of IFAF rows (5,382/5,496 — up from ~5.1% constant-PAT-only rows before this derivation existed), `wp` on **100%**, `wpa` on **99.0%**. This is real, varying model output (not a fixed constant) — spot-checked a same-game sequence of `ep` values (`0.540`, `1.925`, `0.540`, ...) that move with field position and down, exactly as expected. IFAF plays now genuinely participate in EP/WP-based analysis for the first time.

**Corpus regression, worked around per game.** The 11 women's games that regressed between 2026-08-17 and 2026-09-06 (see above) are fixed via a per-game "use whichever snapshot actually validates" policy rather than a blanket revert: for each of the 42 non-forfeit women's games, both the 2026-08-17 and 2026-09-06 `unified-plays`/`events` pair were ingested and run through the full validation check suite (`validation.checks.run_checks`); exactly the same 11 games fail `downs_range` on the 2026-09-06 snapshot and pass cleanly on 2026-08-17 (zero null `down` values in every one), so those 11 had their `unified-plays_{id}.json`/`events_{id}.json` restored from the 2026-08-17 snapshot, keeping the new `game_{id}.json`/`plays_{id}.json` from 2026-09-06 untouched (those two endpoints didn't exist in the old snapshot at all). No game required manual judgment beyond "does it pass validation" — none of the 42 games failed on *both* snapshots. The per-game decision (snapshot used, reason, and both snapshots' play counts) is recorded in `data/raw/ifaf/snapshot_manifest.json` (gitignored alongside the rest of `data/raw/ifaf/`, same as every other raw snapshot — not a committed artifact, but fully reproducible from the two on-disk snapshot directories at any time).

Women's-bracket acceptance is back to **32/48** (10 quarantined, 6 forfeits) — exactly matching the original 2026-08-17 baseline, confirming the restore fully neutralizes the regression rather than just improving on it. Final combined numbers, replacing the "after" column in the table above:

| | before (2026-08-17, women only) | after (2026-09-06 + per-game best-validates restore, both tournaments) |
|---|---:|---:|
| IFAF rows accepted | 3,191 | 5,496 |
| IFAF games accepted / total non-forfeit | 32 / 42 | 57 / 84 (32 women + 25 men) |
| non-null `yards_gained` | 0 (0%) | 3,571 (65.0%) |
| non-null `yards_to_go` | 0 (0%) | 5,496 (100.0%) |
| non-null `play_type` | 1,268 (39.8%) | 4,507 (82.0%) |
| non-null `ep`/`epa` (real model output) | ~0% (structural) | 5,382 (97.9%) |
| non-null `wp` / `wpa` | ~0% (structural) | 5,496 (100.0%) / 5,439 (99.0%) |

Full pipeline (all five sources) after the restore: `plays.parquet` 30,560 rows, `games.parquet` 511 games (147 quarantined, down from 158 — exactly the 11 restored games).

**No game where neither snapshot validates** — every one of the 42 comparable women's games passes on at least one of the two snapshots (31 already passed on 2026-09-06 without needing a restore; the 11 above pass only on 2026-08-17).

### Open questions for the provider

1. ~~Is real per-play distance-to-go ever tracked anywhere?~~ **Resolved this session** — the IFAF 5v5 ruleset's own down structure (four downs to midfield, four more to the goal) makes `yards_to_go` fully derivable from field position; see the update above. The API's own `yardsToGo`/`DISTANCE_CHANGE.payload.yardsToGo` fields remain a hardcoded `10` and should not be trusted directly, but this is no longer a blocker.
2. ~~What happened between 2026-08-17 and 2026-09-06 to a specific subset of women's games?~~ **Worked around this session** (per-game snapshot restore, see above), but the underlying question to the provider stands: was there a manual re-review/correction pass on exactly those 11 games? Is `unified-plays[].corrected` a reliable signal of which snapshot to trust going forward, so future refreshes don't need this same per-game validation dance?
3. **Will the 13 non-forfeit games where `/games/{id}/plays` returns zero entries** (`reconciliation.reason: "no-tries-labelled"`) ever be completed by the review team, or is WM2026 play-by-play permanently partial for those games?
4. `/teams/{id}` and `/players?teamId={id}` both return live 200s and were only status-probed this session — worth a follow-up fetch if roster/player-level data becomes useful for future work.
5. **Why does the events feed's `DISTANCE_CHANGE.payload.marker` occasionally disagree with current field position** (residual ~1.8%, concentrated on `ballOn` values `5` and `45`)? Likely an artifact of independently-firing referee-console events, not a rule question, but worth confirming.

## Nachtrag 2026-09-06 (third follow-up) — competition mislabelling and the men's-tournament exclusion

**The bug.** Both `ffwc26-women` and `ffwc26-men` tournament documents carry the exact same `tournament.name` ("IFAF World Flag 2026") — `_build_game_meta` trusted that name alone for the `competition` column, so once the men's tournament was snapshotted and ingested (see the first Nachtrag above), all 25 accepted men's games silently joined the women's rows under one undifferentiated `competition` label. Every downstream consumer that keys on `competition` (the `competition_tier` lookup, any report grouping by competition) saw one merged "IFAF World Flag 2026" corpus of 57 games / 5,496 rows with no way to tell women's and men's games apart.

**Made worse by a team-code collision.** `data/reference/team_mapping.csv` maps `m-ger` (the men's German national team's cpx.studio id) to the exact same canonical team code `GER` as `w-ger` (the women's team). Any report or corpus filtered by `posteam == "GER"`/`defteam == "GER"` (e.g. `reports/own_team.py`, and — critically — `scripts/explosiveness_comparison.py`, which reads `plays_scored.parquet` with no team or competition scoping at all) would have silently mixed men's and women's national-team rows the moment the men's tournament entered the corpus. This is the M3 explosiveness/comparison script's real exposure, not `features/explosiveness.py`'s `scrimmage_plays` itself — `scrimmage_plays` only filters on `play_type`/`down`, has no source/team scoping, and every caller across the codebase (own-team reports, HC comparisons, the M3 script) is the one responsible for scoping its own input before calling it.

**The fix, three parts:**

1. **Tournament-aware competition labelling.** `_build_game_meta` now appends `tournament.divisions[0]` ("Women"/"Men") to the base tournament name — `"IFAF World Flag 2026 Women"` / `"IFAF World Flag 2026 Men"` — when `divisions` is present; when it's absent entirely (a tournament document shape that predates this addendum, or a future one that never sets it), `competition` stays exactly the bare `tournament.name`, unchanged from the pre-2026-09-06 behavior — there is no ambiguity to resolve for a single tournament with no division info at all (an earlier draft suffixed the raw `tournamentId` in parentheses as a defensive fallback here, but that changed the competition string shape for every existing single-tournament caller with no `divisions` key, including `tests/test_pipeline_ingest.py`'s own synthetic fixture — reverted once the full test suite caught it). `tournament_id` is also kept as its own new canonical extra column (`canonical.NULLABLE_EXTRAS`) so downstream code can key on the stable machine identifier instead of parsing the competition string. Verified: `data/processed/plays.parquet` now shows exactly two distinct IFAF competition labels, 32 women's games / 3,191 rows and 25 men's games / 2,305 rows — matching the accepted-game counts reported earlier in this document.

2. **A dedicated competition tier, excluded from training.** `reference.COMPETITION_TIERS` gained a fourth value, `"mens-international"`, and `data/reference/competition_tier.csv` now has two `ifaf` rows (one per new competition label) instead of one. `model/hyperparams.py`'s `TIER_FEATURE_COLUMNS` (the frozen, already-trained champion models' one-hot tier feature list) was deliberately **not** extended to a fourth column — that would imply retraining, out of scope here; `add_competition_tier_features` still emits a `tier_mens_international` column for any caller building the one-hot fresh from `COMPETITION_TIERS`, the frozen models simply never select it. All 48 known `ffwc26-men` game ids (`ifaf-<id>`) were added to both `train.exclude_games_ep` and `train.exclude_games_wp` in `ffep.toml` — the same existing per-game-id exclusion mechanism already used for two legacy games (`legacy-37`/`legacy-35`), not a new one. Verified: filtering `plays.parquet` by this exclusion list leaves exactly the 3,191 women's rows / 32 games under `ifaf` — zero `"IFAF World Flag 2026 Men"` rows remain training-eligible.

3. **M3 explosiveness/comparison corpus scoping.** `scripts/explosiveness_comparison.py` (the M3-03 comparison script, standalone, not part of the installed package) now resolves each row's `competition_tier` fresh via the existing `reference.load_competition_tier`/`map_competition_tier` and drops any row whose tier is in `EXCLUDED_TIERS` (currently just `mens-international`) immediately after loading `plays_scored.parquet`, before the corpus census print or any calibration/comparison computation. Reports the excluded row/game count so the scope change is never silent. This was NOT wired into `features/explosiveness.py::scrimmage_plays` itself (a shared, heavily-used filter with no existing source/team scoping hook, and out of this session's touched-file scope) — the exclusion belongs at the corpus-loading boundary, mirroring where `own_team.py` already scopes by team.

**Men's rows are not hidden from `ffep score` itself** — `ffep score` has no per-source/tier filtering hook and scores the whole corpus unconditionally; men's IFAF rows get real, varying `ep`/`epa`/`wp`/`wpa` values (97.8%/97.8%/100%/98.9% non-null respectively) exactly like the women's rows (98.0%/98.0%/100%/99.0%). This is intentional — men's rows "may stay in `plays.parquet` with the tier" per the fix's own scope; the exclusion is enforced at training time (`exclude_games_ep`/`exclude_games_wp`) and at the M3 comparison-corpus boundary (`scripts/explosiveness_comparison.py`), not by suppressing scoring output.

**Numbers:**

| | women (`IFAF World Flag 2026 Women`) | men (`IFAF World Flag 2026 Men`) |
|---|---:|---:|
| Games accepted | 32 | 25 |
| Rows accepted | 3,191 | 2,305 |
| Training-eligible rows (after `exclude_games_ep`/`_wp`) | 3,191 | 0 |
| `ep`/`epa` non-null (scored, both still scored) | 3,127 (98.0%) | 2,255 (97.8%) |
| `wp` non-null | 3,191 (100%) | 2,305 (100%) |
| `wpa` non-null | 3,159 (99.0%) | 2,280 (98.9%) |

Full pipeline unaffected by this fix beyond the relabelling: `plays.parquet` still 30,560 rows / 511 games (147 quarantined) — this was purely a labelling and scope-exclusion correction, no rows were added, removed, or re-derived.

**A caveat for future refreshes:** `exclude_games_ep`/`_wp`'s 48-game list is a fixed snapshot of the `ffwc26-men` game ids known at 2026-09-06 — a future fetch that adds more men's games (e.g. a later tournament, or games this snapshot's forfeits/reconciliation gaps eventually resolve) will need this list updated too, since there is no dynamic tier-based training exclusion mechanism in `model/train.py` (deliberately not built here, per "use the existing mechanism rather than inventing one").

## Nachtrag 2026-09-06 (fourth, final follow-up) — corpus-level exclusion, not just training

The third follow-up's fix (a dedicated `mens-international` competition tier, excluded from `exclude_games_ep`/`exclude_games_wp`) was incomplete: it only kept men's rows out of the two champion models' *training* sets. The men's tournament still reached `plays.parquet`/`plays_scored.parquet` with `posteam`/`defteam` = `GER` (colliding with the women's team code, per the third follow-up's own finding), which means it still leaked into every OTHER consumer that groups by team code — `reports/own_team.py`, opponent scouting, HC splits, and any ad-hoc `scrimmage_plays` caller across the codebase that isn't `scripts/explosiveness_comparison.py` (the one place that got an explicit tier filter). None of those has a training-style exclusion list to add men's game ids to.

**The fix: make the corpus safe by construction, not by downstream exclusion.**

1. **`sources.ifaf.ingest_tournaments` gates ingest itself.** `ingest_snapshots` (and `pipeline._ingest_ifaf`) now accept a `tournaments` filter, resolved per game via `games.json`'s `tournamentId` — a game whose tournament isn't in the configured set never becomes a canonical row at all, not even a quarantined one. `ffep.toml` sets `ingest_tournaments = ["ffwc26-women"]`. `ffwc26-men` is still snapshotted to disk (`fetch-ifaf --all-games`, unchanged from the first Nachtrag) but is no longer ingested by default — a genuinely different scope than "fetched" vs. "used." A game whose `tournamentId` can't be resolved at all is excluded too when the filter is active (conservative default, matching the "safe by default" goal). The `exclude_games_ep`/`exclude_games_wp` additions from the third follow-up are reverted — back to just the original two legacy exclusions (`legacy-37`/`legacy-35`) — since there is no longer anything IFAF-related to exclude at training time; the men's rows are simply not in the corpus to begin with.
2. **Distinct canonical team codes for the opt-in case.** `data/reference/team_mapping.csv`'s 16 `ifaf` `m-*` rows now map to a `-M`-suffixed code (`m-ger` → `GER-M`, not `GER`) instead of colliding with the women's team. This means a future user who *does* opt into `ffwc26-men` (by adding it to `ingest_tournaments`) gets a corpus where the men's national team can never merge with the women's under one identity, even though team-code-scoped reports still have no explicit tier-awareness of their own.
3. **The competition tier and label from the third follow-up stay unchanged** — `"IFAF World Flag 2026 Men"` / `mens-international` are still correct for the opt-in case, and `scripts/explosiveness_comparison.py`'s tier filter is still a useful second line of defense if a user opts into the men's tournament without also auditing every other report for team-code scoping.

**Verified: re-ran `ffep ingest` + `ffep score`.** IFAF corpus is now exactly the women's tournament: **32 games / 3,191 rows**, `competition` and `tournament_id` both single-valued (`"IFAF World Flag 2026 Women"` / `"ffwc26-women"`) — zero men's rows anywhere in `plays.parquet`. Full pipeline total: **28,255 rows** (all five sources) — back to the pre-IFAF-full-snapshot baseline this whole session started from, confirming the men's tournament contributed nothing else to the row count once the ingest-time filter is in place. `games.parquet`: 469 games (130 quarantined) — down from 511/147 in the third follow-up's state, exactly the 42 men's non-forfeit games (25 previously accepted + 17 previously quarantined) that no longer appear at all.

| | third follow-up (both tournaments, training-only exclusion) | fourth follow-up (ingest-time tournament filter) |
|---|---:|---:|
| IFAF rows in `plays.parquet` | 5,496 (women 3,191 + men 2,305) | **3,191 (women only)** |
| IFAF games in `plays.parquet`/`games.parquet` | 57 (32 women + 25 men accepted; 84 total incl. quarantined) | **32 (women only)** |
| Full pipeline total rows | 30,560 | **28,255** |
| `games.parquet` total / quarantined | 511 / 147 | 469 / 130 |
| non-null `yards_gained` (IFAF) | 3,571 (65.0%, both tournaments) | 1,957 (61.3%, women only) |
| non-null `yards_to_go` (IFAF) | 5,496 (100%) | 3,191 (100%) |
| non-null `play_type` (IFAF) | 4,507 (82.0%, both) | 2,578 (80.8%, women only) |
| non-null `ep`/`epa` (IFAF women) | 3,127 (98.0%) | 3,127 (98.0%, unchanged) |
| non-null `wp`/`wpa` (IFAF women) | 3,191 (100%) / 3,159 (99.0%) | unchanged |

The women's-only numbers above differ slightly from the third follow-up's "women" row (which already isolated women's rows via the competition label) only in percentage terms — the underlying women's 3,191 rows and their `yards_gained`/`yards_to_go`/`play_type`/`ep`/`epa`/`wp`/`wpa` values are identical; this table just reflects that the denominator context (whole-corpus vs. women-only) changed, not the women's data itself.

**To opt into the men's tournament** for any future analysis: add `"ffwc26-men"` to `ingest_tournaments` in `ffep.toml`, re-run `ffep ingest`. The resulting corpus will have men's rows with their own `GER-M`-style team codes, their own `mens-international` competition tier, and their own `"IFAF World Flag 2026 Men"` competition label — nothing merges with the women's data automatically, but every report/script that scopes by team code or competition should still be checked case-by-case for whether it needs its own explicit exclusion (mirroring `scripts/explosiveness_comparison.py`'s tier filter), since there is no single central "corpus scope" chokepoint downstream of ingest.

## Nachtrag 2026-09-07 — `unified-plays.context` was never a reliable pre-snap state; `/plays` is now the primary IFAF source

**The bug, confirmed against real data.** The user reported that `ifaf-019ffff1-a8db-73ed-91ff-068fd964194c` (women's QF, MEX vs. ESP) showed a broken per-play sequence: the first three rows all read `down 2` / `yards_to_go 21` / `yardline_50 4`, and play 9 → play 10 jumped from `down 2` straight to `down 4`. Re-running the pre-2026-09-07 `flatten_unified_plays` → `derive_outcome_columns` → `derive_yardage_columns` → `derive_yards_to_go` chain against this exact game reproduces it precisely:

| play_id | down (old) | yardline_50 (old) | yards_to_go (old) |
|---:|---:|---:|---:|
| 1 | 2 | 4 | 21 |
| 2 | 2 | 4 | 21 |
| 3 | 2 | 4 | 21 |
| 4 | 2 | 31 | 19 |
| 5 | 2 | 4 | 21 |
| 6 | 2 | 33 | 17 |
| 7 | 0 | 45 | 5 |
| 8 | 2 | 4 | 21 |
| 9 | 2 | 4 | 21 |
| 10 | 4 | 19 | 6 |

`(down=2, ballOn=4)` is not this game's real pre-snap state repeating six times in ten plays — it is `unified-plays`' own literal default/placeholder state (the payload's `context` object clearly isn't populated for many rows, and something upstream fills it with a constant rather than leaving it null). Measured across the full women's `unified-plays` corpus (42 non-forfeit games, 4,057 rows, files currently on disk): **106 rows (2.6%) sit on exactly this literal default state** (`context.down == 2 and context.ballOn == 4`); this one game alone accounts for 43 of the 93 rows in its own `unified-plays` snapshot (46%) — the worst-affected game found, which is exactly the one the user happened to check. The `context` block genuinely alternates between real pre-snap spots, real post-play spots, and this default state row by row, with no reliable way to tell which is which from `unified-plays` alone.

**This invalidates the 2026-09-06 Nachtrag's own agreement-rate claims, and they must not be trusted going forward.** The 98.2% `yards_to_go`/`MIDDLE`-`GOAL` marker agreement and the 71.1%/65.0%/61.3% `yards_gained` coverage numbers reported above were all computed with `unified-plays.context.ballOn`/`context.down` as both the *input* to the derivation being validated *and* (indirectly, via the events-feed cross-check) part of what was being validated against — a source now known to be structurally unreliable for a meaningful share of rows, including this exact game. Re-running the same two derivations (`derive_yardage_columns`/`derive_yards_to_go`, unchanged code, still the fallback path below) against **the correct, /plays-derived pre-snap state confirms the arithmetic and field-position rules themselves were right**; what was wrong was trusting `unified-plays.context` as ground truth for the per-play state those rules were fed.

### The fix: `/games/{id}/plays` (the reviewer feed) is now the primary source

`/games/{id}/plays` (`plays_{game_id}.json`, first snapshotted 2026-09-06) is the human-reviewed, per-play feed cpx.studio itself uses for game reconciliation — real pre-snap `down`, `ballOn`, `half`, `offenseTeamId`, an explicit `nullified` flag for overturned plays, and an `events[]` action list this ingest now derives every outcome flag and `play_type` from directly. Re-running the same game through the new primary path:

| play_id | down (new) | yardline_50 (new) | yards_to_go (new) | posteam | play_type |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 5 | 20 | ESP | pass |
| 2 | 2 | 11 | 14 | ESP | pass |
| 3 | 3 | 31 | 19 | ESP | pass |
| 4 | 2 | 33 | 17 | ESP | pass (TD) |
| 5 | 0 | 45 | 5 | ESP | no_play (nullified TRY) |
| 6 | null | 45 | 5 | ESP | no_play (penalty) |
| 7 | 1 | 5 | 20 | MEX | pass |

1st @5 → 2nd @11 → 3rd @31 → TD, exactly matching what `/games/{id}/plays` itself shows and what the user expected. Row 5 is the reviewer's own nullified TRY (no fake conversion credited); row 6 is a dead-ball penalty with a genuinely missing raw `down` — both preserved as real `no_play` rows, never dropped, never a fabricated result.

### `flatten_plays_records` mapping table

| `/plays` field | canonical column | transform |
|---|---|---|
| `sequence` | `play_id` (renumbered gapless 1..N, sort key) + `source_play_sequence` (raw value preserved, `Float64` — inserted rows use a `.5` suffix, e.g. `907.5`) | sort by `sequence` ascending; a missing/non-numeric `sequence` sorts last, stable |
| `half` | `half` | direct copy |
| `down` | `down` | direct copy, **except** a `TRY`-shaped record always gets `down = 0` (this project's existing PAT convention) even when the record's own `down` is null (true for every observed `TRY` record) |
| `ballOn` | `yardline_50` | direct copy |
| `offenseTeamId` | `posteam` (mapped via `map_teams`) | direct copy |
| `offenseTeamId` vs. `games.json` home/away | `defteam` | whichever of `home_team`/`away_team` isn't `posteam` |
| `nullified` | folds into `play_type == "no_play"` and forces every outcome flag to 0 | never dropped — the raw record (`result_raw`) is still preserved |
| `events[].action` set, only when `{"PENALTY"}` exactly | folds into `play_type == "no_play"`, `penalty = 1` | a dead-ball foul call with no live-play result |
| `events[].action` set (general) | `play_type`, `complete_pass`/`sack`/`interception`/`safety`/`penalty` flags | see `_play_type_from_actions`/`flatten_plays_records` docstrings in `ingest/ifaf.py` |
| `TOUCHDOWN` action, without `INTERCEPTION` | `touchdown = 1` | offensive touchdown |
| `TOUCHDOWN` action, with `INTERCEPTION` | `def_touchdown = 1` (not `touchdown`) | pick-six; no other turnover-shaped touchdown signal exists in this source |
| `TRY` event's `tryPoints`/`tryGood` | `one_point_conv_success` / `two_point_conv_success` | `tryGood is True` and `tryPoints in {1, 2}`; anything else (failed/unlabelled) sets neither. `defensive_two_point_conv` stays permanently 0 for this source — no record combining a failed `TRY`'s defensive return with a score was observed live (documented-absent, not a silently-wrong guess) |
| `PASS` event's `playerId` (fallback: `INCOMPLETE_PASS` event's `playerId`, for the ~1% of records with no separate `PASS` event) | `qb` and `thrown_by` (both set identically — this source carries one passer identity per play, unlike Hudl's two separately-charted columns) | resolved through the local roster JSON (`_load_teams_meta`) to a plain name string |
| `PASS`/`INCOMPLETE_PASS` event's `intendedReceiverId`, else a `RUSH`/`HAND_OFF` event's `playerId` | `target` | mirrors `ingest/sportapp.py`'s existing `rusher -> target` convention |
| `COMPLETE` event's `playerId` | `received_by` | null on an incompletion or interception — the offense never received the ball |
| `PASS`/`INCOMPLETE_PASS` event's `passSide`/`passDepth` | `pass_side`/`pass_depth` (new nullable extras) | direct copy |
| `INCOMPLETE_PASS` event's `incompleteReason` | `incomplete_reason` (new nullable extra) | direct copy |
| `PENALTY` event's `penaltyType` | `penalty_type` (new nullable extra) | direct copy |
| `videoMark`/`videoUrl`/`videoTimeSec` | not mapped | already covered by the standalone video-marks table (`ingest/ifaf_video_marks.py`) |
| (fallback games only) | `source_detail = "unified-plays-fallback"` (new nullable extra) | stamped only when `ingest_snapshots` fell back to `unified-plays` for a game — null on every primary-path row |

`yards_gained` (`derive_yardage_columns_plays`) reuses the same priority-ordered rule set as before (penalty/no-play excluded first, then touchdown, safety, turnover, same-drive-next diff, else null) with one addition: `down == 0` (a TRY row) is explicitly excluded, and a dead-ball penalty record sitting between two live plays absorbs its own yardage adjustment for free (it is simply the preceding play's own "next row", and its `ballOn` already reflects the enforced spot). `yards_to_go` (`derive_yards_to_go`) is **reused completely unchanged** — the `down == 0` PAT convention above is exactly what that function already expected.

### Validated derivation rules, honest rates (this session, 29 primary-path women's games, 2,645 rows, before pipeline validation quarantine)

- **`yards_to_go`/goal-to-go phase, cross-checked against the `/plays` record's own `marker` field** (`MIDDLE`/`GOAL`, a value the reviewer feed carries directly per play, independent of the derivation): **98.2% (823/838)** of the plays that carry a `marker` value agree with the derived `yardline_50 >= 25` rule. This reproduces the 2026-09-06 Nachtrag's number almost exactly — reassuring, since it is now measured against the correct source, not the flawed one.
- **`down`, cross-checked against the events feed's `DOWN_UPDATE` stream** (nearest-preceding `clientTimestamp` before the play's own `startedAt`, within the same game): **82.4% (1,995/2,422)**.
- **`ballOn`, cross-checked against the events feed's `LOS_UPDATE` stream** the same way: **44.6% (853/1,914)**. Lower than `down`, expected for the same reason the 2026-09-06 Nachtrag already flagged for this exact comparison: `LOS_UPDATE` is a much finer-grained bookkeeping stream (mid-drive spot corrections fire far more often than there are `/plays` records), so a nearest-timestamp match undercounts true agreement — this is a known limitation of timestamp-based matching against a finer-grained event stream, not a sign `/plays`' own `ballOn` is unreliable (see the `marker` cross-check above, which doesn't depend on timestamp matching at all and agrees at 98.2%).
- **Explicit event-level `yardsGained`** (an occasional field some `RUSH`/other events carry directly): exactly **1 occurrence** in the entire women's `/plays` corpus (`ffwc26-wa5`, sequence 960, `RUSH` for `-1`). It sits on the final play of that game, where `derive_yardage_columns_plays` is null by rule (no following same-drive row to diff against) — 0/1 recoverable via the diff method, which is a structural limitation of the diff approach on a game's last play, not a disagreement with the value itself.
- **How often `/plays`' own `down` field already reflects a reset after crossing midfield**: of 197 detected within-drive transitions where a play's own `yardline_50` crosses from `< 25` to `>= 25`, the immediately following play's `down` reads `1` (a genuine fresh-set reset) in **189/197 (95.9%)** of cases. The residual 8 include this session's own QF game (its `down` sequence goes `1, 2, 3, 2` across the ESP opening drive — a reviewer inconsistency, not a crossing-detection gap) — this ingest deliberately trusts `/plays`' own `down` field as-given rather than recomputing it from field position, precisely because the reviewed feed itself is not perfectly self-consistent and re-deriving it would be presumptuous, not a correction.

### Fallback categories (48 women's games)

- **29 games** use the primary `/plays` path.
- **13 games** fall back to `unified-plays` — all 13 for the same reason: a real but empty `/plays` response (`reconciliation.reason == "no-tries-labelled"`, the reviewer never finished labelling that game).
- **6 games** are genuine zero-play forfeits (all Nigeria), contributing nothing to either path.

No game in this corpus falls back due to a missing or unparseable `plays_{id}.json` file — every one of the 48 women's games has that file on disk (2026-09-06 full snapshot), so every fallback observed live is the reconciliation-gap case, not a fetch gap.

### Ingest/score re-run

Re-ran `ffep ingest` + `ffep score` (women's tournament only, `sources.ifaf.ingest_tournaments = ["ffwc26-women"]`, unchanged):

| | before (this session, unified-plays for every game) | after (this session, `/plays` primary + fallback) |
|---|---:|---:|
| IFAF games accepted / total non-forfeit | 32 / 42 | **25 / 42** |
| IFAF rows accepted (`plays.parquet`) | 3,191 | **2,264** |
| non-null `down` | 3,191 (100%) | 2,264 (100%) |
| non-null `yards_to_go`/`yardline_50` | 3,191 (100%) | 1,727 (76.3%) |
| non-null `yards_gained` | 1,957 (61.3%) | 1,247 (55.1%) |
| non-null `play_type` | 2,578 (80.8%) | 2,068 (91.3%) |
| non-null `ep`/`epa` | 3,127 (98.0%) | 1,689 / 1,688 (74.6%) |
| non-null `wp`/`wpa` | 3,191 (100%) / 3,159 (99.0%) | 1,727 (76.3%) / 1,707 (75.4%) |

**The accepted-game count drops from 32/42 to 25/42, and this is the correct, honest trade-off, not a regression to fix.** Every game's acceptance is still gated by the unchanged `downs_range` validation check (any null `down` value quarantines the whole game). Under the old, `unified-plays`-only path, 32 games happened to have zero null `down` values — but as the bug above shows, a non-null `down` from `unified-plays.context` was frequently just a wrong or default value, not a real one. Under the new primary path, **17 games now fail `downs_range`** because `/plays` — the reviewed, authoritative feed — genuinely has at least one null `down` value in them (e.g. the QF game's own dead-ball penalty record, `play_id 6` above, whose raw `down` is null in the reviewer feed itself). This ingest does not fabricate a value to keep those games passing; a real gap in the reviewed data is now surfaced as a real gap, exactly as the validation check is designed to do. The coverage drops on `yards_to_go`/`yards_gained`/`ep`/`wp` in the table above are a direct, expected consequence of `ballOn` being genuinely absent on roughly a quarter of `/plays` rows in the accepted games (not every play in this reviewer feed carries a spot) — a smaller but *correct* number, not the previous higher-but-wrong one. `play_type` coverage actually improves (80.8% → 91.3%), since the `/plays` action list gives an unambiguous run/pass signal on far more rows than `unified-plays`' `outcome.type` + `sequence` fallback ever did.

**The QF game itself (`ifaf-019ffff1-a8db-73ed-91ff-068fd964194c`) is one of the 17 quarantined games** in `plays.parquet` (that same `play_id 6` null `down`) — it is still visible, fully corrected, in `data/processed/exports/ifaf_wm2026_pbp.csv` (built directly from `ingest_snapshots`, bypassing the pipeline's validation gate, same as before this session), which is what a user re-checking this specific game should use.

Full pipeline (all five sources): `plays.parquet` **27,328 rows** (down from 28,255 — exactly the 927-row drop from 3,191 → 2,264 IFAF rows), `games.parquet` **469 games (137 quarantined)**.

### Test coverage

`tests/test_ingest_ifaf.py` gained ~67 new tests (140 total, up from 73) covering `flatten_plays_records`, `derive_yardage_columns_plays`, `load_plays_snapshot`, `_load_teams_meta`, and the `ingest_snapshots` primary/fallback branching — every fixture uses fabricated player ids/names (`w-xxx-pN` / "Player One"), never real player data. The full repository test suite (1,951 test functions) passes with zero failures/errors after this change.
