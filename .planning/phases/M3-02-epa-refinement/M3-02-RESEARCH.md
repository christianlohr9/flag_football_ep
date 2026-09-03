# Phase M3-2: EPA-Refinement - Research

**Researched:** 2026-09-03
**Domain:** Retraining EP/WP (XGBoost, LOGO-by-`game_id`) on an enlarged, structurally messy HC corpus; HC-workbook game segmentation; head-coach method reproduction from an Excel workbook
**Confidence:** HIGH for the segmentation root cause and the codebase's label-construction machinery (read/executed directly this session); MEDIUM for the `half`-strategy recommendation (the mechanism is HIGH confidence, the "safe default" is a design judgment); LOW/flagged for the two open HC questions (Frage 1/2), which no amount of local investigation can resolve

## Summary

Both of M3-1's entry blockers were reproduced directly against the real workbooks this session (`uv run python`, `openpyxl`, no PII in any output below). The **over-segmentation root cause** is now pinned precisely: `hc_workbook.py::_split_pair_block` creates a new "game" on every **ordered** `(team1, team2)` pair change, so a real international game's offense/defense possession alternation (`Germany, Ireland` → `Ireland, Germany` → `Germany, Ireland` ...) fragments into dozens of one-to-ten-row "games." Switching to an **unordered** pair as the boundary key cuts the `Scoring Probability`/`Data` tab's large team-pair block from 137 fragments to 22 contiguous games (6.2x) — safe, local, and independently verifiable today. But this fix only reaches about 15% of the total 2,128-game inflation: the dominant share (1,801 of 2,128, ~85%) lives in the `Copy of Data` tab, which this session's inspection found has a **materially different, undocumented column layout** than `Data` (an extra `FH` column, `YARD LN`/`Drive Success` swapped, `Thrown By`/`YAC` absent) and a qualitatively different row pattern (dozens of alternating 2-40-row numeric micro-blocks interleaved with 1-3-row team-pair marker rows, not one large team-pair block). Fixing `Copy of Data`'s segmentation is **not safe to attempt without the HC's answer to the already-open Frage 2** — this session's finding sharpens that blocker rather than resolving it.

The **`half` strategy** turns out to have a materially larger blast radius than CONTEXT's framing ("`half` was adopted for EP only") suggests. `half` is not merely an XGBoost feature column: `features/mutations.py::_mark_half_end` groups `.over(["game_id", "half"])` to compute `half_end`, and `game_end` (`half_end==1 AND half==2`) gates **both** EP's terminal "No_Score" label marker **and** WP's `Winner` backward-fill (`prepare_wp_data` calls the same `_mark_half_end`). If HC rows carry `half = null` for an entire game, `game_end` never fires for that game — WP's `Winner` would never resolve, independent of whether `half` is in `WP_FEATURES`. The one sentinel value that keeps both EP's and WP's label machinery correct is **`half = 2` (a constant, for the whole game)**, not `null`/"unknown-category" and not `half = 1`: it makes `half_end`/`game_end` fire exactly once, at the true last row, and also satisfies the `half_assigned` validation check outright (which only requires `half ∈ {1, 2}`, not that both values are present). The cost is a real, quantifiable one: every HC game's "no score before halftime" boundary disappears, and the feature column blends thousands of un-differentiable rows into whatever `half==2`'s real signal was tuned on (+0.00179 pooled log-loss in 1.3). A cleaner two-column split (a temporary `half`-for-labels column vs. a separate sentinel `half`-for-features value) is recommended over reusing `half=2` for both purposes unmodified.

The head coach's own method is now fully legible from the workbook's formula layer (`data_only=False`): `Reg` is a **per-down, per-field-half OLS/polynomial trendline fit** (`=FORECAST(...)` for down 1, hardcoded polynomial coefficients of increasing degree — linear for down 1-3, quadratic/cubic/degree-6 for down 4 and the "opposite half" block — that read like hand-transcribed Excel chart-trendline equations, not a principled statistical fit). `SP by D&D`/`EP by D&D`/`Sample Size by D&D`/the `*Clustered` variants are pure empirical aggregate tables (down x distance, split by own-half/opponent-half) with no PII — directly snapshot-able per EPA-D03. The `Clustered` tabs' first three distance-bin row labels are corrupted by an Excel autoformat bug (typed range labels like `"1-5"` silently became `datetime(2021, 1, 5)`); the true bin boundaries must be inferred from the surviving labels' pattern, not read literally.

**Primary recommendation:** Wave 1 does three things, not two — the CONTEXT-known pair (`half` strategy, segmentation fix) plus a **newly found third gap**: `data/reference/competition_tier.csv` has zero rows for any `hc_workbook` source, and `reference.map_competition_tier` **raises** `UnmappedCompetitionError` on any unmapped `(source, competition)` pair. The first HC row that reaches training with no matching `competition_tier.csv` row hard-fails the run. Fix the `half` sentinel (constant `2`, decoupled from the feature column) and the `Data`-tab pair-block segmentation (unordered-pair grouping) as evidence-backed, low-risk changes; leave `Copy of Data` fully provisional/excluded pending Frage 2; add `competition_tier.csv` rows for every HC `(source, competition)` combination that will reach training.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| HC game segmentation fix (`segment_games`) | Ingest module (`ingest/hc_workbook.py`) | `data/reference/hc_games.csv` (re-keyed `block_key`s) | Segmentation is a pure row-grouping concern inside the existing reader; a rule change invalidates every existing provisional `block_key`, so the reference CSV must be regenerated, not hand-patched |
| `half` sentinel / label-construction decoupling | Feature-mutation layer (`features/mutations.py`) | `model/hyperparams.py` (`EP_FEATURES`) | `_mark_half_end`/`prepare_ep_data`/`prepare_wp_data` are the single place `half` drives label windowing; the feature-list decision (what value the model sees) is a distinct, downstream concern |
| `competition_tier.csv` HC rows | Reference data (`data/reference/`) | `model/train.py` (`_build_competition_tier` consumer) | Same maintained-CSV pattern as `hc_games.csv`/`half_boundaries.csv` — a missing row is a data-maintenance gap, not a code bug |
| HC method reproduction (SP/EP/Reg tables) | Analysis/reporting script (new, phase-scoped) | `data/reference/hc_sp_tables/*.csv` (snapshot) | Read-only snapshot of aggregate tables plus a parallel computation over our canonical corpus with the same binning; neither belongs in the training pipeline itself |
| Training/ablation (with vs. without HC) | `model/train.py` (`train_ep`/`train_wp`, called twice with a filtered `plays` frame) | MLflow experiment/run tags | No new training-harness code needed — `train_ep`/`train_wp` already take a `plays: pl.DataFrame` directly; the ablation is two calls with different input frames, not a new code path |
| Per-corpus / per-source calibration reporting | `model/evaluate.py` (`per_source_metrics`, `reliability_curves`) already exists | — | Reuse verbatim; `per_source_metrics` already reports per-source log-loss vs. each source's own naive baseline |

## User Constraints (from CONTEXT.md)

<user_constraints>

### Locked Decisions

- **EPA-D01 Fix the corpus first, then train.** Wave 1 = (a) `half` strategy for HC rows, (b) game segmentation fix + `hc_games.csv` refill; only then the training waves. Row counts before/after are reported per source.
- **EPA-D02 Methodology is locked from 1.3:** GroupKFold over `game_id` (D-07), reliability curves + log-loss vs naive baseline (REQ-S1-08), competition tier as covariate, MLflow registry with champion alias — nothing silently overwritten (REQ-S1-11). New corpus = new data hash = new run; the previous champion stays available for the comparison.
- **EPA-D03 HC comparison is tabular and honest:** reproduce his SP/EP-by-down-&-distance tables from HIS `Data` rows and from OUR canonical corpus with the same binning; place model EP next to them; report n per cell; explicitly show where small-sample cells make his point estimates noisy (that is the argument for the model) and where the model disagrees with his intuition (his call to review). Snapshot his tables read-only into `data/reference/hc_sp_tables/*.csv` (aggregates, no names) for reproducibility.
- **EPA-D04 Source provenance in training:** `source` column carried through; ablation "with vs without HC rows" on the frozen GroupKFold folds so the HC sees what his data adds.
- **EPA-D05 Deliverable for the sync:** German `docs/epa-refinement-2026-10.md` (method, corpus counts, calibration, comparison tables, ablation, open questions) — M3-4 turns it into the handout.

### Open Decision (user)
- **`half` for HC rows** — options: (a) allow unknown `half` for HC rows and treat it as its own category/imputed in the EP model (the `half` feature was adopted for EP only in 1.3); (b) heuristic half boundaries from play sequence (PLAY # jumps, drive patterns), marked provisional; (c) ask the HC for half markers (Frage 4). Orchestrator recommendation: (a) for October, (c) in parallel. Planner: plan (a) as the default path with (b)/(c) as documented alternatives unless the user decides otherwise before planning completes.

**Research addendum to this open decision (see `## Common Pitfalls` / `half`-strategy section below): a literal `null`/"unknown category" reading of option (a) breaks WP label construction (`game_end` never fires), not just EP's feature column. The evidence-backed version of option (a) is a constant sentinel `half = 2` for label construction, decoupled from a separate sentinel value for the `EP_FEATURES` column. This is a refinement of (a), not a different option — flagged here because it changes what "(a)" concretely means in code.**

### Claude's Discretion
- Segmentation rule for HC games (team-name/date columns, sheet structure), binning for the D&D tables, ablation design details, MLflow experiment naming.

### Deferred Ideas (OUT OF SCOPE)
- Real game clock for WP (BL-01 OCR); win-driver analysis (BL-04); explosiveness (M3-3).

</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| HC-03 | EP/WP retrained on the enlarged corpus (GroupKFold over `game_id`, calibration, tier eval, MLflow-version) and set tabularly against the HC's "Scoring Probability by Situation" approach (SP/EP by down & distance, clustered/weighted) | This document: segmentation-fix evidence (§ over-segmentation), `half`-sentinel mechanism (§ half strategy), HC-method reproduction (§ HC method / `Reg` tab formulas), reusable `per_source_metrics`/`reliability_curves`/`naive_baseline_logloss` (§ Architecture Patterns), competition-tier gap (§ Common Pitfalls) |

</phase_requirements>

## 1. Over-segmentation: root cause, reproduced against the real workbooks

All numbers below were produced this session by importing `flag_football_ep.ingest.hc_workbook` and calling `read_sheet_rows`/`segment_blocks`/`segment_games` directly against the real, gitignored workbooks under `data/raw/hc_files/` — `[VERIFIED: local execution against real data, 2026-09-03]`. No player names appear anywhere below; all figures are row/game counts and team-pair labels only (team names, not player names, are what these blocks key on).

### 1.1 Per-workbook block structure

| Workbook / sheet | Block(s) | Kind | Rows | `segment_games` count today | Assessment |
|---|---|---|---:|---:|---|
| `Germany Analytics Stats EC 2025...xlsx` / `Data` | 1 | numeric | 269 | 3 | Plausible count; **blocked on Frage 1** (looks like a pasted EP-analysis fragment, header reads `O`/`D` not `PLAY #`/`ODK`) |
| `Offense Analytics 2026...xlsx` / `Data` | 1 | numeric | 1,926 | 35 | **Already correct** — clean PLAY#-reset segmentation, game sizes 12-90 plays, matches "camp/competition scrimmage segments" plausibility |
| `Scoring Probability...xlsx` / `Data` | block 0 (pair) | pair | 653 | **137** | Root cause confirmed — see 1.2 below |
| `Scoring Probability...xlsx` / `Data` | block 1 (numeric) | numeric | 7,963 | 94 | Already correct — real PLAY#-charted games, 9 of these are the confirmed `legacy-39..47` duplicates |
| `Scoring Probability...xlsx` / `Data` | block 3 (numeric) | numeric | 5,193 | 57 | Already correct |
| `Scoring Probability...xlsx` / `Data` | blocks 2, 4 | pair | 1 + 1 | 2 | Single-row noise, not real games (likely stray team-abbreviation cells) |
| `Scoring Probability...xlsx` / `Copy of Data` | ~155 alternating micro-blocks | numeric + pair, interleaved | 3,895 total | ~1,801 pair-block fragments + several hundred tiny numeric games | **Not fixed this session** — different problem, see 1.3 |

`Data`-tab total under today's rule: 290 games (137+94+1+57+1). `Copy of Data` total under today's rule: matches the already-documented 1,801 pair fragments (`docs/hc-workbook-ingest.md`) plus its own numeric micro-block games, summing toward the remaining balance of 2,128.

### 1.2 The fix: unordered team-pair grouping (verified on the `Data`-tab block)

`hc_workbook.py::_split_pair_block` (lines 631-652) creates a boundary whenever the **ordered** tuple `(team1, team2)` changes between consecutive rows:

```python
# src/flag_football_ep/ingest/hc_workbook.py:638-647 (current)
for row_num, values in rows:
    t1 = values[0] if len(values) >= 1 else None
    t2 = values[1] if len(values) >= 2 else None
    pair = (_normalize_pair_label(t1), _normalize_pair_label(t2))
    is_boundary = not current or pair != prev_pair
    ...
```

Printing the first 40 slices of `Data`-tab block 0 shows the exact failure mode: `Germany | Ireland` (5 rows) -> `Ireland | Germany` (5 rows) -> `Germany | Ireland` (4 rows) -> ... — a real game's offense/defense possession alternation, charted as the pair flipping order every few rows. This is the mechanism `docs/hc-workbook-ingest.md` already named qualitatively ("Possession-Wechsel innerhalb desselben echten Spiels"); this session pins it down to the exact ordered-tuple comparison and quantifies the fix.

Grouping by the **unordered** pair (`frozenset({team1, team2})`) instead, on the same 653-row block:

```
naive unordered-pair game count (block0): 22   (down from 137, a 6.2x reduction)
n unique unordered pairs in this block: 13
  {germany, spain}: 27 occurrences | {germany, ireland}: 19 | {germany, switzerland}: 19
  {austria, germany}: 17 | {finland, germany}: 15 | {czech, germany}: 15 | {france, germany}: 15
  ... plus 6 single/double-occurrence noise entries: {at, d}, {s}, {ch, d}, {d, f}, {k}, {d, e}
```

22 contiguous games from 7 distinct real opponents is directly plausible for a multi-season log of German women's national-team friendlies/tournament games — consistent with the todo's own sanity check ("eine Saison hat ~10-20 Spiele"). The residual noise (`{at, d}`, `{s}`, `{k}`, single-row blips) are almost certainly abbreviation variants (`D`=Deutschland, `AT`=Austria, `CH`=Switzerland) or a `K` = kickoff marker mixed into the same column — a full, risk-free resolution needs an alias table (`D`->`germany`, `AT`->`austria`, `CH`->`switzerland`) which is straightforward for the clearly-attested abbreviations but ambiguous for `S`/`F` (Spain vs. Switzerland; Finland vs. France both appear as full names in the same block) — **do not guess these two**; leave the ambiguous single/double-row noise rows as their own tiny, clearly-flagged residual games rather than merging them into a neighbor by inference.

**Recommended Wave-1 code change:** in `_split_pair_block`, compare `frozenset({t1_norm, t2_norm})` instead of the ordered tuple `(t1_norm, t2_norm)`. This is a five-line change, needs no new column, and is independently testable against this session's exact numbers (137 -> 22 on the `Data`-tab block). `block_key`s change as a side effect (fewer, larger games) — `data/reference/hc_games.csv` must be regenerated after the change, not hand-edited; the 9 existing confirmed duplicate declarations (`legacy-39..47`, which live in the *numeric* block, untouched by this fix) are unaffected.

### 1.3 `Copy of Data`: a different, larger, and *not*-safe-to-fix problem

`Copy of Data`'s header (after dropping its one unnamed column) is: `PLAY #, ODK, OFF FORM, Off Str, OFF PLAY, DN, DIST, Drive Success, YARD LN, FH, RESULT, TARGET ROUTE, RECEIVED BY, GN/LS` — **14 columns**, vs. `Data`'s 15: `PLAY #, ODK, OFF FORM, Off Str, OFF PLAY, DN, DIST, YARD LN, RESULT, Drive Success, TARGET ROUTE, RECEIVED BY, GN/LS, Thrown By, YAC`. `YARD LN` and `Drive Success` are in **swapped relative order**, there is an extra `FH` column with no `Data`-tab equivalent, and `Thrown By`/`YAC` are absent entirely. This is a **new finding**, not previously documented — `docs/hc-workbook-ingest.md` only noted that the two tabs "mix two row types under one header," not that the header itself differs between tabs.

Row-level inspection (rows 2-61) additionally shows a qualitatively different structure than `Data`'s one large pair block: dozens of short numeric micro-blocks (2-40 rows each, real PLAY#-reset boundaries) immediately followed by 1-3-row pair "marker" rows, repeating roughly 155 times across the sheet. Whether those marker rows are drive-boundary annotations belonging to the surrounding numeric stretch, or genuinely separate games, cannot be determined without knowing the tab's true column layout — which is exactly Frage 2's open question, now sharpened by the header-mismatch finding above.

**Recommendation: do not attempt a segmentation fix for `Copy of Data` in this phase.** Applying the same unordered-pair heuristic here would help meaningfully *within* individual pair micro-blocks (e.g. block 13: 99 raw slices -> 10 unordered groups; block 55: 24 -> 8) but does not address the more fundamental block-alternation pattern or the unresolved column layout, and risks manufacturing a false sense of precision the underlying data does not support — the same principle `docs/hc-workbook-ingest.md` already applied to this exact tab ("ihnen manuell eine game_id zuzuweisen würde eine Genauigkeit vortäuschen, die die Daten nicht hergeben"). Keep all of `Copy of Data`'s games provisional/excluded from the trainable corpus this phase; log the header-mismatch finding as a sharper, more actionable version of Frage 2 for the HC.

### 1.4 Net effect on the game count

| Component | Today | After Wave-1 fix (this session's scope) |
|---|---:|---:|
| `Data`-tab pair block 0 | 137 | 22 |
| `Data`-tab numeric blocks 1+3 | 151 | 151 (unchanged, already correct) |
| `Data`-tab noise blocks 2+4 | 2 | 2 (recommend dropping as non-games, out of scope for this research) |
| `Offense Analytics` | 35 | 35 (unchanged, already correct) |
| `Germany Analytics EC2025` | 3 | 3 (unchanged, blocked on Frage 1, but count itself plausible) |
| `Copy of Data` (all blocks) | ~1,940 | ~1,940 (unchanged — deliberately deferred) |
| **Total** | **2,128** | **~2,013** |

**Honest framing for the deliverable:** fixing the one segmentation bug this session could safely pin down and verify only moves the total from 2,128 to ~2,013 — most of the inflation is locked behind Frage 2. The *trainable, non-provisional* game count (once `half` is also fixed) is the more meaningful number for the HC sync: roughly 22 (fixed pair block) + 151 (already-clean numeric blocks, minus the 9 legacy duplicates) + 35 (Offense Analytics) ≈ **199 net-new HC games** for October, before `Copy of Data` and the two open Frage answers unlock the rest. State this range, not "2,128 games fixed," in `docs/epa-refinement-2026-10.md`.

## 2. `half` strategy: mechanism, evidence, and a refined recommendation

### 2.1 `half` is not just a feature column — it drives label construction

`[VERIFIED: local code read, features/mutations.py:407-424]`:

```python
def _mark_half_end(df: pl.DataFrame) -> pl.DataFrame:
    ...
    return df.with_columns(
        half_end=(pl.col("index") == pl.col("index").max().over(["game_id", "half"]))
        .cast(pl.Int32)
    ).with_columns(
        game_end=pl.when((pl.col("half_end") == 1) & (pl.col("half") == 2))
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
    )
```

`prepare_ep_data` (EP) and `prepare_wp_data` (WP) **both** call `_mark_half_end`. `prepare_ep_data` uses `half_end` to place a `"No_Score"` sentinel at the true end of each half (line 446: `.when((pl.col("half_end")==1) & (pl.col("scoring_play")==0)).then(pl.lit("No_Score"))`), which is then `backward_fill().over("game_id")` — i.e. every earlier scoreless drive in the game inherits the *next* scoring-event marker it finds, and that marker exists at halftime specifically so a first-half drive is never credited with a second-half score. `prepare_wp_data`/the WP label path backward-fills the final score from `game_end` rows to build `Winner`.

**Consequence:** if `half` is `null` for every row of an HC game (today's actual state), `.over(["game_id", "half"])` groups all of that game's rows into a single null-half group. `half_end` fires once, at the true last row — but `game_end` (which additionally requires `half == 2`) **never fires**, because `null != 2`. This breaks WP's `Winner` resolution for that entire game, independent of `half` ever being in `WP_FEATURES` — CONTEXT's framing ("the `half` feature was adopted for EP only... rejected for WP") describes the *feature-selection* decision correctly but does not capture this *label-construction* dependency, which affects WP too.

### 2.2 Why the constant sentinel must be `2`, not `null` and not `1`

Setting `half` to a single constant value for an entire HC game (rather than leaving it `null`) is required in any case, because `model/train.py`'s `_train` calls `.drop_nulls()` on the selected training columns (line 229) before fitting — a literal `null` `half` value, even after passing `half_assigned` some other way, would be silently dropped at training time regardless of ingest-side quarantine outcome.

Given a constant is required, the value matters:

| Sentinel | `half_assigned` check | `game_end` fires at true last row? | WP `Winner` resolves? | EP "No_Score at halftime" preserved? |
|---|---|---|---|---|
| `null` | FAIL (quarantines, current state) | No | No | No |
| `1` | PASS | No (`game_end` requires `half==2`) | No | No |
| **`2`** | **PASS** | **Yes** | **Yes** | No (by construction — no real halftime boundary exists) |
| `3` (or another out-of-{1,2} value) | FAIL | No | No | No |

`half = 2` (constant, for the whole game) is the only sentinel that both (a) passes `half_assigned` legitimately, matching HC-D05's "never wave through a real structural gap" principle — this is not a bypass, `half_assigned`'s actual contract (`half ∈ {1, 2}`) is genuinely satisfied — and (b) keeps `game_end`/`Winner`/EP's post-game `None`-ing (`epa`/`ep` set to `None` at `game_end`, `features/mutations.py:764-772`) correct. The unavoidable cost: no halftime "No_Score" marker exists for these games, so a drive that in reality ended scoreless at halftime will instead be backward-filled with whatever the *next actual score* in the game is — a real, bounded label-quality degradation, most relevant for the `Scoring Probability` team-pair games (real international games, which do have a true halftime) and largely moot for camp/scrimmage segments (which likely have no formal two-half structure to begin with).

### 2.3 Feature-column implication: decouple label construction from the model feature

Reusing the same `half=2` value as the literal `EP_FEATURES` input has a secondary cost: it makes every HC row informationally indistinguishable from a genuine second-half play in the one feature XGBoost sees, diluting whatever `half==2`'s real signal was (measured +0.00179 pooled log-loss in the 1.3 adoption run, `hyperparams.py:38`). **Recommended implementation:** compute `half_end`/`game_end`/`scoring_event`/`Next_Score_Half`/`Winner` using the constant-`2`-for-labels value (as above), then, immediately before `EP_SELECTED_COLUMNS`/`WP_SELECTED_COLUMNS` selection, overwrite the `half` feature value for HC-sourced rows with a distinct sentinel (e.g. `0`, still an integer XGBoost can split on) so the model can learn "unknown half" as its own signal rather than blending into real half-2 plays. This is a small, localized change (one `pl.when(pl.col("source").str.starts_with("hc_workbook")).then(0).otherwise(pl.col("half"))` after label construction, before feature selection) — flag as `[ASSUMED]` design recommendation, not yet locked; the planner should size this as its own task rather than folding it silently into the label-construction fix, since it touches the feature the 1.3 adoption measurement was based on.

### 2.4 Option (b) — heuristic half-midpoint from play count: evidence is weak (n=2)

Tested directly on the only two games with a real `half_boundaries.csv` entry:

| Game | Total rows | `half2_first_play` | Fraction |
|---|---:|---:|---:|
| `2025-09-27_AUT-vs-UKR_EC25.csv` | 76 | 39 | 51.3% |
| `2025-09-27_SLO-vs-ITA_EC25.csv` | 91 | 43 | 47.3% |

Both land within ~4 percentage points of the literal midpoint — directionally supportive of a "halfway by play count" heuristic — but this is **`[ASSUMED]`, n=2**, both from clean Hudl-charted games with no bearing on whether HC's messier, hand-charted, possibly-uneven-tempo games (or camp scrimmages, which may have no fixed-duration halves at all) would split similarly evenly. Recommend **not adopting (b)** for October; if pursued later, it needs validation against a larger n and should be marked provisional in any output table per CONTEXT's own framing of option (b).

### 2.5 Recommendation

Adopt the refined version of option (a) — constant `half=2` for label construction, decoupled sentinel for the feature column — as the October-safe default, documented explicitly as a label-quality caveat (no halftime boundary for HC games) rather than a silent imputation. Pursue option (c) (ask the HC for half markers, Frage 4) in parallel per CONTEXT's own instruction; nothing above changes that recommendation, it only specifies what "(a)" must mean in code to avoid a WP-label regression the CONTEXT text did not anticipate.

## 3. Validation checks against camp/scrimmage data — per-source check profile is not currently supported

`validation/checks.py` registers exactly six checks (`downs_range`, `yardline_range`, `half_assigned`, `monotonic_drive_ids`, `gapless_play_ids`, `score_reconstruction`, `checks.py:332-339`) and gates quarantine through `partition_games`'s single `warn_only_sources: frozenset[str]` parameter (`checks.py:358`) — **all-or-nothing per source**, not per check. `hc_workbook` is deliberately absent from `_WARN_ONLY_SOURCES` (`pipeline.py:92`, HC-D05).

Real-run failure counts (`docs/hc-workbook-ingest.md`, confirmed unchanged this session — the workbooks are read-only in this research):

| Check | Failing HC games (of 2,128) | Root cause / expected after Wave-1 fixes |
|---|---:|---|
| `half_assigned` | 2,128 (100%) | Fully fixed by the §2 sentinel — this check's contract (`half ∈ {1,2}`) will be genuinely satisfied, not bypassed |
| `gapless_play_ids` | 1,279 | Two distinct causes: (1) fragmentation artifacts — a merged/larger game after the §1 segmentation fix gets a freshly-synthesized `1..N` `PLAY #`, which is gapless by construction (`_fill_synthesized_play_ids`); this component shrinks with segmentation fixes. (2) **Real charting gaps in genuinely numeric PLAY#-charted blocks** (Offense Analytics' real numbering "hat streckenweise Lücken," per `docs/hc-workbook-ingest.md`) — this component is a true data-quality finding and should **continue to quarantine**, not be waved through; it is the check doing its job. |
| `downs_range` | 172 | Plausibly concentrated in `Copy of Data` given the header-mismatch finding (§1.3) — misaligned columns could land garbage values in the `DN` position. Not confirmed this session (would require resolving Frage 2 first); flag as a hypothesis, not a fact. |
| `monotonic_drive_ids` | 8 | Minor; not investigated further this session (small n, low leverage) |
| `score_reconstruction` | 0 evaluated (all SKIPPED) | Already the "honest but not blocking" case by design — `score_reconstruction`'s own logic (`checks.py:275-330`) returns `Status.SKIPPED` (not `FAIL`) when no `final_scores.csv` reference row exists, which is universally true for HC games. **No change needed here.** |

**Recommendation:** do not add a per-check profile mechanism to `partition_games` in this phase — the existing `warn_only_sources`-per-source design already correctly keeps HC games non-warn-only (HC-D05's explicit intent), and `score_reconstruction` already degrades to SKIP rather than FAIL for exactly this reason. The two checks that need attention (`half_assigned`, and indirectly `gapless_play_ids`'s fragmentation-driven component) are addressed by §1/§2's fixes, not by weakening the check itself. Report the *remaining* `gapless_play_ids`/`downs_range` failures after Wave 1 honestly in `docs/epa-refinement-2026-10.md` as real data-quality findings, not as a validation gap to route around.

## 4. HC method reproduction (`SP by D&D`, `EP by D&D`, `Reg`, clustered/weighted tabs)

All content below is aggregate numeric/formula content only — `[VERIFIED: openpyxl inspection this session, data_only=True for values / data_only=False for formulas]`, no PII.

### 4.1 Sheet inventory

`Scoring Probability by Situation 2023-2026.xlsx` sheets: `Data`, `Copy of Data`, `SP by D&D`, `EP by D&D`, `Sample Size by D&D`, `SP by D&D Clustered`, `EP by D&D Clustered`, `General Stats`, `Sample Size by D&D Clustered`, `SP by D&D weighted`, `EP by D&D weighted`, `Reg`, `1st down OppH EPA`, `2nd down OppH EPA`, `3rd down OppH EPA`, `D&D Frequency weighted`, `GainLoss Frequency`, `GainLoss Percentage`.

### 4.2 `SP by D&D` / `Sample Size by D&D` — the empirical core

Both tabs share the same axis structure: rows = distance-to-go (1-14+, in whole yards), columns = down (1-4) x field half (`Own Half` / `Opposite Half`, i.e. which side of midfield the play started on). `SP by D&D` holds empirical scoring probabilities (0-1 floats); `Sample Size by D&D` holds the matching raw counts (e.g. down=1/distance=1/own-half: SP=0.667 on n=21; down=10/distance=1/own-half: n=324). This directly supports EPA-D03's "report n per cell, show where small samples make his point estimates noisy" — several own-half cells at distance >=10 have n in the low teens, next to opposite-half cells with n in the hundreds for the same (down, distance).

**Recommendation for reproduction:** build the same two axes (down x distance x own/opponent-half) from our canonical corpus, with the identical bin edges (distance 1..14 individually, then whatever the corpus's own natural range extends to), and place model EP/SP next to his empirical numbers and sample sizes in one combined table — exactly EPA-D03's ask, and directly mechanical once the axis definition is copied.

### 4.3 `SP by D&D Clustered` / `Sample Size by D&D Clustered` — a real Excel bug in the bin labels

Both clustered tabs show the same corrupted row labels for their first three distance bins:

```
datetime.datetime(2021, 1, 5, 0, 0)   -- likely originally typed "1-5"
datetime.datetime(2021, 6, 10, 0, 0)  -- likely originally typed "6-10"
datetime.datetime(2021, 11, 15, 0, 0) -- likely originally typed "11-15"
'16-19' / '16-20'  (own-half / opposite-half columns use slightly different bin edges)
20.0 / '21-25'
'20+' / '25+'
```

Excel silently reinterpreted range-shaped text (`"1-5"`, `"6-10"`, `"11-15"`) as dates once a `MM-DD` pattern matched a valid calendar date, using the current year at entry time (2021) as the implicit year — this is a well-known Excel autocorrect trap, not a data-entry error by the HC. The bin boundaries above **are `[ASSUMED]`**, reconstructed from the pattern of the surviving, uncorrupted labels (`16-19`, `20`, `20+`) — do not present them as read facts in the deliverable; note the corruption explicitly and show the inferred boundaries with the reasoning, so the HC can confirm or correct them in one sentence.

### 4.4 `Reg` tab — per-down polynomial trendlines, not a principled regression

`data_only=False` inspection of `Reg`'s formula cells (columns: `Half, Down, Distance, Code, Emp, Forecast, Diff, Reg, Diff, Chosen Ones`) shows two parallel smoothing methods per (half-side, down) block:

- `Forecast` (column F): a live Excel `=FORECAST(x, known_ys, known_xs)` call — ordinary least-squares linear regression — against a **fixed row range in `Copy of Data`** that changes per down block (e.g. down=1: `'Copy of Data'!I$2:I$735`; down=2: `I$736:I$1376`; down=3: `I$1376:I$1867`; down=4: `I$1868:I$2180`), multiplied by 6 (a touchdown's point value — implying `Copy of Data` column I is some 0/1-ish scoring indicator).
- `Reg` (column H): a **hardcoded polynomial formula**, literal coefficients, increasing in degree per down and per half-side: `-0.0595*Distance + 4.23` (down 1, own-half side) up to `3.94 + -0.293*Distance + 0.00602*Distance^2` (down 4, own-half) and as high as a degree-6 polynomial for down 4 on the "opposite half" side (`5.62 + -0.0242*C -0.00585*C^2 + 0.000078*C^3` for down 2 opposite-half, escalating further for down 4). The pattern — increasing polynomial order specifically for the noisier higher-down cells — reads like a hand-transcribed Excel chart-trendline equation (right-click a scatter series -> "Add Trendline" -> pick a polynomial order that visually fits -> copy the displayed equation into a cell), not a systematically-fit model.

**Recommendation:** do not attempt to literally reproduce the `Reg` tab's polynomial-trendline methodology — it is exactly the kind of ad-hoc, per-cell, increasing-order fit that the HC's own pitch ("professioneller und nachhaltiger gestalten," CONTEXT `## Specific Ideas`) is asking to move past. Instead, compute the same empirical `SP by D&D`/`Sample Size by D&D` axes from our corpus (§4.2) and place the model's smooth, cross-validated EP/WP predictions next to both his raw empirical numbers and his `Reg`-tab smoothed numbers — this is the comparison EPA-D03 already specifies, and it makes the contrast (principled CV'd model vs. ad-hoc per-cell polynomial) visible without editorializing.

### 4.5 Snapshot format for `data/reference/hc_sp_tables/*.csv`

Every tab inspected above (`SP by D&D`, `Sample Size by D&D`, the two `Clustered` variants, `Reg`) contains only down/distance/field-half/probability/count/regression-coefficient values — no team names, no player names, no dates beyond the corrupted bin-label artifact in §4.3. All are safe to snapshot verbatim as read-only CSVs per EPA-D03, with the corrupted `Clustered` bin labels replaced by their `[ASSUMED]`-reconstructed text form (documented inline) rather than the raw `datetime` values.

## 5. Training / comparison design

### 5.1 Evaluation protocol is LOGO, not a small fixed K — the "frozen folds" ask needs reframing

`docs/model-training.md` §1 and `model/evaluate.py::run_logo` confirm every reported EP/WP metric comes from **leave-one-game-out** (`sklearn.model_selection.LeaveOneGroupOut` over `game_id`), not a small fixed `GroupKFold(n_splits=k)` — that grouped-CV variant (`sklearn.model_selection.GroupKFold`, `INNER_CV_FOLDS`) is used only inside `_tune`'s inner hyperparameter search loop (`train.py:376-411`), never for the reported/logged metric. CONTEXT's "frozen GroupKFold folds for the with/without-HC ablation" (EPA-D04) is best read as: **the set of `game_id`s included in each arm must be pinned and reported**, not that a shared K-fold split object needs to be constructed — LOGO is exhaustive over whatever games are in the input frame, so "frozen" here means "the corpus composition per arm is explicit and reproducible," which `training_data_sha256` (already logged per run, `docs/model-training.md` table) already provides.

### 5.2 Ablation implementation — no new training-harness code needed

`train_ep`/`train_wp` (`model/train.py:124-183`) take a `plays: pl.DataFrame` directly, not a source filter parameter. The with/without-HC ablation is two calls with different input frames:

```python
# Source: local read of model/train.py:124-183 (existing signature, no changes needed)
without_hc = plays.filter(~pl.col("source").str.starts_with("hc_workbook"))
run_id_without = train_ep(without_hc, config, ...)   # arm 1
run_id_with = train_ep(plays, config, ...)           # arm 2 (full corpus)
```

Both calls log to the same `config.train.ep_experiment` MLflow experiment by default (`model/train.py`); tag each run explicitly (`mlflow.set_tag("corpus_arm", "without_hc" | "with_hc")`, or use a dedicated `ep_model_hc_ablation` experiment name mirroring the existing `{ep,wp}_model_candidates` pattern `docs/model-training.md` §4 already establishes for feature-candidate measurement) so the two runs are distinguishable in the MLflow UI without relying on run-id memorization. Neither arm needs to touch the `champion` alias — `ffep promote` stays a separate, explicit, human-reviewed step (`registry.py`), and the previous (pre-HC) champion version stays resolvable exactly as CONTEXT requires, since MLflow registry versions are never overwritten (only the alias moves).

### 5.3 Per-corpus / per-source calibration reporting — already built, reuse verbatim

`model/evaluate.py::per_source_metrics` (`evaluate.py:231-273`) already computes per-source log-loss vs. each source's own naive class-frequency baseline, plus a pooled `__pooled__` row — this is logged today as a Markdown artifact per training run (`train.py`'s `_render_markdown_table` call). No new code is needed for "calibration reporting per corpus" — call this function on the with-HC run's out-of-fold predictions and the `source` column already carried through (EPA-D04), and it produces exactly the per-corpus breakdown the deliverable needs.

### 5.4 Competition-tier assignment for HC games — **new gap found this session**

`data/reference/competition_tier.csv` currently has exactly three rows:

```
source,competition,tier
ifaf,IFAF World Flag 2026,womens-international
legacy,legacy,mixed-other
legacy-sportapp,FlagWC,mixed-other
```

`reference.py::map_competition_tier` (`reference.py:378-416`) left-joins this file onto the training frame on `(source, competition)` and **raises `UnmappedCompetitionError`** listing every unmapped pair — it never lets a null `competition_tier` pass through. `hc_workbook.py::ingest_workbook` stamps `source` (e.g. `hc_workbook:scoring-probability-by-situation-2023-2026:data`) and `competition` (from `hc_games.csv`'s `competition` column, currently `legacy` for the 9 confirmed duplicate rows and `null`/unset for every provisional game) onto every accepted row — but critically, `hc_games.csv`'s own `tier` column (which exists, e.g. `mixed-other` on the 9 duplicate rows) is **resolved but never written to the output frame** (`hc_workbook.py:961-975` stamps `source`/`competition`/`season`/`game_id`/`game_date`/`home_team`/`away_team`, not `tier`). `competition_tier.csv` is therefore the *only* place tier actually gets attached at training time, and it currently has **zero rows matching any `hc_workbook` source**. The first HC row that clears validation and reaches `train_ep`/`train_wp` will hard-fail with `UnmappedCompetitionError` unless this is fixed.

**Recommendation:** treat this as a third, previously-undocumented Wave-1 gap alongside `half`/segmentation. For every distinct `competition` value that maintainer-added `hc_games.csv` rows will carry (a new value like `"HC Camp"`/`"HC Scoring Probability"` for genuinely new games, `legacy` for the 9 confirmed duplicates — already covered by the existing `legacy` row if `source` matching is coarsened, or a new `hc_workbook`-specific row if not), add a matching `competition_tier.csv` row with an appropriate `tier` (`mixed-other` is the existing precedent for `legacy`/`legacy-sportapp`; camps/scrimmages plausibly belong in the same tier absent evidence otherwise — flag as `[ASSUMED]`, confirm with the HC or leave as the planner's documented default).

## Package Legitimacy Audit

No new external packages are required for this phase. `openpyxl>=3.1.5`, `scikit-learn>=1.5.1`, `xgboost>=2.1.4`, `mlflow>=3.15`, `scipy>=1.14.1` are already pinned in `pyproject.toml` and already used by the exact code paths this phase touches (`ingest/hc_workbook.py`, `model/train.py`, `model/evaluate.py`). `[VERIFIED: pyproject.toml inspection this session]`.

| Package | Registry | Disposition |
|---|---|---|
| (none new) | — | N/A — phase reuses existing dependencies only |

**Packages removed due to slopcheck [SLOP] verdict:** none (no new packages).
**Packages flagged as suspicious [SUS]:** none.

## Common Pitfalls

### Pitfall 1: Treating "`half` adopted for EP only" as "the `half` fix only affects EP"
**What goes wrong:** Assuming WP training is unaffected by the `half` decision because `half` is absent from `WP_FEATURES`.
**Why it happens:** CONTEXT's own framing states the feature-adoption fact correctly but doesn't surface that `prepare_wp_data` also calls `_mark_half_end`, which gates `game_end`/`Winner`.
**How to avoid:** Any `half` fix must be validated against both EP's `No_Score`/`Next_Score_Half` chain and WP's `Winner`/label chain — test both, not just EP's reported log-loss.
**Warning signs:** WP training on HC rows silently drops every HC game at the final `.drop_nulls()` (all-null `Winner`) with no error, only a smaller-than-expected `n_folds`.

### Pitfall 2: Regenerating `hc_games.csv` by hand after a segmentation rule change
**What goes wrong:** A segmentation rule change (§1.2) changes every `block_key` in the affected block — hand-editing the 9 existing declared-duplicate rows risks silently mismatching a `block_key` that no longer exists.
**Why it happens:** `block_key` is derived from `game_index` within a block (`b{block_index:02d}-g{game_index:02d}`), which is exactly what the segmentation rule change re-numbers.
**How to avoid:** Re-run `ffep ingest --source hc_workbook` after the code change and let the pipeline's "unknown game" notices drive the new `hc_games.csv` rows, per the existing `## Wartung` maintenance procedure in `docs/hc-workbook-ingest.md` — do not port old `block_key`s forward.
**Warning signs:** A previously-confirmed duplicate (`legacy-39..47`) silently stops matching after the segmentation change (it shouldn't — those 9 live in the *numeric* block 1, untouched by the pair-block fix — but verify this explicitly rather than assuming).

### Pitfall 3: Assuming `competition_tier.csv`'s absence for `hc_workbook` will fail loudly at ingest time
**What goes wrong:** Ingest (`ffep ingest`) succeeds and accepts HC rows into `plays.parquet` even with zero matching `competition_tier.csv` rows — the `UnmappedCompetitionError` only fires later, inside `train_ep`/`train_wp`'s `_build_competition_tier` hook.
**Why it happens:** Tier mapping is a training-time concern (`model/train.py::_build_competition_tier`), not an ingest-time or validation-check concern — it is not one of the six `checks.py` checks.
**How to avoid:** Add the `competition_tier.csv` rows in the same Wave-1 pass as the `half`/segmentation fixes, before the first training run is attempted — otherwise the first `ffep train` against the enlarged corpus fails opaquely partway through, after the (potentially slow) LOGO fit has already started.
**Warning signs:** `ffep train` raises `UnmappedCompetitionError` naming `(source=hc_workbook:..., competition=...)` pairs partway through a run.

### Pitfall 4: Reading the `SP by D&D Clustered` bin labels literally
**What goes wrong:** Presenting `2021-01-05` as a real date in the comparison deliverable, rather than recognizing it as a corrupted `"1-5"` distance-bin label.
**Why it happens:** `data_only=True` openpyxl reads return the *evaluated* cell value, which for an Excel-autocorrected date is a genuine `datetime` object — nothing in the read path signals "this was probably typed as text."
**How to avoid:** Cross-check every `Clustered`-tab row label against the surviving uncorrupted labels' pattern (`16-19`, `20`, `20+`) before using it in `docs/epa-refinement-2026-10.md`; mark reconstructed labels `[ASSUMED]`.
**Warning signs:** Any cell value in a "distance bin" column that is a `datetime.datetime` instance.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest >= 8 (`pyproject.toml`) |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` |
| Quick run command | `uv run pytest tests/test_ingest_hc_workbook.py tests/test_validation_checks.py tests/test_model_train.py tests/test_model_evaluate.py -q` |
| Full suite command | `uv run pytest tests -q` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| HC-03 | Unordered-pair segmentation reduces `Data`-tab pair-block fragment count (137 -> 22 on the real workbook) | unit (synthetic fixture mirroring the real pattern) | `uv run pytest tests/test_ingest_hc_workbook.py -k segment_games -x` | Partial — `tests/test_ingest_hc_workbook.py` exists, needs a new possession-swap-pattern fixture |
| HC-03 | `half=2` sentinel makes `half_assigned` PASS and `game_end` fire exactly once, at the true last row | unit | `uv run pytest tests/test_model_train.py -k half -x` | Needs new test — no existing test pins this |
| HC-03 | `competition_tier.csv` covers every `hc_workbook` `(source, competition)` pair that reaches training | integration | `uv run pytest tests/test_model_train.py -k competition_tier -x` | Needs new test |
| HC-03 | With/without-HC ablation runs both log to MLflow with distinguishable tags and comparable `per_source_metrics` output | integration (may use `mlflow` local file store fixture, consistent with existing `test_model_train.py` patterns) | `uv run pytest tests/test_model_train.py -k ablation -x` | Needs new test |
| HC-03 | HC SP/EP-by-D&D snapshot CSVs (`data/reference/hc_sp_tables/*.csv`) contain no PII and match the workbook's aggregate values | unit (PII gate, extend existing pattern) | `uv run pytest tests/test_m3_hc_pii.py -x` | Existing file, needs new assertions for the new snapshot path |

### Sampling Rate
- **Per task commit:** the quick-run command above.
- **Per wave merge:** `uv run pytest tests -q` (full suite; existing `docs/hc-workbook-ingest.md`/M3-01 precedent documents this run taking a long time past ~67% collection on this machine — plan for it, do not block phase completion on it per the M3-01 precedent).
- **Phase gate:** full suite green before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `tests/test_ingest_hc_workbook.py` — add an unordered-team-pair possession-swap fixture (mirroring the real `Germany/Ireland` <-> `Ireland/Germany` alternation pattern) to pin the segmentation fix's behavior before implementing it (TDD RED, per this codebase's established pattern).
- [ ] `tests/test_model_train.py` — add a `half=2`-sentinel fixture covering both EP's `No_Score`/`Next_Score_Half` chain and WP's `Winner` chain.
- [ ] `tests/test_model_train.py` — add an `UnmappedCompetitionError`-covering fixture for an `hc_workbook` source with no matching `competition_tier.csv` row, to pin the Pitfall-3 failure mode before the fix.
- [ ] No new framework install needed — pytest is already configured and the existing `tests/test_ingest_hc_dedupe.py`/`tests/test_ingest_hc_workbook.py`/`tests/test_model_train.py` establish the exact fixture patterns (synthetic in-process `openpyxl` workbooks, never the real gitignored files) to extend.

## Sources

### Primary (HIGH confidence)
- Local execution against the real, gitignored HC workbooks this session (`uv run python`, `openpyxl.load_workbook(..., data_only=True/False, read_only=True/False)`) — segmentation counts, header layouts, `Reg`-tab formulas, `SP`/`Sample Size`-tab values.
- `src/flag_football_ep/ingest/hc_workbook.py` (read in full) — `segment_blocks`, `_split_pair_block`, `_split_numeric_block`, `segment_games`, `resolve_game_identity`, `ingest_workbook`.
- `src/flag_football_ep/features/mutations.py` (`_mark_half_end`, `prepare_ep_data`, `prepare_wp_data`) — label-construction dependency on `half`.
- `src/flag_football_ep/validation/checks.py` (all six checks, `partition_games`) — quarantine mechanics.
- `src/flag_football_ep/reference.py` (`map_competition_tier`) — the `UnmappedCompetitionError` finding.
- `src/flag_football_ep/model/{train,evaluate,hyperparams,registry}.py` — training/ablation/reporting machinery.
- `docs/hc-workbook-ingest.md`, `.planning/phases/M3-01-hc-workbook-ingest/M3-01-04-SUMMARY.md`, `docs/hc-rueckfragen-2026-09.md`, `docs/model-training.md`, `docs/data-contract.md` — prior real-run findings and locked methodology, cross-checked against this session's own execution.

### Secondary (MEDIUM confidence)
- Interpretation of the `Copy of Data` interleaved-block pattern as "per-play team-pair annotations vs. genuinely separate games" — grounded in direct observation but ultimately blocked on the HC's Frage 2 answer.

### Tertiary (LOW confidence)
- Reconstructed `Clustered`-tab distance-bin boundaries (`"1-5"`/`"6-10"`/`"11-15"`) — inferred from the surviving labels' pattern, not read directly (the literal cells are corrupted `datetime` values).
- Play-count-midpoint heuristic for `half` (option (b)) — n=2 games only.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `half = 2` (constant) is the correct sentinel for HC-game label construction, decoupled from a separate feature-column sentinel | §2.2-2.3 | If wrong, WP `Winner`/EP `No_Score` construction could silently mislabel HC games' terminal drives; the decoupling itself is a new implementation not yet reviewed by the user |
| A2 | Reconstructed `Clustered`-tab bin boundaries (`"1-5"`, `"6-10"`, `"11-15"`) | §4.3 | If wrong, the snapshot table in `data/reference/hc_sp_tables/` mislabels three distance-bin rows; low real-world impact (aggregates only, HC can correct in one sentence) |
| A3 | `Copy of Data`'s 1-3-row team-pair markers are drive/possession annotations, not genuinely separate games | §1.3 | If wrong, deferring `Copy of Data` segmentation entirely (rather than attempting a partial fix) leaves more usable HC data on the table than necessary; low risk since the current default (fully provisional) is already conservative |
| A4 | Camps/scrimmages plausibly belong in the `mixed-other` competition tier, same as `legacy`/`legacy-sportapp` | §5.4 | If wrong, HC games get systematically mis-tiered in the tier-covariate feature; low individual-row impact, moderate if it skews the tier comparison table in the deliverable |
| A5 | `S`/`F` team-abbreviation noise rows (Spain vs. Switzerland; Finland vs. France) cannot be safely disambiguated without an alias table confirmed by evidence | §1.2 | Low risk — recommendation is explicitly to leave these as their own small residual games rather than guess, so a wrong assumption here costs completeness, not correctness |

## Open Questions

1. **Frage 1 (EC-2025 `Data` tab empty) and Frage 2 (`Scoring Probability` team-pair column layout)** — both still open per `docs/hc-rueckfragen-2026-09.md`; Frage 2 is now sharpened by this session's header-mismatch finding (`Copy of Data` vs. `Data` column order differs). Recommendation: route the sharpened Frage 2 finding back through the same `docs/hc-rueckfragen-2026-09.md` channel before attempting any `Copy of Data` segmentation work.
2. **Whether the HC's `Reg`-tab polynomial coefficients were literally hand-transcribed from Excel chart trendlines, or fit some other way** — inferred from the pattern (increasing polynomial order for noisier down-4/opposite-half cells) but not confirmed with the HC. Low stakes: the recommendation (§4.4) is to not reproduce this method regardless of its provenance.
3. **Exact competition-tier value(s) HC games should carry** (§5.4, A4) — needs either an explicit planner default or a one-line HC confirmation; blocks the first successful `ffep train` on the enlarged corpus if left unresolved.

---
*Phase: M3-02-EPA-Refinement*
*Valid until: 2026-10-03 (30 days — the underlying codebase/workbooks are static references, but the two open HC questions may resolve and invalidate the "defer `Copy of Data`" recommendation before then)*
