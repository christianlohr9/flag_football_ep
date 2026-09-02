# Phase M2-4: Messvorschrift - Research

**Researched:** 2026-09-02
**Domain:** Continuous multi-object-tracking quality metrics without identity ground truth; scoring-script CLI design
**Confidence:** MEDIUM-HIGH (label-free metric design is derived from the project's own measured data; label-based library research is HIGH via PyPI/GitHub verification)

## Summary

The phase adds a continuous, label-free metric to `scripts/hackathon/score_tracks.py` alongside the existing pass/fail threshold, split by dev/test in one run, plus a ready (but not yet wired-in) interface for a label-based standard measure (IDF1/HOTA-style) for when M2-3 identity labels exist.

The most important finding is a hard ceiling on what ANY label-free metric can see: this project's own baseline measurement (`docs/baseline-messung.md`) shows the dominant real failure mode is an **identity swap during a crossing/overlap** (39 of 46 failed pilot clips) where neither track dies and neither a new track is born — the tracker's bipartite matcher just hands one player's track ID to the wrong body mid-overlap. No track-based, label-free signal (fragment count, ID churn, active-track count) can detect this, because both tracks stay alive and continuous throughout. This is exactly why CONTEXT.md's two-layer design exists, and the research below treats that ceiling as a documented, load-bearing fact rather than a caveat to bury in a footnote.

Two things make METR-04 (show a difference the threshold swallows) unusually easy to satisfy without new measurement: (1) the human pass/fail threshold — the phase's only defined "Schwellenmetrik" — is undefined (`keine Review`) for 3 of the 4 non-BoT-SORT methods already measured in M2-2, while every automatic/continuous statistic in `data/reference/baseline-methods/{summary,per_clip}.csv` is defined for all 4; (2) `pip index`/`gh api` confirm both `motmetrics` (MIT, mature, IDF1+MOTA, numpy/pandas/scipy-only) and `trackeval` (MIT, HOTA, but pulls opencv-python/pycocotools) exist and are legitimate PyPI packages for the label-based interface, with `motmetrics` the better fit for a torch-free, laptop-in-seconds scoring path.

**Primary recommendation:** Reuse `continuity.py`'s existing per-clip statistics (already computed, already free of identity labels) as the continuous metric's inputs rather than inventing new track-parsing logic. Report `mean_fragments_per_expected_player` (normalizes `n_fragments` by the 5v5 constant `expected_players=10`) as the primary continuous metric, and `active_track_count_deviation` (mean |simultaneous active track_ids − 10| per frame) as a guard metric that partially catches the over-merge failure mode the primary metric is blind to (demonstrated by GTA's 0-split/364-merge measurement). Document the crossing-swap blind spot explicitly in both the code and the challenge docs. Build the label-based interface (`motmetrics`-backed IDF1/MOTA) as an importable function tested only against synthetic fixtures, not wired into the live hackathon CLI path yet.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Continuous label-free metric computation | Standalone script tier (`scripts/hackathon/*`) | Library tier (`src/flag_football_ep/cv/continuity.py`, read-only reuse) | `score_tracks.py` is explicitly the one artifact every team runs unmodified; it must stay a thin caller of `continuity._measure_clip`, never a reimplementation (existing docstring convention) |
| Dev/test split resolution | Standalone script tier | Reference data tier (`data/reference/hackathon_split.csv`) | The split is a plain CSV; `score_tracks.py` must read it directly with `polars`, NOT via `cv.testset.read_hackathon_split`, to respect the file-collision guard (`cv/testset.py` is being built concurrently by 2.2 wave 7 plan 21 and is off-limits to M2-4) |
| Test-label access | Standalone script tier (explicit CLI path) | Label-vault tier (`data/private/test-labels/…`) | `score_tracks.py`'s own docstring convention: never resolve `Config`/`data/reference` paths itself — every input, including the vault path, is an explicit CLI flag supplied by the caller (a wrapper script or the team's own invocation), so private labels are never hard-coded into a public script |
| Label-based association metric (future) | Standalone script tier, new pure function | External library (`motmetrics`) | Not a new algorithm — delegate the Hungarian-matching-based IDF1/MOTA computation to a maintained MIT library rather than hand-rolling it; the phase only builds the interface + synthetic tests now |
| Challenge-description wording (METR-03) | Documentation tier (`docs/hackathon-challenge-reid.md`, `-formular.md`) | — | Pure prose change; no code dependency |

## User Constraints (from CONTEXT.md)

### Locked Decisions
- **Ground truth reality (from M2-2):** human continuity verdicts exist ONLY for BoT-SORT overlays (pilot 15/61); no per-frame identity labels exist yet (that is M2-3/DATA-03). The continuous metric must therefore be computable WITHOUT identity GT for the automatic view (e.g. fragment/switch counts per play from the tracks themselves) AND be defined so that, once identity labels exist, a standard measure (IDF1/HOTA-style association) plugs in with the same CLI surface. Plan both layers; implement the label-free layer now, the label-based layer as a ready interface with tests on synthetic data.
- **One run, both metrics, split by dev/test:** `score_tracks.py` reads the hackathon split (data/reference/hackathon_split.csv, plan 21) — dev = Panama Rojo 61 clips, private_test = Puerto Rico 61 clips — and reports threshold rate + continuous metric per split, n on every rate. Test-split scoring must work with the vaulted GT path (data/private/test-labels/…) without ever copying labels into public locations.
- **METR-03 wording lands in both challenge docs** (`docs/hackathon-challenge-reid.md`, `-formular.md`): acceptance criterion vs direction — the plan proposes the wording; the final BASE-04/METR-03 decision remains the user's checkpoint (formulated as a human-verify step at the end).
- **METR-04 comparison uses the M2-2 rows** (BoT-SORT vs ByteTrack-matched vs CBIoU-matched vs GTA) from data/reference/baseline-methods/per_clip.csv — no re-running trackers.
- **File-collision guard:** do not touch cv/*, docs/hackathon-bundles.md, .dvc/*, data/labels/**; `score_tracks.py` and scripts/hackathon/* are M2-owned; `docs/baseline-messung.md` may gain a section.
- German prose in docs; user global rules for commits.

### Claude's Discretion
- Exact continuous metric definition for the label-free layer (e.g. mean identity fragments per play normalized by expected players, or switches/play from track re-assignments) — must be reproducible from tracks + detections alone and documented with its blind spots (it cannot see silent merges — GTA caveat).
- Output format (JSON + Markdown table), CLI flags, test fixtures.

### Deferred Ideas (OUT OF SCOPE)
- Identity-label-based association metric with real GT — activates in M2-3 once DATA-03 labels exist.
- Transfer-domain scoring (TRANS-01).

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| METR-01 | Neben der Schwellenmetrik wird eine stetige Kennzahl ausgewiesen | Section "Label-Free Continuous Metric Candidates" — recommends `mean_fragments_per_expected_player` (primary) + `active_track_count_deviation` (guard), both reusing `continuity._measure_clip` |
| METR-02 | Das Evaluationsskript gibt beide Kennzahlen in einem Lauf aus, getrennt nach Dev und Test | Section "CLI/Format Design" — one `score_tracks.py` invocation reading `hackathon_split.csv` + explicit dev/test review/tracks paths, emitting JSON + Markdown |
| METR-03 | Die Challenge sagt, welche Kennzahl das Abnahmekriterium ist und welche die Zielrichtung | Section "Wording for METR-03" — SoccerNet/SportsMOT precedent for one acceptance metric + a directional secondary metric; exact insertion points identified in both docs |
| METR-04 | Ein Vergleichslauf zweier Verfahren zeigt, dass die stetige Kennzahl Unterschiede sichtbar macht, die die Schwellenmetrik verschluckt | Section "METR-04 Demonstration Without Re-Running Trackers" — the human threshold is literally undefined for 3/4 measured methods; per_clip.csv already differentiates them continuously |

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `motmetrics` | PyPI | first release 2017-04-13 (~9 yrs), latest 1.4.0 released 2022-12-26 (~3.5 yrs stale, but stable/mature) | ~31k/week [CITED: Snyk Advisor, 2026-09-02 lookup, not independently reverified via pypistats due to rate-limit] | `github.com/cheind/py-motmetrics`, license MIT [VERIFIED: `gh api repos/cheind/py-motmetrics` → `license.spdx_id=MIT`] | [OK] | Approved — recommended label-based-interface backend, dev-only optional dependency |
| `trackeval` | PyPI | first release 2025-08-20 (~1 yr old), latest 1.3.0 released 2026-03-03 | 9,656/month [VERIFIED: pypistats.org API, 2026-09-02] | `github.com/kovalp/TrackEval`, a fork of `JonathonLuiten/TrackEval` (the original HOTA paper's reference implementation); license MIT [VERIFIED: `gh api repos/kovalp/TrackEval` → `license.spdx_id=MIT`, `fork=true`, `parent=JonathonLuiten/TrackEval`] | [OK] | Approved but flagged: young PyPI packaging (~1 yr), the fork itself has only 2 GitHub stars (packaging wrapper, not the underlying research code which has thousands); pulls `opencv-python`+`pycocotools`+`matplotlib` — heavier than the "no torch, laptop in seconds" scoring-path constraint wants. Not recommended as the default; keep as an optional discretionary alternative if HOTA specifically is wanted later |

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none by slopcheck; `trackeval`'s PyPI packaging (as opposed to the well-known underlying research repo) carries a manual maintainer-risk note above — treat as `[SUS]`-adjacent for planning purposes even though the tool itself returned `[OK]`

Both packages were verified live: `python3 -m pip index versions motmetrics` → `1.4.0`; `python3 -m pip index versions trackeval` → `1.3.0`; `curl https://pypi.org/pypi/<pkg>/json` confirms `license: "MIT"` for both. `slopcheck install motmetrics trackeval` returned `[OK]` for both (the tool's own `pip install` step then failed locally only because this sandboxed Python environment has no `pip` binary — the scan itself, which is what matters for legitimacy, completed and printed `2 OK`).

**Neither package should become a hard runtime dependency of the phase.** METR-01/METR-02 need no identity labels and therefore no MOT-metrics library at all. Add `motmetrics` to the `dev` dependency group only (alongside `pytest`), guarded by an import-skip in the synthetic-data test so the base scoring path stays torch-free and installable in seconds, matching CONTEXT.md's "keep the scorer runnable by a team on a laptop in seconds; no torch dependency in the scoring path."

## Label-Free Continuous Metric Candidates

All candidates below are computable from `tracks` (+ `detections`, unused here) alone — no identity ground truth. Each is evaluated against the two known label-free failure modes: (a) **over-merge** — GTA's measured 0 splits / 364 merges across 61 clips, auto-continuity 61/61=100% but explicitly NOT human-confirmed (`docs/baseline-messung.md`); (b) **over-fragment** — many short-lived tracks born from re-detection after a lost association.

A **third, more important** failure mode exists in this dataset that is invisible to every candidate below: a **same-frame identity swap during a crossing/overlap**, where the tracker's per-frame assignment step (Hungarian/IoU matching) hands track A's ID to track B's detection mid-overlap. Neither track dies, no new track is born, the simultaneous active-track count is unchanged — the metric sees a perfectly continuous, perfectly-counted scene with the wrong labels attached. `docs/baseline-messung.md` attributes 39 of 46 pilot fail clips (~85%) to exactly this mode ("Identitätswechsel bei Spieler-Überlagerung"). This is the reason METR-01's "or a standard association-quality measure" clause and CONTEXT.md's two-layer design exist — no amount of clever label-free heuristics closes this gap; only identity ground truth (M2-3) or human review can.

| Candidate | Formula | Catches | Blind to | Confidence |
|-----------|---------|---------|----------|------------|
| **Fragments per expected player** (recommended primary) | `n_fragments / expected_players` (expected_players=10, the 5v5 constant already used as `IDEAL_TRACK_BAND[0]` in `baseline_common.py`); `n_fragments` is `continuity._measure_clip`'s existing count of tracks covering <50% of the clip | Over-fragmentation (tracker repeatedly loses and re-acquires) | Over-merge (GTA: 0 fragments looks perfect while 364 merges may have silently swapped identities); same-frame swaps | HIGH — reuses an already-tested, already-shipped statistic; only the normalization is new |
| **Active-track-count deviation** (recommended guard) | `mean(abs(n_active_track_ids(frame) - 10))` over all frames, or the already-computed `ideal_band_k/n` fraction (`IDEAL_TRACK_BAND=(10,14)`, `baseline_common.py`) | Partially catches over-merge: merging two players into one ID drops the simultaneous count below 10, which this metric penalizes (GTA's own `median_n_player_tracks=18` vs BoT-SORT's `23` in `summary.csv` is a real, measured symptom) | Same-frame swaps (count stays correct); does not distinguish a merge from natural occlusion where a body is briefly undetected | MEDIUM — directionally sound and grounded in measured GTA data, but the [10,14] band was defined descriptively (challenge doc), not validated as a quality signal in isolation |
| **Track-id churn (new IDs after frame k)** | count of `track_id`s whose first `frame_index` is > some threshold (e.g. after the play has "settled", say frame 30), normalized by clip length or expected players | Over-fragmentation, late-starting tracks (already partially covered by `n_tracks` vs `n_player_tracks` split in `baseline_common.py`) | Over-merge; same-frame swaps; also flags *legitimate* re-entries (players leaving/re-entering frame near the sideline) as false positives | LOW-MEDIUM — no threshold-k has been validated against this dataset; would need calibration against `continuity_review.csv`'s `id_switches` column before trusting the number |
| **Switch-event heuristic** (proximity re-birth) | count of `(track A ends at frame f, position p) → (track B starts within r px, n frames)` pairs per clip | The occlusion-then-reacquire pattern (track literally dies and a new ID is born nearby) — a real, distinct failure mode from the crossing-swap | The crossing-swap failure mode itself (no track ends in that case, so this heuristic fires zero times on the dataset's dominant error) — the two failure modes are structurally different and this heuristic only catches the minority one; also needs two tuned parameters (`r`, `n`) with no ground truth in this phase to calibrate them against | LOW — untested threshold choice, deliberately excluded from the recommended primary/guard pair, worth naming as a documented "not implemented, exploratory" option in the code's own docstring rather than shipping an uncalibrated heuristic as an official metric |

**Recommendation:** ship `mean_fragments_per_expected_player` as the one continuous metric METR-01 requires (singular — "eine stetige Kennzahl"), and report `active_track_count_deviation` (or its `ideal_band` fraction form, already computed) as a secondary/diagnostic column in the same JSON/Markdown output, explicitly labeled as a guard, not a second official acceptance-adjacent metric. Do not implement the switch-event heuristic in this phase — name it as a documented future option instead of shipping an uncalibrated heuristic.

**Both metrics build entirely on `continuity._measure_clip`'s existing return values** (`n_fragments`, and `n_tracks`/`n_player_tracks` computed the same way `baseline_common.player_track_counts` already does) — no new track-parsing logic needs to be written, only new normalization/aggregation on top of numbers the codebase already produces per clip.

## Correlation Analysis Outline (Research Question 3)

`data/reference/continuity_review.csv` already joins, per pilot clip, the automatic statistics (`n_tracks`, `longest_track_frac`, `n_fragments`, `auto_flag`) with the human `verdict` (46 fail / 15 pass, all 61 reviewed) and, for fail clips, `id_switches` (41 clips = 1 switch, 2 clips = blank/unrecorded, 1 clip = 2, 1 clip = 4, 1 clip = 5). This is enough to run — without any new measurement — a calibration check for the recommended primary metric:

1. **Point-biserial / Spearman correlation** of `n_fragments` (and `mean_fragments_per_expected_player`) against `verdict` (fail=0/pass=1) across all 61 dev clips.
2. **Spearman correlation** of `n_fragments` against `id_switches` restricted to the 44 fail clips with a recorded switch count.
3. **Expected outcome, stated honestly in advance:** given the already-measured saturation (`auto_flag=ok` 57/61 = 93.44% vs. human pass 15/61 = 24.59%, `docs/baseline-messung.md`), a weak or near-zero correlation between the *binary* `auto_flag` and `verdict` is expected and does not invalidate METR-01 — the binary threshold is known to be saturated. What matters for METR-01's purpose is whether the *continuous* `n_fragments` value (not thresholded into ok/fragmented) shows a directional (even if weak-to-moderate) monotonic relationship with `id_switches` — if it does, the metric has some construct validity; if it does not, that is itself a valid, reportable finding (METR-01 does not require the label-free metric to replace the human threshold, only to be continuous and directional).
4. This analysis can be written as a small script or notebook cell using `polars`/`scipy.stats.spearmanr`, or — more usefully for the phase's own validation — as an actual pytest assertion in Wave 0 (e.g., "the primary continuous metric is not constant across the 61 pilot clips" and "a clip with `id_switches>=4` has a `mean_fragments_per_expected_player` at or above the pilot median" as a smoke-level sanity check, not a strict statistical claim).
5. **Does it validate METR-04?** Only indirectly — METR-04 asks for a cross-*method* comparison (BoT-SORT vs ByteTrack vs CBIoU vs GTA), not a cross-*clip* correlation within one method's tracks. Use this analysis to build confidence that the metric means something on the one method that DOES have human labels (BoT-SORT); use the M2-2 `per_clip.csv` rows (see next section) to satisfy METR-04 itself.

## METR-04 Demonstration Without Re-Running Trackers

`data/reference/baseline-methods/summary.csv` (5 measured method/config rows) already contains everything needed:

| Method/config | Human threshold rate (`human_pass_k/n`) | Automatic threshold rate (`auto_ok_k/n`) | Continuous: `median_n_fragments` | Continuous: `ideal_band_k/n` |
|---|---|---|---|---|
| botsort-existing | 15/61 (24.59%) | 57/61 (93.44%) | 7.0 | 1/61 (1.64%) |
| bytetrack, baseline-matched | **keine Review — undefined** | 57/61 (93.44%) | 7.0 | 0/61 (0.00%) |
| bytetrack, defaults | **keine Review — undefined** | 55/61 (90.16%) | 9.0 | 0/61 (0.00%) |
| cbiou, baseline-matched | **keine Review — undefined** | 58/61 (95.08%) | 6.0 | 2/61 (3.28%) |
| gta | **keine Review — undefined** | 61/61 (100.00%) | 2.0 | 10/61 (16.39%) |

The phase's own defined "Schwellenmetrik" (per `docs/hackathon-challenge-reid.md`'s "Kern-Metrik" paragraph) is the **human** pass rate — and it is literally undefined for 4 of the 5 rows. The continuous metrics are defined for all 5 and clearly differentiate them (median fragments 2.0–9.0, ideal-band fraction 0%–16.4%). **This is the cleanest, already-measured demonstration of METR-04**: the threshold metric swallows every difference among the non-BoT-SORT methods (it has nothing to say about them at all), while the continuous metric ranks them.

A second, narrower illustration inside the automatic-only view: `bytetrack defaults` vs `bytetrack baseline-matched` — the (automatic) threshold moves from 55/61 to 57/61 (a visible but coarse 2-clip swing), while `median_n_fragments` moves from 9.0 to 7.0 and `ideal_band_k/n` stays flat at 0/61 both times — showing the continuous metric adds finer-grained signal even where the coarse threshold does register *some* movement.

**Caveat to carry into the plan, not bury:** GTA's numbers (best on both `median_n_fragments`=2.0 and `ideal_band`=16.4%) are exactly the case `docs/baseline-messung.md` warns about — 0 splits, 364 merges, no human confirmation, generic (non-sport-finetuned) embedding on sampled crops (median 12/track). Presenting GTA as "the winner" on the continuous metric without repeating this caveat would be the "quietly wrong headline number" failure mode the project's own conventions exist to prevent (`continuity.py`'s module docstring, `summarise_review`'s refusal to manufacture a partial-denominator pass rate). The plan should require the METR-04 write-up to carry this caveat verbatim, not just link to it.

## CLI/Format Design

**Constraint from `score_tracks.py`'s own docstring (existing convention, must be preserved):** the script never resolves `Config`/`data/reference` paths on its own — every input is an explicit CLI path, so private test labels are never on a team's disk to accidentally read. `hackathon_split.csv` is a **public** reference file (already gitignore-exempt), so it is safe for the script to accept `--split` as an explicit path and read it directly — this is a new capability, not a violation of the existing convention, as long as the *test labels* path stays an explicit, separately-supplied flag (not auto-derived from the split file).

**Why the split file is needed at all:** `hackathon_split.csv` keys role by `(domain, session_id, clip_number)`; the Puerto Rico test session reuses clip numbers 1–63 that also exist in the Panama Rojo dev session, so `clip_number` alone cannot disambiguate. `REQUIRED_TRACK_COLUMNS` already includes `session_id`, so a team's combined tracks file (or two separate per-game tracks files) can be joined against `hackathon_split.csv` on `(session_id, clip_number)` to assign each row to `dev` or `private_test`.

**Recommended CLI surface (additive, backward-compatible with the current 4 flags):**

```bash
uv run python scripts/hackathon/score_tracks.py \
  --tracks-dev  dev_tracks.csv   --review-dev  data/reference/continuity_review.csv \
  --tracks-test test_tracks.csv  --review-test data/private/test-labels/<PR-session>/continuity_review.csv \
  --split data/reference/hackathon_split.csv \
  [--flag-pulls-dev preds_dev.csv] [--flag-pulls-test preds_test.csv] \
  --out report.json --out-md report.md
```

- `--split` is used only to validate that every `(session_id, clip_number)` in `--tracks-dev`/`--tracks-test` matches the expected role (fail loudly, naming offending rows, if a team accidentally passes Puerto Rico tracks under `--tracks-dev` or vice versa) — a cheap, high-value guard against the exact mistake the game-disjoint test set exists to prevent.
- Keep the existing single-`--tracks`/`--review` flags working unchanged for local dev-only runs (backward compatibility with `baseline_common.score_with_shared_harness`, which subprocess-calls the script with the old 3-flag form and must not break).
- `--out-md`: a new flag writing the same report as a compact Markdown table (columns: split, n, threshold rate `k/n (p%)`, primary continuous metric, guard metric) — satisfies "JSON + Markdown table" from CONTEXT's discretion note without inventing a second report shape; the Markdown renderer should be a small pure function so it can be unit-tested without a subprocess.
- **Do not** import `cv.testset.read_hackathon_split` — read `hackathon_split.csv` directly with `polars` inside `scripts/hackathon/` (either in `score_tracks.py` or a small shared helper in `scripts/hackathon/baseline_common.py`). `cv/testset.py` is concurrently under construction by 2.2 wave 7 plan 21 and is on the file-collision guard's do-not-touch list; importing it would create a hidden cross-phase dependency on a module whose signature is not yet frozen.

**Runtime constraint:** `polars`, `numpy` — already installed, already used by `score_tracks.py`/`baseline_common.py`. No `torch` import anywhere in this call path (verified: `continuity.py` has no torch dependency; `score_tracks.py`/`baseline_common.py` import only `polars`/`numpy`/`supervision`). `uv run python -c "import polars"` and `import numpy` both resolve in the current environment (`polars==1.5.0`, `numpy==2.1.0`).

## Wording for METR-03

SoccerNet and SportsMOT — the two closest comparable sports-MOT benchmarks — both name **one** metric as the acceptance/ranking criterion and report the rest as secondary/diagnostic:

- **SoccerNet tracking challenge:** HOTA is the leaderboard-ranking metric; DetA/AssA are reported as its diagnostic decomposition, not separate acceptance criteria [MEDIUM — WebSearch cross-checked against the SoccerNet tracking task page and multiple challenge technical reports].
- **SportsMOT:** recommends HOTA, AssA and IDF1 as reported metrics, but explicitly frames HOTA as the primary ranking metric because it balances detection and association quality and — per the benchmark's own stated rationale — correlates better with human judgment of tracking quality than MOTA (detection-heavy) or IDF1 alone [MEDIUM — same sourcing caveat].

**Pattern to propose for METR-03:** name the existing human pass/fail rate (`≥90% des Plays ohne Identitätswechsel`, currently the "Kern-Metrik" in `docs/hackathon-challenge-reid.md`'s Benchmark-Design section) as the **acceptance criterion** (it is what BASE-04's 90%-target language is written against), and name the new continuous metric as the **direction** — "higher/lower is better, used to show progress inside a failed play and to rank submissions when the acceptance criterion does not yet distinguish them" (echoing the user's own draft language: "Die Schwellenmetrik verbirgt echte Verbesserungen und belohnt Zufallstreffer"). This mirrors SoccerNet/SportsMOT's one-acceptance-metric-plus-directional-diagnostics pattern rather than inventing a novel framing.

**Concrete insertion points found in the two docs (both already partially anticipate this open question):**
- `docs/hackathon-challenge-reid.md`, `### Benchmark-Design`, the `**Kern-Metrik:**` paragraph (currently ends with "Design offen (siehe Teil 3)") — replace the open-ended "Design offen" sentence with the METR-03 wording once decided.
- `docs/hackathon-challenge-reid.md`, `## Teil 3 — Offene Punkte`, the bullet "Scoring-Skript: Human-Urteile bleiben die Referenz; ob während der Woche ein automatischer Proxy... sinnvoll ist, entscheiden wir beim Aufbereiten der Datasets" — this bullet should be resolved/removed once METR-03 lands, not left dangling next to the new wording.
- `docs/hackathon-challenge-reid.md`, `## Teil 4`, closing paragraph "Genuin offen (das ist die Challenge): ... Evaluation der Kontinuität ohne Identitäts-Ground-Truth" — this line already frames the label-free metric as part of the open research problem; the METR-03 wording should reference it rather than contradict it.
- `docs/hackathon-challenge-reid-formular.md`, `## Beschreibung` (line 17) — the short-form description already states the 90% threshold and target; needs one added sentence naming the continuous metric's role, kept to the section's ~150–300 word budget.

**This wording is Claude's proposal only — CONTEXT.md is explicit that the final BASE-04/METR-03 decision is a user checkpoint.** The plan should express this as a `checkpoint:human-verify` task at the end, not as a task that edits the docs and calls the requirement done.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Identity-association metric (IDF1/MOTA) once M2-3 labels exist | A custom Hungarian-matching / ID-F1 implementation | `motmetrics` (MIT, `MOTAccumulator`, `motMetricsEnadedException`-safe API) | Correctly implementing IDF1's bipartite matching and MOTA's mismatch/miss/FP accounting is a well-known source of subtle bugs (ambiguous-match tie-breaking, event-log bookkeeping); the reference devkit exists, is MIT, and needs only numpy/pandas/scipy |
| Markdown table rendering for the new `--out-md` flag | A templating dependency (jinja2, etc.) | A small pure Python f-string function, same convention as the existing `_fmt_rate` helper | The report shape is fixed and small (one table per split); adding a templating dependency for this would violate the "runs on a laptop in seconds, minimal deps" constraint for no benefit |
| Reading `hackathon_split.csv` | Importing `cv.testset.read_hackathon_split` | A direct `polars.read_csv` call inside `scripts/hackathon/` with the 6 known column names hard-coded as a tuple constant (same pattern as `REQUIRED_TRACK_COLUMNS`) | `cv/testset.py` is concurrently under construction and file-collision-guarded off; importing it would create an undeclared cross-phase coupling to a module whose public signature is not frozen yet |

**Key insight:** every piece of this phase that looks like it needs new logic (fragment counting, active-track counting, split resolution) already exists somewhere in the codebase in a tested form (`continuity.py`, `baseline_common.py`'s `IDEAL_TRACK_BAND`/`player_track_counts`) or as a mature MIT library (`motmetrics`). The actual new work is normalization, aggregation, CLI plumbing, and honest documentation of blind spots — not new measurement algorithms.

## Common Pitfalls

### Pitfall 1: Treating the label-free metric as a threshold-metric replacement
**What goes wrong:** A future reader (or an over-eager plan) starts reporting the continuous metric's improvement as if it proves fewer identity switches.
**Why it happens:** The metric's name ("continuity") and its correlation with `auto_flag` invite this reading, and the GTA row in `baseline-messung.md` already shows exactly this trap (100% auto-continuity, zero human confirmation, likely false merges).
**How to avoid:** Every place the continuous metric is printed or documented must carry the blind-spot sentence (crossing-swaps are invisible; over-merge can inflate the score) inline, not as a separate caveat section a reader can skip.
**Warning signs:** A PLAN.md or doc draft that says "the continuous metric shows Verfahren X improved identity stability" without the word "coverage"/"fragmentation" and without a caveat.

### Pitfall 2: Test-split scoring silently reading zero clips because Puerto Rico review is incomplete
**What goes wrong:** `data/private/test-labels/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE/continuity_review.csv` currently has all 61 clip rows present but only **10 of 61 reviewed** (8 `fail`, 2 `pass`, 51 blank `verdict`) [VERIFIED: direct read of the vault CSV, 2026-09-02]. `continuity.summarise_review` (reused by `score_tracks.py`) refuses to manufacture a `pass_rate` when any clip is unreviewed and returns `pass_rate=None` with an `unreviewed_clips` list — by design, and correctly so. A one-run dev/test report that silently prints `None` for the test split's human threshold rate, or worse, treats the automatic continuous metric as filling that gap without saying so, would be a "quietly wrong headline number."
**Why it happens:** The vault's continuity review for the private test game appears to be in progress (labelling work continuing outside this session), separate from CONTEXT.md's dev-only "15/61 pilot" framing, which does not mention the test-side review's completeness.
**How to avoid:** The plan must explicitly handle (and test) the partial-review case for `--review-test`: print the same honest "Referenz-Baseline: nicht auswertbar" message `score_tracks.py` already prints for a fully-unreviewed dev file, name the unreviewed clip count, and still report the label-free continuous metric for the test split (which needs no verdicts at all — only the clip list, which `_clip_numbers_from_review` already derives without requiring `verdict` to be filled).
**Warning signs:** A plan task whose acceptance criteria assume `--review-test` is fully reviewed at execution time without a check for `unreviewed_clips`.

### Pitfall 3: Vault CSV delimiter mismatch
**What goes wrong:** `data/private/test-labels/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE/continuity_review.csv` is **semicolon-delimited** (`clip_number;n_tracks;longest_track_frac;...`), while `data/reference/continuity_review.csv` (the dev/pilot file) and `continuity.py`'s `_write_review_csv` both use **comma**-delimited output (Polars' `write_csv` default) [VERIFIED: `cat` of both files, 2026-09-02]. A `polars.read_csv(vault_path)` call using default settings against the vault file will parse it as one wide column, not 8 columns, and either crash on schema validation or silently produce garbage.
**Why it happens:** The vault file was very likely produced/edited outside the project's own write path (e.g., a spreadsheet tool exporting semicolon CSVs, common in German locale settings) rather than by `continuity._write_review_csv`.
**How to avoid:** The plan must either (a) auto-detect the delimiter (sniff the first line for `;` vs `,`) when reading `--review-test`, or (b) require the vault file be re-normalized to comma-delimited before this phase reads it, and add a schema-validation test fixture using a semicolon-delimited file to prove the reader handles it (or fails loudly with a clear message) either way. Silently mis-parsing is the worst outcome.
**Warning signs:** A test fixture for `--review-test` that only uses comma-delimited synthetic CSVs — it would not catch this.

### Pitfall 4: Reusing `IDEAL_TRACK_BAND`'s "10" as `expected_players` without accounting for officials
**What goes wrong:** `n_tracks` (used in the existing `auto_flag` logic) counts referees/officials as well as players; `n_player_tracks` (computed separately in `baseline_common.player_track_counts` by filtering `class_name == "player"`) does not. The new continuous metric's denominator must use `n_player_tracks`, not `n_tracks`, to match the "expected_players=10" semantics — mixing the two silently changes the metric's scale between the label-free layer (which only has raw `tracks`, no guaranteed `class_name` column per `REQUIRED_TRACK_COLUMNS`) and the M2-2 measured rows (which do carry `class_name`).
**Why it happens:** `REQUIRED_TRACK_COLUMNS` (the schema `score_tracks.py` enforces on `--tracks`) does **not** include `class_name` — it is optional, present in the M2-2 output files but not guaranteed for an arbitrary team submission.
**How to avoid:** Define the metric's fallback explicitly: when `class_name` is present, filter to `player` rows before computing `n_player_tracks`; when absent, fall back to `n_tracks` (all detected entities) with a printed notice, exactly the pattern `score_tracks.py` already uses for the coverage-denominator fallback ("Hinweis: kein --review angegeben...").
**Warning signs:** A metric implementation that assumes `class_name` is always present, breaking on a team submission that only carries the 8 required columns.

## Code Examples

### Reusing `continuity._measure_clip` for the primary continuous metric
```python
# Source: src/flag_football_ep/cv/continuity.py (existing, already tested)
# ContinuityRow already carries n_fragments; the new code only needs to
# normalize it, not recompute it.
from flag_football_ep.cv.continuity import _measure_clip

result = _measure_clip(clip_number, clip_tracks)
fragments_per_player = result.n_fragments / expected_players  # expected_players = 10
```

### Existing dev-pool / ideal-band aggregation pattern to mirror for the guard metric
```python
# Source: scripts/hackathon/baseline_common.py (existing, already tested)
IDEAL_TRACK_BAND: tuple[int, int] = (10, 14)

def player_track_counts(tracks_df: pl.DataFrame) -> dict[int, int]:
    if tracks_df.height == 0:
        return {}
    counts = (
        tracks_df.filter(pl.col("class_name") == "player")
        .group_by("clip_number")
        .agg(pl.col("track_id").n_unique().alias("n_player_tracks"))
    )
    return {int(row["clip_number"]): int(row["n_player_tracks"]) for row in counts.iter_rows(named=True)}
```

### motmetrics minimal IDF1/MOTA usage (for the label-based interface's synthetic tests)
```python
# Source: PyPI project description / README pattern for py-motmetrics (MIT), verified
# to exist and install-check clean via slopcheck 2026-09-02.
import motmetrics as mm

acc = mm.MOTAccumulator(auto_id=True)
# per frame: acc.update(gt_ids, hyp_ids, distance_matrix)
mh = mm.metrics.create()
summary = mh.compute(acc, metrics=["idf1", "mota", "num_switches"], name="synthetic")
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| MOTA as the primary MOT benchmark ranking metric | HOTA as the primary ranking metric on modern sports-MOT benchmarks (SoccerNet, SportsMOT) | HOTA published 2020 (IJCV), adopted as primary by SportsMOT (2023) and SoccerNet tracking challenges (2022+) | Motivates naming a single acceptance criterion + a directional secondary metric for METR-03, rather than a flat list of equally-weighted numbers |

**Deprecated/outdated:** none directly relevant — `continuity.py`'s fragment-based approach and `motmetrics`'s CLEAR-MOT/IDF1 implementation are both still current, actively-referenced approaches in this domain; the phase is not adopting anything stale.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `expected_players = 10` (5v5, no officials) is the correct normalization constant for the primary continuous metric | Label-Free Continuous Metric Candidates | If officials should be included (project's own `IDEAL_TRACK_BAND=(10,14)` allows up to 14), the metric's absolute scale shifts; relative comparisons across methods/clips are unaffected, only the "what does 1.0 mean" framing changes |
| A2 | Puerto Rico vault review is genuinely in-progress (10/61 reviewed) and not a stale/incomplete artifact that should be ignored | Pitfall 2 | If the 51 blank verdicts are actually meant to stay blank permanently for this phase (e.g., full test-set review is scoped to a later phase), the plan should treat `--review-test`'s partial state as the steady-state input, not a temporary gap to wait out |
| A3 | motmetrics (not trackeval) is the right default for the label-based interface | Package Legitimacy Audit, Don't Hand-Roll | If the eventual identity-labels work (M2-3) specifically wants HOTA (not IDF1/MOTA) as its headline number, `trackeval`'s heavier dependency footprint becomes necessary anyway and the "no torch, laptop-in-seconds" framing would need revisiting for that later phase only, not for M2-4's label-free layer |
| A4 | SoccerNet/SportsMOT's "one primary + secondary metrics" framing is a fair precedent to cite for METR-03, despite those benchmarks using label-based HOTA as the primary (this phase's primary/acceptance metric is the label-free-adjacent human threshold, not a label-based one) | Wording for METR-03 | The analogy is structural (one acceptance metric + directional others), not literal (this project's acceptance metric is human-judged, not computed from GT tracks) — if the user reads it as claiming HOTA-equivalence, the wording proposal should clarify this distinction explicitly in the plan output |

## Open Questions (RESOLVED)

RESOLVED 2026-09-02 by CONTEXT.md decisions: (1) partial test-split review is steady-state input — plan M2-04-01 `--review-test` works at any completeness level, reviewed-only rate carries its own n + "unvollstaendig" flag; (2) the guard metric ships as a DIAGNOSTIC column, the primary `n_fragments / expected_players` is the one officially reported continuous number (METR-01).

1. **Is the Puerto Rico continuity review expected to reach 61/61 before M2-4 executes, or does the phase need to run against the current 10/61 partial state?**
   - What we know: the vault file has all 61 clip rows scaffolded, 10 reviewed, formatted with a semicolon delimiter (Pitfall 3).
   - What's unclear: whether this is mid-labelling (about to complete) or a deliberate partial state for this phase.
   - Recommendation: the plan should build `--review-test` handling to work correctly at any completeness level (0/61 through 61/61) rather than assuming either extreme, and surface the actual count in every report, per the project's own "never manufacture a headline number from a shrunken denominator" convention.

2. **Should the guard metric (`active_track_count_deviation`) be a second officially-reported number, or purely a diagnostic column?**
   - What we know: METR-01 asks for "eine stetige Kennzahl" (singular); CONTEXT.md's discretion note also speaks of one definition ("e.g. ... or ...", not "and").
   - What's unclear: whether reporting two numbers under METR-01/METR-02 satisfies or over-delivers the requirement, and whether the challenge doc's Kern-Metrik paragraph should name one or both.
   - Recommendation: implement both (both are cheap, reuse existing data), but let the plan explicitly mark the guard metric as "diagnostic, not part of the acceptance/direction framing" in the challenge doc wording — keeps METR-03's single acceptance-criterion-vs-direction framing clean while still shipping the over-merge protection.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `uv` | running `score_tracks.py`, the test suite | check inconclusive — `uv --version` did not resolve `python3`'s own reported version cleanly in the audit shell (unrelated `python3` in `PATH` differs from the project's `uv`-managed interpreter); `uv 0.6.16` confirmed present | 0.6.16 | none needed — already the project's standard runner |
| `polars` | all scoring/split logic | ✓ | 1.5.0 | — |
| `numpy` | continuous-metric aggregation, motmetrics' own dependency | ✓ | 2.1.0 | — |
| `motmetrics` | label-based interface's synthetic-data tests only (not the live label-free scoring path) | ✗ (not installed in the current environment) | — | add to `dev` dependency group; skip the synthetic-interface test with a clear message if absent, never fail the whole suite on it |
| `torch` | explicitly NOT required anywhere in this phase's call path | n/a — deliberately excluded per CONTEXT.md ("no torch dependency in the scoring path") | — | — |

**Missing dependencies with no fallback:** none — `motmetrics` is scoped to dev-only synthetic tests and is not on the critical path for METR-01/METR-02.
**Missing dependencies with fallback:** `motmetrics` — install to the `dev` group, gate its tests behind an import check.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest ≥8 (already the project standard, `[tool.pytest.ini_options]` in `pyproject.toml`) |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths=["tests"]`, `addopts="-q"`) |
| Quick run command | `uv run pytest tests/test_hackathon_scoring.py tests/test_cv_continuity.py -x -q` |
| Full suite command | `uv run pytest -q` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| METR-01 | continuous metric computed per clip, normalized, no identity labels needed | unit | `uv run pytest tests/test_cv_continuity.py -k fragments -x -q` (extend existing file) | ✅ (extend `tests/test_cv_continuity.py`) |
| METR-02 | one `score_tracks.py` run emits both metrics split by dev/test, JSON + Markdown | integration (subprocess CLI) | `uv run pytest tests/test_hackathon_scoring.py -k split -x -q` (extend existing file) | ✅ (extend `tests/test_hackathon_scoring.py`) |
| METR-02 | test-split scoring reads the vault path without ever writing labels into a public location | integration | `uv run pytest tests/test_hackathon_scoring.py -k vault -x -q` | ❌ Wave 0 — new test needed, including a semicolon-delimited fixture (Pitfall 3) |
| METR-03 | challenge docs name acceptance criterion vs direction | manual-only | n/a — prose review; covered indirectly by `tests/test_m2_baseline_docs.py`-style doc/table sync checks if the plan adds one | ❌ manual, `checkpoint:human-verify` per CONTEXT.md |
| METR-04 | per_clip.csv-derived comparison shows a difference the threshold metric swallows | unit (assertion over existing CSV data, no new measurement) | `uv run pytest tests/test_hackathon_scoring.py -k metr04 -x -q` | ❌ Wave 0 — new test needed, reading `data/reference/baseline-methods/{summary,per_clip}.csv` directly |
| (interface only) label-based IDF1/MOTA function, synthetic GT | unit | `uv run pytest tests/test_hackathon_scoring.py -k synthetic_identity -x -q` (skip if `motmetrics` not installed) | ❌ Wave 0 — new test + new dev dependency |

### Sampling Rate
- **Per task commit:** `uv run pytest tests/test_hackathon_scoring.py tests/test_cv_continuity.py -x -q`
- **Per wave merge:** `uv run pytest -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_hackathon_scoring.py` — add dev/test split fixtures (two synthetic sessions with overlapping clip numbers, one semicolon-delimited review file) covering METR-02's vault-path and delimiter-tolerance behavior
- [ ] `tests/test_hackathon_scoring.py` — add a METR-04 assertion reading the real `data/reference/baseline-methods/{summary,per_clip}.csv` (no synthetic data — this is the one test that should assert against the actual measured M2-2 numbers, per the locked "no re-running trackers" decision)
- [ ] `tests/test_cv_continuity.py` — extend with `mean_fragments_per_expected_player`/`active_track_count_deviation` unit cases, including a GTA-shaped synthetic fixture (few long merged tracks) proving the guard metric flags what the primary metric misses
- [ ] Dev dependency addition: `motmetrics` in `pyproject.toml`'s `[dependency-groups] dev` list, plus a `pytest.importorskip("motmetrics")` guard in the new synthetic-identity test
- [ ] `tests/test_hackathon_scoring.py` — a synthetic-GT test for the label-based interface function (fixed, hand-computed IDF1/MOTA on a tiny 2-track/2-GT-identity example), proving the interface is wired correctly without needing real M2-3 labels

## Security Domain

Not applicable in the ASVS sense — this phase has no authentication, session, or network-facing surface; it is a local CLI scoring tool over CSV/Parquet files. The one security-adjacent concern already fully covered by existing project convention: `score_tracks.py` must never read private test labels except via an explicit CLI path the caller supplies (documented in its own docstring, verified unchanged in this research), so the private-test-label leak-prevention property carries over unmodified into the new dev/test-split code path.

## Sources

### Primary (HIGH confidence)
- `src/flag_football_ep/cv/continuity.py` — read in full, existing `_measure_clip`/`summarise_review`/`REVIEW_COLUMNS` logic
- `scripts/hackathon/score_tracks.py`, `scripts/hackathon/baseline_common.py` — read in full, existing CLI/harness conventions
- `docs/baseline-messung.md` — measured M2-2 baseline rows, GTA over-merge caveat, saturation finding (57/61 auto vs 15/61 human)
- `data/reference/continuity_review.csv`, `data/reference/baseline-methods/{summary,per_clip}.csv`, `data/reference/hackathon_split.csv` — read directly, verdict/fragment/id_switches distributions computed live
- `data/private/test-labels/2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE/{continuity_review,flag_pull_events}.csv` — read directly, delimiter and completeness verified live
- `.planning/imported/challenge-haertung/REQUIREMENTS.md`, `ABGLEICH.md` — METR-01..04 wording, file-collision/execution-order context
- `docs/hackathon-challenge-reid.md`, `docs/hackathon-challenge-reid-formular.md` — read for METR-03 insertion points
- `.planning/phases/02.2-dataset-buildout/02.2-21-PLAN.md` — vault layout, hackathon_split.csv schema, file-collision guard rationale
- `gh api repos/cheind/py-motmetrics`, `gh api repos/kovalp/TrackEval`, `gh api repos/JonathonLuiten/TrackEval`, `gh api repos/SoccerNet/sn-trackeval` — license verification, all MIT
- `python3 -m pip index versions {motmetrics,trackeval}`, PyPI JSON API (`https://pypi.org/pypi/<pkg>/json`) — version/license/dependency verification
- `slopcheck install motmetrics trackeval` — legitimacy scan, `[OK]` for both

### Secondary (MEDIUM confidence)
- WebSearch: motmetrics PyPI description/downloads (Snyk Advisor) — license cross-verified against `gh api`, downloads not independently reverified via pypistats (rate-limited during research)
- WebSearch: SoccerNet tracking challenge HOTA-as-primary-metric framing — cross-referenced across the SoccerNet tracking task page and multiple 2023 challenge technical reports
- WebSearch: SportsMOT HOTA/AssA/IDF1 primary-metric framing — cross-referenced against the SportsMOT paper's own stated rationale (arxiv.org/pdf/2304.05170)

### Tertiary (LOW confidence)
- none — every claim above was either verified against the local codebase/data, verified via `gh api`/PyPI JSON, or cross-checked across 2+ WebSearch sources

## Metadata

**Confidence breakdown:**
- Standard stack (motmetrics/trackeval): HIGH — both verified live via PyPI JSON + GitHub API license lookup + slopcheck
- Label-free metric design: MEDIUM-HIGH — grounded directly in this project's own measured data (GTA merge counts, fragment/verdict distributions), but the specific normalization constants (expected_players=10, the [10,14] band) are descriptive constants from the challenge doc, not independently re-derived or statistically validated in this research pass
- Architecture (CLI design, file-collision boundaries): HIGH — derived directly from existing code conventions and the locked file-collision guard in CONTEXT.md
- Pitfalls (vault delimiter mismatch, partial review state): HIGH — both directly observed by reading the actual vault files, not inferred

**Research date:** 2026-09-02
**Valid until:** 2026-10-02 (30 days) — the Puerto Rico vault review completeness (Pitfall 2) and hackathon_split.csv contents are the two most likely to change before the phase executes; re-check both immediately before planning if more than a few days have passed
