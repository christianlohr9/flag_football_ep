---
phase: M2-02-ehrliche-baseline
plan: 02
subsystem: cv-tracking-benchmark
tags: [gta-link, torchreid, osnet, deep-person-reid, vendoring, license-gate, polars, score-tracks, hackathon]

requires:
  - phase: M2-02-ehrliche-baseline (plan 01)
    provides: "scripts/hackathon/baseline_common.py's shared measurement primitives (write_tracks, score_with_shared_harness, summarise, append_results, private_test_by_clip, player_track_counts, fmt_rate), reused unchanged by this plan's GTA adapter"
provides:
  - "vendor/gta-link (MIT, pinned e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca) and its vendored reid/ (KaiyangZhou/deep-person-reid, MIT) -- gitignored trees, reproducible from vendor/README.md's pin record"
  - "data/processed/baseline-methods/checkpoints/osnet_x1_0_market1501.pth: the officially-hosted, generic (non-sport-tuned) OSNet checkpoint, SHA-256 recorded and re-verified on every load"
  - "scripts/hackathon/measure_gta.py: BoT-SORT tracks + existing crops -> OSNet embeddings -> vendored gta-link split/merge (called unmodified) -> scored, comparable tracks row"
  - "the GTA row in data/reference/baseline-methods/{summary,per_clip}.csv -- the fourth of BASE-01's four measured methods, with its limitations recorded in the same row as the number"
affects: [M2-02-03 (results table + visual spot-check; must carry GTA's over-merging caveat verbatim, never present auto=100% as an unqualified win), M2-4 (continuous metric over per_clip.csv, GTA rows included)]

tech-stack:
  added: []
  patterns:
    - "Vendor-at-pinned-SHA over pip install for license-gated research code with no PyPI release: live gh api SPDX check before any clone, git ls-remote immediately before pinning (never a document-implied SHA), LICENSE file spot-checked on disk, vendor/.gitignore keeps the tree itself out of git while vendor/README.md's reproduction commands make it re-creatable"
    - "sys.modules stub for an unused, uninstalled logging dependency (loguru) to satisfy a vendored module's import without installing anything or touching the vendored file -- documented as a deviation, not hidden"
    - "Side-channel metadata lookup keyed by the exact (frame_index, bbox) tuple, built BEFORE calling into third-party split/merge code, to reconstruct class_name/confidence/session_id after refinement -- works around a real gap in gta-link's own merge_tracklets (it concatenates .times/.bboxes/.features but not .scores) without patching the vendored source"
    - "Checkpoint integrity as a runtime gate, not just a one-time check: load_embedder re-verifies the checkpoint's SHA-256 against the caller-supplied value on every invocation, SystemExit on mismatch"

key-files:
  created:
    - vendor/README.md
    - vendor/.gitignore
    - scripts/hackathon/measure_gta.py
    - tests/test_m2_gta_adapter.py
  modified:
    - data/reference/baseline-methods/summary.csv
    - data/reference/baseline-methods/per_clip.csv

key-decisions:
  - "gta-link's own vendored reid/ subfolder (already MIT KaiyangZhou/deep-person-reid, verified via LICENSE + class-name + pretrained_urls spot-check) is used as-is -- no separate deep-person-reid clone, per RESEARCH.md's assumption A5, confirmed rather than assumed."
  - "The Market-1501-trained osnet_x1_0 (Rank-1 94.2/mAP 82.6) was chosen over the ImageNet-only osnet_x1_0 checkpoint the pretrained_urls dict defaults to -- both are listed in vendor/gta-link/reid/docs/MODEL_ZOO.md, and a person-ReID-trained backbone is what appearance re-identification needs."
  - "The sports_model.pth.tar-60 checkpoint that shipped bundled INSIDE the pinned gta-link clone (not fetched by this plan, discovered on disk after checkout) was deleted from the local vendor tree and documented in vendor/README.md's Bekannte Probleme -- it was never loaded, never referenced by measure_gta.py, and the deletion doesn't touch the pinned commit."
  - "GTA's 61/61 (100%) automatic continuity rate is reported with an explicit, load-bearing caveat baked into the CSV row's own notes field (not just this SUMMARY): human_pass_k/n stay empty, and the note names the saturation risk and the plausible over-merging explanation (364 merges across 61 clips at median 12 crops/track with a generic embedding) so the number cannot be read as a validated win over the 15/61 human reference without that context traveling with it."
  - "The crop-density diagnostic (Task 3's conditional 10-clip dense re-crop) was evaluated and skipped: its trigger condition requires BOTH 'fewer than 5/61 clips unchanged' AND 'median crops/track < 15' -- only the second held (12.0 < 15); 58/61 clips changed, so the first did not hold, and the AND-gated diagnostic correctly did not run."

requirements-completed: [BASE-01, BASE-03]

duration: ~24min
completed: 2026-09-01
---

# Phase M2-02 Plan 02: GTA Measurement Summary

**GTA (Global Tracklet Association) measured for real on all 61 clips via the vendored MIT `gta-link` split/merge code and a generic Market-1501 OSNet checkpoint -- auto-continuity jumps to 61/61 (100%), but the row's own notes flag that this is unvalidated by human review and plausibly reflects over-merging under a generic embedding at only 12 crops/track, not a proven win over the 15/61 human baseline.**

## Performance

- **Duration:** ~24 min
- **Started:** 2026-09-01 (session start, continuing from plan M2-02-01)
- **Completed:** 2026-09-01T14:58:24Z
- **Tasks:** 3
- **Files created:** 4 (vendor/README.md, vendor/.gitignore, scripts/hackathon/measure_gta.py, tests/test_m2_gta_adapter.py)
- **Files modified:** 2 (data/reference/baseline-methods/summary.csv, per_clip.csv)
- **Heavy step measured wall time (61 clips, 17,059 crops):** embedding=16.55-16.78s (MPS, ~940-1000 crops/s across two runs), split/merge=1.72-1.74s, scoring=0.12-0.13s -- total runtime_s=18.42-18.61s, well under the "low single-digit minutes" RESEARCH estimate.

## Accomplishments

- **License gate enforced live, not assumed:** `gh api repos/sjc042/gta-link --jq '.license.spdx_id'` and the same for `KaiyangZhou/deep-person-reid` both returned `MIT` this session (raw JSON quoted in `vendor/README.md`'s `## Gepinnte Quellen` table). `gta-link` has no tags (`git ls-remote --tags` empty) so it's pinned to the `main` HEAD commit at check time, `e4d5cc4065ceb1ec3fa9dc7478455f13a8d7f9ca` -- a full 40-character SHA, never a branch name.
- **No redundant clone:** `vendor/gta-link/reid/` was confirmed (LICENSE header `Copyright (c) 2018 Kaiyang Zhou`, identical file layout to the canonical repo, matching `pretrained_urls` dict and class names in `osnet.py`) to already be the unmodified official `deep-person-reid`, so no second `git clone` was needed -- RESEARCH's assumption A5 confirmed rather than assumed.
- **Checkpoint chosen from the vendored source's own docs, not memory or web search:** `vendor/gta-link/reid/docs/MODEL_ZOO.md`'s "Same-domain ReID" table names the Market-1501-trained `osnet_x1_0` (Rank-1 94.2, mAP 82.6), fetched via ephemeral `uv run --with gdown`, SHA-256 `2809d3227f7d078f6045f7feb874a34d0684f0e0057b264b99adccf7d4519154` recorded in `vendor/README.md` and re-verified by `measure_gta.load_embedder` on every invocation (SystemExit on mismatch).
- **`scripts/hackathon/measure_gta.py`** (620+ lines) is the full adapter: `load_embedder` (checkpoint SHA-256 gate, OSNet loaded via `importlib.util` file-load since the `torchreid` package `__init__.py` transitively needs uninstalled `yacs`/`h5py`/`gdown`), `embed_crops` (256x128 resize, ImageNet normalisation, L2-normalised 512-d features, MPS), `build_tracklets`/`refine`/`apply_refinement` (the vendored `Tracklet`/`split_tracklets`/`get_spatial_constraints`/`get_distance_matrix`/`merge_tracklets` called exactly as written, never reimplemented -- verified by `grep -v '^ *#' ... | grep -c DBSCAN` returning 0).
- **No row lost, no track silently dropped:** tracks with zero embedded crops pass through unchanged under a fresh id; tracks with SOME embedded and SOME non-embedded frames (the common case, since crops are capped at 12/track) have their non-embedded frames reattached via nearest-embedded-frame-in-time to whichever refined sub-tracklet they belong to. `process_clip` asserts `len(output_rows) == n_input_rows` per clip and raised nothing across all 61 clips.
- **Real measurement, all 61 clips, no synthetic stand-in:** `data/reference/baseline-methods/summary.csv` gained exactly one `gta` row (`config=gta-link@e4d5cc40+osnet_x1_0-generic`), `per_clip.csv` gained 61 rows. `human_pass_k`/`human_pass_n` are empty -- GTA has received no human review, and copying BoT-SORT's 15/61 onto this row would be a fabricated number.
- **The result is reported honestly, including the part that looks too good:** auto=61/61 (100.00%), up from BoT-SORT's 57/61 (93.44%) and even from CBIoU/baseline-matched's 58/61. The row's `notes` field carries a load-bearing caveat (quoted in full below) rather than letting the bare percentage speak for itself.

## GTA measurement (headline numbers)

| Metric | Value |
|---|---|
| Auto-continuity (full 61) | 61/61 (100.00%) |
| Auto-continuity (dev pool, 43) | 43/43 (100.00%) |
| Human pass-rate | **empty -- no review exists for this method** |
| median_n_tracks / median_n_player_tracks | 20.0 / 18.0 (down from BoT-SORT's 25.0/23.0) |
| median crops embedded per track | 12.0 (== `max_crops_per_track` cap; the crop tree is sampled, not exhaustive) |
| Split operations (61 clips) | 0 (every embedded track has <=12 frames, `min_len=100` threshold never reached) |
| Merge operations (61 clips) | 364 |
| Clips with unchanged track partition | 3/61 |
| Runtime (embed + split/merge + score) | 18.42-18.61s across two full runs |
| License | MIT (gta-link) + MIT (deep-person-reid/osnet_x1_0) |

**The row's full `notes` field** (verbatim from `data/reference/baseline-methods/summary.csv`, this is the caveat that must travel with the number into `docs/hackathon-challenge-reid.md` in plan M2-02-03):

> generisches OSNet-Checkpoint (kein sportspezifisches Finetuning) -- Grund: Lizenz/Provenienz (der sport-feingetunte Checkpoint hat keine nachvollziehbare Herkunft, siehe vendor/README.md ## Checkpoint). median Crops/Track=12.0 bei max_crops_per_track=12 (gesampelte Crop-Menge, nicht jeder Frame eingebettet -- Referenzimplementierung embettet jeden Frame). Split-Operationen=0, Merge-Operationen=364, unveraenderte Partition in 3/61 Clips. WICHTIGER VORBEHALT: die automatische Kontinuitaets-Rate ist NICHT durch eine menschliche Review bestaetigt (human_pass_k/n bewusst leer) und misst nur Track-Laenge, nicht Identitaetskorrektheit; sie ist bekanntermassen saettigend (vgl. botsort-existing in derselben Tabelle: auto=93.44% vs. human=24.59%). Bei median nur 12 Crops/Track und einem generischen (nicht sportspezifischen) Embedding koennten einige der vielen Merge-Operationen Tracks verschiedener Spieler faelschlich zusammengefuehrt haben, was die automatische Rate erhoeht, ohne dass dies verifiziert waere -- ein hoher Auto-Wert ist hier kein Beleg fuer eine tatsaechliche Verbesserung gegenueber der 15/61-Referenz.

**Why this caution matters for BASE-04:** BASE-04 asks whether a ready-made method "clearly beats" 24.6%/23.3%. GTA's *automatic* number (100%) is far above that, but the automatic metric only measures single-track length continuity, not whether the identity inside that continuous track is correct -- exactly the thing 364 merge operations under an unvalidated generic embedding could get wrong. Wave 1's own SUMMARY already documented this saturation with BoT-SORT's own auto=93.44% vs human=24.59% gap; GTA's gap could plausibly be even larger given the merge count. This plan does not run the human review needed to settle it (out of scope, no reviewer time budgeted here) -- it makes sure the ambiguity is visible everywhere the number appears, per the plan's explicit "do not declare a winner" and honest-null-result instructions. Plan M2-02-03 must carry this caveat into the challenge description verbatim, not summarise it away.

## Crop-density diagnostic (Task 3)

**Not run -- condition did not hold.** The plan's trigger is an AND of two conditions: (a) fewer than 5/61 clips have an unchanged track partition, AND (b) median crops/track < 15. Measured: (b) held (12.0 < 15) but (a) did not (58/61 clips changed, not fewer than 5) -- so the diagnostic's premise ("maybe crop density alone explains a near-null result") doesn't apply here, since the result is very much non-null (just possibly wrong in the opposite direction -- over-merging, not under-merging). Recorded here rather than silently skipped, per the plan's "if the condition does not hold, skip the diagnostic and say so."

## Task Commits

1. **Task 1: License gate, pin, vendor gta-link, fetch OSNet checkpoint** - `c8300ea` (docs)
2. **Task 2: GTA adapter and its tests** - `3e4a15f` (feat)
3. **Task 3: Measure GTA on all 61 clips, record the result** - `46b5c51` (feat)

**Plan metadata:** SUMMARY committed separately below (per this plan's parallel-execution contract: no STATE.md/ROADMAP.md update from this executor).

## Files Created/Modified

- `vendor/README.md` - pin record: repo URLs, 40-char commit SHAs, live SPDX license verification (raw `gh api` output quoted), reproduction commands, checkpoint source/SHA-256, "not used" section (PyPI `torchreid`, Deep-EIoU), "Bekannte Probleme" (the bundled `sports_model.pth.tar-60` deletion)
- `vendor/.gitignore` - keeps `gta-link/` and `deep-person-reid/` out of git
- `scripts/hackathon/measure_gta.py` - the full adapter (license/checkpoint verification through result-CSV append), reusing `baseline_common.py` unchanged
- `tests/test_m2_gta_adapter.py` - 6 tests: checkpoint SHA mismatch aborts, tracks-without-crops pass through, no rows lost across a synthetic split and merge, output schema conformance, no local DBSCAN reimplementation, no forbidden-artifact mentions
- `data/reference/baseline-methods/summary.csv` - +1 `gta` row
- `data/reference/baseline-methods/per_clip.csv` - +61 `gta` rows

## Decisions Made

See `key-decisions` in the frontmatter above for the five load-bearing ones (reid/ reuse, checkpoint variant choice, bundled-checkpoint deletion, the notes-field caveat, the diagnostic skip). Additional smaller decisions:
- `refine_tracklets.py`'s own `argparse` default for `--eps` is `0.7`, differing from the README's documented `0.6` -- the plan's exact parameter set (`min_len=100, eps=0.6, min_samples=10, max_k=3, spatial_factor=1.0, merge_dist_thres=0.4`, matching the README's CLI example) was used as instructed, and this README-vs-argparse-default discrepancy is recorded here per the plan's read_first instruction, not silently resolved one way.
- Fresh sequential per-clip track ids are assigned to every final entity (refined tracklets AND passthrough tracks) rather than reusing `gta-link`'s own `new_id = max(embedded_track_ids) + 1` scheme, because that scheme only guarantees no collision with OTHER embedded track ids -- not with passthrough track ids excluded from its input, which could numerically exceed it.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Removed the bundled `sports_model.pth.tar-60` found inside the pinned gta-link clone**
- **Found during:** Task 1 (post-clone acceptance criteria check)
- **Issue:** The pinned commit of `gta-link` ships `reid_checkpoints/sports_model.pth.tar-60` directly in the repository (30,393,613 bytes, a real PyTorch `state_dict`, not a placeholder -- confirmed via `git log -- reid_checkpoints/sports_model.pth.tar-60`, commit `1c08b3d`). This file was never downloaded by this plan, but its mere presence on disk under `vendor/` violated the acceptance criterion "the file `sports_model.pth.tar-60` does not exist anywhere under `data/` or `vendor/`."
- **Fix:** `rm vendor/gta-link/reid_checkpoints/sports_model.pth.tar-60` after checkout. Documented in `vendor/README.md`'s `## Bekannte Probleme` with the exact removal command, so a fresh reproduction from the README's clone+checkout steps is warned to expect and re-remove the same file if that same acceptance check is re-run.
- **Files modified:** none tracked (the file lived inside the gitignored `vendor/gta-link/` tree, never staged or committed)
- **Verification:** `find data vendor -iname "sports_model*"` returns nothing after the fix; `measure_gta.py` never references this path.
- **Committed in:** `c8300ea` (Task 1 commit; the removal itself touches only the gitignored working tree, documented in the committed `vendor/README.md`)

**2. [Rule 3 - Blocking] Stubbed `loguru` in `sys.modules` to import `refine_tracklets.py` without installing anything**
- **Found during:** Task 2 (first attempt to import the vendored `refine_tracklets` module)
- **Issue:** `vendor/gta-link/refine_tracklets.py` does `from loguru import logger` at module level (used only for progress logging inside `split_tracklets`/`main`). `loguru` is not part of this project's dependency tree, and this plan is built to need zero permanent installs.
- **Fix:** `measure_gta._stub_loguru_if_missing()` registers a minimal no-op `types.ModuleType("loguru")` with a `logger` attribute (any attribute access returns a no-op callable) into `sys.modules` before importing `refine_tracklets`, only if a real `loguru` isn't already importable. Never touches the vendored file on disk; never installs a package.
- **Files modified:** `scripts/hackathon/measure_gta.py` (the stub lives here, not in vendored code)
- **Verification:** `import refine_tracklets` (and the full 61-clip run) succeeds without `loguru` installed; `uv run python -c "import importlib.metadata as m; m.version('loguru')"` still fails (never installed); `git diff --name-only pyproject.toml uv.lock` empty.
- **Committed in:** `3e4a15f` (Task 2 commit)

**3. [Rule 1 - Bug] The adapter's own docstring/notes prose broke its own acceptance-criteria greps**
- **Found during:** Task 2 (first test run)
- **Issue:** Same class of self-collision as plan M2-02-01's deviation #1: the module docstring explained the "no local DBSCAN" guarantee using the literal word `DBSCAN`, and the notes-string generator explained the checkpoint substitution using the literal substring `sports_model`. The plan's own acceptance-criteria greps (`grep -v '^ *#' ... | grep -c DBSCAN` and `grep -c "...sports_model" ...`) treat these as violations regardless of whether they're prose or code.
- **Fix:** Reworded both: the docstring now says "no local density-based clustering reimplementation" instead of naming the algorithm; the notes string references "der sport-feingetunte Checkpoint" and points to `vendor/README.md ## Checkpoint` instead of naming the forbidden filename.
- **Files modified:** `scripts/hackathon/measure_gta.py`
- **Verification:** `grep -v '^ *#' scripts/hackathon/measure_gta.py | grep -c "DBSCAN"` and `grep -c "pip install torchreid\|sports_model" scripts/hackathon/measure_gta.py` both return 0; all 6 adapter tests pass.
- **Committed in:** `3e4a15f` (fixed before commit, part of Task 2)

### Missing-Critical Addition

**4. [Rule 2 - Missing Critical] Added an explicit over-merging/saturation caution to the GTA row's own `notes` field**
- **Found during:** Task 3 (first full 61-clip run, auto=61/61=100.00% observed)
- **Issue:** The plan's mandatory `notes` content (generic-checkpoint note, crop-sampling caveat, split/merge counts) is necessary but, on its own, would let a 100.00% auto-continuity number stand unqualified next to BoT-SORT's 93.44%/human 24.59% -- exactly the kind of number a reader could misread as "GTA clearly beats the baseline" (triggering BASE-04) without also seeing that this number is unvalidated and that heavy merging (364 ops) under a generic, unvalidated embedding at only 12 crops/track is a plausible alternative explanation. The plan's own environment instructions ("Honest-null-result rule") and `must_haves.truths` ("A null result is reported as a null result... never quietly dropped") extend naturally to a *suspiciously good* result: it needs the same honesty discipline as a null one.
- **Fix:** Added a "WICHTIGER VORBEHALT" (important caveat) paragraph to the `notes` string generated in `measure_gta.py`'s `main()`, naming the missing human review, the saturation precedent from `botsort-existing`'s own row, and the over-merging risk explicitly -- baked into the script so a re-run reproduces the same caveat, not hand-edited into the CSV once.
- **Files modified:** `scripts/hackathon/measure_gta.py`, `data/reference/baseline-methods/summary.csv` (via re-run)
- **Verification:** `data/reference/baseline-methods/summary.csv`'s `gta` row's `notes` cell contains the full caveat (quoted in full above); the measurement was re-run and reproduced identical numbers (auto=61/61, 364 merge ops) both before and after the notes-string edit, confirming the edit is prose-only and doesn't change the measurement.
- **Committed in:** `46b5c51` (Task 3 commit)

---

**Total deviations:** 4 (2 Rule 3 blocking, 1 Rule 1 bug, 1 Rule 2 missing-critical). **Impact on plan:** All four are necessary for correctness (Rule 3 items) or for the plan's own explicit honesty requirements (Rule 1/2 items) -- no scope creep, no method or parameter substitution, no forbidden checkpoint ever fetched or used.

## Issues Encountered

None beyond the four items documented above under Deviations.

## User Setup Required

None - no external service configuration required. `gdown` was used ephemerally via `uv run --with gdown` (never installed); `.venv`/`pyproject.toml`/`uv.lock` are byte-identical to their pre-plan state (`git diff --name-only pyproject.toml uv.lock` empty throughout).

## Next Phase Readiness

- Plan M2-02-03 (results table + visual spot-check) has all four BASE-01 numbers now: BoT-SORT (re-scored), ByteTrack, CBIoU (labelled "closest permissive cousin, NOT Deep-EIoU"), and GTA -- each with a documented, reproducible `start_command` in `summary.csv` (BASE-03).
- **Critical instruction for M2-02-03:** GTA's `notes` field caveat (quoted in full above) MUST travel into `docs/hackathon-challenge-reid.md` alongside the 61/61 number. Presenting "GTA: 100%" without the unvalidated/saturation/over-merging context would misinform the challenge description and could wrongly trigger BASE-04's target-adjustment recommendation on an unverified basis.
- BASE-04's "clearly beats 24.6%" test remains formally untriggered by *validated* numbers from this plan: GTA's auto rate is high but explicitly unvalidated; no non-BoT-SORT method (ByteTrack, CBIoU, or GTA) has received human review. This plan does not recommend a target change -- that judgement call stays with plan M2-02-03/the user, per CONTEXT.md's locked decision that a target adjustment is the user's call toward BWI.
- No blockers. `data/labels/`, `data/video/`, `data/private/`, `docs/` untouched by this plan; `scripts/hackathon/score_tracks.py`, `scripts/hackathon/baseline_common.py`, `pyproject.toml`, `uv.lock` byte-identical to pre-plan state; input tracks parquet (`data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet`) verified byte-identical via SHA-256 (`bcaf89e276e54301a9c9908148eda14c56f083c349087ff52f4627a8ba30ee1b`) before and after every run this session.

## Self-Check: PASSED

- `vendor/README.md` exists: FOUND
- `vendor/.gitignore` exists: FOUND
- `scripts/hackathon/measure_gta.py` exists: FOUND
- `tests/test_m2_gta_adapter.py` exists: FOUND
- Commit `c8300ea` (Task 1) present in `git log --oneline --all`: FOUND
- Commit `3e4a15f` (Task 2) present in `git log --oneline --all`: FOUND
- Commit `46b5c51` (Task 3) present in `git log --oneline --all`: FOUND
- `uv run pytest tests/test_m2_gta_adapter.py tests/test_m2_baseline_measurement.py -q`: 14 passed
- `git diff --name-only scripts/hackathon/score_tracks.py scripts/hackathon/baseline_common.py pyproject.toml uv.lock`: empty
- `shasum -a 256` of the input BoT-SORT tracks parquet: identical before/after (`bcaf89e276e54301a9c9908148eda14c56f083c349087ff52f4627a8ba30ee1b`)
- `git status --porcelain vendor/`: empty (only committed `README.md`/`.gitignore`, cloned tree ignored)
- `grep -v '^ *#' scripts/hackathon/measure_gta.py | grep -c "DBSCAN"`: 0
- `grep -c "pip install torchreid\|sports_model" scripts/hackathon/measure_gta.py`: 0
- `data/reference/baseline-methods/summary.csv` GTA row: exactly 1, `human_pass_k`/`human_pass_n` empty, `notes` names the OSNet substitution
- `data/reference/baseline-methods/per_clip.csv` GTA rows: exactly 61 distinct `clip_number`
- `git status --porcelain`: clean (all plan files committed; gitignored intermediates under `data/processed/baseline-methods/` untracked as intended)

---
*Phase: M2-02-ehrliche-baseline*
*Completed: 2026-09-01*
