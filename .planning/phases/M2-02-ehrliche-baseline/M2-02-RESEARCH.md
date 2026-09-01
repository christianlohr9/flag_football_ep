# Phase M2-2: Ehrliche Baseline - Research

**Researched:** 2026-09-01
**Domain:** Multi-object tracker benchmarking (BoT-SORT, ByteTrack, Deep-EIoU, Global Tracklet Association) on a frozen detection set, license-gated dependency selection
**Confidence:** HIGH (stack/licensing — direct package inspection + GitHub API), MEDIUM (compute estimates — extrapolated, not yet measured), LOW (Deep-EIoU/GTA measured accuracy — no comparable numbers exist for this exact benchmark)

## Summary

The four candidate methods split cleanly into two groups by integration cost. **BoT-SORT and ByteTrack are both already available for free**: the installed `trackers==2.6.0` (Roboflow, Apache-2.0) package ships `BoTSORTTracker` (already used in `cv/track.py`) AND a native `ByteTrackTracker` with an almost identical constructor/`update()` API — no new package, no license research needed, no new install. **BoT-SORT does not even need to be re-run**: the canonical pre-bundle tracks parquet at `data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet` already covers all 61 clips, was produced from the frozen detector run `87a8a5222f7a472787875e974d089c44`, and scores 15/61 (24.6%) — this file IS the BASE-01 BoT-SORT measurement, verified by directly re-running `score_tracks.py` against it.

**Deep-EIoU cannot be measured within the license gate.** Its reference implementation (`github.com/hsiangwei0903/Deep-EIoU`) carries **no LICENSE file** — GitHub's API confirms `license: null`, and the repo root has no `LICENSE`/`COPYING` file at all. Under default copyright this is "all rights reserved": no permission to install, run, or vendor the code exists, not even locally. No permissively-licensed reimplementation exists elsewhere (`boxmot`, the only other tracker toolkit with sports-tuned methods, is itself AGPL-3.0 and does not ship Expansion-IoU or Deep-EIoU anyway — it ships DeepOCSORT/BoTSORT/StrongSORT/OCSORT/ByteTrack). The recommendation is to **skip Deep-EIoU and document the reason** (BASE-01/BASE-03 require it be documented, not necessarily measured — CONTEXT.md's D-02 AGPL-skip precedent extends naturally to "no license" candidates).

**Global Tracklet Association (GTA / `gta-link`) is measurable.** The reference repo (`github.com/sjc042/gta-link`) is confirmed **MIT-licensed** (GitHub API `license.spdx_id: mit`, LICENSE file present in repo root). It is post-processing over existing tracklets (splits tracklets containing multiple identities, merges tracklets belonging to the same identity, via OSNet appearance embeddings + DBSCAN clustering) — it can run directly on this project's own BoT-SORT tracklet output. It vendors the *official* `KaiyangZhou/deep-person-reid` (torchreid) repository under `reid/` as its embedding backend — itself confirmed MIT, 4,905 stars, actively maintained (pushed 2026-01-09). This sidesteps a real trap: the PyPI package literally named `torchreid` is a **third-party repackaging by a different maintainer** (`kadirnar`/`goksenin-uav`), not the canonical KaiyangZhou project — do not `pip install torchreid` from PyPI; use gta-link's vendored copy or `pip install git+https://github.com/KaiyangZhou/deep-person-reid.git` directly. The sports-finetuned OSNet checkpoint (`sports_model.pth.tar-60`) that both Deep-EIoU and gta-link reference has no traceable license or hosting of its own (informal Google Drive link, same filename in both unlicensed-adjacent projects) — substitute the officially-hosted, generically-licensed OSNet backbone (`osnet_x1_0`, ImageNet/Market-1501 pretrained, hosted from KaiyangZhou's own Google Drive links inside the MIT repo) instead, and document the substitution honestly as a measured-with-caveat choice, not a silent swap.

**The integration surface is already built for this.** The frozen detections parquet (`data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet`, 384,689 rows, all 61 clips, single `detector_run_id`) is the shared input every method consumes. `score_tracks.py` only requires 8 columns (`session_id, clip_number, frame_index, track_id, bbox_x1..y2`) from any submitted tracks file — a thin adapter per method is enough, no need to populate the full `TRACKING_COLUMNS` schema. 17,059 player-track crop images already exist on disk at `data/labels/.../bundle-inputs/crops/` covering all 61 clips — GTA's appearance step can embed these directly without re-cropping or re-decoding video.

**Primary recommendation:** Measure BoT-SORT (reuse existing tracks parquet, no rerun), ByteTrack (`trackers.ByteTrackTracker`, new ~50-line adapter script, motion-only — no video decode needed at all since CMC is BoT-SORT-only), and GTA (vendor `gta-link`'s MIT-licensed split/merge code + KaiyangZhou's MIT OSNet against the existing crops and BoT-SORT tracklets). Skip Deep-EIoU with a documented, defensible reason and surface it in the challenge description as-is (an honest "this one hits a real license wall" is itself useful information for the hackathon teams, who will face the identical constraint under OPS-01).

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Detection ingestion (frozen Parquet read) | Batch script (`scripts/hackathon/`) | — | Read-only, no video decode needed for 3 of 4 methods |
| Motion-only tracking (ByteTrack) | Batch script | `trackers` library (Apache-2.0) | Pure CPU association over existing boxes, no CMC/appearance |
| Motion+CMC tracking (BoT-SORT) | Already-produced artifact (`cv/track.py` output) | — | Not recomputed — the canonical 61-clip tracks parquet already exists |
| Appearance embedding (GTA/OSNet) | Batch script | PyTorch/MPS (M5 Max) | Runs over precomputed crop images, CPU/MPS-light (~2.2M-param OSNet) |
| Tracklet split/merge (GTA clustering) | Vendored library code (`gta-link`, MIT) | scikit-learn (DBSCAN, already a dependency) | Pure CPU post-processing over pickled tracklets |
| Scoring | `scripts/hackathon/score_tracks.py` (existing, unmodified) | `cv.continuity` (existing) | Shared harness — every method scored identically, no forking |
| Results publication | `docs/hackathon-challenge-reid.md`, `docs/hackathon-bundles.md` | — | Docs-tier, not code-tier; both confirmed collision-free with 2.2 waves 7-11 |

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **Reference value is the REAL baseline:** 15/61 = 24.6% (BASE-04's comparison point), not 77%. The existing BoT-SORT run IS the first of the four measurements — re-scored with the same harness for comparability, not re-invented.
- **License gate per candidate (D-02):** before installing ANY tracker package, its license must be verified Apache/MIT/BSD-class. Known landscape from the challenge doc: `trackers` (Apache-2.0, already installed, has ByteTrack implementations in recent versions — verify), `gta-link`/Global Tracklet Association research code (license to verify), Deep-EIoU research repo (license to verify). AGPL candidates are measured ONLY if runnable without installing AGPL code into the project (e.g. skipped with a documented reason) — D-02 is not waived for measurements that would leave AGPL in the dependency tree.
- **Same detections, same scoring:** all four methods consume the SAME frozen detections (data/labels/.../bundle-inputs/detections.parquet, frozen run 87a8a522…) and are scored by `scripts/hackathon/score_tracks.py` against `data/reference/continuity_review.csv` — no method gets different inputs.
- **Comparability discipline:** per-domain/per-clip outputs retained; full-61 denominator; n on every rate; documented, repeatable start command per method (BASE-03).
- **Results destination:** measured table into `docs/hackathon-challenge-reid.md` (Baseline section) AND `docs/hackathon-bundles.md`/new doc as appropriate — coordinate with 2.2's pending edits; keep the diff small and commit promptly.
- **Target adjustment (BASE-04):** if a method clearly beats the baseline, the plan surfaces the recommendation as a checkpoint for the USER (the 90% target is a submitted-challenge parameter — changing it is the user's call toward BWI).
- **Package legitimacy:** any NEW package install follows the plan-04 precedent — [ASSUMED]-provenance packages get a blocking human gate; well-known registry-verified packages with clean metadata may proceed with the research documented.

### Claude's Discretion

Which ByteTrack/Deep-EIoU/GTA implementations to use (prefer already-installed `trackers`; vendoring small permissively-licensed research code is acceptable with license file retained); exact CLI/module layout (new `scripts/hackathon/` or `cv/` extension — avoid files 2.2 waves 7–11 modify); run orchestration and output table format.

### Deferred Ideas (OUT OF SCOPE)

- Continuous metric (M2-4) — keep per-clip fragment/switch counts in outputs so M2-4 can consume them without re-running trackers.
- Transfer-domain measurements (TRANS-01, v2) — transfer detections already exist.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| BASE-01 | BoT-SORT, ByteTrack, Deep-EIoU and Global Tracklet Association are each measured once on the benchmark | BoT-SORT: reuse existing tracks parquet (verified 15/61). ByteTrack: `trackers.ByteTrackTracker`, thin adapter, no new dependency. Deep-EIoU: verified unmeasurable within the license gate (no LICENSE on reference repo) — documented skip, not a silent omission. GTA: `gta-link` (MIT) + KaiyangZhou torchreid (MIT), vendorable, runs on existing crops + BoT-SORT tracklets. |
| BASE-02 | Measured values land in the challenge description, not only in the repo | `docs/hackathon-challenge-reid.md` §Baseline-Zahlen confirmed collision-free with 2.2 waves 7-11 (grep across wave 15-20 frontmatter `files_modified` — no hit) |
| BASE-03 | A documented start command per method runs on the provided infrastructure | Exact CLI patterns provided per method below (Code Examples); torch/MPS confirmed available (torch 2.13.0, `mps available: True`) on the primary machine |
| BASE-04 | If a ready method clearly beats 24.6%, the 90% target is adjusted with reasons, surfaced to the user | Checkpoint pattern (not automated) — planner inserts a `checkpoint:human-verify`/decision gate after all four numbers are in, comparing against the 15/61=24.6% (61-clip) / 10/43=23.3% (dev-pool) reference values |
</phase_requirements>

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `trackers` | 2.6.0 (already installed, `cv` extras) | BoT-SORT (existing) + ByteTrack (new use) association | Already the project's only tracking dependency (Apache-2.0, C-06); ships both algorithms natively as of this version — verified via `import trackers; trackers.__all__` on the installed package, not training-data recall |
| `supervision` | ≥0.30.0 (already installed) | `sv.Detections` container both trackers consume | Already a project dependency, MIT-licensed |
| `polars` | ≥1.5.0 (already installed) | Parquet I/O for detections/tracks | Already the project's dataframe library |

### Supporting (GTA measurement only)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `gta-link` (vendored from `github.com/sjc042/gta-link`, MIT, no PyPI release) | pin to a specific commit SHA, not a moving branch | Tracklet split (DBSCAN over embeddings) + merge (spatial+appearance connection) — the actual GTA algorithm | Only for the GTA measurement; not a runtime dependency of anything else in the project |
| `torchreid` (install from `git+https://github.com/KaiyangZhou/deep-person-reid.git`, MIT — **NOT** the PyPI package literally named `torchreid`) | pin to a commit SHA (repo has no recent version tags visible via API metadata alone — verify tag list before pinning) | OSNet appearance embedding extractor GTA needs | Only for the GTA measurement |
| `gdown` | ≥6.1.0 (PyPI, MIT, `github.com/wkentaro/gdown`) | Fetch the officially-hosted OSNet pretrained checkpoint (`osnet_x1_0`) from KaiyangZhou's Google Drive links | Only if the checkpoint isn't fetched by hand once and committed/cached locally |
| `scikit-learn` | ≥1.5.1 (already installed) | DBSCAN clustering inside `gta-link`'s `refine_tracklets.py` | Already a project dependency |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Deep-EIoU (skip) | Reimplement "Expansion IoU" from the paper using `trackers`'s own `CBIoUTracker`/`BIoU` (Cascaded-Buffered IoU is conceptually close: also an "expand the box, then match" strategy) | This would NOT be "measuring Deep-EIoU" — it would be measuring a different, self-built method under a borrowed name. Explicitly out of scope for a challenge-giver measurement phase (BASE-01 measures existing methods; building one is the hackathon team's job, per REQUIREMENTS.md "Out of Scope: Das Tracking-Verfahren selbst bauen"). Available as an optional *extra* measurement the planner could label honestly (e.g. "CBIoU, a Deep-EIoU relative, MIT-licensed, measured for context") but never presented as Deep-EIoU itself. |
| `gta-link`'s sports-finetuned OSNet checkpoint (`sports_model.pth.tar-60`) | KaiyangZhou's officially-hosted `osnet_x1_0` (ImageNet/Market-1501 pretrained, generic person-ReID, not sports-finetuned) | The generic checkpoint likely underperforms the sports-finetuned one on this exact task — document as a measured-with-caveat limitation, not hidden. The sports checkpoint's provenance (informally hosted, same filename appears in the unlicensed Deep-EIoU repo) is not clean enough to use under the D-02 gate's spirit. |
| Vendoring `gta-link`'s `generate_tracklets.py` (expects a SportsMOT/SoccerNet dataset directory layout) | Skip `generate_tracklets.py` entirely; write a small adapter that converts the existing BoT-SORT `tracks.parquet` directly into the `Tracklet` objects `refine_tracklets.py`/`refine_tracklets_batched.py` operate on, embedding from the existing 17,059 crops | `generate_tracklets.py` assumes a specific benchmark folder structure this project doesn't have and doesn't need to reproduce — the split/merge logic (`Tracklet.py`, `refine_tracklets*.py`) is the actual GTA contribution and is a much thinner integration target |

**Installation (GTA measurement path only — everything else needs zero new installs):**
```bash
# vendor gta-link at a pinned commit (MIT — LICENSE file retained per CONTEXT.md discretion note)
git clone https://github.com/sjc042/gta-link.git vendor/gta-link
cd vendor/gta-link && git checkout <pin-to-verified-commit-sha>

# official torchreid — NOT the PyPI "torchreid" package (different, unrelated maintainer)
uv run python -m pip install "git+https://github.com/KaiyangZhou/deep-person-reid.git@<pinned-sha>"

# for fetching the officially-hosted generic OSNet checkpoint
uv add --optional cv gdown  # or reuse the vendored gta-link's own reid/ requirements.txt
```

**Version verification performed this session:**
- `trackers==2.6.0` — confirmed installed via `uv run python -c "import importlib.metadata as m; print(m.version('trackers'))"`. `ByteTrackTracker` confirmed present via `trackers.__all__` and direct source read of `trackers/core/bytetrack/tracker.py`.
- `gta-link` — no PyPI package; GitHub API confirms `license.spdx_id: "mit"`, created 2024-04-21, last pushed 2025-12-12, 91 stars, 14 forks. Root listing (`gh api repos/sjc042/gta-link/contents`) confirms a `LICENSE` file exists.
- `KaiyangZhou/deep-person-reid` — GitHub API confirms `license: "MIT"`, created 2018-03-11, last pushed 2026-01-09, 4,905 stars, 1,217 forks. `gta-link`'s own `reid/` subfolder is an exact structural match (`.flake8`, `Dockerfile`, `LICENSE`, `torchreid/`, `torchreid.egg-info`) — i.e. gta-link already vendors this exact repo, so a *separate* torchreid install may not even be necessary if `gta-link`'s `reid/` is used as-is.
- PyPI `torchreid` (v0.2.5, per `pypi.org/pypi/torchreid/json`) — license classifier says MIT but the maintainer (`kadirnar`, linking to `github.com/goksenin-uav/torchreid-pip`) is **not** KaiyangZhou. `slopcheck scan --pkg pypi torchreid` returns `OK` with no flags — this is exactly the gap the package-name-provenance rule exists for: registry-clean does not mean "the package you think it is." Treat any use of the bare PyPI `torchreid` name as `[ASSUMED]` and prefer the git-source install.
- `hsiangwei0903/Deep-EIoU` — GitHub API confirms `license: null` (no SPDX-detected license), created 2023-06-23, last pushed 2024-08-22, 72 stars. Root file listing (`Deep-EIoU`, `Readme.md`, `demo.mp4`, `detection`, `embedding`) confirms no `LICENSE`/`COPYING` file present.
- `boxmot` (searched to check for an alternative Deep-EIoU/ExpansionIoU implementation) — AGPL-3.0 (already excluded, C-06/D-02), and does not ship Deep-EIoU or ExpansionIoU regardless (only DeepOCSORT/BoTSORT/StrongSORT/OCSORT/ByteTrack, each with LightMBN ReID).

## Package Legitimacy Audit

| Package | Registry | Age | Downloads/Stars | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `trackers` | PyPI | already installed (Phase 2.1) | — | none linked on PyPI (`NO_REPO` info flag) | `OK` (info: NO_REPO) | Approved — already vetted in Phase 2.1, `ByteTrackTracker` verified present in installed 2.6.0 by direct source inspection |
| `gdown` | PyPI | 6.1.0, `github.com/wkentaro/gdown` | — | linked, MIT | `OK` | Approved |
| `torchreid` (bare PyPI name) | PyPI | v0.2.5, third-party (`kadirnar`) | — | `github.com/goksenin-uav/torchreid-pip` (not canonical) | `OK` (slopcheck cannot detect maintainer-mismatch, only hallucination) | **[ASSUMED] — do not use.** Maintainer differs from the canonical `KaiyangZhou/deep-person-reid`. Use git-source install instead. |
| `deep-person-reid` (git source, official) | GitHub (not PyPI) | created 2018, 4,905 stars, MIT | 1,217 forks | `github.com/KaiyangZhou/deep-person-reid` | N/A (not a registry package) | Approved — install via `pip install git+https://github.com/KaiyangZhou/deep-person-reid.git`, or reuse `gta-link`'s vendored `reid/` copy |
| `gta-link` (git source) | GitHub (not PyPI) | created 2024-04-21, 91 stars, MIT | 14 forks | `github.com/sjc042/gta-link` | N/A (not a registry package) | Approved — vendor at a pinned commit SHA, retain LICENSE file |
| `Deep-EIoU` (reference impl) | GitHub (not PyPI) | created 2023-06-23, 72 stars, **no license** | 72 stars | `github.com/hsiangwei0903/Deep-EIoU` | N/A | **REMOVED — no LICENSE file, fails the D-02 gate.** Not installed, not vendored, not run. |
| `sports_model.pth.tar-60` checkpoint | informal Google Drive hosting, referenced by both `gta-link` and `Deep-EIoU` | unknown | unknown | none traceable | N/A | **REMOVED — no license/provenance.** Substitute KaiyangZhou's officially-hosted generic `osnet_x1_0` checkpoint. |
| `torchreid-pip` (name guessed while researching) | PyPI | — | — | — | `SLOP` (`NOT_FOUND` — "Your AI made it up") | Confirmed hallucinated name, never existed; the real PyPI package is spelled `torchreid` (see above, still flagged `[ASSUMED]`) |

**Packages removed due to slopcheck/license verdict:** `Deep-EIoU` reference implementation (no license), `sports_model.pth.tar-60` checkpoint (no traceable license/provenance).
**Packages flagged as suspicious:** bare PyPI `torchreid` — maintainer mismatch with the canonical project; the planner should gate its use behind a `checkpoint:human-verify` (or simply avoid it by using the git-source install, which needs no such gate since it's unambiguously the correct repository).

## Architecture Patterns

### System Architecture Diagram

```
                    ┌──────────────────────────────────────────────┐
                    │ data/labels/.../bundle-inputs/detections.parquet │
                    │ (384,689 rows, 61 clips, single detector_run_id) │
                    └───────────────────────┬──────────────────────┘
                                             │  (SAME input to all 4 methods)
              ┌──────────────────────────────┼──────────────────────────────┐
              │                              │                              │
              ▼                              ▼                              ▼
   ┌─────────────────────┐      ┌─────────────────────┐      ┌───────────────────────┐
   │ BoT-SORT             │      │ ByteTrack             │      │ Deep-EIoU              │
   │ ALREADY PRODUCED:     │      │ trackers.ByteTrack-   │      │ SKIPPED — no LICENSE   │
   │ data/processed/       │      │ Tracker (motion-only, │      │ on reference repo,     │
   │ tracking/…_tracks.    │      │ no video decode, no   │      │ documented reason in   │
   │ parquet (61 clips)    │      │ CMC needed)            │      │ the results table       │
   └──────────┬───────────┘      └──────────┬───────────┘      └───────────┬────────────┘
              │                              │                              │
              │                              │                              │
              ▼                              ▼                              │
   ┌───────────────────────────────────────────────────┐                   │
   │ tracks.parquet / .csv, REQUIRED_TRACK_COLUMNS      │                   │
   │ (session_id, clip_number, frame_index, track_id,    │                   │
   │  bbox_x1..y2) — 8 columns is all score_tracks.py    │                   │
   │  needs                                               │                   │
   └──────────────────────────┬────────────────────────┘                   │
                               │                                             │
                               ▼                                             │
              ┌───────────────────────────────┐                             │
              │ GTA post-processing (only for  │◄────────────────────────────┘
              │ the BoT-SORT tracklets):        │   (not applicable — no Deep-EIoU
              │ 17,059 existing crops → OSNet   │    tracklets exist to refine)
              │ embeddings → DBSCAN split/merge │
              │ (vendored gta-link, MIT)        │
              └───────────────┬─────────────────┘
                               │
                               ▼
              ┌──────────────────────────────────────┐
              │ scripts/hackathon/score_tracks.py      │
              │ (existing, UNMODIFIED — every method   │
              │ scored by the identical harness)        │
              │ --review data/reference/                │
              │ continuity_review.csv                   │
              └───────────────────┬────────────────────┘
                                   │
                                   ▼
              ┌──────────────────────────────────────┐
              │ 4-row comparison table → docs/          │
              │ hackathon-challenge-reid.md §Baseline   │
              │ (each row: k/n, %, start command,       │
              │ license note, honest caveats)           │
              └──────────────────────────────────────┘
```

### Recommended Project Structure

```
scripts/hackathon/
├── score_tracks.py                 # existing, unmodified
├── measure_bytetrack.py            # NEW — thin adapter: detections.parquet -> ByteTrackTracker -> tracks output
├── measure_gta.py                  # NEW — thin adapter: existing BoT-SORT tracks.parquet + crops -> vendored gta-link split/merge -> tracks output
└── run_baseline_suite.py           # NEW (optional) — orchestrates all 3 measurable methods + prints the 4-row table (Deep-EIoU row hardcoded as SKIPPED with reason)

vendor/gta-link/                    # NEW — pinned-commit vendor copy, MIT LICENSE retained at vendor/gta-link/LICENSE
docs/hackathon-challenge-reid.md    # EXTEND — §Baseline-Zahlen gets the 4-row table
docs/hackathon-bundles.md           # EXTEND — cross-reference/coordinate, per CONTEXT.md
```

**Collision check against 2.2 waves 7-11 (plans 02.2-15 through 02.2-20):** grepped every wave's frontmatter `files_modified` list. None touch `cv/track.py`, `cv/schema.py`, `scripts/hackathon/*`, `docs/hackathon-challenge-reid.md`, or `docs/hackathon-bundles.md`. They touch: `cv/detect.py`, `tests/test_cv_detect_train.py`, `docs/dataset-buildout.md`, `data/labels/al-iteration-2/selection.json`, `data/labels/dataset.dvc`, `docs/cv-setup.md`, `docs/dataset-publication.md`, `docs/dataset-card.md`, `tests/test_phase22_docs.py`, `.dvc/config`, `ffep.toml`. New files under `scripts/hackathon/` and `vendor/gta-link/` are unambiguously collision-free; extending `docs/hackathon-challenge-reid.md` §Baseline-Zahlen is also collision-free (that section was last touched by already-completed plan 02.2-10, not by any pending wave-7-11 plan).

### Pattern 1: Motion-only tracker adapter (ByteTrack)

**What:** Read the frozen detections Parquet, group by clip, replay per-frame through `ByteTrackTracker.update()`, write the 8-column tracks schema `score_tracks.py` needs.
**When to use:** For any of the 4 methods that don't need CMC (BoT-SORT is the only one needing decoded video frames in this stack).
**Example:**
```python
# Source: trackers/core/bytetrack/tracker.py (installed 2.6.0, Apache-2.0) +
# src/flag_football_ep/cv/track.py's existing BoT-SORT loop shape (same project convention)
import polars as pl
import supervision as sv
from trackers import ByteTrackTracker

detections_df = pl.read_parquet(DETECTIONS_PARQUET)
rows = []
for clip_number in sorted(detections_df["clip_number"].unique()):
    clip_df = detections_df.filter(pl.col("clip_number") == clip_number).sort("frame_index")
    tracker = ByteTrackTracker()  # defaults — not hand-tuned, unlike BoT-SORT's measured config
    for frame_index, frame_df in clip_df.group_by("frame_index", maintain_order=True):
        dets = sv.Detections(
            xyxy=frame_df.select("bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2").to_numpy(),
            confidence=frame_df["confidence"].to_numpy(),
            class_id=frame_df["class_name"].replace({"player": 0, "referee": 1}).to_numpy(),
        )
        tracked = tracker.update(dets, timestamp=frame_df["timestamp_s"][0])  # NOTE: frame=None — ByteTrack ignores it, no video decode needed
        for i in range(len(tracked)):
            if int(tracked.tracker_id[i]) == -1:
                continue
            x1, y1, x2, y2 = tracked.xyxy[i]
            rows.append({
                "session_id": frame_df["session_id"][0], "clip_number": clip_number,
                "frame_index": frame_index[0], "track_id": int(tracked.tracker_id[i]),
                "bbox_x1": float(x1), "bbox_y1": float(y1), "bbox_x2": float(x2), "bbox_y2": float(y2),
            })
```

### Pattern 2: GTA as post-processing over existing tracklets (not a fresh tracker)

**What:** GTA never runs from raw detections — it refines an *already-tracked* tracklet set. Feed it the existing BoT-SORT output (`data/processed/tracking/2026-05-16_..._tracks.parquet`), not the raw detections.
**When to use:** Only for the GTA measurement; the input is BoT-SORT's tracks, the output is a refined version of those same tracks (fewer ID switches / fragments, ideally).
**Example (conceptual — CLI shape from `gta-link`'s documented usage, verified via `WebFetch` on its README):**
```bash
# Source: github.com/sjc042/gta-link README.md (MIT) — generate_tracklets.py assumes a
# SportsMOT/SoccerNet directory layout this project doesn't have; the thinner path is a
# custom adapter that builds Tracklet objects directly from tracks.parquet + the existing
# 17,059 crops, then calls refine_tracklets.py's split/merge functions in-process.
python refine_tracklets.py \
  --dataset custom --tracker botsort-baseline \
  --track_src <adapter-produced tracklet .pkl dir> \
  --use_split --min_len 100 --eps 0.6 --min_samples 10 --max_k 3 \
  --use_connect --spatial_factor 1.0 --merge_dist_thres 0.4
```

### Anti-Patterns to Avoid

- **Re-detecting for ByteTrack/GTA:** Both operate on the frozen `detections.parquet`/existing tracks. Re-running the detector would violate "same detections" (CONTEXT.md locked decision) and waste the M5 Max's time for no benefit.
- **Downloading the SportsMOT-finetuned OSNet checkpoint from either project's informal Google Drive link:** No traceable license. Use the officially-hosted generic OSNet weights from the MIT-licensed `KaiyangZhou/deep-person-reid` repo instead, and say so in the results.
- **Presenting a self-built "Expansion-IoU-flavored" tracker as "Deep-EIoU":** BASE-01 measures existing methods; mislabeling a homebrew variant as the paper's method would misinform the challenge description.
- **Silent bundle-doc collision:** Do not touch `docs/dataset-buildout.md` or `docs/cv-setup.md` from this phase — those belong to 2.2 waves 7-11 and are actively being edited there.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|--------------|-----|
| ByteTrack association logic | A custom two-stage IoU matcher | `trackers.ByteTrackTracker` | Already installed, Apache-2.0, verified present in 2.6.0 — building one from the paper would be exactly the "solve the ReID task" work this phase is explicitly not supposed to do |
| Tracklet appearance clustering (GTA's split/merge) | A custom DBSCAN-over-embeddings pipeline | Vendored `gta-link` (`Tracklet.py`, `refine_tracklets*.py`), MIT | The paper's actual contribution is the clustering heuristics (min_samples, eps, spatial_factor, merge_dist_thres) — reimplementing loses fidelity to what's being measured |
| ReID embedding extraction | A custom CNN or from-scratch OSNet | `KaiyangZhou/deep-person-reid`'s OSNet, MIT, pretrained weights already published | OSNet is a purpose-built lightweight person-ReID backbone; training one from scratch on 17k crops would be a different (and much bigger) research project |

**Key insight:** every non-trivial piece of this phase (ByteTrack, appearance embedding, tracklet clustering) already has a permissively-licensed, purpose-built implementation. The engineering work is entirely in the *adapter* layer (schema conversion, avoiding file collisions, avoiding the untraceable checkpoint), not in the tracking algorithms themselves.

## Common Pitfalls

### Pitfall 1: Treating "installed via pip, license classifier says MIT" as sufficient verification
**What goes wrong:** The bare PyPI `torchreid` package passes every automated check (slopcheck `OK`, MIT classifier) yet is not the project anyone means when they say "torchreid."
**Why it happens:** Registry metadata reflects what the *uploader* claims, not who wrote the code inside.
**How to avoid:** Cross-check the PyPI page's linked homepage/repo against the canonical GitHub org before installing anything by a recognizable-but-generic name (reid, tracker, detector toolkits are a common squat/rename target).
**Warning signs:** PyPI maintainer username doesn't match the GitHub org everyone cites in papers/blog posts; PyPI "Homepage" links to a fork, not the original.

### Pitfall 2: Assuming a research repo without a visible LICENSE badge is "probably MIT like everything else in ML"
**What goes wrong:** `hsiangwei0903/Deep-EIoU` has 72 stars and reads like a normal open research repo, but GitHub's own license detector returns `null` — there is no LICENSE file at all, meaning default copyright applies.
**Why it happens:** Many research code drops never add a LICENSE file; readers assume permissiveness because the code is public.
**How to avoid:** Always check via `gh api repos/<owner>/<repo> --jq '.license'` (or the GitHub UI's license badge) before use — "public on GitHub" and "licensed for reuse" are different facts.
**Warning signs:** No LICENSE badge on the repo's GitHub page; `gh api ... --jq '.license'` returns empty/null.

### Pitfall 3: Running `generate_tracklets.py` as-is and hitting dataset-layout errors
**What goes wrong:** `gta-link`'s entry script expects SportsMOT/SoccerNet-style directory conventions (per-clip frame folders, specific MOT-format filenames) this project's data doesn't have.
**Why it happens:** The repo was built for benchmark submission workflows, not arbitrary integration.
**How to avoid:** Skip `generate_tracklets.py`; call the split/merge functions in `refine_tracklets.py`/`Tracklet.py` directly from a custom adapter that builds `Tracklet` objects from `tracks.parquet` + the existing crops.
**Warning signs:** `FileNotFoundError` on paths implying a specific benchmark folder structure.

### Pitfall 4: Re-decoding video for methods that don't need it
**What goes wrong:** Copying `cv/track.py`'s per-clip `cv2.VideoCapture` decode loop for ByteTrack, even though ByteTrack's `update()` explicitly ignores the `frame` argument (only warns if passed).
**Why it happens:** `cv/track.py` is the obvious reference implementation and its decode-for-CMC step is easy to copy reflexively.
**How to avoid:** For ByteTrack, never open the clip video at all — only read `detections.parquet`. This alone should make the ByteTrack measurement dramatically faster than the BoT-SORT run was.
**Warning signs:** A `cv2.VideoCapture` call anywhere in the ByteTrack adapter.

## Code Examples

See "Architecture Patterns" above (Pattern 1 and Pattern 2) — those are the load-bearing snippets.

### Reproducing the existing BoT-SORT baseline (BASE-01, no new code needed)

```bash
# Source: this session's direct verification — score matches the documented 15/61 = 24.6%
uv run python scripts/hackathon/score_tracks.py \
  --tracks data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet \
  --review data/reference/continuity_review.csv
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| "77% BoT-SORT baseline" (n=20, optimistic upper bound) | 15/61 = 24.6% (full 61-clip set), 10/43 = 23.3% (dev pool) | 2026-09-01, plan 02.2-03/02.2-10 | This phase's reference point for BASE-04's "clearly beats" test moves from an unrealistic 77% ceiling to a genuinely low bar — makes it plausible that ByteTrack or GTA legitimately beat it, which is exactly what BASE-04 anticipates |
| `trackers` package assumed to ship only OC-SORT/BoT-SORT (per `cv/track.py`'s own docstring, written when OC-SORT was the tracker) | `trackers==2.6.0` ships SORT, ByteTrack, BoT-SORT, OC-SORT, CBIoU, McByte | Verified this session against the installed 2.6.0 (not training-data recall) | ByteTrack requires zero new dependencies — a materially cheaper BASE-01 measurement than assumed when the challenge doc was drafted |

**Deprecated/outdated:** The "20/61 evaluated, 77% upper bound" framing in earlier drafts of `docs/hackathon-challenge-reid.md` — already superseded by plan 02.2-10 (confirmed via `grep -c "obere Schranke 47/61"` returning 0 in that plan's acceptance checks).

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|----------------|
| A1 | ByteTrack tracking-only compute (no video decode, no CMC) will take on the order of seconds-to-low-minutes for all 61 clips / 384,689 detection rows | Summary, Common Pitfalls | If wrong (e.g. per-frame Hungarian matching is slower than assumed at this detection density), the planner should still budget generously — even a 10x miss is well under any reasonable time box, so low risk |
| A2 | GTA embedding extraction over 17,059 existing crops with `osnet_x1_0` on M5 Max (MPS or CPU) completes in low single-digit minutes | Summary | If wrong, still bounded — OSNet is a ~2.2M-parameter model, this is a soft risk, not a blocker |
| A3 | The generic (non-sports-finetuned) OSNet checkpoint will still let GTA measurably split/merge tracklets, just possibly less effectively than the sports-finetuned checkpoint would | Alternatives Considered | If GTA's result looks uninformative (e.g. no measurable change vs. raw BoT-SORT) due to weak generic embeddings, the honest response is to report that limitation, not to silently reach for the untraceable checkpoint |
| A4 | No `gta-link` version tags exist to pin against (only commit SHAs) — verify the tag list before the planner writes an exact pin | Standard Stack | If tags do exist, pinning to a moving branch (`main`) instead of a tag/SHA would be a reproducibility gap — low risk, easily checked by the planner with `git ls-remote --tags` before writing the exact install command |
| A5 | `gta-link`'s vendored `reid/` subfolder (matching `deep-person-reid`'s file layout) is in fact the unmodified official repo and not a divergent fork with the same file names | Standard Stack | If it diverges, the "reuse gta-link's vendored copy" shortcut in Alternatives Considered wouldn't be safe — the planner should diff a few files or just install `deep-person-reid` directly (git-source, not the risky PyPI name) as the safer default rather than relying on this assumption |

## Open Questions (RESOLVED)

1. **Exact commit SHA to pin `gta-link` and `deep-person-reid` at**
   - What we know: both repos exist, are actively maintained, MIT-licensed, confirmed via GitHub API.
   - What's unclear: this research did not fetch the exact HEAD commit SHA at research time (repos may have moved between research and planning).
   - Recommendation: the planner/implementer should `git ls-remote` both repos immediately before writing the pin into a plan, not reuse any SHA implied by this document.
   - RESOLVED (plan M2-02-02, Task 1): the plan pins via a live `git ls-remote` at execution time and records the SHAs in `vendor/README.md`.

2. **Whether the generic OSNet checkpoint produces a GTA result different enough from raw BoT-SORT to be worth reporting**
   - What we know: GTA is model-agnostic post-processing; its whole value proposition depends on embedding quality.
   - What's unclear: whether a non-sports-finetuned OSNet gives GTA enough signal on this specific 5v5 flag-football footage (small, fast-moving, similarly-dressed players) to meaningfully split/merge tracklets.
   - Recommendation: measure it and report honestly either way — a null result ("GTA measured, no meaningful change from the generic checkpoint") is itself a valid, useful BASE-01 outcome, and should not be suppressed or spun.
   - RESOLVED (plan M2-02-02, Task 3): measured with the generic checkpoint, mandatory caveat text in the result row, null result reported honestly; the untraceable sports checkpoint stays forbidden.

3. **Whether the planner should also measure an unlabeled "CBIoU" row as a bonus (Deep-EIoU's closest permissively-licensed cousin already in `trackers`)**
   - What we know: `trackers.CBIoUTracker` implements a buffered/cascaded IoU strategy conceptually adjacent to Deep-EIoU's Expansion-IoU idea, Apache-2.0, zero extra install.
   - What's unclear: whether this is worth the extra row given it's explicitly NOT Deep-EIoU and BASE-01 doesn't ask for it.
   - Recommendation: Claude's Discretion territory — the planner can offer this as an optional 5th row explicitly labeled "not Deep-EIoU, included for context" rather than working around the Deep-EIoU gap by substitution.
   - RESOLVED (plan M2-02-01): CBIoU is measured and labelled "closest permissive cousin, NOT Deep-EIoU" throughout, incl. in plan M2-02-03's result table.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|--------------|-----------|---------|----------|
| `trackers` (BoT-SORT + ByteTrack) | BASE-01 (BoT-SORT re-score, ByteTrack measurement) | ✓ | 2.6.0 | — |
| `torch` + MPS backend | GTA embedding extraction | ✓ | 2.13.0, `mps available: True` | CPU fallback if MPS has issues with a specific op (OSNet is small enough this is a minor slowdown, not a blocker) |
| `scikit-learn` | GTA DBSCAN clustering | ✓ (already installed) | ≥1.5.1 | — |
| `gh` CLI / GitHub API access | Verifying licenses during planning/execution | ✓ (used throughout this research session) | — | — |
| `gta-link` git repo | GTA measurement | ✓ (publicly cloneable, verified reachable this session) | pin TBD by planner | If unreachable at execution time (network outage), GTA measurement is deferred, not silently skipped |
| `deep-person-reid` git repo | GTA embedding backend | ✓ (publicly cloneable, verified reachable this session) | pin TBD by planner | Same as above |
| Officially-hosted `osnet_x1_0` checkpoint (Google Drive) | GTA embedding backend | Not directly probed this session (Google Drive URLs aren't reliably HEAD-checkable via API) | — | If the Drive link is unreachable at execution time, `gdown` will fail loudly — the planner should treat first-run checkpoint fetch as a task with an explicit failure path, not assume it silently succeeds |

**Missing dependencies with no fallback:** none identified — every required piece is either already installed or publicly, redundantly available (multiple mirrors/sources exist for OSNet weights in the broader torchreid ecosystem if the primary Drive link fails).

**Missing dependencies with fallback:** MPS→CPU fallback for GTA's embedding step (soft, low risk given model size).

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest ≥8 (already configured, `pyproject.toml` `[tool.pytest.ini_options]`) |
| Config file | `pyproject.toml` (`testpaths = ["tests"]`) |
| Quick run command | `uv run pytest tests/test_hackathon_scoring.py tests/test_cv_track.py -q` |
| Full suite command | `uv run pytest -q` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|---------------------|--------------|
| BASE-01 (ByteTrack adapter) | ByteTrack adapter script produces a schema-conformant tracks output from a small synthetic detections fixture | unit | `uv run pytest tests/test_hackathon_baseline_measurement.py::test_bytetrack_adapter_schema -x` | ❌ Wave 0 |
| BASE-01 (BoT-SORT reuse) | The existing tracks parquet is read as-is, no accidental re-tracking | smoke | `uv run pytest tests/test_hackathon_baseline_measurement.py::test_botsort_reuses_existing_tracks_parquet -x` | ❌ Wave 0 |
| BASE-01 (Deep-EIoU skip) | The results table names Deep-EIoU as measured-skipped with a reason string, never silently absent | unit (docs-check, same convention as `tests/test_phase22_docs.py`) | `uv run pytest tests/test_hackathon_baseline_measurement.py::test_deep_eiou_documented_as_skipped -x` | ❌ Wave 0 |
| BASE-01 (GTA adapter) | GTA split/merge adapter round-trips a tiny synthetic tracklet set (2-3 tracks, a few crops) without needing the full 61-clip data or a real download | unit | `uv run pytest tests/test_hackathon_baseline_measurement.py::test_gta_adapter_smoke -x` | ❌ Wave 0 |
| BASE-03 (start commands) | Each method's documented CLI command in `docs/hackathon-challenge-reid.md` actually exists as a runnable script | smoke | `uv run pytest tests/test_phase22_docs.py -k hackathon -q` (existing convention, extend if needed) | ✓ (pattern exists) — extend, don't fork |
| BASE-02 (results in challenge doc) | `docs/hackathon-challenge-reid.md` §Baseline-Zahlen contains all 4 method rows | doc-grep check | `grep -c "ByteTrack" docs/hackathon-challenge-reid.md` (and similarly for the other 3 method names) | ❌ Wave 0 (grep-based acceptance check, not a pytest test) |

### Sampling Rate

- **Per task commit:** `uv run pytest tests/test_hackathon_baseline_measurement.py -q` (once it exists)
- **Per wave merge:** `uv run pytest -q` (full suite)
- **Phase gate:** Full suite green before `/gsd:verify-work`, plus the live re-run of `score_tracks.py` against the real 61-clip artifacts (not mocked) for at least the BoT-SORT/ByteTrack rows, since the whole point of BASE-01 is a real measurement, not a passing test suite

### Wave 0 Gaps

- [ ] `tests/test_hackathon_baseline_measurement.py` — new file, covers BASE-01's four sub-claims (schema conformance per method, BoT-SORT reuse-not-rerun, Deep-EIoU documented-skip, GTA adapter smoke test)
- [ ] No new framework/config install needed — pytest is already fully configured

## Security Domain

This phase is offline batch processing (local Parquet/video files, no network-facing input, no user auth) — most ASVS categories are not applicable. The one live-relevant category:

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|----------------|---------|--------------------|
| V2 Authentication | No | No auth surface in this phase |
| V3 Session Management | No | N/A |
| V4 Access Control | No | N/A |
| V5 Input Validation | Yes (narrow) | `score_tracks.py`'s existing `_validate_tracks_schema` already enforces required columns on any submitted tracks file — new adapter scripts should raise the same way on malformed intermediate outputs, not silently coerce |
| V6 Cryptography | No | N/A |
| Supply chain / dependency integrity | Yes | This is the phase's real risk surface — see Package Legitimacy Audit above. The concrete mitigations: pin `gta-link`/`deep-person-reid` to verified commit SHAs (not moving branches), never use the bare PyPI `torchreid` name, never fetch the untraceable sports-finetuned checkpoint |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|------------------------|
| Package-name confusion (a differently-maintained package with the same common name) | Spoofing | Verify the PyPI "Homepage"/"Source" link matches the canonical GitHub org before installing; prefer git-source installs pinned to a commit SHA for research code with no PyPI presence |
| Vendoring unlicensed research code | Tampering (of the project's own license posture) | Hard gate: `gh api repos/<owner>/<repo> --jq '.license'` must return a recognized SPDX ID before any code from that repo is copied/installed; `null` means stop, don't vendor |
| Untraceable model checkpoint (informal file-hosting link, no license, same filename shared across two unlicensed-adjacent projects) | Tampering / Repudiation | Prefer checkpoints hosted directly by the license-holder of the code that trained them (here: KaiyangZhou's own Google Drive links inside the MIT `deep-person-reid` repo) over third-hand informally-shared files |

## Sources

### Primary (HIGH confidence — direct tool verification this session)

- Installed `trackers==2.6.0` package source (`.venv/lib/python3.12/site-packages/trackers/`) — `__init__.py` exports, `core/bytetrack/tracker.py`, `core/botsort/tracker.py`, `core/cbiou/tracker.py`, `utils/iou.py` read directly
- `gh api repos/hsiangwei0903/Deep-EIoU --jq '.license'` and `.../contents` — confirmed null license, no LICENSE file
- `gh api repos/sjc042/gta-link --jq '.license'` and `.../contents` and `.../contents/reid` — confirmed MIT, LICENSE file present, vendored official `deep-person-reid` under `reid/`
- `gh api repos/KaiyangZhou/deep-person-reid --jq '{...}'` — confirmed MIT, 4,905 stars, actively maintained
- `pypi.org/pypi/torchreid/json` and `pypi.org/project/torchreid/` (WebFetch) — confirmed third-party maintainer mismatch
- `pypi.org/pypi/gdown/json` (WebFetch) — confirmed MIT, official `wkentaro/gdown` source
- `slopcheck scan --pkg pypi {torchreid, gdown, trackers, torchreid-pip}` — all results recorded in the Package Legitimacy Audit table above
- Direct read: `data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet` (384,689 rows, 61 clips), `data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet` (354,404 rows, 61 clips, `detector_run_id=87a8a5222f7a472787875e974d089c44`), `data/reference/hackathon_freeze.json`, crops directory (17,059 `.jpg` files across 61 clip subdirectories)
- `uv run python -c "import torch; ..."` — confirmed torch 2.13.0, MPS available on the primary machine
- Direct read of `.planning/phases/02.2-dataset-buildout/02.2-1[5-9]-PLAN.md` + `02.2-20-PLAN.md` frontmatter `files_modified` lists, cross-referenced via grep across all `02.2-*.md` files for `cv/dataset.py`/`cv/track.py`/`hackathon-challenge-reid`/`hackathon-bundles` mentions

### Secondary (MEDIUM confidence — WebFetch on official docs)

- `github.com/hsiangwei0903/Deep-EIoU` README (WebFetch) — confirms OSNet/torchreid dependency, ByteTrack+BoT-SORT lineage, `sports_model.pth.tar-60` checkpoint name
- `github.com/sjc042/gta-link` README + requirements.txt (WebFetch) — confirms MOT-format post-processing design, `generate_tracklets.py`/`refine_tracklets.py` CLI shape, dependency list

### Tertiary (LOW confidence — WebSearch, cross-verified where possible)

- Deep-EIoU/GTA paper summaries (HOTA numbers on SportsMOT/SoccerNet) — WebSearch only, not independently reproduced, included for context not as a claim about this project's benchmark
- `boxmot` tracker-list claim (DeepOCSORT/BoTSORT/StrongSORT/OCSORT/ByteTrack, no Deep-EIoU) — single WebSearch result, not cross-checked against the boxmot repo directly; low-stakes claim (only used to confirm no permissive Deep-EIoU alternative exists)

## Metadata

**Confidence breakdown:**
- Standard stack (trackers/ByteTrack availability, GTA/torchreid licensing): HIGH — verified by direct package/API inspection, not recall
- Deep-EIoU exclusion: HIGH — GitHub API license field is authoritative and unambiguous
- Architecture/integration surface (existing detections/tracks/crops files): HIGH — read directly off the working tree
- Compute estimates (ByteTrack runtime, GTA embedding runtime): MEDIUM — extrapolated from adjacent measured numbers (transfer-set decode+detect+track wall time), not directly measured for this exact workload
- Deep-EIoU/GTA expected accuracy on this specific 5v5 flag-football benchmark: LOW — no comparable published numbers exist for this domain; the entire point of BASE-01 is to produce that number, not predict it

**Research date:** 2026-09-01
**Valid until:** 2026-10-01 (30 days — licenses/packages are stable-ish, but `gta-link`/`deep-person-reid` commit SHAs will drift; re-verify the exact pin immediately before use regardless of this date)
