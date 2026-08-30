"""Unsupervised team-color/appearance classification of tracked player crops.

`TeamClassifier` is the SigLIP-embedding + UMAP-reduction + KMeans-clustering
pipeline (`github.com/roboflow/sports`, MIT) that splits tracked player crops into two
visual clusters without any hand-labeled team assignment: `fit` learns the two-cluster
embedding space from a batch of crops, `predict` assigns cluster ids to new crops.
`scikit-learn` (`KMeans`) is already a core project dependency; only the SigLIP
(`transformers`) and `umap-learn` pieces are new, both gated behind the `cv` extras
group (D-07) and imported inside function bodies only, never at module level.

No jersey-colour HSV thresholding exists anywhere in this module and none should be
added -- RESEARCH explicitly rejects it because the capture protocol already flags
exposure variance as a domain risk that would poison a colour-based split.

`TeamClassifier.predict`'s own cluster label 0/1 is arbitrary and unstable across
separate fits (a `TeamClassifier` fitted on one clip's crops has no relationship to the
label ordering of a `TeamClassifier` fitted on another clip). `assign_teams` re-anchors
that arbitrary label to a fixed real-world meaning before it ever reaches a track's
`team_id`: **`team_id` 0 is always the cluster whose sampled crops read as more red**
(`_redness_score`'s saturation-weighted hue-distance-from-red, median over the
session's fit crops), `team_id` 1 is the other cluster -- regardless of which arbitrary
label KMeans happened to assign to which cluster. When neither cluster reads as
meaningfully more red (ambiguous kit colours -- e.g. two dark/neutral kits), the KMeans
label order is kept as-is and `assign_teams` emits a notice, since guessing which grey
kit is "team 0" would be worse than admitting the ambiguity. `overlay.py`/`radar.py`'s
shared `cv.palette` colour scheme relies on this: `team_id` 0 always draws red,
`team_id` 1 always draws blue.

`assign_teams` is the pure-transform wrapper (mirroring `features/mutations.py`'s
stateless transform-over-a-frame convention) that turns per-track crops into a team-id
column on the tracks frame -- it fits one `TeamClassifier` per session and assigns every
track's predicted cluster, never per-frame (a track's team assignment does not flicker
frame to frame).

`extract_track_crops` builds `assign_teams`'s `crops_by_track` input from a tracks
frame and the session's clips: torso-region crops (upper half of the box's height,
inner 60% of its width) by default, not the full detection box. Added in the
02.1-12/02.1-14 gap-fix iteration after an 11-clip experiment (5 human-reviewed + 6
statistically worst clips of the 61-clip pilot) found the full-body-crop fit conflated
jersey colour with background/field colour bleeding in at the box edges and legs --
torso-only crops fixed all 8 of the human's team-assignment corrections with under 10%
churn among the other, already-correct tracks (the stability bound the gap-fix
iteration's decision rule required), where full-body crops (the plan 02.1-12 baseline)
missed most of them. See the experiment report for the full comparison, including the
colour-histogram alternative that was tried and rejected (worse on both fixed-count
and stability).

Implemented by plan 02.1-06 (together with `cv.registry`); `extract_track_crops` added
in the 02.1-12/02.1-14 gap-fix iteration.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sklearn.cluster import KMeans

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    import numpy as np
    import polars as pl

    from flag_football_ep.config import Config

SIGLIP_MODEL_PATH = "google/siglip-base-patch16-224"

# UMAP on a handful of points produces meaningless projections and would silently
# poison every downstream team assignment -- `fit` refuses below this count.
_MIN_FIT_CROPS = 20

# Batch size for the SigLIP embedding pass, matching the roboflow/sports reference.
_DEFAULT_BATCH_SIZE = 32

# `assign_teams`'s per-track fit-sample cap -- keeps one long track from dominating the
# session-wide fit. A module constant (mirroring `_MIN_FIT_CROPS`'s own convention)
# rather than a function parameter: `assign_teams`'s three-parameter signature
# (`tracks`, `config`, `crops_by_track`) is a fixed contract (`tests/test_cv_contracts.py`),
# so a "configurable number per track" (Task 2 action text) has to live here instead.
_MAX_FIT_CROPS_PER_TRACK = 20

# A track's majority team-cluster share below this threshold is a coin flip, not a
# real signal -- `assign_teams` leaves `team_id` null and emits a notice rather than
# guessing (Task 2 action text).
_MIN_MAJORITY_SHARE = 0.6

# `extract_track_crops`'s default per-track sample size -- validated by the
# 02.1-12/02.1-14 gap-fix iteration's Experiment 2 (fixed 8/8 human team-assignment
# corrections with 8.74% churn among the rest, at this exact sample size). A module
# constant, mirroring `_MAX_FIT_CROPS_PER_TRACK`'s own convention, but still overridable
# per call since `extract_track_crops` is not a fixed-signature contract function.
_DEFAULT_CROPS_PER_TRACK = 6

# Torso-region crop geometry (gap-fix iteration finding): the inner 60% of the box's
# width, upper 50% of its height -- excludes the legs (where grass/field colour bleeds
# in at the box edges during a stride) and the very top/side edges (background bleed
# from imperfect detector boxes), keeping only jersey-coloured pixels.
_TORSO_WIDTH_FRACTION = 0.6
_TORSO_HEIGHT_FRACTION = 0.5

# `_anchor_cluster_labels`'s ambiguity threshold: when the two clusters' median
# `_redness_score`s are within this margin of each other, neither kit reads as
# meaningfully more red than the other (e.g. two dark/neutral kits) -- the KMeans
# label order is kept as-is rather than anchoring on a coin flip.
_AMBIGUOUS_REDNESS_MARGIN = 0.05

__all__ = [
    "ClassifierNotFitted",
    "InsufficientCrops",
    "TeamAssignmentResult",
    "TeamClassifier",
    "assign_teams",
    "extract_track_crops",
]


class InsufficientCrops(CvError):
    """Raised by `TeamClassifier.fit` when fewer than `_MIN_FIT_CROPS` crops are
    supplied -- naming the count and the minimum.
    """


class ClassifierNotFitted(CvError):
    """Raised by `TeamClassifier.predict` when called before `fit`."""


class TeamClassifier:
    """SigLIP + UMAP + KMeans two-cluster team classifier for tracked player crops.

    Two deviations from the `github.com/roboflow/sports` reference implementation:

    1. Dependency injection for testability: when `embedder` is `None` (the production
       path), the SigLIP model and processor are constructed lazily on first use
       (function-local `import transformers`, never at module level) and crop
       embeddings are extracted by feeding batches through them. When `embedder` is
       provided, it is a callable `crops -> np.ndarray` of shape `(n, d)` and no SigLIP
       weights are ever downloaded -- this is what lets the test suite run offline.
    2. Determinism and sample-size guards: `seed` is passed to both `UMAP` and `KMeans`
       (`random_state=seed`, `KMeans(..., n_init=10)`), and `fit` refuses fewer than
       `_MIN_FIT_CROPS` crops.
    """

    def __init__(
        self,
        device: str = "cpu",
        embedder: Callable[[Sequence], np.ndarray] | None = None,
        seed: int = 20260516,
    ) -> None:
        # Function-local import: umap-learn is a `cv`-extras dependency (D-07/D-08);
        # constructing the UMAP object itself is cheap (no weights, no network) so it
        # happens eagerly here, unlike the SigLIP model/processor below.
        import umap

        self._device = device
        self._embedder = embedder
        self._seed = seed
        self._siglip_model = None
        self._siglip_processor = None
        self._reducer = umap.UMAP(n_components=3, random_state=seed)
        self._cluster_model = KMeans(n_clusters=2, random_state=seed, n_init=10)
        self._fitted = False

    def _ensure_siglip(self) -> None:
        if self._siglip_model is not None:
            return
        # Function-local import: transformers is a `cv`-extras dependency, never
        # imported at module level (D-07/D-08).
        #
        # `SiglipImageProcessor` directly, not `AutoProcessor`: verified against the
        # real `google/siglip-base-patch16-224` checkpoint (plan 02.1-12 Task 3's real
        # 61-clip run) that `AutoProcessor.from_pretrained` unconditionally tries to
        # also resolve `SiglipTokenizer` (the paired text processor) even though this
        # class only ever needs the image side -- that raises `ImportError: ...
        # requires the SentencePiece library` on an environment (this project's own)
        # that never installed `sentencepiece`, since nothing here does text encoding.
        # `SiglipImageProcessor` loads only the vision preprocessing config, matching
        # what `SiglipVisionModel` actually consumes, with no tokenizer/SentencePiece
        # dependency at all.
        from transformers import SiglipImageProcessor, SiglipVisionModel

        self._siglip_model = SiglipVisionModel.from_pretrained(SIGLIP_MODEL_PATH).to(
            self._device
        )
        self._siglip_processor = SiglipImageProcessor.from_pretrained(SIGLIP_MODEL_PATH)

    def _extract_siglip_features(self, crops) -> np.ndarray:
        import numpy as np
        import torch

        self._ensure_siglip()
        batches: list[np.ndarray] = []
        for start in range(0, len(crops), _DEFAULT_BATCH_SIZE):
            batch = crops[start : start + _DEFAULT_BATCH_SIZE]
            inputs = self._siglip_processor(images=list(batch), return_tensors="pt").to(
                self._device
            )
            with torch.no_grad():
                outputs = self._siglip_model(**inputs)
            # Mean-pool the last hidden state over the patch/token dimension, matching
            # the roboflow/sports reference's `extract_features`.
            pooled = outputs.last_hidden_state.mean(dim=1)
            batches.append(pooled.cpu().numpy())
        return np.concatenate(batches, axis=0)

    def extract_features(self, crops) -> np.ndarray:
        """Return an `(n, d)` embedding array for `crops`: the injected `embedder` when
        one was provided, otherwise batched SigLIP embeddings (mean-pooled last hidden
        state), constructing the SigLIP model/processor lazily on first use.
        """
        if self._embedder is not None:
            return self._embedder(crops)
        return self._extract_siglip_features(crops)

    def fit(self, crops) -> None:
        """Learn the two-cluster embedding space from `crops`.

        Raises `InsufficientCrops` (naming the count and the minimum) when fewer than
        `_MIN_FIT_CROPS` crops are supplied -- UMAP on a handful of points produces
        meaningless projections that would silently poison every team assignment
        downstream.
        """
        n = len(crops)
        if n < _MIN_FIT_CROPS:
            raise InsufficientCrops(
                f"TeamClassifier.fit received {n} crops, fewer than the minimum of "
                f"{_MIN_FIT_CROPS} required for a meaningful UMAP projection"
            )
        features = self.extract_features(crops)
        projections = self._reducer.fit_transform(features)
        self._cluster_model.fit(projections)
        self._fitted = True

    def predict(self, crops) -> np.ndarray:
        """Assign cluster ids (0 or 1, arbitrary ordering) to `crops`.

        Raises `ClassifierNotFitted` when called before `fit`.
        """
        if not self._fitted:
            raise ClassifierNotFitted("TeamClassifier.predict called before fit")
        features = self.extract_features(crops)
        projections = self._reducer.transform(features)
        return self._cluster_model.predict(projections)


@dataclass
class TeamAssignmentResult:
    """`assign_teams`'s output: `tracks` with `team_id` filled (already passed through
    `schema.conform_tracking`) plus `notices` -- one per track whose majority
    team-cluster share fell below `_MIN_MAJORITY_SHARE` (naming the track id), since a
    coin-flip assignment is worse than an admitted gap.
    """

    tracks: pl.DataFrame
    notices: list[str] = field(default_factory=list)


def assign_teams(
    tracks: pl.DataFrame, config: Config, *, crops_by_track: Mapping[tuple[int, int], Sequence]
) -> TeamAssignmentResult:
    """Fit ONE `TeamClassifier` for the whole session and add a `team_id` column to
    `tracks`, one assignment per TRACK (never per frame -- a track's team assignment
    does not flicker frame to frame).

    `crops_by_track` is keyed by `(clip_number, track_id)`: `track_id` alone is only
    unique *within* a clip (D-02, `cv.track.track_session`'s per-clip tracker
    instances), so the composite key is what makes a track globally addressable across
    the whole session.

    Fitting once per session (not per clip) is required because `sklearn.KMeans`
    cluster labels are arbitrary per fit -- a per-clip fit would produce team ids that
    mean different things in different clips and would silently corrupt any later
    possession or formation analysis. The fit sample is drawn evenly across every
    non-referee track, capped at `_MAX_FIT_CROPS_PER_TRACK` crops per track so one long
    track cannot dominate it, in a fixed (sorted-by-key) order so the fit is
    deterministic regardless of `crops_by_track`'s own iteration order.

    Every track's `team_id` is the majority cluster over that track's own predicted
    crops. A track whose majority share is below `_MIN_MAJORITY_SHARE` gets `team_id`
    null plus a notice naming its track id -- a coin-flip assignment is worse than an
    admitted gap. `class_name == "referee"` rows always get `team_id` null and referee
    crops never reach the fit or the per-track prediction -- officials belong to no
    team and their crops would pull the clusters.

    Before any per-track majority vote, `_anchor_cluster_labels` remaps
    `TeamClassifier`'s own arbitrary cluster label (0/1) to a fixed real-world meaning:
    `team_id` 0 is always the cluster whose fit crops read as more red, `team_id` 1 is
    the other. This mapping is computed once per session fit and applied to every
    track's prediction, so `team_id`'s meaning (which colour it draws) is stable
    session to session, not just track to track within one session. When the fit
    crops' two clusters are ambiguously coloured (neither reads as meaningfully more
    red), the KMeans label order is kept as-is and a notice is appended naming the
    ambiguity.
    """
    import polars as pl

    from flag_football_ep.cv.schema import conform_tracking

    track_class_by_key: dict[tuple[int, int], str] = {
        (int(row["clip_number"]), int(row["track_id"])): row["class_name"]
        for row in tracks.group_by(["clip_number", "track_id"])
        .agg(pl.col("class_name").first())
        .iter_rows(named=True)
    }

    def _is_referee(key: tuple[int, int]) -> bool:
        return track_class_by_key.get(key) == "referee"

    # Deterministic key order: the fit result must not depend on `crops_by_track`'s
    # own iteration order.
    sorted_keys = sorted(crops_by_track)
    player_keys = [key for key in sorted_keys if key in track_class_by_key and not _is_referee(key)]

    fit_crops: list = []
    for key in player_keys:
        crops = list(crops_by_track[key])[:_MAX_FIT_CROPS_PER_TRACK]
        fit_crops.extend(crops)

    classifier = TeamClassifier(device=config.cv.device)
    classifier.fit(fit_crops)

    notices: list[str] = []

    cluster_to_team_id, ambiguous_colors = _anchor_cluster_labels(classifier, fit_crops)
    if ambiguous_colors:
        notices.append(
            "jersey-colour anchoring: neither cluster's sampled crops read as "
            "meaningfully more red than the other (ambiguous kit colours) -- keeping "
            "the arbitrary KMeans cluster order for team_id"
        )

    team_id_by_key: dict[tuple[int, int], int | None] = {}

    for key in player_keys:
        clip_number, track_id = key
        crops = list(crops_by_track[key])
        if not crops:
            team_id_by_key[key] = None
            continue

        predicted = classifier.predict(crops)
        mapped = [cluster_to_team_id[int(label)] for label in predicted]
        counts = Counter(mapped)
        majority_label, majority_count = counts.most_common(1)[0]
        share = majority_count / len(predicted)

        if share < _MIN_MAJORITY_SHARE:
            notices.append(
                f"track {track_id} (clip {clip_number}): majority team-cluster share "
                f"{share:.2f} is below the {_MIN_MAJORITY_SHARE} threshold -- team_id "
                "left null"
            )
            team_id_by_key[key] = None
        else:
            team_id_by_key[key] = majority_label

    assignment_schema = {"clip_number": pl.Int32, "track_id": pl.Int32, "team_id": pl.Int32}
    if team_id_by_key:
        assignment_df = pl.DataFrame(
            [
                {"clip_number": clip_number, "track_id": track_id, "team_id": team_id}
                for (clip_number, track_id), team_id in team_id_by_key.items()
            ],
            schema=assignment_schema,
        )
    else:
        assignment_df = pl.DataFrame(schema=assignment_schema)

    base = tracks.drop("team_id") if "team_id" in tracks.columns else tracks
    base = base.with_columns(
        [pl.col("clip_number").cast(pl.Int32), pl.col("track_id").cast(pl.Int32)]
    )
    joined = base.join(assignment_df, on=["clip_number", "track_id"], how="left")

    # Belt and suspenders: even if `crops_by_track` somehow carried a referee key,
    # every referee row's team_id is forced null here.
    joined = joined.with_columns(
        pl.when(pl.col("class_name") == "referee")
        .then(pl.lit(None).cast(pl.Int32))
        .otherwise(pl.col("team_id"))
        .alias("team_id")
    )

    conformed = conform_tracking(joined)
    return TeamAssignmentResult(tracks=conformed, notices=notices)


def _crop_median_rgb(crop) -> np.ndarray:
    """Median R, G, B (0-255) over `crop`'s pixels: robust to a handful of
    background/edge-bleed pixels within a single torso crop, unlike a plain mean.

    `crop` is treated as an `(..., channels)` array; only the first three channels are
    used, so this also tolerates a 4-channel (RGBA) crop or a 1-D feature vector of
    length >= 3 (the latter path only matters for tests that feed `TeamClassifier` a
    synthetic feature vector directly instead of a real image crop).
    """
    import numpy as np

    array = np.asarray(crop, dtype=float)
    pixels = array.reshape(-1, array.shape[-1])[:, :3]
    return np.median(pixels, axis=0)


def _redness_score(rgb: np.ndarray) -> float:
    """A saturation-weighted closeness-to-red-hue score in `[0, 1]`, computed from a
    single median `(R, G, B)` triplet (0-255 range).

    Converts to HSV (`colorsys.rgb_to_hsv`) and scores how close the hue is to red
    (hue 0/1, wrapping -- `min(h, 1 - h)` is the distance to the nearer of the two),
    scaled so `0.0` at the red hue itself and `0.0` at the maximum possible distance
    (hue 0.5, cyan), then weighted by saturation so a low-saturation (grey/white/black)
    jersey scores near zero regardless of a stray reddish hue reading on a nearly
    colourless crop. `0.0` = not red at all, `1.0` = fully saturated pure red.
    """
    import colorsys

    r, g, b = (float(channel) / 255.0 for channel in rgb[:3])
    hue, saturation, _value = colorsys.rgb_to_hsv(r, g, b)
    hue_distance = min(hue, 1.0 - hue)  # 0.0 at red, 0.5 at cyan
    closeness = max(0.0, 1.0 - hue_distance / 0.5)
    return saturation * closeness


def _anchor_cluster_labels(
    classifier: TeamClassifier, fit_crops: list
) -> tuple[dict[int, int], bool]:
    """Decide which of `TeamClassifier`'s own arbitrary cluster labels (0 or 1) is the
    redder-jersey cluster, and return the `{cluster_label: team_id}` remap that anchors
    `team_id` to jersey colour: `team_id` 0 is always the redder cluster, `team_id` 1
    is the other (this module's docstring's contract).

    Re-predicts `fit_crops` (already used to fit `classifier`, so this is deterministic
    given the same classifier state) to get each fit crop's own cluster label, computes
    each crop's `_redness_score` from its `_crop_median_rgb`, then takes the *median*
    redness score across each cluster's own fit crops -- robust to a handful of outlier
    crops (odd lighting, a stray non-jersey pixel patch) within one cluster.

    Returns `({0: 0, 1: 1}, True)` (the arbitrary KMeans order, unchanged) when the two
    clusters' median redness scores are within `_AMBIGUOUS_REDNESS_MARGIN` of each
    other -- neither kit reads as meaningfully more red than the other (e.g. two
    dark/neutral kits) -- so the caller can emit a notice rather than silently guessing
    which grey kit is "team 0". Otherwise returns the anchoring remap and `False`.
    """
    import numpy as np

    cluster_labels = classifier.predict(fit_crops)
    scores_by_cluster: dict[int, list[float]] = {0: [], 1: []}
    for crop, label in zip(fit_crops, cluster_labels, strict=True):
        rgb = _crop_median_rgb(crop)
        scores_by_cluster[int(label)].append(_redness_score(rgb))

    median_0 = float(np.median(scores_by_cluster[0])) if scores_by_cluster[0] else 0.0
    median_1 = float(np.median(scores_by_cluster[1])) if scores_by_cluster[1] else 0.0

    if abs(median_0 - median_1) < _AMBIGUOUS_REDNESS_MARGIN:
        return {0: 0, 1: 1}, True

    red_cluster = 0 if median_0 > median_1 else 1
    other_cluster = 1 - red_cluster
    return {red_cluster: 0, other_cluster: 1}, False


def _sample_frame_indices(n_rows: int, max_crops: int) -> list[int]:
    """Up to `max_crops` row-positions (0-indexed into a frame-sorted group), spread
    evenly across `n_rows` -- the first and last available frame always included when
    `n_rows > 1`, so the sample spans the whole track's lifetime rather than clustering
    near one moment of the play.
    """
    if n_rows <= max_crops:
        return list(range(n_rows))
    if max_crops <= 1:
        return [0]
    step = (n_rows - 1) / (max_crops - 1)
    return sorted({round(i * step) for i in range(max_crops)})


def _crop_row(frame_rgb: np.ndarray, row: Mapping, *, torso: bool) -> np.ndarray | None:
    """Crop `row`'s bounding box out of `frame_rgb`, restricted to the torso region
    (`_TORSO_WIDTH_FRACTION`/`_TORSO_HEIGHT_FRACTION`) when `torso=True`, else the full
    box. Returns `None` for a box that clips to nothing after rounding/clamping to the
    frame bounds (a box already at the frame edge) -- the caller drops it rather than
    appending an empty array to a track's crop list.
    """
    x1, y1, x2, y2 = row["bbox_x1"], row["bbox_y1"], row["bbox_x2"], row["bbox_y2"]
    if torso:
        w = x2 - x1
        h = y2 - y1
        margin = (1.0 - _TORSO_WIDTH_FRACTION) / 2.0
        x1, x2 = x1 + margin * w, x2 - margin * w
        y2 = y1 + _TORSO_HEIGHT_FRACTION * h

    height, width = frame_rgb.shape[:2]
    ix1 = max(int(round(x1)), 0)
    iy1 = max(int(round(y1)), 0)
    ix2 = min(int(round(x2)), width)
    iy2 = min(int(round(y2)), height)
    if ix2 <= ix1 or iy2 <= iy1:
        return None
    return frame_rgb[iy1:iy2, ix1:ix2].copy()


def extract_track_crops(
    config: Config,
    session_id: str,
    tracks: pl.DataFrame,
    *,
    max_crops_per_track: int = _DEFAULT_CROPS_PER_TRACK,
    torso: bool = True,
) -> dict[tuple[int, int], list[np.ndarray]]:
    """Build `assign_teams`'s `crops_by_track` input: for every `(clip_number,
    track_id)` present in `tracks`, decode that clip once and pull up to
    `max_crops_per_track` crops spread evenly across the track's own lifetime
    (`_sample_frame_indices`).

    `torso=True` (the default, gap-fix iteration finding) crops the inner
    `_TORSO_WIDTH_FRACTION` of the box's width and the upper `_TORSO_HEIGHT_FRACTION`
    of its height, instead of the full detection box -- see the module docstring for
    why. Every row's class (`player` or `referee`) is included; `assign_teams` itself
    is what excludes referees from the fit and from prediction, not this function --
    `extract_track_crops` only answers "what does this track look like," not "should
    this track's crops influence team assignment."

    Raises `MissingClipError` (from `cv.detect`, naming the path) when a clip referenced
    by `tracks` cannot be opened for decoding -- a silently empty crop list for a track
    would look like an ambiguous-team notice downstream, not a missing-file bug.
    """
    import cv2
    import numpy as np
    import polars as pl

    from flag_football_ep.cv import frames
    from flag_football_ep.cv.detect import MissingClipError

    clip_paths = {
        frames.clip_number(path): path for path in frames.clip_paths(config, session_id)
    }
    crops_by_track: dict[tuple[int, int], list[np.ndarray]] = {}

    clip_numbers = sorted(tracks["clip_number"].unique().to_list())
    for clip_number in clip_numbers:
        clip_rows = tracks.filter(pl.col("clip_number") == clip_number)
        if clip_rows.height == 0:
            continue
        clip_path = clip_paths.get(int(clip_number))
        if clip_path is None:
            continue  # clip not registered for this session -- nothing to decode

        frame_to_rows: dict[int, list[Mapping]] = {}
        for (track_id,), group in clip_rows.group_by(["track_id"]):
            ordered = group.sort("frame_index")
            for i in _sample_frame_indices(ordered.height, max_crops_per_track):
                row = ordered.row(i, named=True)
                frame_to_rows.setdefault(int(row["frame_index"]), []).append(row)
        if not frame_to_rows:
            continue

        capture = cv2.VideoCapture(str(clip_path))
        if not capture.isOpened():
            capture.release()
            raise MissingClipError(f"could not open clip for crop extraction: {clip_path}")

        remaining = dict(frame_to_rows)
        frame_index = 0
        try:
            while remaining:
                read_ok, frame_bgr = capture.read()
                if not read_ok:
                    break
                if frame_index in remaining:
                    frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
                    for row in remaining.pop(frame_index):
                        crop = _crop_row(frame_rgb, row, torso=torso)
                        if crop is not None:
                            key = (int(clip_number), int(row["track_id"]))
                            crops_by_track.setdefault(key, []).append(crop)
                frame_index += 1
        finally:
            capture.release()

    return crops_by_track
