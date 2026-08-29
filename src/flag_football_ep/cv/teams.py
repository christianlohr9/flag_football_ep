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

Cluster label 0/1 is arbitrary: which cluster is which real-world team is decided by
plan 02.1-12 when it joins the labels back to tracks, and must never be assumed stable
across clips fitted separately (a `TeamClassifier` fitted on one clip's crops has no
relationship to the label ordering of a `TeamClassifier` fitted on another clip).

`assign_teams` is the pure-transform wrapper (mirroring `features/mutations.py`'s
stateless transform-over-a-frame convention) that turns per-track crops into a team-id
column on the tracks frame -- it fits one `TeamClassifier` per session and assigns every
track's predicted cluster, never per-frame (a track's team assignment does not flicker
frame to frame).

Implemented by plan 02.1-06 (together with `cv.registry`).
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

__all__ = [
    "ClassifierNotFitted",
    "InsufficientCrops",
    "TeamAssignmentResult",
    "TeamClassifier",
    "assign_teams",
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
        from transformers import AutoProcessor, SiglipVisionModel

        self._siglip_model = SiglipVisionModel.from_pretrained(SIGLIP_MODEL_PATH).to(
            self._device
        )
        self._siglip_processor = AutoProcessor.from_pretrained(SIGLIP_MODEL_PATH)

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

    Cluster label 0/1 is arbitrary: which integer means which real-world team is read
    off the radar reel by a human (plan 02.1-16), and nothing downstream may assume a
    fixed mapping -- not even across two different `assign_teams` calls.
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
    team_id_by_key: dict[tuple[int, int], int | None] = {}

    for key in player_keys:
        clip_number, track_id = key
        crops = list(crops_by_track[key])
        if not crops:
            team_id_by_key[key] = None
            continue

        predicted = classifier.predict(crops)
        counts = Counter(int(label) for label in predicted)
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
