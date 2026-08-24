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

from typing import TYPE_CHECKING

from sklearn.cluster import KMeans

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import numpy as np
    import polars as pl

    from flag_football_ep.config import Config

SIGLIP_MODEL_PATH = "google/siglip-base-patch16-224"

# UMAP on a handful of points produces meaningless projections and would silently
# poison every downstream team assignment -- `fit` refuses below this count.
_MIN_FIT_CROPS = 20

# Batch size for the SigLIP embedding pass, matching the roboflow/sports reference.
_DEFAULT_BATCH_SIZE = 32

__all__ = [
    "ClassifierNotFitted",
    "InsufficientCrops",
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


def assign_teams(tracks: pl.DataFrame, config: Config, *, crops_by_track) -> pl.DataFrame:
    """Fit one `TeamClassifier` on `crops_by_track` and add a team-id column to
    `tracks`, one assignment per track (not per frame).
    """
    raise NotImplementedError("cv.teams.assign_teams is implemented by plan 02.1-12")
