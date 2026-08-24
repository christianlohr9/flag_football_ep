"""Unsupervised team-color/appearance classification of tracked player crops.

`TeamClassifier` is the SigLIP-embedding + UMAP-reduction + KMeans-clustering
pipeline (`github.com/roboflow/sports`, MIT) that splits tracked player crops into two
visual clusters without any hand-labeled team assignment: `fit` learns the two-cluster
embedding space from a batch of crops, `predict` assigns cluster ids to new crops.
`scikit-learn` (`KMeans`) is already a core project dependency; only the SigLIP
(`transformers`) and `umap-learn` pieces are new, both gated behind the `cv` extras
group (D-07).

`assign_teams` is the pure-transform wrapper (mirroring `features/mutations.py`'s
stateless transform-over-a-frame convention) that turns per-track crops into a team-id
column on the tracks frame -- it fits one `TeamClassifier` per session and assigns every
track's predicted cluster, never per-frame (a track's team assignment does not flicker
frame to frame).

Implemented by plan 02.1-06 (together with `cv.registry`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np
    import polars as pl

    from flag_football_ep.config import Config


class TeamClassifier:
    """SigLIP + UMAP + KMeans two-cluster team classifier for tracked player crops."""

    def __init__(self, device: str = "cpu", embedder=None, seed: int = 20260516) -> None:
        raise NotImplementedError("cv.teams.TeamClassifier.__init__ is implemented by plan 02.1-06")

    def fit(self, crops) -> None:
        raise NotImplementedError("cv.teams.TeamClassifier.fit is implemented by plan 02.1-06")

    def predict(self, crops) -> np.ndarray:
        raise NotImplementedError("cv.teams.TeamClassifier.predict is implemented by plan 02.1-06")


def assign_teams(tracks: pl.DataFrame, config: Config, *, crops_by_track) -> pl.DataFrame:
    """Fit one `TeamClassifier` on `crops_by_track` and add a team-id column to
    `tracks`, one assignment per track (not per frame).
    """
    raise NotImplementedError("cv.teams.assign_teams is implemented by plan 02.1-06")
