"""Coverage for `flag_football_ep.cv.teams.TeamClassifier`: deterministic two-cluster
splitting from an injected embedder, sample-size guards, and offline-only imports.

Every test injects a synthetic `embedder` (an identity function over pre-computed
feature vectors) so no SigLIP weights are ever downloaded and no network round trip
happens -- per RESEARCH's Validation Architecture rule ("no CV test should require
downloading model weights or real footage").
"""

from __future__ import annotations

import numpy as np
import pytest

from flag_football_ep.cv.teams import ClassifierNotFitted, InsufficientCrops, TeamClassifier


def _identity_embedder(crops) -> np.ndarray:
    """The injected `embedder`: `crops` here are already feature vectors, so this is a
    pure pass-through -- no SigLIP model, no network.
    """
    return np.asarray(crops, dtype=float)


def _two_gaussian_blobs(
    n_per_blob: int = 40, dim: int = 8, seed: int = 1
) -> tuple[np.ndarray, np.ndarray]:
    """Two well-separated Gaussian blobs in `dim`-dimensional space.

    Returns `(vectors, blob_id)` where `blob_id[i]` is 0 or 1 indicating which blob
    `vectors[i]` was drawn from (the ground-truth partition, not a predicted label).
    """
    rng = np.random.default_rng(seed)
    mean_a = np.zeros(dim)
    mean_b = np.full(dim, 20.0)
    blob_a = rng.normal(loc=mean_a, scale=0.5, size=(n_per_blob, dim))
    blob_b = rng.normal(loc=mean_b, scale=0.5, size=(n_per_blob, dim))
    vectors = np.concatenate([blob_a, blob_b], axis=0)
    blob_id = np.concatenate([np.zeros(n_per_blob), np.ones(n_per_blob)])
    return vectors, blob_id


def _as_partition(labels: np.ndarray) -> frozenset[frozenset[int]]:
    """Convert a label array into a set-partition of indices, so cluster *label value*
    (0 vs 1, arbitrary per TeamClassifier's own contract) never matters -- only which
    indices ended up grouped together does.
    """
    groups: dict = {}
    for i, label in enumerate(labels):
        groups.setdefault(int(label), set()).add(i)
    return frozenset(frozenset(group) for group in groups.values())


# --- fit/predict on well-separated blobs ----------------------------------------------------


def test_fit_splits_two_well_separated_blobs_into_correct_partition() -> None:
    vectors, blob_id = _two_gaussian_blobs()
    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)

    classifier.fit(vectors)
    predicted = classifier.predict(vectors)

    assert _as_partition(predicted) == _as_partition(blob_id)


def test_fit_is_deterministic_for_the_same_seed() -> None:
    vectors, _blob_id = _two_gaussian_blobs()

    first = TeamClassifier(embedder=_identity_embedder, seed=20260516)
    first.fit(vectors)
    first_labels = first.predict(vectors)

    second = TeamClassifier(embedder=_identity_embedder, seed=20260516)
    second.fit(vectors)
    second_labels = second.predict(vectors)

    assert _as_partition(first_labels) == _as_partition(second_labels)


def test_predict_on_unseen_vectors_from_same_blobs_assigns_matching_clusters() -> None:
    fit_vectors, fit_blob_id = _two_gaussian_blobs(seed=1)
    unseen_vectors, unseen_blob_id = _two_gaussian_blobs(seed=2)

    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)
    classifier.fit(fit_vectors)

    fit_labels = classifier.predict(fit_vectors)
    unseen_labels = classifier.predict(unseen_vectors)

    # Map each ground-truth blob id to the cluster label the classifier assigned to it
    # on the *fit* data, then check the unseen predictions follow the same mapping --
    # this avoids assuming cluster label 0/1 is stable, only that the partition is.
    label_for_blob = {}
    for blob, label in zip(fit_blob_id, fit_labels, strict=True):
        label_for_blob.setdefault(int(blob), int(label))

    expected = np.array([label_for_blob[int(b)] for b in unseen_blob_id])
    assert np.array_equal(unseen_labels, expected)


# --- sample-size and fit-order guards --------------------------------------------------------


def test_fit_with_too_few_crops_raises_insufficient_crops_naming_count_and_minimum() -> None:
    too_few = np.zeros((19, 4))
    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)

    with pytest.raises(InsufficientCrops) as exc_info:
        classifier.fit(too_few)

    message = str(exc_info.value)
    assert "19" in message
    assert "20" in message


def test_predict_before_fit_raises_classifier_not_fitted() -> None:
    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)

    with pytest.raises(ClassifierNotFitted):
        classifier.predict(np.zeros((5, 4)))


# --- import hygiene: no network-heavy dependency at module level ----------------------------


def test_import_teams_module_pulls_in_neither_transformers_nor_torch() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, flag_football_ep.cv.teams as t; "
            "assert 'transformers' not in sys.modules; "
            "assert 'torch' not in sys.modules; "
            "print('ok')",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
