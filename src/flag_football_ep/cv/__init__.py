"""CV tracking pilot subpackage (Phase 2.1, REQ-S2-02).

This package owns everything downstream of raw drone/tripod footage: frame
sampling, zero-shot pre-labeling, CVAT round-tripping, RF-DETR detector
training/inference, OC-SORT tracking, team classification, manual homography
calibration, coordinate projection, tracking-Parquet schema, CSV export,
overlay rendering, continuity/accuracy measurement against the C-09 go/no-go
gate, and the radar/benchmark reporting used to reach that gate decision.

`CvError` is the base of every named exception raised anywhere in this
subpackage -- each `cv/*` module defines its own more specific subclasses
(e.g. `frames.ClipNotFound`, `dataset.DatasetError`) rather than letting a
bare library exception (`FileNotFoundError`, `KeyError`, an MLflow/CVAT SDK
exception) propagate to the CLI, matching the project's existing
no-silent-failure convention (`flag_football_ep.model.registry.RegistryError`,
`flag_football_ep.reference.MissingReferenceFile`).

This module intentionally imports nothing from its own submodules at package
import time: `import flag_football_ep.cv` (and, more importantly,
`import flag_football_ep.cli`) must stay usable in an environment that never
ran `uv sync --extra cv` (D-07/D-08). Every `cv/*` module's third-party CV
dependency (rfdetr, trackers, supervision, sahi, transformers, umap-learn,
opencv-python, torch) is imported lazily, inside function bodies, by the
plans that implement those functions -- never at this package's or any
`cv/*` module's top level.
"""

from __future__ import annotations


class CvError(Exception):
    """Base class for every failure mode raised anywhere in `flag_football_ep.cv`."""
