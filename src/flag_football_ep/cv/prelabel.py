"""Zero-shot pre-labeling of sampled training frames via Grounding DINO.

Owns the bootstrap-labeling step between `frames.sample_training_frames` and the
CVAT round trip (`dataset.create_cvat_task`): runs the zero-shot open-vocabulary
detector (`transformers.GroundingDinoForObjectDetection`, `IDEA-Research/grounding-dino-tiny`
-- the binding backend decision recorded in `docs/cv-setup.md` and plan 02.1-01-SUMMARY.md;
`autodistill`/`autodistill-grounding-dino` are not used, their `CaptionOntology` is
unimportable due to an undeclared `roboflow` dependency) over every sampled frame in
`frames_dir` and writes a COCO-format pre-annotation package to `out_dir`, one box per
detected player/referee. These pre-annotations are a labeling *head start*, not ground
truth -- every one of them is reviewed and corrected in CVAT before `dataset.validate_coco`
ever sees the export.

`--force`/`force=False` mirrors `fetch.sportapp`'s cache-bypass convention: a frame
already pre-labeled on disk is skipped unless `force=True`.

Implemented by plan 02.1-07.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class PrelabelBackendUnavailable(CvError, RuntimeError):
    """Raised when the zero-shot pre-labeling backend cannot be loaded (e.g. the `cv`
    extras group is not installed, or the Grounding DINO weights cannot be fetched).
    """


@dataclass
class PrelabelResult:
    """The pre-labeling run's output: the COCO package path, and counts (frames
    processed, boxes produced) plus any per-frame notices (e.g. zero detections).
    """

    coco_path: Path
    n_frames: int
    n_boxes: int
    notices: list[str] = field(default_factory=list)


def prelabel_frames(
    config: Config, frames_dir: Path, out_dir: Path, *, force: bool = False
) -> PrelabelResult:
    """Run zero-shot pre-labeling over every frame in `frames_dir`, writing a COCO
    package to `out_dir`. Frames already pre-labeled on disk are skipped unless `force`.
    """
    raise NotImplementedError("cv.prelabel.prelabel_frames is implemented by plan 02.1-07")
