"""Package a frozen detector + frozen eval split into a dev/test/transfer deliverable.

Sibling of `cv.dataset`'s content-hashing and `cv.frames`'s atomic-manifest-write
conventions applied to a new artifact kind: a self-contained, checksummed archive a
hackathon participant can pull without touching the DVC-tracked training set. Depends
on `cv.freeze.resolve_frozen`/the freeze-pin file -- never `cv.registry.resolve_champion`
directly, so a bundle always names the frozen (not the rolling-champion) detector.

Every function below raises `NotImplementedError` naming the plan that implements it
-- this module is a contract freeze only (plan 02.2-05); the real archive-build,
manifest, and delivery logic is implemented by plan 02.2-10.

Implemented by plan 02.2-10.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from pathlib import Path

    from flag_football_ep.config import Config
    from flag_football_ep.cv.freeze import FreezePin

# The three deliverable kinds this phase produces (D-09): a small development sample,
# a held-out evaluation subset, and a transfer-only subset for domains ratified as
# transfer-only under D-11 (e.g. broadcast/TV if it fails the per-domain mAP ablation).
BUNDLE_KINDS: tuple[str, ...] = ("dev", "test", "transfer")


class BundleError(CvError, RuntimeError):
    """Raised when a bundle cannot be assembled: a missing frozen-detector pin, a
    missing eval-clip manifest, an unknown `kind`, or an archive write failure.
    """


@dataclass(frozen=True)
class BundleResult:
    """The output of a `build_bundle` run: where the archive and its manifest were
    written, the archive's reproducible content hash, and how many files it contains.
    """

    archive_path: Path
    manifest_path: Path
    content_sha256: str
    n_files: int


def build_bundle(config: Config, kind: str, pin: FreezePin, out_dir: Path) -> BundleResult:
    """Assemble a `kind` (`BUNDLE_KINDS`) deliverable archive for the detector pinned
    by `pin`, written under `out_dir`.

    Implemented by plan 02.2-10.
    """
    raise NotImplementedError("implemented by plan 02.2-10")


def bundle_manifest(root: Path) -> dict:
    """Read back the manifest of a bundle previously extracted/built at `root`.

    Implemented by plan 02.2-10.
    """
    raise NotImplementedError("implemented by plan 02.2-10")


def deliver_bundle(config: Config, archive: Path, remote: str) -> str:
    """Upload `archive` to `remote` (an OTC OBS URI), returning the remote URI it was
    written to. Never echoes a credential value (T-2.2-13).

    Implemented by plan 02.2-10.
    """
    raise NotImplementedError("implemented by plan 02.2-10")
