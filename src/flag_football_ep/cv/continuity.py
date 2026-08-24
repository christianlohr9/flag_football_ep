"""Per-clip track continuity measurement: the C-09 "trackable without ID switch" gate metric.

`measure_continuity` computes, per clip, the fraction of the clip covered by its
longest single track (`longest_track_frac`) and the number of track fragments a player
was split into (`n_fragments`), auto-flagging clips that fall below the pilot gate's
"trackable without ID switch" threshold for human review in `review_csv`.
`summarise_review` rolls the (possibly human-annotated) review CSV up into the
whole-session summary dict the go/no-go gate doc reports -- explicitly over the full
`n=61`-clip denominator, never a cherry-picked subset (D-09).

Implemented by plan 02.1-14, alongside `overlay.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from flag_football_ep.config import Config


@dataclass(frozen=True)
class ContinuityRow:
    """One clip's continuity measurement: track count, the longest track's coverage
    fraction, fragment count, and an auto-assigned review flag.
    """

    clip_number: int
    n_tracks: int
    longest_track_frac: float
    n_fragments: int
    auto_flag: str


@dataclass
class ContinuityResult:
    """The full continuity measurement: every clip's `ContinuityRow`, the review CSV
    path flagged clips were written to, and the whole-session summary dict.
    """

    rows: list[ContinuityRow] = field(default_factory=list)
    review_csv: Path = field(default_factory=Path)
    summary: dict = field(default_factory=dict)


def measure_continuity(
    tracks: pl.DataFrame, config: Config, *, review_csv: Path | None = None
) -> ContinuityResult:
    """Measure per-clip track continuity over `tracks`, writing flagged clips to
    `review_csv` (defaulting to a config-derived path).
    """
    raise NotImplementedError(
        "cv.continuity.measure_continuity is implemented by plan 02.1-14"
    )


def summarise_review(review_csv: Path) -> dict:
    """Roll a (possibly human-annotated) continuity review CSV up into the
    whole-session summary dict reported in the pilot gate doc.
    """
    raise NotImplementedError(
        "cv.continuity.summarise_review is implemented by plan 02.1-14"
    )
