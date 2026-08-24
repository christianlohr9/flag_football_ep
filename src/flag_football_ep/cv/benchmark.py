"""Inference-runtime benchmark: the C-09 "<1h/game inference" gate metric.

Owns extrapolation from a measured sample (a handful of clips' actual stage timings,
`StageTiming`) to a full-game runtime estimate (`extrapolate_game_runtime`), with the
extrapolation formula documented on the returned `BenchmarkResult` itself rather than
asserted bare -- matching the "Richtwert, kein Messprotokoll" statistical-honesty
framing already established for this project's gate documentation
(`docs/capture-protocol.md`).

Implemented by plan 02.1-11, alongside `detect.load_detector`/`detect_video`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageTiming:
    """One measured pipeline stage's timing: stage name, elapsed seconds, and the
    number of frames that timing covers.
    """

    stage: str
    seconds: float
    frames: int


@dataclass(frozen=True)
class BenchmarkResult:
    """The full-game runtime extrapolation: per-stage timings, the footage duration
    the sample covered, the extrapolated full-game minutes, and the formula string used
    to produce that extrapolation.
    """

    stages: tuple[StageTiming, ...]
    footage_seconds: float
    extrapolated_game_minutes: float
    formula: str


def extrapolate_game_runtime(
    stages: tuple[StageTiming, ...], footage_seconds: float, game_seconds: float
) -> BenchmarkResult:
    """Extrapolate `stages`' measured per-stage timings (covering `footage_seconds` of
    sample footage) to a full `game_seconds` game, returning the documented formula
    alongside the extrapolated minutes.
    """
    raise NotImplementedError(
        "cv.benchmark.extrapolate_game_runtime is implemented by plan 02.1-11"
    )
