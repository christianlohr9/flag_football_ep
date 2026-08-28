"""Inference-runtime benchmark: the C-09 "<1h/game inference" gate metric.

Owns extrapolation from a measured sample (a handful of clips' actual stage timings,
`StageTiming`) to a full-game runtime estimate (`extrapolate_game_runtime`), with the
extrapolation formula documented on the returned `BenchmarkResult` itself rather than
asserted bare -- matching the "Richtwert, kein Messprotokoll" statistical-honesty
framing already established for this project's gate documentation
(`docs/capture-protocol.md`).

`extrapolate_game_runtime`'s assumptions (D-11, mirrored verbatim into every
`BenchmarkResult.formula`):

1. Runtime is **linear** in footage duration at fixed resolution and settings -- a
   per-stage real-time factor (`stage.seconds / footage_seconds`) measured on a sample
   is assumed to hold unchanged over a full game's worth of footage. This is the one
   assumption that could break down at the extremes (a cold-start model-load cost
   amortized over a longer run, cache effects), which is exactly why this module is a
   sample-to-estimate *extrapolation*, not a claim of having measured the real thing.
2. The pilot's 61 per-play clips **exclude dead time between plays** -- `footage_seconds`
   (their total decoded duration) undercounts what a continuous game recording would be.
   `game_seconds` is therefore the honest denominator for C-09, never the raw clip total.
3. `game_seconds` **defaults to 3000** (50 minutes of continuous recording) and is a
   caller-supplied override (the CLI's `--game-minutes`), because the real number
   depends on the capture protocol's battery-swap regime, not something this module can
   assume.
4. The **measurement machine is the primary Mac** (D-11); any Dell/Colab numbers are
   reference only. `BenchmarkResult.machine` records which machine produced the
   timings being extrapolated, so a reader never has to guess.

Implemented by plan 02.1-11, alongside `detect.load_detector`/`detect_video`.
"""

from __future__ import annotations

from dataclasses import dataclass

from flag_football_ep.cv import CvError


class InvalidBenchmarkInput(CvError, ValueError):
    """Raised when `extrapolate_game_runtime` is called with a non-positive
    `footage_seconds`/`game_seconds` or an empty `stages` tuple -- an extrapolation
    from nothing (or from a nonsensical duration) is worse than no number at all for a
    go/no-go gate.
    """


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
    the sample covered, the full-game duration extrapolated to, the machine that
    produced the timings, the extrapolated full-game minutes, and the formula string
    used to produce that extrapolation (D-11 assumption 4 -- machine identity is part
    of the result, not just the log output).
    """

    stages: tuple[StageTiming, ...]
    footage_seconds: float
    game_seconds: float
    machine: str
    extrapolated_game_minutes: float
    formula: str


def _machine_identifier() -> str:
    """A human-readable machine identifier -- never a secret, just `platform.node()`
    (the hostname), matching `cv.detect._machine_identifier`'s own provenance
    convention for training records.
    """
    import platform

    return platform.node()


def extrapolate_game_runtime(
    stages: tuple[StageTiming, ...], footage_seconds: float, game_seconds: float
) -> BenchmarkResult:
    """Extrapolate `stages`' measured per-stage timings (covering `footage_seconds` of
    sample footage) to a full `game_seconds` game, returning the documented formula
    alongside the extrapolated minutes.

    Per-stage real-time factor = `stage.seconds / footage_seconds`; the total factor is
    their sum (every stage's cost accumulates -- decode, detect, and postprocess all run
    for every frame, they do not overlap in this pipeline); the extrapolated full-game
    duration is `total_factor * game_seconds`. See this module's docstring for the four
    assumptions (linearity, continuous-game denominator, the 3000s/50min default,
    primary-Mac measurement) mirrored verbatim into `formula`.

    Raises `InvalidBenchmarkInput` when `footage_seconds` or `game_seconds` is
    non-positive, or when `stages` is empty.
    """
    if not stages:
        raise InvalidBenchmarkInput(
            "cannot extrapolate a game runtime from an empty stage-timing list"
        )
    if footage_seconds <= 0:
        raise InvalidBenchmarkInput(
            f"footage_seconds must be positive, got {footage_seconds}"
        )
    if game_seconds <= 0:
        raise InvalidBenchmarkInput(f"game_seconds must be positive, got {game_seconds}")

    machine = _machine_identifier()

    stage_factors = [(stage, stage.seconds / footage_seconds) for stage in stages]
    total_factor = sum(factor for _, factor in stage_factors)
    extrapolated_seconds = total_factor * game_seconds
    extrapolated_minutes = extrapolated_seconds / 60.0

    stage_terms = " + ".join(
        f"{stage.stage}({stage.seconds}s / {footage_seconds}s footage = {factor}x realtime)"
        for stage, factor in stage_factors
    )
    formula = (
        f"[machine={machine}] linear extrapolation (assumption 1): "
        f"total real-time factor = {stage_terms} = {total_factor}x realtime; "
        f"extrapolated game duration = {total_factor}x * {game_seconds}s game "
        f"(continuous-game denominator, assumption 2) "
        f"= {extrapolated_seconds}s = {extrapolated_minutes} min"
    )

    return BenchmarkResult(
        stages=tuple(stages),
        footage_seconds=footage_seconds,
        game_seconds=game_seconds,
        machine=machine,
        extrapolated_game_minutes=extrapolated_minutes,
        formula=formula,
    )
