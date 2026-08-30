"""Coverage for `flag_football_ep.cv.benchmark.extrapolate_game_runtime`: pure
arithmetic, no MLflow, no video, no `cv` extras dependency (`benchmark.py` imports
nothing beyond the stdlib and `flag_football_ep.cv.CvError`).
"""

from __future__ import annotations

import platform

import pytest

from flag_football_ep.cv.benchmark import (
    BenchmarkResult,
    InvalidBenchmarkInput,
    StageTiming,
    extrapolate_game_runtime,
)


def _stages() -> tuple[StageTiming, ...]:
    # Sample: 10.5 minutes (630s) of footage. decode 6.3s (0.01x realtime), detect
    # 63.0s (0.1x realtime), postprocess 0.63s (0.001x realtime) -- total factor
    # 0.111x realtime.
    return (
        StageTiming(stage="decode", seconds=6.3, frames=630),
        StageTiming(stage="detect", seconds=63.0, frames=630),
        StageTiming(stage="postprocess", seconds=0.63, frames=630),
    )


def test_known_stage_set_produces_exact_expected_extrapolation() -> None:
    result = extrapolate_game_runtime(_stages(), footage_seconds=630.0, game_seconds=3000.0)

    total_factor = (6.3 + 63.0 + 0.63) / 630.0
    expected_seconds = total_factor * 3000.0
    expected_minutes = expected_seconds / 60.0

    assert isinstance(result, BenchmarkResult)
    assert result.extrapolated_game_minutes == pytest.approx(expected_minutes)


def test_formula_contains_every_input_number_and_the_result() -> None:
    stages = _stages()
    footage_seconds = 630.0
    game_seconds = 3000.0

    result = extrapolate_game_runtime(stages, footage_seconds, game_seconds)

    for stage in stages:
        assert str(stage.seconds) in result.formula
    assert str(footage_seconds) in result.formula
    assert str(game_seconds) in result.formula
    assert str(result.extrapolated_game_minutes) in result.formula


def test_doubling_game_seconds_doubles_the_extrapolation() -> None:
    stages = _stages()

    result_1x = extrapolate_game_runtime(stages, footage_seconds=630.0, game_seconds=3000.0)
    result_2x = extrapolate_game_runtime(stages, footage_seconds=630.0, game_seconds=6000.0)

    assert result_2x.extrapolated_game_minutes == pytest.approx(
        2 * result_1x.extrapolated_game_minutes
    )


@pytest.mark.parametrize(
    ("footage_seconds", "game_seconds"),
    [(0.0, 3000.0), (-1.0, 3000.0), (630.0, 0.0), (630.0, -1.0)],
)
def test_non_positive_footage_or_game_seconds_raises(
    footage_seconds: float, game_seconds: float
) -> None:
    with pytest.raises(InvalidBenchmarkInput):
        extrapolate_game_runtime(_stages(), footage_seconds, game_seconds)


def test_empty_stage_list_raises() -> None:
    with pytest.raises(InvalidBenchmarkInput):
        extrapolate_game_runtime((), footage_seconds=630.0, game_seconds=3000.0)


def test_machine_identifier_appears_in_the_result() -> None:
    result = extrapolate_game_runtime(_stages(), footage_seconds=630.0, game_seconds=3000.0)

    machine = platform.node()
    assert result.machine == machine
    assert machine in result.formula


# --- `ffep cv benchmark` CLI: reads the stage-timings JSON `track_session` persists ---------


def _write_timings_json(path, stages: tuple[StageTiming, ...]) -> None:
    import json

    payload = {
        "session_id": "test-session",
        "tracked_at": "20260516T120000Z",
        "stages": [
            {"stage": stage.stage, "seconds": stage.seconds, "frames": stage.frames}
            for stage in stages
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_benchmark_command_reads_the_persisted_stage_timings_artifact(tmp_path) -> None:
    """`ffep cv benchmark --timings` must run against exactly the artifact
    `track_session` writes -- the previous `--tracks` option pointed at the canonical
    tracking Parquet, which carries no stage/seconds/frames columns at all.
    """
    from typer.testing import CliRunner

    from test_config import MINIMAL_TOML

    from flag_football_ep.cli import app

    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")

    timings_path = tmp_path / "test-session_stage_timings.json"
    _write_timings_json(timings_path, _stages())

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["cv", "benchmark", "--config", str(config_path), "--timings", str(timings_path)],
    )

    assert result.exit_code == 0, result.output
    assert "extrapolated:" in result.output
    assert "min/game" in result.output
    assert "formula:" in result.output
