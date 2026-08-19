"""Coverage for `flag_football_ep.charts.pat_breakeven`: chart structure, file-write
behaviour, and the `ffep pat-breakeven` CLI command (REQ-S1-10, REQ-S1-14).

Chart tests assert on figure structure (axes, line data, axis labels, marker positions) and
file behaviour -- never on pixel content, per the plan's explicit instruction.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

import test_pipeline_ingest as tpi
from flag_football_ep.charts.pat_breakeven import render_pat_breakeven, write_pat_breakeven
from flag_football_ep.cli import app
from flag_football_ep.features.mutations import PatBaselines

_CLI_RUNNER = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    """Strip ANSI escape codes -- rich's `--help` rendering can split an option's leading
    `--` from the rest of its name with a color-reset escape sequence, which breaks a naive
    substring check against the raw CliRunner output (mirrors `test_cli_smoke.py::_plain`)."""
    return _ANSI_RE.sub("", output)

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"

_BASELINES = PatBaselines(
    one_point_rate=0.6,
    one_point_attempts=20,
    one_point_successes=12,
    one_point_ci=(0.4, 0.8),
    two_point_rate=0.35,
    two_point_attempts=15,
    two_point_successes=5,
    two_point_ci=(0.15, 0.55),
)

_BASELINES_NO_CI = PatBaselines(
    one_point_rate=0.6,
    one_point_attempts=20,
    one_point_successes=12,
    one_point_ci=None,
    two_point_rate=0.35,
    two_point_attempts=15,
    two_point_successes=5,
    two_point_ci=None,
)


class TestRenderPatBreakeven:
    def test_returns_a_matplotlib_figure(self):
        from matplotlib.figure import Figure

        fig = render_pat_breakeven(_BASELINES)

        assert isinstance(fig, Figure)

    def test_has_exactly_one_axes(self):
        fig = render_pat_breakeven(_BASELINES)

        assert len(fig.axes) == 1

    def test_axis_labels(self):
        fig = render_pat_breakeven(_BASELINES)
        ax = fig.axes[0]

        assert ax.get_xlabel() == "2-point conversion rate"
        assert ax.get_ylabel() == "expected points"

    def test_one_point_line_is_constant_at_the_observed_rate(self):
        fig = render_pat_breakeven(_BASELINES)
        ax = fig.axes[0]

        one_point_line = ax.get_lines()[0]
        y_values = one_point_line.get_ydata()
        assert {round(float(y), 6) for y in y_values} == {
            round(_BASELINES.one_point_rate, 6)
        }

    def test_two_point_line_sweeps_the_full_0_to_1_grid(self):
        fig = render_pat_breakeven(_BASELINES)
        ax = fig.axes[0]

        two_point_line = ax.get_lines()[1]
        x_values = two_point_line.get_xdata()
        y_values = two_point_line.get_ydata()
        assert min(x_values) == pytest.approx(0.0)
        assert max(x_values) == pytest.approx(1.0)
        assert min(y_values) == pytest.approx(0.0)
        assert max(y_values) == pytest.approx(2.0)

    def test_breakeven_marker_at_one_point_rate_over_two(self):
        fig = render_pat_breakeven(_BASELINES)
        ax = fig.axes[0]

        expected_breakeven = _BASELINES.one_point_rate / 2
        vline_x_values = [
            line.get_xdata()[0]
            for line in ax.get_lines()
            if len(set(line.get_xdata())) == 1
        ]
        assert any(x == pytest.approx(expected_breakeven) for x in vline_x_values)

    def test_observed_two_point_rate_marker_present(self):
        fig = render_pat_breakeven(_BASELINES)
        ax = fig.axes[0]

        vline_x_values = [
            line.get_xdata()[0]
            for line in ax.get_lines()
            if len(set(line.get_xdata())) == 1
        ]
        assert any(x == pytest.approx(_BASELINES.two_point_rate) for x in vline_x_values)

    def test_ci_band_present_when_two_point_ci_given(self):
        fig = render_pat_breakeven(_BASELINES)
        ax = fig.axes[0]

        assert len(ax.patches) >= 1

    def test_no_ci_band_when_two_point_ci_is_none(self):
        fig = render_pat_breakeven(_BASELINES_NO_CI)
        ax = fig.axes[0]

        assert len(ax.patches) == 0

    def test_legend_mentions_both_attempt_counts(self):
        fig = render_pat_breakeven(_BASELINES)
        ax = fig.axes[0]

        legend_text = " ".join(t.get_text() for t in ax.get_legend().get_texts())
        assert str(_BASELINES.one_point_attempts) in legend_text


class TestWritePatBreakeven:
    def test_writes_a_png_and_returns_the_path(self, tmp_path: Path):
        out_path = tmp_path / "chart.png"

        result = write_pat_breakeven(_BASELINES, out_path)

        assert result == out_path
        assert out_path.exists()
        assert out_path.read_bytes()[:8] == _PNG_SIGNATURE

    def test_refuses_to_overwrite_existing_file_by_default(self, tmp_path: Path):
        out_path = tmp_path / "chart.png"
        write_pat_breakeven(_BASELINES, out_path)

        with pytest.raises(FileExistsError) as excinfo:
            write_pat_breakeven(_BASELINES, out_path)

        assert str(out_path) in str(excinfo.value)

    def test_overwrite_true_replaces_existing_file(self, tmp_path: Path):
        out_path = tmp_path / "chart.png"
        write_pat_breakeven(_BASELINES, out_path)

        result = write_pat_breakeven(_BASELINES, out_path, overwrite=True)

        assert result == out_path
        assert out_path.read_bytes()[:8] == _PNG_SIGNATURE

    def test_creates_parent_directories(self, tmp_path: Path):
        out_path = tmp_path / "nested" / "dir" / "chart.png"

        write_pat_breakeven(_BASELINES, out_path)

        assert out_path.exists()


def _write_pat_plays_parquet(
    processed_dir: Path, *, one_point: bool = True, two_point: bool = True
) -> None:
    """A minimal `plays.parquet` carrying only the columns `estimate_pat_baselines` needs."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    if one_point:
        for i in range(10):
            rows.append(
                {
                    "down": 0,
                    "yards_to_go": 3,
                    "one_point_conv_success": 1 if i % 2 == 0 else 0,
                    "two_point_conv_success": 0,
                }
            )
    if two_point:
        for i in range(6):
            rows.append(
                {
                    "down": 0,
                    "yards_to_go": 8,
                    "one_point_conv_success": 0,
                    "two_point_conv_success": 1 if i % 3 == 0 else 0,
                }
            )
    if not rows:
        rows.append(
            {
                "down": 1,
                "yards_to_go": 10,
                "one_point_conv_success": 0,
                "two_point_conv_success": 0,
            }
        )
    df = pl.DataFrame(rows)
    df.write_parquet(processed_dir / "plays.parquet")


class TestPatBreakevenCli:
    def test_help_exits_zero_and_lists_out_and_overwrite(self):
        result = _CLI_RUNNER.invoke(app, ["pat-breakeven", "--help"])

        assert result.exit_code == 0
        output = _plain(result.output)
        assert "--out" in output
        assert "--overwrite" in output

    def test_default_out_path_is_under_processed_dir(self, tmp_path: Path, repo_root: Path):
        config = tpi._make_config(tmp_path, repo_root)
        toml_path = tpi._write_toml_config(tmp_path, repo_root)
        _write_pat_plays_parquet(config.paths.processed)

        result = _CLI_RUNNER.invoke(app, ["pat-breakeven", "--config", str(toml_path)])

        assert result.exit_code == 0, result.output
        default_path = config.paths.processed / "pat_breakeven.png"
        assert default_path.exists()
        assert default_path.read_bytes()[:8] == _PNG_SIGNATURE

    def test_explicit_out_path_is_used(self, tmp_path: Path, repo_root: Path):
        config = tpi._make_config(tmp_path, repo_root)
        toml_path = tpi._write_toml_config(tmp_path, repo_root)
        _write_pat_plays_parquet(config.paths.processed)
        out_path = tmp_path / "custom" / "chart.png"

        result = _CLI_RUNNER.invoke(
            app, ["pat-breakeven", "--config", str(toml_path), "--out", str(out_path)]
        )

        assert result.exit_code == 0, result.output
        assert out_path.exists()
        assert out_path.read_bytes()[:8] == _PNG_SIGNATURE

    def test_echoes_both_rates_with_attempt_counts_and_ci(
        self, tmp_path: Path, repo_root: Path
    ):
        config = tpi._make_config(tmp_path, repo_root)
        toml_path = tpi._write_toml_config(tmp_path, repo_root)
        _write_pat_plays_parquet(config.paths.processed)

        result = _CLI_RUNNER.invoke(app, ["pat-breakeven", "--config", str(toml_path)])

        assert result.exit_code == 0, result.output
        assert "1-pt rate" in result.output
        assert "2-pt rate" in result.output
        assert "n=10" in result.output
        assert "n=6" in result.output

    def test_zero_pat_attempts_exits_nonzero_naming_pat_type(
        self, tmp_path: Path, repo_root: Path
    ):
        config = tpi._make_config(tmp_path, repo_root)
        toml_path = tpi._write_toml_config(tmp_path, repo_root)
        _write_pat_plays_parquet(config.paths.processed, one_point=False, two_point=False)

        result = _CLI_RUNNER.invoke(app, ["pat-breakeven", "--config", str(toml_path)])

        assert result.exit_code != 0
        message = str(result.exception) if result.exception else result.output
        assert "one-point" in message

    def test_overwrite_flag_replaces_existing_chart(self, tmp_path: Path, repo_root: Path):
        config = tpi._make_config(tmp_path, repo_root)
        toml_path = tpi._write_toml_config(tmp_path, repo_root)
        _write_pat_plays_parquet(config.paths.processed)
        out_path = tmp_path / "chart.png"
        write_pat_breakeven(_BASELINES, out_path)

        result = _CLI_RUNNER.invoke(
            app,
            [
                "pat-breakeven",
                "--config",
                str(toml_path),
                "--out",
                str(out_path),
                "--overwrite",
            ],
        )

        assert result.exit_code == 0, result.output
        assert out_path.read_bytes()[:8] == _PNG_SIGNATURE
