"""Coverage for `flag_football_ep.charts.fourth_down`: chart structure and file-write
behaviour (REQ-S1-14).

Chart tests assert on figure structure (axes, bar containers, annotation text, axis labels,
line data) and file behaviour -- never on pixel content, per the plan's explicit instruction.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from flag_football_ep.charts.fourth_down import render_fourth_down, write_fourth_down
from flag_football_ep.features.mutations import (
    FOURTH_DOWN_DISTANCE_BUCKETS,
    FourthDownBucket,
    FourthDownRates,
)

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _rates(bucket_overrides: dict[str, dict] | None = None, overall_rate=None) -> FourthDownRates:
    """Build a `FourthDownRates` with one bucket per `FOURTH_DOWN_DISTANCE_BUCKETS` entry.

    `bucket_overrides` maps a bucket label to attempts/conversions/rate/ci overrides; any
    bucket not named there defaults to zero attempts (an empty bucket).
    """
    bucket_overrides = bucket_overrides or {}
    buckets = []
    total_attempts = 0
    total_conversions = 0
    for label, min_distance, max_distance in FOURTH_DOWN_DISTANCE_BUCKETS:
        override = bucket_overrides.get(label)
        if override is None:
            bucket = FourthDownBucket(
                label=label,
                min_distance=min_distance,
                max_distance=max_distance,
                attempts=0,
                conversions=0,
                rate=None,
                ci=None,
            )
        else:
            bucket = FourthDownBucket(
                label=label,
                min_distance=min_distance,
                max_distance=max_distance,
                attempts=override["attempts"],
                conversions=override["conversions"],
                rate=override["rate"],
                ci=override["ci"],
            )
            total_attempts += bucket.attempts
            total_conversions += bucket.conversions
        buckets.append(bucket)

    return FourthDownRates(
        buckets=tuple(buckets),
        total_attempts=total_attempts,
        total_conversions=total_conversions,
        overall_rate=overall_rate,
    )


_POPULATED_RATES = _rates(
    bucket_overrides={
        "1-3": {"attempts": 20, "conversions": 12, "rate": 0.6, "ci": (0.4, 0.8)},
        "4-6": {"attempts": 15, "conversions": 6, "rate": 0.4, "ci": (0.2, 0.6)},
        "7-10": {"attempts": 10, "conversions": 3, "rate": 0.3, "ci": (0.1, 0.5)},
        # "11+" left at zero attempts: an explicit gap alongside populated buckets.
    },
    overall_rate=0.467,
)

_ALL_EMPTY_RATES = _rates()

_NO_OVERALL_RATE = _rates(
    bucket_overrides={
        "1-3": {"attempts": 5, "conversions": 2, "rate": 0.4, "ci": (0.1, 0.7)},
    },
    overall_rate=None,
)


class TestRenderFourthDown:
    def test_returns_a_matplotlib_figure(self):
        from matplotlib.figure import Figure

        fig = render_fourth_down(_POPULATED_RATES)

        assert isinstance(fig, Figure)

    def test_has_exactly_one_axes(self):
        fig = render_fourth_down(_POPULATED_RATES)

        assert len(fig.axes) == 1

    def test_axis_labels(self):
        fig = render_fourth_down(_POPULATED_RATES)
        ax = fig.axes[0]

        assert ax.get_xlabel() == "Distance (yards to go)"
        assert ax.get_ylabel() == "Conversion rate"

    def test_y_limits_are_0_1_or_wider(self):
        fig = render_fourth_down(_POPULATED_RATES)
        ax = fig.axes[0]

        lo, hi = ax.get_ylim()
        assert lo <= 0
        assert hi >= 1

    def test_one_bar_container_per_non_empty_bucket(self):
        import matplotlib.container

        fig = render_fourth_down(_POPULATED_RATES)
        ax = fig.axes[0]

        bar_containers = [
            c for c in ax.containers if isinstance(c, matplotlib.container.BarContainer)
        ]
        non_empty_buckets = [b for b in _POPULATED_RATES.buckets if b.attempts > 0]
        assert len(bar_containers) == len(non_empty_buckets)

    def test_annotation_texts_include_n_for_every_non_empty_bucket(self):
        fig = render_fourth_down(_POPULATED_RATES)
        ax = fig.axes[0]

        annotation_texts = [a.get_text() for a in ax.texts]
        for bucket in _POPULATED_RATES.buckets:
            if bucket.attempts > 0:
                assert any(f"n={bucket.attempts}" in text for text in annotation_texts)

    def test_empty_bucket_annotated_keine_daten(self):
        fig = render_fourth_down(_POPULATED_RATES)
        ax = fig.axes[0]

        annotation_texts = [a.get_text() for a in ax.texts]
        assert any("keine Daten" in text for text in annotation_texts)

    def test_all_empty_rates_renders_a_figure_with_keine_daten_and_raises_nothing(self):
        from matplotlib.figure import Figure

        fig = render_fourth_down(_ALL_EMPTY_RATES)
        ax = fig.axes[0]

        assert isinstance(fig, Figure)
        annotation_texts = [a.get_text() for a in ax.texts]
        assert annotation_texts.count("keine Daten") == len(FOURTH_DOWN_DISTANCE_BUCKETS)
        assert len(ax.containers) == 0

    def _dashed_reference_lines(self, ax):
        # Filter out the errorbar cap Line2D artists (solid, narrow x-span) so only the
        # axhline reference line -- drawn dashed and constant across the full x range -- is
        # considered.
        return [
            line
            for line in ax.get_lines()
            if line.get_linestyle() in ("--", "dashed")
            and len(set(line.get_ydata())) == 1
        ]

    def test_constant_horizontal_line_at_overall_rate_when_present(self):
        fig = render_fourth_down(_POPULATED_RATES)
        ax = fig.axes[0]

        dashed_lines = self._dashed_reference_lines(ax)
        assert any(
            line.get_ydata()[0] == pytest.approx(_POPULATED_RATES.overall_rate)
            for line in dashed_lines
        )

    def test_no_horizontal_line_when_overall_rate_is_none(self):
        fig = render_fourth_down(_NO_OVERALL_RATE)
        ax = fig.axes[0]

        assert self._dashed_reference_lines(ax) == []
        assert ax.get_legend() is None


class TestWriteFourthDown:
    def test_writes_a_png_and_returns_the_path(self, tmp_path: Path):
        out_path = tmp_path / "chart.png"

        result = write_fourth_down(_POPULATED_RATES, out_path)

        assert result == out_path
        assert out_path.exists()
        assert out_path.read_bytes()[:8] == _PNG_SIGNATURE

    def test_refuses_to_overwrite_existing_file_by_default(self, tmp_path: Path):
        out_path = tmp_path / "chart.png"
        write_fourth_down(_POPULATED_RATES, out_path)

        with pytest.raises(FileExistsError) as excinfo:
            write_fourth_down(_POPULATED_RATES, out_path)

        assert str(out_path) in str(excinfo.value)

    def test_overwrite_true_replaces_existing_file(self, tmp_path: Path):
        out_path = tmp_path / "chart.png"
        write_fourth_down(_POPULATED_RATES, out_path)

        result = write_fourth_down(_POPULATED_RATES, out_path, overwrite=True)

        assert result == out_path
        assert out_path.read_bytes()[:8] == _PNG_SIGNATURE

    def test_creates_parent_directories(self, tmp_path: Path):
        out_path = tmp_path / "nested" / "dir" / "chart.png"

        write_fourth_down(_POPULATED_RATES, out_path)

        assert out_path.exists()
