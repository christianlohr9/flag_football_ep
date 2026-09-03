"""Coverage for `flag_football_ep.charts.explosiveness`: chart structure only, mirroring
`tests/test_charts_fourth_down.py` -- never pixel content.

Input frames are built inline with polars, matching the shape
`scripts/explosiveness_comparison.py` writes to `data/reference/explosiveness/*.csv`;
nothing here reads from `data/`.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.container
import polars as pl

from flag_football_ep.charts.explosiveness import render_cliff_zone, render_definition_comparison
from flag_football_ep.features.explosiveness import DEFINITIONS

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"

_CLIFF_TABLE = pl.DataFrame(
    {
        "yards_gained": list(range(8, 17)),
        "n": [10, 20, 30, 40, 50, 5, 5, 5, 5],
        "share": [0.05, 0.10, 0.15, 0.20, 0.25, 0.025, 0.025, 0.025, 0.025],
        "hc_explosive": [False, False, False, False, False, True, True, True, True],
    }
)

_EMPTY_CLIFF_TABLE = pl.DataFrame(
    schema={
        "yards_gained": pl.Int32,
        "n": pl.Int64,
        "share": pl.Float64,
        "hc_explosive": pl.Boolean,
    }
)

_COMPARISON_ROWS = [
    {
        "definition": "explosive_epa_magnitude",
        "label_de": "Explosiveness (EPA-Magnitude auf Erfolgen)",
        "n": 100,
        "successes": 20,
        "rate": 0.20,
        "ci_low": 0.13,
        "ci_high": 0.29,
        "muted": False,
    },
    {
        "definition": "baseline_hc_workbook",
        "label_de": "HC-Workbook Explosive % (Yards > 12, nur Pass)",
        "n": 50,
        "successes": 8,
        "rate": 0.16,
        "ci_low": 0.07,
        "ci_high": 0.29,
        "muted": False,
    },
    {
        "definition": "success_rate_epa",
        "label_de": "Success Rate (EPA > 0)",
        "n": 200,
        "successes": 100,
        "rate": 0.50,
        "ci_low": 0.43,
        "ci_high": 0.57,
        "muted": False,
    },
    {
        "definition": "baseline_hc_verbal",
        "label_de": "HC mündliche Regel (Yards > 12 oder EPA > 0, nur Pass)",
        "n": 3,
        "successes": 2,
        "rate": 0.667,
        "ci_low": 0.09,
        "ci_high": 0.99,
        "muted": True,
    },
    {
        "definition": "unknown_future_definition",
        "label_de": "Sollte verworfen werden",
        "n": 5,
        "successes": 1,
        "rate": 0.20,
        "ci_low": 0.01,
        "ci_high": 0.60,
        "muted": False,
    },
]
_COMPARISON_FRAME = pl.DataFrame(_COMPARISON_ROWS)

_EMPTY_COMPARISON_FRAME = pl.DataFrame(
    schema={
        "definition": pl.Utf8,
        "label_de": pl.Utf8,
        "n": pl.Int64,
        "successes": pl.Int64,
        "rate": pl.Float64,
        "ci_low": pl.Float64,
        "ci_high": pl.Float64,
        "muted": pl.Boolean,
    }
)

_ONLY_UNKNOWN_COMPARISON_FRAME = pl.DataFrame([_COMPARISON_ROWS[-1]])


class TestRenderCliffZone:
    def test_returns_a_matplotlib_figure(self):
        from matplotlib.figure import Figure

        fig = render_cliff_zone(_CLIFF_TABLE)

        assert isinstance(fig, Figure)

    def test_one_bar_per_yards_gained_row(self):
        fig = render_cliff_zone(_CLIFF_TABLE)
        ax = fig.axes[0]

        bar_containers = [
            c for c in ax.containers if isinstance(c, matplotlib.container.BarContainer)
        ]
        assert len(bar_containers) == 1
        assert len(bar_containers[0].patches) == _CLIFF_TABLE.height

    def test_x_axis_label_names_yards(self):
        fig = render_cliff_zone(_CLIFF_TABLE)
        ax = fig.axes[0]

        assert "Yards" in ax.get_xlabel()

    def test_vertical_rule_between_threshold_and_threshold_plus_one(self):
        fig = render_cliff_zone(_CLIFF_TABLE, threshold=12)
        ax = fig.axes[0]

        yards = _CLIFF_TABLE.sort("yards_gained")["yards_gained"].to_list()
        expected_x = (yards.index(12) + yards.index(13)) / 2

        dashed_vlines = [
            line
            for line in ax.get_lines()
            if line.get_linestyle() in ("--", "dashed") and len(set(line.get_xdata())) == 1
        ]
        assert any(line.get_xdata()[0] == expected_x for line in dashed_vlines)

    def test_10_11_12_bars_annotated_with_combined_share(self):
        fig = render_cliff_zone(_CLIFF_TABLE, threshold=12)
        ax = fig.axes[0]

        # shares at yards 10, 11, 12 in the fixture: 0.15 + 0.20 + 0.25 = 0.60
        expected_text = "Klippen-Zone 10-12: 60.0%"
        annotation_texts = [t.get_text() for t in ax.texts]
        matches = [t for t in annotation_texts if t == expected_text]
        assert len(matches) == 3, annotation_texts

    def test_empty_table_returns_figure_with_keine_daten_and_no_bars(self):
        from matplotlib.figure import Figure

        fig = render_cliff_zone(_EMPTY_CLIFF_TABLE)
        ax = fig.axes[0]

        assert isinstance(fig, Figure)
        annotation_texts = [t.get_text() for t in ax.texts]
        assert annotation_texts == ["keine Daten"]
        bar_containers = [
            c for c in ax.containers if isinstance(c, matplotlib.container.BarContainer)
        ]
        assert bar_containers == []


class TestRenderDefinitionComparison:
    def test_returns_a_matplotlib_figure(self):
        from matplotlib.figure import Figure

        fig = render_definition_comparison(_COMPARISON_FRAME)

        assert isinstance(fig, Figure)

    def test_one_bar_per_known_definition_in_definitions_order(self):
        fig = render_definition_comparison(_COMPARISON_FRAME)
        ax = fig.axes[0]

        labels = [t.get_text() for t in ax.get_yticklabels()]
        expected = [d.label_de for d in DEFINITIONS]
        assert labels == expected

    def test_unknown_definition_is_dropped_not_plotted(self):
        fig = render_definition_comparison(_COMPARISON_FRAME)
        ax = fig.axes[0]

        labels = [t.get_text() for t in ax.get_yticklabels()]
        assert "Sollte verworfen werden" not in labels

    def test_asymmetric_error_bars_present_for_every_known_row(self):
        fig = render_definition_comparison(_COMPARISON_FRAME)
        ax = fig.axes[0]

        error_containers = [
            c for c in ax.containers if isinstance(c, matplotlib.container.ErrorbarContainer)
        ]
        assert len(error_containers) == len(DEFINITIONS)

    def test_every_bar_annotated_with_its_n(self):
        fig = render_definition_comparison(_COMPARISON_FRAME)
        ax = fig.axes[0]

        annotation_texts = [t.get_text() for t in ax.texts]
        for row in _COMPARISON_ROWS[:4]:
            assert any(f"n={row['n']}" in text for text in annotation_texts)

    def test_muted_row_rendered_grey_and_still_annotated(self):
        import matplotlib.colors as mcolors

        fig = render_definition_comparison(_COMPARISON_FRAME)
        ax = fig.axes[0]

        muted_index = [d.key for d in DEFINITIONS].index("baseline_hc_verbal")
        bar_containers = [
            c for c in ax.containers if isinstance(c, matplotlib.container.BarContainer)
        ]
        muted_patch = bar_containers[0].patches[muted_index]
        assert mcolors.to_hex(muted_patch.get_facecolor()) == "#999999"

        annotation_texts = [t.get_text() for t in ax.texts]
        assert any("n=3" in text for text in annotation_texts)

    def test_empty_frame_returns_figure_with_keine_daten(self):
        fig = render_definition_comparison(_EMPTY_COMPARISON_FRAME)
        ax = fig.axes[0]

        annotation_texts = [t.get_text() for t in ax.texts]
        assert annotation_texts == ["keine Daten"]

    def test_frame_with_only_unknown_definitions_returns_placeholder(self):
        fig = render_definition_comparison(_ONLY_UNKNOWN_COMPARISON_FRAME)
        ax = fig.axes[0]

        annotation_texts = [t.get_text() for t in ax.texts]
        assert annotation_texts == ["keine Daten"]


def test_neither_renderer_writes_a_file(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    render_cliff_zone(_CLIFF_TABLE)
    render_definition_comparison(_COMPARISON_FRAME)

    assert list(tmp_path.rglob("*.png")) == []
    assert list(tmp_path.rglob("*")) == []


def test_module_sets_agg_backend_without_display() -> None:
    import matplotlib

    render_cliff_zone(_CLIFF_TABLE)
    assert matplotlib.get_backend().lower() == "agg"
