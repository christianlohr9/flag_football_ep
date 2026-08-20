"""Coverage for `flag_football_ep.charts.tendency`: chart structure only (REQ-S1-12, REQ-S1-13).

Chart tests assert on figure structure (axes, bar counts, colors, annotations, line data) --
never on pixel content, mirroring `tests/test_charts_pat_breakeven.py`'s explicit convention.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.charts.tendency import render_share_bars


def _share_table(n_rows: int = 5) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "label": [f"Route {i}" for i in range(n_rows)],
            "share": [0.05 * (i + 1) for i in range(n_rows)],
            "n": [10 + i for i in range(n_rows)],
            "muted": [i % 2 == 0 for i in range(n_rows)],
        }
    )


class TestRenderShareBars:
    def test_returns_a_matplotlib_figure_with_one_axes(self):
        from matplotlib.figure import Figure

        fig = render_share_bars(_share_table(), label_col="label", title="Test")

        assert isinstance(fig, Figure)
        assert len(fig.axes) == 1

    def test_one_bar_patch_per_row(self):
        table = _share_table(5)
        fig = render_share_bars(table, label_col="label", title="Test")
        ax = fig.axes[0]

        assert len(ax.patches) == 5

    def test_x_limits_are_zero_to_one(self):
        fig = render_share_bars(_share_table(), label_col="label", title="Test")
        ax = fig.axes[0]

        assert ax.get_xlim() == pytest.approx((0.0, 1.0))

    def test_bars_ordered_by_share_descending(self):
        table = _share_table(5)
        fig = render_share_bars(table, label_col="label", title="Test")
        ax = fig.axes[0]

        expected_order = table.sort("share", descending=True)["label"].to_list()
        actual_order = [t.get_text() for t in ax.get_yticklabels()]
        assert actual_order == expected_order

    def test_every_bar_has_an_n_annotation(self):
        table = _share_table(4)
        fig = render_share_bars(table, label_col="label", title="Test")
        ax = fig.axes[0]

        n_annotations = [t for t in ax.texts if t.get_text().startswith("n=")]
        assert len(n_annotations) == 4

    def test_muted_row_facecolor_differs_from_non_muted(self):
        table = pl.DataFrame(
            {
                "label": ["A", "B"],
                "share": [0.6, 0.4],
                "n": [10, 8],
                "muted": [True, False],
            }
        )
        fig = render_share_bars(table, label_col="label", title="Test")
        ax = fig.axes[0]

        colors = {bar.get_facecolor() for bar in ax.patches}
        assert len(colors) == 2

    def test_more_than_max_bars_keeps_top_and_annotates_omitted_count(self):
        table = _share_table(20)
        fig = render_share_bars(table, label_col="label", title="Test", max_bars=12)
        ax = fig.axes[0]

        assert len(ax.patches) == 12
        omitted_texts = [
            t.get_text()
            for t in ax.texts
            if "8" in t.get_text() and "nicht angezeigt" in t.get_text()
        ]
        assert omitted_texts

    def test_empty_table_renders_keine_daten_placeholder_without_raising(self):
        empty = pl.DataFrame(
            schema={
                "label": pl.Utf8,
                "share": pl.Float64,
                "n": pl.Int64,
                "muted": pl.Boolean,
            }
        )

        fig = render_share_bars(empty, label_col="label", title="Test")
        ax = fig.axes[0]

        texts = [t.get_text() for t in ax.texts]
        assert "keine Daten" in texts

    def test_muted_note_present(self):
        fig = render_share_bars(_share_table(3), label_col="label", title="Test")
        ax = fig.axes[0]

        texts = [t.get_text() for t in ax.texts]
        assert any("geringe Stichprobe" in t for t in texts)
