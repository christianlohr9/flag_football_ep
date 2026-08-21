"""Coverage for `flag_football_ep.reports.decisions`: the standalone game-decisions cheat
sheet combining PAT break-even and 4th-down conversion by distance on one printable page
(REQ-S1-14)."""

from __future__ import annotations

import matplotlib
import polars as pl

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from flag_football_ep.reports.decisions import build_decisions_page

_FULL_CORPUS_DEFAULTS = {
    "down": 4,
    "yards_to_go": 3,
    "play_type": "run",
    "penalty": 0,
    "first_down": 0,
    "touchdown": 0,
    "one_point_conv_success": 0,
    "two_point_conv_success": 0,
}

# One-point attempts (down=0, yards_to_go<=5): 5 attempts, 4 successes -> rate 0.8
# Two-point attempts (down=0, yards_to_go>5): 4 attempts, 1 success -> rate 0.25
# 4th-down attempts spanning three populated buckets plus one deliberately empty ("11+")
_FULL_CORPUS_ROWS = [
    {"down": 0, "yards_to_go": 3, "one_point_conv_success": 1},
    {"down": 0, "yards_to_go": 3, "one_point_conv_success": 1},
    {"down": 0, "yards_to_go": 3, "one_point_conv_success": 1},
    {"down": 0, "yards_to_go": 3, "one_point_conv_success": 1},
    {"down": 0, "yards_to_go": 3, "one_point_conv_success": 0},
    {"down": 0, "yards_to_go": 8, "two_point_conv_success": 1},
    {"down": 0, "yards_to_go": 8, "two_point_conv_success": 0},
    {"down": 0, "yards_to_go": 8, "two_point_conv_success": 0},
    {"down": 0, "yards_to_go": 8, "two_point_conv_success": 0},
    {"down": 4, "yards_to_go": 2, "first_down": 1},
    {"down": 4, "yards_to_go": 2, "first_down": 1},
    {"down": 4, "yards_to_go": 2, "first_down": 0},
    {"down": 4, "yards_to_go": 5, "first_down": 1},
    {"down": 4, "yards_to_go": 5, "first_down": 0},
    {"down": 4, "yards_to_go": 8, "first_down": 0},
    {"down": 4, "yards_to_go": 8, "first_down": 0},
]


def _plays_frame(rows: list[dict]) -> pl.DataFrame:
    full_rows = [{**_FULL_CORPUS_DEFAULTS, **row} for row in rows]
    columns = list(_FULL_CORPUS_DEFAULTS)
    data = {col: [row[col] for row in full_rows] for col in columns}
    return pl.DataFrame(data)


def _full_corpus_plays() -> pl.DataFrame:
    return _plays_frame(_FULL_CORPUS_ROWS)


class TestBuildDecisionsPage:
    def test_returns_html_string_starting_with_doctype(self):
        html = build_decisions_page(_full_corpus_plays())

        assert isinstance(html, str)
        assert html.startswith("<!DOCTYPE html")

    def test_embeds_exactly_two_charts(self):
        html = build_decisions_page(_full_corpus_plays())

        assert html.count("data:image/png;base64,") == 2

    def test_contains_breakeven_percentage(self):
        html = build_decisions_page(_full_corpus_plays())

        # one_point_rate = 4/5 = 0.8 -> breakeven 2-point rate = 0.4 -> "40%"
        assert "40%" in html

    def test_contains_n_equals_for_pat_and_fourth_down_readings(self):
        html = build_decisions_page(_full_corpus_plays())

        assert "n=4" in html  # two-point attempts (the PAT reading carrying n)
        assert "n=3" in html  # "1-3" 4th-down bucket attempts

    def test_no_down_zero_rows_returns_fourth_down_section_and_pat_hinweis(self):
        rows = [row for row in _FULL_CORPUS_ROWS if row["down"] != 0]

        html = build_decisions_page(_plays_frame(rows))

        assert "Hinweis" in html
        assert "data:image/png;base64," in html  # 4th-down chart still embedded

    def test_closes_all_figures_after_call(self):
        build_decisions_page(_full_corpus_plays())

        assert plt.get_fignums() == []

    def test_no_script_tag_and_no_external_resource_references(self):
        html = build_decisions_page(_full_corpus_plays())

        assert "<script" not in html
        assert "http://" not in html
        assert "https://" not in html


class TestGoNoGoVerdictThreeBranches:
    """T-1.4-21: a go/no-go verdict must never be stated confidently on a straddling
    interval -- the reading has three explicit branches, not a false-confidence default."""

    def test_straddling_ci_prints_honest_too_thin_reading(self):
        # one_point_rate 0.8 -> breakeven 0.4; two-point n=4, 1 success -> wide CI straddles it
        html = build_decisions_page(_full_corpus_plays())

        assert "Zu dünne Datenlage für eine klare Empfehlung." in html

    def test_ci_clearly_above_breakeven_recommends_two_point(self):
        rows = (
            [{"down": 0, "yards_to_go": 3, "one_point_conv_success": 1}] * 4
            + [{"down": 0, "yards_to_go": 3, "one_point_conv_success": 0}] * 16
            # one_point_rate = 0.2 -> breakeven 0.1
            + [{"down": 0, "yards_to_go": 8, "two_point_conv_success": 1}] * 45
            + [{"down": 0, "yards_to_go": 8, "two_point_conv_success": 0}] * 5
            # two_point_rate = 0.9, n=50 -> tight CI, clearly above 0.1
        )

        html = build_decisions_page(_plays_frame(rows))

        assert "Empfehlung: 2-Punkte-Versuch" in html
        assert "Zu dünne Datenlage" not in html

    def test_ci_clearly_below_breakeven_recommends_one_point(self):
        rows = (
            [{"down": 0, "yards_to_go": 3, "one_point_conv_success": 1}] * 45
            + [{"down": 0, "yards_to_go": 3, "one_point_conv_success": 0}] * 5
            # one_point_rate = 0.9 -> breakeven 0.45
            + [{"down": 0, "yards_to_go": 8, "two_point_conv_success": 1}] * 2
            + [{"down": 0, "yards_to_go": 8, "two_point_conv_success": 0}] * 48
            # two_point_rate = 0.04, n=50 -> tight CI, clearly below 0.45
        )

        html = build_decisions_page(_plays_frame(rows))

        assert "Empfehlung: 1-Punkt-Versuch" in html
        assert "Zu dünne Datenlage" not in html


class TestDecisionsCheatsheetTemplate:
    def test_contains_both_chart_headings(self):
        html = build_decisions_page(_full_corpus_plays())

        assert "<h2>PAT: 1 Punkt vs. 2 Punkte</h2>" in html
        assert "<h2>4th down: Conversion nach Distance</h2>" in html

    def test_each_chart_is_inside_a_chart_img_element(self):
        html = build_decisions_page(_full_corpus_plays())

        assert html.count('class="chart-img"') == 2

    def test_summary_block_appears_before_first_chart_block(self):
        html = build_decisions_page(_full_corpus_plays())

        summary_index = html.index('class="summary-block"')
        first_chart_block_index = html.index('class="chart-block"')

        assert summary_index < first_chart_block_index

    def test_footnotes_mention_clopper_pearson(self):
        html = build_decisions_page(_full_corpus_plays())

        assert "Clopper-Pearson" in html

    def test_contains_german_lang_attribute_inherited_from_base(self):
        html = build_decisions_page(_full_corpus_plays())

        assert 'lang="de"' in html
