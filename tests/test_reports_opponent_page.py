"""Unit coverage for `flag_football_ep.reports.opponent`'s HTML page assembly
(`team_slug`, `opponent_filename`, `build_opponent_page`) -- REQ-S1-12's rendered
deliverable -- and `templates/opponent_report.html.j2`'s structural/escaping behaviour.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import pytest
from markupsafe import escape

from flag_football_ep.reports.opponent import (
    OpponentReportData,
    build_opponent_data,
    build_opponent_page,
    opponent_filename,
    team_slug,
)
from flag_football_ep.testing import canonical_plays


def _single_game_data(team_name: str = "Team AAA") -> OpponentReportData:
    """One game's worth of plays for team "AAA": a muted (group_n=2) and an unmuted
    (group_n=8) group in every section, with both `off_form` and `target_route`
    charted, so every section is non-empty and the two share-based sections each get a
    chart. `n_games=1` deliberately triggers the "nur 1 Spiel" notice, so this fixture
    also covers the non-empty-notices path.
    """
    down = [3, 3] + [1] * 8
    distance = [8, 8] + [3] * 8
    yardline = [25, 25] + [5] * 8
    off_form = ["Bunch", "Bunch"] + ["Trips"] * 8
    target_route = ["Slant", "Slant"] + [
        "Go",
        "Slant",
        "Go",
        "Slant",
        "Go",
        "Slant",
        "Go",
        "Slant",
    ]
    play_type = ["pass", "pass"] + ["pass"] * 5 + ["run"] * 3

    df = canonical_plays(
        n_games=1,
        plays_per_game=10,
        overrides={
            "posteam": ["AAA"] * 10,
            "defteam": ["BBB"] * 10,
            "down": down,
            "yards_to_go": distance,
            "yardline_50": yardline,
            "play_type": play_type,
        },
        extras={"off_form": off_form, "target_route": target_route},
    )
    return build_opponent_data(df, team="AAA", team_name=team_name)


def _healthy_data(team_name: str = "Team AAA") -> OpponentReportData:
    """Two games of the same shape as `_single_game_data` -- clears the "nur 1 Spiel"
    notice, so `notices` is empty and no section carries an `empty_notice`. Used to
    test that the notices `.warning-block` only appears when it has something to say.
    """
    down = ([3, 3] + [1] * 8) * 2
    distance = ([8, 8] + [3] * 8) * 2
    yardline = ([25, 25] + [5] * 8) * 2
    off_form = (["Bunch", "Bunch"] + ["Trips"] * 8) * 2
    target_route = (
        ["Slant", "Slant"]
        + ["Go", "Slant", "Go", "Slant", "Go", "Slant", "Go", "Slant"]
    ) * 2
    play_type = (["pass", "pass"] + ["pass"] * 5 + ["run"] * 3) * 2

    df = canonical_plays(
        n_games=2,
        plays_per_game=10,
        overrides={
            "posteam": ["AAA"] * 20,
            "defteam": ["BBB"] * 20,
            "down": down,
            "yards_to_go": distance,
            "yardline_50": yardline,
            "play_type": play_type,
        },
        extras={"off_form": off_form, "target_route": target_route},
    )
    return build_opponent_data(df, team="AAA", team_name=team_name)


def _zero_film_data(team_name: str = "Team ZZZ") -> OpponentReportData:
    df = canonical_plays(n_games=1, plays_per_game=6, overrides={"posteam": ["AAA"] * 6})
    return build_opponent_data(df, team="ZZZ", team_name=team_name)


class TestTeamSlug:
    def test_leaves_plain_code_unchanged(self) -> None:
        assert team_slug("FRA") == "FRA"

    def test_sanitises_path_traversal_attempt(self) -> None:
        result = team_slug("../etc")
        assert "/" not in result
        assert ".." not in result

    def test_sanitises_value_containing_slash(self) -> None:
        result = team_slug("a/b")
        assert "/" not in result

    def test_raises_on_empty_string(self) -> None:
        with pytest.raises(ValueError):
            team_slug("")

    def test_raises_on_separator_only_string(self) -> None:
        with pytest.raises(ValueError):
            team_slug("---")


class TestOpponentFilename:
    def test_matches_expected_pattern(self) -> None:
        assert opponent_filename("FRA") == "opponent-FRA.html"

    def test_contains_no_path_separator(self) -> None:
        assert "/" not in opponent_filename("../etc")


class TestBuildOpponentPage:
    def test_returns_standalone_html_document(self) -> None:
        page = build_opponent_page(_single_game_data())
        assert page.startswith("<!DOCTYPE html")

    def test_summary_sentences_appear_in_output(self) -> None:
        # Sentences pass through the autoescaping Jinja2 environment like any other
        # data-derived string (e.g. a literal "&" in "Down & Distance" phrasing becomes
        # "&amp;"), so compare against the escaped form rather than the raw sentence.
        data = _single_game_data()
        page = build_opponent_page(data)
        for sentence in data.summary_sentences:
            assert str(escape(sentence)) in page

    def test_one_chart_per_nonempty_share_section(self) -> None:
        page = build_opponent_page(_single_game_data())
        assert page.count("data:image/png;base64,") == 2

    def test_no_leaked_figures(self) -> None:
        # compare against the pre-call set: other test modules may legitimately
        # hold open figures, so asserting an empty registry is order-dependent
        before = set(plt.get_fignums())

        build_opponent_page(_single_game_data())

        assert set(plt.get_fignums()) == before

    def test_zero_film_page_contains_every_empty_notice_and_does_not_raise(self) -> None:
        data = _zero_film_data()
        page = build_opponent_page(data)
        for section in data.sections:
            assert section.empty_notice is not None
            assert section.empty_notice in page

    def test_no_script_or_external_reference(self) -> None:
        page = build_opponent_page(_single_game_data())
        assert "<script" not in page
        assert "http://" not in page
        assert "https://" not in page


class TestOpponentReportTemplate:
    def test_summary_block_precedes_first_heading(self) -> None:
        page = build_opponent_page(_single_game_data())
        assert page.index("summary-block") < page.index("<h2>")

    def test_one_heading_per_section(self) -> None:
        data = _single_game_data()
        page = build_opponent_page(data)
        assert page.count("<h2>") == len(data.sections)

    def test_muted_row_carries_muted_cell_class(self) -> None:
        page = build_opponent_page(_single_game_data())
        assert 'class="muted-cell"' in page

    def test_footnotes_mention_locked_definitions(self) -> None:
        page = build_opponent_page(_single_game_data())
        assert "Clopper-Pearson" in page
        assert "Red Zone" in page
        assert "±6" in page

    def test_notices_block_absent_when_notices_empty(self) -> None:
        # base.html.j2's stylesheet always defines the `.warning-block` CSS rule; what
        # matters is whether an element actually uses the class in the document body.
        data = _healthy_data()
        assert data.notices == ()
        page = build_opponent_page(data)
        assert 'class="warning-block"' not in page

    def test_notices_block_present_when_notices_nonempty(self) -> None:
        data = _single_game_data()
        assert data.notices != ()
        page = build_opponent_page(data)
        assert 'class="warning-block"' in page

    def test_team_name_with_markup_renders_escaped(self) -> None:
        data = _zero_film_data(team_name="<b>x</b>")
        page = build_opponent_page(data)
        assert "&lt;b&gt;" in page
        assert "<b>" not in page

    def test_no_script_or_src_href_http(self) -> None:
        page = build_opponent_page(_single_game_data())
        assert "<script" not in page
        assert 'src="http' not in page
        assert 'href="http' not in page
