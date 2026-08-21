"""Coverage for `flag_football_ep.reports.wp_review` (REQ-S1-15): the per-game WP review
page assembler, its path-safe filename helpers, and WP provenance resolution.
"""

from __future__ import annotations

import pytest

import polars as pl

from flag_football_ep.reports.wp_review import (
    GAME_SLUG_PATTERN,
    build_wp_review_page,
    game_slug,
    wp_review_filename,
)


def _game_plays(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal one-game `game_plays` frame with every column
    `build_wp_review_page`/`render_wp_review`/`select_wp_annotations` touch."""
    defaults = {
        "game_id": "2026-06-14_GER-vs-AUT_EM",
        "home_team": "GER",
        "away_team": "AUT",
        "home_team_score": 21,
        "away_team_score": 14,
        "posteam": "GER",
        "touchdown": 0,
        "interception": 0,
        "one_point_conv_success": 0,
        "two_point_conv_success": 0,
        "safety": 0,
    }
    built = [{**defaults, **row} for row in rows]
    return pl.DataFrame(built)


def _plain_game(n_plays: int = 30) -> pl.DataFrame:
    rows = []
    for play_id in range(1, n_plays + 1):
        home_wp = 0.5 + 0.005 * play_id
        rows.append({"play_id": play_id, "home_wp": home_wp, "wpa": 0.01})
    rows[4] = {**rows[4], "wpa": 0.35}  # play 5: big positive swing
    rows[19] = {**rows[19], "wpa": -0.30, "touchdown": 1}  # play 20: TD + big swing
    return _game_plays(rows)


class TestGameSlug:
    def test_hudl_style_id_is_unchanged(self):
        assert game_slug("2026-06-14_GER-vs-AUT_EM") == "2026-06-14_GER-vs-AUT_EM"

    def test_replaces_slashes_spaces_and_dot_runs(self):
        slug = game_slug("../../etc/passwd")
        assert "/" not in slug
        assert ".." not in slug

    def test_raises_for_empty_string(self):
        with pytest.raises(ValueError):
            game_slug("")

    def test_raises_for_dot_dot_slash_dot_dot(self):
        with pytest.raises(ValueError):
            game_slug("../..")

    def test_raises_for_string_of_only_separators(self):
        with pytest.raises(ValueError):
            game_slug("----....")

    def test_result_always_matches_pattern_or_raises(self):
        for candidate in ["a b/c", "..hidden", "///", "valid-id_2026.01"]:
            try:
                slug = game_slug(candidate)
            except ValueError:
                continue
            assert GAME_SLUG_PATTERN.match(slug)


class TestWpReviewFilename:
    def test_expected_filename(self):
        assert (
            wp_review_filename("2026-06-14_GER-vs-AUT_EM")
            == "wp-review-2026-06-14_GER-vs-AUT_EM.html"
        )

    def test_output_contains_no_slash_and_matches_pattern(self):
        filename = wp_review_filename("../../etc/passwd")
        assert "/" not in filename
        assert filename.startswith("wp-review-")
        assert filename.endswith(".html")


class TestBuildWpReviewPage:
    def test_returns_html_document_string(self):
        page = build_wp_review_page(_plain_game(), game_id="2026-06-14_GER-vs-AUT_EM")
        assert isinstance(page, str)
        assert page.startswith("<!DOCTYPE html")

    def test_exactly_one_embedded_chart(self):
        page = build_wp_review_page(_plain_game(), game_id="g1")
        assert page.count("data:image/png;base64,") == 1

    def test_synthetic_clock_notice_present_when_true(self):
        page = build_wp_review_page(_plain_game(), game_id="g1", synthetic_clock=True)
        assert "synthetisch" in page.lower()

    def test_synthetic_clock_notice_absent_when_false(self):
        page = build_wp_review_page(_plain_game(), game_id="g1", synthetic_clock=False)
        assert "synthetische" not in page.lower()

    def test_swing_table_has_one_row_per_annotation(self):
        from flag_football_ep.charts.wp_review import select_wp_annotations

        game = _plain_game()
        annotations = select_wp_annotations(game)
        page = build_wp_review_page(game, game_id="g1")
        # thead contributes exactly one extra <tr>
        assert page.count("<tr") == annotations.height + 1

    def test_one_row_game_returns_no_wp_data_notice_and_does_not_raise(self):
        game = _game_plays([{"play_id": 1, "home_wp": 0.5, "wpa": None}])
        page = build_wp_review_page(game, game_id="g1")
        assert "keine WP-Daten" in page

    def test_mixed_wp_source_frame_names_both_counts(self):
        game = _plain_game()
        n = game.height
        sources = ["oof"] * (n // 2) + ["champion"] * (n - n // 2)
        game = game.with_columns(wp_source=pl.Series(sources))
        page = build_wp_review_page(game, game_id="g1")
        assert "out-of-fold" in page
        assert "Champion-Modell" in page

    def test_frame_without_wp_source_column_names_provenance_unknown(self):
        game = _plain_game()
        assert "wp_source" not in game.columns
        page = build_wp_review_page(game, game_id="g1")
        assert "WP-Provenienz unbekannt" in page

    def test_no_open_figures_after_call(self):
        import matplotlib.pyplot as plt

        build_wp_review_page(_plain_game(), game_id="g1")
        assert plt.get_fignums() == []
