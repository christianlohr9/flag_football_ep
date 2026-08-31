"""Coverage for `flag_football_ep.reports.wp_review` (REQ-S1-15): the per-game WP review
page assembler, its path-safe filename helpers, and WP provenance resolution.
"""

from __future__ import annotations

import pytest

import polars as pl

from flag_football_ep.features.mutations import add_wp_variables, prepare_wp_data
from flag_football_ep.reports.wp_review import (
    GAME_SLUG_PATTERN,
    attach_wp_provenance,
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

        # compare against the pre-call set: other test modules may legitimately
        # hold open figures, so asserting an empty registry is order-dependent
        before = set(plt.get_fignums())

        build_wp_review_page(_plain_game(), game_id="g1")
        assert set(plt.get_fignums()) == before


class TestWpReviewTemplate:
    def test_warning_block_present_only_when_synthetic_clock_true(self):
        page_with = build_wp_review_page(
            _plain_game(), game_id="g1", synthetic_clock=True
        )
        page_without = build_wp_review_page(
            _plain_game(), game_id="g1", synthetic_clock=False
        )
        assert 'class="warning-block"' in page_with
        assert 'class="warning-block"' not in page_without

    def test_swing_table_row_count_matches_annotations_exactly(self):
        from flag_football_ep.charts.wp_review import select_wp_annotations

        game = _plain_game()
        annotations = select_wp_annotations(game)
        page = build_wp_review_page(game, game_id="g1")
        assert page.count("<tr") == annotations.height + 1

    def test_footnotes_contain_exact_provenance_string(self):
        from flag_football_ep.reports.wp_review import _wp_provenance_text

        game = _plain_game()
        n = game.height
        sources = ["oof"] * (n // 2) + ["champion"] * (n - n // 2)
        game = game.with_columns(wp_source=pl.Series(sources))
        expected = _wp_provenance_text(game)
        page = build_wp_review_page(game, game_id="g1")
        assert expected in page

    def test_summary_block_precedes_chart_block_in_document_order(self):
        page = build_wp_review_page(_plain_game(), game_id="g1")
        # search past the <style> block (which references both class names in the print
        # media query) so this asserts body content order, not CSS selector order.
        body_start = page.index("<body>")
        assert page.index('class="summary-block"', body_start) < page.index(
            'class="chart-block"', body_start
        )

    def test_game_id_with_markup_renders_escaped(self):
        game = _game_plays([{"play_id": 1, "home_wp": 0.5, "wpa": 0.01}])
        # only home_team/away_team drive game_label when both are present -- drop them so
        # the fallback path (bare game_id) is exercised.
        game = game.drop("home_team", "away_team")
        page = build_wp_review_page(game, game_id="<b>x</b>")
        assert "<b>x</b>" not in page
        assert "&lt;b&gt;x&lt;/b&gt;" in page

    def test_no_script_tags_and_no_external_references(self):
        page = build_wp_review_page(_plain_game(), game_id="g1")
        assert "<script" not in page
        assert 'src="http' not in page
        assert 'href="http' not in page


def _provenance_plays() -> pl.DataFrame:
    """A 4-play single-game frame carrying every column `attach_wp_provenance`'s pipeline
    (`prepare_wp_data` + `add_wp_variables`) needs: play 1 and 2 will be matched by the OOF
    fixture below, play 3 only by `scored`, play 4 by neither."""
    rows = []
    for play_id in range(1, 5):
        rows.append(
            {
                "game_id": "G1",
                "play_id": play_id,
                "half": 1,
                "score_differential": 0,
                "posteam": "HOME",
                "home_team": "HOME",
                "away_team": "AWAY",
                "home_team_score": 0,
                "away_team_score": 0,
                "touchdown": 0,
                "interception": 0,
                "one_point_conv_success": 0,
                "two_point_conv_success": 0,
                "safety": 0,
            }
        )
    return pl.DataFrame(rows)


def _write_oof_parquet(tmp_path, rows: list[dict]) -> None:
    pl.DataFrame(rows).write_parquet(tmp_path / "oof_predictions_wp.parquet")


class TestAttachWpProvenance:
    def _scored(self) -> pl.DataFrame:
        # play 2's champion values deliberately conflict with its OOF value (0.55) --
        # attach_wp_provenance must never let this reach a play the OOF join already matched.
        return pl.DataFrame(
            {
                "game_id": ["G1", "G1"],
                "play_id": [2, 3],
                "wp": [0.10, 0.42],
                "home_wp": [0.10, 0.42],
                "away_wp": [0.90, 0.58],
                "wpa": [0.20, -0.05],
            }
        )

    def test_oof_matched_play_gets_oof_source(self, tmp_path):
        _write_oof_parquet(
            tmp_path, {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        )
        result = attach_wp_provenance(
            _provenance_plays(), processed_dir=tmp_path, scored=self._scored()
        )
        row = result.filter(pl.col("play_id") == 1).row(0, named=True)
        assert row["wp_source"] == "oof"

    def test_champion_only_play_gets_champion_source(self, tmp_path):
        _write_oof_parquet(
            tmp_path, {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        )
        result = attach_wp_provenance(
            _provenance_plays(), processed_dir=tmp_path, scored=self._scored()
        )
        row = result.filter(pl.col("play_id") == 3).row(0, named=True)
        assert row["wp_source"] == "champion"
        assert row["wp"] == pytest.approx(0.42)

    def test_play_in_neither_source_gets_null_source_and_null_home_wp(self, tmp_path):
        _write_oof_parquet(
            tmp_path, {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        )
        result = attach_wp_provenance(
            _provenance_plays(), processed_dir=tmp_path, scored=self._scored()
        )
        row = result.filter(pl.col("play_id") == 4).row(0, named=True)
        assert row["wp_source"] is None
        assert row["home_wp"] is None

    def test_conflicting_values_take_the_oof_value(self, tmp_path):
        _write_oof_parquet(
            tmp_path, {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        )
        result = attach_wp_provenance(
            _provenance_plays(), processed_dir=tmp_path, scored=self._scored()
        )
        row = result.filter(pl.col("play_id") == 2).row(0, named=True)
        assert row["wp_source"] == "oof"
        assert row["wp"] == pytest.approx(0.55)  # not the scored 0.10

    def test_oof_derived_columns_match_add_wp_variables_directly(self, tmp_path):
        oof_rows = {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        _write_oof_parquet(tmp_path, oof_rows)
        result = attach_wp_provenance(
            _provenance_plays(), processed_dir=tmp_path, scored=self._scored()
        )

        # Independently derive the same two OOF rows through the production pipeline and
        # compare -- proves attach_wp_provenance did not hand-roll the arithmetic.
        context = (
            _provenance_plays()
            .filter(pl.col("play_id").is_in([1, 2]))
            .join(pl.DataFrame(oof_rows), on=["game_id", "play_id"], how="inner")
            .sort(["game_id", "play_id"])
        )
        expected = add_wp_variables(prepare_wp_data(context))

        for play_id, expected_home_wp, expected_wpa in zip(
            expected["play_id"].to_list(),
            expected["home_wp"].to_list(),
            expected["wpa"].to_list(),
        ):
            actual = result.filter(pl.col("play_id") == play_id).row(0, named=True)
            assert actual["home_wp"] == pytest.approx(expected_home_wp)
            if expected_wpa is None:
                assert actual["wpa"] is None
            else:
                assert actual["wpa"] == pytest.approx(expected_wpa)

    def test_row_count_and_order_preserved(self, tmp_path):
        _write_oof_parquet(
            tmp_path, {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        )
        plays = _provenance_plays()
        result = attach_wp_provenance(
            plays, processed_dir=tmp_path, scored=self._scored()
        )
        assert result.height == plays.height
        assert result["play_id"].to_list() == [1, 2, 3, 4]

    def test_missing_oof_file_raises_nothing_and_yields_champion_for_matched_scored_rows(
        self, tmp_path
    ):
        plays = _provenance_plays()
        assert not (tmp_path / "oof_predictions_wp.parquet").exists()
        result = attach_wp_provenance(
            plays, processed_dir=tmp_path, scored=self._scored()
        )
        assert result.height == plays.height
        row2 = result.filter(pl.col("play_id") == 2).row(0, named=True)
        row3 = result.filter(pl.col("play_id") == 3).row(0, named=True)
        assert row2["wp_source"] == "champion"
        assert row3["wp_source"] == "champion"

    def test_scored_none_with_present_oof_file_still_resolves_historical_rows(
        self, tmp_path
    ):
        _write_oof_parquet(
            tmp_path, {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        )
        plays = _provenance_plays()
        result = attach_wp_provenance(plays, processed_dir=tmp_path, scored=None)
        row1 = result.filter(pl.col("play_id") == 1).row(0, named=True)
        row3 = result.filter(pl.col("play_id") == 3).row(0, named=True)
        assert row1["wp_source"] == "oof"
        assert row1["wp"] == pytest.approx(0.60)
        assert row3["wp_source"] is None
        assert row3["home_wp"] is None

    def test_page_built_from_resolved_frame_names_actual_counts(self, tmp_path):
        _write_oof_parquet(
            tmp_path, {"game_id": ["G1", "G1"], "play_id": [1, 2], "wp": [0.60, 0.55]}
        )
        plays = _provenance_plays()
        resolved = attach_wp_provenance(
            plays, processed_dir=tmp_path, scored=self._scored()
        )
        page = build_wp_review_page(resolved, game_id="G1")
        # 2 oof (play 1, 2), 1 champion (play 3), 1 without a model value (play 4)
        assert "2 Plays out-of-fold" in page
        assert "1 Plays Champion-Modell" in page
        assert "1 Plays ohne Modellwert" in page
