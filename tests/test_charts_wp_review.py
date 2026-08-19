"""Coverage for `flag_football_ep.charts.wp_review` (REQ-S1-15): annotation selection and
chart rendering for the per-game win-probability review.

Chart tests assert on figure structure (axes, line data, axis labels, marker/annotation
positions) and file behaviour -- never on pixel content, per the plan's explicit
instruction.
"""

from __future__ import annotations

import re

import polars as pl

from flag_football_ep.charts.wp_review import (
    WP_ANNOTATION_MAX,
    WP_ANNOTATION_TOP_K,
    select_wp_annotations,
)

_PLAY_ID_RE = re.compile(r"^P\d+ ")


def _game_plays(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal one-game `game_plays` frame with the columns
    `select_wp_annotations`/`render_wp_review` require. Every row defaults every
    scoring/turnover flag to 0 and `home_team`/`away_team` to `HOME`/`AWAY`; `overrides`
    per row take precedence.
    """
    defaults = {
        "home_team": "HOME",
        "away_team": "AWAY",
        "posteam": "HOME",
        "touchdown": 0,
        "interception": 0,
        "one_point_conv_success": 0,
        "two_point_conv_success": 0,
        "safety": 0,
    }
    built = []
    for row in rows:
        merged = {**defaults, **row}
        built.append(merged)
    if not built:
        return pl.DataFrame(
            schema={
                "play_id": pl.Int32,
                "home_wp": pl.Float64,
                "wpa": pl.Float64,
                "posteam": pl.Utf8,
                "home_team": pl.Utf8,
                "away_team": pl.Utf8,
                "touchdown": pl.Int32,
                "interception": pl.Int32,
                "one_point_conv_success": pl.Int32,
                "two_point_conv_success": pl.Int32,
                "safety": pl.Int32,
            }
        )
    return pl.DataFrame(built)


def _plain_game(n_plays: int = 40) -> pl.DataFrame:
    """A 40-play game: play 5 and play 30 are big swings, plays 10/20 are a TD/INT with a
    tiny `wpa`, and 8 further scoring/turnover rows are scattered through so the
    marker-cap-vs-priority behaviour has something to bite on."""
    rows = []
    for play_id in range(1, n_plays + 1):
        home_wp = 0.5 + 0.005 * play_id
        wpa = 0.01
        row = {"play_id": play_id, "home_wp": home_wp, "wpa": wpa}
        rows.append(row)

    rows[4] = {**rows[4], "wpa": 0.35}  # play 5: big positive swing
    rows[29] = {**rows[29], "wpa": -0.40}  # play 30: big negative swing
    rows[9] = {**rows[9], "wpa": 0.001, "touchdown": 1}  # play 10: TD, tiny wpa
    rows[19] = {**rows[19], "wpa": -0.002, "interception": 1}  # play 20: INT, tiny wpa

    # 8 more scoring/turnover rows so scoring/turnover rows alone exceed WP_ANNOTATION_MAX.
    extra_score_plays = [2, 3, 12, 14, 16, 18, 22, 24]
    for play_id in extra_score_plays:
        rows[play_id - 1] = {**rows[play_id - 1], "wpa": 0.02, "touchdown": 1}

    return _game_plays(rows)


class TestSelectWpAnnotations:
    def test_top_k_largest_absolute_wpa_rows_are_selected(self):
        game = _game_plays(
            [
                {"play_id": 1, "home_wp": 0.5, "wpa": 0.01},
                {"play_id": 2, "home_wp": 0.5, "wpa": 0.30},
                {"play_id": 3, "home_wp": 0.5, "wpa": 0.02},
                {"play_id": 4, "home_wp": 0.5, "wpa": 0.25},
                {"play_id": 5, "home_wp": 0.5, "wpa": 0.03},
            ]
        )

        out = select_wp_annotations(game, top_k=2, max_markers=8)

        assert set(out["play_id"].to_list()) == {2, 4}

    def test_large_negative_wpa_is_selected_as_readily_as_positive(self):
        game = _game_plays(
            [
                {"play_id": 1, "home_wp": 0.5, "wpa": 0.01},
                {"play_id": 2, "home_wp": 0.5, "wpa": -0.30},
                {"play_id": 3, "home_wp": 0.5, "wpa": 0.02},
            ]
        )

        out = select_wp_annotations(game, top_k=1, max_markers=8)

        assert out["play_id"].to_list() == [2]

    def test_touchdown_row_with_tiny_wpa_is_still_selected(self):
        game = _game_plays(
            [
                {"play_id": 1, "home_wp": 0.5, "wpa": 0.01, "touchdown": 1},
                {"play_id": 2, "home_wp": 0.5, "wpa": 0.02},
                {"play_id": 3, "home_wp": 0.5, "wpa": 0.03},
            ]
        )

        out = select_wp_annotations(game, top_k=1, max_markers=8)

        assert 1 in out["play_id"].to_list()

    def test_result_never_exceeds_max_markers(self):
        game = _plain_game()

        out = select_wp_annotations(
            game, top_k=WP_ANNOTATION_TOP_K, max_markers=WP_ANNOTATION_MAX
        )

        assert out.height <= WP_ANNOTATION_MAX

    def test_scoring_row_survives_over_smaller_swing_only_row_when_capped(self):
        game = _plain_game()

        out = select_wp_annotations(
            game, top_k=WP_ANNOTATION_TOP_K, max_markers=WP_ANNOTATION_MAX
        )

        # play 10 is a scoring row with a tiny |wpa|; play 2/3/etc are also scoring rows with
        # a small |wpa|. All scoring/turnover rows here outnumber WP_ANNOTATION_MAX, so every
        # surviving row must be a scoring/turnover row -- a swing-only row must never survive
        # in preference to one of these.
        surviving_ids = set(out["play_id"].to_list())
        scoring_ids = {2, 3, 10, 12, 14, 16, 18, 20, 22, 24}
        assert surviving_ids.issubset(scoring_ids)

    def test_output_is_sorted_by_play_id(self):
        game = _plain_game()

        out = select_wp_annotations(game)

        play_ids = out["play_id"].to_list()
        assert play_ids == sorted(play_ids)

    def test_annotation_label_contains_play_id_event_tag_and_team(self):
        game = _game_plays(
            [
                {
                    "play_id": 7,
                    "home_wp": 0.6,
                    "wpa": 0.5,
                    "touchdown": 1,
                    "posteam": "GER",
                }
            ]
        )

        out = select_wp_annotations(game, top_k=1, max_markers=8)

        label = out["annotation_label"][0]
        assert "P7" in label
        assert "TD" in label
        assert "GER" in label

    def test_empty_input_returns_empty_frame_with_annotation_label_and_raises_nothing(
        self,
    ):
        game = _game_plays([])

        out = select_wp_annotations(game)

        assert out.height == 0
        assert "annotation_label" in out.columns

    def test_all_null_wpa_returns_empty_frame_and_raises_nothing(self):
        game = _game_plays(
            [
                {"play_id": 1, "home_wp": 0.5, "wpa": None},
                {"play_id": 2, "home_wp": 0.5, "wpa": None},
            ]
        )

        out = select_wp_annotations(game)

        assert out.height == 0
        assert "annotation_label" in out.columns

    def test_every_label_matches_play_id_prefix_pattern(self):
        game = _plain_game()

        out = select_wp_annotations(game)

        for label in out["annotation_label"].to_list():
            assert _PLAY_ID_RE.match(label)
