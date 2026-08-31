"""Unit coverage for `flag_football_ep.reports.opponent` (REQ-S1-12).

Every test builds inputs with `flag_football_ep.testing.canonical_plays` plus targeted
`overrides`/`extras`, matching the class-per-function layout of `tests/test_features_mutations.py`
and `tests/test_reports_aggregate.py`.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.reports.aggregate import MUTED_MIN_N
from flag_football_ep.reports.opponent import (
    OpponentReportData,
    ReportSection,
    build_opponent_data,
    formation_tendencies,
    fourth_down_and_pat_behavior,
    playcall_by_score_state,
    route_distribution,
    top_tendency_sentences,
)
from flag_football_ep.testing import canonical_plays


class TestFormationTendencies:
    def test_shares_sum_to_one_within_group(self) -> None:
        n = 8
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "defteam": ["BBB"] * n,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "yardline_50": [5] * n,
                "play_type": ["pass"] * 5 + ["run"] * 3,
            },
            extras={"off_form": ["Trips"] * n},
        )
        section = formation_tendencies(df, team="AAA")
        shares = section.table.group_by(["off_form", "down", "distance_bucket", "field_zone"]).agg(
            total_share=pl.col("share").sum()
        )
        for value in shares["total_share"].to_list():
            assert value == pytest.approx(1.0)

    def test_only_posteam_team_rows_counted(self) -> None:
        n = 8
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA", "BBB"] * 4,
                "defteam": ["BBB", "AAA"] * 4,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "yardline_50": [5] * n,
            },
            extras={"off_form": ["Trips", "Spread"] * 4},
        )
        section = formation_tendencies(df, team="AAA")
        assert set(section.table["off_form"].to_list()) == {"Trips"}
        assert section.table["n"].sum() == 4

    def test_null_off_form_excluded_without_raising(self) -> None:
        n = 6
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "yardline_50": [5] * n,
            },
            extras={"off_form": ["Trips", "Trips", "Trips", None, None, None]},
        )
        section = formation_tendencies(df, team="AAA")
        assert section.table["n"].sum() == 3
        assert None not in section.table["off_form"].to_list()

    def test_fully_uncharted_frame_yields_empty_section(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=6, overrides={"posteam": ["AAA"] * 6})
        section = formation_tendencies(df, team="AAA")
        assert section.table.height == 0
        assert section.empty_notice is not None
        assert section.basis.n_plays == 0

    def test_muted_below_threshold_group(self) -> None:
        small_n = MUTED_MIN_N - 2
        large_n = MUTED_MIN_N + 3
        n = small_n + large_n
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1] * small_n + [2] * large_n,
                "yards_to_go": [2] * n,
                "yardline_50": [5] * n,
            },
            extras={"off_form": ["Trips"] * small_n + ["Spread"] * large_n},
        )
        section = formation_tendencies(df, team="AAA")
        small_group = section.table.filter(pl.col("off_form") == "Trips")
        large_group = section.table.filter(pl.col("off_form") == "Spread")
        assert bool(small_group["muted"].to_list()[0]) is True
        assert bool(large_group["muted"].to_list()[0]) is False


class TestRouteDistribution:
    def test_two_subtables_combined_with_gruppierung(self) -> None:
        n = 6
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "yardline_50": [5] * n,
            },
            extras={"target_route": ["Slant"] * n},
        )
        section = route_distribution(df, team="AAA")
        assert set(section.table["gruppierung"].to_list()) == {"Down & Distance", "Feldzone"}
        assert section.heading == "Ziel-Routen-Verteilung"

    def test_fully_uncharted_frame_yields_empty_section(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=6, overrides={"posteam": ["AAA"] * 6})
        section = route_distribution(df, team="AAA")
        assert section.table.height == 0
        assert section.empty_notice is not None
        assert section.basis.n_plays == 0

    def test_only_posteam_team_rows_counted(self) -> None:
        n = 8
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA", "BBB"] * 4,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "yardline_50": [5] * n,
            },
            extras={"target_route": ["Slant", "Go"] * 4},
        )
        section = route_distribution(df, team="AAA")
        assert set(section.table["target_route"].to_list()) == {"Slant"}


class TestPlaycallByScoreState:
    def test_score_state_matches_differential_boundaries(self) -> None:
        n = 4
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "score_differential": [-7, -6, 0, 7],
            },
        )
        section = playcall_by_score_state(df, team="AAA")
        states = set(section.table["score_state"].to_list())
        assert states.issubset({"Rückstand", "Ausgeglichen", "Führung", "unbekannt"})
        assert "Rückstand" in states
        assert "Führung" in states
        assert "Ausgeglichen" in states

    def test_null_score_differential_becomes_unbekannt_and_is_not_dropped(self) -> None:
        n = 4
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "score_differential": [0, None, 0, None],
            },
        )
        section = playcall_by_score_state(df, team="AAA")
        assert "unbekannt" in section.table["score_state"].to_list()
        assert section.table["n"].sum() == n

    def test_pat_rows_excluded(self) -> None:
        n = 4
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1, 0, 1, 0],
                "yards_to_go": [2, 3, 2, 8],
            },
        )
        section = playcall_by_score_state(df, team="AAA")
        assert section.table["n"].sum() == 2
        assert 0 not in section.table["down"].to_list()

    def test_total_play_count_reconciles_with_non_pat_rows(self) -> None:
        n = 6
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1, 1, 2, 0, 3, 0],
                "yards_to_go": [2] * n,
            },
        )
        section = playcall_by_score_state(df, team="AAA")
        non_pat_count = df.filter(pl.col("down") != 0).height
        assert section.table["n"].sum() == non_pat_count


class TestFourthDownAndPatBehavior:
    def test_touchdown_without_first_down_counts_as_conversion(self) -> None:
        n = 1
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"],
                "down": [4],
                "yards_to_go": [2],
                "play_type": ["run"],
                "penalty": [0],
                "first_down": [0],
                "touchdown": [1],
            },
        )
        section = fourth_down_and_pat_behavior(df, team="AAA")
        conversion_row = section.table.filter(
            (pl.col("kennzahl") == "4th-Down Conversion-Rate")
            & (pl.col("bucket") == "Short 1-3")
        )
        assert conversion_row["n"].to_list() == [1]
        assert conversion_row["successes"].to_list() == [1]

    def test_zero_fourth_down_snaps_yields_padded_rows_not_omitted(self) -> None:
        n = 4
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1, 2, 3, 1] ,
                "yards_to_go": [2] * n,
            },
        )
        section = fourth_down_and_pat_behavior(df, team="AAA")
        go_rate_rows = section.table.filter(pl.col("kennzahl") == "4th-Down Go-Rate")
        assert go_rate_rows.height == 4
        assert set(go_rate_rows["n"].to_list()) == {0}
        assert all(r is None for r in go_rate_rows["rate"].to_list())

    def test_zero_pat_attempts_does_not_raise(self) -> None:
        n = 3
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1, 2, 3],
                "yards_to_go": [2] * n,
            },
        )
        section = fourth_down_and_pat_behavior(df, team="AAA")
        pat_rows = section.table.filter(pl.col("kennzahl") == "PAT-Erfolgsquote")
        assert pat_rows.height == 2
        assert set(pat_rows["n"].to_list()) == {0}

    def test_pat_choice_and_success_use_verbatim_predicates(self) -> None:
        n = 3
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [0, 0, 0],
                "yards_to_go": [3, 8, 3],
                "one_point_conv_success": [1, 0, 0],
                "two_point_conv_success": [0, 1, 0],
            },
        )
        section = fourth_down_and_pat_behavior(df, team="AAA")
        pat_success = section.table.filter(pl.col("kennzahl") == "PAT-Erfolgsquote")
        one_point_row = pat_success.filter(pl.col("bucket") == "1-Punkt")
        two_point_row = pat_success.filter(pl.col("bucket") == "2-Punkt")
        assert one_point_row["n"].to_list() == [2]
        assert one_point_row["successes"].to_list() == [1]
        assert two_point_row["n"].to_list() == [1]
        assert two_point_row["successes"].to_list() == [1]


class TestTopTendencySentences:
    def _formation_section(self, table: pl.DataFrame) -> ReportSection:
        from flag_football_ep.reports.aggregate import section_basis

        return ReportSection(
            key="formation_tendencies",
            heading="Formation × Down & Distance × Feldzone",
            table=table,
            basis=section_basis(pl.DataFrame({"game_id": ["g1"] * table.height, "source": ["hudl"] * table.height})),
            empty_notice=None,
        )

    def test_no_sentence_from_cell_below_muted_min_n(self) -> None:
        table = pl.DataFrame(
            {
                "off_form": ["Trips"],
                "down": [1],
                "distance_bucket": ["Short 1-3"],
                "field_zone": ["Red Zone"],
                "play_type": ["pass"],
                "n": [MUTED_MIN_N - 1],
                "group_n": [MUTED_MIN_N - 1],
                "share": [1.0],
                "ci_low": [0.1],
                "ci_high": [1.0],
                "muted": [True],
            }
        )
        section = self._formation_section(table)
        sentences = top_tendency_sentences((section,), team="AAA")
        assert sentences == ("Zu dünn für verlässliche Tendenzen (n=1).",)

    def test_every_sentence_contains_n_equals(self) -> None:
        n = MUTED_MIN_N + 5
        table = pl.DataFrame(
            {
                "off_form": ["Trips", "Trips"],
                "down": [1, 1],
                "distance_bucket": ["Short 1-3", "Short 1-3"],
                "field_zone": ["Red Zone", "Red Zone"],
                "play_type": ["pass", "run"],
                "n": [n, 1],
                "group_n": [n + 1, n + 1],
                "share": [0.9, 0.1],
                "ci_low": [0.5, 0.0],
                "ci_high": [1.0, 0.5],
                "muted": [False, False],
            }
        )
        section = self._formation_section(table)
        sentences = top_tendency_sentences((section,), team="AAA")
        assert len(sentences) > 0
        for sentence in sentences:
            assert "n=" in sentence

    def test_at_most_two_sentences_per_section(self) -> None:
        n = MUTED_MIN_N + 5
        table = pl.DataFrame(
            {
                "off_form": ["A", "B", "C"],
                "down": [1, 2, 3],
                "distance_bucket": ["Short 1-3", "Medium 4-6", "Long 7-10"],
                "field_zone": ["Red Zone", "Mittelfeld", "Eigene Hälfte"],
                "play_type": ["pass", "run", "pass"],
                "n": [n, n, n],
                "group_n": [n, n, n],
                "share": [1.0, 1.0, 1.0],
                "ci_low": [0.5, 0.5, 0.5],
                "ci_high": [1.0, 1.0, 1.0],
                "muted": [False, False, False],
            }
        )
        section = self._formation_section(table)
        sentences = top_tendency_sentences((section,), team="AAA", max_sentences=5)
        assert len(sentences) <= 2

    def test_thin_corpus_yields_exactly_one_sentence_with_total_count(self) -> None:
        from flag_football_ep.reports.aggregate import section_basis

        table = pl.DataFrame(
            {
                "off_form": ["Trips"],
                "down": [1],
                "distance_bucket": ["Short 1-3"],
                "field_zone": ["Red Zone"],
                "play_type": ["pass"],
                "n": [2],
                "group_n": [2],
                "share": [1.0],
                "ci_low": [0.1],
                "ci_high": [1.0],
                "muted": [True],
            }
        )
        basis = section_basis(
            pl.DataFrame({"game_id": ["g1", "g1"], "source": ["hudl", "hudl"]})
        )
        section = ReportSection(
            key="formation_tendencies",
            heading="Formation × Down & Distance × Feldzone",
            table=table,
            basis=basis,
            empty_notice=None,
        )
        sentences = top_tendency_sentences((section,), team="AAA")
        assert len(sentences) == 1
        assert "zu dünn" in sentences[0].lower()
        assert "n=2" in sentences[0]


class TestBuildOpponentData:
    def test_team_absent_from_corpus_returns_empty_sections_without_raising(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=6, overrides={"posteam": ["AAA"] * 6})
        data = build_opponent_data(df, team="NOT_IN_CORPUS", team_name="x")
        assert isinstance(data, OpponentReportData)
        for section in data.sections:
            if section.key in ("formation_tendencies", "route_distribution"):
                assert section.table.height == 0
        assert len(data.notices) > 0

    def test_overall_basis_n_plays_matches_posteam_row_count(self) -> None:
        n = 8
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={"posteam": ["AAA", "BBB"] * 4},
        )
        data = build_opponent_data(df, team="AAA", team_name="Team AAA")
        assert data.overall_basis.n_plays == df.filter(pl.col("posteam") == "AAA").height

    def test_deterministic_summary_sentences_across_calls(self) -> None:
        n = 8
        df = canonical_plays(
            n_games=1,
            plays_per_game=n,
            overrides={
                "posteam": ["AAA"] * n,
                "down": [1] * n,
                "yards_to_go": [2] * n,
                "yardline_50": [5] * n,
            },
            extras={"off_form": ["Trips"] * n},
        )
        data1 = build_opponent_data(df, team="AAA", team_name="Team AAA")
        data2 = build_opponent_data(df, team="AAA", team_name="Team AAA")
        assert data1.summary_sentences == data2.summary_sentences

    def test_zero_plays_team_returns_keine_daten_summary_and_notice(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"posteam": ["AAA"] * 4})
        data = build_opponent_data(df, team="ZZZ", team_name="Team ZZZ")
        assert len(data.summary_sentences) == 1
        assert "keine daten" in data.summary_sentences[0].lower()
        assert any("keine daten" in notice.lower() for notice in data.notices)
