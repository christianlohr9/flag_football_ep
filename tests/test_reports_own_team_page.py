"""Coverage for `flag_football_ep.reports.own_team.build_own_team_page` and its template
`own_team_report.html.j2`: REQ-S1-13's rendered deliverable -- one standalone HTML file
stating its own EPA provenance, with the unmapped-player warning prominent above the tables
(T-1.4-53) and every charted player name escaped (T-1.4-51)."""

from __future__ import annotations

import matplotlib
import polars as pl

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from flag_football_ep.reports.aggregate import ReportSection, SectionBasis
from flag_football_ep.reports.own_team import OWN_TEAM_FILENAME, OwnTeamReportData, build_own_team_page


def _basis(n_plays: int = 10) -> SectionBasis:
    if n_plays == 0:
        return SectionBasis((), (), 0, "Datenbasis: keine Daten")
    return SectionBasis(
        games=("g1",), sources=("hudl",), n_plays=n_plays,
        text=f"Datenbasis: 1 Spiel (hudl), {n_plays} Plays",
    )


def _call_section() -> ReportSection:
    table = pl.DataFrame(
        {
            "dimension": ["off_form", "off_play", "off_play", "target_route"],
            "wert": ["Trips", "Screen", "Slant", "Go"],
            "n_cycle": [10, 8, 3, 12],
            "epa_play_cycle": [0.15, 0.4, -0.2, 0.05],
            "n_alltime": [20, 15, 6, 25],
            "epa_play_alltime": [0.1, 0.35, -0.15, 0.02],
            "muted": [False, False, True, False],
        }
    )
    return ReportSection(
        key="efficiency_by_call",
        heading="EPA/Play nach Formation, Play-Call und Route",
        table=table,
        basis=_basis(50),
        empty_notice=None,
    )


def _empty_call_section() -> ReportSection:
    schema = {
        "dimension": pl.Utf8, "wert": pl.Utf8, "n_cycle": pl.Int64,
        "epa_play_cycle": pl.Float64, "n_alltime": pl.Int64,
        "epa_play_alltime": pl.Float64, "muted": pl.Boolean,
    }
    return ReportSection(
        key="efficiency_by_call",
        heading="EPA/Play nach Formation, Play-Call und Route",
        table=pl.DataFrame(schema=schema),
        basis=_basis(0),
        empty_notice="Kein Charting-Material für diese Auswertung vorhanden.",
    )


def _player_section(receiver_name: str = "Meier") -> ReportSection:
    table = pl.DataFrame(
        {
            "rolle": ["QB", "Receiver"],
            "spieler": ["Schmidt", receiver_name],
            "n_cycle": [20, 15],
            "epa_play_cycle": [0.2, 0.3],
            "n_alltime": [40, 30],
            "epa_play_alltime": [0.18, 0.28],
            "completion_n": [20, None],
            "completion_rate": [0.65, None],
            "completion_ci_low": [0.4, None],
            "completion_ci_high": [0.85, None],
            "yac_summe": [None, 120],
            "yac_schnitt": [None, 8.0],
            "yac_anteil": [None, 0.55],
            "muted": [False, False],
        }
    )
    return ReportSection(
        key="player_efficiency",
        heading="EPA pro QB und Receiver, YAC-Anteile",
        table=table,
        basis=_basis(35),
        empty_notice=None,
    )


def _empty_player_section() -> ReportSection:
    schema = {
        "rolle": pl.Utf8, "spieler": pl.Utf8, "n_cycle": pl.Int64,
        "epa_play_cycle": pl.Float64, "n_alltime": pl.Int64, "epa_play_alltime": pl.Float64,
        "completion_n": pl.Int64, "completion_rate": pl.Float64,
        "completion_ci_low": pl.Float64, "completion_ci_high": pl.Float64,
        "yac_summe": pl.Int64, "yac_schnitt": pl.Float64, "yac_anteil": pl.Float64,
        "muted": pl.Boolean,
    }
    return ReportSection(
        key="player_efficiency",
        heading="EPA pro QB und Receiver, YAC-Anteile",
        table=pl.DataFrame(schema=schema),
        basis=_basis(0),
        empty_notice="Kein Charting-Material für diese Auswertung vorhanden.",
    )


def _drive_section() -> ReportSection:
    table = pl.DataFrame(
        {
            "drive_outcome": ["TD", "Turnover", "Downs"],
            "n": [10, 5, 3],
            "group_n": [18, 18, 18],
            "share": [10 / 18, 5 / 18, 3 / 18],
            "ci_low": [0.3, 0.1, 0.05],
            "ci_high": [0.7, 0.4, 0.3],
            "muted": [False, False, True],
            "punkte_pro_drive": [3.5, 3.5, 3.5],
            "drive_count": [18, 18, 18],
        }
    )
    return ReportSection(
        key="drive_outcomes",
        heading="Drive Success: Punkte pro Drive und Drive-Ausgänge",
        table=table,
        basis=_basis(60),
        empty_notice=None,
    )


def _empty_drive_section() -> ReportSection:
    schema = {
        "drive_outcome": pl.Utf8, "n": pl.Int64, "group_n": pl.Int64, "share": pl.Float64,
        "ci_low": pl.Float64, "ci_high": pl.Float64, "muted": pl.Boolean,
        "punkte_pro_drive": pl.Float64, "drive_count": pl.Int64,
    }
    return ReportSection(
        key="drive_outcomes",
        heading="Drive Success: Punkte pro Drive und Drive-Ausgänge",
        table=pl.DataFrame(schema=schema),
        basis=_basis(0),
        empty_notice="Keine Drives vorhanden.",
    )


def _defense_section() -> ReportSection:
    table = pl.DataFrame(
        {
            "dimension": ["def_front", "coverage"],
            "wert": ["4-1", "Man"],
            "n": [12, 9],
            "epa_play_allowed": [-0.1, 0.05],
            "muted": [False, True],
            "interceptions_n": [2, 2],
            "def_touchdowns_n": [1, 1],
        }
    )
    return ReportSection(
        key="defense_section",
        heading="Defense: EPA/Play erlaubt nach DEF FRONT und COVERAGE (niedriger ist besser)",
        table=table,
        basis=_basis(21),
        empty_notice=None,
    )


def _empty_defense_section() -> ReportSection:
    schema = {
        "dimension": pl.Utf8, "wert": pl.Utf8, "n": pl.Int64, "epa_play_allowed": pl.Float64,
        "muted": pl.Boolean, "interceptions_n": pl.Int64, "def_touchdowns_n": pl.Int64,
    }
    return ReportSection(
        key="defense_section",
        heading="Defense: EPA/Play erlaubt nach DEF FRONT und COVERAGE (niedriger ist besser)",
        table=pl.DataFrame(schema=schema),
        basis=_basis(0),
        empty_notice="Kein Charting-Material für diese Auswertung vorhanden.",
    )


def _full_data(
    *,
    unmapped_players: tuple[str, ...] = ("Xx Unbekannt",),
    notices: tuple[str, ...] = (),
    receiver_name: str = "Meier",
) -> OwnTeamReportData:
    return OwnTeamReportData(
        team="HOME",
        cycle_start_season=2026,
        sections=(
            _call_section(),
            _player_section(receiver_name=receiver_name),
            _drive_section(),
            _defense_section(),
        ),
        unmapped_players=unmapped_players,
        overall_basis=_basis(80),
        notices=notices,
        n_epa_oof=120,
        n_epa_champion=15,
        n_epa_none=3,
        overall_epa_play_cycle=0.12,
        overall_epa_play_n_cycle=80,
    )


def _fully_empty_data() -> OwnTeamReportData:
    return OwnTeamReportData(
        team="HOME",
        cycle_start_season=2026,
        sections=(
            _empty_call_section(),
            _empty_player_section(),
            _empty_drive_section(),
            _empty_defense_section(),
        ),
        unmapped_players=(),
        overall_basis=_basis(0),
        notices=("Keine Offense-Plays für 'HOME' im Korpus gefunden.",),
        n_epa_oof=0,
        n_epa_champion=0,
        n_epa_none=0,
        overall_epa_play_cycle=None,
        overall_epa_play_n_cycle=0,
    )


class TestBuildOwnTeamPageAssembler:
    def test_own_team_filename_constant(self):
        assert OWN_TEAM_FILENAME == "own-team.html"

    def test_returns_html_string_starting_with_doctype(self):
        html = build_own_team_page(_full_data())

        assert isinstance(html, str)
        assert html.startswith("<!DOCTYPE html")

    def test_embeds_one_chart_per_non_empty_charted_section(self):
        # call (off_play rows present), player (receiver row present), drive_outcomes: 3
        # charts. defense_section never charts (table-only per plan).
        html = build_own_team_page(_full_data())

        assert html.count("data:image/png;base64,") == 3

    def test_empty_charted_sections_contribute_no_chart(self):
        html = build_own_team_page(_fully_empty_data())

        assert html.count("data:image/png;base64,") == 0

    def test_closes_all_figures_after_call(self):
        before = set(plt.get_fignums())

        build_own_team_page(_full_data())

        assert set(plt.get_fignums()) == before

    def test_unmapped_player_names_appear_in_output(self):
        html = build_own_team_page(_full_data(unmapped_players=("Xx Unbekannt", "Yy Fremd")))

        assert "Xx Unbekannt" in html
        assert "Yy Fremd" in html

    def test_provenance_sentence_names_both_oof_and_champion_counts(self):
        html = build_own_team_page(_full_data())

        assert "120" in html  # n_epa_oof
        assert "15" in html  # n_epa_champion
        assert "Out-of-fold" in html
        assert "Champion" in html

    def test_fully_empty_data_object_renders_and_raises_nothing(self):
        html = build_own_team_page(_fully_empty_data())

        assert html.startswith("<!DOCTYPE html")
        assert "Keine Offense-Plays für" in html
        assert "HOME" in html
        assert "im Korpus gefunden." in html

    def test_no_script_tag_and_no_external_resource_references(self):
        html = build_own_team_page(_full_data())

        assert "<script" not in html
        assert "http://" not in html
        assert "https://" not in html

    def test_grep_style_no_filesystem_side_effects_in_source(self):
        import inspect

        from flag_football_ep.reports import own_team

        source = inspect.getsource(own_team)
        for forbidden in ("write_text", "savefig", "mkdir"):
            assert forbidden not in source


class TestOwnTeamReportTemplate:
    def test_unmapped_warning_appears_before_first_h2(self):
        html = build_own_team_page(_full_data(unmapped_players=("Xx Unbekannt",)))

        warning_index = html.index('class="warning-block"')
        first_h2_index = html.index("<h2>")

        assert warning_index < first_h2_index

    def test_no_unmapped_block_when_empty(self):
        html = build_own_team_page(_full_data(unmapped_players=()))

        assert "Nicht zugeordnete Spielerinnen-Namen" not in html

    def test_player_name_with_script_tag_renders_escaped(self):
        html = build_own_team_page(_full_data(receiver_name="<script>x</script>"))

        assert "<script" not in html
        assert "&lt;script&gt;" in html

    def test_muted_cells_carry_muted_cell_class(self):
        html = build_own_team_page(_full_data())

        assert 'class="muted-cell"' in html

    def test_footnotes_contain_cycle_definition_and_both_provenance_counts(self):
        html = build_own_team_page(_full_data())

        footer_start = html.index("<footer>")
        footer = html[footer_start:]

        assert "Saison 2026" in footer
        assert "Out-of-fold" in footer
        assert "Champion" in footer

    def test_defense_heading_states_lower_is_better(self):
        html = build_own_team_page(_full_data())

        assert "niedriger ist besser" in html

    def test_contains_german_lang_attribute_inherited_from_base(self):
        html = build_own_team_page(_full_data())

        assert 'lang="de"' in html

    def test_title_names_the_team(self):
        html = build_own_team_page(_full_data())

        assert "Eigene Effizienz" in html
        assert "HOME" in html
