"""Coverage for `flag_football_ep.reports.player_analysis.build_player_analysis_page` and its
template `player_analysis.html.j2` (HC-05): his columns next to ours, unavailable columns named
rather than hidden (never a silent zero), the Camp IV/VI conflict visible in the heading area,
escaped player names (autoescape proof -- no `|safe` anywhere) and the two M3-3 charts embedded
once per page, never once per section."""

from __future__ import annotations

from pathlib import Path

import matplotlib
import polars as pl
import pytest

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import flag_football_ep
from flag_football_ep.reports.aggregate import SectionBasis
from flag_football_ep.reports.player_analysis import (
    _HC_COLUMN_SCHEMA,
    _M3_COLUMN_SCHEMA,
    HcColumnTable,
    PlayerAnalysisReportData,
    PlayerAnalysisSplit,
    build_player_analysis_page,
)

_DEFINITION_KEYS: tuple[str, ...] = (
    "baseline_hc_workbook",
    "baseline_hc_verbal",
    "success_rate_epa",
    "explosive_epa_magnitude",
)

_COMPARISON_SCHEMA: dict[str, pl.DataType] = {
    "definition": pl.Utf8,
    "label_de": pl.Utf8,
    "rate": pl.Float64,
    "ci_low": pl.Float64,
    "ci_high": pl.Float64,
    "n": pl.Int64,
    "muted": pl.Boolean,
}

_CLIFF_SCHEMA: dict[str, pl.DataType] = {
    "yards_gained": pl.Int32,
    "n": pl.Int64,
    "share": pl.Float64,
    "hc_explosive": pl.Boolean,
}


def _basis(n_plays: int = 16) -> SectionBasis:
    if n_plays == 0:
        return SectionBasis((), (), 0, "Datenbasis: keine Daten")
    return SectionBasis(
        games=("g1",),
        sources=("hudl",),
        n_plays=n_plays,
        text=f"Datenbasis: 1 Spiel (hudl), {n_plays} Plays",
    )


def _comparison_table() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "definition": list(_DEFINITION_KEYS),
            "label_de": [
                "HC-Workbook Explosive % (Yards > 12, nur Pass)",
                "HC mündliche Regel (Yards > 12 oder EPA > 0, nur Pass)",
                "Success Rate (EPA > 0)",
                "Explosiveness (EPA-Magnitude auf Erfolgen)",
            ],
            "rate": [0.2, 0.25, 0.5, 0.3],
            "ci_low": [0.1, 0.15, 0.4, 0.2],
            "ci_high": [0.3, 0.35, 0.6, 0.4],
            "n": [20, 20, 50, 50],
            "muted": [False, False, False, False],
        },
        schema=_COMPARISON_SCHEMA,
    )


def _cliff_table() -> pl.DataFrame:
    yards = list(range(8, 17))
    return pl.DataFrame(
        {
            "yards_gained": yards,
            "n": [5] * len(yards),
            "share": [0.05] * len(yards),
            "hc_explosive": [y > 12 for y in yards],
        },
        schema=_CLIFF_SCHEMA,
    )


def _hc_row(spieler: str = "Schmidt", **overrides: object) -> dict:
    row = {
        "spieler": spieler,
        "comps": 10,
        "incs": 5,
        "attempts": 16,
        "tds": 2,
        "comp_pct": 0.625,
        "adj_comp_pct": 0.7,
        "ints": 1,
        "sacks": 1,
        "pass_yards": 150.0,
        "air_yards": 120.0,
        "ypa": 9.4,
        "adj_pass_yards": 160.0,
        "adj_ypa": 10.0,
        "exp_plays": 3,
        "explosive_pct": 0.1875,
        "efficiency": 0.6,
        "efficiency_drops": 0.55,
        "carries": 4,
        "rush_yards": 20.0,
        "rush_tds": 1,
        "muted": False,
    }
    row.update(overrides)
    return row


def _hc_table(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=_HC_COLUMN_SCHEMA)
    return pl.DataFrame(rows, schema=_HC_COLUMN_SCHEMA)


def _m3_row(spieler: str = "Schmidt", **overrides: object) -> dict:
    row: dict = {"spieler": spieler}
    for definition_key in _DEFINITION_KEYS:
        row[f"{definition_key}_rate"] = 0.4
        row[f"{definition_key}_n"] = 20
        row[f"{definition_key}_ci_low"] = 0.2
        row[f"{definition_key}_ci_high"] = 0.6
        row[f"{definition_key}_muted"] = False
        row[f"{definition_key}_shrunk_rate"] = 0.42
    row["explosive_score_mean"] = 1.23
    row.update(overrides)
    return row


def _m3_table(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=_M3_COLUMN_SCHEMA)
    return pl.DataFrame(rows, schema=_M3_COLUMN_SCHEMA)


def _hc_column_table(
    *, rows: list[dict] | None = None, unavailable: tuple[str, ...] = (), n_plays: int = 16
) -> HcColumnTable:
    rows = rows if rows is not None else [_hc_row()]
    return HcColumnTable(
        table=_hc_table(rows),
        unavailable=tuple(unavailable),
        notices=(),
        basis=_basis(n_plays),
    )


def _split(
    *,
    key: str = "korpus",
    heading: str = "Alle Camps (Korpus gesamt)",
    label_status: str = "n/a",
    hc_rows: list[dict] | None = None,
    m3_rows: list[dict] | None = None,
    unavailable: tuple[str, ...] = (),
    empty_notice: str | None = None,
    n_plays: int = 16,
) -> PlayerAnalysisSplit:
    hc_rows = hc_rows if hc_rows is not None else [_hc_row()]
    m3_rows = m3_rows if m3_rows is not None else [_m3_row()]
    return PlayerAnalysisSplit(
        key=key,
        heading=heading,
        label_status=label_status,
        columns=_hc_column_table(rows=hc_rows, unavailable=unavailable, n_plays=n_plays),
        m3_table=_m3_table(m3_rows),
        basis=_basis(n_plays),
        empty_notice=empty_notice,
    )


def _data(
    *,
    splits: tuple[PlayerAnalysisSplit, ...] | None = None,
    unmapped_players: tuple[str, ...] = (),
    notices: tuple[str, ...] = (),
    n_hc_rows: int = 3,
    comparison_table: pl.DataFrame | None = None,
    cliff_table: pl.DataFrame | None = None,
) -> PlayerAnalysisReportData:
    splits = splits if splits is not None else (_split(),)
    return PlayerAnalysisReportData(
        team="HOME",
        splits=tuple(splits),
        unresolved_games=(),
        unmapped_players=tuple(unmapped_players),
        notices=tuple(notices),
        n_hc_rows=n_hc_rows,
        overall_basis=_basis(16),
        corpus_comparison_table=(
            comparison_table if comparison_table is not None else _comparison_table()
        ),
        corpus_cliff_table=cliff_table if cliff_table is not None else _cliff_table(),
    )


class TestBuildPlayerAnalysisPage:
    def test_returns_non_empty_html_string_with_player_row(self) -> None:
        html = build_player_analysis_page(_data())

        assert isinstance(html, str)
        assert html.startswith("<!DOCTYPE html")
        assert "Schmidt" in html

    def test_unavailable_column_renders_named_state_never_zero_or_blank(self) -> None:
        html = build_player_analysis_page(
            _data(
                splits=(
                    _split(unavailable=("adj_comp_pct", "adj_pass_yards", "adj_ypa")),
                )
            )
        )

        assert html.count("nicht verfügbar (siehe Hinweise)") == 3

    def test_conflict_split_heading_and_marker_contain_both_camp_names(self) -> None:
        html = build_player_analysis_page(
            _data(
                splits=(
                    _split(
                        key="camp-iv-vi",
                        heading="Camp IV / Camp VI (unklar benannt)",
                        label_status="conflict",
                    ),
                )
            )
        )

        assert "Camp IV" in html
        assert "Camp VI" in html
        assert "Konflikt" in html

    def test_non_conflict_split_has_no_conflict_marker(self) -> None:
        html = build_player_analysis_page(_data(splits=(_split(label_status="verified"),)))

        assert "Konflikt" not in html

    def test_empty_split_renders_notice_and_no_table_body(self) -> None:
        html = build_player_analysis_page(
            _data(
                splits=(
                    _split(
                        hc_rows=[],
                        m3_rows=[],
                        empty_notice=(
                            "Abschnitt 'X': keine Daten im Korpus für diesen Abschnitt."
                        ),
                    ),
                )
            )
        )

        assert "keine Daten im Korpus für diesen Abschnitt" in html
        assert "<tbody>" not in html

    def test_player_name_with_script_tag_renders_escaped(self) -> None:
        html = build_player_analysis_page(
            _data(
                splits=(
                    _split(
                        hc_rows=[_hc_row("<script>x</script>")],
                        m3_rows=[_m3_row("<script>x</script>")],
                    ),
                )
            )
        )

        assert "<script>" not in html
        assert "&lt;script&gt;" in html

    def test_no_safe_filter_anywhere_in_template(self) -> None:
        template_path = (
            Path(flag_football_ep.__file__).parent / "templates" / "player_analysis.html.j2"
        )
        source = template_path.read_text(encoding="utf-8")

        assert "|safe" not in source

    def test_both_charts_embedded_once_per_page(self) -> None:
        html = build_player_analysis_page(_data())

        assert html.count("data:image/png;base64,") == 2

    def test_no_data_image_when_both_chart_renderers_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import flag_football_ep.reports.player_analysis as player_analysis_module

        def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("synthetic chart failure")

        monkeypatch.setattr(player_analysis_module, "render_definition_comparison", _boom)
        monkeypatch.setattr(player_analysis_module, "render_cliff_zone", _boom)

        html = build_player_analysis_page(_data())

        assert "data:image" not in html
        assert "synthetic chart failure" in html

    def test_one_chart_failure_still_embeds_the_other(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import flag_football_ep.reports.player_analysis as player_analysis_module

        def _boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("synthetic cliff failure")

        monkeypatch.setattr(player_analysis_module, "render_cliff_zone", _boom)

        html = build_player_analysis_page(_data())

        assert html.count("data:image/png;base64,") == 1
        assert "synthetic cliff failure" in html

    def test_closes_all_figures_after_call(self) -> None:
        before = set(plt.get_fignums())

        build_player_analysis_page(_data())

        assert set(plt.get_fignums()) == before

    def test_muted_row_carries_muted_cell_class(self) -> None:
        html = build_player_analysis_page(
            _data(splits=(_split(hc_rows=[_hc_row(muted=True, attempts=2)]),))
        )

        assert 'class="muted-cell"' in html

    def test_n_hc_rows_zero_is_stated_plainly(self) -> None:
        html = build_player_analysis_page(_data(n_hc_rows=0))

        assert "0 Zeile(n)" in html

    def test_unmapped_player_names_appear_in_output(self) -> None:
        html = build_player_analysis_page(_data(unmapped_players=("Xx Unbekannt",)))

        assert "Xx Unbekannt" in html

    def test_notices_appear_in_output(self) -> None:
        html = build_player_analysis_page(_data(notices=("Ein Test-Hinweis.",)))

        assert "Ein Test-Hinweis." in html

    def test_no_script_tag_and_no_external_resource_references(self) -> None:
        html = build_player_analysis_page(_data())

        assert "<script" not in html
        assert "http://" not in html
        assert "https://" not in html

    def test_contains_german_lang_attribute_inherited_from_base(self) -> None:
        html = build_player_analysis_page(_data())

        assert 'lang="de"' in html

    def test_title_names_the_team(self) -> None:
        html = build_player_analysis_page(_data())

        assert "Player Analysis" in html
        assert "HOME" in html

    def test_grep_style_no_filesystem_side_effects_in_source(self) -> None:
        import inspect

        from flag_football_ep.reports import player_analysis

        source = inspect.getsource(player_analysis)
        for forbidden in ("write_text", "savefig", "mkdir"):
            assert forbidden not in source
