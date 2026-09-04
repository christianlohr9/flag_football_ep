"""Unit coverage for `flag_football_ep.reports.player_analysis`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays` plus targeted
`overrides`/`extras`, matching `tests/test_reports_own_team.py`'s fixture style. Each test is
named after the `<behavior>` bullet it proves, cited to the formula cell it reproduces
(`M3-04-RESEARCH.md` Pattern 1).
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.config import (
    Config,
    CvSettings,
    IfafSource,
    Paths,
    ReferenceFiles,
    ReportSettings,
    Sources,
    SportappSource,
    TrainSettings,
)
from flag_football_ep.features.explosiveness import (
    CALIBRATION_SCHEMA_VERSION,
    DEFINITIONS,
    ExplosivenessCalibration,
    definition_comparison,
    write_calibration,
)
from flag_football_ep.reports.player_analysis import (
    HcColumnTable,
    PlayerAnalysisReportData,
    PlayerAnalysisSplit,
    _HC_COLUMN_SCHEMA,
    _M3_COLUMN_SCHEMA,
    build_player_analysis_data,
    hc_columns_by_qb,
    load_report_calibration,
    m3_columns_by_qb,
)
from flag_football_ep.testing import canonical_plays

_HOME = "HOME"
_AWAY = "AWAY"


def _calibration(*, threshold: float = 2.0, iqr: float = 1.0) -> ExplosivenessCalibration:
    """A hand-built `ExplosivenessCalibration` for tests that only need the flag/score math --
    mirrors `tests/test_features_explosiveness.py::_make_calibration`.
    """
    return ExplosivenessCalibration(
        schema_version=CALIBRATION_SCHEMA_VERSION,
        epa_quantile=0.80,
        epa_threshold=threshold,
        epa_median_success=threshold * 0.5,
        epa_iqr_success=iqr,
        corpus_n=100,
        n_success=50,
        corpus_sources=("hudl",),
        corpus_fingerprint="deadbeef",
        calibrated_on="2026-09-03T00:00:00+00:00",
    )


def _make_config(tmp_path: Path) -> Config:
    """Mirrors `tests/test_reports_own_team.py::_make_config` exactly, plus explicit
    `hc_games`/`hc_splits` reference paths (own_team.py's helper leaves them at their
    pre-M3-04 defaults, which point outside `tmp_path`) so every test controls its own
    reference-file presence/absence.
    """
    paths = Paths(
        data_root=tmp_path,
        raw_hudl=tmp_path / "raw_hudl",
        raw_sportapp=tmp_path / "raw_sportapp",
        raw_ifaf=tmp_path / "raw_ifaf",
        raw_legacy=tmp_path / "raw_legacy",
        processed=tmp_path / "processed",
        reference=tmp_path / "reference",
        models=tmp_path / "models",
        mlruns=tmp_path / "mlruns",
        contract=tmp_path / "contract.json",
        reports=tmp_path / "reports",
        video=tmp_path / "video",
        labels=tmp_path / "labels",
        tracking=tmp_path / "processed" / "tracking",
    )
    reference = ReferenceFiles(
        half_boundaries=tmp_path / "half_boundaries.csv",
        final_scores=tmp_path / "final_scores.csv",
        team_mapping=tmp_path / "team_mapping.csv",
        sportapp_games=tmp_path / "sportapp_games.csv",
        competition_tier=tmp_path / "competition_tier.csv",
        player_mapping=tmp_path / "player_mapping.csv",
        group_opponents=tmp_path / "group_opponents.csv",
        hover_positions=tmp_path / "hover_positions.csv",
        homography_calibration=tmp_path / "homography_calibration.csv",
        gt_positions=tmp_path / "gt_positions.csv",
        continuity_review=tmp_path / "continuity_review.csv",
        hc_games=tmp_path / "hc_games.csv",
        hc_splits=tmp_path / "hc_splits.csv",
    )
    sources = Sources(
        sportapp=SportappSource(base_url="https://x", api_key_env="X"),
        ifaf=IfafSource(base_url="https://x", tournament="x", api_key_env="X"),
    )
    train = TrainSettings(
        ep_experiment="ep", wp_experiment="wp", exclude_games_ep=[], exclude_games_wp=[]
    )
    report = ReportSettings(own_team=_HOME, cycle_start_season=2026)
    cv = CvSettings(
        pilot_session_id="test-session",
        detector_model="cv_detector_model_test",
        detector_experiment="cv_detector_test",
        resolution=672,
        sahi=False,
        sahi_slice=640,
        sahi_overlap=0.2,
        train_epochs=1,
        train_batch_size=4,
        train_grad_accum=4,
        device="cpu",
        label_frame_target=10,
        cvat_host="http://localhost:8080",
        cvat_username_env="CVAT_USERNAME",
        cvat_password_env="CVAT_PASSWORD",
        field_length_yards=50.0,
        field_width_yards=25.0,
        endzone_yards=10.0,
        dvc_remote_name="otc-obs",
        dvc_remote_url="s3://test-bucket/flag-football-datasets",
        dvc_remote_endpoint="https://obs.eu-de.otc.t-systems.com",
        otc_obs_access_key_env="OTC_OBS_ACCESS_KEY_ID",
        otc_obs_secret_key_env="OTC_OBS_SECRET_ACCESS_KEY",
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


def _assert_matches(actual, expected) -> None:
    if expected is None:
        assert actual is None
    else:
        assert actual == pytest.approx(expected)


# --- Synthetic hc_games.csv / hc_splits.csv builders (never the real committed files, per
# `M3-04-04-PLAN.md`'s file-collision guard: `hc_games.csv` may be in flux under a concurrent
# plan, and this module's own tests must not depend on its content) ---------------------------

_WORKBOOK = "wb1"
_SHEET = "data"

_HC_GAMES_COLUMNS: tuple[str, ...] = (
    "workbook",
    "sheet",
    "block_key",
    "source_team1",
    "source_team2",
    "game_id",
    "home_team",
    "away_team",
    "competition",
    "season",
    "game_date",
    "tier",
    "corpus_game_id",
    "note",
)

_HC_SPLITS_COLUMNS: tuple[str, ...] = (
    "workbook",
    "sheet",
    "first_row",
    "last_row",
    "split_key",
    "label_de",
    "label_status",
    "source_tabs",
    "note",
)


def _hc_game_row(
    game_id: str, *, note: str, workbook: str = _WORKBOOK, sheet: str = _SHEET
) -> dict:
    return {
        "workbook": workbook,
        "sheet": sheet,
        "block_key": f"b01-{game_id}",
        "source_team1": "",
        "source_team2": "",
        "game_id": game_id,
        "home_team": "GER",
        "away_team": "OPP",
        "competition": "HC Camps 2026",
        "season": 2026,
        "game_date": "",
        "tier": "womens-national",
        "corpus_game_id": "",
        "note": note,
    }


def _write_hc_games(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        frame = pl.DataFrame(rows, schema_overrides={"season": pl.Int32}).select(
            list(_HC_GAMES_COLUMNS)
        )
    else:
        frame = pl.DataFrame(schema={c: pl.Utf8 for c in _HC_GAMES_COLUMNS})
    frame.write_csv(path)


def _hc_split_row(
    split_key: str,
    *,
    first_row: int,
    last_row: int,
    label_de: str,
    label_status: str = "verified",
    workbook: str = _WORKBOOK,
    sheet: str = _SHEET,
) -> dict:
    return {
        "workbook": workbook,
        "sheet": sheet,
        "first_row": first_row,
        "last_row": last_row,
        "split_key": split_key,
        "label_de": label_de,
        "label_status": label_status,
        "source_tabs": "x",
        "note": "synthetic test window",
    }


def _write_hc_splits(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        frame = pl.DataFrame(
            rows, schema_overrides={"first_row": pl.Int32, "last_row": pl.Int32}
        ).select(list(_HC_SPLITS_COLUMNS))
    else:
        frame = pl.DataFrame(schema={c: pl.Utf8 for c in _HC_SPLITS_COLUMNS})
    frame.write_csv(path)


def _hc_plays(*, game_id: str, n_plays: int, thrown_by: str = "QB1") -> pl.DataFrame:
    """`n_plays` completed pass plays for the own team (`posteam == "HOME"`), `source ==
    "hc_workbook"`, on a caller-chosen `game_id` -- so a synthetic `hc_games.csv` row can
    reference it by exact value (auto-derived `make_game_id("hc_workbook", ...)` ids never
    start with the `"hc-"` prefix `load_hc_games` requires, so this override is deliberate,
    not incidental).
    """
    df = canonical_plays(
        n_games=1,
        plays_per_game=n_plays,
        source="hc_workbook",
        overrides={
            "game_id": [game_id] * n_plays,
            "posteam": [_HOME] * n_plays,
            "defteam": [_AWAY] * n_plays,
            "complete_pass": [1] * n_plays,
            "yards_gained": [5] * n_plays,
        },
        extras={"thrown_by": [thrown_by] * n_plays},
    )
    return df.with_columns(epa=pl.Series([1.0] * n_plays))


def _pat_filler_rows() -> pl.DataFrame:
    """Two `down == 0` PAT rows, unrelated to any test's actual scrimmage plays --
    `build_player_analysis_data`'s `attach_epa` call runs `estimate_pat_baselines` on the FULL
    corpus before any team filtering, and that raises `InsufficientPatAttempts` without at
    least one 1-pt and one 2-pt attempt (mirrors `tests/test_reports_own_team.py::_pat_ready`,
    but as two dedicated extra rows rather than overwriting the first two of the caller's own
    plays -- this module's tests need exact, hand-picked scrimmage-play counts per split).
    `posteam` is deliberately the away side so these rows never enter any offense-filtered
    split section.
    """
    return canonical_plays(
        n_games=1,
        plays_per_game=2,
        overrides={
            "down": [0, 0],
            "yards_to_go": [3, 10],
            "one_point_conv_success": [1, 0],
            "two_point_conv_success": [0, 1],
            "posteam": [_AWAY, _AWAY],
            "defteam": [_HOME, _HOME],
            "game_id": ["pat-filler", "pat-filler"],
        },
    ).with_columns(epa=pl.lit(None, dtype=pl.Float64))


def _row(table: pl.DataFrame, spieler: str) -> dict:
    matches = table.filter(pl.col("spieler") == spieler)
    assert matches.height == 1, f"expected exactly one row for {spieler!r}, got {matches.height}"
    return matches.to_dicts()[0]


# --- Task 1: counting/yardage columns, on his denominators ------------------


class TestCountingAndYardageColumns:
    def test_hc_attempts_excludes_sacks(self) -> None:
        """One QB, one completion, one incompletion, one interception, two sacks:
        comps==1, incs==1, ints==1, sacks==2, attempts==3 (Attempts = Comps+Incs+INTs,
        Sacks is a separate column, workbook cell D2)."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=5,
            overrides={
                "complete_pass": [1, 0, 0, 0, 0],
                "interception": [0, 0, 1, 0, 0],
                "sack": [0, 0, 0, 1, 1],
            },
            extras={"thrown_by": ["QB1"] * 5},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["comps"] == 1
        assert row["incs"] == 1
        assert row["ints"] == 1
        assert row["sacks"] == 2
        assert row["attempts"] == 3

    def test_comp_pct_matches_comps_over_attempts(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=4,
            overrides={"complete_pass": [1, 1, 0, 0], "interception": [0, 0, 0, 0]},
            extras={"thrown_by": ["QB1"] * 4},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["comp_pct"] == pytest.approx(row["comps"] / row["attempts"])

    def test_comp_pct_null_on_zero_attempts_and_muted_below_threshold(self) -> None:
        """A QB with only a sack (present via the sacks scope, zero Comps/Incs/INTs):
        attempts == 0 (a real number, not null -- they are known to have thrown), comp_pct
        is null (never his `iferror(...,0)` zero), and they are flagged muted."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=1,
            overrides={"sack": [1]},
            extras={"thrown_by": ["QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["attempts"] == 0
        assert row["comp_pct"] is None
        assert row["muted"] is True

    def test_rushing_only_qb_gets_null_pass_columns(self) -> None:
        """A QB who never appears in the pass/sack scope at all (rushing-only) gets NULL
        pass columns, not a fabricated zero (distinct from the sack-only case above)."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"play_type": ["run", "run"]},
            extras={"qb": ["QB1", "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["attempts"] is None
        assert row["comp_pct"] is None
        assert row["carries"] == 2

    def test_pass_yards_only_counts_completions(self) -> None:
        """An incompletion with a non-zero yards_gained never contributes to Pass Yards."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={
                "complete_pass": [1, 0],
                "yards_gained": [10, 25],
            },
            extras={"thrown_by": ["QB1", "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["pass_yards"] == 10.0

    def test_rush_columns_do_not_touch_pass_columns(self) -> None:
        """A 30-yard run for the same QB never changes attempts, ypa or any pass column."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=3,
            overrides={
                "play_type": ["pass", "pass", "run"],
                "complete_pass": [1, 0, 0],
                "yards_gained": [10, 0, 30],
                "touchdown": [0, 0, 1],
            },
            extras={"thrown_by": ["QB1", "QB1", None], "qb": [None, None, "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["attempts"] == 2
        assert row["pass_yards"] == 10.0
        assert row["ypa"] == pytest.approx(5.0)
        assert row["carries"] == 1
        assert row["rush_yards"] == 30.0
        assert row["rush_tds"] == 1

    def test_player_identity_coalesces_thrown_by_and_qb(self) -> None:
        """A rushing QB (thrown_by null on the run row) resolves via `qb`."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"play_type": ["pass", "run"], "complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", None], "qb": ["QB1", "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert result.table["spieler"].to_list() == ["QB1"]
        row = _row(result.table, "QB1")
        assert row["carries"] == 1

    def test_rows_with_null_identity_excluded(self) -> None:
        """A row with both thrown_by and qb null is excluded, not grouped under a null key."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 1]},
            extras={"thrown_by": ["QB1", None], "qb": [None, None]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert None not in result.table["spieler"].to_list()
        assert result.table["spieler"].to_list() == ["QB1"]

    def test_air_yards_null_when_column_absent(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]}
        ).drop("air_yards")
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["air_yards"] is None

    def test_air_yards_summed_on_completions_when_present(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"], "air_yards": [8, 99]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["air_yards"] == 8.0

    def test_empty_frame_returns_schema_correct_table(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1).filter(pl.lit(False))
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert result.table.height == 0
        assert result.table.schema == _HC_COLUMN_SCHEMA


# --- Task 2: delegated columns and named availability state -----------------


class TestDelegatedColumnsAndAvailability:
    def test_exp_plays_and_explosive_pct_match_explosiveness_module(self) -> None:
        from flag_football_ep.features.explosiveness import hc_workbook_explosive_rate

        df = canonical_plays(
            n_games=1,
            plays_per_game=3,
            overrides={"yards_gained": [20, 5, 3]},
            extras={"thrown_by": ["QB1", "QB1", "QB1"]},
        )
        expected = hc_workbook_explosive_rate(df, group_col="thrown_by")
        expected_row = expected.filter(pl.col("thrown_by") == "QB1").to_dicts()[0]

        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["exp_plays"] == expected_row["exp_plays"]
        assert row["explosive_pct"] == pytest.approx(expected_row["explosive_pct"])

    def test_efficiency_unavailable_when_column_has_no_real_signal(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["efficiency"] is None
        assert "efficiency" in result.unavailable
        assert any("Efficiency" in n for n in result.notices)

    def test_efficiency_computed_when_synthetic_column_present(self) -> None:
        from flag_football_ep.features.explosiveness import hc_efficiency_table

        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"], "efficiency": [1, 0]},
        )
        expected = hc_efficiency_table(df, group_col="thrown_by")
        expected_row = expected.filter(pl.col("thrown_by") == "QB1").to_dicts()[0]

        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["efficiency"] == pytest.approx(expected_row["efficiency"])
        assert "efficiency" not in result.unavailable

    def test_adjusted_columns_unavailable_when_drop_column_absent(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"]},
        )
        assert df["drop"].null_count() == df.height  # sanity: no real drop signal

        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        for column in ("adj_comp_pct", "adj_pass_yards", "adj_ypa"):
            assert row[column] is None
            assert column in result.unavailable
        assert row["adj_comp_pct"] != row["comp_pct"]

    def test_adjusted_columns_computed_when_synthetic_drop_column_present(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0], "yards_gained": [10, 0]},
            extras={
                "thrown_by": ["QB1", "QB1"],
                "drop": [None, "X"],
                "air_yards": [None, 7],
            },
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        for column in ("adj_comp_pct", "adj_pass_yards", "adj_ypa"):
            assert column not in result.unavailable
        assert row["adj_comp_pct"] == pytest.approx((1 + 1) / 2)
        assert row["adj_pass_yards"] == pytest.approx(17.0)

    def test_drop_flag_never_derived_from_other_columns(self) -> None:
        """The drop flag is `drop` non-null and non-empty after stripping -- never derived
        from any other column. A whitespace-only drop value is not flagged."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"], "drop": ["X", "   "]},
        )
        # "X" is a real flag (row 1); the whitespace-only row 2 is not -- so drop IS
        # available overall (row 1 has real signal), but the whitespace row must not be
        # counted as a dropped incompletion.
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        # row 1 is a completion (not eligible as a dropped incompletion), row 2 is
        # whitespace-only (not a real flag) -- so no dropped incompletions at all.
        assert row["adj_comp_pct"] == pytest.approx(row["comp_pct"])

    def test_notices_include_air_yards_deviation_when_available(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert any("Air Yards" in n for n in result.notices)

    def test_notices_include_hc_workbook_corpus_count(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert any("hc_workbook" in n for n in result.notices)

    def test_unavailable_and_notices_nonempty_on_todays_real_column_set(self) -> None:
        """`canonical_plays()`'s default frame carries `drop`/`efficiency` as present but
        entirely null columns -- exactly today's real corpus state (zero HC rows ingested
        into `plays_scored.parquet` yet). Both must still be treated as unavailable."""
        df = canonical_plays(n_games=1, plays_per_game=2, extras={"thrown_by": ["QB1", "QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert result.unavailable
        assert result.notices
        assert {"adj_comp_pct", "adj_pass_yards", "adj_ypa", "efficiency"} <= set(
            result.unavailable
        )


# --- Task 1 continued: load_report_calibration -------------------------------------------


class TestLoadReportCalibration:
    def test_missing_file_returns_none_and_notice(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        calibration, notices = load_report_calibration(config)
        assert calibration is None
        assert len(notices) == 1
        assert "nicht gefunden" in notices[0]

    def test_valid_file_returns_calibration_and_no_notice(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        path = config.paths.reference / "explosiveness" / "calibration.json"
        write_calibration(_calibration(), path)

        calibration, notices = load_report_calibration(config)
        assert calibration is not None
        assert calibration.epa_threshold == pytest.approx(2.0)
        assert notices == ()

    def test_unknown_schema_version_returns_none_and_notice(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        path = config.paths.reference / "explosiveness" / "calibration.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"schema_version": 999}))

        calibration, notices = load_report_calibration(config)
        assert calibration is None
        assert len(notices) == 1


# --- Task 1: m3_columns_by_qb --------------------------------------------------------------


class TestM3ColumnsByQb:
    def test_matches_live_definition_comparison_call(self) -> None:
        """Every value m3_columns_by_qb reports for QB1 equals a direct, independent
        `definition_comparison` call on the same frame -- a hard-coded expected rate would let
        a future definition change pass silently (plan Task 1 action)."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=8,
            overrides={
                "complete_pass": [1, 1, 0, 0, 1, 0, 1, 0],
                "interception": [0, 0, 0, 0, 0, 0, 0, 0],
                "yards_gained": [5, 20, 0, 0, 15, 0, 8, 0],
            },
            extras={"thrown_by": ["QB1"] * 8},
        ).with_columns(epa=pl.Series([1.0, 3.0, -1.0, -0.5, 2.5, -2.0, 0.5, -1.5]))
        calibration = _calibration()

        table, notices = m3_columns_by_qb(df, calibration=calibration, group_col="thrown_by")
        assert notices == ()

        expected = definition_comparison(df, ["thrown_by"], calibration=calibration)
        row = table.filter(pl.col("spieler") == "QB1").to_dicts()[0]

        for definition in DEFINITIONS:
            exp_row = expected.filter(
                (pl.col("thrown_by") == "QB1") & (pl.col("definition") == definition.key)
            ).to_dicts()[0]
            assert row[f"{definition.key}_n"] == exp_row["n"]
            assert row[f"{definition.key}_muted"] == exp_row["muted"]
            _assert_matches(row[f"{definition.key}_rate"], exp_row["rate"])
            _assert_matches(row[f"{definition.key}_ci_low"], exp_row["ci_low"])
            _assert_matches(row[f"{definition.key}_ci_high"], exp_row["ci_high"])
            _assert_matches(row[f"{definition.key}_shrunk_rate"], exp_row["shrunk_rate"])

    def test_player_outside_definition_scope_keeps_n_zero_row(self) -> None:
        """A QB present only via a sack row: `HC_PASS_ATTEMPT_SCOPE` excludes sacks, so the
        two `baseline_hc_*` definitions see `n == 0` for this player -- present, not dropped;
        the two EPA-scoped definitions (scope = every scrimmage play) still see `n == 1`."""
        df = canonical_plays(
            n_games=1, plays_per_game=1, overrides={"sack": [1]}, extras={"thrown_by": ["QB1"]}
        ).with_columns(epa=pl.Series([-1.0]))
        calibration = _calibration()

        table, _ = m3_columns_by_qb(df, calibration=calibration, group_col="thrown_by")
        row = table.filter(pl.col("spieler") == "QB1").to_dicts()[0]
        assert row["baseline_hc_workbook_n"] == 0
        assert row["baseline_hc_verbal_n"] == 0
        assert row["success_rate_epa_n"] == 1
        assert row["explosive_epa_magnitude_n"] == 1

    def test_explosive_score_mean_is_mean_over_scrimmage_plays(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, extras={"thrown_by": ["QB1", "QB1"]}
        ).with_columns(epa=pl.Series([4.0, 2.0]))
        calibration = _calibration(threshold=2.0, iqr=1.0)

        table, _ = m3_columns_by_qb(df, calibration=calibration, group_col="thrown_by")
        row = table.filter(pl.col("spieler") == "QB1").to_dicts()[0]

        import math

        expected = (1 / (1 + math.exp(-2.0)) + 1 / (1 + math.exp(-0.0))) / 2
        assert row["explosive_score_mean"] == pytest.approx(expected)

    def test_explosive_score_mean_null_without_any_epa(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, extras={"thrown_by": ["QB1", "QB1"]}
        ).with_columns(epa=pl.lit(None, dtype=pl.Float64))
        calibration = _calibration()

        table, _ = m3_columns_by_qb(df, calibration=calibration, group_col="thrown_by")
        row = table.filter(pl.col("spieler") == "QB1").to_dicts()[0]
        assert row["explosive_score_mean"] is None

    def test_calibration_none_returns_null_schema_correct_table(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, extras={"thrown_by": ["QB1", "QB1"]}
        ).with_columns(epa=pl.Series([1.0, -1.0]))

        table, notices = m3_columns_by_qb(df, calibration=None, group_col="thrown_by")
        assert notices == ()
        assert table.height == 1
        assert table.schema == _M3_COLUMN_SCHEMA
        row = table.to_dicts()[0]
        assert row["spieler"] == "QB1"
        for column, value in row.items():
            if column == "spieler":
                continue
            assert value is None

    def test_m3_player_universe_is_subset_of_hc_columns_players(self) -> None:
        """Joining the M3-3 block onto `HcColumnTable.table` must add no player absent from
        his columns (plan Task 1 behaviour) -- m3's player set is always a subset of hc's."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=3,
            overrides={"play_type": ["pass", "pass", "run"], "complete_pass": [1, 0, 0]},
            extras={"thrown_by": ["QB1", "QB1", None], "qb": [None, None, "QB2"]},
        ).with_columns(epa=pl.Series([1.0, -1.0, 0.5]))
        calibration = _calibration()

        hc = hc_columns_by_qb(df, group_col="thrown_by")
        m3_table, _ = m3_columns_by_qb(df, calibration=calibration, group_col="thrown_by")

        hc_players = set(hc.table["spieler"].to_list())
        m3_players = set(m3_table["spieler"].to_list())
        assert m3_players
        assert m3_players <= hc_players


# --- Task 2: split sections and build_player_analysis_data ---------------------------------


class TestBuildPlayerAnalysisData:
    def test_returns_korpus_hc_gesamt_and_one_section_per_split_key(
        self, tmp_path: Path
    ) -> None:
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)

        hc = _hc_plays(game_id="hc-g1", n_plays=3)
        hudl = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={
                "posteam": [_HOME] * 2,
                "defteam": [_AWAY] * 2,
                "game_id": ["hudl-g0"] * 2,
            },
            extras={"thrown_by": ["QB2"] * 2},
        ).with_columns(epa=pl.Series([1.0, -1.0]))
        df = pl.concat([_pat_filler_rows(), hc, hudl], how="vertical")

        _write_hc_games(
            config.reference.hc_games,
            [_hc_game_row("hc-g1", note="refill: numeric block, rows 100-150, 3 plays")],
        )
        _write_hc_splits(
            config.reference.hc_splits,
            [_hc_split_row("camp-a", first_row=2, last_row=1000, label_de="Camp A")],
        )

        result = build_player_analysis_data(df, config=config, scored=None)
        assert isinstance(result, PlayerAnalysisReportData)

        keys = {s.key for s in result.splits}
        assert keys == {"korpus", "hc-gesamt", "camp-a"}

        korpus = next(s for s in result.splits if s.key == "korpus")
        hc_gesamt = next(s for s in result.splits if s.key == "hc-gesamt")
        camp_a = next(s for s in result.splits if s.key == "camp-a")

        assert korpus.basis.n_plays == 5
        assert hc_gesamt.basis.n_plays == 3
        assert camp_a.basis.n_plays == 3
        assert camp_a.label_status == "verified"
        assert camp_a.heading == "Camp A"
        assert result.n_hc_rows == 3

    def test_empty_state_zero_hc_rows(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)
        offense = canonical_plays(
            n_games=1,
            plays_per_game=4,
            overrides={"posteam": [_HOME] * 4, "defteam": [_AWAY] * 4},
            extras={"thrown_by": ["QB1"] * 4},
        ).with_columns(epa=pl.Series([1.0, -1.0, 0.5, -0.5]))
        df = pl.concat([_pat_filler_rows(), offense], how="vertical")

        _write_hc_games(config.reference.hc_games, [])
        _write_hc_splits(
            config.reference.hc_splits,
            [
                _hc_split_row("camp-a", first_row=2, last_row=1000, label_de="Camp A"),
                _hc_split_row("camp-b", first_row=1001, last_row=2000, label_de="Camp B"),
            ],
        )

        result = build_player_analysis_data(df, config=config, scored=None)
        assert isinstance(result, PlayerAnalysisReportData)
        assert result.n_hc_rows == 0

        camp_splits = [s for s in result.splits if s.key not in ("korpus", "hc-gesamt")]
        assert len(camp_splits) == 2
        for split in camp_splits:
            assert split.empty_notice is not None
            assert split.columns.table.height == 0

    def test_two_windows_report_different_row_counts(self, tmp_path: Path) -> None:
        """Pitfall-3 guard: two camps with head-coach rows in different windows must not
        report identical numbers -- that would mean the filter never discriminated."""
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)

        hc_a = _hc_plays(game_id="hc-a", n_plays=3, thrown_by="QB1")
        hc_b = _hc_plays(game_id="hc-b", n_plays=5, thrown_by="QB2")
        df = pl.concat([_pat_filler_rows(), hc_a, hc_b], how="vertical")

        _write_hc_games(
            config.reference.hc_games,
            [
                _hc_game_row("hc-a", note="refill: numeric block, rows 100-150, 3 plays"),
                _hc_game_row("hc-b", note="refill: numeric block, rows 2100-2200, 5 plays"),
            ],
        )
        _write_hc_splits(
            config.reference.hc_splits,
            [
                _hc_split_row("camp-a", first_row=2, last_row=1000, label_de="Camp A"),
                _hc_split_row("camp-b", first_row=2001, last_row=3000, label_de="Camp B"),
            ],
        )

        result = build_player_analysis_data(df, config=config, scored=None)
        camp_a = next(s for s in result.splits if s.key == "camp-a")
        camp_b = next(s for s in result.splits if s.key == "camp-b")

        assert camp_a.basis.n_plays == 3
        assert camp_b.basis.n_plays == 5
        assert camp_a.basis.n_plays != camp_b.basis.n_plays

    def test_unresolved_game_named_and_excluded_from_every_camp_section(
        self, tmp_path: Path
    ) -> None:
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)

        hc_matched = _hc_plays(game_id="hc-a", n_plays=3, thrown_by="QB1")
        hc_unresolved = _hc_plays(game_id="hc-z", n_plays=2, thrown_by="QB2")
        df = pl.concat([_pat_filler_rows(), hc_matched, hc_unresolved], how="vertical")

        _write_hc_games(
            config.reference.hc_games,
            [
                _hc_game_row("hc-a", note="refill: numeric block, rows 100-150, 3 plays"),
                _hc_game_row("hc-z", note="refill: numeric block, rows 9000-9100, 2 plays"),
            ],
        )
        _write_hc_splits(
            config.reference.hc_splits,
            [_hc_split_row("camp-a", first_row=2, last_row=1000, label_de="Camp A")],
        )

        result = build_player_analysis_data(df, config=config, scored=None)

        assert ("hc-z", "outside-known-windows") in result.unresolved_games

        hc_gesamt = next(s for s in result.splits if s.key == "hc-gesamt")
        assert set(hc_gesamt.columns.table["spieler"].to_list()) == {"QB1", "QB2"}

        camp_a = next(s for s in result.splits if s.key == "camp-a")
        assert set(camp_a.columns.table["spieler"].to_list()) == {"QB1"}

    def test_missing_hc_games_csv_yields_only_korpus_section(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)
        offense = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"posteam": [_HOME] * 2, "defteam": [_AWAY] * 2},
            extras={"thrown_by": ["QB1"] * 2},
        ).with_columns(epa=pl.Series([1.0, -1.0]))
        df = pl.concat([_pat_filler_rows(), offense], how="vertical")
        # config.reference.hc_games/hc_splits point at tmp_path files that are never written.

        result = build_player_analysis_data(df, config=config, scored=None)

        assert len(result.splits) == 1
        assert result.splits[0].key == "korpus"
        assert any("Referenzdatei" in n for n in result.notices)

    def test_standing_opp_notice_always_present(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)
        offense = canonical_plays(
            n_games=1,
            plays_per_game=1,
            overrides={"posteam": [_HOME], "defteam": [_AWAY]},
            extras={"thrown_by": ["QB1"]},
        ).with_columns(epa=pl.Series([1.0]))
        df = pl.concat([_pat_filler_rows(), offense], how="vertical")

        result = build_player_analysis_data(df, config=config, scored=None)
        assert any("OPP" in n for n in result.notices)

    def test_conflict_label_status_triggers_notice(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)
        offense = canonical_plays(
            n_games=1,
            plays_per_game=1,
            overrides={"posteam": [_HOME], "defteam": [_AWAY]},
            extras={"thrown_by": ["QB1"]},
        ).with_columns(epa=pl.Series([1.0]))
        df = pl.concat([_pat_filler_rows(), offense], how="vertical")

        _write_hc_games(config.reference.hc_games, [])
        _write_hc_splits(
            config.reference.hc_splits,
            [
                _hc_split_row(
                    "camp-conflict",
                    first_row=2,
                    last_row=1000,
                    label_de="Camp Conflict",
                    label_status="conflict",
                )
            ],
        )

        result = build_player_analysis_data(df, config=config, scored=None)
        assert any("Konflikt" in n for n in result.notices)

    def test_empty_plays_frame_does_not_raise(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        config.paths.processed.mkdir(parents=True, exist_ok=True)
        df = canonical_plays(n_games=1, plays_per_game=1).filter(pl.lit(False))

        result = build_player_analysis_data(df, config=config, scored=None)
        assert isinstance(result, PlayerAnalysisReportData)
        assert result.splits[0].key == "korpus"
