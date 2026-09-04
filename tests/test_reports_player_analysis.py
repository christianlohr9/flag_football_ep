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
    _HC_COLUMN_SCHEMA,
    _M3_COLUMN_SCHEMA,
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
