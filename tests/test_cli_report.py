"""Coverage for `flag_football_ep.reports.build` (REQ-S1-16) and the `ffep report` CLI
command.

Module-level fixtures/helpers land here in task 1; the `ffep report` CLI tests (task 2) and
the full raw-tree-to-written-HTML end-to-end test (task 3) extend this same file.

Config/corpus construction reuses `tests/test_model_score.py`'s `_make_config`/
`_trained_config`/`_register_run` helpers (imported as `tms`) rather than duplicating them --
those already build a fully-populated `Config` (including `competition_tier.csv`, which
`score_plays` requires) and a real train-then-register round trip against a temporary MLflow
store.
"""

from __future__ import annotations

import re
import socket
from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

import test_cli_run as tcr
import test_model_score as tms
import test_pipeline_ingest as tpi
from flag_football_ep.cli import app
from flag_football_ep.config import Config, ReportSettings
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.reports.build import (
    PRODUCTS,
    ReportRunResult,
    build_reports,
    run_report_pipeline,
)
from flag_football_ep.reports.decisions import DECISIONS_FILENAME
from flag_football_ep.reports.opponent import opponent_filename
from flag_football_ep.reports.own_team import OWN_TEAM_FILENAME
from flag_football_ep.reports.wp_review import wp_review_filename
from flag_football_ep.testing import canonical_plays_with_scores

_CLI_RUNNER = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    """Strip ANSI escape codes -- rich's `--help` rendering can split an option's leading
    `--` from the rest of its name with a color-reset escape sequence, which breaks a naive
    substring check against the raw CliRunner output (mirrors
    `test_charts_pat_breakeven.py::_plain`)."""
    return _ANSI_RE.sub("", output)


# --- shared fixtures/helpers ----------------------------------------------------------------


def _write_group_opponents(reference_dir: Path, rows: list[tuple[str, str]]) -> None:
    reference_dir.mkdir(parents=True, exist_ok=True)
    lines = ["canonical_team,team_name"] + [f"{code},{name}" for code, name in rows]
    (reference_dir / "group_opponents.csv").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _write_player_mapping(
    reference_dir: Path, rows: list[tuple[str, str, str]] | None = None
) -> None:
    reference_dir.mkdir(parents=True, exist_ok=True)
    lines = ["source,source_player,canonical_player"]
    for row in rows or []:
        lines.append(",".join(row))
    (reference_dir / "player_mapping.csv").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _plays_and_fake_scored(
    n_games: int = 2, plays_per_game: int = 8
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """A full canonical corpus plus a hand-built `scored` frame (no real model involved) --
    enough for `build_reports`'s own dispatch/validation/isolation behaviour, which never
    inspects the numeric EP/WP values themselves.

    Play 7 (index 6) of every game is overridden to a 1-pt PAT attempt (`down=0`,
    `yards_to_go=3`) and play 8 (index 7) to a 2-pt PAT attempt (`down=0`, `yards_to_go=8`) --
    `estimate_pat_baselines` (used by `own_team.attach_epa`/`reports.decisions`) raises on a
    corpus with zero attempts of either type, matching `test_model_score.py::_training_corpus`.
    """
    height = n_games * plays_per_game
    down = [((i % plays_per_game) - 1) % 4 + 1 for i in range(height)]
    yards_to_go = [5 + ((i % plays_per_game) % 16) for i in range(height)]
    for game_idx in range(n_games):
        base = game_idx * plays_per_game
        down[base + 6] = 0
        yards_to_go[base + 6] = 3
        down[base + 7] = 0
        yards_to_go[base + 7] = 8

    plays = canonical_plays_with_scores(
        n_games=n_games,
        plays_per_game=plays_per_game,
        overrides={
            "down": down,
            "yards_to_go": yards_to_go,
            "one_point_conv_success": [
                1 if (i % plays_per_game) == 6 else 0 for i in range(height)
            ],
            "two_point_conv_success": [
                1 if (i % plays_per_game) == 7 else 0 for i in range(height)
            ],
        },
    )
    scored = plays.with_columns(
        ep=pl.lit(0.5, dtype=pl.Float64),
        epa=pl.lit(0.05, dtype=pl.Float64),
        wp=pl.lit(0.5, dtype=pl.Float64),
        home_wp=pl.lit(0.5, dtype=pl.Float64),
        away_wp=pl.lit(0.5, dtype=pl.Float64),
        wpa=pl.lit(0.01, dtype=pl.Float64),
    )
    return plays, scored


def _wp_only_plays(game_id: str = "G1", n_plays: int = 4) -> pl.DataFrame:
    """The minimal column set `attach_wp_provenance`'s pipeline (`prepare_wp_data` +
    `add_wp_variables`) and `build_wp_review_page` both need -- mirrors
    `tests/test_reports_wp_review.py::_provenance_plays`/`_game_plays`."""
    rows = []
    for play_id in range(1, n_plays + 1):
        rows.append(
            {
                "game_id": game_id,
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


def _write_oof_wp_parquet(processed_dir: Path, rows: dict) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(processed_dir / "oof_predictions_wp.parquet")


def _config_to_toml(config: Config) -> str:
    """Serialise an arbitrary already-built `Config` back into `ffep.toml` text -- generic
    over whichever helper produced it (`tms._make_config`/`_trained_config`), so the CLI
    tests below exercise the exact same paths (including the temporary MLflow store and any
    trained/promoted models) a `Config` object already carries."""
    return f"""
[paths]
data_root = "{config.paths.data_root}"
raw_hudl = "{config.paths.raw_hudl}"
raw_sportapp = "{config.paths.raw_sportapp}"
raw_ifaf = "{config.paths.raw_ifaf}"
raw_legacy = "{config.paths.raw_legacy}"
processed = "{config.paths.processed}"
reference = "{config.paths.reference}"
models = "{config.paths.models}"
mlruns = "{config.paths.mlruns}"
contract = "{config.paths.contract}"
reports = "{config.paths.reports}"
video = "{config.paths.video}"
labels = "{config.paths.labels}"
tracking = "{config.paths.tracking}"

[reference]
half_boundaries = "{config.reference.half_boundaries}"
final_scores = "{config.reference.final_scores}"
team_mapping = "{config.reference.team_mapping}"
sportapp_games = "{config.reference.sportapp_games}"
competition_tier = "{config.reference.competition_tier}"
player_mapping = "{config.reference.player_mapping}"
group_opponents = "{config.reference.group_opponents}"
hover_positions = "{config.reference.hover_positions}"
homography_calibration = "{config.reference.homography_calibration}"
gt_positions = "{config.reference.gt_positions}"
continuity_review = "{config.reference.continuity_review}"

[sources.sportapp]
base_url = "{config.sources.sportapp.base_url}"
api_key_env = "{config.sources.sportapp.api_key_env}"

[sources.ifaf]
base_url = "{config.sources.ifaf.base_url}"
tournament = "{config.sources.ifaf.tournament}"
api_key_env = "{config.sources.ifaf.api_key_env}"

[train]
ep_experiment = "{config.train.ep_experiment}"
wp_experiment = "{config.train.wp_experiment}"
exclude_games_ep = []
exclude_games_wp = []

[report]
own_team = "{config.report.own_team}"
cycle_start_season = {config.report.cycle_start_season}

[cv]
pilot_session_id = "{config.cv.pilot_session_id}"
detector_model = "{config.cv.detector_model}"
detector_experiment = "{config.cv.detector_experiment}"
resolution = {config.cv.resolution}
sahi = {str(config.cv.sahi).lower()}
sahi_slice = {config.cv.sahi_slice}
sahi_overlap = {config.cv.sahi_overlap}
train_epochs = {config.cv.train_epochs}
train_batch_size = {config.cv.train_batch_size}
train_grad_accum = {config.cv.train_grad_accum}
device = "{config.cv.device}"
label_frame_target = {config.cv.label_frame_target}
cvat_host = "{config.cv.cvat_host}"
cvat_username_env = "{config.cv.cvat_username_env}"
cvat_password_env = "{config.cv.cvat_password_env}"
field_length_yards = {config.cv.field_length_yards}
field_width_yards = {config.cv.field_width_yards}
endzone_yards = {config.cv.endzone_yards}
"""


def _write_toml(config: Config, root: Path) -> Path:
    toml_path = root / "ffep.toml"
    toml_path.write_text(_config_to_toml(config), encoding="utf-8")
    return toml_path


def _promoted_config(tmp_path: Path) -> Config:
    """A config with both models trained and promoted to `champion` -- the shape
    `run_report_pipeline`'s own tests need to reach the scoring stage at all."""
    config, ep_run, wp_run = tms._trained_config(tmp_path)

    # `train_ep`/`train_wp` each leave the MLflow fluent API's active experiment pointed at
    # whichever one ran last -- `mlflow.start_run(run_id=...)` inside `_register_run` rejects a
    # run id whose own experiment doesn't match that active experiment, so it must be reset to
    # each run's own experiment right before registering it.
    mlflow_store.ensure_experiment(config.train.ep_experiment, config)
    tms._register_run(config, ep_run, "ep")
    registry.promote(registry.registered_model_name("ep"), ep_run, config)

    mlflow_store.ensure_experiment(config.train.wp_experiment, config)
    tms._register_run(config, wp_run, "wp")
    registry.promote(registry.registered_model_name("wp"), wp_run, config)

    return config


# --- build_reports: allow-list validation ---------------------------------------------------


def test_build_reports_rejects_opponent_outside_reference_file(tmp_path: Path) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    with pytest.raises(ValueError, match="NOPE"):
        build_reports(
            plays, scored, config=config, opponents=["NOPE"], products=["opponents"]
        )


def test_build_reports_valid_opponent_subset_produces_exactly_that_subset(
    tmp_path: Path,
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    rendered, notices = build_reports(
        plays, scored, config=config, opponents=["AWAY"], products=["opponents"]
    )

    assert notices == ()
    assert set(rendered) == {opponent_filename("AWAY")}


def test_build_reports_unknown_product_raises(tmp_path: Path) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    with pytest.raises(ValueError, match="bogus"):
        build_reports(plays, scored, config=config, products=["bogus"])


def test_products_tuple_is_the_four_locked_families(tmp_path: Path) -> None:
    assert PRODUCTS == ("opponents", "own-team", "decisions", "wp-review")


def test_build_reports_own_team_product_uses_own_team_filename(tmp_path: Path) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    _write_player_mapping(config.reference.player_mapping.parent)
    plays, scored = _plays_and_fake_scored()

    rendered, notices = build_reports(
        plays, scored, config=config, opponents=[], products=["own-team"]
    )

    assert notices == ()
    assert OWN_TEAM_FILENAME in rendered


def test_build_reports_none_opponents_builds_every_group_opponent(
    tmp_path: Path,
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent,
        [("AWAY", "Away Team"), ("HOME", "Home Team")],
    )
    plays, scored = _plays_and_fake_scored()

    rendered, notices = build_reports(
        plays, scored, config=config, products=["opponents"]
    )

    assert notices == ()
    assert set(rendered) == {opponent_filename("AWAY"), opponent_filename("HOME")}


# --- build_reports: per-product failure isolation (T-1.4-59) -------------------------------


def test_build_reports_product_failure_yields_notice_and_other_products_still_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AWAY", "Away Team")]
    )
    plays, scored = _plays_and_fake_scored()

    import flag_football_ep.reports.build as build_module

    def _boom(*args: object, **kwargs: object) -> str:
        raise RuntimeError("synthetic decisions failure")

    monkeypatch.setattr(build_module, "build_decisions_page", _boom)

    rendered, notices = build_reports(
        plays,
        scored,
        config=config,
        opponents=["AWAY"],
        products=["opponents", "decisions"],
    )

    assert DECISIONS_FILENAME not in rendered
    assert opponent_filename("AWAY") in rendered
    assert any("decisions" in n.lower() or "entscheidung" in n.lower() for n in notices)
    assert any("synthetic decisions failure" in n for n in notices)


# --- build_reports: WP review pages come from the provenance-resolved frame (T-1.4-67) ------


def test_build_reports_wp_review_page_states_out_of_fold_provenance(
    tmp_path: Path,
) -> None:
    config = tms._make_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    plays = _wp_only_plays(game_id="G1", n_plays=4)
    _write_oof_wp_parquet(
        config.paths.processed,
        {"game_id": ["G1"] * 4, "play_id": [1, 2, 3, 4], "wp": [0.5, 0.55, 0.6, 0.65]},
    )

    scored = plays.with_columns(
        wp=pl.lit(0.5, dtype=pl.Float64),
        home_wp=pl.lit(0.5, dtype=pl.Float64),
        away_wp=pl.lit(0.5, dtype=pl.Float64),
        wpa=pl.lit(0.0, dtype=pl.Float64),
    )
    rendered, notices = build_reports(
        plays, scored, config=config, opponents=[], products=["wp-review"]
    )

    assert notices == ()
    page = rendered[wp_review_filename("G1")]
    assert "out-of-fold" in page
    assert "WP-Provenienz unbekannt" not in page


# --- run_report_pipeline: stage durations and the Pitfall-1 regression guard ----------------


def test_run_report_pipeline_never_trains_and_durations_has_exactly_three_stage_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _promoted_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    _write_player_mapping(config.reference.player_mapping.parent)

    plays = tms._training_corpus()
    config.paths.processed.mkdir(parents=True, exist_ok=True)
    plays.write_parquet(config.paths.processed / "plays.parquet")

    import flag_football_ep.model.train as train_module

    def _fail_if_called(*args: object, **kwargs: object) -> str:
        raise AssertionError(
            "run_report_pipeline must never call train_ep/train_wp (RESEARCH Pitfall 1)"
        )

    monkeypatch.setattr(train_module, "train_ep", _fail_if_called)
    monkeypatch.setattr(train_module, "train_wp", _fail_if_called)

    result = run_report_pipeline(
        config, opponents=[], products=["decisions"], skip_ingest=True
    )

    assert isinstance(result, ReportRunResult)
    assert set(result.durations) == {"ingest", "score", "report"}
    assert DECISIONS_FILENAME in result.filenames


def test_run_report_pipeline_skip_ingest_records_zero_duration_and_notice(
    tmp_path: Path,
) -> None:
    config = _promoted_config(tmp_path)
    _write_group_opponents(config.reference.group_opponents.parent, [])
    _write_player_mapping(config.reference.player_mapping.parent)

    plays = tms._training_corpus()
    config.paths.processed.mkdir(parents=True, exist_ok=True)
    plays.write_parquet(config.paths.processed / "plays.parquet")

    result = run_report_pipeline(
        config, opponents=[], products=["decisions"], skip_ingest=True
    )

    assert result.durations["ingest"] == 0.0
    assert any("skip-ingest" in n or "übersprungen" in n for n in result.notices)


# --- ffep report CLI ------------------------------------------------------------------------


class TestReportCli:
    def test_help_exits_zero_and_lists_every_option(self) -> None:
        result = _CLI_RUNNER.invoke(app, ["report", "--help"])

        assert result.exit_code == 0
        output = _plain(result.output)
        assert "--opponent" in output
        assert "--product" in output
        assert "--skip-ingest" in output
        assert "--config" in output

    def test_unknown_product_exits_nonzero_listing_valid_products(
        self, tmp_path: Path
    ) -> None:
        config = tms._make_config(tmp_path)
        _write_group_opponents(config.reference.group_opponents.parent, [])
        toml_path = _write_toml(config, tmp_path)

        result = _CLI_RUNNER.invoke(
            app, ["report", "--config", str(toml_path), "--product", "bogus"]
        )

        assert result.exit_code != 0
        assert "bogus" in result.output
        for name in PRODUCTS:
            assert name in result.output

    def test_opponent_outside_reference_file_exits_nonzero_naming_value(
        self, tmp_path: Path
    ) -> None:
        config = _promoted_config(tmp_path)
        _write_group_opponents(
            config.reference.group_opponents.parent, [("AUT", "Austria")]
        )
        _write_player_mapping(config.reference.player_mapping.parent)
        plays = tms._training_corpus()
        config.paths.processed.mkdir(parents=True, exist_ok=True)
        plays.write_parquet(config.paths.processed / "plays.parquet")
        toml_path = _write_toml(config, tmp_path)

        result = _CLI_RUNNER.invoke(
            app,
            [
                "report",
                "--config",
                str(toml_path),
                "--opponent",
                "NOPE",
                "--skip-ingest",
            ],
        )

        assert result.exit_code != 0
        message = str(result.exception) if result.exception else result.output
        assert "NOPE" in message

    def test_successful_run_echoes_report_run_latest_notice_and_duration_block(
        self, tmp_path: Path
    ) -> None:
        config = _promoted_config(tmp_path)
        _write_group_opponents(config.reference.group_opponents.parent, [])
        _write_player_mapping(config.reference.player_mapping.parent)
        plays = tms._training_corpus()
        config.paths.processed.mkdir(parents=True, exist_ok=True)
        plays.write_parquet(config.paths.processed / "plays.parquet")
        toml_path = _write_toml(config, tmp_path)

        result = _CLI_RUNNER.invoke(
            app,
            [
                "report",
                "--config",
                str(toml_path),
                "--product",
                "decisions",
                "--skip-ingest",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "report: " in result.output
        assert "run:" in result.output
        assert "latest:" in result.output
        for stage in ("ingest", "score", "report", "total"):
            assert stage in result.output


# --- end-to-end: synthetic raw tree -> ffep report -> written HTML on disk (task 3) ---------

_MAPPED_RECEIVER = "Anna Mapped"
_UNMAPPED_RECEIVER = "Unmapped Spielerin"
_RICH_HUDL_COLUMNS = [
    "PLAY #",
    "ODK",
    "DN",
    "DIST",
    "YARD LN",
    "PLAY TYPE",
    "RESULT",
    "GN/LS",
    "OFF FORM",
    "OFF PLAY",
    "TARGET ROUTE",
    "THROWN BY",
    "RECEIVED BY",
    "YAC",
]


def _write_rich_hudl_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [";".join(_RICH_HUDL_COLUMNS)]
    for row in rows:
        lines.append(";".join(str(row.get(c, "")) for c in _RICH_HUDL_COLUMNS))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def _write_rich_drive_game(
    hudl_dir: Path, filename: str, drives: list[tuple[str, int]]
) -> int:
    """Same drive shape/play-count totals as `test_cli_run._write_drive_game` (score/turnover
    drives, alternating 1-pt/2-pt PATs), with rich charting columns populated on every offensive
    play -- so the opponent/own-team reports have real formation/route/player data to render,
    and the unmapped-player-warning path (`_UNMAPPED_RECEIVER`, deliberately absent from the
    seeded player-mapping CSV) is exercised end to end. Returns the total play count written."""
    rows: list[dict] = []
    play_num = 1
    yard = 20
    turnover_idx = 0
    score_idx = 0
    for kind, n_plays in drives:
        if kind == "score":
            odk = "O"
        else:
            odk = "O" if turnover_idx % 2 == 0 else "D"
            turnover_idx += 1
        for i in range(n_plays):
            is_last = i == n_plays - 1
            if is_last and kind == "score":
                result = "Rush, TD"
            elif is_last and kind == "turnover":
                result = "Rush, Fumble"
            else:
                result = "Rush"
            receiver = _MAPPED_RECEIVER if play_num % 2 == 0 else _UNMAPPED_RECEIVER
            rows.append(
                {
                    "PLAY #": str(play_num),
                    "ODK": odk,
                    "DN": str((i % 4) + 1),
                    "DIST": "10",
                    "YARD LN": str(yard),
                    "PLAY TYPE": "Rush",
                    "RESULT": result,
                    "GN/LS": "5",
                    "OFF FORM": "Trips" if play_num % 2 == 0 else "Doubles",
                    "OFF PLAY": "Inside Zone",
                    "TARGET ROUTE": "Slant" if play_num % 2 == 0 else "Go",
                    "THROWN BY": "QB Eins",
                    "RECEIVED BY": receiver,
                    "YAC": "3",
                }
            )
            play_num += 1
            yard = min(yard + 5, 45)
        if kind == "score":
            if score_idx % 2 == 0:
                pat_dist, pat_yard = "3", "5"
            else:
                pat_dist, pat_yard = "8", "10"
            score_idx += 1
            rows.append(
                {
                    "PLAY #": str(play_num),
                    "ODK": "O",
                    "DN": "0",
                    "DIST": pat_dist,
                    "YARD LN": pat_yard,
                    "PLAY TYPE": "PAT",
                    "RESULT": "Good",
                    "GN/LS": "0",
                }
            )
            play_num += 1
    _write_rich_hudl_csv(hudl_dir / filename, rows)
    return play_num - 1


def _write_rich_training_tree(config: Config) -> None:
    """Writes `test_cli_run._GAMES`' three-game shape onto `config`'s raw Hudl directory via
    `_write_rich_drive_game`, plus matching `half_boundaries.csv`/`final_scores.csv` entries --
    mirrors `test_cli_run._write_training_tree` exactly (same totals, same helper reused for
    the final-score point count), so the corpus trains a real (if tiny) EP/WP fit."""
    tpi._write_reference_csvs(config.reference.half_boundaries.parent)

    hb_lines = []
    fs_lines = []
    for filename, halves in tcr._GAMES.items():
        all_drives = halves["half1"] + halves["half2"]
        half1_len = _write_rich_drive_game(
            config.paths.raw_hudl, filename, halves["half1"]
        )
        _write_rich_drive_game(config.paths.raw_hudl, filename, all_drives)
        n_points = tcr._expected_offense_points(all_drives)
        hb_lines.append(f"{filename},{half1_len + 1}\n")
        fs_lines.append(f"{filename.removesuffix('.csv')},GER,AUT,{n_points},0,test\n")

    hb_path = config.reference.half_boundaries.parent / "half_boundaries.csv"
    hb_path.write_text(hb_path.read_text() + "".join(hb_lines))
    fs_path = config.reference.final_scores.parent / "final_scores.csv"
    fs_path.write_text(fs_path.read_text() + "".join(fs_lines))


def _report_ready_toml(tmp_path: Path, repo_root: Path) -> Path:
    """Build the full synthetic raw tree, train + promote both models once, seed the
    group-opponents/player-mapping reference CSVs, and write the matching `ffep.toml` --
    everything `ffep report --config ...` needs to run for real, end to end."""
    config = tpi._make_config(tmp_path, repo_root)
    config = replace(
        config, report=ReportSettings(own_team="GER", cycle_start_season=2020)
    )

    _write_rich_training_tree(config)
    _write_group_opponents(
        config.reference.group_opponents.parent, [("AUT", "Austria")]
    )
    _write_player_mapping(
        config.reference.player_mapping.parent,
        [("hudl", "QB Eins", "QB Eins"), ("hudl", _MAPPED_RECEIVER, _MAPPED_RECEIVER)],
    )

    from flag_football_ep.model.train import train_ep, train_wp
    from flag_football_ep.pipeline import run_ingest

    run_ingest(config, ["hudl"])
    plays = pl.read_parquet(config.paths.processed / "plays.parquet")

    ep_run = train_ep(plays, config)
    mlflow_store.ensure_experiment(config.train.ep_experiment, config)
    tms._register_run(config, ep_run, "ep")
    registry.promote(registry.registered_model_name("ep"), ep_run, config)

    wp_run = train_wp(plays, config)
    mlflow_store.ensure_experiment(config.train.wp_experiment, config)
    tms._register_run(config, wp_run, "wp")
    registry.promote(registry.registered_model_name("wp"), wp_run, config)

    return _write_toml(config, tmp_path)


_HTTP_URL_RE = re.compile(r"https?://")
_MAX_REPORT_BYTES = 8 * 1024 * 1024


def _assert_self_contained_html(path: Path) -> None:
    content = path.read_text(encoding="utf-8")
    assert content.startswith("<!DOCTYPE html"), f"{path} does not start with a doctype"
    assert "data:image/png;base64," in content, f"{path} has no embedded chart"
    assert "<script" not in content.lower(), f"{path} contains a <script> tag"
    assert not _HTTP_URL_RE.search(content), (
        f"{path} references an external http(s) URL"
    )
    assert path.stat().st_size < _MAX_REPORT_BYTES, f"{path} exceeds the 8 MB budget"


class TestReportEndToEnd:
    def test_full_run_writes_self_contained_reports_with_no_network(
        self, tmp_path: Path, repo_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        toml_path = _report_ready_toml(tmp_path, repo_root)

        def _no_network(*args: object, **kwargs: object) -> None:
            raise AssertionError("ffep report must never attempt a network connection")

        monkeypatch.setattr(socket.socket, "connect", _no_network)

        result = _CLI_RUNNER.invoke(app, ["report", "--config", str(toml_path)])
        assert result.exit_code == 0, result.output

        reports_root = tmp_path / "reports"
        run_dir = reports_root / date.today().isoformat()
        latest_dir = reports_root / "latest"
        assert run_dir.is_dir()
        assert latest_dir.is_dir()

        run_files = {p.name for p in run_dir.iterdir()}
        latest_files = {p.name for p in latest_dir.iterdir()}
        assert run_files == latest_files
        assert run_files  # never an empty run

        expected = {
            opponent_filename("AUT"),
            OWN_TEAM_FILENAME,
            DECISIONS_FILENAME,
        }
        assert expected <= run_files
        wp_review_files = [n for n in run_files if n.startswith("wp-review-")]
        assert len(wp_review_files) == len(tcr._GAMES)

        for name in run_files:
            _assert_self_contained_html(run_dir / name)

        own_team_html = (run_dir / OWN_TEAM_FILENAME).read_text(encoding="utf-8")
        assert _UNMAPPED_RECEIVER in own_team_html
        assert "Nicht zugeordnete Spielernamen" in own_team_html

        for name in wp_review_files:
            page = (run_dir / name).read_text(encoding="utf-8")
            assert "out-of-fold" in page or "Champion-Modell" in page
            assert "WP-Provenienz unbekannt" not in page

    def test_opponent_flag_restricts_to_that_opponent_plus_non_opponent_products(
        self, tmp_path: Path, repo_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        toml_path = _report_ready_toml(tmp_path, repo_root)
        monkeypatch.setattr(
            socket.socket,
            "connect",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("no network")),
        )

        result = _CLI_RUNNER.invoke(
            app, ["report", "--config", str(toml_path), "--opponent", "AUT"]
        )
        assert result.exit_code == 0, result.output

        latest_dir = tmp_path / "reports" / "latest"
        files = {p.name for p in latest_dir.iterdir()}
        opponent_files = {n for n in files if n.startswith("opponent-")}
        assert opponent_files == {opponent_filename("AUT")}
        assert OWN_TEAM_FILENAME in files
        assert DECISIONS_FILENAME in files
        assert any(n.startswith("wp-review-") for n in files)

    def test_second_run_replaces_latest_rather_than_merging(
        self, tmp_path: Path, repo_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        toml_path = _report_ready_toml(tmp_path, repo_root)
        monkeypatch.setattr(
            socket.socket,
            "connect",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("no network")),
        )

        first = _CLI_RUNNER.invoke(app, ["report", "--config", str(toml_path)])
        assert first.exit_code == 0, first.output

        latest_dir = tmp_path / "reports" / "latest"
        stray = latest_dir / "stray-leftover.html"
        stray.write_text("<!DOCTYPE html><html></html>", encoding="utf-8")
        assert stray.exists()

        second = _CLI_RUNNER.invoke(app, ["report", "--config", str(toml_path)])
        assert second.exit_code == 0, second.output

        assert not stray.exists()
        run_dir = tmp_path / "reports" / date.today().isoformat()
        run_files = {p.name for p in run_dir.iterdir()}
        latest_files = {p.name for p in latest_dir.iterdir()}
        assert run_files == latest_files
