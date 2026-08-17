"""Unit coverage for the Markdown validation report and console summary."""

from __future__ import annotations

from pathlib import Path

import pytest

from flag_football_ep.validation.checks import CheckResult, GameResult, Status
from flag_football_ep.validation.report import console_summary, render_report, write_report


def _cr(check: str, status: Status, detail: str = "ok", n_offending: int = 0) -> CheckResult:
    return CheckResult(game_id="g1", check=check, status=status, detail=detail, n_offending=n_offending)


_ALL_CHECK_NAMES = [
    "downs_range",
    "yardline_range",
    "half_assigned",
    "monotonic_drive_ids",
    "gapless_play_ids",
    "score_reconstruction",
]


def _passing_game(game_id: str, source: str = "hudl") -> GameResult:
    results = [
        CheckResult(game_id=game_id, check=name, status=Status.PASS, detail="ok", n_offending=0)
        for name in _ALL_CHECK_NAMES
    ]
    return GameResult(game_id=game_id, source=source, results=results, quarantined=False, reasons=[])


def _quarantined_game(game_id: str, source: str = "hudl") -> GameResult:
    results = [
        CheckResult(game_id=game_id, check=name, status=Status.PASS, detail="ok", n_offending=0)
        for name in _ALL_CHECK_NAMES
        if name != "gapless_play_ids"
    ]
    results.append(
        CheckResult(
            game_id=game_id,
            check="gapless_play_ids",
            status=Status.FAIL,
            detail="missing play_id(s) 5 (1 missing)",
            n_offending=1,
        )
    )
    return GameResult(
        game_id=game_id,
        source=source,
        results=results,
        quarantined=True,
        reasons=["gapless_play_ids: missing play_id(s) 5 (1 missing)"],
    )


def _skipped_score_game(game_id: str, source: str = "hudl") -> GameResult:
    results = [
        CheckResult(game_id=game_id, check=name, status=Status.PASS, detail="ok", n_offending=0)
        for name in _ALL_CHECK_NAMES
        if name != "score_reconstruction"
    ]
    results.append(
        CheckResult(
            game_id=game_id,
            check="score_reconstruction",
            status=Status.SKIPPED,
            detail="no reference entry",
            n_offending=0,
        )
    )
    return GameResult(game_id=game_id, source=source, results=results, quarantined=False, reasons=[])


class TestRenderReport:
    def test_starts_with_h1_containing_run_id(self):
        report = render_report([], notices={}, contract_version="1.1", run_id="20260817-101500")
        assert report.startswith("# ")
        assert "20260817-101500" in report.splitlines()[0]

    def test_metadata_block_lists_contract_version_and_counts(self):
        games = [_passing_game("g1"), _quarantined_game("g2")]
        report = render_report(games, notices={}, contract_version="1.1", run_id="run1")
        assert "1.1" in report
        assert "Games ingested: 2" in report
        assert "Games quarantined: 1" in report

    def test_quarantine_section_appears_before_per_game_sections(self):
        games = [_passing_game("g1"), _quarantined_game("g2")]
        report = render_report(games, notices={}, contract_version="1.1", run_id="run1")
        quarantine_idx = report.index("## Quarantined games")
        first_game_idx = report.index("### ")
        assert quarantine_idx < first_game_idx

    def test_quarantine_section_lists_game_id_and_reasons(self):
        games = [_quarantined_game("g2")]
        report = render_report(games, notices={}, contract_version="1.1", run_id="run1")
        section = report[report.index("## Quarantined games") : report.index("## Summary")]
        assert "g2" in section
        assert "gapless_play_ids" in section

    def test_no_quarantine_renders_explicit_statement(self):
        games = [_passing_game("g1")]
        report = render_report(games, notices={}, contract_version="1.1", run_id="run1")
        section = report[report.index("## Quarantined games") : report.index("## Summary")]
        assert "quarantined" in section.lower()
        assert "###" not in section

    def test_summary_table_has_one_row_per_game_and_a_column_per_check(self):
        games = [_passing_game("g1"), _quarantined_game("g2")]
        report = render_report(games, notices={}, contract_version="1.1", run_id="run1")
        summary_section = report[report.index("## Summary") : report.index("## Missing reference data")]
        for name in _ALL_CHECK_NAMES:
            assert name in summary_section
        assert "g1" in summary_section
        assert "g2" in summary_section
        assert "FAIL" in summary_section

    def test_per_game_section_lists_every_check_with_status_and_detail(self):
        games = [_passing_game("g1")]
        report = render_report(games, notices={}, contract_version="1.1", run_id="run1")
        section = report[report.index("### g1") :]
        for name in _ALL_CHECK_NAMES:
            assert name in section

    def test_per_game_section_includes_notices_when_present(self):
        games = [_passing_game("g1")]
        notices = {"g1": ["materialized optional column off_form"]}
        report = render_report(games, notices=notices, contract_version="1.1", run_id="run1")
        section = report[report.index("### g1") :]
        assert "materialized optional column off_form" in section

    def test_missing_reference_section_lists_skipped_score_game(self):
        games = [_skipped_score_game("g1")]
        report = render_report(games, notices={}, contract_version="1.1", run_id="run1")
        section = report[report.index("## Missing reference data") : report.index("### g1")]
        assert "g1" in section


class TestWriteReport:
    def test_writes_timestamped_and_latest_files_with_identical_content(self, tmp_path: Path):
        markdown = "# Test Report\n\ncontent\n"
        out_path = write_report(markdown, tmp_path, "20260817-101500")
        assert out_path == tmp_path / "validation-report-20260817-101500.md"
        assert out_path.exists()
        latest = tmp_path / "validation-report-latest.md"
        assert latest.exists()
        assert out_path.read_text() == latest.read_text() == markdown

    def test_creates_out_dir_when_absent(self, tmp_path: Path):
        out_dir = tmp_path / "nested" / "processed"
        write_report("# x\n", out_dir, "run1")
        assert (out_dir / "validation-report-run1.md").exists()


class TestConsoleSummary:
    def test_empty_list_does_not_raise(self):
        console_summary([])

    def test_runs_without_raising_for_mixed_results(self):
        console_summary([_passing_game("g1"), _quarantined_game("g2")])
