"""Tests for scripts/explosiveness_comparison.py's tier-exclusion scope filter.

2026-09-06 addendum: the IFAF ffwc26-men tournament (competition_tier
mens-international) must never enter the M3 explosiveness/comparison
corpus -- m-ger/w-ger both map to canonical team code GER (data/reference/
team_mapping.csv), so team-code filtering alone would silently mix men's
and women's national-team rows.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import explosiveness_comparison as ec  # noqa: E402


@dataclass
class _FakeReferenceConfig:
    competition_tier: Path


@dataclass
class _FakeConfig:
    reference: _FakeReferenceConfig


def _write_tier_csv(tmp_path: Path) -> Path:
    path = tmp_path / "competition_tier.csv"
    path.write_text(
        "source,competition,tier\n"
        "ifaf,IFAF World Flag 2026 Women,womens-international\n"
        "ifaf,IFAF World Flag 2026 Men,mens-international\n"
        "legacy,legacy,mixed-other\n",
        encoding="utf-8",
    )
    return path


def _plays(rows):
    return pl.DataFrame(rows)


def test_exclude_tiers_drops_mens_international_rows(tmp_path):
    tier_path = _write_tier_csv(tmp_path)
    config = _FakeConfig(reference=_FakeReferenceConfig(competition_tier=tier_path))

    plays = _plays(
        [
            {"source": "ifaf", "competition": "IFAF World Flag 2026 Women", "game_id": "ifaf-w1"},
            {"source": "ifaf", "competition": "IFAF World Flag 2026 Men", "game_id": "ifaf-m1"},
            {"source": "ifaf", "competition": "IFAF World Flag 2026 Men", "game_id": "ifaf-m1"},
        ]
    )

    result = ec._exclude_tiers(plays, config)

    assert result["game_id"].to_list() == ["ifaf-w1"]
    assert "tier" not in result.columns


def test_exclude_tiers_keeps_other_sources_and_tiers_untouched(tmp_path):
    tier_path = _write_tier_csv(tmp_path)
    config = _FakeConfig(reference=_FakeReferenceConfig(competition_tier=tier_path))

    plays = _plays(
        [
            {"source": "legacy", "competition": "legacy", "game_id": "legacy-1"},
            {"source": "ifaf", "competition": "IFAF World Flag 2026 Women", "game_id": "ifaf-w1"},
        ]
    )

    result = ec._exclude_tiers(plays, config)

    assert set(result["game_id"].to_list()) == {"legacy-1", "ifaf-w1"}


def test_exclude_tiers_reports_excluded_row_and_game_count(tmp_path, capsys):
    tier_path = _write_tier_csv(tmp_path)
    config = _FakeConfig(reference=_FakeReferenceConfig(competition_tier=tier_path))

    plays = _plays(
        [
            {"source": "ifaf", "competition": "IFAF World Flag 2026 Men", "game_id": "ifaf-m1"},
            {"source": "ifaf", "competition": "IFAF World Flag 2026 Men", "game_id": "ifaf-m1"},
            {"source": "ifaf", "competition": "IFAF World Flag 2026 Men", "game_id": "ifaf-m2"},
        ]
    )

    ec._exclude_tiers(plays, config)
    captured = capsys.readouterr()

    assert "excluding 3 row(s) across 2 game(s)" in captured.out
    assert "mens-international" in captured.out


def test_exclude_tiers_missing_source_or_competition_column_is_noop(tmp_path):
    tier_path = _write_tier_csv(tmp_path)
    config = _FakeConfig(reference=_FakeReferenceConfig(competition_tier=tier_path))

    plays = pl.DataFrame({"game_id": ["x1", "x2"]})
    result = ec._exclude_tiers(plays, config)
    assert result["game_id"].to_list() == ["x1", "x2"]


def test_excluded_tiers_constant_contains_mens_international():
    assert "mens-international" in ec.EXCLUDED_TIERS
