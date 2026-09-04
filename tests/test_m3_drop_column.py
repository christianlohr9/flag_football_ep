"""Cross-layer proof for M3-04-06: the `drop` canonical extra (Data!W, the head
coach's "Drop" column) survives `conform_to_canonical` unchanged, and -- once
`flag_football_ep.reports.player_analysis` exists -- unblocks `Adj Comp %`,
`adj Pass Yards` and `adj YPA` with no report-code change (M3-04-03's design).

This plan (M3-04-06) was executed ahead of its nominal wave, file-disjoint from
plans 03/04 (`reports/player_analysis.py` is owned there and is READ-ONLY for
this plan's `file_collision_guard`). At the time this file was written,
`flag_football_ep.reports.player_analysis` does not exist yet in this worktree
-- `hc_columns_by_qb`/`HcColumnTable` below are guarded with
`pytest.importorskip`, so they activate automatically once that module lands
(same worktree merge or a later `uv run pytest -q`) instead of being faked or
silently omitted. Their bodies are written against the exact interface
`M3-04-03-PLAN.md` specifies (`HcColumnTable.table`/`.unavailable`/`.notices`,
`hc_columns_by_qb(plays, *, group_col="thrown_by")`), so they are a real
contract check the moment the module exists, not a placeholder.

All fixtures are synthetic (`flag_football_ep.testing` factories plus
`with_columns`/dict overrides) -- no real workbook, no real corpus, no
`data/**` write, per this plan's `<pii_discipline>` and `file_collision_guard`.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.canonical import conform_to_canonical
from flag_football_ep.testing import canonical_plays

# --- Bullet 1: `drop` survives conform_to_canonical -------------------------
# (does not need flag_football_ep.reports.player_analysis at all -- this is
# the pure Task 1 proof, exercised again here as the cross-layer fixture that
# bullets 2/3 build on.)


def test_drop_extra_survives_conform_to_canonical_with_raw_text_preserved() -> None:
    """A raw HC-shaped frame carrying a `drop` column (post header-mapping,
    Task 1's `_HC_ONLY_RENAME["DROP"]`) keeps its charted text verbatim after
    `conform_to_canonical`, and is not re-materialized as a fresh null."""
    df = canonical_plays(
        n_games=1,
        plays_per_game=4,
        extras={"drop": ["X", None, "", "X"]},
    )

    conformed, report = conform_to_canonical(df, source="test")

    assert conformed["drop"].to_list() == ["X", None, "", "X"]
    assert "drop" not in report.materialized_extras


def test_drop_extra_materialized_null_when_column_absent_from_source() -> None:
    """The mirror case: a source frame with no `drop` column at all still
    conforms -- `drop` is a typed null, named in `materialized_extras`,
    exactly like the four M3-01-02 extras behave (`bf_action`, `hand`,
    `air_yards`, `efficiency`) when their header is absent."""
    df = canonical_plays(n_games=1, plays_per_game=4).drop("drop")
    assert "drop" not in df.columns  # sanity: the factory really omitted it

    conformed, report = conform_to_canonical(df, source="test")

    assert "drop" in conformed.columns
    assert conformed["drop"].null_count() == conformed.height
    assert "drop" in report.materialized_extras


# --- Bullets 2/3: the report layer picks it up with no code change ----------
# Guarded: flag_football_ep.reports.player_analysis is plan M3-04-03/04's
# deliverable, not this plan's. These tests activate the moment that module
# exists (import succeeds); until then they are skipped by name, not faked.

try:
    from flag_football_ep.reports.player_analysis import hc_columns_by_qb

    _HAS_PLAYER_ANALYSIS = True
except ImportError:
    hc_columns_by_qb = None  # type: ignore[assignment]
    _HAS_PLAYER_ANALYSIS = False

_MISSING_PLAYER_ANALYSIS_REASON = (
    "flag_football_ep.reports.player_analysis is M3-04-03/04's deliverable and "
    "does not exist yet in this worktree (M3-04-06 runs ahead of its nominal "
    "wave, file-disjoint from plans 03/04). This test activates automatically "
    "once that module lands."
)
_skip_without_player_analysis = pytest.mark.skipif(
    not _HAS_PLAYER_ANALYSIS, reason=_MISSING_PLAYER_ANALYSIS_REASON
)

_ADJUSTED_COLUMNS = ("adj_comp_pct", "adj_pass_yards", "adj_ypa")


def _qb_fixture(*, with_drop: bool) -> pl.DataFrame:
    """One QB, two pass plays: one completion (10 yards), one incompletion
    the head coach charted as dropped (`drop="X"`, `air_yards=7`). With
    `with_drop=True` this reproduces his `Adj Comp %`/`adj Pass Yards`
    formula's numerator on a fixture with at least one dropped incompletion
    (M3-04-RESEARCH Pattern 1, `G2`/`N2`); with `with_drop=False` the `drop`
    column is removed entirely (not merely null-valued) to simulate a corpus
    with no HC rows -- the state every real ingest run produced before this
    plan (M3-02-04-SUMMARY.md: HC rows exist, but with no `drop` mapping)."""
    df = canonical_plays(
        n_games=1,
        plays_per_game=2,
        overrides={
            "complete_pass": [1, 0],
            "yards_gained": [10, 0],
        },
        extras={
            "thrown_by": ["Spieler A", "Spieler A"],
            "drop": [None, "X"],
            "air_yards": [None, 7],
        },
    )
    if not with_drop:
        df = df.drop("drop")
    return df


@_skip_without_player_analysis
def test_adjusted_columns_available_and_correct_when_drop_present() -> None:
    plays = _qb_fixture(with_drop=True)

    result = hc_columns_by_qb(plays, group_col="thrown_by")

    for column in _ADJUSTED_COLUMNS:
        assert column not in result.unavailable, (
            f"{column} unexpectedly still unavailable with drop present: "
            f"{result.notices}"
        )

    row = result.table.filter(pl.col("spieler") == "Spieler A")
    assert row.height == 1
    comp_pct = row["comp_pct"][0]
    adj_comp_pct = row["adj_comp_pct"][0]
    assert adj_comp_pct is not None and comp_pct is not None
    # the workbook's G2 reading (comps + dropped incompletions) / attempts --
    # strictly greater than the unadjusted comp_pct, never a copy of it.
    assert adj_comp_pct > comp_pct

    pass_yards = row["pass_yards"][0]
    adj_pass_yards = row["adj_pass_yards"][0]
    assert adj_pass_yards is not None and pass_yards is not None
    assert adj_pass_yards > pass_yards


@_skip_without_player_analysis
def test_adjusted_columns_named_unavailable_when_drop_column_absent() -> None:
    plays = _qb_fixture(with_drop=False)
    assert "drop" not in plays.columns

    result = hc_columns_by_qb(plays, group_col="thrown_by")

    for column in _ADJUSTED_COLUMNS:
        assert column in result.unavailable
    joined_notices = " ".join(result.notices)
    assert joined_notices, "expected at least one German notice naming the unavailable state"

    row = result.table.filter(pl.col("spieler") == "Spieler A")
    assert row.height == 1
    for column in _ADJUSTED_COLUMNS:
        assert row[column][0] is None
    # never silently mirrors its unadjusted neighbour
    assert row["adj_comp_pct"][0] != row["comp_pct"][0]
