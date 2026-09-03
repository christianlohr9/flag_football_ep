"""Tests for `flag_football_ep.ingest.hc_dedupe`: declared-pair + fingerprint
dedupe of head-coach workbook rows against the already-ingested corpus
(HC-02, HC-D03).

Corpus/HC frames are built with `flag_football_ep.testing.canonical_plays`
(synthetic team codes, no real names); `hc_games` frames are built inline
with only the two columns `dedupe_hc_rows` reads (`game_id`,
`corpus_game_id`) -- everything else in the real 14-column schema is
irrelevant to this module.
"""

from __future__ import annotations

import polars as pl

from flag_football_ep.ingest.hc_dedupe import (
    FINGERPRINT_COLUMNS,
    DedupeReport,
    add_fingerprint,
    dedupe_hc_rows,
)
from flag_football_ep.testing import canonical_plays


def _hc_games(rows: list[dict]) -> pl.DataFrame:
    """Minimal `hc_games`-shaped frame: only `game_id`/`corpus_game_id` matter here."""
    if not rows:
        return pl.DataFrame(schema={"game_id": pl.Utf8, "corpus_game_id": pl.Utf8})
    return pl.DataFrame(rows, schema={"game_id": pl.Utf8, "corpus_game_id": pl.Utf8})


def test_fingerprint_columns_is_the_documented_seven_column_tuple() -> None:
    assert FINGERPRINT_COLUMNS == (
        "down",
        "yards_to_go",
        "yardline",
        "result_raw",
        "yards_gained",
        "received_by",
        "thrown_by",
    )


def test_add_fingerprint_nulls_are_part_of_identity_not_a_wildcard() -> None:
    df = canonical_plays(n_games=1, plays_per_game=2, source="hudl")
    fp = add_fingerprint(df)
    assert "_hc_dedupe_fingerprint" in fp.columns
    # Two rows that are both null in a fingerprint column must fingerprint
    # identically on that part (sentinel, not two different "null" values).
    both_null = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("received_by"))
    fp2 = add_fingerprint(both_null)
    values = fp2["_hc_dedupe_fingerprint"].to_list()
    assert all("~" in v for v in values)


def test_declared_pair_excludes_only_fingerprint_matches_reports_counts() -> None:
    corpus = canonical_plays(
        n_games=1,
        plays_per_game=3,
        source="hudl",
        overrides={"down": [1, 2, 3]},
        extras={"yards_gained": [5, 5, 5], "received_by": ["A", "B", "C"], "thrown_by": ["QB", "QB", "QB"]},
    )
    corpus_game_id = corpus["game_id"][0]

    # HC game: row 0 duplicates corpus row 0 (down=1), row 1 does not (down=9, unique).
    hc = canonical_plays(
        n_games=1,
        plays_per_game=2,
        source="hc_workbook:wb:sheet",
        overrides={"down": [1, 9]},
        extras={"yards_gained": [5, 99], "received_by": ["A", "Z"], "thrown_by": ["QB", "QB"]},
    )
    hc_game_id = hc["game_id"][0]

    hc_games = _hc_games([{"game_id": hc_game_id, "corpus_game_id": corpus_game_id}])

    kept, report = dedupe_hc_rows(hc, corpus, hc_games)

    assert isinstance(report, DedupeReport)
    assert kept.height == 1
    assert report.n_excluded == 1
    assert report.n_hc_rows == 2
    assert len(report.pairs) == 1
    pair = report.pairs[0]
    assert pair["hc_game_id"] == hc_game_id
    assert pair["partner_game_id"] == corpus_game_id
    assert pair["n_hc"] == 2
    assert pair["n_matched"] == 1
    assert pair["n_unmatched"] == 1
    # the surviving row is the one that did not match
    assert kept["down"].to_list() == [9]


def test_empty_corpus_game_id_loses_no_rows() -> None:
    corpus = canonical_plays(n_games=1, plays_per_game=3, source="hudl")
    hc = canonical_plays(n_games=1, plays_per_game=3, source="hc_workbook:wb:sheet")

    # no hc_games row at all for this game -- declared pairing absent
    kept, report = dedupe_hc_rows(hc, corpus, _hc_games([]))

    assert kept.height == hc.height
    assert report.n_excluded == 0
    assert report.pairs == []


def test_cross_game_overlap_reported_not_excluded() -> None:
    corpus = canonical_plays(
        n_games=2,
        plays_per_game=2,
        source="hudl",
        overrides={"down": [1, 2, 1, 2]},
        extras={
            "yards_gained": [7, 7, 7, 7],
            "received_by": ["X", "X", "X", "X"],
            "thrown_by": ["Q", "Q", "Q", "Q"],
        },
    )
    game_ids = corpus["game_id"].unique(maintain_order=True).to_list()
    declared_partner, undeclared_other = game_ids[0], game_ids[1]

    # HC game's rows fingerprint-match BOTH corpus games identically (same
    # down/yards_gained/received_by/thrown_by content on both sides), but
    # only declared_partner is named as the pairing.
    hc = canonical_plays(
        n_games=1,
        plays_per_game=2,
        source="hc_workbook:wb:sheet",
        overrides={"down": [1, 2]},
        extras={"yards_gained": [7, 7], "received_by": ["X", "X"], "thrown_by": ["Q", "Q"]},
    )
    hc_game_id = hc["game_id"][0]

    hc_games = _hc_games([{"game_id": hc_game_id, "corpus_game_id": declared_partner}])

    kept, report = dedupe_hc_rows(hc, corpus, hc_games)

    # excluded via the declared pair only
    assert kept.height == 0
    assert report.n_excluded == 2

    # the undeclared match is reported, not reflected in any exclusion
    assert len(report.cross_game_overlaps) == 1
    overlap = report.cross_game_overlaps[0]
    assert overlap["hc_game_id"] == hc_game_id
    assert overlap["other_game_id"] == undeclared_other
    assert overlap["n_matching"] == 2


def test_pairing_between_two_hc_games_in_same_frame() -> None:
    # An unrelated corpus game keeps corpus_df non-empty (required for this
    # module to run at all -- see the empty-frame short-circuit test below).
    corpus = canonical_plays(n_games=1, plays_per_game=1, source="hudl")

    hc = pl.concat(
        [
            canonical_plays(
                n_games=1,
                plays_per_game=2,
                source="hc_workbook:wb:data",
                overrides={"down": [1, 2]},
                extras={
                    "yards_gained": [4, 4],
                    "received_by": ["P", "P"],
                    "thrown_by": ["Q", "Q"],
                },
            ),
            canonical_plays(
                n_games=1,
                plays_per_game=1,
                source="hc_workbook:wb:copy-of-data",
                overrides={"down": [1]},
                extras={"yards_gained": [4], "received_by": ["P"], "thrown_by": ["Q"]},
            ),
        ],
        how="vertical",
    )
    game_ids = hc["game_id"].unique(maintain_order=True).to_list()
    data_game_id, copy_game_id = game_ids[0], game_ids[1]

    hc_games = _hc_games([{"game_id": copy_game_id, "corpus_game_id": data_game_id}])

    kept, report = dedupe_hc_rows(hc, corpus, hc_games)

    # the Copy-of-Data game's one row duplicates a Data-game row -> excluded
    assert report.n_excluded == 1
    assert kept.height == hc.height - 1
    assert copy_game_id not in kept["game_id"].unique().to_list()
    assert data_game_id in kept["game_id"].unique().to_list()


def test_intra_game_duplicate_reported_never_excluded() -> None:
    corpus = canonical_plays(n_games=1, plays_per_game=1, source="hudl")

    # Two identical rows inside the same HC game, no declared pairing at all.
    hc = canonical_plays(
        n_games=1,
        plays_per_game=2,
        source="hc_workbook:wb:sheet",
        overrides={"down": [1, 1], "yardline": [7, 7], "yards_to_go": [10, 10]},
        extras={"yards_gained": [3, 3], "received_by": ["A", "A"], "thrown_by": ["Q", "Q"]},
    )

    kept, report = dedupe_hc_rows(hc, corpus, _hc_games([]))

    assert kept.height == 2  # neither row excluded
    assert report.n_excluded == 0
    assert any("mehrfach innerhalb desselben Spiels" in m for m in report.messages)


def test_row_conservation_kept_plus_excluded_equals_input() -> None:
    corpus = canonical_plays(
        n_games=1,
        plays_per_game=4,
        source="hudl",
        overrides={"down": [1, 2, 3, 4]},
        extras={
            "yards_gained": [1, 2, 3, 4],
            "received_by": ["A", "B", "C", "D"],
            "thrown_by": ["Q", "Q", "Q", "Q"],
        },
    )
    corpus_game_id = corpus["game_id"][0]
    hc = canonical_plays(
        n_games=1,
        plays_per_game=4,
        source="hc_workbook:wb:sheet",
        overrides={"down": [1, 2, 99, 99]},
        extras={
            "yards_gained": [1, 2, 0, 0],
            "received_by": ["A", "B", "Z", "Z"],
            "thrown_by": ["Q", "Q", "Q", "Q"],
        },
    )
    hc_games = _hc_games([{"game_id": hc["game_id"][0], "corpus_game_id": corpus_game_id}])

    kept, report = dedupe_hc_rows(hc, corpus, hc_games)

    assert kept.height + report.n_excluded == hc.height


def test_empty_hc_frame_returns_input_unchanged_no_exception() -> None:
    corpus = canonical_plays(n_games=1, plays_per_game=2, source="hudl")
    empty_hc = corpus.clear()

    kept, report = dedupe_hc_rows(empty_hc, corpus, _hc_games([]))

    assert kept.height == 0
    assert report.n_hc_rows == 0
    assert report.pairs == []
    assert report.cross_game_overlaps == []


def test_empty_corpus_frame_returns_input_unchanged_no_exception() -> None:
    hc = canonical_plays(n_games=1, plays_per_game=2, source="hc_workbook:wb:sheet")
    empty_corpus = hc.clear()

    kept, report = dedupe_hc_rows(hc, empty_corpus, _hc_games([]))

    assert kept.height == hc.height
    assert report.n_excluded == 0


def test_summary_lines_never_contain_a_player_label_only_counts() -> None:
    corpus = canonical_plays(
        n_games=1,
        plays_per_game=1,
        source="hudl",
        overrides={"down": [1]},
        extras={"yards_gained": [5], "received_by": ["Spieler X"], "thrown_by": ["QB"]},
    )
    corpus_game_id = corpus["game_id"][0]
    hc = canonical_plays(
        n_games=1,
        plays_per_game=1,
        source="hc_workbook:wb:sheet",
        overrides={"down": [1]},
        extras={"yards_gained": [5], "received_by": ["Spieler X"], "thrown_by": ["QB"]},
    )
    hc_games = _hc_games([{"game_id": hc["game_id"][0], "corpus_game_id": corpus_game_id}])

    _, report = dedupe_hc_rows(hc, corpus, hc_games)
    joined = " ".join(report.summary_lines())

    assert "Spieler X" not in joined
    assert str(report.n_excluded) in joined
