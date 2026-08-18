"""Cross-source contract test for the canonical `play_type` column.

Guards the canonical-schema promise ("a column means the same thing across
sources") for `play_type` specifically: `canonical.PLAY_TYPE_VOCABULARY` is
the single definition of the column's value set, and every ingest module
(hudl, legacy, sportapp, and the grandfathered WC24 rows) must emit only
values from that set. WR-11 found sportapp emitting its own "rush" synonym
for what every other source calls "run" -- silently dropping every sportapp
run play from any downstream `play_type == "run"` filter. A future source
that wants a new play_type value must extend `PLAY_TYPE_VOCABULARY`
deliberately; this test fails loudly if it instead emits an unlisted value.

Kept to on-disk fixtures already used elsewhere in the suite (the committed
sportapp fixture pair, plus the session `hudl_sample_paths` fixture, which
skips gracefully when no contract-named Hudl exports are present locally) --
no new data files, no MLflow, no model stages.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from flag_football_ep.canonical import PLAY_TYPE_VOCABULARY
from flag_football_ep.ingest.hudl import ingest_file
from flag_football_ep.ingest.sportapp import ingest_snapshots
from flag_football_ep.validation.schema import load_contract

SPORTAPP_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sportapp"


def test_play_type_vocabulary_equals_the_expected_set():
    assert PLAY_TYPE_VOCABULARY == frozenset(
        {"run", "pass", "no_play", "qb_kneel", "extra_point", "kickoff"}
    )


def test_hudl_and_sportapp_emitted_play_types_are_a_subset_of_the_vocabulary(
    hudl_sample_paths, contract_path
):
    contract = load_contract(contract_path)

    hudl_play_types: set[str] = set()
    for path in hudl_sample_paths:
        df, _notices = ingest_file(path, contract)
        hudl_play_types |= set(df["play_type"].drop_nulls().unique().to_list())

    team_mapping = pl.DataFrame(
        {
            "source": ["sportapp", "sportapp"],
            "source_team": ["101", "102"],
            "canonical_team": ["HOM", "AWY"],
        }
    )
    results = ingest_snapshots(SPORTAPP_FIXTURE_DIR, team_mapping, game_ids=["TEST001"])
    _game_id, sportapp_df, notices = results[0]
    assert notices == []
    sportapp_play_types = set(sportapp_df["play_type"].drop_nulls().unique().to_list())

    emitted = hudl_play_types | sportapp_play_types
    assert emitted, "expected at least one non-null play_type from the fixtures"
    assert emitted <= PLAY_TYPE_VOCABULARY
    assert "rush" not in emitted


def test_ifaf_emitted_play_types_are_a_subset_of_the_vocabulary():
    # REVIEW WR-04: IFAF used to emit null play_type for every row, so this
    # contract test was blind to the source. The committed sample payload must
    # now yield at least one non-null canonical play_type.
    import json

    from flag_football_ep.ingest.ifaf import derive_outcome_columns, flatten_unified_plays

    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "ifaf" / "unified-plays_sample.json").read_text(
            encoding="utf-8"
        )
    )
    game_meta = {"home_team": "w-usa", "away_team": "w-ger"}
    df = derive_outcome_columns(flatten_unified_plays(payload, game_meta, "g1"))
    ifaf_play_types = set(df["play_type"].drop_nulls().unique().to_list())
    assert ifaf_play_types, "expected at least one non-null play_type from the IFAF sample"
    assert ifaf_play_types <= PLAY_TYPE_VOCABULARY
