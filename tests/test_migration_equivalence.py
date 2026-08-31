"""Proves the ported package reproduces the frozen notebook's EP/WP training frames
(REQ-S1-04, CONTEXT.md's migration-equivalence test decision).

Compares the package's `model_data` frames -- built by running the same chain the
notebook ran (`ingest_legacy` -> `prepare_ep_data`/`prepare_wp_data` ->
`make_ep_model_mutations`/`make_wp_model_mutations`) on the same input
(`data/raw/legacy/data_raw.csv`) -- against `tests/fixtures/baseline_{ep,wp}_model_data.
parquet`, frozen by `scripts/capture_notebook_baseline.py` from the unmodified `Python/`
helpers (plan 01.2-02). Compares statistics (column names/order, row count, dtypes, label
distribution, per-column min/max/mean), never byte equality -- per this plan's action
block, one run should report every differing column and its delta, not just the first.

## Known, expected deltas (do not weaken the tolerance for anything else)

1. **`game_id` format** -- the legacy path's `game_id` is now `legacy-<n>` instead of a
   bare integer. Irrelevant here: `game_id` is never a model column.

2. **EP `Total_W_Scaled` is corpus-dependent** -- the baseline's EP frame was built from
   `data_raw.csv` diagonally concatenated with the WC24 corpus (`build_ep_frame` in
   `scripts/capture_notebook_baseline.py`; 01.2-11-SUMMARY.md's WC24 disposition), so its
   sample weights (`Drive_Score_Dist_W`/`ScoreDiff_W`/`Total_W_Scaled`) are computed over
   the combined corpus. This test's EP comparison runs the legacy-only chain (matching
   this plan's `<action>` instruction), so `Total_W_Scaled` is computed over a different,
   smaller corpus and is not expected to match -- documented, not asserted equal.

3. **`label` (EP and WP) and WP's `receive_2h_ko`** -- both trace back to scoring-flag
   chains that had real bugs in the frozen notebook, already fixed during the port:
   - `Python/helper_add_hudl_mutations.py`'s `scoring_play_team` derivation chains
     `pl.col("touchdown") | pl.col("one_point_conv_success") | pl.col("two_point_conv_success") == 1`
     inside a single `pl.when` -- Python/polars operator precedence binds `==` tighter
     than `|`, so this evaluates as `flag_a | flag_b | (flag_c == 1)`: only the *last*
     flag is ever compared to 1. `canonical.add_scoring_play_team` (this package's ported
     version) fixes this explicitly -- see its docstring. Every downstream chain keyed off
     `scoring_play`/`scoring_play_team` (`Next_Score_Half` -> EP `label`; `Winner` -> WP
     `label`) inherits the fix, so the frozen baseline's `label` distribution reflects the
     notebook's original mis-firing flags and will not match exactly.
   - `Python/helper_add_hudl_mutations.py`'s `prepare_wp_data` redefines `start_posteam`
     a *second* time, from scratch, without the `.forward_fill()` the first definition
     had -- so it is non-null only on each game's very first play, making
     `receive_2h_ko = when(start_posteam == posteam).then(0).otherwise(1)` evaluate to 1
     for every row except a game's first play (`otherwise` catches the null comparison).
     `features.mutations.prepare_wp_data` (this package's ported version) keeps a single,
     `.forward_fill().over("game_id")`-scoped `start_posteam`, so `receive_2h_ko` reflects
     the real per-play posteam-vs-opening-team comparison instead.
   Both are within CONTEXT.md's "obvious fixes only" porting policy (silent logic bugs,
   not methodology changes -- PAT baselines, GroupKFold etc. stay frozen for phase 1.3).
   Documented here as a discovered, explained delta rather than asserted equal.

4. **`half` and the `tier_*` competition columns (REQ-S1-09, plan 01.3-09 "Apply the
   combined feature-candidate adoption decision")** -- `EP_FEATURES` gained `half` and the
   three competition-tier one-hot columns (`tier_womens_international`, `tier_womens_national`,
   `tier_mixed_other`); `WP_FEATURES` gained the same three tier columns. These are real
   feature-set changes, not port deltas, so they are not "known deltas" of the frozen
   baseline in the sense of items 1-3 above -- but the frozen baseline fixtures
   (`tests/fixtures/baseline_{ep,wp}_model_data.parquet`) were captured before this
   adoption and never had these columns, so the tests below build them (via
   `add_competition_tier_features`, the same construction `model/train.py`'s
   `_build_competition_tier` `build_fn` hook uses in production) and compare only the
   baseline's pre-existing columns for statistics, asserting the adopted columns are
   present and well-formed separately. Measured pooled LOGO log-loss deltas (control minus
   candidate, real corpus, 01.3-07-SUMMARY.md / 01.3-09-SUMMARY.md) that justify each
   adoption:
     - `half` (EP): +0.001790 (adopted; rejected for WP at -0.002360, so `WP_FEATURES` does
       not include it)
     - `competition_tier` (EP): +0.000331 (adopted)
     - `competition_tier` (WP): +0.000023 (adopted)
     - combined EP arm (`half` + `competition_tier` together, 01.3-09-SUMMARY.md's
       combined-confirmation run): control=1.029113, combined=1.027657, delta=+0.001457
       (beats the control, so the union is adopted rather than the single best-performing
       subset)

Any delta on a column NOT in this list is a real port bug and must fail the test.

The WR-02 fix to `canonical.add_score_columns` (evaluating the scoring branch before the
play-1 zero seed, so a first-play score is no longer silently discarded) was verified to
produce zero delta on this corpus: `data_raw.csv` has 0 first plays (of 47 games) with a
non-null `scoring_play_team`, so the branch reorder never changes which value is selected
here.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep import reference
from flag_football_ep.canonical import make_game_id
from flag_football_ep.features.mutations import (
    add_competition_tier_features,
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.ingest.legacy import ingest_legacy
from flag_football_ep.model.hyperparams import (
    EP_SELECTED_COLUMNS,
    TIER_FEATURE_COLUMNS,
    WP_SELECTED_COLUMNS,
)

_FLOAT_TOL = 1e-9
_NUMERIC_DTYPES = (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64)

# Columns whose statistics are not expected to match the frozen baseline, with the reason
# recorded in the module docstring above.
_EP_KNOWN_DELTA_COLUMNS = {"label", "Total_W_Scaled"}
_WP_KNOWN_DELTA_COLUMNS = {"label", "receive_2h_ko"}

# REQ-S1-09 adoption (module docstring, known delta #4): columns `EP_FEATURES`/`WP_FEATURES`
# gained after the frozen baseline fixtures were captured. `EP_SELECTED_COLUMNS`/
# `WP_SELECTED_COLUMNS` append new features before the trailing weight column (EP) or at the
# list's end (WP), so these land in the same position in `EP_SELECTED_COLUMNS`/
# `WP_SELECTED_COLUMNS`.
_EP_ADOPTED_COLUMNS = ["half", *TIER_FEATURE_COLUMNS]
_WP_ADOPTED_COLUMNS = list(TIER_FEATURE_COLUMNS)


def _with_competition_tier(df: pl.DataFrame, repo_root: Path) -> pl.DataFrame:
    """Build the REQ-S1-09 `competition_tier` one-hot columns on `df` -- mirrors
    `model/train.py::_build_competition_tier`'s production ordering (before `prepare_fn`/
    `mutate_fn`, since the join needs the raw canonical `competition` column)."""
    mapping = reference.load_competition_tier(
        repo_root / "data" / "reference" / "competition_tier.csv"
    )
    augmented, _tier_features = add_competition_tier_features(df, mapping)
    return augmented


def _label_distribution(df: pl.DataFrame, col: str = "label") -> dict[str, int]:
    """Value counts keyed by string label (a `"null"` bucket included), sorted by key.

    Mirrors `scripts/capture_notebook_baseline.py`'s `label_distribution` so the two are
    directly comparable.
    """
    counts = df[col].value_counts()
    raw: dict[str, int] = {}
    for row in counts.iter_rows(named=True):
        key = row[col]
        raw["null" if key is None else str(key)] = int(row["count"])
    raw.setdefault("null", 0)
    sorted_keys = sorted(raw, key=lambda k: (k == "null", int(k) if k != "null" else 0))
    return {k: raw[k] for k in sorted_keys}


def _numeric_stat_mismatches(
    actual: pl.DataFrame,
    expected: pl.DataFrame,
    columns: list[str],
    skip: set[str] = frozenset(),
    tol: float = _FLOAT_TOL,
) -> list[str]:
    """Compare dtype and min/max/mean for every column in `columns` (except `skip`).

    Returns a list of human-readable mismatch descriptions -- empty when everything
    matches within `tol`. Never raises: every column is checked and every mismatch is
    reported, so one call surfaces the full delta instead of stopping at the first
    difference.
    """
    mismatches: list[str] = []
    for col in columns:
        if col in skip:
            continue
        a = actual[col]
        e = expected[col]
        if a.dtype != e.dtype:
            mismatches.append(f"{col}: dtype {a.dtype} != baseline dtype {e.dtype}")
            continue
        if a.dtype not in _NUMERIC_DTYPES:
            continue
        for stat_name, stat_fn in (("min", pl.Series.min), ("max", pl.Series.max), ("mean", pl.Series.mean)):
            actual_value = stat_fn(a)
            expected_value = stat_fn(e)
            if actual_value is None or expected_value is None:
                if actual_value != expected_value:
                    mismatches.append(
                        f"{col}.{stat_name}: {actual_value!r} != baseline {expected_value!r}"
                    )
                continue
            delta = actual_value - expected_value
            if abs(delta) > tol:
                mismatches.append(
                    f"{col}.{stat_name}: {actual_value!r} != baseline {expected_value!r} "
                    f"(delta={delta!r})"
                )
    return mismatches


def _skip_reason_if_fixtures_missing(repo_root: Path) -> str | None:
    """Return a skip reason if the baseline fixtures or the legacy CSV are unusable.

    Checked in order: manifest present, both baseline Parquet files present, legacy CSV
    present, and its sha256 matches the hash the manifest recorded (the same "frozen
    input" gate `tests/test_ingest_legacy.py`'s real-file tests use) -- else the
    comparison would be against a baseline built from different source data.
    """
    manifest_path = repo_root / "tests" / "fixtures" / "baseline_manifest.json"
    if not manifest_path.exists():
        return "tests/fixtures/baseline_manifest.json not present"

    ep_path = repo_root / "tests" / "fixtures" / "baseline_ep_model_data.parquet"
    wp_path = repo_root / "tests" / "fixtures" / "baseline_wp_model_data.parquet"
    if not ep_path.exists() or not wp_path.exists():
        return "baseline_ep_model_data.parquet / baseline_wp_model_data.parquet not present"

    legacy_csv = repo_root / "data" / "raw" / "legacy" / "data_raw.csv"
    if not legacy_csv.exists():
        return "data/raw/legacy/data_raw.csv not present"

    manifest = json.loads(manifest_path.read_text())
    expected_hash = manifest["inputs"]["legacy_csv_sha256"]
    digest = hashlib.sha256()
    with open(legacy_csv, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected_hash:
        return "data_raw.csv content differs from the frozen baseline manifest"

    return None


def _load_fixtures(repo_root: Path) -> tuple[dict, pl.DataFrame, pl.DataFrame, Path]:
    """Load the manifest and both baseline frames. Caller must have already checked
    `_skip_reason_if_fixtures_missing` returns `None`."""
    fixtures_dir = repo_root / "tests" / "fixtures"
    manifest = json.loads((fixtures_dir / "baseline_manifest.json").read_text())
    baseline_ep = pl.read_parquet(fixtures_dir / "baseline_ep_model_data.parquet")
    baseline_wp = pl.read_parquet(fixtures_dir / "baseline_wp_model_data.parquet")
    legacy_csv = repo_root / "data" / "raw" / "legacy" / "data_raw.csv"
    return manifest, baseline_ep, baseline_wp, legacy_csv


# --- self-check: the mismatch helper actually reports column + delta ----------------------


def test_numeric_stat_mismatches_names_column_and_delta_on_perturbed_frame() -> None:
    baseline = pl.DataFrame({"a": [1, 2, 3], "b": [10.0, 20.0, 30.0]})
    perturbed = pl.DataFrame({"a": [1, 2, 3], "b": [10.0, 20.0, 999.0]})

    mismatches = _numeric_stat_mismatches(perturbed, baseline, ["a", "b"])

    assert mismatches, "expected at least one mismatch for the perturbed column"
    assert any("b" in m for m in mismatches)
    assert not any(m.startswith("a.") for m in mismatches)
    # The delta itself must be discoverable from the message, not just "frames differ".
    assert any("999" in m for m in mismatches)


def test_numeric_stat_mismatches_empty_for_identical_frames() -> None:
    df = pl.DataFrame({"a": [1, 2, 3], "b": [10.0, 20.0, 30.0]})

    assert _numeric_stat_mismatches(df, df, ["a", "b"]) == []


def test_skip_reason_if_fixtures_missing_reports_missing_manifest(tmp_path: Path) -> None:
    reason = _skip_reason_if_fixtures_missing(tmp_path)

    assert reason is not None
    assert "manifest" in reason


# --- WP equivalence -------------------------------------------------------------------------


def test_wp_model_data_matches_baseline_within_tolerance(repo_root: Path) -> None:
    skip_reason = _skip_reason_if_fixtures_missing(repo_root)
    if skip_reason:
        pytest.skip(skip_reason)

    manifest, _baseline_ep, baseline_wp, legacy_csv = _load_fixtures(repo_root)

    conformed, _notices = ingest_legacy(legacy_csv, team_mapping=None)
    conformed = _with_competition_tier(conformed, repo_root)
    wp_exclude_id = make_game_id("legacy", manifest["excluded_games"]["wp"])
    wp_input = conformed.filter(pl.col("game_id") != wp_exclude_id)
    wp_prepared = prepare_wp_data(wp_input)
    wp_model_data = make_wp_model_mutations(wp_prepared, WP_SELECTED_COLUMNS)

    # REQ-S1-09 adoption (known delta #4): `WP_SELECTED_COLUMNS` gained the tier columns
    # after the baseline was captured -- the frozen baseline has none of them.
    expected_columns = [*baseline_wp.columns, *_WP_ADOPTED_COLUMNS]
    assert wp_model_data.columns == expected_columns
    assert wp_model_data.height == baseline_wp.height
    assert wp_model_data.dtypes[: len(baseline_wp.dtypes)] == baseline_wp.dtypes

    mismatches = _numeric_stat_mismatches(
        wp_model_data, baseline_wp, list(baseline_wp.columns), skip=_WP_KNOWN_DELTA_COLUMNS
    )
    assert not mismatches, "WP model_data statistics differ from the frozen baseline:\n" + "\n".join(
        mismatches
    )

    # The newly adopted tier columns themselves: well-formed one-hot Int32 indicators.
    for col in _WP_ADOPTED_COLUMNS:
        assert set(wp_model_data[col].unique().to_list()) <= {0, 1}
    tier_sum = wp_model_data.select(pl.sum_horizontal(_WP_ADOPTED_COLUMNS).alias("s"))["s"]
    assert set(tier_sum.unique().to_list()) == {1}, (
        "exactly one tier column must be 1 per row"
    )


def test_wp_label_and_receive_2h_ko_documented_delta(repo_root: Path) -> None:
    """`label`/`receive_2h_ko` are excluded from the strict comparison above -- this test
    documents the actual counts so the delta is visible (not just silently skipped), per
    the module docstring's "known, expected deltas" #3."""
    skip_reason = _skip_reason_if_fixtures_missing(repo_root)
    if skip_reason:
        pytest.skip(skip_reason)

    manifest, _baseline_ep, baseline_wp, legacy_csv = _load_fixtures(repo_root)

    conformed, _notices = ingest_legacy(legacy_csv, team_mapping=None)
    conformed = _with_competition_tier(conformed, repo_root)
    wp_exclude_id = make_game_id("legacy", manifest["excluded_games"]["wp"])
    wp_input = conformed.filter(pl.col("game_id") != wp_exclude_id)
    wp_prepared = prepare_wp_data(wp_input)
    wp_model_data = make_wp_model_mutations(wp_prepared, WP_SELECTED_COLUMNS)

    # Both columns are still well-formed booleans-as-Int32, just not baseline-identical.
    assert set(wp_model_data["receive_2h_ko"].unique().to_list()) <= {0, 1}
    assert set(wp_model_data["label"].drop_nulls().unique().to_list()) <= {0, 1}

    package_label_dist = _label_distribution(wp_model_data)
    baseline_label_dist = _label_distribution(baseline_wp)
    assert package_label_dist != baseline_label_dist, (
        "label distribution unexpectedly matched the baseline exactly -- if the "
        "scoring_play precedence fix (see module docstring) was reverted, this "
        "assertion should be replaced with an exact-match assertion"
    )


# --- EP equivalence (legacy subset of the combined legacy+WC24 baseline) -------------------


def test_ep_model_data_matches_baseline_legacy_subset_within_tolerance(repo_root: Path) -> None:
    """Runs the legacy-only EP chain and compares it against the *legacy portion* of the
    baseline's combined legacy+WC24 frame. `n_legacy` (the subset boundary) is derived by
    running the package's own legacy-only chain and taking its row count, rather than
    hardcoding a row number -- `scripts/capture_notebook_baseline.py`'s `build_ep_frame`
    concatenates legacy rows before WC24 rows (`pl.concat([df, df_wc], how="diagonal")`)
    and neither `prepare_ep_data` nor `make_ep_model_mutations` reorders rows, so the
    baseline's first `n_legacy` rows are exactly the legacy portion.
    """
    skip_reason = _skip_reason_if_fixtures_missing(repo_root)
    if skip_reason:
        pytest.skip(skip_reason)

    manifest, baseline_ep, _baseline_wp, legacy_csv = _load_fixtures(repo_root)

    conformed, _notices = ingest_legacy(legacy_csv, team_mapping=None)
    conformed = _with_competition_tier(conformed, repo_root)
    ep_exclude_id = make_game_id("legacy", manifest["excluded_games"]["ep"])
    ep_input = conformed.filter(pl.col("game_id") != ep_exclude_id)
    ep_prepared = prepare_ep_data(ep_input)
    ep_model_data = make_ep_model_mutations(ep_prepared, EP_SELECTED_COLUMNS)

    n_legacy = ep_model_data.height
    assert n_legacy <= baseline_ep.height, (
        "the legacy-only chain produced more rows than the combined legacy+WC24 baseline "
        "-- the corpora are not in the expected legacy-before-WC24 order"
    )
    baseline_legacy_subset = baseline_ep.head(n_legacy)

    # REQ-S1-09 adoption (known delta #4): `EP_SELECTED_COLUMNS` appends `half` and the tier
    # columns right before the trailing `Total_W_Scaled` -- the frozen baseline has none of
    # them, so they are inserted at the same position for the comparison.
    expected_columns = [
        *baseline_legacy_subset.columns[:-1],
        *_EP_ADOPTED_COLUMNS,
        baseline_legacy_subset.columns[-1],
    ]
    assert ep_model_data.columns == expected_columns
    assert ep_model_data.dtypes[: len(baseline_legacy_subset.dtypes) - 1] == (
        baseline_legacy_subset.dtypes[:-1]
    )
    assert ep_model_data.dtypes[-1] == baseline_legacy_subset.dtypes[-1]

    mismatches = _numeric_stat_mismatches(
        ep_model_data,
        baseline_legacy_subset,
        list(baseline_legacy_subset.columns),
        skip=_EP_KNOWN_DELTA_COLUMNS,
    )
    assert not mismatches, (
        "EP model_data statistics differ from the legacy subset of the frozen baseline "
        f"(subset boundary n_legacy={n_legacy}, derived from the package's own legacy-only "
        "chain, not hardcoded):\n" + "\n".join(mismatches)
    )

    # The newly adopted columns themselves: `half` is a passthrough Int32 core column
    # (already validated by canonical.py); the tier columns are well-formed one-hot Int32
    # indicators, exactly one 1 per row.
    tier_sum = ep_model_data.select(pl.sum_horizontal(TIER_FEATURE_COLUMNS).alias("s"))["s"]
    assert set(tier_sum.unique().to_list()) == {1}, (
        "exactly one tier column must be 1 per row"
    )


def test_ep_total_w_scaled_documented_corpus_dependent_delta(repo_root: Path) -> None:
    """`Total_W_Scaled` is corpus-dependent (module docstring, known delta #2): the
    baseline's weights were computed over legacy+WC24 combined, this test's weights over
    legacy alone. Documents the actual values instead of asserting equality."""
    skip_reason = _skip_reason_if_fixtures_missing(repo_root)
    if skip_reason:
        pytest.skip(skip_reason)

    manifest, baseline_ep, _baseline_wp, legacy_csv = _load_fixtures(repo_root)

    conformed, _notices = ingest_legacy(legacy_csv, team_mapping=None)
    conformed = _with_competition_tier(conformed, repo_root)
    ep_exclude_id = make_game_id("legacy", manifest["excluded_games"]["ep"])
    ep_input = conformed.filter(pl.col("game_id") != ep_exclude_id)
    ep_prepared = prepare_ep_data(ep_input)
    ep_model_data = make_ep_model_mutations(ep_prepared, EP_SELECTED_COLUMNS)

    # Still a valid, well-formed [0, 1]-scaled weight column -- just not baseline-identical
    # because the corpus it was scaled over differs (legacy-only vs. legacy+WC24).
    assert ep_model_data["Total_W_Scaled"].min() >= 0.0
    assert ep_model_data["Total_W_Scaled"].max() <= 1.0
    assert ep_model_data["Total_W_Scaled"].dtype == baseline_ep["Total_W_Scaled"].dtype
