"""Freeze the current notebook EP/WP training frames as regression fixtures.

This is a standalone script, NOT part of the installed `flag_football_ep`
package: it imports directly from `Python/`, which the porting plans in this
phase delete once the package absorbs that logic. Its only job is to
reproduce models/ep_model.ipynb cells 1+3 and models/wp_model.ipynb cells 1+3
byte-for-byte, using the current (unmodified) `Python/` helpers, so later
porting work has a ground truth to diff against (tests/test_migration_equivalence.py,
plan 16).

Re-run this script any time you need to regenerate the baseline fixtures from
the same inputs — with unchanged inputs and unmodified `Python/` helpers, the
two Parquet frames are byte-identical across runs (`equals()` is True).
"""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

import polars as pl

EP_SELECTED_COLUMNS = [
    "label",
    "yardline_50",
    "yards_to_go",
    "down0",
    "down1",
    "down2",
    "down3",
    "down4",
    "Total_W_Scaled",
]

WP_SELECTED_COLUMNS = [
    "label",
    "receive_2h_ko",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "Diff_Time_Ratio",
    "score_differential",
    "down",
    "yards_to_go",
    "yardline_50",
]

EP_EXCLUDED_GAME_ID = 37
WP_EXCLUDED_GAME_ID = 35

WC24_SCHEMA_OVERRIDES = {
    "posteam": pl.String,
    "posteam_after": pl.String,
    "defteam": pl.String,
    "defteam_after": pl.String,
    "home_team": pl.String,
    "home_team_after": pl.String,
    "away_team": pl.String,
    "away_team_after": pl.String,
    "scoring_play_team": pl.String,
    "penalty": pl.Int32,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--legacy-csv",
        default="data_raw.csv",
        help="Path to the legacy Hudl export (default: data_raw.csv, falling back "
        "to data/raw/legacy/data_raw.csv if the default is absent)",
    )
    parser.add_argument(
        "--wc24-csv",
        default="data/wc24_pbp.csv",
        help="Path to the pre-mutated WC24 pbp CSV (default: data/wc24_pbp.csv)",
    )
    parser.add_argument(
        "--out-dir",
        default="tests/fixtures",
        help="Directory to write baseline fixtures into (default: tests/fixtures)",
    )
    return parser.parse_args(argv)


def resolve_legacy_csv(raw_arg: str) -> Path:
    """Resolve --legacy-csv, falling back to data/raw/legacy/data_raw.csv."""
    primary = Path(raw_arg)
    if primary.exists():
        return primary
    fallback = Path("data/raw/legacy/data_raw.csv")
    if fallback.exists():
        return fallback
    # Neither exists — return the primary path so downstream code raises a
    # clear "file not found" error rather than silently picking nothing.
    return primary


def import_helpers():
    """Import the exact, unmodified Python/ helper functions being frozen."""
    sys.path.insert(0, "Python")
    from helper_add_hudl_mutations import (  # noqa: E402
        make_hudl_mutations,
        prepare_ep_data,
        prepare_wp_data,
    )
    from helper_add_model_mutations import (  # noqa: E402
        make_ep_model_mutations,
        make_wp_model_mutations,
    )

    return (
        make_hudl_mutations,
        prepare_ep_data,
        prepare_wp_data,
        make_ep_model_mutations,
        make_wp_model_mutations,
    )


def build_ep_frame(legacy_csv: Path, wc24_csv: Path, helpers) -> pl.DataFrame:
    make_hudl_mutations, prepare_ep_data, _prepare_wp_data, make_ep_model_mutations, _ = helpers

    df = pl.read_csv(legacy_csv, separator=";", infer_schema_length=0)
    df = make_hudl_mutations(df)
    df = df.with_columns(pl.col(pl.UInt32).cast(pl.Int32).name.keep())
    df_wc = pl.read_csv(
        wc24_csv,
        separator=",",
        schema_overrides=WC24_SCHEMA_OVERRIDES,
    ).with_columns(pl.col(pl.Int64).cast(pl.Int32).name.keep())
    df = pl.concat([df, df_wc], how="diagonal")
    df2 = prepare_ep_data(df)
    ep_data = make_ep_model_mutations(
        df2.filter(pl.col("game_id") != EP_EXCLUDED_GAME_ID), EP_SELECTED_COLUMNS
    )
    return ep_data


def build_wp_frame(legacy_csv: Path, helpers) -> pl.DataFrame:
    make_hudl_mutations, _prepare_ep_data, prepare_wp_data, _, make_wp_model_mutations = helpers

    df1 = make_hudl_mutations(pl.read_csv(legacy_csv, separator=";", infer_schema_length=0))
    df2 = prepare_wp_data(df1)
    wp_data = make_wp_model_mutations(
        df2.filter(pl.col("game_id") != WP_EXCLUDED_GAME_ID), WP_SELECTED_COLUMNS
    )
    return wp_data


def print_frame_stats(name: str, df: pl.DataFrame) -> None:
    print(f"\n{name}: {df.height} rows, {df.width} columns")
    null_counts = df.null_count()
    for col in df.columns:
        print(f"  {col}: null_count={null_counts[col][0]}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    legacy_csv = resolve_legacy_csv(args.legacy_csv)
    wc24_csv = Path(args.wc24_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        helpers = import_helpers()
    except Exception:
        print("Failed to import Python/ helpers:", file=sys.stderr)
        traceback.print_exc()
        return 1

    try:
        ep_data = build_ep_frame(legacy_csv, wc24_csv, helpers)
        wp_data = build_wp_frame(legacy_csv, helpers)
    except (AttributeError, TypeError):
        # Signals a polars API drift (e.g. get_games()'s .melt/.pivot calls,
        # RESEARCH Pitfall 5). Do NOT patch Python/ helpers to work around
        # this — the baseline must come from unmodified helper code.
        print(
            "get_games()/mutation chain raised AttributeError or TypeError — "
            "likely a polars API drift in Python/helper_add_hudl_mutations.py "
            "(.melt/.pivot signature). Not patching Python/ helpers; baseline "
            "capture aborted.",
            file=sys.stderr,
        )
        traceback.print_exc()
        return 1

    ep_path = out_dir / "baseline_ep_model_data.parquet"
    wp_path = out_dir / "baseline_wp_model_data.parquet"
    ep_data.write_parquet(ep_path)
    wp_data.write_parquet(wp_path)

    print_frame_stats("EP model data", ep_data)
    print_frame_stats("WP model data", wp_data)

    print(f"\nWrote {ep_path}")
    print(f"Wrote {wp_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
