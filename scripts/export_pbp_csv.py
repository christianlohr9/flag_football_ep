"""Export play-by-play CSVs (and optionally an xlsx) for the project owner
(local, gitignored under `data/processed/exports/`).

Promoted into the repo (2026-09-10) from a one-off ad-hoc script -- the owner opens these
files directly in Excel, so every CSV here is written `utf-8-sig` (a leading BOM,
`flag_football_ep.owner_csv.write_owner_csv`) rather than plain UTF-8: a plain UTF-8 CSV with
no BOM is routinely mis-decoded by Excel on macOS, garbling every umlaut on screen (e.g.
"Nühse" -> "NÃ¼hse"). When the owner then re-saves a file this way opened, Excel typically
writes it back semicolon-delimited, `mac_roman`/`cp1252`, CRLF-terminated -- every reader of
an owner-edited file elsewhere in this project already tolerates that shape
(`flag_football_ep.owner_csv`, `ingest.ifaf.load_spot_fill`/`load_corrections`,
`scripts/player_mapping_template.py --apply-filled`); this script is writer-only, it never
reads one of its own exports back in.

Writes three CSVs:

1. `korpus_alle_quellen_pbp.csv` -- the whole canonical, scored corpus (every source),
   straight from `plays_scored.parquet`.
2. `ifaf_wm2026_pbp.csv` -- IFAF World Flag 2026, both divisions, re-ingested straight from
   the redacted snapshots (`ingest_snapshots`, with the owner's own spot-fill/corrections
   applied), joined against the scored corpus for `ep`/`epa`/`wp`/`wpa`. The model columns are
   placed right after `yards_gained` (not at the far right) so they read next to the play they
   score.
3. `ifaf_wm2026_video_marks.csv` -- a CSV copy of the committed `ifaf_video_marks.parquet`
   table, for convenience, when that parquet exists.

`--xlsx <path>`, when given, additionally writes the WM export (`ifaf_wm2026_pbp.csv`'s own
frame) as a single-sheet `.xlsx` via `openpyxl` (already a project dependency, used elsewhere
for reading the head-coach workbooks) -- the owner's own preferred format for that file.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from flag_football_ep.config import Config, load_config  # noqa: E402
from flag_football_ep.ingest.ifaf import ingest_snapshots  # noqa: E402
from flag_football_ep.owner_csv import write_owner_csv  # noqa: E402
from flag_football_ep.reference import load_team_mapping  # noqa: E402

# Placed right after `yards_gained` (not at the far right) so the model's own scores read
# next to the play they score -- everything else keeps `wm`'s existing column order.
_FRONT_COLUMNS: tuple[str, ...] = (
    "competition",
    "game_id",
    "game_date",
    "half",
    "play_id",
    "drive_id",
    "posteam",
    "defteam",
    "down",
    "yards_to_go",
    "yardline_50",
    "play_type",
    "result_raw",
    "yards_gained",
    "ep",
    "epa",
    "wp",
    "wpa",
    "qb",
    "thrown_by",
    "received_by",
    "target",
)


def build_korpus_frame(config: Config) -> pl.DataFrame:
    """The whole canonical, scored corpus (every source), verbatim from `plays_scored.parquet`."""
    return pl.read_parquet(config.paths.processed / "plays_scored.parquet")


def _select_front_first(df: pl.DataFrame, front_columns: tuple[str, ...]) -> pl.DataFrame:
    """Reorder `df`'s columns so every name in `front_columns` that is actually present comes
    first (in `front_columns`'s own order), followed by every remaining column in its existing
    order -- a name in `front_columns` absent from `df` is skipped, never an error."""
    front = [c for c in front_columns if c in df.columns]
    return df.select(front + [c for c in df.columns if c not in front])


def build_wm_frame(config: Config, scored: pl.DataFrame) -> pl.DataFrame:
    """IFAF World Flag 2026 (both divisions), re-ingested from the redacted snapshots with the
    owner's own spot-fill/corrections applied, joined against `scored` for `ep`/`epa`/`wp`/
    `wpa`, sorted `(competition, game_id, play_id)`, with the model columns moved right after
    `yards_gained` (`_FRONT_COLUMNS`)."""
    team_mapping = load_team_mapping(config.reference.team_mapping)
    results = ingest_snapshots(
        config.paths.raw_ifaf,
        team_mapping,
        tournaments=None,
        spot_fill_dir=config.reference.ifaf_spot_fill,
        corrections_dir=config.reference.ifaf_corrections,
    )
    frames = [df for _, df, _ in results if df.height > 0]
    wm = pl.concat(frames, how="diagonal_relaxed")

    score_cols = [c for c in ("ep", "epa", "wp", "wpa") if c in scored.columns and c not in wm.columns]
    wm = wm.join(scored.select(["game_id", "play_id", *score_cols]), on=["game_id", "play_id"], how="left")
    wm = wm.sort(["competition", "game_id", "play_id"])

    return _select_front_first(wm, _FRONT_COLUMNS)


def write_xlsx(df: pl.DataFrame, path: Path) -> None:
    """Write `df` as a single-sheet `.xlsx` via `openpyxl` -- one header row, then one row per
    `df` row, in column order. No formatting/formulas, a plain data dump for the owner's own
    Excel-native workflow."""
    import openpyxl

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "pbp"
    ws.append(df.columns)
    for row in df.iter_rows():
        ws.append(row)
    wb.save(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("ffep.toml"), help="Path to ffep.toml")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Output directory for the CSVs (default: data/processed/exports under the config's own paths.processed)",
    )
    parser.add_argument(
        "--xlsx",
        type=Path,
        default=None,
        help="Also write the WM export as a single-sheet xlsx to this path",
    )
    args = parser.parse_args(argv)

    config = load_config(args.config)
    out_dir = args.out_dir or (config.paths.processed / "exports")
    out_dir.mkdir(parents=True, exist_ok=True)

    scored = build_korpus_frame(config)
    korpus_path = out_dir / "korpus_alle_quellen_pbp.csv"
    write_owner_csv(scored, korpus_path)
    print(f"korpus: {scored.height} rows, {scored.width} cols -> {korpus_path}")

    wm = build_wm_frame(config, scored)
    wm_path = out_dir / "ifaf_wm2026_pbp.csv"
    write_owner_csv(wm, wm_path)
    summary = wm.group_by("competition").agg(
        pl.len().alias("plays"),
        pl.col("game_id").n_unique().alias("games"),
        pl.col("yards_gained").is_not_null().mean().round(3).alias("yards_share"),
        (
            pl.col("epa").is_not_null().mean().round(3).alias("epa_share")
            if "epa" in wm.columns
            else pl.lit(None).alias("epa_share")
        ),
    )
    print(summary)
    print(f"wm: {wm.height} rows, {wm.width} cols -> {wm_path}")
    print(f"columns: {wm.columns}")

    video_marks_path = config.paths.processed / "ifaf_video_marks.parquet"
    if video_marks_path.exists():
        vm_out = out_dir / "ifaf_wm2026_video_marks.csv"
        write_owner_csv(pl.read_parquet(video_marks_path), vm_out)
        print(f"video marks -> {vm_out}")

    if args.xlsx:
        write_xlsx(wm, args.xlsx)
        print(f"wm xlsx -> {args.xlsx}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
