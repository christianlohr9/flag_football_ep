"""Build (and optionally apply) the head coach's player-label mapping template.

Reads the latest gitignored unmapped-label dump
(`data/raw/hc_files/unmapped_players_*.txt`) and `data/reference/roster.csv`,
suggests a canonical player for every label where the match is unique --
surname for a name-form label, jersey number for a numeric label -- and
writes a gitignored template CSV (`data/raw/hc_files/player_mapping_hc_template.csv`)
for the owner to review by hand.

`--apply-unique` additionally appends every *unambiguous* suggestion (a
non-empty `canonical_player` with no `candidates`) to the committed
`data/reference/player_mapping.csv` (source `hc_workbook`), then regenerates
the template so only the labels still needing the owner's attention remain.
Never overwrites an existing `(source, source_player)` row already in
`player_mapping.csv` -- those are the owner's own manually-approved mappings.

Scoped to the German WOMEN's national team (`team_id == 17`), not
`team_name == "Germany"`. `roster.csv` carries both the women's roster
(`team_id 17`) and the men's roster (`team_id 37`) under the identical
`team_name` "Germany" -- filtering on `team_name` alone (the original,
one-off version of this script) mixes men's players into the women's HC
mapping suggestions. The head coach this template serves is the women's
national team's, so only `team_id 17` is a legitimate candidate pool.

Moved from a throwaway job-tmp script into the repo (2026-09) so the mapping
step is durable and re-runnable as the roster grows -- no PII lives in this
file itself, only in the gitignored inputs/outputs it reads and writes.
"""

from __future__ import annotations

import argparse
import csv
import glob
import re
import sys
import unicodedata
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from flag_football_ep.config import load_config  # noqa: E402

GERMANY_WOMEN_TEAM_ID = 17

_TEMPLATE_FIELDNAMES = ("source", "source_player", "canonical_player", "match_basis", "candidates")


def _fold(s: str) -> str:
    """Lowercase, strip German umlauts/eszett and drop non-letters.

    Lowercases FIRST -- an all-caps label straight from the HC workbook
    (e.g. `"TÖRNER"`, `"NÜHSE"`) has an uppercase umlaut that the literal
    `"ö"`/`"ü"` replacements below never match if applied beforehand, so a
    caps-locked surname would silently never fold to the same key as its
    roster counterpart (bug in the original one-off version of this script).
    """
    s = s.lower()
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", s)


def already_mapped_labels(player_mapping_path: Path) -> set[str]:
    """Return every `source_player` already mapped under `source=hc_workbook`."""
    if not player_mapping_path.exists():
        return set()
    existing = list(csv.DictReader(player_mapping_path.open(encoding="utf-8")))
    return {r["source_player"] for r in existing if r.get("source") == "hc_workbook"}


def _latest_unmapped_dump(raw_hc_files: Path) -> Path:
    matches = sorted(glob.glob(str(raw_hc_files / "unmapped_players_*.txt")))
    if not matches:
        raise FileNotFoundError(
            f"no unmapped_players_*.txt found under {raw_hc_files}"
        )
    return Path(matches[-1])


def build_template_rows(labels: list[str], roster_women: pl.DataFrame) -> list[dict]:
    """Suggest a canonical player for every label in `labels`.

    Numeric labels match on `player_jersey`; name-form labels match on a
    folded surname (either the folded name ends with the folded label, or the
    label folds to exactly the last whitespace-separated token). A label
    resolves only when exactly one roster row matches; more than one match is
    recorded in `candidates` and left for the owner.
    """
    rows = []
    for label in labels:
        suggestion, basis, candidates = "", "", ""
        if re.fullmatch(r"-?\d+", label):
            hits = roster_women.filter(
                pl.col("player_jersey").cast(pl.Utf8) == label
            )["player_name"].to_list()
            basis = "jersey"
        else:
            key = _fold(label)
            hits = [
                name
                for name in roster_women["player_name"].to_list()
                if key and (_fold(name).endswith(key) or _fold(name).split()[-1:] == [key])
            ]
            basis = "surname"

        if len(hits) == 1:
            suggestion = hits[0]
        elif len(hits) > 1:
            candidates = " | ".join(hits)

        rows.append(
            {
                "source": "hc_workbook",
                "source_player": label,
                "canonical_player": suggestion,
                "match_basis": basis if suggestion else "",
                "candidates": candidates,
            }
        )
    return rows


def write_template(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_TEMPLATE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def apply_unique(rows: list[dict], player_mapping_path: Path) -> int:
    """Append every unambiguous template suggestion to `player_mapping.csv`.

    Skips a label whose `(source, source_player)` pair is already present --
    never overwrites an existing (owner-approved) row. Returns the count
    added.
    """
    existing = list(csv.DictReader(player_mapping_path.open(encoding="utf-8")))
    have = {(r["source"], r["source_player"]) for r in existing}

    to_add = [
        r
        for r in rows
        if r.get("canonical_player")
        and not r.get("candidates")
        and (r["source"], r["source_player"]) not in have
    ]
    if not to_add:
        return 0

    text = player_mapping_path.read_text(encoding="utf-8")
    needs_newline = bool(existing) and not text.endswith("\n")
    with player_mapping_path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        if needs_newline:
            fh.write("\n")
        for r in to_add:
            writer.writerow(["hc_workbook", r["source_player"], r["canonical_player"]])
    return len(to_add)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path("ffep.toml"), help="Path to ffep.toml"
    )
    parser.add_argument(
        "--roster",
        type=Path,
        default=Path("data/reference/roster.csv"),
        help="Path to the maintained roster CSV",
    )
    parser.add_argument(
        "--unmapped",
        type=Path,
        default=None,
        help="Override the unmapped-label dump path (default: the latest "
        "data/raw/hc_files/unmapped_players_*.txt)",
    )
    parser.add_argument(
        "--apply-unique",
        action="store_true",
        help="append every unambiguous suggestion to player_mapping.csv, "
        "then regenerate the template with only the remaining labels",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    raw_hc_files = cfg.paths.raw_hc_files
    player_mapping_path = cfg.reference.player_mapping
    template_path = raw_hc_files / "player_mapping_hc_template.csv"

    unmapped_path = args.unmapped or _latest_unmapped_dump(raw_hc_files)
    all_labels = [
        line.strip()
        for line in unmapped_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    roster = pl.read_csv(args.roster)
    roster_women = roster.filter(pl.col("team_id") == GERMANY_WOMEN_TEAM_ID)

    # Labels already resolved in a prior run (owner-approved or a previous
    # --apply-unique pass) are dropped before the template is built, so a
    # regenerated template only ever shows what is still left -- never a
    # label the owner (or a prior run of this script) already mapped.
    already_mapped = already_mapped_labels(player_mapping_path)
    labels = [label for label in all_labels if label not in already_mapped]

    rows = build_template_rows(labels, roster_women)
    write_template(rows, template_path)

    n_num = sum(1 for r in rows if re.fullmatch(r"-?\d+", r["source_player"]))
    n_unique = sum(1 for r in rows if r["canonical_player"])
    n_multi = sum(1 for r in rows if r["candidates"])
    print(
        f"labels {len(all_labels)} total, {len(rows)} still unmapped "
        f"(numeric {n_num}, names {len(rows) - n_num}); "
        f"unique suggestions {n_unique}; ambiguous {n_multi}; "
        f"unresolved {len(rows) - n_unique - n_multi}; wrote {template_path}"
    )

    if args.apply_unique:
        added = apply_unique(rows, player_mapping_path)
        print(f"applied {added} unique mapping(s) to {player_mapping_path}")

        already_mapped = already_mapped_labels(player_mapping_path)
        labels = [label for label in all_labels if label not in already_mapped]
        rows = build_template_rows(labels, roster_women)
        write_template(rows, template_path)
        n_unique_after = sum(1 for r in rows if r["canonical_player"])
        n_multi_after = sum(1 for r in rows if r["candidates"])
        print(
            f"after apply: {len(rows)} still unmapped; "
            f"unique suggestions {n_unique_after}; "
            f"ambiguous {n_multi_after}; "
            f"unresolved {len(rows) - n_unique_after - n_multi_after}; "
            f"regenerated {template_path}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
