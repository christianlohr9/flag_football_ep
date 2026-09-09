"""Build (and optionally apply) the cross-source player-label mapping template.

Generalises the original `hc_player_mapping_template.py` (which only handled the head
coach's workbook labels) to every source `player_mapping.csv` needs to cover:
`hc_workbook`, `legacy` and `ifaf`. The unmapped-label pool is no longer read from a
gitignored dump file -- it is built DIRECTLY from `data/processed/plays_scored.parquet`'s
own-team offense rows, via the same `_PLAYER_SOURCE_COLUMNS`/`_mapping_source_key` the
`player_efficiency`/`own_team` report itself uses, so the template's per-source label lists
are always exactly what the report would flag as "Nicht zugeordnete Spielernamen" (never a
stale, hand-refreshed dump that can drift from what the report actually shows).

Suggests a canonical player for every still-unmapped label where the match is unique. The
matching RULE differs per source -- each reflects how that source charts identity:

- `hc_workbook`: numeric label -> jersey number; name label -> folded surname suffix.
  Matched against every GER (`team_id == 17`) roster row, women's national team only, both
  pre-2026 and 2026 rows together (unchanged from the original one-off script).
- `legacy`: numeric label -> jersey number, matched ONLY against GER roster rows WITHOUT
  `season == "2026"` (the pre-2026 squad the 47 hand-charted legacy games actually cover);
  uppercase/mixed-case surname label -> folded surname suffix, matched against every GER
  roster row (a legacy-era surname can belong to a player who is still on the 2026 roster,
  e.g. under a fuller 2026 name).
- `ifaf`: full-name label -> folded FULL-NAME equality (not a suffix), matched against every
  `roster.csv` row REGARDLESS of team -- the IFAF tournament data attributes some rows in the
  own team's offense frame to opposing-team player identities (e.g. an interception's
  defender), so an IFAF label is not always a GER player.

A label resolves only when exactly one roster row matches; more than one match is recorded
in `candidates` and left for the owner. `legacy`'s numeric sentinels (`"-1"`/`"0"`, see
`flag_football_ep.reports.own_team._SENTINEL_PLAYER_LABELS_BY_SOURCE_KEY`) are never even
unmapped in the first place -- the report already nulls them out before they can appear as a
"player" -- so they never reach this template; this script additionally reports how many
sentinel occurrences it found and excluded, per source, so that exclusion stays visible
rather than silently invisible.

`--apply-unique` appends every *unambiguous* suggestion for EVERY source to the committed
`data/reference/player_mapping.csv` (each row's `source` column set correctly -- `legacy`,
`ifaf` or `hc_workbook`), then regenerates the template so only the labels still needing the
owner's attention remain. Never overwrites an existing `(source, source_player)` row already
in `player_mapping.csv` -- an unmapped label whose `(source, source_player)` pair already has
a mapping row is, by construction, never in the corpus-derived unmapped set to begin with
(`reference.map_players` excludes it), so there is nothing left to guard beyond what
`map_players` already guarantees.

Writes the gitignored template CSV to `data/raw/hc_files/player_mapping_template.csv`
(renamed from the old `player_mapping_hc_template.csv` now that it is no longer HC-only).

No PII lives in this file itself, only in the gitignored inputs/outputs it reads and writes,
and in the committed `data/reference/{roster.csv,player_mapping.csv}` -- the two files
`tests/test_m3_hc_pii.py` deliberately excludes from its no-player-name gate (HC-D02: "the
maintained CSVs are the mapping").
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from flag_football_ep.config import Config, load_config  # noqa: E402
from flag_football_ep.reference import load_player_mapping, map_players  # noqa: E402
from flag_football_ep.reports.own_team import (  # noqa: E402
    _PLAYER_SOURCE_COLUMNS,
    _SENTINEL_PLAYER_LABELS_BY_SOURCE_KEY,
    _mapping_source_key,
    _strip_sentinel_labels,
)

GERMANY_WOMEN_TEAM_ID = 17

_TEMPLATE_FIELDNAMES = ("source", "source_player", "canonical_player", "match_basis", "candidates")

_SOURCE_KEYS: tuple[str, ...] = ("hc_workbook", "legacy", "ifaf")


def _fold(s: str) -> str:
    """Lowercase, strip German umlauts/eszett and drop non-letters (spaces included).

    Lowercases FIRST -- an all-caps label straight from a source charting sheet (e.g.
    `"TÖRNER"`, `"NÜHSE"`) has an uppercase umlaut that the literal `"ö"`/`"ü"` replacements
    below never match if applied beforehand, so a caps-locked surname would silently never
    fold to the same key as its lowercase roster counterpart (bug in the original one-off
    version of this script). Stripping every non-letter (not just diacritics) also makes this
    whitespace-insensitive for free -- `"Ferro avolese"` and `"Ferro Avolese"` fold identically
    because the space between them is removed either way.
    """
    s = s.lower()
    s = s.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", s)


def _is_numeric_label(label: str) -> bool:
    return bool(re.fullmatch(r"-?\d+", label))


def _ger_roster(roster: pl.DataFrame) -> pl.DataFrame:
    """Every `roster.csv` row for the German WOMEN's national team.

    Scoped to `team_id == 17`, not `team_name == "Germany"` -- `roster.csv` carries both the
    women's roster (`team_id 17`) and the men's roster (`team_id 37`) under the identical
    `team_name` "Germany"; filtering on `team_name` alone mixes men's players into the
    women's mapping suggestions (bug in the original one-off version of this script).
    """
    return roster.filter(pl.col("team_id") == GERMANY_WOMEN_TEAM_ID)


def _pre_2026_ger_roster(roster: pl.DataFrame) -> pl.DataFrame:
    """`_ger_roster` restricted to rows WITHOUT `season == "2026"`.

    The 47 hand-charted `legacy` games predate the 2026 roster refresh -- a `legacy`
    `thrown_by`/`qb` jersey number is charted against the squad as it was THEN, not the 2026
    squad, so matching it against a 2026-only jersey would silently misattribute a play to
    whoever happens to wear that number now. `season` is empty (parsed as null by polars, not
    the literal string `""`) for every pre-2026 row.
    """
    ger = _ger_roster(roster)
    return ger.filter(pl.col("season").is_null() | (pl.col("season") != "2026"))


def _surname_hits(label: str, pool: pl.DataFrame) -> list[str]:
    key = _fold(label)
    if not key:
        return []
    return [name for name in pool["player_name"].to_list() if _fold(name).endswith(key)]


def _full_name_hits(label: str, pool: pl.DataFrame) -> list[str]:
    key = _fold(label)
    if not key:
        return []
    return [name for name in pool["player_name"].to_list() if _fold(name) == key]


def _jersey_hits(label: str, pool: pl.DataFrame) -> list[str]:
    return pool.filter(pl.col("player_jersey").cast(pl.Utf8) == label)["player_name"].to_list()


def match_label(label: str, source_key: str, roster: pl.DataFrame) -> tuple[list[str], str]:
    """Return `(hits, match_basis)` for `label` under `source_key`'s matching rule.

    See the module docstring for the per-source rule. `hits` is deduplicated but NOT
    filtered for uniqueness here -- the caller decides what more than one hit means
    (ambiguous, listed in `candidates`).
    """
    is_numeric = _is_numeric_label(label)

    if source_key == "hc_workbook":
        pool = _ger_roster(roster)
        if is_numeric:
            return sorted(set(_jersey_hits(label, pool))), "jersey"
        return sorted(set(_surname_hits(label, pool))), "surname"

    if source_key == "legacy":
        if is_numeric:
            return sorted(set(_jersey_hits(label, _pre_2026_ger_roster(roster)))), "jersey"
        return sorted(set(_surname_hits(label, _ger_roster(roster)))), "surname"

    if source_key == "ifaf":
        if is_numeric:
            return sorted(set(_jersey_hits(label, roster))), "jersey"
        return sorted(set(_full_name_hits(label, roster))), "full_name"

    raise ValueError(f"unknown source key: {source_key!r} (expected one of {_SOURCE_KEYS})")


@dataclass(frozen=True)
class SourceLabels:
    """One source key's still-unmapped labels plus the sentinel labels excluded from them."""

    unmapped: tuple[str, ...]
    sentinels_excluded: tuple[str, ...]


def load_own_team_offense(config: Config) -> pl.DataFrame:
    """The scored corpus's own-team offense rows, unfiltered by EPA availability.

    Reads `plays_scored.parquet` directly rather than routing through `attach_epa` --
    `attach_epa` only adds/derives `ep`/`epa`/`wp`/`home_wp`/`wpa`/`epa_source`; it never
    touches `source` or any of `_PLAYER_SOURCE_COLUMNS`, and preserves row count and order,
    so skipping it changes nothing about which player labels this function sees while
    avoiding a dependency on the OOF-predictions parquet files.
    """
    plays = pl.read_parquet(config.paths.processed / "plays_scored.parquet")
    return plays.filter(pl.col("posteam") == config.report.own_team)


def unmapped_labels_by_source(
    offense: pl.DataFrame, mapping: pl.DataFrame
) -> dict[str, SourceLabels]:
    """Per (coarse) source key: every still-unmapped player label, plus every sentinel label
    found and excluded.

    Mirrors `flag_football_ep.reports.own_team._canonicalise_players`'s per-(fine-grained)
    `source` mapping pass -- same `_PLAYER_SOURCE_COLUMNS`, same `_mapping_source_key`
    translation, same `_strip_sentinel_labels` call before `map_players` -- so a label ends
    up here if and only if the report's own "Nicht zugeordnete Spielernamen" warning would
    also flag it. Deliberately does NOT reuse `_canonicalise_players` directly: that function
    returns one flat, source-merged set (all the report's warning banner needs), while this
    template needs to know which coarse source key ("hc_workbook", "legacy", "ifaf") each
    label belongs to, since `player_mapping.csv`'s `source` column and the matching rule both
    depend on it.
    """
    columns = [c for c in _PLAYER_SOURCE_COLUMNS if c in offense.columns]
    if offense.height == 0 or not columns:
        return {}

    unmapped: dict[str, set[str]] = {}
    sentinels_found: dict[str, set[str]] = {}

    for fine_source in sorted(offense["source"].drop_nulls().unique().to_list()):
        key = _mapping_source_key(fine_source)
        subset = offense.filter(pl.col("source") == fine_source)

        sentinels = _SENTINEL_PLAYER_LABELS_BY_SOURCE_KEY.get(key)
        if sentinels:
            found: set[str] = set()
            for col in columns:
                found.update(v for v in subset[col].drop_nulls().unique().to_list() if v in sentinels)
            if found:
                sentinels_found.setdefault(key, set()).update(found)

        stripped = _strip_sentinel_labels(subset, key, columns)
        result = map_players(stripped, mapping, key, columns)
        if result.unmapped:
            unmapped.setdefault(key, set()).update(result.unmapped)

    all_keys = set(unmapped) | set(sentinels_found)
    return {
        key: SourceLabels(
            unmapped=tuple(sorted(unmapped.get(key, set()))),
            sentinels_excluded=tuple(sorted(sentinels_found.get(key, set()))),
        )
        for key in sorted(all_keys)
    }


def build_template_rows(
    labels_by_source: dict[str, SourceLabels], roster: pl.DataFrame
) -> list[dict]:
    """Suggest a canonical player for every still-unmapped label in `labels_by_source`."""
    rows: list[dict] = []
    for source_key in sorted(labels_by_source):
        for label in labels_by_source[source_key].unmapped:
            hits, basis = match_label(label, source_key, roster)
            suggestion, candidates = "", ""
            if len(hits) == 1:
                suggestion = hits[0]
            elif len(hits) > 1:
                candidates = " | ".join(hits)
            rows.append(
                {
                    "source": source_key,
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
    """Append every unambiguous template suggestion to `player_mapping.csv`, across all
    sources present in `rows`.

    Skips a label whose `(source, source_player)` pair is already present -- never
    overwrites an existing (owner-approved) row. Returns the count added.
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
            writer.writerow([r["source"], r["source_player"], r["canonical_player"]])
    return len(to_add)


def _print_summary(rows: list[dict], labels_by_source: dict[str, SourceLabels], prefix: str) -> None:
    by_source: dict[str, list[dict]] = {}
    for r in rows:
        by_source.setdefault(r["source"], []).append(r)

    for source_key in sorted(set(by_source) | set(labels_by_source)):
        srows = by_source.get(source_key, [])
        n_total = len(srows)
        n_unique = sum(1 for r in srows if r["canonical_player"])
        n_multi = sum(1 for r in srows if r["candidates"])
        n_unresolved = n_total - n_unique - n_multi
        n_sentinels = len(labels_by_source.get(source_key, SourceLabels((), ())).sentinels_excluded)
        print(
            f"{prefix}{source_key}: {n_total} still unmapped "
            f"(sentinels excluded: {n_sentinels}); "
            f"unique suggestions {n_unique}; ambiguous {n_multi}; unresolved {n_unresolved}"
        )


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
        "--apply-unique",
        action="store_true",
        help="append every unambiguous suggestion (any source) to player_mapping.csv, "
        "then regenerate the template with only the remaining labels",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    raw_hc_files = cfg.paths.raw_hc_files
    player_mapping_path = cfg.reference.player_mapping
    template_path = raw_hc_files / "player_mapping_template.csv"

    roster = pl.read_csv(args.roster)
    offense = load_own_team_offense(cfg)

    def _build() -> tuple[list[dict], dict[str, SourceLabels]]:
        mapping = load_player_mapping(player_mapping_path)
        labels_by_source = unmapped_labels_by_source(offense, mapping)
        return build_template_rows(labels_by_source, roster), labels_by_source

    rows, labels_by_source = _build()
    write_template(rows, template_path)
    print(f"wrote {template_path}")
    _print_summary(rows, labels_by_source, prefix="")

    if args.apply_unique:
        added = apply_unique(rows, player_mapping_path)
        print(f"applied {added} unique mapping(s) to {player_mapping_path}")

        rows, labels_by_source = _build()
        write_template(rows, template_path)
        print(f"regenerated {template_path}")
        _print_summary(rows, labels_by_source, prefix="after apply: ")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
