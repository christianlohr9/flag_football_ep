"""Per-opponent tendency report data (REQ-S1-12).

This module computes REQ-S1-12's tendency data for exactly one opponent's OFFENSE
(`posteam == team`). It performs no rendering and no filesystem I/O, and never hides a
thin cell -- every aggregation goes through `reports.aggregate`, so the muting
threshold and the confidence-interval method are defined in exactly one place. Plan 10
renders `OpponentReportData` into the HTML report; this module only builds it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

import polars as pl

from flag_football_ep.reports.aggregate import (
    DISTANCE_BUCKETS,
    MUTED_MIN_N,
    SectionBasis,
    add_report_buckets,
    charted_only,
    rate_table,
    section_basis,
    share_table,
)

_DISTANCE_LABELS: tuple[str, ...] = tuple(label for label, _, _ in DISTANCE_BUCKETS)
_PAT_TYPE_LABELS: tuple[str, ...] = ("1-Punkt", "2-Punkt")
_PAT_CHOICE_LABELS: tuple[str, ...] = ("Gesamt",)

_NO_CHARTED_MATERIAL = "Kein Charting-Material für diese Auswertung vorhanden."
_NO_PLAYS = "Keine Plays für diese Auswertung vorhanden."


@dataclass(frozen=True)
class ReportSection:
    """One rendered block of the opponent report.

    `empty_notice` is a German sentence set when `table` has nothing meaningful to show
    (CONTEXT's locked "explicit no-data block rather than failing" behaviour, resolved
    as a per-section notice) and `None` otherwise.
    """

    key: str
    heading: str
    table: pl.DataFrame
    basis: SectionBasis
    empty_notice: str | None


@dataclass(frozen=True)
class OpponentReportData:
    """Everything one opponent's tendency report page renders, precomputed."""

    team: str
    team_name: str
    summary_sentences: tuple[str, ...]
    sections: tuple[ReportSection, ...]
    overall_basis: SectionBasis
    notices: tuple[str, ...]


def formation_tendencies(plays: pl.DataFrame, *, team: str) -> ReportSection:
    """Formation x Down & Distance x Field zone play-call distribution.

    Restricted to Hudl-charted rows (`off_form` non-null) -- CONTEXT locks this section
    to Hudl-only games since formations are never charted from PBP sources. The section's
    `basis` describes that charted subset, not the full opponent frame, so the report
    states exactly which games this table is built from.
    """
    df = plays.filter(pl.col("posteam") == team)
    df = add_report_buckets(df)
    charted = charted_only(df, "off_form")
    basis = section_basis(charted)

    table = share_table(
        charted,
        group_cols=["off_form", "down", "distance_bucket", "field_zone"],
        category_col="play_type",
    )
    if table.height > 0:
        table = table.filter(pl.col("group_n") > 0).sort(
            ["off_form", "group_n", "share"],
            descending=[False, True, True],
        )

    empty_notice = _NO_CHARTED_MATERIAL if table.height == 0 else None
    return ReportSection(
        key="formation_tendencies",
        heading="Formation × Down & Distance × Feldzone",
        table=table,
        basis=basis,
        empty_notice=empty_notice,
    )


def route_distribution(plays: pl.DataFrame, *, team: str) -> ReportSection:
    """Target-route distribution by down & distance, and separately by field zone.

    Restricted to Hudl-charted rows (`target_route` non-null), same graceful-degradation
    reasoning as `formation_tendencies`. The two groupings are combined into one
    long-format table with a `gruppierung` column naming which grouping each row belongs
    to (`"Down & Distance"` or `"Feldzone"`), so a template renders two sub-tables from one
    frame.
    """
    df = plays.filter(pl.col("posteam") == team)
    df = add_report_buckets(df)
    charted = charted_only(df, "target_route")
    basis = section_basis(charted)

    combined_schema = {
        "gruppierung": pl.Utf8,
        "gruppe": pl.Utf8,
        "target_route": pl.Utf8,
        "n": pl.Int64,
        "group_n": pl.Int64,
        "share": pl.Float64,
        "ci_low": pl.Float64,
        "ci_high": pl.Float64,
        "muted": pl.Boolean,
    }

    if charted.height == 0:
        table = pl.DataFrame(schema=combined_schema)
    else:
        by_down_distance = _label_share_groups(
            share_table(charted, ["down", "distance_bucket"], "target_route"),
            group_cols=["down", "distance_bucket"],
            category_col="target_route",
            gruppierung="Down & Distance",
            schema=combined_schema,
        )
        by_zone = _label_share_groups(
            share_table(charted, ["field_zone"], "target_route"),
            group_cols=["field_zone"],
            category_col="target_route",
            gruppierung="Feldzone",
            schema=combined_schema,
        )
        table = pl.concat([by_down_distance, by_zone], how="vertical")

    empty_notice = _NO_CHARTED_MATERIAL if table.height == 0 else None
    return ReportSection(
        key="route_distribution",
        heading="Ziel-Routen-Verteilung",
        table=table,
        basis=basis,
        empty_notice=empty_notice,
    )


def _label_share_groups(
    df: pl.DataFrame,
    *,
    group_cols: list[str],
    category_col: str,
    gruppierung: str,
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Fold a `share_table` grouping into `route_distribution`'s combined long format."""
    if df.height == 0:
        return pl.DataFrame(schema=schema)
    gruppe_expr = pl.concat_str(
        [pl.col(c).cast(pl.Utf8).fill_null(pl.lit("unbekannt")) for c in group_cols],
        separator=" / ",
    )
    return df.with_columns(
        gruppe_expr.alias("gruppe"),
        pl.lit(gruppierung).alias("gruppierung"),
    ).select(
        "gruppierung", "gruppe", category_col, "n", "group_n", "share", "ci_low", "ci_high", "muted"
    ).rename({category_col: "target_route"})


def playcall_by_score_state(plays: pl.DataFrame, *, team: str) -> ReportSection:
    """Play-call distribution by score state, down and distance.

    Uses the FULL opponent frame, not the charted subset -- CONTEXT locks that
    down/distance and play-call behaviour also pull from sportapp and IFAF PBP, where the
    charted columns do not exist. PAT rows (`down == 0`) are excluded -- they belong to
    `fourth_down_and_pat_behavior`. A null `score_state` (no reconstructed score) becomes
    an explicit `"unbekannt"` group rather than being dropped, so the section's play
    counts reconcile with the section's own basis.
    """
    df = plays.filter(pl.col("posteam") == team)
    df = add_report_buckets(df)
    basis = section_basis(df)

    non_pat = df.filter(pl.col("down") != 0).with_columns(
        pl.col("score_state").fill_null(pl.lit("unbekannt")).alias("score_state")
    )
    table = share_table(non_pat, ["score_state", "down", "distance_bucket"], "play_type")

    empty_notice = _NO_PLAYS if table.height == 0 else None
    return ReportSection(
        key="playcall_by_score_state",
        heading="Play-Call nach Score State",
        table=table,
        basis=basis,
        empty_notice=empty_notice,
    )


def _rate_table_with_buckets(
    df: pl.DataFrame,
    bucket_col: str,
    buckets: Sequence[str],
    success: pl.Expr,
) -> pl.DataFrame:
    """`rate_table` grouped by `bucket_col`, padded so every label in `buckets` appears --
    even with zero attempts -- rather than being silently omitted. Mirrors
    `features.mutations.estimate_fourth_down_rates`'s always-full-buckets contract: a
    corpus legitimately may have zero attempts in a bucket, and the report still has to
    render an explicit gap, not a missing row.
    """
    label_col = f"{bucket_col}__label"
    table = rate_table(df, [bucket_col], success, label_col=label_col)
    schema = table.schema
    present = set(table[bucket_col].to_list()) if table.height > 0 else set()
    missing = [label for label in buckets if label not in present]

    if missing:
        missing_df = pl.DataFrame(
            {
                bucket_col: missing,
                label_col: missing,
                "n": [0] * len(missing),
                "successes": [0] * len(missing),
                "rate": [None] * len(missing),
                "ci_low": [None] * len(missing),
                "ci_high": [None] * len(missing),
                "muted": [True] * len(missing),
            },
            schema=schema,
        )
        table = pl.concat([table, missing_df], how="vertical")

    order_df = pl.DataFrame(
        {bucket_col: list(buckets), "__order__": list(range(len(buckets)))}
    )
    table = table.join(order_df, on=bucket_col, how="left").sort("__order__").drop("__order__")
    return table


def _to_kennzahl_rows(rate_df: pl.DataFrame, bucket_col: str, kennzahl: str) -> pl.DataFrame:
    """Fold one padded `rate_table` result into `fourth_down_and_pat_behavior`'s combined
    long format: a `kennzahl` column naming the metric family, a `bucket` column naming
    its distance/PAT-type bucket, and the `rate_table` columns (`n`, `successes`, `rate`,
    `ci_low`, `ci_high`, `muted`).
    """
    return rate_df.select(
        pl.lit(kennzahl).alias("kennzahl"),
        pl.col(bucket_col).alias("bucket"),
        "n",
        "successes",
        "rate",
        "ci_low",
        "ci_high",
        "muted",
    )


def fourth_down_and_pat_behavior(plays: pl.DataFrame, *, team: str) -> ReportSection:
    """4th-down go/conversion rates by distance, plus PAT choice and success by type.

    Uses the FULL opponent frame, not the charted subset -- same all-sources reasoning as
    `playcall_by_score_state`. One long-format table with a `kennzahl` column covering,
    per row, the metric family and its bucket:

    - 4th-down go rate: among 4th-down snaps (`down == 4`), the share that were an actual
      `run`/`pass` attempt rather than `no_play`, by `distance_bucket`.
    - 4th-down conversion rate: among 4th-down attempts (`play_type` in `{"run", "pass"}`
      and `penalty != 1`), the share reaching `first_down == 1` or `touchdown == 1` -- the
      same attempt and conversion predicates
      `features.mutations.estimate_fourth_down_rates` uses, so the two definitions never
      drift. A 4th-down touchdown with `first_down == 0` still counts as a conversion.
    - PAT choice: among `down == 0` rows, the share that were 1-point tries
      (`yards_to_go <= 5`) -- reusing `features.mutations.estimate_pat_baselines`'s
      predicates verbatim (2-point tries are `yards_to_go > 5`).
    - PAT success: the conversion rate of each PAT type (`one_point_conv_success` /
      `two_point_conv_success`).

    Every bucket in `DISTANCE_BUCKETS` and every PAT type always appears in the output,
    even with zero attempts (`n == 0`, `rate` null) -- a thin or empty bucket is shown,
    never omitted. Zero 4th-down snaps and zero PAT attempts both produce a fully padded,
    all-zero table rather than raising.
    """
    df = plays.filter(pl.col("posteam") == team)
    df = add_report_buckets(df)
    basis = section_basis(df)

    fourth_down_df = df.filter(pl.col("down") == 4)
    go_rate = _rate_table_with_buckets(
        fourth_down_df,
        "distance_bucket",
        _DISTANCE_LABELS,
        pl.col("play_type").is_in(["run", "pass"]),
    )

    attempts_df = fourth_down_df.filter(
        pl.col("play_type").is_in(["run", "pass"]) & (pl.col("penalty") != 1)
    )
    conversion_rate = _rate_table_with_buckets(
        attempts_df,
        "distance_bucket",
        _DISTANCE_LABELS,
        (pl.col("first_down") == 1) | (pl.col("touchdown") == 1),
    )

    pat_df = df.filter(pl.col("down") == 0).with_columns(
        pl.when(pl.col("yards_to_go") <= 5)
        .then(pl.lit("1-Punkt"))
        .otherwise(pl.lit("2-Punkt"))
        .alias("pat_type"),
        pl.lit("Gesamt").alias("pat_gesamt"),
    )
    pat_choice = _rate_table_with_buckets(
        pat_df, "pat_gesamt", _PAT_CHOICE_LABELS, pl.col("pat_type") == "1-Punkt"
    )
    pat_success = _rate_table_with_buckets(
        pat_df,
        "pat_type",
        _PAT_TYPE_LABELS,
        (
            (pl.col("pat_type") == "1-Punkt") & (pl.col("one_point_conv_success") == 1)
        )
        | (
            (pl.col("pat_type") == "2-Punkt") & (pl.col("two_point_conv_success") == 1)
        ),
    )

    table = pl.concat(
        [
            _to_kennzahl_rows(go_rate, "distance_bucket", "4th-Down Go-Rate"),
            _to_kennzahl_rows(conversion_rate, "distance_bucket", "4th-Down Conversion-Rate"),
            _to_kennzahl_rows(pat_choice, "pat_gesamt", "PAT-Wahl (Anteil 1-Punkt)"),
            _to_kennzahl_rows(pat_success, "pat_type", "PAT-Erfolgsquote"),
        ],
        how="vertical",
    )

    empty_notice = _NO_PLAYS if df.height == 0 else None
    return ReportSection(
        key="fourth_down_and_pat_behavior",
        heading="4th down & PAT",
        table=table,
        basis=basis,
        empty_notice=empty_notice,
    )


def _share_candidates(
    table: pl.DataFrame,
    section_key: str,
    *,
    category_col: str,
    sentence_fn,
) -> list[tuple[float, str, str]]:
    """Score each `share_table`-shaped row as a summary-sentence candidate.

    The baseline for a category value is its pooled share across the section's own table
    (total rows in that category / total rows in the table) -- the "corresponding overall
    baseline share for that team" this section can express without recomputing an
    unrelated aggregation. A candidate's score is how far its cell's share deviates from
    that baseline, weighted by `sqrt(n)` so a large deviation on a handful of plays does
    not outrank a solid deviation on a large sample. Only cells with `n >= MUTED_MIN_N`
    are considered -- CONTEXT's thin-sample rule applied to the summary block.
    """
    if table.height == 0 or "share" not in table.columns or category_col not in table.columns:
        return []

    pooled = table.group_by(category_col).agg(total_n=pl.col("n").sum())
    total = pooled["total_n"].sum()
    if not total:
        return []
    baseline = {row[category_col]: row["total_n"] / total for row in pooled.to_dicts()}

    candidates: list[tuple[float, str, str]] = []
    for row in table.to_dicts():
        n = row.get("n")
        share = row.get("share")
        if n is None or share is None or n < MUTED_MIN_N:
            continue
        base = baseline.get(row.get(category_col), 0.0)
        score = abs(share - base) * math.sqrt(n)
        candidates.append((score, section_key, sentence_fn(row)))
    return candidates


def _rate_candidates(table: pl.DataFrame, section_key: str) -> list[tuple[float, str, str]]:
    """Score each `fourth_down_and_pat_behavior` row as a summary-sentence candidate.

    The baseline for a `kennzahl` metric family is its pooled conversion rate across all
    of that family's buckets in the table -- the same pooled-baseline idea as
    `_share_candidates`, applied to a rate rather than a share.
    """
    if table.height == 0:
        return []

    pooled = table.group_by("kennzahl").agg(
        total_n=pl.col("n").sum(), total_successes=pl.col("successes").sum()
    )
    baseline: dict[str, float | None] = {}
    for row in pooled.to_dicts():
        total_n = row["total_n"]
        baseline[row["kennzahl"]] = (row["total_successes"] / total_n) if total_n else None

    candidates: list[tuple[float, str, str]] = []
    for row in table.to_dicts():
        n = row.get("n")
        rate = row.get("rate")
        if n is None or rate is None or n < MUTED_MIN_N:
            continue
        base = baseline.get(row["kennzahl"])
        if base is None:
            continue
        score = abs(rate - base) * math.sqrt(n)
        sentence = f"{row['kennzahl']} bei {row['bucket']}: {rate:.0%} (n={n})."
        candidates.append((score, section_key, sentence))
    return candidates


def _candidates_for_section(section: ReportSection) -> list[tuple[float, str, str]]:
    table = section.table
    if section.key == "formation_tendencies":
        return _share_candidates(
            table,
            section.key,
            category_col="play_type",
            sentence_fn=lambda row: (
                f"Aus {row['off_form']} bei {row['down']}. & {row['distance_bucket']} "
                f"in der {row['field_zone']}: {row['share']:.0%} {row['play_type']} "
                f"(n={row['n']})."
            ),
        )
    if section.key == "route_distribution":
        return _share_candidates(
            table,
            section.key,
            category_col="target_route",
            sentence_fn=lambda row: (
                f"{row['gruppierung']} {row['gruppe']}: {row['share']:.0%} Ziel-Route "
                f"{row['target_route']} (n={row['n']})."
            ),
        )
    if section.key == "playcall_by_score_state":
        return _share_candidates(
            table,
            section.key,
            category_col="play_type",
            sentence_fn=lambda row: (
                f"Bei Score State {row['score_state']}, {row['down']}. & "
                f"{row['distance_bucket']}: {row['share']:.0%} {row['play_type']} "
                f"(n={row['n']})."
            ),
        )
    if section.key == "fourth_down_and_pat_behavior":
        return _rate_candidates(table, section.key)
    return []


def top_tendency_sentences(
    sections: Sequence[ReportSection], *, team: str, max_sentences: int = 5
) -> tuple[str, ...]:
    """Pick the most actionable findings across all sections as plain German sentences.

    Selection rule (CONTEXT's "top-tendency selection logic is Claude's discretion",
    resolved concretely here): consider only cells with `n >= MUTED_MIN_N`, score each
    candidate by how far its share/rate deviates from the corresponding pooled baseline
    for that team, weighted by `sqrt(n)` so a large deviation on a handful of plays does
    not outrank a solid deviation on a large sample. Take the highest-scoring
    `max_sentences` candidates, at most two from any single section, so the summary is
    not monopolised by one table. When no cell clears `MUTED_MIN_N` anywhere, return a
    single sentence stating the data is too thin for reliable tendencies, naming the
    total play count -- never an empty summary block and never a made-up tendency.
    """
    candidates: list[tuple[float, str, str]] = []
    for section in sections:
        candidates.extend(_candidates_for_section(section))

    if not candidates:
        total_n = max((s.basis.n_plays for s in sections), default=0)
        return (f"Zu dünn für verlässliche Tendenzen (n={total_n}).",)

    candidates.sort(key=lambda c: c[0], reverse=True)

    chosen: list[str] = []
    per_section_count: dict[str, int] = {}
    for _, section_key, sentence in candidates:
        if len(chosen) >= max_sentences:
            break
        if per_section_count.get(section_key, 0) >= 2:
            continue
        chosen.append(sentence)
        per_section_count[section_key] = per_section_count.get(section_key, 0) + 1

    return tuple(chosen)


def build_opponent_data(
    plays: pl.DataFrame, *, team: str, team_name: str
) -> OpponentReportData:
    """Turn the corpus into one opponent's complete, render-ready report data.

    Filters nothing itself beyond delegating: calls the four section builders and
    `top_tendency_sentences`, and computes `overall_basis` via `section_basis` on the
    whole `posteam == team` frame. `notices` collects one German string per degraded
    condition: zero plays for this team, no Hudl-charted games at all, fewer than two
    games in the corpus, and any section that came back empty. When the opponent has zero
    plays in the corpus, returns empty sections, a single "keine Daten" summary sentence
    and a notice -- CONTEXT's locked zero-film behaviour -- and never raises.
    """
    team_plays = plays.filter(pl.col("posteam") == team)
    overall_basis = section_basis(team_plays)

    sections = (
        formation_tendencies(plays, team=team),
        route_distribution(plays, team=team),
        playcall_by_score_state(plays, team=team),
        fourth_down_and_pat_behavior(plays, team=team),
    )

    notices: list[str] = []
    if overall_basis.n_plays == 0:
        notices.append(f"Keine Daten für {team_name} im Datenkorpus vorhanden.")
        summary_sentences: tuple[str, ...] = (
            f"Keine Daten für {team_name} im Datenkorpus vorhanden (n=0).",
        )
    else:
        charted = charted_only(team_plays, "off_form")
        if charted.height == 0:
            notices.append(
                f"Keine Hudl-Charting-Spiele für {team_name} -- Formation- und "
                "Routen-Auswertung nicht verfügbar."
            )
        if len(overall_basis.games) < 2:
            game_word = "Spiel" if len(overall_basis.games) == 1 else "Spiele"
            notices.append(
                f"Nur {len(overall_basis.games)} {game_word} für {team_name} im "
                "Datenkorpus -- Tendenzen entsprechend unsicher."
            )
        for section in sections:
            if section.empty_notice:
                notices.append(f"{section.heading}: {section.empty_notice}")

        summary_sentences = top_tendency_sentences(sections, team=team)

    return OpponentReportData(
        team=team,
        team_name=team_name,
        summary_sentences=summary_sentences,
        sections=sections,
        overall_basis=overall_basis,
        notices=tuple(notices),
    )
