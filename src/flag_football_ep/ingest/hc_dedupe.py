"""Cross-source dedupe for head-coach workbook rows (HC-02, HC-D03).

Lives outside `ingest/hc_workbook.py` because it is a cross-source concern
(M3-01-RESEARCH.md "Architectural Responsibility Map"): it must see both the
freshly-ingested HC rows and the already-ingested rows from every other
source (hudl/legacy/sportapp/ifaf, already concatenated by
`pipeline.run_ingest`) to decide what to exclude -- a single-source ingest
module never sees the other side. `pipeline.run_ingest` calls
`dedupe_hc_rows` after every other source's frames are concatenated and
before the HC frame joins the final combined frame.

Two stages, in the head coach's own stated preference (HC-D03, verbatim in
M3-01-CONTEXT.md): "wir reichern eher unsere Daten um seine an ... Duplikate
bei ihm erkennen und nicht berücksichtigen; sonst mit der Doppelung leben" --
we enrich our data with his; detect duplicates *in his data* and drop them;
otherwise live with the duplication. This module therefore only ever removes
rows from the HC side, never from the corpus side.

Stage 1 -- declared pairing: a human maintains `data/reference/hc_games.csv`'s
`corpus_game_id` column, naming which already-known game (a corpus game or
another HC game -- see below) one HC block duplicates. Only inside a
declared pair does this module ever exclude a row; an HC game with an empty
`corpus_game_id` is never touched, however similar its content looks to
something else.

Stage 2 -- content fingerprint: within a declared pair, an HC row is
excluded only when its fingerprint (`FINGERPRINT_COLUMNS`) occurs among the
paired game's rows. A fingerprint match between an HC game and any *other*
game that is not its declared partner is reported under
`cross_game_overlaps` -- report only, never excluded; guessing a pairing
from content alone risks deleting a real play that merely happens to look
similar. A fingerprint that repeats more than once inside one and the same
HC game is reported too, also never excluded -- the head coach charting the
same situation twice is plausible; only cross-game duplication is a
duplicate.

Pairing resolves against both frames combined (`hc_df` and `corpus_df`), so
the `Data`/`Copy of Data` case inside the Scoring-Probability workbook --
two HC games in the same ingest run, one declared as the other's
`corpus_game_id` -- works exactly like an HC-vs-corpus pair (M3-01-RESEARCH.md
Pitfall 4).

Deliberate deviation from HC-D03's literal column list: the head coach's
`GN/LS` column has no canonical column of its own (`docs/data-contract.md`
marks it informational, non-authoritative -- yards are derived from
`yardline_50` deltas into `yards_gained`), so the fingerprint uses the
derived `yards_gained` instead, which both sides of any pair compute the
same way and is therefore a more consistent comparison than a raw column
only one side reliably has.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

__all__ = ["FINGERPRINT_COLUMNS", "DedupeReport", "add_fingerprint", "dedupe_hc_rows"]

# HC-D03's own list, with GN/LS replaced by the derived yards_gained (see
# module docstring's "Deliberate deviation" paragraph).
FINGERPRINT_COLUMNS: tuple[str, ...] = (
    "down",
    "yards_to_go",
    "yardline",
    "result_raw",
    "yards_gained",
    "received_by",
    "thrown_by",
)

# Null sentinel for fingerprint parts: nullness must be part of a row's
# identity (two rows both null in a field match each other on that part),
# never a wildcard that matches anything.
_NULL_SENTINEL = "~"
_FINGERPRINT_COL = "_hc_dedupe_fingerprint"
_ORIG_INDEX_COL = "_hc_dedupe_orig_index"

# DedupeReport.summary_lines() renders at most this many individual
# cross_game_overlaps lines (see that method's docstring for why).
_MAX_OVERLAP_LINES = 20


@dataclass
class DedupeReport:
    """Machine-readable record of one `dedupe_hc_rows` call.

    `pairs` has one entry per HC game with a declared `corpus_game_id`:
    `{hc_game_id, partner_game_id, n_hc, n_matched, n_unmatched}`.
    `cross_game_overlaps` has one entry per (hc_game_id, other_game_id) pair
    with at least one matching fingerprint outside the declared pairing:
    `{hc_game_id, other_game_id, n_matching}`. `messages` carries free-text
    findings (intra-game duplicate fingerprints) that do not fit either
    table. `summary_lines()` renders all of it as the German lines
    `pipeline.run_ingest` folds into its source notices and
    `docs/hc-workbook-ingest.md` reuses verbatim.
    """

    n_hc_rows: int
    n_excluded: int = 0
    pairs: list[dict] = field(default_factory=list)
    cross_game_overlaps: list[dict] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)

    def summary_lines(self) -> list[str]:
        """Render this report as German lines for the pipeline's source
        notices / the validation report / `docs/hc-workbook-ingest.md`.

        `cross_game_overlaps` can be large against a real, sizeable corpus
        (a short fingerprint -- e.g. a common down/distance/yardline combo
        with no receiver -- coincidentally matches many unrelated games), so
        this method renders at most `_MAX_OVERLAP_LINES` individual pair
        lines (largest `n_matching` first) plus one aggregate line stating
        the true total -- never a silently truncated total, and the full,
        untruncated list always remains available on
        `report.cross_game_overlaps` for any caller that needs it (e.g. a
        test, or a future per-game drill-down). Rendering every pair
        individually at real-corpus scale (hundreds of thousands of entries
        observed against the committed legacy+ifaf corpus) would make the
        validation report and console output unusable -- this is a Rule 1
        fix discovered during the M3-01-04 real run, not a change to the
        stage-2 exclusion logic above, which is untouched.
        """
        lines: list[str] = [
            f"hc_dedupe: {self.n_hc_rows} HC-Zeile(n) geprüft, {self.n_excluded} "
            "wegen erklärter Duplikat-Paarung ausgeschlossen"
        ]
        for pair in self.pairs:
            lines.append(
                f"hc_dedupe: Paar {pair['hc_game_id']!r} <-> {pair['partner_game_id']!r}: "
                f"{pair['n_hc']} HC-Zeile(n), {pair['n_matched']} übereinstimmend "
                f"(ausgeschlossen), {pair['n_unmatched']} übernommen"
            )
        if self.cross_game_overlaps:
            total_rows = sum(o["n_matching"] for o in self.cross_game_overlaps)
            n_hc_games = len({o["hc_game_id"] for o in self.cross_game_overlaps})
            lines.append(
                f"hc_dedupe: {len(self.cross_game_overlaps)} unerklärte Überschneidung(en) "
                f"über {n_hc_games} HC-Spiel(e) hinweg ({total_rows} übereinstimmende "
                "Zeile(n) insgesamt), keine davon ausgeschlossen (keine erklärte Paarung) "
                "-- die größten Überschneidungen:"
            )
            top = sorted(
                self.cross_game_overlaps, key=lambda o: o["n_matching"], reverse=True
            )
            for overlap in top[:_MAX_OVERLAP_LINES]:
                lines.append(
                    f"hc_dedupe: unerklärte Überschneidung {overlap['hc_game_id']!r} <-> "
                    f"{overlap['other_game_id']!r}: {overlap['n_matching']} "
                    "übereinstimmende Zeile(n), nicht ausgeschlossen (keine erklärte "
                    "Paarung)"
                )
            remaining = len(self.cross_game_overlaps) - _MAX_OVERLAP_LINES
            if remaining > 0:
                lines.append(
                    f"hc_dedupe: ... {remaining} weitere unerklärte Überschneidung(en) "
                    "nicht einzeln aufgeführt (vollständige Liste: "
                    "DedupeReport.cross_game_overlaps)"
                )
        lines.extend(self.messages)
        return lines


def add_fingerprint(df: pl.DataFrame) -> pl.DataFrame:
    """Add a `_hc_dedupe_fingerprint` Utf8 column identifying each row's content.

    Every `FINGERPRINT_COLUMNS` part is cast to Utf8 and null-filled with a
    sentinel before concatenation (`pl.concat_str(..., ignore_nulls=False)`),
    so nullness participates in the identity rather than acting as a
    wildcard. A column absent from `df` (should not happen for an
    already-canonical frame, but guarded rather than raising) contributes
    the sentinel for every row.
    """
    parts = [
        (
            pl.col(col).cast(pl.Utf8).fill_null(_NULL_SENTINEL)
            if col in df.columns
            else pl.lit(_NULL_SENTINEL, dtype=pl.Utf8)
        )
        for col in FINGERPRINT_COLUMNS
    ]
    return df.with_columns(
        pl.concat_str(parts, separator="|", ignore_nulls=False).alias(_FINGERPRINT_COL)
    )


def dedupe_hc_rows(
    hc_df: pl.DataFrame, corpus_df: pl.DataFrame, hc_games: pl.DataFrame
) -> tuple[pl.DataFrame, DedupeReport]:
    """Exclude HC rows that duplicate an already-known game, per HC-D03.

    `hc_df` is every row this run's HC sources produced (any number of
    sheets/games, already conformed to the canonical schema). `corpus_df` is
    every already-ingested row from the other sources in this run (may be
    empty). `hc_games` is the maintained `data/reference/hc_games.csv` frame
    -- its `game_id`/`corpus_game_id` columns declare the pairing.

    Row conservation holds: `kept.height + report.n_excluded ==
    hc_df.height`; a violation raises `RuntimeError` (mirrors
    `validation.checks.partition_games`'s wording -- a silent row drop in a
    dedupe gate is indistinguishable from bad data). An empty `hc_df` or an
    empty `corpus_df` returns the input unchanged with an empty report and
    no exception -- deliberately literal: an HC game whose only possible
    partner lives in another HC sheet (the Data/Copy of Data case) still
    needs *some* corpus rows to compare against, and a genuinely empty run
    has nothing to dedupe against either way.
    """
    report = DedupeReport(n_hc_rows=hc_df.height)

    if hc_df.height == 0 or corpus_df.height == 0:
        return hc_df, report

    declared = hc_games.filter(
        pl.col("corpus_game_id").is_not_null() & (pl.col("corpus_game_id") != "")
    )
    declared_pairs: dict[str, str] = dict(
        zip(declared["game_id"].to_list(), declared["corpus_game_id"].to_list())
    )

    hc_fp = add_fingerprint(hc_df).with_row_index(_ORIG_INDEX_COL)
    corpus_fp = add_fingerprint(corpus_df)
    # Combined lookup so a declared partner can be another HC game in the
    # same run (Data/Copy of Data) as well as an already-ingested corpus game.
    combined_fp = pl.concat(
        [hc_fp.drop(_ORIG_INDEX_COL), corpus_fp], how="vertical"
    )

    kept_frames: list[pl.DataFrame] = []
    hc_game_ids = sorted(hc_fp["game_id"].unique().to_list())

    for hc_game_id in hc_game_ids:
        hc_game_rows = hc_fp.filter(pl.col("game_id") == hc_game_id)
        partner_id = declared_pairs.get(hc_game_id)

        if partner_id is None:
            kept_frames.append(hc_game_rows)
        else:
            partner_keys = (
                combined_fp.filter(pl.col("game_id") == partner_id)
                .select(_FINGERPRINT_COL)
                .unique()
            )
            matched = hc_game_rows.join(partner_keys, on=_FINGERPRINT_COL, how="semi")
            unmatched = hc_game_rows.join(partner_keys, on=_FINGERPRINT_COL, how="anti")
            report.n_excluded += matched.height
            report.pairs.append(
                {
                    "hc_game_id": hc_game_id,
                    "partner_game_id": partner_id,
                    "n_hc": hc_game_rows.height,
                    "n_matched": matched.height,
                    "n_unmatched": unmatched.height,
                }
            )
            kept_frames.append(unmatched)

        # Intra-game duplicate fingerprints: reported, never excluded.
        dup_counts = (
            hc_game_rows.group_by(_FINGERPRINT_COL)
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
        )
        if dup_counts.height:
            total_dup_rows = int(dup_counts["n"].sum())
            report.messages.append(
                f"hc_dedupe: {hc_game_id!r}: {dup_counts.height} Fingerprint(s) treten "
                f"mehrfach innerhalb desselben Spiels auf ({total_dup_rows} Zeile(n) "
                "insgesamt) -- nicht ausgeschlossen (Duplikate innerhalb eines Spiels "
                "sind plausibel)"
            )

        # Cross-game overlaps: fingerprint matches against any OTHER game
        # that is not this game itself and not its declared partner (if
        # any) -- report only, never excluded.
        excluded_game_ids = {hc_game_id}
        if partner_id is not None:
            excluded_game_ids.add(partner_id)
        other_keys = (
            combined_fp.filter(~pl.col("game_id").is_in(sorted(excluded_game_ids)))
            .select([_FINGERPRINT_COL, "game_id"])
            .unique()
        )
        overlap_matches = hc_game_rows.select(_FINGERPRINT_COL).join(
            other_keys, on=_FINGERPRINT_COL, how="inner"
        )
        if overlap_matches.height:
            overlap_counts = overlap_matches.group_by("game_id").agg(
                pl.len().alias("n_matching")
            )
            for row in overlap_counts.sort("game_id").to_dicts():
                report.cross_game_overlaps.append(
                    {
                        "hc_game_id": hc_game_id,
                        "other_game_id": row["game_id"],
                        "n_matching": row["n_matching"],
                    }
                )

    kept = (
        pl.concat(kept_frames, how="vertical")
        if kept_frames
        else hc_fp.filter(pl.lit(False))
    )
    kept = kept.sort(_ORIG_INDEX_COL).drop([_ORIG_INDEX_COL, _FINGERPRINT_COL])

    if kept.height + report.n_excluded != hc_df.height:
        raise RuntimeError(
            "row conservation violated in dedupe_hc_rows: "
            f"kept={kept.height} + excluded={report.n_excluded} != input={hc_df.height}"
        )

    return kept, report
