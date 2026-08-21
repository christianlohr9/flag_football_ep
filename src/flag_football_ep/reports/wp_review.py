"""Per-game WP review page assembly (REQ-S1-15): turns the annotated WP curve from
`charts.wp_review` into a standalone HTML page per game -- the film-session artifact staff
open next to the tape, with the curve, a findable swing table, an explicit synthetic-clock
notice, and a WP provenance sentence derived from the data ("wo ist das Spiel gekippt?").

`build_wp_review_page` performs no filesystem writes -- it renders through
`reports.render.fig_to_data_uri`/`render_page`, exactly like every other Phase 1.4 report
builder (`reports/opponent.py`, `reports/own_team.py`, `reports/decisions.py`). `game_slug`/
`wp_review_filename` are the only functions in this module that touch a caller-controlled
string destined for a filesystem path -- hudl `game_id` values are export filename stems from
a directory an analyst controls (`canonical.make_game_id`), so they are the untrusted half of
the output path (T-1.4-26) and must be sanitised through an allow-list, never assumed safe.

`attach_wp_provenance` is the WP twin of plan 09's `own_team.attach_epa`, and both implement
the same locked 1.3 contract (out-of-fold for historical plays, the promoted champion for new
ones) -- if one changes, the other must change with it. They exist as two functions, not one
shared helper, because they serve different callers: `attach_epa` is own-team-filtered and
resolves both EP and WP together for efficiency rollups, while `attach_wp_provenance` is
per-game, WP-only, and runs over every team's games (not just the own team's) for this review
product. The split matters here specifically because a review page is read as evidence about
decisions the team already made: an in-sample WP curve on a game the champion trained on would
flatter exactly the moments the staff is reviewing, which is the flattering-in-hindsight
failure CONTEXT's statistical-honesty bias rules out (T-1.4-66).
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Literal

import polars as pl

from flag_football_ep.charts.wp_review import render_wp_review, select_wp_annotations
from flag_football_ep.features.mutations import (
    WP_PROBABILITY_COLUMN,
    add_wp_variables,
    prepare_wp_data,
)
from flag_football_ep.reports.render import fig_to_data_uri, render_page

# Belt-and-braces allow-list, re-checked against `game_slug`'s own sanitised output before it
# is ever returned -- mirrors `model/score.py::_validate_run_id`'s "validate against a known
# identifier pattern before interpolating into a path" discipline.
GAME_SLUG_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")

_UNSAFE_CHAR_PATTERN = re.compile(r"[^A-Za-z0-9._-]")
_DASH_RUN_PATTERN = re.compile(r"-{2,}")

WpSource = Literal["oof", "champion"]

_NO_WP_DATA_NOTICE = "keine WP-Daten für dieses Spiel"

_WP_VALUE_COLUMNS = (WP_PROBABILITY_COLUMN, "home_wp", "away_wp", "wpa")
_PROVENANCE_ROW_ID = "_wp_provenance_row_id"
_OOF_PREDICTIONS_FILENAME = "oof_predictions_wp.parquet"


def game_slug(game_id: str) -> str:
    """Turn `game_id` into a filesystem-safe slug.

    Every character outside `[A-Za-z0-9._-]` becomes `-`, runs of `-` collapse to one, and
    leading `.`/`-` characters are stripped -- a leading dot must never survive, so a slug can
    never be read as a relative path segment such as `..`. Raises `ValueError` naming the
    input when the sanitised result is empty or does not match `GAME_SLUG_PATTERN` (the
    belt-and-braces re-check).
    """
    replaced = _UNSAFE_CHAR_PATTERN.sub("-", game_id)
    collapsed = _DASH_RUN_PATTERN.sub("-", replaced)
    slug = collapsed.lstrip(".-")
    if not slug or not GAME_SLUG_PATTERN.match(slug):
        raise ValueError(
            f"game_slug: {game_id!r} sanitises to an empty or unsafe slug -- refusing to "
            "build an output filename from it"
        )
    return slug


def wp_review_filename(game_id: str) -> str:
    """The path-safe filename for `game_id`'s review page: `wp-review-{game_slug}.html`."""
    return f"wp-review-{game_slug(game_id)}.html"


def _game_label(game_plays: pl.DataFrame, *, game_id: str) -> str:
    """Build a human-readable game label from `home_team`/`away_team`, falling back to the
    bare `game_id` when either column is absent, the frame is empty, or either value is null
    on the first row."""
    if (
        game_plays.height > 0
        and "home_team" in game_plays.columns
        and "away_team" in game_plays.columns
    ):
        home = game_plays["home_team"][0]
        away = game_plays["away_team"][0]
        if home is not None and away is not None:
            return f"{home} vs {away} ({game_id})"
    return game_id


def _format_pct(value: float | None) -> str:
    if value is None:
        return "–"
    return f"{value * 100:.1f}%"


def _format_signed_pct(value: float | None) -> str:
    if value is None:
        return "–"
    return f"{value * 100:+.1f}%"


def _swing_table_rows(annotations: pl.DataFrame) -> list[dict[str, object]]:
    """Build one row per `select_wp_annotations` row: play number, event label, possessing
    team, and WP before/after/change in home-team perspective, all pre-formatted as
    percentages.

    `wpa` is posteam-relative (`charts.wp_review`'s convention); it is re-signed to the home
    team's perspective here so the table's "WP-Änderung" column agrees with the
    home-perspective curve the chart plots. Computed row-wise in a small Python loop (at most
    `WP_ANNOTATION_MAX` rows), matching `charts/wp_review.py::_annotation_label`'s own
    row-wise-loop precedent rather than a chained polars expression.
    """
    rows: list[dict[str, object]] = []
    for row in annotations.iter_rows(named=True):
        home_wp = row.get("home_wp")
        wpa = row.get("wpa")
        posteam = row.get("posteam")
        home_team = row.get("home_team")
        if home_wp is not None and wpa is not None and home_team is not None:
            home_wpa = wpa if posteam == home_team else -wpa
            wp_after = home_wp
            wp_before = wp_after - home_wpa
        else:
            home_wpa = None
            wp_after = home_wp
            wp_before = None
        rows.append(
            {
                "play_id": row.get("play_id"),
                "event": row.get("annotation_label"),
                "posteam": posteam,
                "wp_before": _format_pct(wp_before),
                "wp_after": _format_pct(wp_after),
                "wp_change": _format_signed_pct(home_wpa),
            }
        )
    return rows


def _header_summary(game_plays: pl.DataFrame) -> dict[str, object] | None:
    """The page's short header summary: final score, biggest single WP swing, and lead-change
    count (crossings of `home_wp == 0.5`). Returns `None` (never raises) when fewer than two
    rows carry a non-null `home_wp` -- the caller renders the `keine WP-Daten` notice instead
    of a summary."""
    if "home_wp" not in game_plays.columns:
        return None
    non_null = game_plays.filter(pl.col("home_wp").is_not_null())
    if non_null.height < 2:
        return None

    last = game_plays.tail(1)
    home_team = last["home_team"][0] if "home_team" in last.columns else None
    away_team = last["away_team"][0] if "away_team" in last.columns else None
    home_score = (
        last["home_team_score"][0] if "home_team_score" in last.columns else None
    )
    away_score = (
        last["away_team_score"][0] if "away_team_score" in last.columns else None
    )

    biggest_swing = None
    if "wpa" in game_plays.columns:
        swings = game_plays.filter(pl.col("wpa").is_not_null())
        if swings.height > 0:
            idx = swings["wpa"].abs().arg_max()
            row = swings.row(idx, named=True)
            biggest_swing = {"play_id": row["play_id"], "wpa": row["wpa"]}

    ordered = non_null.sort("play_id")["home_wp"].to_list()
    lead_changes = 0
    prev_side: str | None = None
    for wp in ordered:
        if wp == 0.5:
            continue
        side = "home" if wp > 0.5 else "away"
        if prev_side is not None and side != prev_side:
            lead_changes += 1
        prev_side = side

    return {
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "biggest_swing": biggest_swing,
        "lead_changes": lead_changes,
    }


def _wp_provenance_text(game_plays: pl.DataFrame) -> str:
    """A German sentence naming the per-source WP counts the frame actually carries.

    Never static prose: computed from the `wp_source` column `attach_wp_provenance` fills, so
    a review page can never claim a provenance the data does not carry. When the column is
    entirely absent (a frame that never went through `attach_wp_provenance`), returns the
    explicit `WP-Provenienz unbekannt` string instead of guessing.
    """
    if "wp_source" not in game_plays.columns:
        return "WP-Provenienz unbekannt (Frame ohne wp_source-Spalte)"

    counts = Counter(game_plays["wp_source"].to_list())
    oof_n = counts.get("oof", 0)
    champion_n = counts.get("champion", 0)
    null_n = counts.get(None, 0)

    parts = [f"{oof_n} Plays out-of-fold", f"{champion_n} Plays Champion-Modell"]
    if null_n:
        parts.append(f"{null_n} Plays ohne Modellwert")
    return "WP-Werte: " + ", ".join(parts)


def build_wp_review_page(
    game_plays: pl.DataFrame,
    *,
    game_id: str,
    synthetic_clock: bool = True,
    generated_on: date | None = None,
) -> str:
    """Assemble REQ-S1-15's standalone per-game WP review page. No filesystem writes -- the
    chart reaches HTML only through `reports.render.fig_to_data_uri`, and the page itself only
    through `reports.render.render_page`.

    A game with fewer than two non-null `home_wp` rows still returns a page (with a
    `keine WP-Daten` notice in place of the header summary) -- one broken game must never abort
    a batch run over many games (T-1.4-29).
    """
    generated_on = generated_on or date.today()
    game_label = _game_label(game_plays, game_id=game_id)

    fig = render_wp_review(
        game_plays, game_label=game_label, synthetic_clock=synthetic_clock
    )
    chart_data_uri = fig_to_data_uri(fig)

    annotations = select_wp_annotations(game_plays)
    swing_rows = _swing_table_rows(annotations)
    summary = _header_summary(game_plays)
    wp_provenance = _wp_provenance_text(game_plays)

    return render_page(
        "wp_review.html.j2",
        game_label=game_label,
        game_id=game_id,
        generated_on=generated_on,
        synthetic_clock=synthetic_clock,
        summary=summary,
        no_wp_data_notice=_NO_WP_DATA_NOTICE,
        chart_data_uri=chart_data_uri,
        swing_rows=swing_rows,
        wp_provenance=wp_provenance,
    )


def _resolve_oof_wp(
    working: pl.DataFrame, processed_dir: Path, keys: list[str]
) -> pl.DataFrame:
    """Return `(_PROVENANCE_ROW_ID, wp, home_wp, away_wp, wpa)` for every row of `working`
    matched by `oof_predictions_wp.parquet`'s `(game_id, play_id)` key.

    A missing file, or a file present but matching no row, both return an empty (not an
    error) frame with the same schema -- the caller turns "no OOF match" into "score with the
    champion instead", never a crash.

    The persisted OOF file carries only the raw `wp` probability column (`oof_frame`'s
    contract, `model/evaluate.py` lines 300-331) -- `home_wp`/`away_wp`/`wpa` are derived from
    it here via `prepare_wp_data` + `add_wp_variables`, the exact same two-step pipeline
    `model/score.py::score_plays` runs for the champion side, so the OOF and champion halves
    of a review page are never derived by two different formulas.
    """
    empty = (
        working.select(_PROVENANCE_ROW_ID)
        .clear()
        .with_columns(
            [pl.lit(None, dtype=pl.Float64).alias(c) for c in _WP_VALUE_COLUMNS]
        )
    )

    oof_path = Path(processed_dir) / _OOF_PREDICTIONS_FILENAME
    if not oof_path.exists():
        return empty

    oof_raw = pl.read_parquet(oof_path).select(*keys, WP_PROBABILITY_COLUMN)
    context = working.join(oof_raw, on=keys, how="inner").sort(keys)
    if context.height == 0:
        return empty

    derived = add_wp_variables(prepare_wp_data(context))
    return derived.select(_PROVENANCE_ROW_ID, *_WP_VALUE_COLUMNS)


def attach_wp_provenance(
    plays: pl.DataFrame,
    *,
    processed_dir: Path,
    scored: pl.DataFrame | None,
) -> pl.DataFrame:
    """Resolve WP values for `plays` the same honest way `own_team.attach_epa` resolves EPA:
    out-of-fold for historical plays, the promoted champion for new ones -- never mixed for
    the same play (T-1.4-66).

    Adds `wp`, `home_wp`, `away_wp`, `wpa` and a `wp_source` Utf8 column (`"oof"` |
    `"champion"` | `None`) to a copy of `plays`. Row count and `(game_id, play_id)` order are
    preserved, so a caller can partition the result by `game_id` without re-sorting.

    Resolution order, and why it is never a coalesce:
    1. Read `oof_predictions_wp.parquet` from `processed_dir` when it exists; a missing file
       is not an error, it just means no play qualifies as historical.
    2. Join those OOF rows onto `plays` on `(game_id, play_id)`; matched rows get
       `wp_source = "oof"`.
    3. The champion join (from `scored`, e.g. `plays_scored.parquet`) is applied ONLY to the
       rows step 2 did not match -- an anti-join on the OOF-matched row ids removes them from
       the candidate set before the champion join ever runs, so a play present in both
       sources can never have its OOF value silently overwritten. This is "by construction",
       not a later tie-break: the champion join literally never sees an OOF-matched row.
    4. A row matched by neither keeps null WP columns and a null `wp_source`; the caller
       (`build_wp_review_page`) excludes it from the curve/swing table and counts it in the
       page's provenance sentence.
    """
    keys = ["game_id", "play_id"]
    working = plays.drop(
        [c for c in _WP_VALUE_COLUMNS if c in plays.columns]
    ).with_row_index(name=_PROVENANCE_ROW_ID)

    oof_resolved = _resolve_oof_wp(working, Path(processed_dir), keys)

    # Historical half: rows matched by the persisted out-of-fold predictions.
    oof_matched = working.join(
        oof_resolved, on=_PROVENANCE_ROW_ID, how="inner"
    ).with_columns(wp_source=pl.lit("oof", dtype=pl.Utf8))

    # New-game half: the champion join runs only against rows the OOF join above never
    # touched (anti-join on the row id) -- the "never coalesce OOF and champion values for
    # the same play" guarantee, enforced by construction.
    oof_unmatched = working.join(
        oof_resolved.select(_PROVENANCE_ROW_ID), on=_PROVENANCE_ROW_ID, how="anti"
    )
    if scored is not None:
        champion_cols = scored.select(*keys, *_WP_VALUE_COLUMNS)
        oof_unmatched = oof_unmatched.join(champion_cols, on=keys, how="left")
    else:
        oof_unmatched = oof_unmatched.with_columns(
            [pl.lit(None, dtype=pl.Float64).alias(c) for c in _WP_VALUE_COLUMNS]
        )
    oof_unmatched = oof_unmatched.with_columns(
        wp_source=pl.when(pl.col(WP_PROBABILITY_COLUMN).is_not_null())
        .then(pl.lit("champion"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )

    result = (
        pl.concat([oof_matched, oof_unmatched], how="vertical")
        .sort(_PROVENANCE_ROW_ID)
        .drop(_PROVENANCE_ROW_ID)
    )
    return result
