"""Standalone "game decisions" cheat-sheet page: PAT break-even and 4th-down conversion by
distance on one printable, camp-ready page with plain-German readings (REQ-S1-14).

CONTEXT.md locks this as the page that "stands alone, printable, camp-ready, usable without
the other reports" -- both charts already exist (PAT baselines from Phase 1.3,
`estimate_fourth_down_rates`/`render_fourth_down` from plan 03); this module is the assembly
step that turns them into one HTML page an HC can open on a tablet or print at a camp.
`build_decisions_page` performs no filesystem writes -- plan 12's `build.py` owns every write.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from flag_football_ep.charts.fourth_down import render_fourth_down
from flag_football_ep.charts.pat_breakeven import render_pat_breakeven
from flag_football_ep.features.mutations import (
    FOURTH_DOWN_DISTANCE_BUCKETS,
    InsufficientPatAttempts,
    estimate_fourth_down_rates,
    estimate_pat_baselines,
)
from flag_football_ep.reports.render import fig_to_data_uri, render_page

DECISIONS_FILENAME: str = "decisions.html"

_PAT_HINWEIS_NO_ATTEMPTS = (
    "Hinweis: Für PAT-Versuche liegen in diesem Datensatz keine Daten vor -- der "
    "Break-even-Vergleich kann nicht berechnet werden."
)


def build_decisions_page(plays: pl.DataFrame, *, generated_on: date | None = None) -> str:
    """Assemble the standalone PAT break-even + 4th-down decisions cheat sheet.

    Calls `estimate_pat_baselines(plays)` and `estimate_fourth_down_rates(plays)` -- reused,
    never reimplemented -- renders both charts via `render_pat_breakeven`/`render_fourth_down`,
    embeds each through `reports.render.fig_to_data_uri` (never the Figure's own direct PNG
    export, so the close-in-`finally` discipline stays in one place), then renders
    `decisions_cheatsheet.html.j2` and returns the resulting HTML string. Performs no
    filesystem writes.

    A corpus with zero PAT attempts makes `estimate_pat_baselines` raise
    `InsufficientPatAttempts`; this function catches that, renders the page with the 4th-down
    half only, and passes an explicit German `Hinweis` string for the PAT half rather than
    letting the whole page fail to render on a thin corpus.

    The plain-German `readings` passed to the template always carry the sample size (`n=...`)
    of the underlying rate -- the project's statistical-honesty bias applies to prose as much
    as to charts and tables.
    """
    generated_on = generated_on or date.today()

    readings: list[str] = []
    pat_hinweis: str | None = None
    pat_chart_uri: str | None = None

    try:
        baselines = estimate_pat_baselines(plays)
    except InsufficientPatAttempts:
        baselines = None
        pat_hinweis = _PAT_HINWEIS_NO_ATTEMPTS

    if baselines is not None:
        breakeven_rate = baselines.one_point_rate / 2
        readings.append(
            f"Ab einer 2-Punkte-Rate von {breakeven_rate:.0%} lohnt sich der "
            "2-Punkte-Versuch."
        )

        if baselines.two_point_ci is not None:
            low, high = baselines.two_point_ci
            readings.append(
                f"Beobachtet: {baselines.two_point_rate:.0%} "
                f"(n={baselines.two_point_attempts}, CI {low:.0%}–{high:.0%})."
            )
            if low > breakeven_rate:
                readings.append(
                    "Empfehlung: 2-Punkte-Versuch -- die beobachtete Rate liegt mit ihrem "
                    "Konfidenzintervall über der Break-even-Rate."
                )
            elif high < breakeven_rate:
                readings.append(
                    "Empfehlung: 1-Punkt-Versuch -- die beobachtete Rate liegt mit ihrem "
                    "Konfidenzintervall unter der Break-even-Rate."
                )
            else:
                readings.append("Zu dünne Datenlage für eine klare Empfehlung.")
        else:
            readings.append(
                f"Beobachtet: {baselines.two_point_rate:.0%} "
                f"(n={baselines.two_point_attempts})."
            )
            readings.append("Zu dünne Datenlage für eine klare Empfehlung.")

        pat_fig = render_pat_breakeven(baselines)
        pat_chart_uri = fig_to_data_uri(pat_fig)

    fourth_down_rates = estimate_fourth_down_rates(plays)
    populated_buckets = [
        bucket
        for bucket in fourth_down_rates.buckets
        if bucket.attempts > 0 and bucket.rate is not None
    ]
    if populated_buckets:
        best = max(populated_buckets, key=lambda bucket: bucket.rate)
        worst = min(populated_buckets, key=lambda bucket: bucket.rate)
        readings.append(
            f"4th down, beste Distance: {best.label} yards mit {best.rate:.0%} "
            f"(n={best.attempts})."
        )
        readings.append(
            f"4th down, schwächste Distance: {worst.label} yards mit {worst.rate:.0%} "
            f"(n={worst.attempts})."
        )
    else:
        readings.append("4th down: keine Daten für diesen Korpus.")

    if fourth_down_rates.overall_rate is not None:
        readings.append(
            f"4th down insgesamt: {fourth_down_rates.overall_rate:.0%} "
            f"(n={fourth_down_rates.total_attempts})."
        )
    else:
        readings.append("4th down insgesamt: keine Daten.")

    fourth_down_fig = render_fourth_down(fourth_down_rates)
    fourth_down_chart_uri = fig_to_data_uri(fourth_down_fig)

    return render_page(
        "decisions_cheatsheet.html.j2",
        generated_on=generated_on,
        readings=readings,
        pat_hinweis=pat_hinweis,
        pat_chart_uri=pat_chart_uri,
        fourth_down_chart_uri=fourth_down_chart_uri,
        distance_buckets=FOURTH_DOWN_DISTANCE_BUCKETS,
    )
