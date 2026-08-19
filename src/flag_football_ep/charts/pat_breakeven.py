"""PAT break-even chart: expected points for a 1-point try vs a 2-point try across the range
of possible 2-point success rates, with the dataset's own empirical rates marked (REQ-S1-10,
REQ-S1-14).

Answers "ab welchem Spielstand/Restzeit lohnt 2?" (docs/plan-1-analytics-refresh.md §Phase
1.3) at the level this dataset supports: a single break-even 2-point success rate, derived
from the pooled 1-point rate (`baselines.one_point_rate / 2`), against which the dataset's
own observed 2-point rate -- with its confidence interval -- is plotted. `render_pat_breakeven`
sets the `Agg` backend before importing `pyplot` so this module never requires a display,
matching `model/evaluate.py`'s headless-figure convention.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from flag_football_ep.features.mutations import PatBaselines

if TYPE_CHECKING:
    from matplotlib.figure import Figure


def render_pat_breakeven(baselines: PatBaselines, title: str | None = None) -> "Figure":
    """Render the PAT break-even chart from `baselines`, without showing or saving it.

    Plots, over a 0-to-1 grid of candidate 2-point success rates `p2`: a horizontal line at
    the observed 1-pt expected points (`1 * baselines.one_point_rate`, constant, labelled
    with the 1-pt rate and attempt count) and the line `2 * p2` for the swept 2-point rate.
    Going for 2 is the higher-EV call exactly when `p2 > baselines.one_point_rate / 2` -- that
    break-even rate is marked with a vertical line and annotated "go for 2 above this rate".
    The dataset's own observed 2-point rate is marked with a second vertical line, shaded
    across `baselines.two_point_ci` when available, and annotated with its attempt count.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    p2_grid = np.linspace(0.0, 1.0, 200)
    one_point_ep = np.full_like(p2_grid, baselines.one_point_rate)
    two_point_ep = 2 * p2_grid
    breakeven_p2 = baselines.one_point_rate / 2

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(
        p2_grid,
        one_point_ep,
        label=(
            f"1-pt try (observed rate {baselines.one_point_rate:.3f}, "
            f"n={baselines.one_point_attempts})"
        ),
    )
    ax.plot(p2_grid, two_point_ep, label="2-pt try (2 x p2)")

    ax.axvline(breakeven_p2, color="gray", linestyle="--")
    ax.annotate(
        f"go for 2 above this rate ({breakeven_p2:.3f})",
        xy=(breakeven_p2, 2 * breakeven_p2),
        xytext=(min(breakeven_p2 + 0.05, 0.95), 2 * breakeven_p2 + 0.3),
        ha="left",
    )

    ax.axvline(baselines.two_point_rate, color="tab:orange", linestyle=":")
    if baselines.two_point_ci is not None:
        ax.axvspan(
            baselines.two_point_ci[0],
            baselines.two_point_ci[1],
            color="tab:orange",
            alpha=0.15,
        )
    ax.annotate(
        (
            f"observed 2-pt rate {baselines.two_point_rate:.3f} "
            f"(n={baselines.two_point_attempts})"
        ),
        xy=(baselines.two_point_rate, 0),
        xytext=(min(baselines.two_point_rate + 0.02, 0.95), 0.15),
        ha="left",
    )

    ax.set_xlabel("2-point conversion rate")
    ax.set_ylabel("expected points")
    ax.set_title(title or "PAT break-even: 1-pt try vs 2-pt try")
    ax.legend(loc="lower right")
    fig.tight_layout()
    return fig


def write_pat_breakeven(
    baselines: PatBaselines, out_path: Path, overwrite: bool = False
) -> Path:
    """Render and write the PAT break-even chart as a PNG at `out_path`.

    Refuses to overwrite an existing file unless `overwrite` is True (mirrors
    `model/train.py`'s `_export_pickle` write-once guard), so an accidental rerun cannot
    silently replace a chart already shared with the coaching staff.
    """
    out_path = Path(out_path)
    if out_path.exists() and not overwrite:
        raise FileExistsError(
            f"refusing to overwrite existing PAT break-even chart: {out_path}"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig = render_pat_breakeven(baselines)
    try:
        fig.savefig(out_path)
    finally:
        import matplotlib.pyplot as plt

        plt.close(fig)
    return out_path
