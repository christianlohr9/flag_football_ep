"""Shared Jinja2 rendering machinery for every Phase 1.4 report product (REQ-S1-12, REQ-S1-16).

`build_environment`/`render_page` are pure -- they perform no filesystem writes and, aside
from `PackageLoader` resolving template sources from the installed package, no I/O beyond
reading the shipped `.j2` templates. `write_report_run` is the only function in this module
that touches the filesystem, and it is the single place a report run lands on disk: every
figure a builder wants to show reaches HTML through `fig_to_data_uri` (never `fig.savefig`
directly in a builder module), matching the headless-Agg, close-on-write discipline already
established in `charts/pat_breakeven.py`.
"""

from __future__ import annotations

import base64
import io
import shutil
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import Environment, PackageLoader, select_autoescape

if TYPE_CHECKING:
    from matplotlib.figure import Figure

EMBED_DPI: int = 150
"""DPI for chart PNGs embedded as data URIs. 150 balances tablet legibility and
print-to-PDF adequacy against the size a base64-inflated PNG adds to the HTML document
(RESEARCH Pitfall 5) -- higher DPIs bloat the file for marginal visible gain on a tablet
screen or printed page, lower DPIs read as blurry."""

ACCENT_COLOR: str = "#1f4e79"
"""The single accent colour used across every report template (CONTEXT.md's "clean
neutral -- white background, one accent color, no logos" style discretion)."""


@lru_cache(maxsize=1)
def build_environment() -> Environment:
    """Build (and cache) the Jinja2 `Environment` every report template renders through.

    Cached via `lru_cache` so a batch run producing dozens of pages does not rebuild the
    `PackageLoader` each time. `autoescape` must be set explicitly -- Jinja2 defaults it to
    `False`, and charted free text (`thrown_by`, `received_by`, play `description`, team
    names) reaches these templates verbatim from hand-charted Hudl/PBP exports.

    `select_autoescape` decides purely by matching the template name's final suffix against
    its enabled-extensions list. Every template source file in this package is named
    `*.html.j2` (so an editor/diff tool can tell it's both HTML and a Jinja source at a
    glance) -- the final suffix is `.j2`, not `.html`, so `["html", "htm", "xml"]` alone
    would silently leave autoescape off for every template this project actually ships.
    `"j2"` is added to the enabled list so the real `*.html.j2` filenames match.
    """
    return Environment(
        loader=PackageLoader("flag_football_ep", "templates"),
        autoescape=select_autoescape(["html", "htm", "xml", "j2"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def render_page(template_name: str, /, **context: object) -> str:
    """Render `template_name` from the cached environment with `context`. No I/O."""
    template = build_environment().get_template(template_name)
    return template.render(**context)


def fig_to_data_uri(fig: "Figure", *, dpi: int = EMBED_DPI) -> str:
    """Render `fig` to a base64 PNG `data:` URI, in memory -- no PNG is ever written to disk.

    This is the ONLY path from a rendered matplotlib `Figure` into a report's HTML in this
    codebase. A report builder that calls `fig.savefig` directly re-opens RESEARCH Pitfall
    6: a batch run producing dozens of report pages would leak that many open figures into
    pyplot's global registry, since nothing else in the builder closes them.

    `fig` is always closed via `matplotlib.pyplot.close(fig)` in a `finally` block, so a
    `savefig` failure mid-render still releases the figure. `pyplot` is imported lazily
    inside the function, matching the `charts/` package's headless-Agg convention of never
    importing pyplot at module import time.
    """
    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
        encoded = base64.b64encode(buf.getvalue()).decode("ascii")
    finally:
        import matplotlib.pyplot as plt

        plt.close(fig)
    return f"data:image/png;base64,{encoded}"


def write_report_run(rendered: dict[str, str], reports_root: Path) -> tuple[Path, Path]:
    """Write `rendered` ({filename: html}) into `reports_root/<YYYY-MM-DD>/`, then replace
    `reports_root/latest/` with a copy of that run folder. Returns `(run_dir, latest_dir)`.

    Order matters and is the point: the run folder is written to completion FIRST, and only
    after every file in `rendered` has landed does `latest/` get removed and recreated as a
    copy of it. A half-written run can therefore never be presented as `latest/` -- this is
    `pipeline._atomic_write_parquet`'s "never leave prior output half-written" discipline
    applied at directory granularity, using a copy (not a symlink) per CONTEXT.md's explicit
    delivery wording.

    Each key in `rendered` must be a plain filename: a caller-derived team code or game id
    can never escape `run_dir` via a path separator, `..`, or a null byte. A key containing
    any of those raises `ValueError` naming the offending key before any write happens
    (Security Domain row 2 -- defence in depth behind plan 12's allow-list validation).

    A stale same-day `run_dir` from an earlier call is cleared before this run's files are
    written, so a second run later the same day replaces rather than accumulates onto the
    first -- one call to `write_report_run` is one complete, self-contained report run, not
    an incremental add.
    """
    for filename in rendered:
        if "/" in filename or "\\" in filename or ".." in filename or "\x00" in filename:
            raise ValueError(f"unsafe filename key in rendered report set: {filename!r}")

    run_dir = reports_root / date.today().isoformat()
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    for filename, html in rendered.items():
        (run_dir / filename).write_text(html, encoding="utf-8")

    latest_dir = reports_root / "latest"
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    shutil.copytree(run_dir, latest_dir)
    return run_dir, latest_dir
