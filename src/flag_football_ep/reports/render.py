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

from functools import lru_cache

from jinja2 import Environment, PackageLoader, select_autoescape

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
