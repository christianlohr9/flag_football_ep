"""Report aggregation and HTML assembly for Phase 1.4's coaching products (REQ-S1-12..16).

This package owns turning scored plays into the four report families the HC consumes
(opponent tendency, own-team efficiency, decision cheat-sheet, WP review): polars
aggregation modules build the per-product context, `render.py` turns that context plus a
Jinja2 template into a standalone HTML string, and `build.py` owns every filesystem write
-- the dated run folder under `reports/` and the always-current `reports/latest/` copy.
Every matplotlib `Figure` reaches HTML through `render.fig_to_data_uri`; a builder module
must never call `fig.savefig` directly, or a batch run risks leaking dozens of open
figures into pyplot's global registry (mirrors the headless-Agg, close-on-write discipline
already established in `charts/pat_breakeven.py`).
"""

from __future__ import annotations
