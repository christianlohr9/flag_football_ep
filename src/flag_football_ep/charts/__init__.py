"""Reusable, headless chart renderers shared between Phase 1.3's analysis and Phase 1.4's
coaching products (REQ-S1-14).

Every renderer in this package returns a matplotlib `Figure` rather than displaying or
saving it directly -- the caller chooses whether to write a PNG (`write_*` helpers) or log
the figure to MLflow as a run artifact. This keeps chart construction reusable across a CLI
command, a training run's artifact logging, and the opponent/own-team report generation
Phase 1.4 builds on top of this package.
"""

from __future__ import annotations
