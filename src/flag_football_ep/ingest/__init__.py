"""Ingest stage: source-specific raw-file parsers converging on `canonical.CANONICAL_COLUMNS`.

Each module here (`hudl`, `legacy`, `sportapp`, `ifaf`) owns exactly one source's raw
vocabulary and mutation chain. They do not share mutation code with each other -- the
only shared convergence point is `flag_football_ep.canonical.conform_to_canonical` plus
the small, already-canonical score/points tail in `canonical.add_score_columns` /
`canonical.add_scoring_play_team`. See `ingest/sportapp.py`'s module docstring for the
anti-unification rationale (RESEARCH Architecture Pattern 3).
"""
