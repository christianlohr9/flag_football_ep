"""Ingest package: source-specific parsers (hudl, legacy, sportapp, ifaf) that each
converge on `flag_football_ep.canonical.conform_to_canonical` as the single shared
schema, never on shared mutation code between sources.

This file stays empty of re-exports so sibling plans can add `hudl.py`/`legacy.py`/
`sportapp.py` beside `ifaf.py` without touching this file.
"""
