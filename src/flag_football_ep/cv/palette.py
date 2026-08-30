"""Shared team/marker colour palette for `overlay.py` and `radar.py`.

BGR (cv2 convention) colours, chosen to be visually distinct from one another and
from typical grass/turf green: two team colours, one referee colour, one
null-team-id colour.

`teams.assign_teams` anchors `team_id` to jersey colour (see `teams.py`'s module
docstring): team_id 0 is always the cluster whose sampled crops read as more red,
team_id 1 is the other. This palette follows that contract exactly -- team_id 0 draws
RED, team_id 1 draws BLUE -- so a viewer matching dots to jersey colour on the
showcase reel sees the colour they expect, regardless of which arbitrary KMeans
cluster label produced the assignment underneath.

A single shared definition, imported by both `overlay.py` and `radar.py`, instead of
each module keeping its own copy: two divergent palettes previously let the overlay
half and the radar half of the showcase reel draw the same team_id in different
colours.
"""

from __future__ import annotations

TEAM_COLORS: dict[int, tuple[int, int, int]] = {
    0: (0, 60, 230),  # red -- team_id 0 (jersey-colour anchored, see teams.py)
    1: (255, 80, 0),  # blue -- team_id 1
}
REFEREE_COLOR: tuple[int, int, int] = (0, 220, 255)  # yellow
NULL_TEAM_COLOR: tuple[int, int, int] = (170, 170, 170)  # gray

__all__ = ["TEAM_COLORS", "REFEREE_COLOR", "NULL_TEAM_COLOR"]
