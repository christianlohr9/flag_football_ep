"""AGPL supply-chain guard for the Phase 2.1 `cv` optional-dependency group.

`boxmot` (the tracker named in the source plan docs and research notes) is
AGPL-3.0-licensed as of 2026-08-24 and violates the project's no-AGPL policy
(C-06). `trackers` (roboflow, Apache-2.0) is the tracker implementation this
phase uses instead (OC-SORT through plan 02.1-12; BoT-SORT from the 02.1-12/
02.1-14 gap-fix iteration onward, same package) — see
`.planning/phases/02.1-cv-tracking-pilot-go-no-go-gate/02.1-RESEARCH.md` §Common
Pitfalls (Pitfall 2).
"""

from __future__ import annotations

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"
UV_LOCK = REPO_ROOT / "uv.lock"


def test_boxmot_absent_from_pyproject() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")
    assert "boxmot" not in text, (
        "pyproject.toml mentions 'boxmot' — AGPL-3.0, forbidden by C-06; "
        "use `trackers` (roboflow, Apache-2.0) instead"
    )


def test_boxmot_absent_from_uv_lock() -> None:
    if not UV_LOCK.exists():
        return
    text = UV_LOCK.read_text(encoding="utf-8")
    assert "boxmot" not in text, (
        "uv.lock mentions 'boxmot' — AGPL-3.0, forbidden by C-06; "
        "use `trackers` (roboflow, Apache-2.0) instead"
    )


def test_cv_group_exists_and_lists_trackers() -> None:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    cv_group = data["project"]["optional-dependencies"]["cv"]
    assert any("trackers" in entry for entry in cv_group), (
        "the `cv` optional-dependencies group must list `trackers` "
        "(roboflow, Apache-2.0) — the tracker implementation this phase uses; "
        "`boxmot` is forbidden (AGPL-3.0, C-06)"
    )
