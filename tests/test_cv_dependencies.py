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


def test_rfdetr_pml_variants_absent_from_source() -> None:
    """RF-DETR's XLarge/2XLarge variants (and the `rfdetr_plus` package they ship
    under) are PML-1.0-licensed, not Apache-2.0 like the Nano/Small/Medium/Large
    tier — forbidden by C-06. See
    `.planning/phases/02.2-dataset-buildout/02.2-RESEARCH.md` Common Pitfalls,
    Pitfall 2.

    Comment lines and inline-comment suffixes are stripped before matching, so
    this test itself (and any doc-comment naming these strings for explanatory
    purposes) does not trip its own guard — never assert on an unfiltered
    whole-file grep.
    """
    forbidden = ("RFDETRXLarge", "RFDETR2XLarge", "rfdetr_plus")
    src_root = REPO_ROOT / "src"
    offenders: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if line.strip().startswith("#"):
                continue
            code_part = line.split("#", 1)[0]
            for name in forbidden:
                if name in code_part:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno} references {name!r}")

    assert not offenders, (
        "PML-1.0-licensed RF-DETR variant referenced under src/ — forbidden by "
        f"C-06 (02.2-RESEARCH.md Pitfall 2): {offenders}"
    )


def test_versioning_group_lists_dvc() -> None:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    versioning_group = data["project"]["optional-dependencies"]["versioning"]
    cv_group = data["project"]["optional-dependencies"]["cv"]

    assert any(entry.startswith("dvc>") or entry == "dvc" for entry in versioning_group), (
        "the `versioning` optional-dependencies group must list `dvc`"
    )
    assert any(entry.startswith("dvc-s3") for entry in versioning_group), (
        "the `versioning` optional-dependencies group must list `dvc-s3`"
    )
    assert not any("dvc" in entry for entry in cv_group), (
        "`dvc`/`dvc-s3` must live in the `versioning` group, not `cv` — a "
        "`dvc pull`-only workflow must not drag in the torch stack"
    )
