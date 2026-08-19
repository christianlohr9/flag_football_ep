"""Repo hygiene gates -- keep the plan 01.2-17 cleanup from creeping back.

These tests enforce, as automated gates rather than discipline, that:
- notebooks/ only carries thin demos + moved SHAP notebooks, all valid JSON
- no notebook or src/ file resurrects the old `sys.path.insert(0, '../Python')` /
  `from helper_add_*` pattern the package absorbed in earlier plans
- `Python/` (the deleted helper directory) does not come back
- the repository root stays free of `.csv`/`.xlsx` sprawl
- demo notebooks stay thin (capped code-cell count) so they cannot grow into pipelines
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"
SRC_DIR = REPO_ROOT / "src"
MODELS_DIR = REPO_ROOT / "models"

# The four notebook-era fixed-filename pickles plan 01.3-03 retired to models/legacy/ --
# these must never reappear directly under models/ (models/legacy/ itself is exempt).
_FIXED_NAME_PICKLES = (
    "ep_model.pkl",
    "ep_model_simple.pkl",
    "wp_model.pkl",
    "wp_model_simple.pkl",
)

# The three thin demos created by plan 01.2-17; capped at 15 code cells each so they
# cannot silently grow pipeline logic instead of importing it.
DEMO_NOTEBOOKS = ("pipeline_demo.ipynb", "ep_model_demo.ipynb", "wp_model_demo.ipynb")
MAX_DEMO_CODE_CELLS = 15


def _notebook_paths() -> list[Path]:
    return sorted(NOTEBOOKS_DIR.glob("*.ipynb"))


def _code_cell_sources(nb: dict) -> list[str]:
    return [
        "".join(cell.get("source", []))
        for cell in nb.get("cells", [])
        if cell.get("cell_type") == "code"
    ]


def test_notebooks_dir_has_at_least_the_three_demos_and_two_moved_notebooks():
    paths = _notebook_paths()
    names = {p.name for p in paths}
    assert len(paths) >= 5, paths
    for demo in DEMO_NOTEBOOKS:
        assert demo in names, f"missing demo notebook: {demo}"
    assert "explainable_ai_ep.ipynb" in names
    assert "explainable_ai_wp.ipynb" in names


def test_every_notebook_under_notebooks_parses_as_json():
    paths = _notebook_paths()
    assert paths, "no notebooks found under notebooks/"
    for path in paths:
        json.loads(path.read_text(encoding="utf-8"))


def test_no_notebook_code_cell_contains_sys_path_insert():
    for path in _notebook_paths():
        nb = json.loads(path.read_text(encoding="utf-8"))
        for src in _code_cell_sources(nb):
            assert "sys.path.insert" not in src, f"{path} still does sys.path.insert"


def test_no_notebook_code_cell_imports_from_helper_modules():
    for path in _notebook_paths():
        nb = json.loads(path.read_text(encoding="utf-8"))
        for src in _code_cell_sources(nb):
            assert "from helper_" not in src, f"{path} still imports from a helper_ module"


def test_no_src_file_contains_sys_path():
    """Matches the plan's own acceptance gate: grep -rn "sys.path" src/ must be empty --
    including in comments/docstrings, since a lingering mention is exactly the kind of
    thing that invites the hack to come back.
    """
    offenders = []
    for path in SRC_DIR.rglob("*.py"):
        if "sys.path" in path.read_text(encoding="utf-8"):
            offenders.append(path)
    assert not offenders, f"src/ files still reference sys.path: {offenders}"


def test_python_helper_directory_does_not_exist():
    assert not (REPO_ROOT / "Python").exists()


def test_insights_directory_does_not_exist():
    assert not (REPO_ROOT / "insights").exists()


def test_repository_root_has_no_csv_or_xlsx_sprawl():
    root_csvs = list(REPO_ROOT.glob("*.csv"))
    root_xlsx = list(REPO_ROOT.glob("*.xlsx"))
    assert not root_csvs, f"root has stray CSVs: {root_csvs}"
    assert not root_xlsx, f"root has stray XLSX files: {root_xlsx}"


def test_models_dir_has_no_fixed_name_pickles():
    """`models/` (not `models/legacy/`) must never hold a notebook-era fixed-filename
    pickle -- `flag_football_ep.model.train._export_pickle` writes
    `{prefix}_model_{YYYYMMDD}_{hash8}.pkl` and refuses to overwrite; a fixed-name pickle
    reappearing directly under `models/` means something bypassed that guard (T-1.3-10).
    """
    offenders = [name for name in _FIXED_NAME_PICKLES if (MODELS_DIR / name).exists()]
    assert not offenders, f"models/ has stale fixed-name pickle(s): {offenders}"


@pytest.mark.parametrize("demo_name", DEMO_NOTEBOOKS)
def test_demo_notebook_has_at_most_15_code_cells(demo_name: str):
    path = NOTEBOOKS_DIR / demo_name
    nb = json.loads(path.read_text(encoding="utf-8"))
    n_code_cells = len(_code_cell_sources(nb))
    assert n_code_cells <= MAX_DEMO_CODE_CELLS, (
        f"{demo_name} has {n_code_cells} code cells, expected at most {MAX_DEMO_CODE_CELLS}"
    )


@pytest.mark.parametrize("demo_name", DEMO_NOTEBOOKS)
def test_demo_notebook_has_no_mutation_or_write_calls(demo_name: str):
    """Demo notebooks read processed Parquet and visualize -- no mutation logic, no
    model fitting, no file writing (plan 01.2-17 task 2 action item 3)."""
    path = NOTEBOOKS_DIR / demo_name
    nb = json.loads(path.read_text(encoding="utf-8"))
    forbidden = ("write_parquet", "write_csv", ".fit(", "xgb.train", "mlflow.")
    for src in _code_cell_sources(nb):
        for token in forbidden:
            assert token not in src, f"{demo_name} contains forbidden token {token!r}"
