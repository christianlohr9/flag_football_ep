"""Contract guard tests for the Phase 2.1 CV tracking pilot subpackage skeleton.

Four guard groups, all runnable without the `cv` extras group installed and without
touching video, weights or network:

1. Lazy-import guard (D-07/D-08): `import flag_football_ep.cli` must never pull in a
   `cv` extras third-party dependency.
2. Help-surface guard: `ffep cv --help` must list every one of the 17 pilot verbs.
3. Signature guard: every contract function/method named in plan 02.1-02's
   `<interfaces>` block must exist on its module with its declared parameter names --
   this is what stops a later plan from silently renaming a parameter another plan
   already calls.
4. Delegation guard: `cv/commands.py` must import nothing from `flag_football_ep` at
   module level, and `cli.py` must wire `cv_app` in exactly once.
"""

from __future__ import annotations

import importlib
import inspect
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_PATH = REPO_ROOT / "src" / "flag_football_ep" / "cli.py"
COMMANDS_PATH = REPO_ROOT / "src" / "flag_football_ep" / "cv" / "commands.py"

# --- 1. lazy-import guard --------------------------------------------------------

FORBIDDEN_CV_MODULES = frozenset(
    {
        "rfdetr",
        "trackers",
        "supervision",
        "sahi",
        "transformers",
        "umap",
        "autodistill",
        "autodistill_grounding_dino",
        "torch",
        "cv2",
    }
)

CV_CONTRACT_MODULES: tuple[str, ...] = (
    "frames",
    "sighting",
    "prelabel",
    "dataset",
    "detect",
    "registry",
    "track",
    "teams",
    "homography",
    "coordinates",
    "schema",
    "export",
    "overlay",
    "continuity",
    "accuracy",
    "radar",
    "benchmark",
)


def test_cli_import_pulls_no_cv_extras_dependency() -> None:
    """D-07/D-08: `import flag_football_ep.cli` stays usable without `uv sync --extra cv`."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import flag_football_ep.cli; import json, sys; "
            "print(json.dumps(sorted(sys.modules)))",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    loaded_modules = set(json.loads(result.stdout))
    leaked = FORBIDDEN_CV_MODULES & loaded_modules
    assert not leaked, (
        f"importing flag_football_ep.cli pulled in cv-extras module(s) {leaked} -- "
        "D-07/D-08 requires the core CLI to stay usable without `uv sync --extra cv`; "
        "every cv/* module's heavy dependency must be imported inside a function body"
    )


def test_every_cv_contract_module_imports_without_cv_extras() -> None:
    """The per-module counterpart to the `cli.py`-chain guard above: a stray
    module-top-level `import cv2`/`import torch`/etc. buried inside a single `cv/*`
    module (not reachable from `cli.py`'s own import chain, since `cv/commands.py`
    only imports each `cv/*` module inside a function body) must still fail loudly here.
    """
    import_lines = "; ".join(
        f"import flag_football_ep.cv.{module}" for module in CV_CONTRACT_MODULES
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"{import_lines}; import json, sys; print(json.dumps(sorted(sys.modules)))",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    loaded_modules = set(json.loads(result.stdout))
    leaked = FORBIDDEN_CV_MODULES & loaded_modules
    assert not leaked, (
        f"importing every flag_football_ep.cv.* contract module pulled in cv-extras "
        f"module(s) {leaked} -- every cv/* module's heavy dependency must be imported "
        "inside a function body, never at module top level"
    )


# --- 2. help-surface guard --------------------------------------------------------

CV_VERBS: tuple[str, ...] = (
    "sight",
    "sample",
    "prelabel",
    "cvat-push",
    "cvat-pull",
    "dataset",
    "train",
    "promote",
    "track",
    "calibrate",
    "coords",
    "export",
    "overlay",
    "continuity",
    "accuracy",
    "radar",
    "benchmark",
)


@pytest.fixture(scope="module")
def cv_help_output() -> str:
    from typer.testing import CliRunner

    from flag_football_ep.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["cv", "--help"])
    assert result.exit_code == 0, result.output
    return result.output


@pytest.mark.parametrize("verb", CV_VERBS)
def test_cv_help_lists_verb(cv_help_output: str, verb: str) -> None:
    assert verb in cv_help_output, f"`ffep cv --help` is missing verb {verb!r}"


def test_ffep_help_lists_cv_subcommand() -> None:
    from typer.testing import CliRunner

    from flag_football_ep.cli import app

    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0, result.output
    assert "cv" in result.output


# --- 3. signature guard -----------------------------------------------------------

# (module, function_name, declared parameter names, in declared order)
FUNCTION_CONTRACTS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("frames", "clip_paths", ("config", "session_id")),
    ("frames", "clip_number", ("path",)),
    ("frames", "extract_frames", ("clip", "out_dir", "at_seconds")),
    ("frames", "sample_training_frames", ("config", "session_id", "target", "seed", "out_dir")),
    ("frames", "write_manifest", ("manifest", "path")),
    ("frames", "read_manifest", ("path",)),
    ("sighting", "sight_session", ("config", "session_id", "out_csv")),
    ("sighting", "recommend_inference_settings", ("rows", "config")),
    ("prelabel", "prelabel_frames", ("config", "frames_dir", "out_dir", "force")),
    ("dataset", "validate_coco", ("coco_dir", "manifest")),
    ("dataset", "dataset_hash", ("root",)),
    ("dataset", "create_cvat_task", ("config", "coco_dir", "name")),
    ("dataset", "export_cvat_task", ("config", "task_id", "out_dir")),
    (
        "detect",
        "train_detector",
        (
            "config",
            "dataset_dir",
            "epochs",
            "batch_size",
            "grad_accum",
            "resolution",
            "device",
            "output_dir",
            "register",
            "from_artifacts",
        ),
    ),
    ("detect", "load_detector", ("config", "run_id")),
    ("detect", "detect_video", ("config", "clip", "model", "resolution", "sahi")),
    ("registry", "detector_model_name", ("config",)),
    ("registry", "register_detector_model", ("checkpoint", "name", "config")),
    ("registry", "promote", ("name", "run_id", "config")),
    ("registry", "resolve_champion", ("name", "config")),
    (
        "track",
        "track_session",
        ("config", "session_id", "run_id", "resolution", "sahi", "out_path"),
    ),
    ("teams", "assign_teams", ("tracks", "config", "crops_by_track")),
    ("homography", "load_calibration", ("path",)),
    ("homography", "transformer_for", ("hover_position_id", "calibration")),
    ("homography", "reprojection_error_yards", ("transformer", "source", "target")),
    ("homography", "pick_points", ("clip", "hover_position_id", "out_csv", "at_second")),
    ("coordinates", "foot_point", ("xyxy",)),
    ("coordinates", "add_field_coordinates", ("tracks", "config", "calibration")),
    ("schema", "empty_tracking_frame", ()),
    ("schema", "conform_tracking", ("df",)),
    ("schema", "write_tracking_parquet", ("df", "path")),
    ("export", "export_tracking_csv", ("parquet_path", "csv_path")),
    ("overlay", "render_track_overlay", ("config", "clip", "tracks", "out_path")),
    ("continuity", "measure_continuity", ("tracks", "config", "review_csv")),
    ("continuity", "summarise_review", ("review_csv",)),
    ("accuracy", "load_gt_positions", ("path",)),
    ("accuracy", "prepare_gt_frames", ("config", "tracks", "n_frames", "out_dir")),
    ("accuracy", "measure_position_error", ("gt", "tracks", "config")),
    ("radar", "render_radar_frame", ("tracks_at_frame", "config", "size_wh")),
    ("radar", "render_showcase_reel", ("config", "clip_numbers", "tracks", "out_path")),
    ("benchmark", "extrapolate_game_runtime", ("stages", "footage_seconds", "game_seconds")),
)

# (module, class_name, method_name, declared parameter names, in declared order)
METHOD_CONTRACTS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("teams", "TeamClassifier", "__init__", ("self", "device", "embedder", "seed")),
    ("teams", "TeamClassifier", "fit", ("self", "crops")),
    ("teams", "TeamClassifier", "predict", ("self", "crops")),
    ("homography", "ViewTransformer", "__init__", ("self", "source", "target")),
    ("homography", "ViewTransformer", "transform_points", ("self", "points")),
    (
        "homography",
        "ViewTransformer",
        "transform_image",
        ("self", "image", "resolution_wh"),
    ),
    ("registry", "RFDETRWrapper", "predict", ("self", "context", "model_input", "params")),
)


@pytest.mark.parametrize("module_name,function_name,params", FUNCTION_CONTRACTS)
def test_contract_function_signature(
    module_name: str, function_name: str, params: tuple[str, ...]
) -> None:
    module = importlib.import_module(f"flag_football_ep.cv.{module_name}")
    assert hasattr(module, function_name), (
        f"flag_football_ep.cv.{module_name} is missing contract function "
        f"{function_name!r} (declared in plan 02.1-02's <interfaces> block)"
    )
    func = getattr(module, function_name)
    actual = tuple(inspect.signature(func).parameters)
    assert actual == params, (
        f"flag_football_ep.cv.{module_name}.{function_name} has parameters {actual!r}, "
        f"expected {params!r} -- a later plan renamed a parameter another plan calls"
    )


@pytest.mark.parametrize("module_name,class_name,method_name,params", METHOD_CONTRACTS)
def test_contract_method_signature(
    module_name: str, class_name: str, method_name: str, params: tuple[str, ...]
) -> None:
    module = importlib.import_module(f"flag_football_ep.cv.{module_name}")
    assert hasattr(module, class_name), (
        f"flag_football_ep.cv.{module_name} is missing contract class {class_name!r}"
    )
    cls = getattr(module, class_name)
    assert hasattr(cls, method_name), (
        f"flag_football_ep.cv.{module_name}.{class_name} is missing method {method_name!r}"
    )
    method = getattr(cls, method_name)
    actual = tuple(inspect.signature(method).parameters)
    assert actual == params, (
        f"flag_football_ep.cv.{module_name}.{class_name}.{method_name} has parameters "
        f"{actual!r}, expected {params!r} -- a later plan renamed a parameter another "
        "plan calls"
    )


def test_cverror_is_base_of_every_named_cv_exception() -> None:
    from flag_football_ep.cv import CvError
    from flag_football_ep.cv.dataset import DatasetError
    from flag_football_ep.cv.detect import MissingClipError, WeightsNotFound
    from flag_football_ep.cv.frames import ClipNotFound
    from flag_football_ep.cv.homography import CalibrationError
    from flag_football_ep.cv.prelabel import PrelabelBackendUnavailable

    for exc_type in (
        DatasetError,
        MissingClipError,
        WeightsNotFound,
        ClipNotFound,
        CalibrationError,
        PrelabelBackendUnavailable,
    ):
        assert issubclass(exc_type, CvError), f"{exc_type} does not subclass CvError"


def test_dataset_class_names_excludes_ball() -> None:
    """C-12: no ball detection in early CV phases."""
    from flag_football_ep.cv.dataset import CLASS_NAMES

    assert CLASS_NAMES == ("player", "referee")
    assert "ball" not in CLASS_NAMES


# --- 4. delegation guard -----------------------------------------------------------


def test_commands_module_has_no_top_level_flag_football_ep_import() -> None:
    source = COMMANDS_PATH.read_text(encoding="utf-8")
    for line in source.splitlines():
        if line.startswith("from flag_football_ep") or line.startswith("import flag_football_ep"):
            pytest.fail(
                f"cv/commands.py has a module-top-level flag_football_ep import: {line!r} "
                "-- every cv/* delegation must be a function-body-local import"
            )


def test_commands_module_only_imports_typer_and_pathlib_at_top_level() -> None:
    import ast

    tree = ast.parse(COMMANDS_PATH.read_text(encoding="utf-8"))
    top_level_modules = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            top_level_modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top_level_modules.add(node.module.split(".")[0])
    assert top_level_modules <= {"typer", "pathlib", "typing"}, (
        f"cv/commands.py has unexpected top-level imports {top_level_modules}; only "
        "typer/pathlib/typing may be imported at module level"
    )


def test_cli_wires_cv_app_exactly_once() -> None:
    source = CLI_PATH.read_text(encoding="utf-8")
    count = source.count("add_typer(cv_app")
    assert count == 1, f"cli.py contains {count} 'add_typer(cv_app' occurrences, expected 1"


def test_cli_diff_from_cv_wiring_is_minimal() -> None:
    source = CLI_PATH.read_text(encoding="utf-8")
    assert "from flag_football_ep.cv.commands import cv_app" in source
    assert 'app.add_typer(cv_app, name="cv")' in source
