"""DVC layout tests for the Phase 2.2 dataset-versioning infrastructure (D-18).

Two concerns:
1. Prove the DVC add/push/pull mechanics work at all, against a throwaway local
   directory remote -- no network, no credentials. The real OTC OBS endpoint is
   untested in this environment (02.2-RESEARCH.md Common Pitfalls, Pitfall 3)
   and is validated separately once real credentials/bucket exist (plan 02.2-20).
2. Repo-level guards: no `.dvc` pointer file may reference raw video (PII,
   T-2.2-11), and the checked-in `.dvc/config` remote must carry an
   `endpointurl` but never a literal credential (T-2.2-10).

`pytest.importorskip("dvc")` at module scope means this file is a no-op skip
on a machine that never ran `uv sync --extra versioning`.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("dvc")

REPO_ROOT = Path(__file__).resolve().parent.parent


def _dvc(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Invoke DVC through `python -m dvc` so the test uses the environment's own
    install rather than relying on a `dvc` console script being on PATH."""
    return subprocess.run(
        [sys.executable, "-m", "dvc", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )


def test_dvc_local_remote_add_push_pull_round_trip(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    remote_dir = tmp_path / "remote"
    workspace.mkdir()
    remote_dir.mkdir()

    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)
    subprocess.run(
        ["git", "config", "user.email", "dvc-smoke-test@example.invalid"],
        cwd=workspace,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "DVC Smoke Test"], cwd=workspace, check=True)

    _dvc(["init", "-q"], cwd=workspace)

    tracked_file = workspace / "sample.txt"
    tracked_content = "dvc local-remote round-trip smoke test content\n"
    tracked_file.write_text(tracked_content, encoding="utf-8")

    _dvc(["add", "sample.txt"], cwd=workspace)
    _dvc(["remote", "add", "-d", "local", str(remote_dir)], cwd=workspace)
    _dvc(["push"], cwd=workspace)

    # Simulate a fresh checkout: drop the local cache and the workspace copy,
    # keeping only the `.dvc` pointer file -- exactly what a real `git clone` +
    # `dvc pull` would start from.
    shutil.rmtree(workspace / ".dvc" / "cache", ignore_errors=True)
    tracked_file.unlink()
    assert not tracked_file.exists()

    _dvc(["pull"], cwd=workspace)

    assert tracked_file.exists()
    assert tracked_file.read_text(encoding="utf-8") == tracked_content


def test_no_dvc_file_tracks_raw_video() -> None:
    # `*.dvc` also glob-matches the literal `.dvc/` config directory itself
    # (`*` matches a zero-length prefix) -- restrict to actual pointer files.
    offending = [
        str(path.relative_to(REPO_ROOT))
        for path in REPO_ROOT.rglob("*.dvc")
        if path.is_file() and "data/video/" in path.read_text(encoding="utf-8")
    ]
    assert not offending, (
        "`.dvc` pointer file(s) reference data/video/ -- raw footage is PII and "
        f"must never enter DVC tracking (T-2.2-11): {offending}"
    )


def test_dvc_config_remote_has_endpoint_but_no_credentials() -> None:
    config_path = REPO_ROOT / ".dvc" / "config"
    assert config_path.exists(), ".dvc/config not found -- was `dvc init` run?"
    text = config_path.read_text(encoding="utf-8")

    assert "endpointurl" in text, ".dvc/config remote is missing an endpointurl"
    assert "access_key_id" not in text.lower(), (
        ".dvc/config must not carry a literal access_key_id (T-2.2-10)"
    )
    assert "secret_access_key" not in text.lower(), (
        ".dvc/config must not carry a literal secret_access_key (T-2.2-10)"
    )
