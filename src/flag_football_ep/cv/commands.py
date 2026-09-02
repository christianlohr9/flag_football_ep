"""Typer sub-app for the `ffep cv` CV tracking pilot command surface (Phase 2.1).

Every command follows `cli.py`'s own shape exactly: load the TOML config, lazily
import the `flag_football_ep.cv.*` module it delegates to, and call exactly one
contract function defined there. Every `cv/*` stage module (and its heavy third-party
CV dependency -- `rfdetr`, `trackers`, `supervision`, `sahi`, `transformers`,
`umap-learn`, `opencv-python`, `torch`) is imported inside the command function body,
never at this module's top level, so `import flag_football_ep.cli` (which imports
`cv_app` from this module) stays usable without the `cv` extras group installed
(D-07/D-08). This module itself only defines the option surface and the delegation
contract, so plans 03-16 (which implement the `cv/*` module bodies) never have to edit
it.

Secrets (CVAT credentials, resolved from `cfg.cv.cvat_username_env`/`cvat_password_env`)
are read from environment variables and are never printed or interpolated into echoed
output, matching `cli.py`'s existing rule.
"""

from pathlib import Path
from typing import List, Optional

import typer

cv_app = typer.Typer(
    no_args_is_help=True, add_completion=False, help="CV tracking pilot (Phase 2.1)"
)

DEFAULT_CONFIG = Path("ffep.toml")


@cv_app.command()
def sight(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    session: Optional[str] = typer.Option(
        None, "--session", help="Pilot session id (default: cfg.cv.pilot_session_id)"
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the hover-positions CSV output path"
    ),
    domain: str = typer.Option(
        "drone", "--domain", help="Capture domain to sight (drone, sideline, broadcast)"
    ),
) -> None:
    """Run the sighting pass over every clip in a session."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_id = session or cfg.cv.pilot_session_id

    from flag_football_ep.cv.sighting import sight_session

    result = sight_session(cfg, session_id, out_csv=out, domain=domain)

    typer.echo(f"sighting: {result.csv_path} ({len(result.rows)} clips)")
    for notice in result.notices:
        typer.echo(f"notice: {notice}")


@cv_app.command()
def sample(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    session: Optional[str] = typer.Option(
        None, "--session", help="Pilot session id (default: cfg.cv.pilot_session_id)"
    ),
    target: Optional[int] = typer.Option(
        None, "--target", help="Target frame count (default: cfg.cv.label_frame_target)"
    ),
    seed: int = typer.Option(
        20260516, "--seed", help="Random seed for the stratified sample"
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the sampled-frames output directory"
    ),
    domain: str = typer.Option(
        "drone",
        "--domain",
        help="Capture domain to sample from (drone, sideline, broadcast)",
    ),
) -> None:
    """Draw the stratified training-frame sample for labeling."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_id = session or cfg.cv.pilot_session_id
    target_count = target or cfg.cv.label_frame_target
    out_dir = out or (cfg.paths.labels / "frames")

    from flag_football_ep.cv.frames import sample_training_frames, write_manifest

    manifest = sample_training_frames(
        cfg, session_id, target=target_count, seed=seed, out_dir=out_dir, domain=domain
    )
    manifest_path = write_manifest(manifest, out_dir / "manifest.json")

    typer.echo(f"manifest: {manifest_path} ({len(manifest.frames)} frames)")


@cv_app.command()
def prelabel(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    frames: Path = typer.Option(..., "--frames", help="Input sampled-frames directory"),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the COCO pre-label output directory"
    ),
    force: bool = typer.Option(
        False,
        "--force/--no-force",
        help="Re-run pre-labeling even if pre-annotations already exist on disk",
    ),
    backend: Optional[str] = typer.Option(
        None,
        "--backend",
        help=(
            "Force a specific pre-labeling backend by name (e.g. 'finetuned' for "
            "active-learning iterations, which must never silently fall back to a "
            "zero-shot backend); default: auto-resolve the zero-shot fallback chain"
        ),
    ),
) -> None:
    """Pre-label sampled frames -- zero-shot via Grounding DINO by default, or a
    forced backend (e.g. the fine-tuned detector) when `--backend` is given."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    out_dir = out or (cfg.paths.labels / "prelabel")

    from flag_football_ep.cv.prelabel import prelabel_frames

    result = prelabel_frames(cfg, frames, out_dir, force=force, backend=backend)

    typer.echo(
        f"coco: {result.coco_path} ({result.n_frames} frames, {result.n_boxes} boxes)"
    )
    for notice in result.notices:
        typer.echo(f"notice: {notice}")


@cv_app.command(name="cvat-push")
def cvat_push(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    coco: Path = typer.Option(
        ..., "--coco", help="Prelabel COCO package directory to push"
    ),
    name: str = typer.Option(..., "--name", help="CVAT task name"),
    max_images: Optional[int] = typer.Option(
        None,
        "--max-images",
        help=(
            "Split into multiple <= N-image tasks first (weekend-sized labelling "
            "sessions), named '<name>-<index>' (1-based); default: push the whole "
            "directory as one task"
        ),
    ),
) -> None:
    """Push a pre-labeled COCO package to CVAT as a new task (or several, split by
    `--max-images`)."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.cv.dataset import create_cvat_task

    if max_images is None:
        task_id = create_cvat_task(cfg, coco, name=name)
        typer.echo(f"cvat task: {task_id} ({name})")
        return

    import json
    import tempfile

    from flag_football_ep.cv.dataset import split_coco_for_task_upload

    with tempfile.TemporaryDirectory() as tmp_dir:
        chunk_dirs = split_coco_for_task_upload(coco, Path(tmp_dir), max_images=max_images)
        for index, chunk_dir in enumerate(chunk_dirs, start=1):
            chunk_name = f"{name}-{index}"
            n_frames = len(
                json.loads((chunk_dir / "instances.json").read_text(encoding="utf-8"))["images"]
            )
            task_id = create_cvat_task(cfg, chunk_dir, name=chunk_name)
            typer.echo(f"cvat task: {task_id} ({chunk_name}, {n_frames} frames)")


@cv_app.command(name="cvat-pull")
def cvat_pull(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    task: int = typer.Option(..., "--task", help="CVAT task id"),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the COCO export output directory"
    ),
) -> None:
    """Pull reviewed annotations for a CVAT task back down as a COCO export."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    out_dir = out or (cfg.paths.labels / "cvat-export")

    from flag_football_ep.cv.dataset import export_cvat_task

    coco_path = export_cvat_task(cfg, task, out_dir)

    typer.echo(f"coco: {coco_path}")


@cv_app.command()
def dataset(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    coco: Path = typer.Option(..., "--coco", help="COCO export directory to validate"),
    manifest: Path = typer.Option(
        ..., "--manifest", help="Sample manifest to validate against"
    ),
) -> None:
    """Validate a COCO export against its sample manifest and report dataset stats."""
    from flag_football_ep.config import load_config

    load_config(config)

    from flag_football_ep.cv.frames import read_manifest

    loaded_manifest = read_manifest(manifest)

    from flag_football_ep.cv.dataset import validate_coco

    stats = validate_coco(coco, loaded_manifest)

    typer.echo(f"dataset: {coco} ({stats.n_images} images, {stats.content_sha256})")
    for class_name, count in stats.n_boxes.items():
        typer.echo(f"  {class_name}: {count} boxes")


@cv_app.command()
def train(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    dataset: Optional[Path] = typer.Option(
        None,
        "--dataset",
        help="Validated COCO dataset directory (required unless --from-artifacts is given)",
    ),
    epochs: Optional[int] = typer.Option(
        None, "--epochs", help="Override cfg.cv.train_epochs"
    ),
    batch_size: Optional[int] = typer.Option(
        None, "--batch-size", help="Override cfg.cv.train_batch_size"
    ),
    grad_accum: Optional[int] = typer.Option(
        None, "--grad-accum", help="Override cfg.cv.train_grad_accum"
    ),
    resolution: Optional[int] = typer.Option(
        None, "--resolution", help="Override cfg.cv.resolution"
    ),
    device: Optional[str] = typer.Option(
        None, "--device", help="Override cfg.cv.device"
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Artifact output directory"),
    register: bool = typer.Option(
        True,
        "--register/--no-register",
        help=(
            "Register the trained checkpoint in MLflow "
            "(--no-register is the remote-training machine's mode, D-05)"
        ),
    ),
    from_artifacts: Optional[Path] = typer.Option(
        None,
        "--from-artifacts",
        help=(
            "Register a checkpoint+metrics directory produced by an earlier "
            "--no-register run on another machine, without retraining"
        ),
    ),
    resume: Optional[Path] = typer.Option(
        None,
        "--resume",
        help=(
            "Resume from a full PyTorch Lightning checkpoint (e.g. <out>/last.ckpt) -- "
            "continues toward --epochs as a total target, not an additional count"
        ),
    ),
) -> None:
    """Train the RF-DETR player/referee detector."""
    if from_artifacts is None and dataset is None:
        raise typer.BadParameter(
            "--dataset is required unless --from-artifacts is given"
        )

    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.cv.detect import train_detector

    result = train_detector(
        cfg,
        dataset,
        epochs=epochs,
        batch_size=batch_size,
        grad_accum=grad_accum,
        resolution=resolution,
        device=device,
        output_dir=out,
        register=register,
        from_artifacts=from_artifacts,
        resume=resume,
    )

    typer.echo(f"run: {result.run_id}")
    typer.echo(f"checkpoint: {result.checkpoint}")
    for metric_name, value in result.metrics.items():
        typer.echo(f"  {metric_name}: {value}")


@cv_app.command()
def promote(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    run: Optional[str] = typer.Option(
        None,
        "--run",
        help=(
            "MLflow run id to promote; defaults to the most recent FINISHED run of "
            "cfg.cv.detector_experiment"
        ),
    ),
) -> None:
    """Promote a detector training run to the `champion` alias."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import mlflow

    from flag_football_ep.cv import registry
    from flag_football_ep.model import mlflow_store

    if run is not None:
        run_id = run
    else:
        mlflow_store.configure(cfg)
        runs = mlflow.search_runs(
            experiment_names=[cfg.cv.detector_experiment],
            filter_string="attributes.status = 'FINISHED'",
            order_by=["attributes.start_time DESC"],
            max_results=1,
            output_format="list",
        )
        if not runs:
            raise typer.BadParameter(
                f"no FINISHED runs found for experiment {cfg.cv.detector_experiment!r} "
                "to promote"
            )
        run_id = runs[0].info.run_id

    name = registry.detector_model_name(cfg)
    version = registry.promote(name, run_id, cfg)
    typer.echo(f"{name}: promoted run {run_id} to champion (version {version})")


@cv_app.command()
def track(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    session: Optional[str] = typer.Option(
        None, "--session", help="Pilot session id (default: cfg.cv.pilot_session_id)"
    ),
    run: Optional[str] = typer.Option(
        None, "--run", help="Detector MLflow run id (default: champion alias)"
    ),
    resolution: Optional[int] = typer.Option(
        None, "--resolution", help="Override cfg.cv.resolution"
    ),
    sahi: Optional[bool] = typer.Option(
        None, "--sahi/--no-sahi", help="Override SAHI tiled-slicing inference"
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the tracking Parquet output path"
    ),
) -> None:
    """Detect + track every clip in a session."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_id = session or cfg.cv.pilot_session_id

    from flag_football_ep.cv.track import track_session

    result = track_session(
        cfg, session_id, run_id=run, resolution=resolution, sahi=sahi, out_path=out
    )

    typer.echo(
        f"tracks: {result.parquet_path} ({result.n_clips} clips, {result.n_tracks} tracks)"
    )
    typer.echo(f"stage timings: {result.timings_path}")
    for notice in result.notices:
        typer.echo(f"notice: {notice}")


@cv_app.command()
def teams(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    session: Optional[str] = typer.Option(
        None, "--session", help="Pilot session id (default: cfg.cv.pilot_session_id)"
    ),
    out: Optional[Path] = typer.Option(
        None,
        "--out",
        help="Override the output Parquet path (default: rewrite --tracks in place)",
    ),
) -> None:
    """Assign per-track team ids over a tracking Parquet and rewrite it.

    Mirrors exactly how the pilot runs drove the library: `extract_track_crops`
    (torso-region crops) feeds one session-wide `assign_teams` fit, and the
    `team_id`-filled frame is written back through `write_tracking_parquet`.
    """
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_id = session or cfg.cv.pilot_session_id

    import polars as pl

    tracks_df = pl.read_parquet(tracks)

    from flag_football_ep.cv.teams import assign_teams, extract_track_crops

    crops_by_track = extract_track_crops(cfg, session_id, tracks_df)
    result = assign_teams(tracks_df, cfg, crops_by_track=crops_by_track)

    from flag_football_ep.cv.schema import write_tracking_parquet

    out_path = out or tracks
    written = write_tracking_parquet(result.tracks, out_path)

    n_assigned = (
        result.tracks.filter(pl.col("team_id").is_not_null())
        .select(["clip_number", "track_id"])
        .unique()
        .height
    )
    typer.echo(f"teams: {written} ({n_assigned} tracks assigned)")
    for notice in result.notices:
        typer.echo(f"notice: {notice}")


@cv_app.command()
def calibrate(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    clip: int = typer.Option(
        ..., "--clip", help="Clip number to extract a calibration still from"
    ),
    hover_position: str = typer.Option(
        ..., "--hover-position", help="Hover position id"
    ),
    at_second: float = typer.Option(
        0.0,
        "--at-second",
        help="Timestamp (seconds) to extract the calibration still from",
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the calibration CSV output path"
    ),
) -> None:
    """Extract a calibration still and seed the homography calibration CSV."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.cv.frames import clip_number as clip_number_of
    from flag_football_ep.cv.frames import clip_paths

    matches = [
        path
        for path in clip_paths(cfg, cfg.cv.pilot_session_id)
        if clip_number_of(path) == clip
    ]
    if not matches:
        raise typer.BadParameter(
            f"clip {clip} not found for session {cfg.cv.pilot_session_id!r}"
        )
    clip_path = matches[0]
    out_csv = out or cfg.reference.homography_calibration

    from flag_football_ep.cv.homography import pick_points

    written = pick_points(clip_path, hover_position, out_csv, at_second=at_second)

    typer.echo(f"calibration: {written}")


@cv_app.command()
def coords(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the field-coordinates output Parquet path"
    ),
) -> None:
    """Project tracked boxes into field-yard coordinates via homography."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    tracks_df = pl.read_parquet(tracks)

    from flag_football_ep.cv.homography import load_calibration

    calibration = load_calibration(cfg.reference.homography_calibration)

    from flag_football_ep.cv.coordinates import add_field_coordinates

    projected = add_field_coordinates(tracks_df, cfg, calibration)

    from flag_football_ep.cv.schema import write_tracking_parquet

    out_path = out or tracks
    written = write_tracking_parquet(projected, out_path)

    typer.echo(f"coords: {written} ({projected.height} rows)")


@cv_app.command()
def export(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output CSV path"),
) -> None:
    """Export the tracking Parquet as a plain CSV."""
    from flag_football_ep.config import load_config

    load_config(config)

    out_path = out or tracks.with_suffix(".csv")

    from flag_football_ep.cv.export import export_tracking_csv

    written = export_tracking_csv(tracks, out_path)

    typer.echo(f"csv: {written}")


@cv_app.command()
def overlay(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    clip: List[int] = typer.Option(
        [], "--clip", help="Clip number(s) to render (repeatable); default: all clips"
    ),
    out_dir: Optional[Path] = typer.Option(
        None, "--out-dir", help="Override the overlay video output directory"
    ),
) -> None:
    """Render per-clip tracking overlay videos."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    tracks_df = pl.read_parquet(tracks)
    clip_numbers = clip or sorted(tracks_df["clip_number"].unique().to_list())
    # Overlays are rendered player footage (PII, T-2.1-01) -- they belong under the
    # gitignored label tree, never under `reports/` or `data/processed/`.
    out_directory = out_dir or (cfg.paths.labels / cfg.cv.pilot_session_id / "overlays")

    from flag_football_ep.cv.frames import clip_number as clip_number_of
    from flag_football_ep.cv.frames import clip_paths
    from flag_football_ep.cv.overlay import render_track_overlay

    paths_by_number = {
        clip_number_of(path): path for path in clip_paths(cfg, cfg.cv.pilot_session_id)
    }
    for number in clip_numbers:
        clip_path = paths_by_number[number]
        clip_tracks = tracks_df.filter(pl.col("clip_number") == number)
        written = render_track_overlay(
            cfg, clip_path, clip_tracks, out_directory / f"clip_{number:03d}.mp4"
        )
        typer.echo(f"overlay: {written}")


@cv_app.command()
def continuity(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    review: Optional[Path] = typer.Option(
        None, "--review", help="Override the continuity review CSV path"
    ),
) -> None:
    """Measure per-clip track continuity (the C-09 ID-switch gate metric)."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    tracks_df = pl.read_parquet(tracks)

    from flag_football_ep.cv.continuity import measure_continuity

    result = measure_continuity(tracks_df, cfg, review_csv=review)

    typer.echo(f"continuity: {result.review_csv} ({len(result.rows)} clips)")
    for key, value in result.summary.items():
        typer.echo(f"  {key}: {value}")
    for notice in result.notices:
        typer.echo(f"notice: {notice}")


@cv_app.command()
def accuracy(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    gt: Optional[Path] = typer.Option(
        None,
        "--gt",
        help="Ground-truth positions CSV (default: cfg.reference.gt_positions)",
    ),
    prepare: bool = typer.Option(
        False,
        "--prepare/--measure",
        help=(
            "--prepare exports frames to be ground-truth-labelled and seeds the GT CSV "
            "rows; --measure (default) computes the position error against filled-in "
            "GT rows"
        ),
    ),
    n_frames: int = typer.Option(
        20, "--n-frames", help="Number of frames to export when --prepare is used"
    ),
) -> None:
    """Prepare ground-truth frames or measure position error (the C-09 accuracy gate metric)."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    tracks_df = pl.read_parquet(tracks)

    if prepare:
        from flag_football_ep.cv.accuracy import prepare_gt_frames

        out_dir = cfg.paths.labels / "gt-frames"
        written = prepare_gt_frames(cfg, tracks_df, n_frames=n_frames, out_dir=out_dir)
        typer.echo(f"gt frames: {written}")
        return

    gt_path = gt or cfg.reference.gt_positions

    from flag_football_ep.cv.accuracy import load_gt_positions, measure_position_error

    gt_df = load_gt_positions(gt_path)
    result = measure_position_error(gt_df, tracks_df, cfg)

    typer.echo(
        f"accuracy: n={result.n_points} median={result.median_yards:.2f}yd "
        f"p90={result.p90_yards:.2f}yd max={result.max_yards:.2f}yd"
    )


@cv_app.command()
def radar(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    clip: List[int] = typer.Option(
        [], "--clip", help="Clip number(s) to render (repeatable); default: all clips"
    ),
    out: Optional[Path] = typer.Option(None, "--out", help="Output showcase reel path"),
) -> None:
    """Render the radar-view showcase reel."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    tracks_df = pl.read_parquet(tracks)
    clip_numbers = clip or sorted(tracks_df["clip_number"].unique().to_list())
    # The reel's left half is rendered player footage (PII, T-2.1-01) -- like the
    # overlay videos it belongs under the gitignored label tree, never under
    # `reports/` or `data/processed/` (which is ignored as regenerable pipeline
    # output, not as PII).
    out_path = out or (
        cfg.paths.labels / cfg.cv.pilot_session_id / "showcase" / "showcase.mp4"
    )

    from flag_football_ep.cv.radar import render_showcase_reel

    written = render_showcase_reel(cfg, clip_numbers, tracks_df, out_path)

    typer.echo(f"showcase: {written}")


@cv_app.command()
def freeze(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    run: str = typer.Option(..., "--run", help="MLflow detector run id to freeze"),
    dataset_hash: Optional[str] = typer.Option(
        None,
        "--dataset-hash",
        help="Content hash of the dataset the run was trained on",
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the tracked pin file path"
    ),
    force: bool = typer.Option(
        False,
        "--force/--no-force",
        help="Re-pin over an existing pin for a different run id (a re-freeze is a decision)",
    ),
) -> None:
    """Pin a detector run as the hackathon-frozen baseline (distinct from champion)."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.cv import registry
    from flag_football_ep.cv.freeze import freeze as freeze_run
    from flag_football_ep.cv.freeze import write_freeze_pin

    name = registry.detector_model_name(cfg)
    version = freeze_run(name, run, cfg)
    # Tracked, no-PII pin path (data/reference/, not the gitignored data/processed/) --
    # bundle reproducibility must survive a clean checkout.
    pin_path = out or (cfg.paths.reference / "hackathon_freeze.json")
    if force and pin_path.exists():
        pin_path.unlink()
    pin_path = write_freeze_pin(cfg, run, dataset_hash or "", pin_path)

    typer.echo(f"frozen: {name} v{version} (run={run})")
    typer.echo(f"pin: {pin_path}")


@cv_app.command()
def bundle(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    kind: str = typer.Option(
        ...,
        "--kind",
        help="Bundle kind: dev, test, or transfer (cv.bundle.BUNDLE_KINDS)",
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the bundle output directory"
    ),
) -> None:
    """Build a dev/test/transfer deliverable bundle from the frozen detector."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.cv.bundle import build_bundle
    from flag_football_ep.cv.freeze import read_freeze_pin

    pin = read_freeze_pin(cfg.paths.reference / "hackathon_freeze.json")
    out_dir = out or (cfg.paths.processed / "bundles" / kind)

    result = build_bundle(cfg, kind, pin, out_dir)

    typer.echo(
        f"bundle: {result.archive_path} ({result.n_files} files, {result.content_sha256})"
    )


@cv_app.command()
def deliver(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    archive: Path = typer.Option(
        ..., "--archive", help="Bundle archive path to deliver"
    ),
    remote: str = typer.Option(
        ..., "--remote", help="Remote URI to deliver the bundle to"
    ),
) -> None:
    """Deliver a built bundle archive to a remote location (e.g. the OTC OBS bucket)."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.cv.bundle import deliver_bundle

    # T-2.2-13: echo the remote URI only -- never a credential value.
    remote_uri = deliver_bundle(cfg, archive, remote)

    typer.echo(f"delivered: {remote_uri}")


@cv_app.command(name="active-learn")
def active_learn(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    iteration: int = typer.Option(
        ..., "--iteration", help="Active-learning iteration number"
    ),
    target: int = typer.Option(
        ..., "--target", help="Target frame count for this iteration"
    ),
    seed: int = typer.Option(20260516, "--seed", help="Random seed for the selection"),
    session: List[str] = typer.Option(
        [],
        "--session",
        help="Session id(s) to draw from (repeatable); default: cfg.cv.pilot_session_id",
    ),
    out_dir: Optional[Path] = typer.Option(
        None, "--out-dir", help="Override the AL selection output directory"
    ),
) -> None:
    """Select the next active-learning iteration's frames by uncertainty + diversity."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_ids = session or [cfg.cv.pilot_session_id]
    out_directory = out_dir or (
        cfg.paths.labels / "active-learning" / f"iteration-{iteration}"
    )

    from flag_football_ep.cv.active_learning import select_al_frames, selection_to_frame_manifest
    from flag_football_ep.cv.frames import write_manifest

    selection = select_al_frames(
        cfg, session_ids, iteration, target, seed, out_directory
    )

    # Bridge the AL-native selection manifest into the FrameSampleManifest shape
    # `ffep cv prelabel`/`ffep cv dataset` expect at `<out_dir>/manifest.json` --
    # requires a single-session selection (see selection_to_frame_manifest), which
    # is exactly what this command's own single-session-per-call usage produces.
    frame_manifest = selection_to_frame_manifest(cfg, selection)
    write_manifest(frame_manifest, out_directory / "manifest.json")

    typer.echo(f"selection: {out_directory} ({len(selection.frames)} frames)")


@cv_app.command(name="eval-split")
def eval_split(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    domain: List[str] = typer.Option(
        [],
        "--domain",
        help="Capture domain(s) to include (repeatable); default: drone, sideline, broadcast",
    ),
    fraction: float = typer.Option(
        0.2, "--fraction", help="Fraction of each domain's clips to hold out"
    ),
    seed: int = typer.Option(
        20260516, "--seed", help="Random seed for the eval-clip split"
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the eval-split CSV output path"
    ),
) -> None:
    """Freeze the per-domain held-out evaluation-clip split (D-04/D-13)."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    domains = domain or ["drone", "sideline", "broadcast"]
    out_csv = out or (cfg.paths.reference / "eval_split.csv")

    from flag_football_ep.cv.frames import freeze_eval_clips

    split = freeze_eval_clips(cfg, domains, fraction, seed, out_csv)

    n_clips = sum(len(clips) for clips in split.clips_by_domain.values())
    typer.echo(f"eval split: {out_csv} ({n_clips} clips)")


@cv_app.command(name="hackathon-split")
def hackathon_split(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    dev_session: Optional[str] = typer.Option(
        None,
        "--dev-session",
        help="Public dev-set session id (default: cfg.cv.pilot_session_id)",
    ),
    test_session: str = typer.Option(
        ..., "--test-session", help="Private test-set session id (the second game)"
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the hackathon-split CSV output path"
    ),
    exclusions: Optional[Path] = typer.Option(
        None, "--exclusions", help="Override the AL-exclusion CSV output path"
    ),
) -> None:
    """Write the hackathon dev/private_test role split and the training-pool exclusion
    for the private test session (DATA-04). The only sanctioned way to produce or
    refresh `data/reference/hackathon_split.csv`/`data/reference/al_excluded_sessions.csv`
    -- never hand-edit either file."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    dev_session_id = dev_session or cfg.cv.pilot_session_id
    out_csv = out or (cfg.paths.reference / "hackathon_split.csv")
    exclusions_csv = exclusions or (cfg.paths.reference / "al_excluded_sessions.csv")

    from flag_football_ep.cv.testset import write_al_exclusion, write_hackathon_split

    split = write_hackathon_split(cfg, dev_session_id, test_session, out_csv)
    write_al_exclusion(
        cfg,
        test_session,
        reason="private hackathon test game -- never a training-pool candidate",
        requirement="DATA-04",
        out_csv=exclusions_csv,
    )

    typer.echo(
        f"hackathon split: {out_csv} (dev={len(split.dev_clips)} clips, "
        f"test={len(split.test_clips)} clips)"
    )
    typer.echo(f"al exclusion: {exclusions_csv} ({test_session})")


@cv_app.command()
def detections(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    session: Optional[str] = typer.Option(
        None, "--session", help="Pilot session id (default: cfg.cv.pilot_session_id)"
    ),
    domain: str = typer.Option(
        "drone", "--domain", help="Capture domain to export detections for"
    ),
    run: Optional[str] = typer.Option(
        None, "--run", help="Detector MLflow run id (default: champion alias)"
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the detections Parquet output path"
    ),
) -> None:
    """Run the detector over a session/domain and export raw per-frame detections."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_id = session or cfg.cv.pilot_session_id
    out_path = out or (
        cfg.paths.processed / f"{session_id}_{domain}_detections.parquet"
    )

    from flag_football_ep.cv.export import export_detections_parquet

    written = export_detections_parquet(cfg, session_id, domain, run, out_path)

    typer.echo(f"detections: {written}")


@cv_app.command()
def crops(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    session: Optional[str] = typer.Option(
        None, "--session", help="Pilot session id (default: cfg.cv.pilot_session_id)"
    ),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    out_dir: Optional[Path] = typer.Option(
        None, "--out-dir", help="Override the crop image output directory"
    ),
) -> None:
    """Write one image crop per tracked box to a directory."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_id = session or cfg.cv.pilot_session_id

    import polars as pl

    tracks_df = pl.read_parquet(tracks)
    out_directory = out_dir or (cfg.paths.labels / session_id / "crops")

    from flag_football_ep.cv.export import export_track_crops

    n_crops = export_track_crops(cfg, session_id, tracks_df, out_directory)

    typer.echo(f"crops: {out_directory} ({n_crops} crops)")


@cv_app.command(name="eval-domains")
def eval_domains(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    run: str = typer.Option(..., "--run", help="Detector MLflow run id to evaluate"),
    split: Optional[Path] = typer.Option(
        None,
        "--split",
        help="Eval-clip split CSV (default: cfg.reference eval_split.csv)",
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the per-domain metrics output path"
    ),
) -> None:
    """Evaluate a detector run per domain against the frozen eval-clip split (C-05/D-04)."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    split_path = split or (cfg.paths.reference / "eval_split.csv")
    out_path = out or (cfg.paths.reports / f"eval_domains_{run}.json")

    from flag_football_ep.cv.detect import evaluate_per_domain

    metrics = evaluate_per_domain(cfg, run, split_path, out_path)

    for domain_name, domain_metrics in metrics.items():
        typer.echo(f"{domain_name}: {domain_metrics}")


@cv_app.command()
def benchmark(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    timings: Path = typer.Option(
        ...,
        "--timings",
        help=(
            "Stage-timings JSON written by `ffep cv track` (the "
            "`<session>_stage_timings.json` sibling of the tracking Parquet)"
        ),
    ),
    game_minutes: float = typer.Option(
        50.0, "--game-minutes", help="Full-game duration in minutes to extrapolate to"
    ),
) -> None:
    """Extrapolate measured per-stage timings to a full-game inference runtime
    (the C-09 <1h/game runtime gate metric)."""
    from flag_football_ep.config import load_config

    load_config(config)

    import json

    payload = json.loads(timings.read_text(encoding="utf-8"))

    from flag_football_ep.cv.benchmark import StageTiming, extrapolate_game_runtime

    stages = tuple(
        StageTiming(
            stage=entry["stage"], seconds=entry["seconds"], frames=entry["frames"]
        )
        for entry in payload["stages"]
    )
    # The artifact's own footage_seconds: the sum of the session clips' real
    # (inventory-declared) durations, persisted by `track_session` -- never a
    # per-stage frame sum divided by a hardcoded fps (every stage covers the same
    # frames, so summing them would inflate the denominator ~4x and understate the
    # extrapolated runtime in exactly the direction that flatters the gate).
    footage_seconds = payload.get("footage_seconds")
    if not isinstance(footage_seconds, (int, float)) or footage_seconds <= 0:
        raise typer.BadParameter(
            f"{timings} carries no positive footage_seconds -- re-run `ffep cv track` "
            "to regenerate the stage-timings artifact"
        )

    result = extrapolate_game_runtime(
        stages, float(footage_seconds), game_minutes * 60.0
    )

    typer.echo(f"extrapolated: {result.extrapolated_game_minutes:.2f} min/game")
    typer.echo(f"formula: {result.formula}")
