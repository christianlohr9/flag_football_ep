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
) -> None:
    """Run the sighting pass over every clip in a session."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    session_id = session or cfg.cv.pilot_session_id

    from flag_football_ep.cv.sighting import sight_session

    result = sight_session(cfg, session_id, out_csv=out)

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
    seed: int = typer.Option(20260516, "--seed", help="Random seed for the stratified sample"),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the sampled-frames output directory"
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
        cfg, session_id, target=target_count, seed=seed, out_dir=out_dir
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
) -> None:
    """Zero-shot pre-label sampled frames via Grounding DINO."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)
    out_dir = out or (cfg.paths.labels / "prelabel")

    from flag_football_ep.cv.prelabel import prelabel_frames

    result = prelabel_frames(cfg, frames, out_dir, force=force)

    typer.echo(f"coco: {result.coco_path} ({result.n_frames} frames, {result.n_boxes} boxes)")
    for notice in result.notices:
        typer.echo(f"notice: {notice}")


@cv_app.command(name="cvat-push")
def cvat_push(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    coco: Path = typer.Option(..., "--coco", help="Prelabel COCO package directory to push"),
    name: str = typer.Option(..., "--name", help="CVAT task name"),
) -> None:
    """Push a pre-labeled COCO package to CVAT as a new task."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.cv.dataset import create_cvat_task

    task_id = create_cvat_task(cfg, coco, name=name)

    typer.echo(f"cvat task: {task_id} ({name})")


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
    manifest: Path = typer.Option(..., "--manifest", help="Sample manifest to validate against"),
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
    dataset: Path = typer.Option(..., "--dataset", help="Validated COCO dataset directory"),
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
    device: Optional[str] = typer.Option(None, "--device", help="Override cfg.cv.device"),
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

    typer.echo(f"tracks: {result.parquet_path} ({result.n_clips} clips, {result.n_tracks} tracks)")
    for notice in result.notices:
        typer.echo(f"notice: {notice}")


@cv_app.command()
def calibrate(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    clip: int = typer.Option(..., "--clip", help="Clip number to extract a calibration still from"),
    hover_position: str = typer.Option(..., "--hover-position", help="Hover position id"),
    at_second: float = typer.Option(
        0.0, "--at-second", help="Timestamp (seconds) to extract the calibration still from"
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
        path for path in clip_paths(cfg, cfg.cv.pilot_session_id) if clip_number_of(path) == clip
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
    out_directory = out_dir or (cfg.paths.processed / "overlay")

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


@cv_app.command()
def accuracy(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(..., "--tracks", help="Input tracking Parquet"),
    gt: Optional[Path] = typer.Option(
        None, "--gt", help="Ground-truth positions CSV (default: cfg.reference.gt_positions)"
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
    out_path = out or (cfg.paths.processed / "showcase.mp4")

    from flag_football_ep.cv.radar import render_showcase_reel

    written = render_showcase_reel(cfg, clip_numbers, tracks_df, out_path)

    typer.echo(f"showcase: {written}")


@cv_app.command()
def benchmark(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tracks: Path = typer.Option(
        ..., "--tracks", help="Input tracking Parquet (carries per-stage timing metadata)"
    ),
    game_minutes: float = typer.Option(
        50.0, "--game-minutes", help="Full-game duration in minutes to extrapolate to"
    ),
) -> None:
    """Extrapolate measured per-stage timings to a full-game inference runtime
    (the C-09 <1h/game runtime gate metric)."""
    from flag_football_ep.config import load_config

    load_config(config)

    import polars as pl

    tracks_df = pl.read_parquet(tracks)

    from flag_football_ep.cv.benchmark import StageTiming, extrapolate_game_runtime

    stages = tuple(
        StageTiming(stage=row["stage"], seconds=row["seconds"], frames=row["frames"])
        for row in tracks_df.iter_rows(named=True)
    )
    footage_seconds = sum(stage.frames for stage in stages) / 30.0

    result = extrapolate_game_runtime(stages, footage_seconds, game_minutes * 60.0)

    typer.echo(f"extrapolated: {result.extrapolated_game_minutes:.2f} min/game")
    typer.echo(f"formula: {result.formula}")
