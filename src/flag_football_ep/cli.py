"""Typer CLI for the flag-football-ep pipeline.

Every command follows the same shape: load the TOML config, lazily import the
stage module it delegates to, and call exactly one stage function. Stage
modules (`config`, `pipeline`, `fetch.sportapp`, `fetch.ifaf`, `model.train`,
`model.score`) are implemented by later plans; this module only defines the
option surface and the delegation contract so later plans never have to edit
this file (except `pipeline.run_ingest`/`run_all`, wired in plans 14 and 16).

Secrets (API keys) are read from environment variables named in the config
and are never printed or interpolated into echoed output.
"""

from pathlib import Path
from typing import List, Optional

import typer

app = typer.Typer(no_args_is_help=True, add_completion=False, help="flag-football EP/WP pipeline")

DEFAULT_CONFIG = Path("ffep.toml")
DEFAULT_SOURCES = ["hudl", "legacy", "sportapp", "ifaf"]


@app.command()
def ingest(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    source: List[str] = typer.Option(
        DEFAULT_SOURCES, "--source", help="Source(s) to ingest (repeatable)"
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Override the processed output directory"
    ),
    strict: bool = typer.Option(
        False, "--strict/--no-strict", help="Fail hard on per-game validation errors"
    ),
) -> None:
    """Ingest raw exports into the canonical Parquet dataset."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.pipeline import run_ingest

    run_ingest(cfg, source, out, strict)


@app.command(name="fetch-sportapp")
def fetch_sportapp(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    game_ids: Optional[str] = typer.Option(
        None, "--game-ids", help="Comma-separated sportapp.fi game ids"
    ),
    games_file: Optional[Path] = typer.Option(
        None, "--games-file", help="File with one sportapp.fi game id per line"
    ),
    force: bool = typer.Option(
        False, "--force/--no-force", help="Re-fetch games even if already cached on disk"
    ),
) -> None:
    """Fetch sportapp.fi play-by-play games into the raw sportapp directory."""
    import os

    from flag_football_ep.config import load_config

    cfg = load_config(config)

    if game_ids:
        ids = [g.strip() for g in game_ids.split(",") if g.strip()]
    elif games_file:
        ids = [
            line.strip()
            for line in games_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    else:
        ids = list(cfg.reference.sportapp_games)

    api_key = os.environ.get(cfg.sources.sportapp.api_key_env)
    if not api_key:
        raise typer.BadParameter(
            f"environment variable {cfg.sources.sportapp.api_key_env} is not set"
        )

    out_dir = cfg.paths.raw / "sportapp"

    from flag_football_ep.fetch.sportapp import fetch_games

    fetch_games(ids, out_dir, api_key, cfg.sources.sportapp.base_url, force)


@app.command(name="fetch-ifaf")
def fetch_ifaf(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    tournament: Optional[str] = typer.Option(
        None, "--tournament", help="Tournament identifier (defaults to config)"
    ),
    game_id: Optional[str] = typer.Option(
        None, "--game-id", help="Fetch a single IFAF game instead of a whole tournament"
    ),
    limit: int = typer.Option(500, "--limit", help="Maximum number of games to fetch"),
    force: bool = typer.Option(
        False, "--force/--no-force", help="Re-fetch games even if already cached on disk"
    ),
) -> None:
    """Fetch IFAF tournament play-by-play into the raw ifaf directory."""
    import os

    from flag_football_ep.config import load_config

    cfg = load_config(config)

    resolved_tournament = tournament or cfg.sources.ifaf.tournament
    api_key_env = getattr(cfg.sources.ifaf, "api_key_env", None)
    api_key = os.environ.get(api_key_env) if api_key_env else None

    out_dir = cfg.paths.raw / "ifaf"

    from flag_football_ep.fetch.ifaf import fetch_tournament

    fetch_tournament(
        cfg.sources.ifaf.base_url,
        resolved_tournament,
        out_dir,
        game_id,
        limit,
        api_key,
        force,
    )


@app.command()
def train(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    model: str = typer.Option("both", "--model", help="One of: ep, wp, both"),
    tune: bool = typer.Option(False, "--tune/--no-tune", help="Run hyperopt tuning"),
    max_evals: int = typer.Option(100, "--max-evals", help="Max hyperopt evaluations when tuning"),
    export_pkl: bool = typer.Option(
        False, "--export-pkl/--no-export-pkl", help="Also export a .pkl alongside the MLflow run"
    ),
) -> None:
    """Train the EP and/or WP models from the canonical Parquet dataset."""
    if model not in {"ep", "wp", "both"}:
        raise typer.BadParameter("--model must be one of: ep, wp, both")

    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    plays = pl.read_parquet(cfg.paths.processed / "plays.parquet")

    from flag_football_ep.model.train import train_ep, train_wp

    if model in {"ep", "both"}:
        train_ep(plays, cfg, tune, max_evals, export_pkl)
    if model in {"wp", "both"}:
        train_wp(plays, cfg, tune, max_evals, export_pkl)


@app.command()
def score(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    ep_run: Optional[str] = typer.Option(None, "--ep-run", help="MLflow run id for the EP model"),
    wp_run: Optional[str] = typer.Option(None, "--wp-run", help="MLflow run id for the WP model"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output Parquet path"),
) -> None:
    """Score the canonical Parquet dataset with trained EP/WP models."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    plays = pl.read_parquet(cfg.paths.processed / "plays.parquet")

    from flag_football_ep.model.score import score_plays

    scored = score_plays(plays, cfg, ep_run, wp_run)

    out_path = out or (cfg.paths.processed / "plays_scored.parquet")
    scored.write_parquet(out_path)


@app.command()
def run(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    skip_fetch: bool = typer.Option(
        True,
        "--skip-fetch/--no-skip-fetch",
        help="Skip network fetch and ingest from disk only (default: skip)",
    ),
    tune: bool = typer.Option(False, "--tune/--no-tune", help="Run hyperopt tuning"),
) -> None:
    """Run the full pipeline: ingest, train, score."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    from flag_football_ep.pipeline import run_all

    run_all(cfg, tune)


if __name__ == "__main__":
    app()
