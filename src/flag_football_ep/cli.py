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

    result = run_ingest(cfg, source, out, strict)

    typer.echo(f"plays:  {result.plays_path} ({result.n_plays} rows)")
    typer.echo(f"games:  {result.games_path} ({result.n_games} games, {result.n_quarantined} quarantined)")
    typer.echo(f"report: {result.report_path}")

    for notice in result.notices:
        typer.echo(f"notice: {notice}")

    if strict and result.n_quarantined > 0:
        raise typer.Exit(code=1)


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
        from flag_football_ep.reference import load_sportapp_games

        ids = load_sportapp_games(cfg.reference.sportapp_games)["source_game_id"].to_list()

    api_key = os.environ.get(cfg.sources.sportapp.api_key_env)
    if not api_key:
        raise typer.BadParameter(
            f"environment variable {cfg.sources.sportapp.api_key_env} is not set"
        )

    out_dir = cfg.paths.raw_sportapp

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

    out_dir = cfg.paths.raw_ifaf

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


@app.command(name="pat-breakeven")
def pat_breakeven(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    out: Optional[Path] = typer.Option(None, "--out", help="Output PNG path"),
    overwrite: bool = typer.Option(
        False, "--overwrite/--no-overwrite", help="Overwrite an existing PNG at --out"
    ),
) -> None:
    """Render the PAT break-even chart (REQ-S1-10/REQ-S1-14) from the canonical dataset."""
    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    plays = pl.read_parquet(cfg.paths.processed / "plays.parquet")

    from flag_football_ep.features.mutations import estimate_pat_baselines

    baselines = estimate_pat_baselines(plays)

    from flag_football_ep.charts.pat_breakeven import write_pat_breakeven

    out_path = out or (cfg.paths.processed / "pat_breakeven.png")
    written_path = write_pat_breakeven(baselines, out_path, overwrite=overwrite)

    typer.echo(f"chart: {written_path}")
    typer.echo(
        f"1-pt rate: {baselines.one_point_rate:.4f} "
        f"(n={baselines.one_point_attempts}, ci={baselines.one_point_ci})"
    )
    typer.echo(
        f"2-pt rate: {baselines.two_point_rate:.4f} "
        f"(n={baselines.two_point_attempts}, ci={baselines.two_point_ci})"
    )


@app.command()
def experiment(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    candidate: str = typer.Option("all", "--candidate", help="Candidate name or 'all'"),
    model: str = typer.Option("both", "--model", help="One of: ep, wp, both"),
) -> None:
    """Run REQ-S1-09 feature-candidate experiments and print each result's verdict."""
    if model not in {"ep", "wp", "both"}:
        raise typer.BadParameter("--model must be one of: ep, wp, both")

    from flag_football_ep.model.experiments import CANDIDATES

    valid_candidates = [*sorted(CANDIDATES), "all"]
    if candidate not in valid_candidates:
        raise typer.BadParameter(
            f"--candidate must be one of: {', '.join(valid_candidates)}"
        )

    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import polars as pl

    plays = pl.read_parquet(cfg.paths.processed / "plays.parquet")

    from flag_football_ep.model.experiments import run_candidate

    prefixes = ["ep", "wp"] if model == "both" else [model]
    candidate_names = sorted(CANDIDATES) if candidate == "all" else [candidate]

    for prefix in prefixes:
        for name in candidate_names:
            spec = CANDIDATES[name]
            if prefix not in spec.applies_to:
                continue
            result = run_candidate(plays=plays, config=cfg, model_prefix=prefix, spec=spec)
            verdict = "adopted" if result.adopted else "rejected"
            typer.echo(
                f"{name} ({prefix}): control={result.control_logloss:.6f} "
                f"candidate={result.candidate_logloss:.6f} delta={result.delta:.6f} "
                f"verdict={verdict}"
            )


@app.command()
def promote(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="Path to ffep.toml"),
    model: str = typer.Option("both", "--model", help="One of: ep, wp, both"),
    run: Optional[str] = typer.Option(
        None,
        "--run",
        help=(
            "MLflow run id to promote; defaults to the most recent FINISHED run of that "
            "model's experiment"
        ),
    ),
) -> None:
    """Promote a training run to the `champion` alias so `ffep score` resolves it."""
    if model not in {"ep", "wp", "both"}:
        raise typer.BadParameter("--model must be one of: ep, wp, both")
    if run is not None and model == "both":
        raise typer.BadParameter(
            "--run cannot be combined with --model both (one run id cannot be both models)"
        )

    from flag_football_ep.config import load_config

    cfg = load_config(config)

    import mlflow

    from flag_football_ep.model import mlflow_store, registry

    experiments = {"ep": cfg.train.ep_experiment, "wp": cfg.train.wp_experiment}
    prefixes = ["ep", "wp"] if model == "both" else [model]

    for prefix in prefixes:
        experiment = experiments[prefix]
        if run is not None:
            run_id = run
        else:
            mlflow_store.configure(cfg)
            runs = mlflow.search_runs(
                experiment_names=[experiment],
                filter_string="attributes.status = 'FINISHED'",
                order_by=["attributes.start_time DESC"],
                max_results=1,
                output_format="list",
            )
            if not runs:
                raise typer.BadParameter(
                    f"no FINISHED runs found for experiment {experiment!r} to promote"
                )
            run_id = runs[0].info.run_id

        name = registry.registered_model_name(prefix)
        version = registry.promote(name, run_id, cfg)
        typer.echo(f"{name}: promoted run {run_id} to champion (version {version})")


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
    from flag_football_ep.config import load_config, secret

    cfg = load_config(config)

    if not skip_fetch:
        from flag_football_ep.fetch.ifaf import fetch_tournament
        from flag_football_ep.fetch.sportapp import fetch_games
        from flag_football_ep.reference import load_sportapp_games

        sportapp_api_key = secret(cfg.sources.sportapp.api_key_env, required=False)
        if sportapp_api_key:
            game_ids = load_sportapp_games(cfg.reference.sportapp_games)["source_game_id"].to_list()
            fetch_games(game_ids, cfg.paths.raw_sportapp, sportapp_api_key, cfg.sources.sportapp.base_url)
        else:
            typer.echo(
                f"skipping sportapp.fi fetch: {cfg.sources.sportapp.api_key_env} is not set"
            )

        ifaf_api_key_env = getattr(cfg.sources.ifaf, "api_key_env", None)
        ifaf_api_key = secret(ifaf_api_key_env, required=False) if ifaf_api_key_env else None
        fetch_tournament(
            cfg.sources.ifaf.base_url,
            cfg.sources.ifaf.tournament,
            cfg.paths.raw_ifaf,
            api_key=ifaf_api_key,
        )

    from flag_football_ep.pipeline import run_all

    result = run_all(cfg, tune)

    typer.echo(
        f"ingest: {result.ingest.plays_path} "
        f"({result.ingest.n_plays} plays, {result.ingest.n_quarantined} quarantined)"
    )
    for notice in result.ingest.notices:
        typer.echo(f"notice: {notice}")
    typer.echo(f"ep run:  {result.ep_run}")
    typer.echo(f"wp run:  {result.wp_run}")
    typer.echo(f"scored:  {result.scored_path}")
    for stage, seconds in result.durations.items():
        typer.echo(f"  {stage}: {seconds:.2f}s")
    typer.echo(f"  total: {sum(result.durations.values()):.2f}s")


if __name__ == "__main__":
    app()
