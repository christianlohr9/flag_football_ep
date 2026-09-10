"""Worksheets for manually re-spotting IFAF women's games with a null `ballOn`.

Builds one local, gitignored, PII-carrying "helper view" per partially spotted
women's game (`data/raw/ifaf/spot_fill_worksheets/<game_id>.csv`) so the project
owner can locate each null-`ballOn` play in the broadcast video quickly (video
URL + timestamp, down/offense/passer/receiver context, the last known real
spot for orientation) while typing the re-spotted yard line into the committed,
PII-free `data/reference/ifaf_spot_fill/<game_id>.csv` fill file
(`ifaf.apply_spot_fill` reads that one, never this module's own output).

Never wired into the ingest path itself -- this is authoring tooling only, run
on demand (`ffep ifaf spot-fill-worksheets` / `scripts/ifaf_spot_fill_worksheets.py`).
Reuses `ingest.ifaf`'s own private per-game metadata/classification helpers
(`_load_games_meta`, `_load_tournaments_meta`, `_build_game_meta`,
`_load_usable_plays_records`, `flatten_plays_records`) rather than
re-implementing them, so "which games/records are in scope" always matches
what `ingest_snapshots` itself would compute -- and `ingest.ifaf_video_marks`'s
own `_video_fields` for the video URL/timestamp resolution, so the fallback
(document-level `videoUrl` + the play's own derived `videoTimeSec`) behaves
identically to the committed `ifaf_video_marks.parquet` table.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import polars as pl

from flag_football_ep.canonical import make_game_id
from flag_football_ep.ingest.ifaf import (
    IngestNotices,
    _build_game_meta,
    _load_games_meta,
    _load_teams_meta,
    _load_tournaments_meta,
    _load_usable_plays_records,
    _plays_record_sort_key,
    flatten_plays_records,
)
from flag_football_ep.ingest.ifaf_video_marks import _video_fields
from flag_football_ep.owner_csv import OWNER_CSV_WRITE_ENCODING, read_owner_csv

WORKSHEET_COLUMNS: tuple[str, ...] = (
    "game_id",
    "sequence",
    "play_id",
    "half",
    "down",
    "offense_team",
    "passer",
    "receiver",
    "result_raw",
    "prev_ballOn",
    "video_url",
    "video_time_s",
    "video_time_mmss",
    "ballOn",
    "note",
    "spot_status",
)


def _mmss(seconds: Any) -> str | None:
    """`123.4` -> `"2:03"`. `None` for a non-numeric/absent value."""
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool):
        return None
    total = int(round(seconds))
    minutes, secs = divmod(total, 60)
    return f"{minutes}:{secs:02d}"


def _fmt_sequence(seq: Any) -> str:
    """`24` and `24.0` both format as `"24"`; a genuine `.5`-suffixed
    inserted-row sequence (e.g. `907.5`) keeps its fraction. Used both for
    the worksheet's own `sequence` cell and as the merge key against an
    existing worksheet on disk, so a prior run's `"24"` and a fresh run's
    `24.0` compare equal."""
    if isinstance(seq, bool) or seq is None:
        return ""
    if isinstance(seq, float) and seq.is_integer():
        return str(int(seq))
    return str(seq)


def find_partially_spotted_women_games(raw_dir: Path) -> dict[str, dict[str, Any]]:
    """Identify every accepted-or-quarantined IFAF women's game whose
    `/plays` reviewer feed has at least one real record with a null
    `ballOn` -- "accepted-or-quarantined" meaning it actually reaches
    `ingest_snapshots`' `/plays`-primary path (excludes a game whose
    snapshot is missing/unparseable/a structured "not reviewed" signal, or
    that falls back to `unified-plays`, since the manual-fill workflow this
    module supports is scoped to the reviewer feed's own null `ballOn`
    records).

    Returns `{source_game_id: {"canonical_game_id", "null_ballon_count",
    "total_records", "with_video_url"}}`, sorted by nothing in particular
    (the caller sorts as needed) -- `with_video_url` counts how many of the
    null-`ballOn` records resolve a video URL via `_video_fields` (own
    `videoMark`, or the document-level `videoUrl` + the record's own
    derived `videoTimeSec`), so a caller can report video-mark coverage
    without downloading anything.
    """
    raw_dir = Path(raw_dir)
    games_meta = _load_games_meta(raw_dir)
    tournaments_meta = _load_tournaments_meta(raw_dir)

    result: dict[str, dict[str, Any]] = {}
    for path in sorted(raw_dir.glob("plays_*.json")):
        gid = path.stem.removeprefix("plays_")
        game_entry = games_meta.get(gid, {})
        tournament_entry = tournaments_meta.get(game_entry.get("tournamentId"), {})
        game_meta = _build_game_meta(game_entry, tournament_entry)
        if game_meta.get("gender") != "women":
            continue

        notices = IngestNotices(game_id=gid)
        records, exclude_reason = _load_usable_plays_records(raw_dir, gid, notices)
        if records is None:
            continue  # excluded entirely, or falls back to unified-plays -- out of scope

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        doc_video_url = payload.get("videoUrl") if isinstance(payload, dict) else None

        null_records = [r for r in records if isinstance(r, dict) and r.get("ballOn") is None]
        if not null_records:
            continue

        with_video_url = 0
        for r in null_records:
            video_url, _time, _source = _video_fields(r, doc_video_url)
            if video_url is not None:
                with_video_url += 1

        result[gid] = {
            "canonical_game_id": make_game_id("ifaf", gid),
            "null_ballon_count": len(null_records),
            "total_records": len(records),
            "with_video_url": with_video_url,
        }

    return result


def build_worksheet_rows(
    raw_dir: Path,
    gid: str,
    team_lookup: dict[str, str],
    player_names: dict[str, str],
) -> list[dict]:
    """Build one game's worksheet rows: every real `/plays` record with a
    null `ballOn` (`spot_status = "missing"`, `ballOn` left empty), plus the
    one real record immediately before and after each null-`ballOn` run
    (`spot_status = "real"`, `ballOn` shown for orientation) -- the
    "neighbouring rows" deliverable calls out. `prev_ballOn` is the last
    known real spot strictly before each shown row (computed over every
    record in sequence order, not just the shown ones), for orientation
    even deep inside a long null run.

    Returns `[]` for a game with no usable `/plays` snapshot -- callers are
    expected to have already filtered via `find_partially_spotted_women_games`.
    """
    raw_dir = Path(raw_dir)
    games_meta = _load_games_meta(raw_dir)
    tournaments_meta = _load_tournaments_meta(raw_dir)
    game_entry = games_meta.get(gid, {})
    tournament_entry = tournaments_meta.get(game_entry.get("tournamentId"), {})
    game_meta = _build_game_meta(game_entry, tournament_entry)

    notices = IngestNotices(game_id=gid)
    records, _exclude_reason = _load_usable_plays_records(raw_dir, gid, notices)
    if not records:
        return []

    plays_path = raw_dir / f"plays_{gid}.json"
    try:
        payload = json.loads(plays_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    doc_video_url = payload.get("videoUrl") if isinstance(payload, dict) else None

    canonical_df = flatten_plays_records(records, game_meta, gid, player_names)
    canonical_rows = canonical_df.to_dicts()

    ordered = [
        rec
        for _, rec in sorted(
            enumerate(records), key=lambda pair: _plays_record_sort_key(pair[0], pair[1])
        )
    ]
    n = len(ordered)
    is_null = [not isinstance(r, dict) or r.get("ballOn") is None for r in ordered]

    prev_real_at: list[int | None] = []
    last_real: int | None = None
    for r, null in zip(ordered, is_null):
        prev_real_at.append(last_real)
        if not null:
            last_real = r.get("ballOn")

    include_idx: set[int] = set()
    for i, null in enumerate(is_null):
        if null:
            include_idx.add(i)
            if i - 1 >= 0 and not is_null[i - 1]:
                include_idx.add(i - 1)
            if i + 1 < n and not is_null[i + 1]:
                include_idx.add(i + 1)

    canonical_id = make_game_id("ifaf", gid)
    rows_out: list[dict] = []
    for i in sorted(include_idx):
        raw_record = ordered[i]
        crow = canonical_rows[i]
        ball_on = raw_record.get("ballOn") if isinstance(raw_record, dict) else None
        offense_raw = raw_record.get("offenseTeamId") if isinstance(raw_record, dict) else None
        offense_code = team_lookup.get(offense_raw, offense_raw)
        video_url, video_time_sec, _source = _video_fields(raw_record, doc_video_url)

        rows_out.append(
            {
                "game_id": canonical_id,
                "sequence": _fmt_sequence(raw_record.get("sequence") if isinstance(raw_record, dict) else None),
                "play_id": crow.get("play_id"),
                "half": raw_record.get("half") if isinstance(raw_record, dict) else None,
                "down": raw_record.get("down") if isinstance(raw_record, dict) else None,
                "offense_team": offense_code,
                "passer": crow.get("qb"),
                "receiver": crow.get("received_by"),
                "result_raw": crow.get("result_raw"),
                "prev_ballOn": prev_real_at[i],
                "video_url": video_url,
                "video_time_s": video_time_sec,
                "video_time_mmss": _mmss(video_time_sec),
                "ballOn": "" if ball_on is None else ball_on,
                "note": "",
                "spot_status": "missing" if ball_on is None else "real",
            }
        )

    return rows_out


def _read_existing_csv(path: Path) -> dict[str, dict]:
    """Read an existing worksheet into `{sequence_key: row_dict}` (one file
    is always scoped to a single game, so `sequence` alone is a unique key).

    Tolerant of a BOM, `;` delimiter, mac_roman/cp1252 encoding and CRLF
    (`flag_football_ep.owner_csv.read_owner_csv`) -- this is the exact file
    `_write_worksheet` hands the project owner to open, type into and save
    in Excel, so a subsequent read of that same file must survive whatever
    Excel round trip it went through, same "never destroy prior work"
    contract this function already documents.
    """
    rows, _notices = read_owner_csv(path)
    return {row.get("sequence", ""): row for row in rows}


def _write_worksheet(path: Path, rows: list[dict]) -> None:
    """Write one game's worksheet, merging in any `ballOn`/`note` the owner
    already typed into the existing file on disk (matched on `sequence`) --
    idempotent, never destroys prior work. A row no longer present in the
    freshly computed set (e.g. the underlying snapshot changed) is dropped,
    same as every other IFAF ingest recompute-from-source convention.

    Written `utf-8-sig` (a leading BOM) -- this file is opened directly in
    Excel by the project owner, and a plain UTF-8 CSV with no BOM is
    routinely mis-decoded by Excel on macOS (German umlauts render as
    mojibake, e.g. "Nühse" -> "NÃ¼hse"); the BOM is Excel's own signal to
    trust UTF-8 instead of guessing.
    """
    existing = _read_existing_csv(path)

    merged: list[dict] = []
    for row in rows:
        prior = existing.get(row["sequence"])
        if prior:
            if prior.get("ballOn"):
                row = {**row, "ballOn": prior["ballOn"]}
            if prior.get("note"):
                row = {**row, "note": prior["note"]}
        merged.append(row)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=OWNER_CSV_WRITE_ENCODING, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(WORKSHEET_COLUMNS), lineterminator="\n")
        writer.writeheader()
        for row in merged:
            writer.writerow({col: row.get(col, "") for col in WORKSHEET_COLUMNS})


_FILL_COLUMNS: tuple[str, ...] = ("game_id", "sequence", "ballOn", "note")


def _read_fill_rows(path: Path) -> list[dict]:
    """Read one fill CSV's rows as plain dicts (string cells, no type
    coercion) -- `--collect`'s own merge logic below works on the raw text,
    same as `_read_existing_csv`/`_write_worksheet` above do for worksheets;
    `ifaf.load_spot_fill` (typed, `pl.DataFrame`) stays the ingest-path
    reader and is not reused here."""
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_fill_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(_FILL_COLUMNS), lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in _FILL_COLUMNS})


def _find_fill_file_for_game(fill_dir: Path, game_id: str) -> Path | None:
    """The existing fill file (any name -- filenames are free, 2026-09-08)
    that already carries at least one row for `game_id`, or `None` if no
    fill file under `fill_dir` mentions this game yet."""
    fill_dir = Path(fill_dir)
    if not fill_dir.is_dir():
        return None
    for path in sorted(fill_dir.glob("*.csv")):
        for row in _read_fill_rows(path):
            if row.get("game_id") == game_id:
                return path
    return None


def collect_worksheet_fills(worksheet_dir: Path, fill_dir: Path) -> tuple[dict[str, int], list[str]]:
    """Copy every `ballOn`/`note` value the project owner has already typed
    into a worksheet (`data/raw/ifaf/spot_fill_worksheets/<game_id>.csv`,
    rows with `spot_status != "real"` only -- a `"real"` row is orientation
    context, never a value to collect) into that game's committed fill file
    under `fill_dir`.

    Creates `<game_id>.csv` if no fill file mentions this game yet;
    otherwise appends/merges into whichever existing fill file already
    does (any name -- `_find_fill_file_for_game`), leaving that file's
    other rows (this game's own already-filled rows, or another game's
    rows it happens to also carry) untouched.

    Idempotent: re-running never overwrites a fill `ballOn` already present
    with a DIFFERENT number -- that is a notice (never silent), and the
    existing fill value wins, same "never silently overwritten" contract
    `ifaf.apply_spot_fill` already applies on the ingest side. The same
    value twice is a harmless no-op, not a notice. A worksheet cell with no
    `ballOn` yet (only a `note`, or genuinely empty) contributes its `note`
    once a `ballOn` exists for that row but is never counted as collected
    on its own.

    Returns `({game_id: collected_count}, notices)` -- `collected_count` is
    only for rows that gained a NEW `ballOn` value in the fill file this
    run (an unchanged existing value is not re-counted).
    """
    worksheet_dir = Path(worksheet_dir)
    fill_dir = Path(fill_dir)
    report: dict[str, int] = {}
    notices: list[str] = []

    if not worksheet_dir.is_dir():
        return report, notices

    for wpath in sorted(worksheet_dir.glob("*.csv")):
        with wpath.open("r", encoding="utf-8", newline="") as f:
            wrows = list(csv.DictReader(f))
        if not wrows:
            continue
        game_id = wrows[0].get("game_id") or ""
        if not game_id:
            continue

        candidates = [
            row
            for row in wrows
            if row.get("spot_status") != "real"
            and ((row.get("ballOn") or "").strip() or (row.get("note") or "").strip())
        ]
        if not candidates:
            continue

        target_path = _find_fill_file_for_game(fill_dir, game_id) or (fill_dir / f"{game_id}.csv")
        existing_rows = _read_fill_rows(target_path)
        by_key = {(r.get("game_id", ""), r.get("sequence", "")): r for r in existing_rows}

        collected = 0
        changed = False
        for wrow in candidates:
            seq = wrow.get("sequence", "")
            ball = (wrow.get("ballOn") or "").strip()
            note = (wrow.get("note") or "").strip()
            key = (game_id, seq)
            prior = by_key.get(key)

            if prior is None:
                if not ball:
                    continue  # note-only row, nothing to collect yet
                new_row = {"game_id": game_id, "sequence": seq, "ballOn": ball, "note": note}
                by_key[key] = new_row
                existing_rows.append(new_row)
                collected += 1
                changed = True
                continue

            prior_ball = (prior.get("ballOn") or "").strip()
            if ball and prior_ball and ball != prior_ball:
                notices.append(
                    f"{target_path.name}: worksheet ballOn={ball!r} for {game_id} sequence "
                    f"{seq!r} conflicts with existing fill value {prior_ball!r}, kept existing"
                )
                continue
            if not prior_ball and ball:
                prior["ballOn"] = ball
                collected += 1
                changed = True
            if note and not (prior.get("note") or "").strip():
                prior["note"] = note
                changed = True

        if changed:
            _write_fill_rows(target_path, existing_rows)
        if collected:
            report[game_id] = report.get(game_id, 0) + collected

    return report, notices


def generate_worksheets(
    raw_dir: Path,
    worksheet_dir: Path,
    team_mapping: pl.DataFrame,
) -> dict[str, dict[str, Any]]:
    """(Re)generate the per-game spot-fill worksheets for every partially
    spotted IFAF women's game under `raw_dir`. Idempotent: re-running never
    overwrites a `ballOn`/`note` cell the owner already typed into a
    worksheet on disk (`_write_worksheet`'s own merge).

    Returns the same report shape as `find_partially_spotted_women_games`,
    with `worksheet_path` (str) added per game.
    """
    raw_dir = Path(raw_dir)
    worksheet_dir = Path(worksheet_dir)
    player_names = _load_teams_meta(raw_dir)

    ifaf_map = team_mapping.filter(pl.col("source") == "ifaf")
    team_lookup = dict(
        zip(ifaf_map["source_team"].to_list(), ifaf_map["canonical_team"].to_list())
    )

    games = find_partially_spotted_women_games(raw_dir)

    report: dict[str, dict[str, Any]] = {}
    for gid, info in sorted(games.items()):
        rows = build_worksheet_rows(raw_dir, gid, team_lookup, player_names)
        worksheet_path = worksheet_dir / f"{info['canonical_game_id']}.csv"
        _write_worksheet(worksheet_path, rows)
        report[gid] = {**info, "worksheet_path": str(worksheet_path)}

    return report
