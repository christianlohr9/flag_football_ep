"""Position-error measurement against hand-labeled ground truth: the C-09 "~<=1m
position error" gate metric.

`load_gt_positions` loads the hand-labeled ground-truth CSV (`config.reference.gt_positions`,
following `reference._read_reference_csv`'s typed-schema-loader convention), validating on
load: `field_zone` vocabulary, `class_name` vocabulary (only for already-marked rows --
a freshly seeded row has no `class_name` yet), pixel coordinates inside the clip's declared
frame size (from `data/reference/video_inventory.csv`), `hover_position_id` membership in
`data/reference/hover_positions.csv`, `gt_id` uniqueness, and `scale_pair_id` well-formedness
(exactly two rows per id, matching non-null `scale_true_yards`). Every violation raises
`GtValidationError` naming the offending `gt_id` (T-2.1-02 -- a hand-edited CSV is untrusted
input).

`prepare_gt_frames` selects a spread of tracked frames -- across clips, across the five
`field_zone` values (from the tracked `x_yards` distribution), across both `team_id` values,
favouring frames with the most simultaneously tracked players -- exports each as a
grid-annotated JPEG plus a local HTML picker page (the same pattern
`data/labels/calibration/picker.html` established in plan 02.1-13 for calibration: no
5-second-bounded `cv2` window, a browser page the operator clicks in instead), and seeds
`GT_COLUMNS`-shaped rows in the GT CSV for the operator to fill in. Never overwrites a row
that already carries coordinates (T-2.1-25 -- a re-run must not destroy prior labelling work).

`measure_position_error` transforms every coordinate-carrying GT point to field yards through
the SAME per-hover-position homography the pipeline uses (`homography.transformer_for`),
greedily matches each to the nearest pipeline track foot point in the same
`(clip_number, frame_index)` within a 3-yard radius, and reports the resulting error
distribution (median/p90/max in yards), the match rate (unmatched points are counted, never
silently dropped -- T-2.1-37), a per-`field_zone` breakdown, and the known-distance scale-pair
check (measured vs. `scale_true_yards`, signed). Raises `InsufficientGroundTruth` below 50
coordinate-carrying points -- a distribution from a handful of points is not a measurement.
Reported as measured error distributions, never a bare pass/fail number, matching the
"Richtwert, kein Messprotokoll" statistical-honesty framing `docs/capture-protocol.md`
already establishes for this project's gate documentation.

Implemented by plan 02.1-15.
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from flag_football_ep.cv import CvError
from flag_football_ep.reference import MissingReferenceFile

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class GtValidationError(CvError, ValueError):
    """Raised when `data/reference/gt_positions.csv` fails a validation rule --
    vocabulary, pixel bounds, `hover_position_id` membership, `gt_id` uniqueness, or
    scale-pair well-formedness -- naming the offending `gt_id`(s) (T-2.1-02).
    """


class InsufficientGroundTruth(CvError, ValueError):
    """Raised when fewer than `_MIN_GT_POINTS` GT points carry coordinates -- a
    position-error distribution from a handful of points is not a measurement.
    """


# The five field-zone names the GT CSV's `field_zone` column and `prepare_gt_frames`'s
# stratified selection both use -- west/east endzones plus a west-half/midfield/east-half
# split of the 50-yard field-of-play into thirds (D-13 axis convention: x=0 west goal
# line, x=field_length_yards east goal line).
FIELD_ZONES: tuple[str, ...] = (
    "west-endzone",
    "west-half",
    "midfield",
    "east-half",
    "east-endzone",
)

# The pilot detects/tracks no ball (C-12); GT class_name follows the same two-class
# vocabulary cv.schema.CLASS_VOCABULARY already enforces on tracking output.
_CLASS_VOCABULARY: tuple[str, ...] = ("player", "referee")

GT_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "frame_index": pl.Int32,
    "gt_id": pl.Utf8,
    "class_name": pl.Utf8,
    "team_hint": pl.Utf8,
    "foot_x_px": pl.Float64,
    "foot_y_px": pl.Float64,
    "hover_position_id": pl.Utf8,
    "field_zone": pl.Utf8,
    "scale_pair_id": pl.Utf8,
    "scale_true_yards": pl.Float64,
    "notes": pl.Utf8,
}

GT_COLUMNS: tuple[str, ...] = tuple(GT_SCHEMA)

# A transformed GT point matches a pipeline track foot point only within this many
# yards -- beyond this, a "match" would be pairing unrelated points, not measuring
# real pipeline error (this plan's <action> block).
_MATCH_RADIUS_YARDS = 3.0

# Below this many coordinate-carrying GT points, a position-error distribution is not
# a measurement (this plan's <action> block).
_MIN_GT_POINTS = 50

_INVENTORY_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "resolution": pl.Utf8,
    "local_path": pl.Utf8,
}

_HOVER_POSITIONS_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int64,
    "hover_position_id": pl.Utf8,
}


def _parse_resolution(resolution: str) -> tuple[int, int]:
    """Parse `video_inventory.csv`'s `"1920x1080"`-style resolution string."""
    width_str, _, height_str = resolution.lower().partition("x")
    return int(width_str), int(height_str)


def _clip_frame_sizes(config: Config) -> dict[int, tuple[int, int]]:
    """Read `(width, height)` per `clip_number` for the pilot session's drone clips
    from `data/reference/video_inventory.csv` -- the source `load_gt_positions`'s
    pixel-bounds check validates against.
    """
    from flag_football_ep.cv.frames import clip_number as clip_number_of

    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        return {}

    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter(
        (pl.col("domain") == "drone") & (pl.col("session_id") == config.cv.pilot_session_id)
    )

    sizes: dict[int, tuple[int, int]] = {}
    for row in rows.iter_rows(named=True):
        local_path = row["local_path"]
        resolution = row["resolution"]
        if not local_path or not resolution:
            continue
        try:
            n = clip_number_of(Path(local_path))
        except Exception:  # noqa: BLE001 - an unparsable filename just isn't indexed
            continue
        sizes[n] = _parse_resolution(resolution)

    return sizes


def _valid_hover_position_ids(config: Config) -> set[str]:
    """The set of `hover_position_id` values registered in
    `data/reference/hover_positions.csv` -- the membership check `load_gt_positions`
    runs against every GT row's `hover_position_id`.
    """
    path = config.reference.hover_positions
    if not path.exists():
        return set()

    df = pl.read_csv(path, schema_overrides=_HOVER_POSITIONS_SCHEMA, columns=["hover_position_id"])
    return set(df["hover_position_id"].drop_nulls().unique().to_list())


def load_gt_positions(path: Path) -> pl.DataFrame:
    """Load the hand-labeled ground-truth position CSV at `path`."""
    path = Path(path)
    if not path.exists():
        raise MissingReferenceFile(f"reference file not found: {path}")

    df = pl.read_csv(path, schema_overrides=GT_SCHEMA)

    if df.height == 0:
        warnings.warn(
            f"{path} is header-only; loading as an empty typed frame",
            stacklevel=2,
        )
        return df

    dupes = (
        df.group_by("gt_id")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)["gt_id"]
        .to_list()
    )
    if dupes:
        raise GtValidationError(f"duplicate gt_id(s) in {path}: {sorted(dupes)}")

    bad_zone = df.filter(~pl.col("field_zone").is_in(list(FIELD_ZONES)))
    if bad_zone.height:
        raise GtValidationError(
            f"field_zone outside {FIELD_ZONES} for gt_id(s) "
            f"{bad_zone['gt_id'].to_list()} in {path}"
        )

    labelled_class = df.filter(pl.col("class_name").is_not_null())
    bad_class = labelled_class.filter(~pl.col("class_name").is_in(list(_CLASS_VOCABULARY)))
    if bad_class.height:
        raise GtValidationError(
            f"class_name outside {_CLASS_VOCABULARY} for gt_id(s) "
            f"{bad_class['gt_id'].to_list()} in {path}"
        )

    from flag_football_ep.config import load_config

    cfg = load_config()
    frame_sizes = _clip_frame_sizes(cfg)

    labelled_xy = df.filter(pl.col("foot_x_px").is_not_null() & pl.col("foot_y_px").is_not_null())
    for row in labelled_xy.iter_rows(named=True):
        size = frame_sizes.get(row["clip_number"])
        if size is None:
            continue
        width, height = size
        if not (0 <= row["foot_x_px"] <= width and 0 <= row["foot_y_px"] <= height):
            raise GtValidationError(
                f"gt_id {row['gt_id']!r}: pixel coordinates "
                f"({row['foot_x_px']}, {row['foot_y_px']}) outside frame bounds "
                f"{width}x{height} in {path}"
            )

    valid_hover_ids = _valid_hover_position_ids(cfg)
    if valid_hover_ids:
        bad_hover = df.filter(~pl.col("hover_position_id").is_in(sorted(valid_hover_ids)))
        if bad_hover.height:
            raise GtValidationError(
                f"hover_position_id not in {sorted(valid_hover_ids)} for gt_id(s) "
                f"{bad_hover['gt_id'].to_list()} in {path}"
            )

    pair_rows = df.filter(pl.col("scale_pair_id").is_not_null())
    if pair_rows.height:
        counts = pair_rows.group_by("scale_pair_id").agg(
            pl.len().alias("n"),
            pl.col("scale_true_yards").n_unique().alias("n_true"),
            pl.col("scale_true_yards").null_count().alias("n_null_true"),
        )

        bad_count = counts.filter(pl.col("n") != 2)
        if bad_count.height:
            raise GtValidationError(
                f"scale_pair_id(s) not on exactly two rows in {path}: "
                f"{bad_count['scale_pair_id'].to_list()}"
            )

        bad_true = counts.filter((pl.col("n_null_true") > 0) | (pl.col("n_true") > 1))
        if bad_true.height:
            raise GtValidationError(
                f"scale_pair_id(s) with missing/mismatched scale_true_yards in {path}: "
                f"{bad_true['scale_pair_id'].to_list()}"
            )

    return df


def _field_zone_expr(config: Config) -> pl.Expr:
    """The chained `pl.when/then/otherwise` field-zone bucket expression over
    `x_yards`, mirroring `reports.aggregate._field_zone_expr`'s idiom -- one source of
    truth built from a tuple constant, never a per-row Python loop.
    """
    length = config.cv.field_length_yards
    third = length / 3.0
    return (
        pl.when(pl.col("x_yards") < 0)
        .then(pl.lit(FIELD_ZONES[0]))  # west-endzone
        .when(pl.col("x_yards") > length)
        .then(pl.lit(FIELD_ZONES[4]))  # east-endzone
        .when(pl.col("x_yards") < third)
        .then(pl.lit(FIELD_ZONES[1]))  # west-half
        .when(pl.col("x_yards") < 2 * third)
        .then(pl.lit(FIELD_ZONES[2]))  # midfield
        .otherwise(pl.lit(FIELD_ZONES[3]))  # east-half
    )


def _select_gt_frames(tracks: pl.DataFrame, config: Config, n_frames: int) -> list[dict]:
    """Select up to `n_frames` (clip_number, frame_index) frames for GT labelling.

    Stratified across the five `FIELD_ZONES` (quota split as evenly as `n_frames`
    allows), preferring -- within each zone's quota -- frames from clips not yet used
    in that zone (spread across clips), frames where both `team_id` values are
    present, and frames with the most players simultaneously tracked in that zone.
    Falls back to any remaining tracked frame (ranked the same way) when a zone has
    fewer candidates than its quota, so the full `n_frames` budget is still used.
    """
    located = tracks.filter(pl.col("x_yards").is_not_null() & pl.col("frame_index").is_not_null())
    if located.height == 0 or n_frames <= 0:
        return []

    zoned = located.with_columns(_field_zone_expr(config).alias("field_zone"))

    frame_stats = (
        zoned.group_by(["clip_number", "frame_index"])
        .agg(
            pl.col("track_id").n_unique().alias("n_tracks"),
            pl.col("team_id").drop_nulls().n_unique().alias("n_teams"),
            pl.col("hover_position_id").first().alias("hover_position_id"),
            pl.col("timestamp_s").first().alias("timestamp_s"),
        )
        .with_columns((pl.col("n_teams") >= 2).alias("has_both_teams"))
        .drop("n_teams")
    )

    zone_presence = zoned.group_by(["clip_number", "frame_index", "field_zone"]).agg(
        pl.len().alias("n_in_zone")
    )

    quota_base, remainder = divmod(n_frames, len(FIELD_ZONES))
    quotas = {
        zone: quota_base + (1 if i < remainder else 0) for i, zone in enumerate(FIELD_ZONES)
    }

    selected: dict[tuple[int, int], dict] = {}
    used_clips_by_zone: dict[str, set[int]] = {zone: set() for zone in FIELD_ZONES}

    for zone in FIELD_ZONES:
        quota = quotas[zone]
        if quota <= 0:
            continue

        candidates = (
            zone_presence.filter(pl.col("field_zone") == zone)
            .join(frame_stats, on=["clip_number", "frame_index"], how="inner")
            .sort(
                ["has_both_teams", "n_in_zone", "n_tracks", "clip_number", "frame_index"],
                descending=[True, True, True, False, False],
            )
        )
        rows = [
            row
            for row in candidates.iter_rows(named=True)
            if (row["clip_number"], row["frame_index"]) not in selected
        ]

        chosen = 0
        for row in rows:
            if chosen >= quota:
                break
            if row["clip_number"] in used_clips_by_zone[zone]:
                continue
            key = (row["clip_number"], row["frame_index"])
            selected[key] = {**row, "field_zone": zone}
            used_clips_by_zone[zone].add(row["clip_number"])
            chosen += 1

        if chosen < quota:
            for row in rows:
                if chosen >= quota:
                    break
                key = (row["clip_number"], row["frame_index"])
                if key in selected:
                    continue
                selected[key] = {**row, "field_zone": zone}
                chosen += 1

    if len(selected) < n_frames:
        dominant_zone = (
            zone_presence.sort(
                ["clip_number", "frame_index", "n_in_zone"], descending=[False, False, True]
            )
            .unique(subset=["clip_number", "frame_index"], keep="first")
        )
        dominant_by_key = {
            (row["clip_number"], row["frame_index"]): row["field_zone"]
            for row in dominant_zone.iter_rows(named=True)
        }

        backfill_candidates = frame_stats.sort(
            ["has_both_teams", "n_tracks", "clip_number", "frame_index"],
            descending=[True, True, False, False],
        )
        for row in backfill_candidates.iter_rows(named=True):
            if len(selected) >= n_frames:
                break
            key = (row["clip_number"], row["frame_index"])
            if key in selected:
                continue
            selected[key] = {**row, "field_zone": dominant_by_key.get(key, FIELD_ZONES[0])}

    ordered = sorted(selected.values(), key=lambda row: (row["clip_number"], row["frame_index"]))
    return ordered[:n_frames]


def _draw_gt_grid(frame, clip_number: int, frame_index: int):
    """Annotate `frame` with a 100px pixel grid (axis-labelled) plus a clip/frame
    caption, mirroring `homography._draw_reference_grid`'s reference-frame convention
    so an operator can hand-read pixel coordinates the same way, if the HTML picker
    page is unavailable.
    """
    import cv2

    annotated = frame.copy()
    height, width = annotated.shape[:2]
    color = (0, 255, 0)

    for x in range(0, width, 100):
        cv2.line(annotated, (x, 0), (x, height), color, 1)
        cv2.putText(annotated, str(x), (x + 2, 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)
    for y in range(0, height, 100):
        cv2.line(annotated, (0, y), (width, y), color, 1)
        cv2.putText(annotated, str(y), (2, y + 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)

    cv2.putText(
        annotated,
        f"clip {clip_number} frame {frame_index}",
        (10, height - 10),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.6,
        (0, 200, 255),
        2,
    )
    return annotated


def _export_annotated_frame(
    clip: Path, timestamp_s: float, clip_number: int, frame_index: int, out_dir: Path
) -> Path:
    """Extract the frame at `timestamp_s` from `clip` (via `frames.extract_frames`,
    ffmpeg -- no cv2 needed for extraction) and grid-annotate it in place.
    """
    from flag_football_ep.cv.frames import extract_frames

    extracted = extract_frames(clip, out_dir, [timestamp_s])[0]
    final_path = out_dir / f"c{clip_number:03d}_f{frame_index:05d}.jpg"

    import cv2

    frame = cv2.imread(str(extracted))
    if frame is not None:
        annotated = _draw_gt_grid(frame, clip_number, frame_index)
        cv2.imwrite(str(final_path), annotated)
        if extracted != final_path:
            extracted.unlink(missing_ok=True)
    elif extracted != final_path:
        extracted.replace(final_path)

    return final_path


def _write_picker_html(out_dir: Path, written_frames: list[dict]) -> Path:
    """Write the local HTML foot-point picker page (`out_dir/picker.html`), the same
    pattern `data/labels/calibration/picker.html` established in plan 02.1-13: the
    production interactive `cv2` picker's ~5s wait bound (T-2.1-12) is impractical for
    a human labelling hundreds of points, so a browser page the operator clicks in is
    strongly preferred over hand-reading pixel coordinates off the grid-annotated JPEGs.
    Every click adds a new, freely-placed foot point (unlike the calibration picker's
    fixed landmark list); the operator sets each point's class/team/scale-pair inline
    and copies out CSV rows matching `GT_COLUMNS` exactly.
    """
    import json

    frames_json = json.dumps(
        [
            {
                "clip": f["clip_number"],
                "frame": f["frame_index"],
                "img": f["image_path"],
                "zone": f["field_zone"],
                "hover": f["hover_position_id"],
            }
            for f in written_frames
        ]
    )

    html = _PICKER_HTML_TEMPLATE.replace("__FRAMES_JSON__", frames_json)
    html_path = out_dir / "picker.html"
    html_path.write_text(html, encoding="utf-8")
    return html_path


_PICKER_HTML_TEMPLATE = """<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<title>Ground-Truth Fusspositionen</title>
<style>
  body { font-family: -apple-system, sans-serif; margin: 16px; background: #f5f5f7; color: #1d1d1f; }
  h1 { font-size: 1.3em; } h2 { font-size: 1.05em; margin-top: 24px; }
  .wrap { display: flex; gap: 16px; align-items: flex-start; }
  .imgbox { position: relative; flex: 1; min-width: 0; }
  .imgbox img { width: 100%; display: block; border-radius: 8px; cursor: crosshair; }
  .dot { position: absolute; width: 12px; height: 12px; margin: -6px 0 0 -6px; border-radius: 50%;
         background: #ff3b30; border: 2px solid white; box-shadow: 0 0 4px rgba(0,0,0,.6); }
  .dot.referee { background: #af52de; }
  .dot span { position: absolute; left: 12px; top: -4px; font-size: 10px; color: white;
              background: rgba(0,0,0,.65); padding: 1px 4px; border-radius: 4px; white-space: nowrap; }
  .panel { width: 320px; flex-shrink: 0; background: white; border-radius: 8px; padding: 10px; max-height: 80vh; overflow-y: auto; }
  .pt { border-bottom: 1px solid #eee; padding: 6px 2px; font-size: 12px; }
  .pt label { display: inline-block; width: 62px; }
  .pt select, .pt input[type=text], .pt input[type=number] { font-size: 12px; padding: 1px 3px; }
  .pt input[type=checkbox] { vertical-align: middle; }
  button { margin: 4px 4px 4px 0; padding: 5px 10px; border-radius: 6px; border: 1px solid #ccc; background: white; cursor: pointer; }
  button:hover { background: #eee; }
  textarea { width: 100%; height: 160px; font-family: monospace; font-size: 11px; margin-top: 8px; }
  .status { font-size: 13px; margin: 6px 0; }
  .ok { color: #34c759; } .warn { color: #ff9500; }
  .hint { font-size: 12px; color: #666; }
  .meta { font-size: 12px; color: #666; margin: 2px 0 8px; }
</style>
</head>
<body>
<h1>Ground-Truth Fusspositionen klicken</h1>
<p class="hint">Pro Bild: irgendwo auf ein Bild klicken setzt einen neuen Punkt genau dort (Fusspunkt =
wo die Person den Boden beruehrt, kein Koerperschwerpunkt, kein Schatten). Rechts daneben Klasse
(player/referee), optional Team-Hinweis und -- fuer mind. 5 Punktpaare insgesamt -- "Massstabs-Paar"
ankreuzen mit gemeinsamer Paar-ID und der wahren Distanz in Yards (von der Feldmarkierung abgelesen).
Punkt loeschen ueber das X. Unten "CSV kopieren" fuer alle Bilder zusammen.</p>

<div id="sections"></div>

<h2>Ergebnis (CSV-Zeilen zum Kopieren)</h2>
<div class="status" id="summary"></div>
<button onclick="copyCsv()">CSV kopieren</button>
<textarea id="csv" readonly></textarea>

<script>
const FRAMES = __FRAMES_JSON__;
const state = {};   // state[frameKey] = [{x,y,class,team,scalePairId,scaleTrueYards}, ...]

function frameKey(f) { return f.clip + ":" + f.frame; }

const sections = document.getElementById("sections");
for (const f of FRAMES) {
  const key = frameKey(f);
  state[key] = [];
  const h = document.createElement("h2");
  h.textContent = `Clip ${f.clip}, Frame ${f.frame}`;
  sections.appendChild(h);
  const meta = document.createElement("div");
  meta.className = "meta";
  meta.textContent = `Zone: ${f.zone} -- Hover-Position: ${f.hover || "?"}`;
  sections.appendChild(meta);

  const wrap = document.createElement("div");
  wrap.className = "wrap";
  const box = document.createElement("div");
  box.className = "imgbox";
  box.id = "box-" + key;
  const img = document.createElement("img");
  img.src = f.img;
  img.id = "img-" + key;
  img.addEventListener("click", (e) => onImgClick(f, img, e));
  box.appendChild(img);
  const panel = document.createElement("div");
  panel.className = "panel";
  panel.id = "panel-" + key;
  wrap.appendChild(box);
  wrap.appendChild(panel);
  sections.appendChild(wrap);
}

function onImgClick(f, img, e) {
  const key = frameKey(f);
  const r = img.getBoundingClientRect();
  const x = (e.clientX - r.left) * (img.naturalWidth / r.width);
  const y = (e.clientY - r.top) * (img.naturalHeight / r.height);
  state[key].push({
    x: Math.round(x * 10) / 10,
    y: Math.round(y * 10) / 10,
    cls: "player",
    team: "",
    scalePairId: "",
    scaleTrueYards: "",
  });
  render();
}

function removePoint(key, idx) {
  state[key].splice(idx, 1);
  render();
}

function updatePoint(key, idx, field, value) {
  state[key][idx][field] = value;
  render();
}

function render() {
  for (const f of FRAMES) {
    const key = frameKey(f);
    const box = document.getElementById("box-" + key);
    const img = document.getElementById("img-" + key);
    box.querySelectorAll(".dot").forEach(d => d.remove());
    const panel = document.getElementById("panel-" + key);
    panel.innerHTML = "";

    state[key].forEach((pt, idx) => {
      const d = document.createElement("div");
      d.className = "dot" + (pt.cls === "referee" ? " referee" : "");
      d.style.left = (pt.x / img.naturalWidth * 100) + "%";
      d.style.top = (pt.y / img.naturalHeight * 100) + "%";
      const s = document.createElement("span");
      s.textContent = "p" + (idx + 1);
      d.appendChild(s);
      box.appendChild(d);

      const row = document.createElement("div");
      row.className = "pt";
      row.innerHTML = `
        <b>p${idx + 1}</b> (${pt.x}, ${pt.y})
        <button onclick="removePoint('${key}',${idx})" style="float:right;padding:0 6px;">x</button><br>
        <label>Klasse</label>
        <select onchange="updatePoint('${key}',${idx},'cls',this.value)">
          <option value="player" ${pt.cls === "player" ? "selected" : ""}>player</option>
          <option value="referee" ${pt.cls === "referee" ? "selected" : ""}>referee</option>
        </select><br>
        <label>Team</label>
        <input type="text" value="${pt.team}" onchange="updatePoint('${key}',${idx},'team',this.value)"
          placeholder="optional"><br>
        <label>Paar-ID</label>
        <input type="text" value="${pt.scalePairId}"
          onchange="updatePoint('${key}',${idx},'scalePairId',this.value)" placeholder="z.B. sp-1"
          style="width:70px;">
        <label style="width:auto;margin-left:4px;">Yards</label>
        <input type="number" value="${pt.scaleTrueYards}" step="0.1"
          onchange="updatePoint('${key}',${idx},'scaleTrueYards',this.value)" style="width:50px;">
      `;
      panel.appendChild(row);
    });
  }

  const lines = [
    "clip_number,frame_index,gt_id,class_name,team_hint,foot_x_px,foot_y_px," +
    "hover_position_id,field_zone,scale_pair_id,scale_true_yards,notes",
  ];
  let total = 0;
  const zones = new Set();
  const pairIds = new Set();
  for (const f of FRAMES) {
    const key = frameKey(f);
    state[key].forEach((pt, idx) => {
      const gtId = `c${f.clip}f${f.frame}p${idx + 1}`;
      const team = pt.team ? pt.team.replace(/,/g, ";") : "";
      const pairId = pt.scalePairId || "";
      const trueYards = pt.scaleTrueYards || "";
      lines.push(
        `${f.clip},${f.frame},${gtId},${pt.cls},${team},${pt.x},${pt.y},` +
        `${f.hover || ""},${f.zone},${pairId},${trueYards},`
      );
      total++;
      zones.add(f.zone);
      if (pairId) pairIds.add(pairId);
    });
  }
  document.getElementById("csv").value = lines.join("\\n");
  const ok = total >= 200 && zones.size === 5 && pairIds.size >= 5;
  document.getElementById("summary").innerHTML =
    `<span class="${ok ? 'ok' : 'warn'}">${total} Punkte, ${zones.size} von 5 Zonen, ` +
    `${pairIds.size} Massstabs-Paare (min. 200 / 5 / 5) ${ok ? '\\u2713' : '\\u2014 noch nicht genug'}</span>`;
}

function copyCsv() {
  const ta = document.getElementById("csv");
  ta.select();
  document.execCommand("copy");
}
render();
</script>
</body>
</html>
"""


def _seed_gt_rows(path: Path, written_frames: list[dict]) -> None:
    """Seed one `GT_COLUMNS`-shaped row per newly-exported frame in the GT CSV
    (`gt_id = c{clip}f{frame}p1`, coordinates/class left null for the operator to
    fill). Never touches a `(clip_number, frame_index)` group that already carries at
    least one coordinate -- re-running `--prepare` must not destroy prior labelling.
    """
    path = Path(path)
    existing = (
        pl.read_csv(path, schema_overrides=GT_SCHEMA) if path.exists() else pl.DataFrame(schema=GT_SCHEMA)
    )

    locked_keys: set[tuple[int, int]] = set()
    if existing.height:
        locked = existing.filter(pl.col("foot_x_px").is_not_null())
        locked_keys = set(
            zip(locked["clip_number"].to_list(), locked["frame_index"].to_list())
        )

    keep = existing
    if existing.height:
        reseed_keys = {
            (f["clip_number"], f["frame_index"]) for f in written_frames
        } - locked_keys
        if reseed_keys:
            reseed_df = pl.DataFrame(
                {
                    "clip_number": [k[0] for k in reseed_keys],
                    "frame_index": [k[1] for k in reseed_keys],
                },
                schema={"clip_number": pl.Int32, "frame_index": pl.Int32},
            )
            keep = existing.join(reseed_df, on=["clip_number", "frame_index"], how="anti")

    new_rows = [
        {
            "clip_number": f["clip_number"],
            "frame_index": f["frame_index"],
            "gt_id": f"c{f['clip_number']}f{f['frame_index']}p1",
            "class_name": None,
            "team_hint": None,
            "foot_x_px": None,
            "foot_y_px": None,
            "hover_position_id": f["hover_position_id"],
            "field_zone": f["field_zone"],
            "scale_pair_id": None,
            "scale_true_yards": None,
            "notes": None,
        }
        for f in written_frames
        if (f["clip_number"], f["frame_index"]) not in locked_keys
    ]
    new_df = pl.DataFrame(new_rows, schema=GT_SCHEMA) if new_rows else pl.DataFrame(schema=GT_SCHEMA)

    combined = pl.concat([keep, new_df], how="vertical").sort(
        ["clip_number", "frame_index", "gt_id"]
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        combined.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def prepare_gt_frames(config: Config, tracks: pl.DataFrame, *, n_frames: int, out_dir: Path) -> Path:
    """Export `n_frames` tracked frames for hand ground-truth labeling, seeding
    `GT_COLUMNS`-shaped rows under `out_dir`.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    selected = _select_gt_frames(tracks, config, n_frames)

    from flag_football_ep.cv.frames import clip_number as clip_number_of
    from flag_football_ep.cv.frames import clip_paths

    session_id = config.cv.pilot_session_id
    clips_by_number = {clip_number_of(p): p for p in clip_paths(config, session_id)}

    written_frames: list[dict] = []
    for row in selected:
        clip_num = int(row["clip_number"])
        frame_idx = int(row["frame_index"])
        clip_path = clips_by_number.get(clip_num)
        if clip_path is None:
            continue
        image_path = _export_annotated_frame(
            clip_path, float(row["timestamp_s"]), clip_num, frame_idx, out_dir
        )
        written_frames.append(
            {
                "clip_number": clip_num,
                "frame_index": frame_idx,
                "hover_position_id": row.get("hover_position_id"),
                "field_zone": row["field_zone"],
                "image_path": image_path.name,
            }
        )

    _write_picker_html(out_dir, written_frames)
    _seed_gt_rows(config.reference.gt_positions, written_frames)

    return out_dir


@dataclass(frozen=True)
class AccuracyResult:
    """The measured position-error distribution against ground truth: point count,
    median/p90/max error in yards, the match rate, and a per-zone breakdown.

    `n_points` is every coordinate-carrying GT point considered (matched + unmatched);
    `median_yards`/`p90_yards`/`max_yards` are computed only over matched points
    (T-2.1-37 -- an unmatched point still counts toward `match_rate`, but including it
    in the distance distribution would be measuring nothing). `per_zone` maps each
    `FIELD_ZONES` name present in the GT set to its own `{n_points, median_yards,
    p90_yards, max_yards, match_rate}` dict. `scale_pairs` lists one dict per
    `scale_pair_id` (`scale_pair_id`, `measured_yards`, `true_yards`,
    `signed_error_yards`) -- the independent known-distance check.
    """

    n_points: int
    median_yards: float
    p90_yards: float
    max_yards: float
    per_zone: dict = field(default_factory=dict)
    match_rate: float = 0.0
    n_unmatched: int = 0
    scale_pairs: list = field(default_factory=list)


def _transform_gt_to_yards(gt_labelled: pl.DataFrame, config: Config) -> pl.DataFrame:
    """Transform every labelled GT row's `(foot_x_px, foot_y_px)` to
    `(gt_x_yards, gt_y_yards)` through its own hover position's calibration -- the
    SAME calibration `add_field_coordinates` projects the pipeline's tracks through,
    so the resulting error isolates pipeline foot-point error from homography error.
    """
    from flag_football_ep.cv.homography import load_calibration, transformer_for

    calibration = load_calibration(config.reference.homography_calibration)

    groups: list[pl.DataFrame] = []
    for hover_position_id in gt_labelled["hover_position_id"].unique(maintain_order=True).to_list():
        transformer = transformer_for(hover_position_id, calibration)
        group = gt_labelled.filter(pl.col("hover_position_id") == hover_position_id)
        source = group.select("foot_x_px", "foot_y_px").to_numpy()
        projected = transformer.transform_points(source)
        groups.append(
            group.with_columns(
                pl.Series("gt_x_yards", projected[:, 0]).cast(pl.Float64),
                pl.Series("gt_y_yards", projected[:, 1]).cast(pl.Float64),
            )
        )

    return pl.concat(groups, how="vertical")


def _match_gt_to_tracks(gt_yards: pl.DataFrame, tracks: pl.DataFrame) -> list[dict]:
    """Greedily match every GT point in `gt_yards` to the nearest pipeline track foot
    point in the same `(clip_number, frame_index)` within `_MATCH_RADIUS_YARDS`,
    without reusing a track for two different GT points. Returns one dict per GT row:
    `{gt_id, field_zone, matched, distance_yards}` -- unmatched rows carry
    `distance_yards=None` and are still present (T-2.1-37, never silently dropped).
    """
    tracks_located = tracks.filter(
        pl.col("x_yards").is_not_null() & pl.col("y_yards").is_not_null()
    )

    results: list[dict] = []
    groups = gt_yards.partition_by(["clip_number", "frame_index"], as_dict=True)
    for key, group in groups.items():
        clip_number, frame_index = key
        candidates = tracks_located.filter(
            (pl.col("clip_number") == clip_number) & (pl.col("frame_index") == frame_index)
        )
        cand_xy = candidates.select("x_yards", "y_yards").to_numpy()

        gt_rows = group.to_dicts()
        pairs: list[tuple[float, int, int]] = []
        for gi, row in enumerate(gt_rows):
            gx, gy = row["gt_x_yards"], row["gt_y_yards"]
            for ci in range(len(cand_xy)):
                tx, ty = cand_xy[ci]
                distance = ((gx - tx) ** 2 + (gy - ty) ** 2) ** 0.5
                if distance <= _MATCH_RADIUS_YARDS:
                    pairs.append((distance, gi, ci))
        pairs.sort(key=lambda p: p[0])

        used_gt: set[int] = set()
        used_cand: set[int] = set()
        matched_distance: dict[int, float] = {}
        for distance, gi, ci in pairs:
            if gi in used_gt or ci in used_cand:
                continue
            used_gt.add(gi)
            used_cand.add(ci)
            matched_distance[gi] = distance

        for gi, row in enumerate(gt_rows):
            results.append(
                {
                    "gt_id": row["gt_id"],
                    "field_zone": row["field_zone"],
                    "matched": gi in matched_distance,
                    "distance_yards": matched_distance.get(gi),
                }
            )

    return results


def _distribution_stats(distances: list[float]) -> tuple[float, float, float]:
    """`(median, p90, max)` of `distances`, computed with `numpy` (linear
    interpolation for `p90`, matching `numpy.percentile`'s default).
    """
    import numpy as np

    arr = np.asarray(distances, dtype=np.float64)
    return float(np.median(arr)), float(np.percentile(arr, 90)), float(np.max(arr))


def measure_position_error(gt: pl.DataFrame, tracks: pl.DataFrame, config: Config) -> AccuracyResult:
    """Join `gt` against `tracks`' field-yard coordinates and measure the position
    error distribution.
    """
    labelled = gt.filter(pl.col("foot_x_px").is_not_null() & pl.col("foot_y_px").is_not_null())
    if labelled.height < _MIN_GT_POINTS:
        raise InsufficientGroundTruth(
            f"only {labelled.height} GT point(s) carry coordinates, need at least "
            f"{_MIN_GT_POINTS} -- a position-error distribution from a handful of "
            "points is not a measurement"
        )

    gt_yards = _transform_gt_to_yards(labelled, config)
    matches = _match_gt_to_tracks(gt_yards, tracks)

    matched_distances = [m["distance_yards"] for m in matches if m["matched"]]
    n_unmatched = sum(1 for m in matches if not m["matched"])
    match_rate = (len(matches) - n_unmatched) / len(matches) if matches else 0.0

    if matched_distances:
        median_yards, p90_yards, max_yards = _distribution_stats(matched_distances)
    else:
        median_yards = p90_yards = max_yards = float("nan")

    per_zone: dict[str, dict] = {}
    for zone in FIELD_ZONES:
        zone_matches = [m for m in matches if m["field_zone"] == zone]
        if not zone_matches:
            continue
        zone_distances = [m["distance_yards"] for m in zone_matches if m["matched"]]
        zone_unmatched = sum(1 for m in zone_matches if not m["matched"])
        zone_match_rate = (
            (len(zone_matches) - zone_unmatched) / len(zone_matches) if zone_matches else 0.0
        )
        if zone_distances:
            z_median, z_p90, z_max = _distribution_stats(zone_distances)
        else:
            z_median = z_p90 = z_max = float("nan")
        per_zone[zone] = {
            "n_points": len(zone_matches),
            "median_yards": z_median,
            "p90_yards": z_p90,
            "max_yards": z_max,
            "match_rate": zone_match_rate,
            "n_unmatched": zone_unmatched,
        }

    scale_rows = labelled.filter(pl.col("scale_pair_id").is_not_null())
    scale_pairs: list[dict] = []
    if scale_rows.height:
        scale_yards = _transform_gt_to_yards(scale_rows, config)
        for scale_pair_id, group in scale_yards.partition_by("scale_pair_id", as_dict=True).items():
            pair_id = scale_pair_id[0] if isinstance(scale_pair_id, tuple) else scale_pair_id
            if group.height != 2:
                continue
            p1, p2 = group.to_dicts()
            measured = (
                (p1["gt_x_yards"] - p2["gt_x_yards"]) ** 2
                + (p1["gt_y_yards"] - p2["gt_y_yards"]) ** 2
            ) ** 0.5
            true_yards = p1["scale_true_yards"]
            scale_pairs.append(
                {
                    "scale_pair_id": pair_id,
                    "measured_yards": measured,
                    "true_yards": true_yards,
                    "signed_error_yards": measured - true_yards,
                }
            )

    return AccuracyResult(
        n_points=len(matches),
        median_yards=median_yards,
        p90_yards=p90_yards,
        max_yards=max_yards,
        per_zone=per_zone,
        match_rate=match_rate,
        n_unmatched=n_unmatched,
        scale_pairs=scale_pairs,
    )
