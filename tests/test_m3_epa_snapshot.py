"""PII gate and schema/domain guards for the M3-02 HC SP/EP snapshot
(`data/reference/hc_sp_tables/*.csv`, written by `scripts/hc_sp_snapshot.py`).

This file exists SEPARATELY from `tests/test_m3_hc_pii.py` on purpose:
`tests/test_m3_hc_pii.py` is owned by phase M3-3 (M3-03-02) and is edited by
that phase's executor in parallel with this plan (M3-02-03). Putting this
plan's guards in a brand-new file keeps the two phases' file sets disjoint --
no cross-phase edit conflict on the same test file. The roster-name check
below mirrors `tests/test_m3_hc_pii.py`'s loader/heuristic rather than
importing it, for the same reason (that file's internals are not this
phase's to depend on).

Every check here runs against the COMMITTED CSVs only -- never against the
gitignored `hc_files` PII workbook directory this plan's script reads. No
test in this file opens anything under that raw-inputs tree.

Stdlib + polars + pytest only, no network, sub-second.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
ROSTER = REPO_ROOT / "data" / "reference" / "roster.csv"
HC_SP_TABLES = REPO_ROOT / "data" / "reference" / "hc_sp_tables"

_MIN_SURNAME_LEN = 6
_MAX_CELL_STRING_LEN = 24
_DATE_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")

# Columns whose length is legitimately unbounded and independently PII-free,
# matching the exemptions `scripts/hc_sp_snapshot.py` itself applies before
# writing (see `_assert_no_datetime_or_long_strings`'s `exempt_columns`):
# `reg_formulas.csv`'s `formula_text` is a verbatim Excel formula (cell
# references and literal coefficients, RESEARCH section 4.4's no-PII
# verification of the `Reg` tab); `manifest.csv`'s `tab`/`source_workbook`/
# `out_file`/`workbook_sha256` are tab names, the workbook basename (this
# plan's own <pii_discipline> block: "it is a filename, not a person"), an
# output filename, and a hex digest.
_LENGTH_EXEMPT_COLUMNS: dict[str, tuple[str, ...]] = {
    "reg_formulas.csv": ("formula_text",),
    "manifest.csv": ("tab", "source_workbook", "out_file", "workbook_sha256"),
}

# `sp_by_dd*.csv`'s `value` column is an empirical scoring probability.
# Real head-coach data (verified in the committed CSVs this plan wrote) has
# a handful of small-sample cells slightly ABOVE 1.0 (max observed: 1.05,
# `sp_by_dd.csv` down=1/distance=8/opponent) -- exactly the kind of
# small-sample noise EPA-D03 wants surfaced, not silently clamped or
# "fixed" by hand. The domain guard below is therefore non-negative and
# bounded well above 1.0 (real bugs -- e.g. accidentally writing a raw
# count into this column -- produce values in the tens or hundreds, not a
# few percent over 1.0), not a strict [0, 1] check.
_SP_MAX = 1.5

ALL_TABLE_CSVS = tuple(
    sorted(HC_SP_TABLES.glob("*.csv"))
) if HC_SP_TABLES.exists() else ()

MATRIX_CSVS = tuple(
    p for p in ALL_TABLE_CSVS if p.name not in ("manifest.csv", "reg_formulas.csv")
)

CLUSTERED_CSVS = tuple(p for p in MATRIX_CSVS if "clustered" in p.name)


def _require_snapshot() -> None:
    if not HC_SP_TABLES.exists() or not ALL_TABLE_CSVS:
        pytest.skip(f"{HC_SP_TABLES} has no committed CSVs -- run scripts/hc_sp_snapshot.py first")


def _load_roster_names() -> tuple[set[str], set[str]]:
    """Return `(full_names, long_surnames)` from `roster.csv`, same shape as
    `tests/test_m3_hc_pii.py::_load_roster_names` (not imported -- see
    module docstring)."""
    if not ROSTER.exists():
        pytest.skip(f"{ROSTER} does not exist -- cannot run the PII gate")
    with ROSTER.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    full_names = {row["player_name"].strip() for row in rows if row.get("player_name")}
    if not full_names:
        pytest.skip("roster.csv produced zero player names -- loader broken?")

    surnames: set[str] = set()
    for name in full_names:
        parts = name.split()
        if not parts:
            continue
        surname = parts[-1]
        if len(surname) >= _MIN_SURNAME_LEN:
            surnames.add(surname)
    return full_names, surnames


def _read_all_cells(path: Path) -> list[tuple[str, str]]:
    """`(column_name, raw_cell_text)` for every cell in `path`, read as raw
    CSV text (not typed) so string-length/date/name checks see exactly what
    is committed to disk."""
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cells: list[tuple[str, str]] = []
        for row in reader:
            for col, val in row.items():
                if val is not None:
                    cells.append((col, val))
    return cells


def test_no_roster_name_or_long_surname_in_committed_snapshot() -> None:
    _require_snapshot()
    full_names, surnames = _load_roster_names()

    for path in ALL_TABLE_CSVS:
        text = path.read_text(encoding="utf-8")
        lower = text.lower()
        for name in full_names:
            assert name.lower() not in lower, f"player name {name!r} found in {path}"
        for surname in surnames:
            pattern = re.compile(rf"\b{re.escape(surname)}\b", re.IGNORECASE)
            assert not pattern.search(text), f"surname {surname!r} found in {path}"


def test_no_cell_longer_than_24_chars_outside_documented_exemptions() -> None:
    _require_snapshot()
    for path in ALL_TABLE_CSVS:
        exempt = _LENGTH_EXEMPT_COLUMNS.get(path.name, ())
        for col, val in _read_all_cells(path):
            if col in exempt:
                continue
            assert len(val) <= _MAX_CELL_STRING_LEN, (
                f"{path}: column {col!r} has a cell longer than "
                f"{_MAX_CELL_STRING_LEN} chars: {val!r}"
            )


def test_no_date_pattern_in_any_committed_cell() -> None:
    """RESEARCH Pitfall 4: Excel silently autocorrected `"1-5"`/`"6-10"`/
    `"11-15"` into `datetime(2021, ...)` cells. This is the regression guard
    for that trap -- no committed cell, in any column of any file (including
    the length-exempt ones), may contain a `YYYY-MM-DD` date pattern.

    `manifest.csv`'s `extracted_at` column is the one intentional, documented
    exception: a real UTC extraction date (this plan's task 1 action), not a
    corrupted distance-bin label -- it is deliberately excluded here."""
    _require_snapshot()
    for path in ALL_TABLE_CSVS:
        date_exempt = {"extracted_at"} if path.name == "manifest.csv" else set()
        for col, val in _read_all_cells(path):
            if col in date_exempt:
                continue
            assert not _DATE_PATTERN.search(val), (
                f"{path}: column {col!r} contains a YYYY-MM-DD date pattern: {val!r} "
                "-- an Excel-autocorrected bin label slipped through unreconstructed"
            )


def test_field_half_and_down_domains() -> None:
    _require_snapshot()
    for path in MATRIX_CSVS:
        df = pl.read_csv(path)
        if "field_half" in df.columns:
            halves = set(df["field_half"].unique().to_list())
            assert halves <= {"own", "opponent"}, f"{path}: unexpected field_half values {halves}"
        if "down" in df.columns:
            downs = set(df["down"].unique().to_list())
            assert downs <= {1, 2, 3, 4}, f"{path}: unexpected down values {downs}"

    reg_path = HC_SP_TABLES / "reg_formulas.csv"
    if reg_path.exists():
        df = pl.read_csv(reg_path)
        halves = set(df["field_half"].unique().to_list())
        assert halves <= {"own", "opponent"}, f"{reg_path}: unexpected field_half values {halves}"
        downs = set(df["down"].unique().to_list())
        assert downs <= {1, 2, 3, 4}, f"{reg_path}: unexpected down values {downs}"


def test_sp_probability_column_in_domain() -> None:
    _require_snapshot()
    for path in HC_SP_TABLES.glob("sp_by_dd*.csv"):
        df = pl.read_csv(path)
        values = df["value"]
        assert (values >= 0.0).all(), f"{path}: negative SP value found"
        assert (values <= _SP_MAX).all(), f"{path}: SP value above {_SP_MAX} found"


def test_sample_size_column_is_non_negative_integer() -> None:
    _require_snapshot()
    for path in HC_SP_TABLES.glob("sample_size_by_dd*.csv"):
        df = pl.read_csv(path)
        assert df["n"].dtype in (pl.Int64, pl.Int32), f"{path}: n is not an integer column"
        assert (df["n"] >= 0).all(), f"{path}: negative n found"


def test_sp_never_has_a_key_without_a_matching_sample_size() -> None:
    """A probability without a sample size is a failure -- every key in
    `sp_by_dd.csv` must have a matching key in `sample_size_by_dd.csv`.

    The reverse does not hold in the real workbook: `sample_size_by_dd.csv`
    has 83 (down, distance_bin, field_half) keys with no matching SP row,
    because the head coach's own `SP by D&D` tab leaves a cell blank rather
    than computing a ratio there -- most notably down=1..4/distance=26/own,
    where `n` is 2290/2024/1596/947 (real, large sample sizes) but SP was
    never filled in. That gap is itself a finding worth carrying into
    M3-02-06/07, not something this snapshot should paper over by inventing
    a value the source workbook does not contain."""
    _require_snapshot()
    sp_path = HC_SP_TABLES / "sp_by_dd.csv"
    n_path = HC_SP_TABLES / "sample_size_by_dd.csv"
    if not sp_path.exists() or not n_path.exists():
        pytest.skip("sp_by_dd.csv or sample_size_by_dd.csv missing")

    sp = pl.read_csv(sp_path)
    n = pl.read_csv(n_path)
    key_cols = ["down", "distance_bin", "field_half"]
    sp_keys = set(sp.select(key_cols).rows())
    n_keys = set(n.select(key_cols).rows())
    assert sp_keys <= n_keys, f"sp_by_dd.csv has keys missing from sample_size_by_dd.csv: {sp_keys - n_keys}"


def test_clustered_files_carry_the_three_reconstructed_bin_labels() -> None:
    """A future re-run that silently loses the reconstruction fails here."""
    _require_snapshot()
    if not CLUSTERED_CSVS:
        pytest.skip("no *_clustered.csv files committed")

    for path in CLUSTERED_CSVS:
        df = pl.read_csv(path)
        reconstructed = df.filter(pl.col("distance_bin_source") == "reconstructed")
        assert reconstructed.height > 0, f"{path}: no reconstructed rows found"
        labels = set(reconstructed["distance_bin"].unique().to_list())
        assert labels == {"1-5", "6-10", "11-15"}, (
            f"{path}: reconstructed labels {labels} != " "{'1-5', '6-10', '11-15'}"
        )


def test_manifest_lists_all_nine_tabs_and_agrees_with_row_counts() -> None:
    _require_snapshot()
    manifest_path = HC_SP_TABLES / "manifest.csv"
    if not manifest_path.exists():
        pytest.skip("manifest.csv missing")

    manifest = pl.read_csv(manifest_path)
    assert manifest.height == 9, f"manifest.csv has {manifest.height} rows, expected 9 tabs"

    for row in manifest.iter_rows(named=True):
        out_path = HC_SP_TABLES / row["out_file"]
        assert out_path.exists(), f"manifest.csv references missing file {out_path}"
        actual_rows = pl.read_csv(out_path).height
        assert actual_rows == row["n_records"], (
            f"{out_path}: manifest says n_records={row['n_records']}, "
            f"actual row count={actual_rows}"
        )
