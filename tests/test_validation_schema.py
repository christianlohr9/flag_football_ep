"""Tests for the contract loader and column-level validation in `validation.schema`."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.validation.schema import (
    Contract,
    ContractVersionError,
    MissingCoreColumnsError,
    check_column_domains,
    load_contract,
    validate_header,
)

CORE_COLUMNS = {"PLAY #", "ODK", "DN", "DIST", "YARD LN", "PLAY TYPE", "RESULT", "GN/LS"}


def _load_raw(contract_path: Path) -> dict:
    return json.loads(contract_path.read_text(encoding="utf-8"))


def _write_contract(tmp_path: Path, raw: dict, name: str = "contract.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(raw), encoding="utf-8")
    return path


@pytest.fixture
def contract(contract_path: Path) -> Contract:
    return load_contract(contract_path)


def _core_row(**overrides: str) -> dict:
    row = {
        "PLAY #": "1",
        "ODK": "O",
        "DN": "1",
        "DIST": "10",
        "YARD LN": "25",
        "PLAY TYPE": "Rush",
        "RESULT": "Complete",
        "GN/LS": "5",
    }
    row.update(overrides)
    return row


# --- load_contract -----------------------------------------------------------


def test_load_contract_happy_path(contract_path: Path) -> None:
    contract = load_contract(contract_path)
    assert contract.version == "1.1"
    assert contract.status == "provisional"
    assert set(contract.core_columns) == CORE_COLUMNS
    assert len(contract.optional_columns) >= 8


def test_load_contract_tolerates_minor_bump(tmp_path: Path, contract_path: Path) -> None:
    raw = _load_raw(contract_path)
    raw["contract_version"] = "1.2"
    path = _write_contract(tmp_path, raw)

    with pytest.warns(UserWarning):
        contract = load_contract(path)

    assert contract.version == "1.2"


def test_load_contract_tolerates_larger_minor_bump(tmp_path: Path, contract_path: Path) -> None:
    raw = _load_raw(contract_path)
    raw["contract_version"] = "1.7"
    path = _write_contract(tmp_path, raw)

    with pytest.warns(UserWarning):
        contract = load_contract(path)

    assert contract.version == "1.7"


def test_load_contract_rejects_major_bump(tmp_path: Path, contract_path: Path) -> None:
    raw = _load_raw(contract_path)
    raw["contract_version"] = "2.0"
    path = _write_contract(tmp_path, raw)

    with pytest.raises(ContractVersionError) as exc_info:
        load_contract(path)

    message = str(exc_info.value)
    assert "2" in message
    assert "1" in message


def test_load_contract_tolerates_unknown_top_level_and_column_keys(
    tmp_path: Path, contract_path: Path
) -> None:
    raw = _load_raw(contract_path)
    raw["some_future_top_level_key"] = {"anything": True}
    raw["columns"][0]["some_future_column_key"] = "anything"
    path = _write_contract(tmp_path, raw)

    contract = load_contract(path)  # must not raise

    assert contract.version == raw["contract_version"]


def test_load_contract_exposes_unknown_column_by_presence(tmp_path: Path, contract_path: Path) -> None:
    raw = _load_raw(contract_path)
    raw["columns"].append(
        {"name": "NEW COLUMN", "dtype": "str", "presence": "optional", "required": False}
    )
    path = _write_contract(tmp_path, raw)

    contract = load_contract(path)

    assert "NEW COLUMN" in contract.column_specs
    assert contract.column_specs["NEW COLUMN"]["presence"] == "optional"
    assert "NEW COLUMN" in contract.optional_columns


# --- validate_header -----------------------------------------------------------


def test_validate_header_missing_core_column_raises(contract: Contract) -> None:
    row = _core_row()
    del row["RESULT"]
    df = pl.DataFrame([row])

    with pytest.raises(MissingCoreColumnsError) as exc_info:
        validate_header(df, contract)

    assert "RESULT" in str(exc_info.value)


def test_validate_header_materializes_missing_optional_column(contract: Contract) -> None:
    df = pl.DataFrame([_core_row()])
    assert "COVERAGE" not in df.columns

    result_df, report = validate_header(df, contract)

    assert "COVERAGE" in result_df.columns
    assert result_df.schema["COVERAGE"] == pl.Utf8
    assert result_df["COVERAGE"].null_count() == result_df.height
    assert "COVERAGE" in report.materialized_optional


def test_validate_header_lists_unknown_columns_and_keeps_frame_usable(contract: Contract) -> None:
    row = _core_row(**{"TARGET ROUTE": "SLANT", "MOTION": "Y", "WRISTCOACH #": "12"})
    df = pl.DataFrame([row])

    result_df, report = validate_header(df, contract)

    assert set(report.unknown) == {"TARGET ROUTE", "MOTION", "WRISTCOACH #"}
    assert result_df.height == 1
    for col in ("TARGET ROUTE", "MOTION", "WRISTCOACH #"):
        assert col in result_df.columns


def test_validate_header_strips_bom_from_first_column(contract: Contract) -> None:
    row = _core_row()
    df = pl.DataFrame([row]).rename({"PLAY #": "﻿PLAY #"})

    result_df, _report = validate_header(df, contract)

    assert "PLAY #" in result_df.columns
    assert "﻿PLAY #" not in result_df.columns


# --- check_column_domains -----------------------------------------------------------


def test_check_column_domains_flags_dn_out_of_range(contract: Contract) -> None:
    rows = [_core_row(DN=v) for v in ["1", "2", "5", "9"]]
    df, _ = validate_header(pl.DataFrame(rows), contract)

    violations = check_column_domains(df, contract)
    dn_violations = [v for v in violations if v.column == "DN" and v.rule == "range"]
    assert len(dn_violations) == 1
    assert dn_violations[0].n_rows == 2
    assert set(dn_violations[0].examples) <= {"5", "9"}


def test_check_column_domains_flags_yard_ln_out_of_range(contract: Contract) -> None:
    rows = [_core_row(**{"YARD LN": v}) for v in ["-50", "0", "50", "51", "-60"]]
    df, _ = validate_header(pl.DataFrame(rows), contract)

    violations = check_column_domains(df, contract)
    yl_violations = [v for v in violations if v.column == "YARD LN" and v.rule == "range"]
    assert len(yl_violations) == 1
    assert yl_violations[0].n_rows == 2


def test_check_column_domains_reports_values_open_as_unseen_not_violation(contract: Contract) -> None:
    rows = [
        _core_row(**{"DEF FRONT": "LINE 5"}),
        _core_row(**{"DEF FRONT": "SOME NEW CALL"}),
    ]
    df, _ = validate_header(pl.DataFrame(rows), contract)

    violations = check_column_domains(df, contract)
    def_front = [v for v in violations if v.column == "DEF FRONT"]
    assert len(def_front) == 1
    assert def_front[0].rule == "unseen_value"
    assert "SOME NEW CALL" in def_front[0].examples
    assert all(v.rule != "value" for v in def_front)


def test_check_column_domains_ignores_null_cells(contract: Contract) -> None:
    rows = [_core_row(DN="1"), _core_row(DN="2")]
    df, _ = validate_header(pl.DataFrame(rows), contract)
    df = df.with_columns(pl.Series("DN", [None, "2"], dtype=pl.Utf8))

    violations = check_column_domains(df, contract)
    dn_violations = [v for v in violations if v.column == "DN"]
    assert dn_violations == []


def test_check_column_domains_flags_dtype_cast_failure(contract: Contract) -> None:
    rows = [_core_row(DN="1"), _core_row(DN="not-a-number")]
    df, _ = validate_header(pl.DataFrame(rows), contract)

    violations = check_column_domains(df, contract)
    dtype_violations = [v for v in violations if v.column == "DN" and v.rule == "dtype"]
    assert len(dtype_violations) == 1
    assert dtype_violations[0].n_rows == 1
    assert "not-a-number" in dtype_violations[0].examples
