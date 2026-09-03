"""Contract loader and column-level validation driven by docs/data-contract.schema.json.

Column-level validation is entirely data-driven: nothing here hard-codes the
core/optional column lists or their dtypes/ranges/value sets. The contract
file is the single source of truth, so a ratified post-1.1 bump changes
behaviour with no code edit, as long as the major version stays 1.
"""

from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

SUPPORTED_CONTRACT_MAJOR = 1

# The contract version this code was written against. A minor bump above
# this is tolerated (warned about); a major bump is rejected. Bumped to 2 for
# the v1.2 amendment (2026-09-03): six head-coach RESULT tokens added
# (docs/data-contract.md v1.2-Änderungsvermerk).
_BASELINE_MINOR = 2

_DTYPE_MAP: dict[str, pl.DataType] = {
    "int": pl.Int64,
    "float": pl.Float64,
    "str": pl.Utf8,
}


class ContractVersionError(Exception):
    """Raised when a contract's major version is not supported by this code."""


class MissingCoreColumnsError(Exception):
    """Raised when a header is missing one or more core (required) columns."""


@dataclass(frozen=True)
class Contract:
    version: str
    status: str
    core_columns: list[str]
    optional_columns: list[str]
    column_specs: dict[str, dict[str, Any]]
    unknown_rule: str
    file_format: dict[str, Any]
    derived_fields: dict[str, Any]
    raw: dict[str, Any]


@dataclass(frozen=True)
class HeaderReport:
    missing_core: list[str]
    materialized_optional: list[str]
    unknown: list[str]


@dataclass(frozen=True)
class DomainViolation:
    column: str
    rule: str
    n_rows: int
    examples: list[str]


def _parse_major_minor(version: str) -> tuple[int, int]:
    parts = str(version).split(".")
    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 1 else 0
    return major, minor


def load_contract(path: Path) -> Contract:
    """Load and parse the data contract at `path`.

    Tolerates unknown top-level and per-column keys. Accepts any minor
    version bump within `SUPPORTED_CONTRACT_MAJOR` (warns, does not raise).
    Raises `ContractVersionError` on a major-version mismatch.
    """
    with open(path, encoding="utf-8") as f:
        raw: dict[str, Any] = json.load(f)

    version = str(raw["contract_version"])
    major, minor = _parse_major_minor(version)

    if major != SUPPORTED_CONTRACT_MAJOR:
        raise ContractVersionError(
            f"contract major version {major} (from contract_version='{version}') is not "
            f"supported by this code; supported major version is {SUPPORTED_CONTRACT_MAJOR}"
        )

    if minor != _BASELINE_MINOR:
        warnings.warn(
            f"contract_version '{version}' has a different minor version than the "
            f"{SUPPORTED_CONTRACT_MAJOR}.{_BASELINE_MINOR} baseline this code was written "
            "against; verify behaviour after a contract bump",
            stacklevel=2,
        )

    status = str(raw.get("status", ""))

    columns: list[dict[str, Any]] = raw.get("columns", [])
    column_specs: dict[str, dict[str, Any]] = {c["name"]: c for c in columns}

    # Built from the per-column `presence` field, not the contract_model
    # shortcut list, so a ratified contract that promotes a column changes
    # behaviour with no code edit.
    core_columns = [c["name"] for c in columns if c.get("presence") == "core"]
    optional_columns = [c["name"] for c in columns if c.get("presence") == "optional"]

    contract_model = raw.get("contract_model", {})
    shortcut_core = contract_model.get("core_columns")
    if shortcut_core is not None and set(shortcut_core) != set(core_columns):
        warnings.warn(
            "contract_model.core_columns disagrees with the per-column 'presence' field: "
            f"shortcut={sorted(shortcut_core)} vs columns={sorted(core_columns)}",
            stacklevel=2,
        )

    unknown_rule = str(contract_model.get("unknown_rule", ""))
    file_format = raw.get("file_format", {})
    derived_fields = raw.get("derived_fields", {})

    return Contract(
        version=version,
        status=status,
        core_columns=core_columns,
        optional_columns=optional_columns,
        column_specs=column_specs,
        unknown_rule=unknown_rule,
        file_format=file_format,
        derived_fields=derived_fields,
        raw=raw,
    )


def _strip_bom(name: str) -> str:
    return name.lstrip("﻿")


def validate_header(df: pl.DataFrame, contract: Contract) -> tuple[pl.DataFrame, HeaderReport]:
    """Validate `df`'s header against `contract` (exact column-name equality).

    - Missing core columns raise `MissingCoreColumnsError`.
    - Missing optional columns are materialized as all-null Utf8 columns.
    - Columns not known to the contract are left in place (not dropped) and
      reported as `unknown` per the contract's `unknown_rule` ("ignored",
      i.e. contract-neutral, not a validation failure).
    """
    columns = list(df.columns)
    if columns:
        stripped_first = _strip_bom(columns[0])
        if stripped_first != columns[0]:
            df = df.rename({columns[0]: stripped_first})
            columns[0] = stripped_first

    missing_core = [c for c in contract.core_columns if c not in columns]
    if missing_core:
        raise MissingCoreColumnsError(f"missing core column(s): {missing_core}")

    materialized_optional: list[str] = []
    for col in contract.optional_columns:
        if col not in columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
            materialized_optional.append(col)

    known = set(contract.core_columns) | set(contract.optional_columns)
    unknown = [c for c in columns if c not in known]

    report = HeaderReport(
        missing_core=missing_core,
        materialized_optional=materialized_optional,
        unknown=unknown,
    )
    return df, report


def check_column_domains(df: pl.DataFrame, contract: Contract) -> list[DomainViolation]:
    """Check dtype/range/value domains for every contract column present in `df`.

    Null cells are ignored (absent charting is not a domain violation).
    Columns with `values_open: true` report unseen values as informational
    rule `"unseen_value"`, never as a violation.
    """
    violations: list[DomainViolation] = []

    for name, spec in contract.column_specs.items():
        if name not in df.columns:
            continue

        col = df[name]
        not_null = col.is_not_null()
        if int(not_null.sum()) == 0:
            continue

        dtype_name = spec.get("dtype")
        target_dtype = _DTYPE_MAP.get(dtype_name)

        casted: pl.Series | None = None
        if target_dtype is not None:
            str_col = col.cast(pl.Utf8, strict=False)
            casted = str_col.cast(target_dtype, strict=False)
            dtype_fail = not_null & casted.is_null()
            n_dtype_fail = int(dtype_fail.sum())
            if n_dtype_fail:
                examples = (
                    df.filter(dtype_fail)[name]
                    .cast(pl.Utf8, strict=False)
                    .drop_nulls()
                    .unique()
                    .head(5)
                    .to_list()
                )
                violations.append(
                    DomainViolation(column=name, rule="dtype", n_rows=n_dtype_fail, examples=examples)
                )

        rng = spec.get("range")
        if rng is not None and casted is not None:
            lo, hi = rng
            valid_range = not_null & casted.is_not_null() & casted.is_between(lo, hi, closed="both")
            out_of_range = not_null & casted.is_not_null() & ~valid_range
            n_out = int(out_of_range.sum())
            if n_out:
                examples = (
                    df.filter(out_of_range)[name].cast(pl.Utf8, strict=False).unique().head(5).to_list()
                )
                violations.append(
                    DomainViolation(column=name, rule="range", n_rows=n_out, examples=examples)
                )

        values = spec.get("values")
        if values is not None:
            values_open = bool(spec.get("values_open", False))
            str_col = col.cast(pl.Utf8, strict=False)
            not_in_values = not_null & ~str_col.is_in(values)
            n_unseen = int(not_in_values.sum())
            if n_unseen:
                examples = df.filter(not_in_values)[name].unique().head(5).to_list()
                rule = "unseen_value" if values_open else "value"
                violations.append(
                    DomainViolation(column=name, rule=rule, n_rows=n_unseen, examples=examples)
                )

    return violations
