"""Tests for `flag_football_ep.owner_csv` -- the shared tolerant reader for CSV files the
project owner edits in Microsoft Excel (BOM, `;` delimiter, mac_roman/cp1252 encoding, CRLF).

Every fixture is synthetic, no real player names.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from flag_football_ep.owner_csv import (
    OWNER_CSV_WRITE_ENCODING,
    decode_owner_csv_bytes,
    read_owner_csv,
    sniff_owner_csv_delimiter,
    write_owner_csv,
)

# --- decode_owner_csv_bytes -----------------------------------------------------------


def test_decode_owner_csv_bytes_plain_utf8_no_bom_has_no_fallback() -> None:
    text, fallback = decode_owner_csv_bytes("a,b\n1,2\n".encode("utf-8"))
    assert text == "a,b\n1,2\n"
    assert fallback is None


def test_decode_owner_csv_bytes_strips_leading_bom_no_fallback() -> None:
    text, fallback = decode_owner_csv_bytes("a,b\n1,2\n".encode("utf-8-sig"))
    assert text == "a,b\n1,2\n"
    assert fallback is None
    assert not text.startswith("﻿")


def test_decode_owner_csv_bytes_bom_with_umlaut_body_decodes_correctly() -> None:
    text, fallback = decode_owner_csv_bytes("game_id,note\ng1,Nühse\n".encode("utf-8-sig"))
    assert text == "game_id,note\ng1,Nühse\n"
    assert fallback is None


def test_decode_owner_csv_bytes_mac_roman_export_detected() -> None:
    raw = b"game_id;note\ng1;\x9fberfl\x9fssig\r\n"  # mac_roman for "überflüssig"
    text, fallback = decode_owner_csv_bytes(raw)
    assert fallback == "mac_roman"
    assert "überflüssig" in text


def test_decode_owner_csv_bytes_cp1252_export_detected() -> None:
    raw = "game_id;note\ng1;caf\xe9\r\n".encode("cp1252")  # "café" -- no mojibake marker byte
    text, fallback = decode_owner_csv_bytes(raw)
    assert fallback == "cp1252"
    assert "café" in text


# --- sniff_owner_csv_delimiter --------------------------------------------------------


def test_sniff_owner_csv_delimiter_comma() -> None:
    assert sniff_owner_csv_delimiter("a,b,c\n1,2,3\n") == ","


def test_sniff_owner_csv_delimiter_semicolon() -> None:
    assert sniff_owner_csv_delimiter("a;b;c\n1;2;3\n") == ";"


def test_sniff_owner_csv_delimiter_empty_text_defaults_to_comma() -> None:
    assert sniff_owner_csv_delimiter("") == ","


# --- read_owner_csv --------------------------------------------------------------------


def test_read_owner_csv_missing_file_returns_empty(tmp_path: Path) -> None:
    rows, notices = read_owner_csv(tmp_path / "does-not-exist.csv")
    assert rows == []
    assert notices == []


def test_read_owner_csv_plain_utf8_comma_no_notices(tmp_path: Path) -> None:
    path = tmp_path / "f.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    rows, notices = read_owner_csv(path)

    assert rows == [{"a": "1", "b": "2"}]
    assert notices == []


def test_read_owner_csv_strips_bom_from_first_column_name(tmp_path: Path) -> None:
    path = tmp_path / "f.csv"
    path.write_bytes("a,b\n1,2\n".encode("utf-8-sig"))

    rows, notices = read_owner_csv(path)

    assert rows == [{"a": "1", "b": "2"}]
    assert notices == []
    assert "﻿a" not in rows[0]


def test_read_owner_csv_semicolon_mac_roman_crlf(tmp_path: Path) -> None:
    path = tmp_path / "f.csv"
    raw = b"game_id;note\r\nifaf-g1;\x9fberfl\x9fssig\r\n"
    path.write_bytes(raw)

    rows, notices = read_owner_csv(path)

    assert rows == [{"game_id": "ifaf-g1", "note": "überflüssig"}]
    assert any("decoded as mac_roman" in n for n in notices)
    assert any("semicolon-delimited" in n for n in notices)


def test_read_owner_csv_bom_plus_semicolon_plus_crlf_combined(tmp_path: Path) -> None:
    """The full Excel round trip at once: BOM (Excel opened it as UTF-8), semicolon
    (German-locale save), CRLF."""
    path = tmp_path / "f.csv"
    text = "game_id;note\r\nifaf-g1;Nühse\r\n"
    path.write_bytes(text.encode("utf-8-sig"))

    rows, notices = read_owner_csv(path)

    assert rows == [{"game_id": "ifaf-g1", "note": "Nühse"}]
    assert notices == ["f.csv: semicolon-delimited (Excel export), auto-detected"]


def test_owner_csv_write_encoding_is_utf8_sig() -> None:
    assert OWNER_CSV_WRITE_ENCODING == "utf-8-sig"


# --- write_owner_csv --------------------------------------------------------------------


def test_write_owner_csv_writes_utf8_sig_bom(tmp_path: Path) -> None:
    df = pl.DataFrame({"game_id": ["g1"], "note": ["Nühse"]})
    path = tmp_path / "out.csv"

    write_owner_csv(df, path)

    raw = path.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")
    assert raw[3:].startswith(b"game_id,")


def test_write_owner_csv_round_trips_through_read_owner_csv(tmp_path: Path) -> None:
    df = pl.DataFrame({"game_id": ["g1", "g2"], "note": ["Nühse", "plain"]})
    path = tmp_path / "out.csv"

    write_owner_csv(df, path)
    rows, notices = read_owner_csv(path)

    assert rows == [
        {"game_id": "g1", "note": "Nühse"},
        {"game_id": "g2", "note": "plain"},
    ]
    assert notices == []


def test_write_owner_csv_creates_parent_directories(tmp_path: Path) -> None:
    df = pl.DataFrame({"a": [1]})
    path = tmp_path / "nested" / "dir" / "out.csv"

    write_owner_csv(df, path)

    assert path.exists()
