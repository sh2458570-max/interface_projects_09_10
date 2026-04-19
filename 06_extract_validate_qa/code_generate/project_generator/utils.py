"""Shared helpers for the protocol project generator."""

from __future__ import annotations

import json
import re
from pathlib import Path


_NON_ASCII_RE = re.compile(r"[^0-9A-Za-z_]+")
_MULTI_UNDERSCORE_RE = re.compile(r"_+")


def normalize_token(raw: str) -> str:
    """Converts arbitrary text into a stable ASCII-like identifier token.

    Args:
        raw: Raw source text.

    Returns:
        A token that is safe to use in file names and identifiers.
    """

    pieces: list[str] = []
    for char in raw.strip():
        if char.isascii() and (char.isalnum() or char == "_"):
            pieces.append(char.lower())
        elif char.isalnum():
            pieces.append(f"u{ord(char):x}")
        else:
            pieces.append("_")
    text = "".join(pieces)
    text = _NON_ASCII_RE.sub("_", text)
    text = _MULTI_UNDERSCORE_RE.sub("_", text).strip("_")
    if not text:
        text = "field"
    if text[0].isdigit():
        text = f"f_{text}"
    return text


def to_type_name(raw: str) -> str:
    """Converts a raw protocol name into a C++ type-like name.

    Args:
        raw: Raw protocol name.

    Returns:
        A normalized C++ type name.
    """

    token = normalize_token(raw)
    parts = [part for part in token.split("_") if part]
    transformed = []
    for part in parts:
        if part and part[0].isalpha():
            transformed.append(part[0].upper() + part[1:])
        else:
            transformed.append(part.upper())
    return "_".join(transformed) or "Protocol"


def to_snake_name(raw: str) -> str:
    """Converts text into a snake_case-like file token.

    Args:
        raw: Raw input text.

    Returns:
        A normalized snake token.
    """

    return normalize_token(raw)


def ensure_directory(path: Path) -> None:
    """Creates a directory when it does not exist.

    Args:
        path: Directory path to create.
    """

    path.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, content: str) -> None:
    """Writes UTF-8 text to disk.

    Args:
        path: Destination file path.
        content: Text to persist.
    """

    ensure_directory(path.parent)
    path.write_text(content, encoding="utf-8")


def dump_json(path: Path, payload: object) -> None:
    """Writes formatted JSON to disk.

    Args:
        path: Destination file path.
        payload: JSON-serializable object.
    """

    ensure_directory(path.parent)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

