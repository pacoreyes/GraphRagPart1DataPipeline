# -----------------------------------------------------------
# I/O and Serialization Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
from pathlib import Path
from typing import Any, Optional

import msgspec

JSONDecodeError = msgspec.DecodeError


async def async_read_json_file(path: Path) -> Optional[dict[str, Any]]:
    """
    Reads and decodes a JSON file asynchronously using msgspec.
    """
    if not await asyncio.to_thread(path.exists):
        return None

    try:
        def read_bytes():
            with open(path, "rb") as f:
                return f.read()
        
        data = await asyncio.to_thread(read_bytes)
        return msgspec.json.decode(data)
    except (OSError, msgspec.DecodeError):
        return None


async def async_write_json_file(path: Path, data: Any) -> None:
    """
    Encodes and writes data to a JSON file asynchronously using msgspec.
    Ensures parent directories exist.
    """
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)

    def write_bytes():
        with open(path, "wb") as f:
            f.write(msgspec.json.encode(data))

    await asyncio.to_thread(write_bytes)


async def async_read_text_file(path: Path) -> Optional[str]:
    """
    Reads a text file asynchronously.
    """
    if not await asyncio.to_thread(path.exists):
        return None

    try:
        def read_text():
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        
        return await asyncio.to_thread(read_text)
    except OSError:
        return None


async def async_write_text_file(path: Path, content: str) -> None:
    """
    Writes content to a text file asynchronously.
    Ensures parent directories exist.
    """
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)

    def write_text():
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    await asyncio.to_thread(write_text)


async def async_clear_file(path: Path) -> None:
    """
    Deletes the file if it exists asynchronously.
    """
    if await asyncio.to_thread(path.exists):
        await asyncio.to_thread(path.unlink)


def decode_json(data: bytes) -> Any:
    """
    Decodes JSON bytes using msgspec.
    """
    return msgspec.json.decode(data)


def encode_json(data: Any) -> bytes:
    """
    Encodes data to JSON bytes using msgspec.
    """
    return msgspec.json.encode(data)


async def async_append_jsonl(path: Path, items: list[Any]) -> None:
    """
    Appends a list of items as JSONL lines to a file asynchronously.
    Filters out keys with None values (sparse JSON).
    """
    if not items:
        return

    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)

    def write_lines():
        with open(path, "ab") as f:
            for item in items:
                # Convert to dict if it's a struct/dataclass to allow filtering
                if not isinstance(item, dict):
                    try:
                        d = msgspec.to_builtins(item)
                    except TypeError:
                        d = item
                else:
                    d = item

                if isinstance(d, dict):
                    cleaned = {k: v for k, v in d.items() if v is not None}
                else:
                    cleaned = d

                f.write(msgspec.json.encode(cleaned))
                f.write(b"\n")

    await asyncio.to_thread(write_lines)
