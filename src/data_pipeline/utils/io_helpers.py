# -----------------------------------------------------------
# Input/Output Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
IO helpers
"""
import shutil
from pathlib import Path
from typing import Any, IO, Iterable, AsyncIterable, TypeVar, Union, Set

import msgspec

T = TypeVar("T")


class JSONLWriter:
    """
    A context manager for writing JSONL files efficiently using msgspec.
    """

    def __init__(self, path: Path, mode: str = "wb"):
        self.path = path
        self.mode = mode
        self.encoder = msgspec.json.Encoder()
        self._file: IO[bytes] | None = None

    def __enter__(self) -> "JSONLWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, self.mode)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._file:
            self._file.close()

    def write(self, record: Any) -> None:
        """
        Encodes the record as JSON and writes it to the file followed by a newline.
        """
        if self._file:
            self._file.write(self.encoder.encode(record))
            self._file.write(b"\n")


def merge_jsonl_files(input_paths: list[Path], output_path: Path) -> None:
    """
    Merges multiple JSONL files into a single file efficiently.

    Args:
        input_paths (list[Path]): A list of Path objects for the files to merge.
        output_path (Path): The Path object for the destination file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as outfile:
        for input_path in input_paths:
            if input_path.exists():
                with open(input_path, "rb") as infile:
                    shutil.copyfileobj(infile, outfile)


async def stream_to_jsonl(
    items: Union[Iterable[T], AsyncIterable[T]],
    output_path: Path,
    append: bool = False
) -> None:
    """
    Consumes an async (or sync) iterable and writes items to disk immediately.
    
    Args:
        items: An iterable or async iterable of items to write.
        output_path: The destination file path.
        append: Whether to append to the file instead of overwriting.
    """
    mode = "ab" if append else "wb"
    with JSONLWriter(output_path, mode=mode) as writer:
        if isinstance(items, AsyncIterable):
            async for item in items:
                writer.write(item)
        else:
            for item in items:
                writer.write(item)


async def deduplicate_stream(
    items: Union[Iterable[T], AsyncIterable[T]],
    key_attr: Union[str, list[str]] = "id"
) -> AsyncIterable[T]:
    """
    Middleware generator that filters duplicates from a stream based on a unique key or composite keys.
    
    Args:
        items: The input stream (async or sync).
        key_attr: The attribute name(s) or dict key(s) to use for deduplication. 
                  Can be a single string or a list of strings for composite keys.
    
    Yields:
        Unique items.
    """
    seen_ids: Set[Any] = set()
    is_composite = isinstance(key_attr, list)
    
    # Helper to extract key value
    def get_key_value(item: Any, attr: str) -> Any:
        val = getattr(item, attr, None)
        if val is None and isinstance(item, dict):
            return item.get(attr)
        return val

    # Helper to extract the unique identifier (single or tuple)
    def get_unique_id(item: Any) -> Any:
        if is_composite:
            # For composite keys, create a tuple of values
            return tuple(get_key_value(item, k) for k in key_attr)
        else:
            return get_key_value(item, key_attr)  # type: ignore

    if isinstance(items, AsyncIterable):
        async for item in items:
            unique_id = get_unique_id(item)
            
            # If a composite key has any None, we might still want to dedup it as (None, Val)
            # But usually if the primary ID is missing we might skip. 
            # Assuming robust data, we treat 'None' as a value to check against.
            
            if unique_id not in seen_ids:
                seen_ids.add(unique_id)
                yield item
    else:
        for item in items:
            unique_id = get_unique_id(item)
            if unique_id not in seen_ids:
                seen_ids.add(unique_id)
                yield item


def read_items_from_jsonl(path: Path, model: type[T]) -> Iterable[T]:
    """
    Reads a JSONL file and yields items as instances of the specified msgspec model.
    This is a generator that streams the file line by line.

    Args:
        path (Path): Path to the JSONL file.
        model (type[T]): The msgspec.Struct class to decode into.

    Yields:
        T: Instances of the model.
    """
    if not path.exists():
        return

    decoder = msgspec.json.Decoder(model)
    with open(path, "rb") as f:
        for line in f:
            try:
                yield decoder.decode(line)
            except msgspec.DecodeError:
                # In a robust pipeline, you might log this or handle it, 
                # but for now we skip malformed lines to prevent crashing.
                continue
