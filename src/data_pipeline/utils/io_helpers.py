# -----------------------------------------------------------
# Input/Output Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any, Iterable, AsyncIterable, TypeVar, Union


T = TypeVar("T")


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
    seen_ids: set[Any] = set()
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
