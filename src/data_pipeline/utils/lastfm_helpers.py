# -----------------------------------------------------------
# Last.fm API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from dataclasses import dataclass
from typing import Any, Optional
from pathlib import Path

from dagster import AssetExecutionContext

from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
    AsyncClient,
)
from data_pipeline.utils.io_helpers import (
    async_read_json_file, 
    async_write_json_file, 
    generate_cache_key
)


async def _async_cache_lastfm_data(key: str, data: dict[str, Any], cache_dir: Path) -> None:
    """Helper function to cache Last.fm data asynchronously."""
    cache_key = generate_cache_key(key.lower())
    cache_file = cache_dir / f"{cache_key}.json"
    await async_write_json_file(cache_file, data)


async def async_fetch_lastfm_data_with_cache(
    context: AssetExecutionContext,
    params: dict[str, Any],
    cache_key_source: str,
    api_key: str,
    api_url: str,
    cache_dir: Path,
    client: Optional[AsyncClient] = None,
    timeout: int = 60,
    rate_limit_delay: float = 0.0,
) -> Optional[dict[str, Any]]:
    """
    Generic Last.fm API fetcher with local file caching.
    """
    if not all([api_key, api_url]):
        context.log.warning("Last.fm API key or URL not provided. Skipping fetch.")
        return None

    cache_key_str = generate_cache_key(cache_key_source.lower())
    cache_file = cache_dir / f"{cache_key_str}.json"

    # 1. Check Cache
    cached_data = await async_read_json_file(cache_file)
    if cached_data and "error" not in cached_data:
        return cached_data

    # 2. Fetch from API
    try:
        # Merge api_key into params
        request_params = {**params, "api_key": api_key, "format": "json"}

        response = await make_async_request_with_retries(
            context=context,
            url=api_url,
            method="GET",
            params=request_params,
            timeout=timeout,
            rate_limit_delay=rate_limit_delay,
            client=client,
        )
        data = response.json()
        
        if data and "error" not in data:
            await _async_cache_lastfm_data(cache_key_source, data, cache_dir)
            return data
        elif data and "error" in data:
            # Cache the error to avoid re-querying failing entities
            await _async_cache_lastfm_data(cache_key_source, data, cache_dir)
            
        return data
    except Exception as e:
        context.log.warning(f"Last.fm request failed: {e}")
        return None


@dataclass
class LastFmArtistInfo:
    """Parsed artist information from Last.fm API response."""
    tags: list[str]
    similar_artists: list[str]


def parse_lastfm_artist_response(response: Optional[dict[str, Any]]) -> LastFmArtistInfo:
    """
    Parses the Last.fm artist.getInfo API response.

    Extracts tags and similar artists from the nested response structure.
    Handles edge cases where the API returns a single dict instead of a list.

    Args:
        response: Raw JSON response from Last.fm artist.getInfo endpoint.

    Returns:
        LastFmArtistInfo with extracted tags and similar artists.
        Returns empty lists if response is invalid or missing data.
    """
    tags: list[str] = []
    similar_artists: list[str] = []

    if not response or "artist" not in response:
        return LastFmArtistInfo(tags=tags, similar_artists=similar_artists)

    artist_data = response.get("artist") or {}

    # Parse tags - API may return dict (single) or list (multiple)
    raw_tags = (artist_data.get("tags") or {}).get("tag") or []
    if isinstance(raw_tags, dict):
        raw_tags = [raw_tags]
    tags = [t["name"] for t in raw_tags if isinstance(t, dict) and "name" in t]

    # Parse similar artists - API may return dict (single) or list (multiple)
    raw_similar = (artist_data.get("similar") or {}).get("artist") or []
    if isinstance(raw_similar, dict):
        raw_similar = [raw_similar]
    similar_artists = [
        s["name"] for s in raw_similar if isinstance(s, dict) and "name" in s
    ]

    return LastFmArtistInfo(tags=tags, similar_artists=similar_artists)
