# -----------------------------------------------------------
# Last.fm API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Last.fm API Helpers (Infrastructure Layer)
Generic functions for interacting with Last.fm API with caching.
"""
import asyncio
import hashlib
import json
from typing import Any, Dict, Optional

import httpx
from dagster import AssetExecutionContext

from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
)

LASTFM_CACHE_DIR = settings.lastfm_cache_dirpath


def get_cache_key(text: str) -> str:
    """Creates a SHA256 hash of a string to use as a cache key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def _async_cache_lastfm_data(key: str, data: dict[str, Any]) -> None:
    """Helper function to cache Last.fm data asynchronously."""
    cache_key = get_cache_key(key.lower())
    cache_file = LASTFM_CACHE_DIR / f"{cache_key}.json"
    await asyncio.to_thread(LASTFM_CACHE_DIR.mkdir, parents=True, exist_ok=True)

    def write_json():
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    await asyncio.to_thread(write_json)


async def async_fetch_lastfm_data_with_cache(
    context: AssetExecutionContext,
    params: Dict[str, Any],
    cache_key_source: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[Dict[str, Any]]:
    """
    Generic Last.fm API fetcher with local file caching.
    """
    api_key = settings.LASTFM_API_KEY # Default from settings
    api_url = settings.LASTFM_API_URL

    if not all([api_key, api_url]):
        context.log.warning("Last.fm API key or URL not provided. Skipping fetch.")
        return None

    cache_key_str = get_cache_key(cache_key_source.lower())
    cache_file = LASTFM_CACHE_DIR / f"{cache_key_str}.json"

    if await asyncio.to_thread(cache_file.exists):
        try:
            def read_json():
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            data = await asyncio.to_thread(read_json)
            if "error" not in data:
                return data
        except Exception:
            pass

    # Fetch from API
    try:
        if settings.LASTFM_RATE_LIMIT_DELAY > 0:
            await asyncio.sleep(settings.LASTFM_RATE_LIMIT_DELAY)

        # Merge api_key into params
        request_params = {**params, "api_key": api_key, "format": "json"}

        response = await make_async_request_with_retries(
            context=context,
            url=api_url,
            method="GET",
            params=request_params,
            timeout=settings.LASTFM_REQUEST_TIMEOUT,
            client=client,
        )
        data = response.json()
        
        if data and "error" not in data:
            await _async_cache_lastfm_data(cache_key_source, data)
            return data
        elif data and "error" in data:
            # Cache the error to avoid re-querying failing entities
            await _async_cache_lastfm_data(cache_key_source, data)
            
        return data
    except Exception as e:
        context.log.warning(f"Last.fm request failed: {e}")
        return None
