# -----------------------------------------------------------
# Last.fm API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
import hashlib
import json
from typing import Any, Dict, List, Optional

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
    # If key is an MBID (UUID), use it directly; otherwise hash it
    # But simple logic: always hash to ensure filesystem safety and uniformity
    cache_key = get_cache_key(key.lower())
    cache_file = LASTFM_CACHE_DIR / f"{cache_key}.json"
    await asyncio.to_thread(LASTFM_CACHE_DIR.mkdir, parents=True, exist_ok=True)

    def write_json():
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    await asyncio.to_thread(write_json)


async def async_fetch_lastfm_data_with_cache(
    context: AssetExecutionContext,
    artist_name: str,
    api_key: str,
    api_url: str,
    artist_mbid: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetches artist data from the Last.fm API asynchronously, using a local file cache.
    Prioritizes querying by MBID if available. Validates against the MusicBrainz ID (MBID) if provided.
    """
    if not all([artist_name, api_key, api_url]):
        context.log.warning("Last.fm API key or URL not provided. Skipping fetch.")
        return None

    # Determine primary cache key: MBID if available, else Name
    primary_key = artist_mbid if artist_mbid else artist_name
    cache_key_str = get_cache_key(primary_key.lower())
    cache_file = LASTFM_CACHE_DIR / f"{cache_key_str}.json"

    exists = await asyncio.to_thread(cache_file.exists)

    if exists:
        try:
            def read_json():
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)

            data = await asyncio.to_thread(read_json)
            if "error" in data:
                # If cached error, we might want to retry if logic changed, but for now respect cache
                return None
            context.log.info(f"Using cached Last.fm data for '{artist_name}' (Key: {primary_key}).")
            return data
        except Exception as e:
            context.log.warning(
                f"Could not read cache for '{artist_name}'. Refetching. Error: {e}"
            )

    # Helper for API Request
    async def fetch_from_api(params: dict[str, Any]) -> Optional[dict[str, Any]]:
        try:
            if settings.LASTFM_RATE_LIMIT_DELAY > 0:
                await asyncio.sleep(settings.LASTFM_RATE_LIMIT_DELAY)

            response = await make_async_request_with_retries(
                context=context,
                url=api_url,
                method="GET",
                params=params,
                timeout=settings.LASTFM_REQUEST_TIMEOUT,
                client=client,
            )
            return response.json()
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            context.log.warning(f"Last.fm request failed: {e}")
            return None

    data = None
    
    # Strategy 1: Query by MBID (if available)
    if artist_mbid:
        params_mbid = {
            "method": "artist.getInfo",
            "mbid": artist_mbid,
            "api_key": api_key,
            "format": "json",
            "autocorrect": 1,
        }
        data = await fetch_from_api(params_mbid)
        
        # If successful (no error), cache and return
        if data and "error" not in data:
            await _async_cache_lastfm_data(artist_mbid, data)
            return data
        else:
            context.log.info(f"MBID lookup failed for {artist_name} ({artist_mbid}). Falling back to name.")

    # Strategy 2: Query by Name (Fallback or Primary)
    params_name = {
        "method": "artist.getInfo",
        "artist": artist_name,
        "api_key": api_key,
        "format": "json",
        "autocorrect": 1,
    }
    
    data = await fetch_from_api(params_name)

    if not data:
        return None

    if "error" in data:
        context.log.warning(
            f"Last.fm API error for '{artist_name}': {data.get('message', 'Unknown error')} "
            f"(Code: {data['error']})"
        )
        # Cache the error to avoid re-querying
        await _async_cache_lastfm_data(primary_key, data)
        return None

    # Validation: If we have an MBID, check if the name-based result matches it
    if artist_mbid:
        response_mbid = data.get("artist", {}).get("mbid")
        if response_mbid and response_mbid != artist_mbid:
            context.log.warning(
                f"MBID mismatch for '{artist_name}'. Wikidata MBID: {artist_mbid}, "
                f"Last.fm MBID: {response_mbid}. Skipping."
            )
            return None

    # Cache successful result using the primary key logic
    await _async_cache_lastfm_data(primary_key, data)
    return data


async def async_get_artist_info_with_fallback(
    context: AssetExecutionContext,
    artist_name: str,
    aliases: List[str],
    artist_mbid: Optional[str],
    api_key: str,
    api_url: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetches artist data from Last.fm asynchronously, trying the primary name first,
    then falling back to aliases.
    """
    # 1. Try Primary Name (which prioritizes MBID internally)
    main_artist_data = await async_fetch_lastfm_data_with_cache(
        context, artist_name, api_key, api_url, artist_mbid, client
    )
    if main_artist_data:
        return main_artist_data

    # 2. Try Aliases (Fallback)
    # Note: Aliases usually don't have distinct MBIDs associated in our dataset context,
    # but we pass the original MBID for validation if found.
    if aliases:
        for alias in aliases:
            context.log.info(f"Trying alias '{alias}' for artist '{artist_name}'.")
            alias_data = await async_fetch_lastfm_data_with_cache(
                context, alias, api_key, api_url, artist_mbid, client
            )
            if alias_data:
                context.log.info(
                    f"Found artist '{artist_name}' on Last.fm using alias '{alias}'."
                )
                return alias_data

    context.log.warning(
        f"Could not find artist '{artist_name}' on Last.fm using primary name or aliases."
    )
    return None


def parse_lastfm_tags(artist_data: dict[str, Any]) -> list[str]:
    """
    Parses tags from the Last.fm artist data structure.
    """
    tags = artist_data.get("tags", {}).get("tag", [])
    if isinstance(tags, dict):  # Single tag case
        tags = [tags]
    return [tag["name"] for tag in tags if isinstance(tag, dict) and "name" in tag]


def parse_lastfm_similar_artists(artist_data: dict[str, Any]) -> list[str]:
    """
    Parses similar artists from the Last.fm artist data structure.
    """
    similar = artist_data.get("similar", {}).get("artist", [])
    if isinstance(similar, dict):  # Single similar artist case
        similar = [similar]
    return [sim["name"] for sim in similar if isinstance(sim, dict) and "name" in sim]