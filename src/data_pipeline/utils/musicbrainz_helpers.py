# -----------------------------------------------------------
# MusicBrainz API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from typing import Any, Optional

from dagster import AssetExecutionContext

from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
    AsyncClient,
)
from data_pipeline.utils.io_helpers import async_read_json_file, async_write_json_file


async def fetch_artist_release_groups_async(
    context: AssetExecutionContext, 
    artist_mbid: str, 
    client: AsyncClient,
    cache_dirpath: Path,
    api_url: str,
    headers: dict[str, str],
    rate_limit_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetches all release groups for an artist from MusicBrainz.
    Handles pagination automatically. Implements local JSON caching.
    Uses AsyncClient and exponential backoff retries.
    """
    cache_file = cache_dirpath / f"{artist_mbid}_release.json"
    all_release_groups = []

    # 1. Check Cache
    cached_data = await async_read_json_file(cache_file)
    if cached_data is not None:
        return cached_data

    # 2. Fetch from API if not cached
    limit = 100
    offset = 0
    url = f"{api_url}/release-group"
    
    try:
        while True:
            params = {
                "artist": artist_mbid,
                "limit": limit,
                "offset": offset,
                "fmt": "json"
            }
            
            response = await make_async_request_with_retries(
                context=context,
                url=url,
                method="GET",
                params=params,
                headers=headers,
                client=client,
                rate_limit_delay=rate_limit_delay
            )
            
            data = response.json()
            batch = data.get('release-groups', [])
            all_release_groups.extend(batch)
            
            if len(batch) < limit:
                break
                
            offset += limit
        
        # Save to cache
        await async_write_json_file(cache_file, all_release_groups)
            
    except Exception as e:
        context.log.error(f"MusicBrainz API Async Error for {artist_mbid}: {e}")
        return []

    return all_release_groups


async def fetch_releases_for_group_async(
    context: AssetExecutionContext,
    release_group_mbid: str,
    client: AsyncClient,
    cache_dirpath: Path,
    api_url: str,
    headers: dict[str, str],
    rate_limit_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetches the list of releases associated with a Release Group.
    """
    cache_file = cache_dirpath / f"{release_group_mbid}_releases.json"

    # 1. Check Cache
    cached_data = await async_read_json_file(cache_file)
    if cached_data is not None:
        return cached_data

    context.log.info(f"Cache MISS for {release_group_mbid}. Fetching from API.")

    # 2. Fetch from API
    url = f"{api_url}/release-group/{release_group_mbid}"
    params = {"inc": "releases", "fmt": "json"}
    
    try:
        response = await make_async_request_with_retries(
            context=context,
            url=url,
            method="GET",
            params=params,
            headers=headers,
            client=client,
            rate_limit_delay=rate_limit_delay
        )
        data = response.json()
        releases = data.get("releases", [])

        await async_write_json_file(cache_file, releases)
        return releases

    except Exception as e:
        context.log.error(f"Error fetching releases for group {release_group_mbid}: {e}")
        return []


async def fetch_tracks_for_release_async(
    context: AssetExecutionContext,
    release_mbid: str,
    client: AsyncClient,
    cache_dirpath: Path,
    api_url: str,
    headers: dict[str, str],
    rate_limit_delay: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetches the tracklist for a specific Release MBID.
    """
    cache_file = cache_dirpath / f"{release_mbid}_tracks.json"
    
    # 1. Check Cache
    cached_tracks = await async_read_json_file(cache_file)
    if cached_tracks is not None:
        return cached_tracks

    # context.log.debug(f"Cache MISS for Release {release_mbid}. Fetching from API.")

    # 2. Fetch from API
    url = f"{api_url}/release/{release_mbid}"
    params = {"inc": "recordings", "fmt": "json"}
    
    try:
        response = await make_async_request_with_retries(
            context=context,
            url=url,
            method="GET",
            params=params,
            headers=headers,
            client=client,
            rate_limit_delay=rate_limit_delay
        )
        data = response.json()
        
        tracks = []
        for medium in data.get("media", []):
            for track in medium.get("tracks", []):
                recording = track.get("recording", {})
                if not recording:
                    continue
                    
                tracks.append({
                    "id": recording["id"],
                    "title": recording["title"],
                    "length": recording.get("length")
                })
        
        await async_write_json_file(cache_file, tracks)
        return tracks

    except Exception as e:
        context.log.error(f"Error fetching tracks for release {release_mbid}: {e}")
        return []


# --- Release Group Filtering and Parsing ---

ALLOWED_PRIMARY_TYPES = {"Album", "Single"}


def filter_release_groups(
    release_groups: list[dict[str, Any]],
    allowed_types: Optional[set[str]] = None,
    exclude_secondary_types: bool = True,
) -> list[dict[str, Any]]:
    """
    Filters release groups by primary type and secondary types.

    Args:
        release_groups: List of release group dicts from MusicBrainz API.
        allowed_types: Set of allowed primary-type values (default: Album, Single).
        exclude_secondary_types: If True, excludes release groups with secondary types
            (e.g., Compilation, Live, Soundtrack).

    Returns:
        Filtered list of release groups.
    """
    if allowed_types is None:
        allowed_types = ALLOWED_PRIMARY_TYPES

    return [
        rg for rg in release_groups
        if rg.get("primary-type") in allowed_types
        and (not exclude_secondary_types or not rg.get("secondary-types"))
    ]


def parse_release_year(date_string: Optional[str]) -> Optional[int]:
    """
    Parses a year from a MusicBrainz date string.

    MusicBrainz dates can be in formats: "YYYY", "YYYY-MM", or "YYYY-MM-DD".

    Args:
        date_string: Date string from MusicBrainz (e.g., "1999-03-15").

    Returns:
        Year as integer, or None if parsing fails.
    """
    if not date_string:
        return None
    try:
        return int(date_string.split("-")[0])
    except (ValueError, IndexError):
        return None


def select_best_release(releases: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """
    Selects the best release from a list of releases for a release group.

    Strategy:
    1. Prefer releases with "Official" status over other statuses.
    2. Among same-status releases, prefer the oldest by release date.

    This ensures we get the canonical/original release rather than
    reissues, deluxe editions, or regional variants.

    Args:
        releases: List of release dicts from MusicBrainz API.

    Returns:
        The best release dict, or None if the list is empty.
    """
    if not releases:
        return None

    def sort_key(release: dict[str, Any]) -> tuple[int, str]:
        # Official status gets rank 0, anything else gets rank 1
        status_rank = 0 if release.get("status") == "Official" else 1
        # Use far-future date as default for missing dates
        date = release.get("date", "9999-99-99") or "9999-99-99"
        return status_rank, date

    sorted_releases = sorted(releases, key=sort_key)
    return sorted_releases[0]
