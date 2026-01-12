# -----------------------------------------------------------
# MusicBrainz API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import json
import musicbrainzngs
import httpx
from typing import Any
from dagster import AssetExecutionContext
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import make_async_request_with_retries


def setup_musicbrainz():
    """Configures the MusicBrainz client with the project User-Agent."""
    musicbrainzngs.set_useragent(
        settings.APP_NAME, 
        settings.APP_VERSION, 
        settings.CONTACT_EMAIL
    )
    # Global rate limit compliance for the library if needed, 
    # though we handle it via our own delays usually.
    musicbrainzngs.set_rate_limit(limit_or_interval=1.0, new_requests=1)


async def fetch_artist_release_groups_async(
    context: AssetExecutionContext, 
    artist_mbid: str, 
    client: httpx.AsyncClient
) -> list[dict[str, Any]]:
    """
    Async version of get_artist_release_groups_filtered.
    Fetches all release groups for an artist, filtered by:
    - Primary Type: 'Album' or 'Single'
    - Secondary Type: None/Empty
    
    Handles pagination automatically. Implements local JSON caching.
    Uses httpx and exponential backoff retries.
    """
    cache_file = settings.MUSICBRAINZ_CACHE_DIRPATH / f"{artist_mbid}_release.json"
    all_release_groups = []

    # 1. Check Cache
    if cache_file.exists():
        try:
            # We use synchronous file read here as it's a local JSON file 
            # and usually fast enough. Could be asyncified if needed.
            with open(cache_file, "r", encoding="utf-8") as f:
                all_release_groups = json.load(f)
            # If cache hits, we return immediately (already filtered when saved? 
            # The sync version saved raw data then filtered. Let's check logic.)
            # The sync version logic: 
            # - Fetch ALL (pagination) -> Save ALL to cache -> Return FILTERED.
            # So if we load from cache, we must Filter.
            
            # Wait, the sync version code:
            # - fetches all
            # - saves all to cache
            # - filters from all_release_groups
            # So yes, we need to filter after loading from cache.
        except json.JSONDecodeError:
            pass  # corrupted cache, re-fetch

    # 2. Fetch from API if not cached
    if not all_release_groups:
        limit = 100
        offset = 0
        url = f"{settings.MUSICBRAINZ_API_URL}/release-group"
        
        try:
            while True:
                params = {
                    "artist": artist_mbid,
                    "limit": limit,
                    "offset": offset,
                    "fmt": "json"
                }
                
                # Use shared client and retry logic
                response = await make_async_request_with_retries(
                    context=context,
                    url=url,
                    method="GET",
                    params=params,
                    headers=settings.DEFAULT_REQUEST_HEADERS,
                    client=client,
                    rate_limit_delay=settings.MUSICBRAINZ_RATE_LIMIT_DELAY
                )
                
                data = response.json()
                # Note: API JSON key is 'release-groups', 'musicbrainzngs' might have mapped it to 'release-group-list'
                batch = data.get('release-groups', [])
                all_release_groups.extend(batch)
                
                if len(batch) < limit:
                    break
                    
                offset += limit
            
            # Save to cache
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(all_release_groups, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            context.log.error(f"MusicBrainz API Async Error for {artist_mbid}: {e}")
            # Return what we have
            return []

    # 3. Filter
    filtered_groups = []
    for rg in all_release_groups:
        primary = rg.get('primary-type')
        # API JSON key is 'secondary-types' (list), mbngs maps it to 'secondary-type-list'
        secondary = rg.get('secondary-types', [])

        # Determine if secondary is empty.
        # In raw JSON, it's a list of strings.
        
        if primary in ["Album", "Single"] and not secondary:
            filtered_groups.append(rg)
            
    return filtered_groups


async def fetch_tracks_for_release_group_async(
    context: AssetExecutionContext,
    release_group_mbid: str,
    client: httpx.AsyncClient
) -> list[dict[str, Any]]:
    """
    Fetches the tracklist for a given Release Group MBID.
    Strategy:
    1. Fetch all releases for the Release Group.
    2. Pick the earliest "Official" release.
    3. Fetch recordings (tracks) for that specific Release.
    4. Cache the resulting tracklist.
    """
    cache_file = settings.MUSICBRAINZ_CACHE_DIRPATH / f"{release_group_mbid}_tracks.json"
    
    # 1. Check Cache
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            pass

    # 2. Step A: Find representative Release
    try:
        url_rg = f"{settings.MUSICBRAINZ_API_URL}/release-group/{release_group_mbid}"
        params_rg = {"inc": "releases", "fmt": "json"}
        
        response_rg = await make_async_request_with_retries(
            context=context,
            url=url_rg,
            method="GET",
            params=params_rg,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client,
            rate_limit_delay=settings.MUSICBRAINZ_RATE_LIMIT_DELAY
        )
        data_rg = response_rg.json()
        releases = data_rg.get("releases", [])
        
        if not releases:
            context.log.warning(f"No releases found for Release Group {release_group_mbid}")
            return []

        # Filter and Sort Strategy:
        # - Prefer 'Official' status
        # - Prefer oldest (earliest) date
        
        def sort_key(r):
            status_rank = 0 if r.get("status") == "Official" else 1
            date = r.get("date", "9999-99-99") or "9999-99-99"
            return (status_rank, date)

        releases.sort(key=sort_key)
        best_release = releases[0]
        release_id = best_release["id"]
        
        # 3. Step B: Fetch Recordings for the chosen Release
        url_rel = f"{settings.MUSICBRAINZ_API_URL}/release/{release_id}"
        params_rel = {"inc": "recordings", "fmt": "json"}
        
        response_rel = await make_async_request_with_retries(
            context=context,
            url=url_rel,
            method="GET",
            params=params_rel,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client,
            rate_limit_delay=settings.MUSICBRAINZ_RATE_LIMIT_DELAY
        )
        data_rel = response_rel.json()
        
        tracks = []
        # MusicBrainz structure: release -> media (list) -> tracks (list)
        for medium in data_rel.get("media", []):
            for track in medium.get("tracks", []):
                recording = track.get("recording", {})
                if not recording:
                    continue
                    
                tracks.append({
                    "id": recording["id"],
                    "title": recording["title"],
                    "length": recording.get("length") # in milliseconds
                })
        
        # 4. Cache and Return
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(tracks, f, indent=2, ensure_ascii=False)
            
        return tracks

    except Exception as e:
        context.log.error(f"Error fetching tracks for Release Group {release_group_mbid}: {e}")
        return []
