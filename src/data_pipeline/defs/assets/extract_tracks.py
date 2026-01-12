# -----------------------------------------------------------
# Extract Tracks Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Track, TRACK_SCHEMA
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import async_append_jsonl, async_clear_file
from data_pipeline.utils.musicbrainz_helpers import fetch_tracks_for_release_group_async
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text


@asset(
    name="tracks",
    description="Extract Tracks dataset from the Releases list using MusicBrainz API.",
)
async def extract_tracks(
    context: AssetExecutionContext, 
    releases: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Retrieves all tracks for each release (Release Group) in the releases dataset from MusicBrainz.
    Returns a Polars LazyFrame backed by a temporary JSONL file.
    """
    context.log.info("Starting tracks extraction from MusicBrainz.")

    # Temp file
    assert settings.DATASETS_DIRPATH is not None
    temp_file = settings.DATASETS_DIRPATH / ".temp" / "tracks.jsonl"
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    await async_clear_file(temp_file)

    # 1. Collect Release MBIDs
    # The 'id' in releases is the MBID of the Release Group.
    releases_df = releases.select(["id", "title"]).collect()
    
    rows = releases_df.to_dicts()
    total_releases = len(rows)

    if total_releases == 0:
        context.log.warning("No releases found. Returning empty tracks.")
        return pl.DataFrame(schema=TRACK_SCHEMA).lazy()

    context.log.info(f"Found {total_releases} releases to process tracks for.")

    # 2. Worker Function
    # We process sequentially to respect MB rate limits (1 req/sec).
    # Since each release group needs 2 calls (if not cached), we must be careful.
    
    buffer = []
    processed_count = 0
    
    async with httpx.AsyncClient(timeout=settings.MUSICBRAINZ_REQUEST_TIMEOUT) as client:
        for i, row in enumerate(rows):
            release_group_mbid = row["id"]
            release_title = row["title"]
            
            if i % 10 == 0:
                context.log.info(f"Processing tracks for release {i}/{total_releases}: {release_title}")

            # Fetch Tracks (Async with retries and 2-step MB logic)
            mb_tracks = await fetch_tracks_for_release_group_async(context, release_group_mbid, client)
            
            for t in mb_tracks:
                track = Track(
                    id=t["id"],
                    title=normalize_and_clean_text(t["title"]),
                    album_id=release_group_mbid,
                )
                buffer.append(msgspec.to_builtins(track))
                
            # Flush buffer periodically
            if len(buffer) >= 200:
                await async_append_jsonl(temp_file, buffer)
                processed_count += len(buffer)
                buffer = []
                
    # Flush remaining
    if buffer:
        await async_append_jsonl(temp_file, buffer)
        processed_count += len(buffer)

    context.log.info(f"Tracks extraction complete. Saved {processed_count} tracks to {temp_file}")
    
    if processed_count == 0:
        return pl.DataFrame(schema=TRACK_SCHEMA).lazy()

    return pl.scan_ndjson(str(temp_file))
