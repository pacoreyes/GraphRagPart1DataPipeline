# -----------------------------------------------------------
# Extract Releases Asset (MusicBrainz)
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

from data_pipeline.models import Release, RELEASE_SCHEMA
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import async_append_jsonl, async_clear_file
from data_pipeline.utils.musicbrainz_helpers import fetch_artist_release_groups_async
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text


@asset(
    name="releases",
    description="Extract Releases (Albums/Singles) from MusicBrainz.",
)
async def extract_releases(
    context: AssetExecutionContext, 
    artists: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Retrieves all filtered release groups (Albums/Singles) for each artist from MusicBrainz.
    Uses the 'mbid' from the artist dataset.
    Returns a Polars LazyFrame backed by a temporary JSONL file.
    """
    context.log.info("Starting releases extraction from MusicBrainz.")

    # Temp file
    assert settings.DATASETS_DIRPATH is not None
    temp_file = settings.DATASETS_DIRPATH / ".temp" / "releases.jsonl"
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    await async_clear_file(temp_file)

    # 1. Collect Artist MBIDs
    # We need 'id' (QID) to link back, and 'mbid' to query MB.
    # Filter out artists without MBID.
    artists_df = artists.select(["id", "mbid", "name"]).collect()
    
    rows = artists_df.to_dicts()
    total_artists = len(rows)

    if total_artists == 0:
        context.log.warning("No artists with MBIDs found. Returning empty releases.")
        return pl.DataFrame(schema=RELEASE_SCHEMA).lazy()

    context.log.info(f"Found {total_artists} artists with MBIDs to process.")

    # 2. Worker Function
    # We process sequentially to be safe and polite (1 req/sec).
    
    buffer = []
    processed_count = 0
    
    async with httpx.AsyncClient(timeout=settings.MUSICBRAINZ_REQUEST_TIMEOUT) as client:
        for i, row in enumerate(rows):
            artist_qid = row["id"]
            artist_mbid = row["mbid"]
            artist_name = row["name"]
            
            # Log progress every 10 artists
            if i % 10 == 0:
                context.log.info(f"Processing artist {i}/{total_artists}: {artist_name}")

            # Fetch Release Groups (Async with retries)
            rgs = await fetch_artist_release_groups_async(context, artist_mbid, client)
            
            for rg in rgs:
                rg_id = rg["id"]
                title = rg["title"]
                first_release_date = rg.get("first-release-date", "")
                
                # Parse Year
                year = None
                if first_release_date:
                    try:
                        year = int(first_release_date.split("-")[0])
                    except (ValueError, IndexError):
                        pass
                
                # Note: We are NOT calling get_release_group_details here for every album
                # because that would explode the runtime (e.g. 50k requests).
                # We rely on the browse data.
                
                album = Release(
                    id=rg_id,  # MusicBrainz ID
                    title=normalize_and_clean_text(title),
                    year=year,
                    artist_id=artist_qid,  # Link to Graph QID
                )
                buffer.append(msgspec.to_builtins(album))
                
            # Flush buffer periodically
            if len(buffer) >= 100:
                await async_append_jsonl(temp_file, buffer)
                processed_count += len(buffer)
                buffer = []
            
    # Flush remaining
    if buffer:
        await async_append_jsonl(temp_file, buffer)
        processed_count += len(buffer)

    context.log.info(f"Extraction complete. Saved {processed_count} releases to {temp_file}")
    
    if processed_count == 0:
        return pl.DataFrame(schema=release_schema).lazy()

    return pl.scan_ndjson(str(temp_file))
