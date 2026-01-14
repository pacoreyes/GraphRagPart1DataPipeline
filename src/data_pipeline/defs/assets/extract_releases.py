# -----------------------------------------------------------
# Extract Releases Asset (MusicBrainz)
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Release
from data_pipeline.settings import settings
from data_pipeline.utils.musicbrainz_helpers import (
    fetch_artist_release_groups_async,
    filter_release_groups,
    parse_release_year,
)
from data_pipeline.utils.data_transformation_helpers import normalize_and_clean_text
from data_pipeline.defs.resources import MusicBrainzResource


@asset(
    name="releases",
    description="Extract Releases (Albums/Singles) from MusicBrainz.",
)
async def extract_releases(
    context: AssetExecutionContext, 
    musicbrainz: MusicBrainzResource,
    artists: pl.LazyFrame
) -> list[Release]:
    """
    Retrieves all filtered release groups (Albums/Singles) for each artist from MusicBrainz.
    Uses the 'mbid' from the artist dataset.
    Returns a list of Release objects.
    """
    context.log.info("Starting releases extraction from MusicBrainz.")

    all_releases = []
    # 1. Collect Artist MBIDs
    artists_df = artists.select(["id", "mbid", "name"]).collect()
    
    rows = artists_df.to_dicts()
    total_artists = len(rows)

    if total_artists == 0:
        context.log.warning("No artists with MBIDs found.")
        return []

    context.log.info(f"Found {total_artists} artists with MBIDs to process.")

    # 2. Processing
    async with musicbrainz.get_client(context) as client:
        for i, row in enumerate(rows):
            artist_qid = row["id"]
            artist_mbid = row["mbid"]
            artist_name = row["name"]
            
            if i % 10 == 0:
                context.log.info(f"Processing artist {i}/{total_artists}: {artist_name}")

            # Fetch all Release Groups
            all_rgs = await fetch_artist_release_groups_async(
                context=context,
                artist_mbid=artist_mbid,
                client=client,
                cache_dirpath=settings.MUSICBRAINZ_CACHE_DIRPATH,
                api_url=musicbrainz.api_url,
                headers=settings.DEFAULT_REQUEST_HEADERS,
                rate_limit_delay=musicbrainz.rate_limit_delay
            )
            
            # Filter to Albums/Singles without secondary types
            filtered_rgs = filter_release_groups(all_rgs)

            for rg in filtered_rgs:
                rg_id = rg["id"]
                title = rg["title"]
                year = parse_release_year(rg.get("first-release-date"))

                all_releases.append(
                    Release(
                        id=rg_id,
                        title=normalize_and_clean_text(title),
                        year=year,
                        artist_id=artist_qid,
                    )
                )
    
    return all_releases
