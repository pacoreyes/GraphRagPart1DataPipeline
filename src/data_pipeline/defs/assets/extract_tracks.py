# -----------------------------------------------------------
# Extract Tracks Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from dagster import asset, AssetExecutionContext

import polars as pl
from data_pipeline.models import Track
from data_pipeline.utils.musicbrainz_helpers import (
    fetch_releases_for_group_async,
    fetch_tracks_for_release_async,
    select_best_release,
)
from data_pipeline.utils.data_transformation_helpers import normalize_and_clean_text
from data_pipeline.defs.resources import MusicBrainzResource


@asset(
    name="tracks",
    description="Extract Tracks dataset from the Releases list using MusicBrainz API.",
)
async def extract_tracks(
    context: AssetExecutionContext, 
    musicbrainz: MusicBrainzResource,
    releases: pl.LazyFrame
) -> list[Track]:
    """
    Retrieves all tracks for each release (Release Group) in the releases dataset from MusicBrainz.
    Strategy:
    1. Fetch all releases for the Release Group.
    2. Pick the earliest "Official" release.
    3. Fetch tracks for that specific Release.
    Returns a list of Track objects.
    """
    context.log.info("Starting tracks extraction from MusicBrainz.")

    all_tracks = []
    # 1. Collect Release MBIDs
    releases_df = releases.select(["id", "title"]).collect()
    """ TEMPORARY SOLUTION, DO NOT DELETE LINE 47 """
    releases_df = releases_df.head(releases_df.height // 2)
    """ TEMPORARY SOLUTION, DO NOT DELETE LINE 47 """
    rows = releases_df.to_dicts()
    total_releases = len(rows)

    if total_releases == 0:
        context.log.warning("No releases found.")
        return []

    context.log.info(f"Found {total_releases} releases to process tracks for.")

    # 2. Processing
    async with musicbrainz.get_client(context) as client:
        for i, row in enumerate(rows, 1):
            release_group_mbid = row["id"]
            release_title = row["title"]
            
            context.log.info(f"[{i}/{total_releases}] Processing tracks for: {release_title}")

            # A. Find representative Release
            mb_releases = await fetch_releases_for_group_async(
                context=context,
                release_group_mbid=release_group_mbid,
                client=client,
                cache_dirpath=Path(musicbrainz.cache_dir),
                api_url=musicbrainz.api_url,
                headers={},
                rate_limit_delay=musicbrainz.rate_limit_delay
            )
            
            # Select best release (Official status preferred, oldest date)
            best_release = select_best_release(mb_releases)
            if not best_release:
                continue

            release_id = best_release["id"]

            # B. Fetch Tracks for the chosen Release
            mb_tracks = await fetch_tracks_for_release_async(
                context=context,
                release_mbid=release_id,
                client=client,
                cache_dirpath=Path(musicbrainz.cache_dir),
                api_url=musicbrainz.api_url,
                headers={},
                rate_limit_delay=musicbrainz.rate_limit_delay
            )
            
            for t in mb_tracks:
                all_tracks.append(
                    Track(
                        id=t["id"],
                        title=normalize_and_clean_text(t["title"]),
                        album_id=release_group_mbid,
                    )
                )

    return all_tracks
