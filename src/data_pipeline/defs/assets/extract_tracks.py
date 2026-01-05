# -----------------------------------------------------------
# Extract Tracks Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from typing import Any

import httpx
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Track
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import stream_to_jsonl, deduplicate_stream
from data_pipeline.utils.network_helpers import yield_batches_concurrently
from data_pipeline.utils.sparql_queries import get_tracks_by_albums_batch_query
from data_pipeline.utils.wikidata_helpers import (
    fetch_sparql_query_async,
    get_sparql_binding_value,
)
from data_pipeline.utils.transformation_helpers import normalize_and_clean_text


@asset(
    name="extract_tracks",
    deps=["extract_albums"],
    description="Extract Tracks dataset from the Albums list using Wikidata SPARQL.",
)
async def extract_tracks(context: AssetExecutionContext) -> Path:
    """
    Retrieves all tracks for each album in the albums dataset from Wikidata.
    """
    context.log.info("Starting track extraction from albums dataset.")

    # 1. Load albums and extract QIDs
    try:
        # We need the Album ID to query, and we'll use it to link the tracks.
        df = pl.read_ndjson(settings.albums_filepath)
        album_qids = df["id"].to_list()
    except Exception as e:
        context.log.error(f"Could not read albums file: {e}")
        # If albums file doesn't exist or is empty, we produce an empty tracks file
        await stream_to_jsonl([], settings.tracks_filepath)
        return settings.tracks_filepath

    if not album_qids:
        context.log.warning("No albums found. Creating empty tracks file.")
        await stream_to_jsonl([], settings.tracks_filepath)
        return settings.tracks_filepath

    context.log.info(f"Fetching tracks for {len(album_qids)} albums.")

    # 2. Define worker function for batch processing
    async def process_batch(
        qid_chunk: list[str], client: httpx.AsyncClient
    ) -> list[Track]:
        query = get_tracks_by_albums_batch_query(qid_chunk)
        results = await fetch_sparql_query_async(context, query, client=client)

        # Map to aggregate aliases and deduplicate within batch
        # Key: (track_id, album_id) -> {title, genres}
        track_map: dict[tuple[str, str], dict[str, Any]] = {}

        for row in results:
            track_uri = get_sparql_binding_value(row, "track")
            album_uri = get_sparql_binding_value(row, "album")
            title = get_sparql_binding_value(row, "trackLabel")
            genre_uri = get_sparql_binding_value(row, "genre")

            if not all([track_uri, album_uri, title]):
                continue

            title = normalize_and_clean_text(title)
            track_id = track_uri.split("/")[-1]
            album_id = album_uri.split("/")[-1]

            key = (track_id, album_id)
            if key not in track_map:
                track_map[key] = {
                    "title": title,
                    "genres": set()
                }

            if genre_uri:
                genre_id = genre_uri.split("/")[-1]
                track_map[key]["genres"].add(genre_id)

        return [
            Track(
                id=tid,
                title=data["title"],
                album_id=aid,
                genres=list(data["genres"]),
            )
            for (tid, aid), data in track_map.items()
        ]

    # 3. Stream processing
    track_stream = yield_batches_concurrently(
        items=album_qids,
        batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
        processor_fn=process_batch,
        concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
        description="Fetching tracks",
        timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
    )

    # 4. Save results (Deduplicate globally by ID + AlbumID)
    await stream_to_jsonl(
        deduplicate_stream(track_stream, key_attr=["id", "album_id"]),
        settings.tracks_filepath
    )

    context.log.info(
        f"Successfully saved tracks to {settings.tracks_filepath}"
    )

    return settings.tracks_filepath
