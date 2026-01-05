# -----------------------------------------------------------
# Extract Genres Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from collections import defaultdict

import httpx
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Genre
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import stream_to_jsonl
from data_pipeline.utils.network_helpers import yield_batches_concurrently
from data_pipeline.utils.transformation_helpers import extract_unique_ids_from_column, normalize_and_clean_text
from data_pipeline.utils.sparql_queries import get_genre_parents_batch_query
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    extract_wikidata_aliases,
    extract_wikidata_label,
    fetch_sparql_query_async,
    get_sparql_binding_value,
)


@asset(
    name="extract_genres",
    deps=["extract_tracks"],
    description="Extract Genres dataset genres.jsonl from artists, albums and tracks",
)
async def extract_genres(context: AssetExecutionContext) -> Path:
    """
    Extracts all unique music genre IDs from artists, albums, and tracks,
    fetches their English labels, aliases, and parents from Wikidata,
    and saves the results to a JSONL file.
    """
    context.log.info("Starting genre extraction from artists, albums, and tracks.")

    # 1. Read datasets and extract unique genre IDs
    genre_ids = set()
    schema_overrides = {"genres": pl.List(pl.String)}

    for filepath in [settings.artists_filepath, settings.albums_filepath, settings.tracks_filepath]:
        try:
            if filepath.exists():
                df = pl.read_ndjson(filepath, schema_overrides=schema_overrides)
                genre_ids.update(extract_unique_ids_from_column(df, "genres"))
        except Exception as e:
            context.log.warning(f"Could not read {filepath}: {e}")

    unique_genre_ids = sorted(list(genre_ids))
    context.log.info(f"Found {len(unique_genre_ids)} unique genre IDs.")

    if not unique_genre_ids:
        context.log.warning("No genre IDs found. Creating empty file.")
        await stream_to_jsonl([], settings.genres_filepath)
        return settings.genres_filepath

    # 2. Define combined worker function
    async def process_batch_combined(
        id_chunk: list[str], client: httpx.AsyncClient
    ) -> list[Genre]:
        # A. Fetch Metadata
        entity_data_map = await async_fetch_wikidata_entities_batch(
            context, id_chunk, client=client
        )
        
        # B. Fetch Parents
        query = get_genre_parents_batch_query(id_chunk)
        sparql_results = await fetch_sparql_query_async(context, query, client=client)
        
        parents_map = defaultdict(list)
        for row in sparql_results:
            genre_uri = get_sparql_binding_value(row, "genre")
            parent_uri = get_sparql_binding_value(row, "parent")
            if genre_uri and parent_uri:
                gid = genre_uri.split("/")[-1]
                pid = parent_uri.split("/")[-1]
                parents_map[gid].append(pid)

        batch_results = []
        for genre_id in id_chunk:
            genre_entity = entity_data_map.get(genre_id)
            if not genre_entity:
                continue

            label = extract_wikidata_label(genre_entity)
            if not label:
                continue

            label = normalize_and_clean_text(label)
            aliases = [normalize_and_clean_text(a) for a in extract_wikidata_aliases(genre_entity)]
            parent_ids = parents_map.get(genre_id, [])
            
            batch_results.append(Genre(id=genre_id, name=label, aliases=aliases, parent_ids=parent_ids))

        return batch_results

    # 3. Stream processing and writing
    genre_stream = yield_batches_concurrently(
        items=unique_genre_ids,
        batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
        processor_fn=process_batch_combined,
        concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
        description="Fetching genre metadata and parents",
    )
    
    # 4. Save results
    await stream_to_jsonl(genre_stream, settings.genres_filepath)
    context.log.info(f"Successfully processed genres.")

    return settings.genres_filepath