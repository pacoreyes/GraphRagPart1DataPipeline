# -----------------------------------------------------------
# Extract Genres Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from collections import defaultdict

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Genre
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import yield_batches_concurrently
from data_pipeline.utils.text_transformation_helpers import extract_unique_ids_from_column, normalize_and_clean_text
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    extract_wikidata_aliases,
    extract_wikidata_label,
    fetch_sparql_query_async,
    get_sparql_binding_value,
)
from data_pipeline.defs.resources import WikidataResource


def get_genre_parents_batch_query(genre_qids: list[str]) -> str:
    """
    Builds a SPARQL query to fetch the parent genres (subclass of P279) 
    for a list of genre QIDs.

    Args:
        genre_qids: List of Genre Wikidata QIDs.

    Returns:
        A SPARQL query string.
    """
    values = " ".join([f"wd:{qid}" for qid in genre_qids])
    return f"""
    SELECT DISTINCT ?genre ?parent WHERE {{
      VALUES ?genre {{ {values} }}
      ?genre wdt:P279 ?parent.
    }}
    """


@asset(
    name="genres",
    description="Extract Genres dataset from artists, albums and tracks",
)
async def extract_genres(
    context: AssetExecutionContext, 
    wikidata: WikidataResource,
    artists: pl.DataFrame,
    albums: pl.DataFrame,
    tracks: pl.DataFrame
) -> pl.DataFrame:
    """
    Extracts all unique music genre IDs from artists, albums, and tracks,
    fetches their English labels, aliases, and parents from Wikidata.
    Returns a Polars DataFrame.
    """
    context.log.info("Starting genre extraction from artists, albums, and tracks.")

    # 1. Read input DataFrames and extract unique genre IDs
    genre_ids = set()
    
    for df in [artists, albums, tracks]:
        genre_ids.update(extract_unique_ids_from_column(df, "genres"))

    unique_genre_ids = sorted(list(genre_ids))
    context.log.info(f"Found {len(unique_genre_ids)} unique genre IDs.")

    if not unique_genre_ids:
        context.log.warning("No genre IDs found. Returning empty DataFrame.")
        return pl.DataFrame()

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
            
            batch_results.append(Genre(
                id=genre_id, 
                name=label, 
                aliases=aliases if aliases else None,
                parent_ids=parent_ids if parent_ids else None,
            ))

        return batch_results

    # 3. Stream processing
    async with wikidata.yield_for_execution(context) as client:
        genre_stream = yield_batches_concurrently(
            items=unique_genre_ids,
            batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
            processor_fn=process_batch_combined,
            concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
            description="Fetching genre metadata and parents",
            client=client,
        )
        
        # 4. Collect results
        context.log.info("Collecting genres.")
        genres_list = [msgspec.to_builtins(g) async for g in genre_stream]

    context.log.info(f"Successfully fetched {len(genres_list)} genres.")
    return pl.DataFrame(genres_list)
