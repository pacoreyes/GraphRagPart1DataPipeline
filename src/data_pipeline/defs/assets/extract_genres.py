# -----------------------------------------------------------
# Extract Genres Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from collections import defaultdict
from typing import Any

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Genre
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import async_append_jsonl, async_clear_file
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text
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
    description="Extract Genres dataset from the Artists dataset.",
)
async def extract_genres(
    context: AssetExecutionContext, 
    wikidata: WikidataResource,
    artists: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Extracts all unique music genre IDs from the artists dataset,
    fetches their English labels, aliases, and parents from Wikidata.
    Returns a Polars LazyFrame backed by a temporary JSONL file.
    """
    context.log.info("Starting genre extraction from artists.")

    # Temp file
    temp_file = settings.DATASETS_DIRPATH / ".temp" / "genres.jsonl"
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    await async_clear_file(temp_file)

    # 1. Lazy ID Extraction
    # Extract unique genre QIDs from the 'genres' column in the artists dataset.
    unique_genre_ids = (
        artists.select("genres")
        .explode("genres")
        .unique()
        .drop_nulls()
        .collect()
        .to_series()
        .to_list()
    )
    
    context.log.info(f"Found {len(unique_genre_ids)} unique genre IDs in artists.")

    if not unique_genre_ids:
        context.log.warning("No genre IDs found. Returning empty LazyFrame.")
        return pl.LazyFrame()

    # 2. Worker function
    async def process_batch(
        id_chunk: list[str], client: httpx.AsyncClient
    ) -> list[dict[str, Any]]:
        # A. Fetch Metadata
        entity_data_map = await async_fetch_wikidata_entities_batch(
            context, 
            id_chunk, 
            api_url=settings.WIKIDATA_ACTION_API_URL,
            cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client
        )
        
        # B. Fetch Parents
        query = get_genre_parents_batch_query(id_chunk)
        sparql_results = await fetch_sparql_query_async(
            context, 
            query, 
            sparql_endpoint=settings.WIKIDATA_SPARQL_ENDPOINT,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
            client=client
        )
        
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
            
            batch_results.append(
                msgspec.to_builtins(
                    Genre(
                        id=genre_id, 
                        name=label, 
                        aliases=aliases if aliases else None,
                        parent_ids=parent_ids if parent_ids else None,
                    )
                )
            )

        return batch_results

    # 3. Stream processing
    batch_size = settings.WIKIDATA_ACTION_BATCH_SIZE
    total_genres = len(unique_genre_ids)
    
    async with wikidata.get_client(context) as client:
        # We can iterate the list directly since we materialized IDs
        for i in range(0, total_genres, batch_size):
            chunk = unique_genre_ids[i: i + batch_size]
            context.log.info(f"Processing genre batch {i}/{total_genres}")
            
            batch_data = await process_batch(chunk, client)
            await async_append_jsonl(temp_file, batch_data)
        
    context.log.info(f"Successfully fetched genres to {temp_file}.")
    return pl.scan_ndjson(str(temp_file))
