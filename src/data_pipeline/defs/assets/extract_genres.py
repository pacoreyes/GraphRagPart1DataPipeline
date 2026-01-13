# -----------------------------------------------------------
# Extract Genres Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any

import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Genre
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import AsyncClient
from data_pipeline.utils.data_transformation_helpers import normalize_and_clean_text
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    extract_wikidata_aliases,
    extract_wikidata_claim_ids,
    extract_wikidata_label,
)
from data_pipeline.defs.resources import WikidataResource


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
    Returns a Polars LazyFrame.
    """
    context.log.info("Starting genre extraction from artists.")

    # 1. ID Extraction
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
        return pl.DataFrame(schema=msgspec.to_builtins(Genre)).lazy()

    # 2. Worker function
    async def process_batch(
        id_chunk: list[str], client: AsyncClient
    ) -> list[dict[str, Any]]:
        # A. Fetch Metadata
        entity_data_map = await async_fetch_wikidata_entities_batch(
            context, 
            id_chunk, 
            api_url=settings.WIKIDATA_ACTION_API_URL,
            cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
            languages=settings.WIKIDATA_FALLBACK_LANGUAGES,
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client
        )
        
        batch_results = []
        for genre_id in id_chunk:
            genre_entity = entity_data_map.get(genre_id)
            if not genre_entity:
                continue

            label = extract_wikidata_label(
                genre_entity, languages=settings.WIKIDATA_FALLBACK_LANGUAGES
            )
            if not label:
                continue

            label = normalize_and_clean_text(label)
            aliases = [
                normalize_and_clean_text(a) 
                for a in extract_wikidata_aliases(
                    genre_entity, languages=settings.WIKIDATA_FALLBACK_LANGUAGES
                )
            ]
            # P279 is "subclass of", which represents the parent genre
            parent_ids = extract_wikidata_claim_ids(genre_entity, "P279")
            
            batch_results.append(
                msgspec.to_builtins(
                    Genre(
                        id=genre_id, 
                        name=label, 
                        aliases=aliases,
                        parent_ids=parent_ids,
                    )
                )
            )

        return batch_results

    # 3. Processing
    batch_size = settings.WIKIDATA_ACTION_BATCH_SIZE
    total_genres = len(unique_genre_ids)
    all_genres = []
    
    async with wikidata.get_client(context) as client:
        for i in range(0, total_genres, batch_size):
            chunk = unique_genre_ids[i: i + batch_size]
            context.log.info(f"Processing genre batch {i}/{total_genres}")
            
            batch_data = await process_batch(chunk, client)
            all_genres.extend(batch_data)
        
    return pl.DataFrame(all_genres).lazy()
