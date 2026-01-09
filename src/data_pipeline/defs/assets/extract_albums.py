# -----------------------------------------------------------
# Extract Albums Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Album
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import deduplicate_stream
from data_pipeline.utils.network_helpers import yield_batches_concurrently
from data_pipeline.utils.wikidata_helpers import (
    fetch_sparql_query_async,
    get_sparql_binding_value,
)
from data_pipeline.utils.transformation_helpers import normalize_and_clean_text
from data_pipeline.defs.resources import WikidataResource


def get_albums_by_artists_batch_query(artist_qids: list[str]) -> str:
    """
    Builds a SPARQL query to fetch albums for multiple artists in one request.

    Args:
        artist_qids: List of Wikidata QIDs.

    Returns:
        A SPARQL query string.
    """
    values = " ".join([f"wd:{qid}" for qid in artist_qids])
    return f"""
    SELECT ?album ?albumLabel ?releaseDate ?artist ?genre WHERE {{
      VALUES ?artist {{ {values} }}
      ?album wdt:P175 ?artist.
      
      # Exclude non-standard releases
      FILTER NOT EXISTS {{ ?album wdt:P31 wd:Q134556. }}   # exclude singles
      FILTER NOT EXISTS {{ ?album wdt:P7937 wd:Q222910. }}  # exclude compilations
      FILTER NOT EXISTS {{ ?album wdt:P7937 wd:Q209939. }}  # exclude live albums
      FILTER NOT EXISTS {{ ?album wdt:P31 wd:Q10590726. }}  # exclude video albums
      
      OPTIONAL {{ ?album wdt:P577 ?releaseDate. }}
      OPTIONAL {{ ?album wdt:P136 ?genre. }}
      
      # Label Fallback: English -> Any
      OPTIONAL {{ ?album rdfs:label ?enLabel . FILTER(LANG(?enLabel) = "en") }}
      OPTIONAL {{ ?album rdfs:label ?anyLabel . }}
      BIND(COALESCE(?enLabel, ?anyLabel) AS ?albumLabel)
    }}
    """


@asset(
    name="albums",
    description="Extract Albums dataset from the Artist list using Wikidata SPARQL.",
)
async def extract_albums(
    context: AssetExecutionContext, 
    wikidata: WikidataResource, 
    artists: pl.DataFrame
) -> pl.DataFrame:
    """
    Retrieves all albums for each artist in the artists dataset from Wikidata.
    Returns a Polars DataFrame.
    """
    context.log.info("Starting album extraction from artists dataset.")

    # 1. Get QIDs from input DataFrame
    artist_qids = artists["id"].to_list()

    if not artist_qids:
        context.log.warning("No artists found. Returning empty DataFrame.")
        return pl.DataFrame()

    context.log.info(f"Fetching albums for {len(artist_qids)} artists.")

    # 2. Define worker function for batch processing
    async def process_batch(
        qid_chunk: list[str], client: httpx.AsyncClient
    ) -> list[Album]:
        query = get_albums_by_artists_batch_query(qid_chunk)
        results = await fetch_sparql_query_async(context, query, client=client)

        # Temporary storage to handle multiple release dates (pick earliest) and aggregate aliases
        # key: (artist_id, album_id) -> {title, year, genres}
        album_map: dict[tuple[str, str], dict[str, Any]] = {}

        for row in results:
            album_uri = get_sparql_binding_value(row, "album")
            artist_uri = get_sparql_binding_value(row, "artist")
            title = get_sparql_binding_value(row, "albumLabel")
            date_str = get_sparql_binding_value(row, "releaseDate")
            genre_uri = get_sparql_binding_value(row, "genre")

            if not all([album_uri, artist_uri, title]):
                continue

            title = normalize_and_clean_text(title)
            album_id = album_uri.split("/")[-1]
            artist_id = artist_uri.split("/")[-1]

            year = None
            if date_str:
                try:
                    # Wikidata dates are ISO format strings (often YYYY-MM-DD...)
                    year = int(date_str[:4])
                except (ValueError, TypeError):
                    pass

            key = (artist_id, album_id)
            if key not in album_map:
                album_map[key] = {"title": title, "year": year, "genres": set()}
            else:
                # Keep the earliest year if multiple found for SAME album ID
                existing_year = album_map[key]["year"]
                if year is not None:
                    if existing_year is None or year < existing_year:
                        album_map[key]["year"] = year

            if genre_uri:
                genre_id = genre_uri.split("/")[-1]
                album_map[key]["genres"].add(genre_id)

        # Deduplicate by Title per Artist (Keep earliest year)
        # key: (artist_id, title) -> Album
        final_map = {}
        
        for (artist_id, aid), data in album_map.items():
            title_key = (artist_id, data["title"])
            year = data["year"]
            
            new_album = Album(
                id=aid,
                title=data["title"],
                year=year,
                artist_id=artist_id,
                genres=list(data["genres"]) if data["genres"] else None,
            )
            
            if title_key not in final_map:
                final_map[title_key] = new_album
            else:
                existing = final_map[title_key]
                # Compare years: prefer min year
                if year is not None:
                    if existing.year is None or year < existing.year:
                        final_map[title_key] = new_album

        return list(final_map.values())

    # 3. Stream processing
    async with wikidata.yield_for_execution(context) as client:
        album_stream = yield_batches_concurrently(
            items=artist_qids,
            batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
            processor_fn=process_batch,
            concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
            description="Fetching albums",
            timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
            client=client,
        )

        # 4. Collect results (Deduplicate globally by Title + Artist)
        context.log.info("Collecting and deduplicating albums.")
        albums_list = [
            msgspec.to_builtins(album) 
            async for album in deduplicate_stream(album_stream, key_attr=["title", "artist_id"])
        ]

    context.log.info(f"Successfully fetched {len(albums_list)} albums.")
    
    context.add_output_metadata({
        "album_count": len(albums_list),
        "artist_count": len(artist_qids)
    })
    
    return pl.DataFrame(albums_list)
