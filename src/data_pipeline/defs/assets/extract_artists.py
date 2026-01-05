# -----------------------------------------------------------
# Extract Artists Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from typing import Any, Optional

import httpx
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Artist
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import stream_to_jsonl, deduplicate_stream
from data_pipeline.utils.lastfm_helpers import (
    async_get_artist_info_with_fallback,
    parse_lastfm_similar_artists,
    parse_lastfm_tags,
)
from data_pipeline.utils.network_helpers import (
    process_batches_concurrently,
    run_tasks_concurrently,
    yield_batches_concurrently,
)
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    async_resolve_qids_to_labels,
    extract_wikidata_aliases,
    extract_wikidata_claim_value,
    extract_wikidata_country_id,
    extract_wikidata_genre_ids,
    extract_wikidata_wikipedia_url,
)
from data_pipeline.utils.transformation_helpers import normalize_and_clean_text


async def _enrich_artist_batch(
    artist_batch: list[dict[str, Any]],
    context: AssetExecutionContext,
    api_key: str,
    api_url: str,
    client: httpx.AsyncClient,
) -> list[Artist]:
    """
    Enriches a batch of artists with Wikidata and Last.fm data.
    """
    qids_map = {}
    clean_qids = []
    for artist in artist_batch:
        uri = artist.get("artist_uri", "")
        if "/" in uri:
            qid = uri.split("/")[-1]
        else:
            qid = uri
        qids_map[uri] = qid
        clean_qids.append(qid)

    # 1. Fetch Wikidata entities
    wikidata_entities = await async_fetch_wikidata_entities_batch(
        context, clean_qids, client=client
    )

    # 2. Collect QIDs to resolve (Only Countries need labels, Genres use QIDs)
    qids_to_resolve = set()

    # Store temporary mappings for each artist
    artist_metadata_map = {}  # qid -> {country_qid, genre_qids}

    for qid in clean_qids:
        info = wikidata_entities.get(qid, {})

        # Country (P495 or P27)
        c_id = extract_wikidata_country_id(info)

        if c_id:
            qids_to_resolve.add(c_id)

        # Genres (P136 or P101) - Store QIDs directly
        g_ids = extract_wikidata_genre_ids(info)

        artist_metadata_map[qid] = {
            "country_qid": c_id,
            "genre_qids": g_ids,
        }

    # 3. Resolve Labels for Country QIDs
    labels_map = await async_resolve_qids_to_labels(
        context, list(qids_to_resolve), client=client
    )

    enriched_artists = []

    # 4. Process each artist (Last.fm)
    async def process_single_artist(artist_record: dict[str, Any]) -> Optional[Artist]:
        qid = qids_map.get(artist_record.get("artist_uri", ""))
        name = artist_record.get("name", "")

        if not qid or not name:
            return None

        meta = artist_metadata_map.get(qid)
        if not meta:
            # Skip artist if it didn't have a Wikipedia URL
            return None

        wikidata_info = wikidata_entities.get(qid, {})

        # Country (Label)
        country_label = labels_map.get(meta.get("country_qid"))
        if country_label:
            country_label = normalize_and_clean_text(country_label)

        # Genres (QIDs)
        genre_qids = meta.get("genre_qids", [])

        # Aliases
        aliases = [normalize_and_clean_text(a) for a in extract_wikidata_aliases(wikidata_info)]

        # MBID (P434)
        mbid = extract_wikidata_claim_value(wikidata_info, "P434")

        # Last.fm
        lastfm_data = await async_get_artist_info_with_fallback(
            context, name, aliases, mbid, api_key, api_url, client=client
        )

        tags = []
        similar_artists = []
        if lastfm_data and "artist" in lastfm_data:
            artist_data = lastfm_data["artist"]
            tags = parse_lastfm_tags(artist_data)
            similar_artists = parse_lastfm_similar_artists(artist_data)

        # Construct Model
        return Artist(
            id=qid,
            name=name,
            aliases=aliases,
            country=country_label,
            genres=genre_qids,
            tags=tags,
            similar_artists=similar_artists,
        )

    results = await run_tasks_concurrently(
        items=artist_batch,
        processor=process_single_artist,
        concurrency_limit=settings.LASTFM_CONCURRENT_REQUESTS,
        description=f"Enriching batch of {len(artist_batch)}",
    )

    for res in results:
        if res:
            enriched_artists.append(res)

    return enriched_artists


@asset(
    name="extract_artists",
    deps=["build_artist_index"],
    description="Enrich artists with Wikidata and Last.fm data.",
    required_resource_keys={"api_config"},
)
async def extract_artist(context: AssetExecutionContext) -> Path:
    """
    Loads artists from the index, enriches them, and saves to artists.jsonl.
    """
    context.log.info("Starting artist enrichment.")

    api_key = context.resources.api_config.lastfm_api_key
    api_url = settings.LASTFM_API_URL

    # Load Data
    df = pl.read_ndjson(settings.artist_index_filepath)
    artists_list = df.to_dicts()

    # Wrap the processor to match signature
    async def process_batch_wrapper(
        batch: list[dict[str, Any]], client: httpx.AsyncClient
    ) -> list[Artist]:
        return await _enrich_artist_batch(batch, context, api_key, api_url, client)

    # Process batches concurrently
    artist_stream = yield_batches_concurrently(
        items=artists_list,
        batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
        processor_fn=process_batch_wrapper,
        concurrency_limit=1,  # Sequential batches to respect Last.fm limits inside
        description="Processing Artist Batches",
        timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
    )

    # Save results
    await stream_to_jsonl(
        deduplicate_stream(artist_stream, key_attr="id"),
        settings.artists_filepath
    )

    context.log.info(f"Saved enriched artists to {settings.artists_filepath}")
    return settings.artists_filepath
