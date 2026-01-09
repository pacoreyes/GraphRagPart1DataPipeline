# -----------------------------------------------------------
# Extract Artists Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any, Optional

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Artist
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import deduplicate_stream
from data_pipeline.utils.network_helpers import (
    run_tasks_concurrently,
    yield_batches_concurrently,
)
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    async_resolve_qids_to_labels,
    extract_wikidata_aliases,
    extract_wikidata_claim_value,
    extract_wikidata_claim_ids,
    extract_wikidata_wikipedia_url,
)
from data_pipeline.utils.lastfm_helpers import async_fetch_lastfm_data_with_cache
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text
from data_pipeline.defs.resources import WikidataResource, LastFmResource

# --- Music Domain Constants ---
WIKIDATA_PROP_COUNTRY = ["P495", "P27"]
WIKIDATA_PROP_GENRE = ["P136", "P101"]
WIKIDATA_PROP_MBID = "P434"


async def _enrich_artist_batch(
    artist_batch: list[dict[str, Any]],
    context: AssetExecutionContext,
    lastfm: LastFmResource,
    client: httpx.AsyncClient,
) -> list[Artist]:
    """
    Enriches a batch of artists with Wikidata and Last.fm data.
    Contains Music-Domain specific mapping logic.
    """
    qids_map = {}
    clean_qids = []
    for artist in artist_batch:
        uri = artist.get("artist_uri", "")
        qid = uri.split("/")[-1] if "/" in uri else uri
        qids_map[uri] = qid
        clean_qids.append(qid)

    # 1. Fetch Wikidata entities
    wikidata_entities = await async_fetch_wikidata_entities_batch(
        context, clean_qids, client=client
    )

    # 2. Collect metadata and resolve countries
    qids_to_resolve = set()
    artist_metadata_map = {}

    for qid in clean_qids:
        info = wikidata_entities.get(qid, {})
        
        wiki_url = extract_wikidata_wikipedia_url(info)
        if not wiki_url:
            continue

        # Country logic
        country_qid = None
        for prop in WIKIDATA_PROP_COUNTRY:
            country_qid = extract_wikidata_claim_value(info, prop)
            if country_qid:
                qids_to_resolve.add(country_qid)
                break

        # Genre logic
        genre_qids = []
        for prop in WIKIDATA_PROP_GENRE:
            genre_qids.extend(extract_wikidata_claim_ids(info, prop))
        genre_qids = sorted(list(set(genre_qids)))

        artist_metadata_map[qid] = {
            "country_qid": country_qid,
            "genre_qids": genre_qids,
        }

    # 3. Resolve Labels for Countries
    labels_map = await async_resolve_qids_to_labels(
        context, list(qids_to_resolve), client=client
    )

    enriched_artists = []

    # 4. Worker function for Last.fm and Model construction
    async def process_single_artist(artist_record: dict[str, Any]) -> Optional[Artist]:
        qid = qids_map.get(artist_record.get("artist_uri", ""))
        name = artist_record.get("name", "")
        if not qid or not name:
            return None

        meta = artist_metadata_map.get(qid)
        if not meta:
            return None

        wikidata_info = wikidata_entities.get(qid, {})
        
        # Country
        country_label = labels_map.get(meta.get("country_qid"))
        if country_label:
            country_label = normalize_and_clean_text(country_label)

        # Aliases
        aliases = [normalize_and_clean_text(a) for a in extract_wikidata_aliases(wikidata_info)]

        # MBID
        mbid = extract_wikidata_claim_value(wikidata_info, WIKIDATA_PROP_MBID)

        # Last.fm Strategy (Business Logic)
        lastfm_data = None
        
        # Priority 1: MBID
        if mbid:
            lastfm_data = await async_fetch_lastfm_data_with_cache(
                context, {
                    "method": "artist.getInfo",
                    "mbid": mbid,
                    "autocorrect": 1
                },
                mbid,
                api_key=lastfm.api_key,
                client=client
            )
        
        # Priority 2: Name
        if not lastfm_data or "error" in lastfm_data:
            lastfm_data = await async_fetch_lastfm_data_with_cache(
                context, {
                    "method": "artist.getInfo",
                    "artist": name,
                    "autocorrect": 1
                },
                name,
                api_key=lastfm.api_key,
                client=client
            )
            
        # Priority 3: Aliases
        if (not lastfm_data or "error" in lastfm_data) and aliases:
            for alias in aliases:
                lastfm_data = await async_fetch_lastfm_data_with_cache(
                    context, {
                        "method": "artist.getInfo",
                        "artist": alias,
                        "autocorrect": 1
                    },
                    alias,
                    api_key=lastfm.api_key,
                    client=client
                )
                if lastfm_data and "error" not in lastfm_data:
                    break

        tags = []
        similar_artists = []
        if lastfm_data and "artist" in lastfm_data:
            # Domain-specific parsing of Last.fm response
            artist_data = lastfm_data.get("artist") or {}
            
            # Tags
            raw_tags = (artist_data.get("tags") or {}).get("tag") or []
            if isinstance(raw_tags, dict):
                raw_tags = [raw_tags]
            tags = [t["name"] for t in raw_tags if isinstance(t, dict) and "name" in t]
            
            # Similar
            raw_sim = (artist_data.get("similar") or {}).get("artist") or []
            if isinstance(raw_sim, dict):
                raw_sim = [raw_sim]
            similar_artists = [s["name"] for s in raw_sim if isinstance(s, dict) and "name" in s]

        return Artist(
            id=qid,
            name=name,
            aliases=aliases if aliases else None,
            country=country_label if country_label else None,
            genres=meta.get("genre_qids") if meta.get("genre_qids") else None,
            tags=tags if tags else None,
            similar_artists=similar_artists if similar_artists else None,
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
    name="artists",
    description="Enrich artists with Wikidata and Last.fm data.",
)
async def extract_artists(
    context: AssetExecutionContext, 
    wikidata: WikidataResource, 
    lastfm: LastFmResource,
    artist_index: pl.DataFrame
) -> pl.DataFrame:
    """
    Enriches all artists from the merged index.
    """
    context.log.info("Starting artist enrichment for full index.")

    # Use input DataFrame directly
    artists_list = artist_index.to_dicts()

    # Wrap the processor to match signature
    async def process_batch_wrapper(
        batch: list[dict[str, Any]], client: httpx.AsyncClient
    ) -> list[Artist]:
        return await _enrich_artist_batch(batch, context, lastfm, client)

    # Process batches concurrently
    async with wikidata.yield_for_execution(context) as client:
        artist_stream = yield_batches_concurrently(
            items=artists_list,
            batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
            processor_fn=process_batch_wrapper,
            concurrency_limit=1,
            description="Processing Artist Batches",
            timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
            client=client,
        )

        # Collect results
        context.log.info("Collecting enriched artists.")
        enriched_artists_dicts = [
            msgspec.to_builtins(a) 
            async for a in deduplicate_stream(artist_stream, key_attr="id")
        ]

    context.log.info(f"Enriched {len(enriched_artists_dicts)} artists.")
    
    context.add_output_metadata({
        "artist_count": len(enriched_artists_dicts)
    })
    
    return pl.DataFrame(enriched_artists_dicts)
