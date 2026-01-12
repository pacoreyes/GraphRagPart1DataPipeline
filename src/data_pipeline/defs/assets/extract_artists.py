# -----------------------------------------------------------
# Extract Artists Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import re
from typing import Any, Optional

import httpx
import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Artist
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import (
    run_tasks_concurrently,
)
from data_pipeline.utils.io_helpers import async_append_jsonl, async_clear_file
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

# Latin script unicode ranges:
# Basic Latin: \u0000-\u007F (Includes ASCII punctuation/digits)
# Latin-1 Supplement: \u0080-\u00FF
# Latin Extended-A: \u0100-\u017F
# Latin Extended-B: \u0180-\u024F
# Latin Extended Additional: \u1E00-\u1EFF
LATIN_REGEX = re.compile(r"^[\u0000-\u007F\u0080-\u00FF\u0100-\u017F\u0180-\u024F\u1E00-\u1EFF]*$")


def _is_latin_name(name: str) -> bool:
    """
    Checks if the name consists only of Latin characters, numbers, and common symbols.
    """
    if not name:
        return False
    return bool(LATIN_REGEX.match(name))


def _validate_artist_data(wikidata_info: dict[str, Any], country_label: Optional[str]) -> Optional[str]:
    """
    Centralized validation logic for an artist.
    Returns the MBID if valid (Latin Check assumed passed upstream), else None.
    
    Criteria:
    1. Must have English Wikipedia Article.
    2. Must have MusicBrainz ID (MBID).
    3. Must have a resolved Country Label.
    """
    # 1. Wikipedia Check
    if not extract_wikidata_wikipedia_url(wikidata_info):
        return None

    # 2. MBID Check
    mbid = extract_wikidata_claim_value(wikidata_info, WIKIDATA_PROP_MBID)
    if not mbid:
        return None

    # 3. Country Check
    if not country_label:
        return None

    return mbid


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
        context,
        clean_qids,
        api_url=settings.WIKIDATA_ACTION_API_URL,
        cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
        timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
        rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
        headers=settings.DEFAULT_REQUEST_HEADERS,
        client=client
    )

    # 2. Collect metadata and resolve countries
    # Note: We optimistically collect metadata for all items here.
    # Validation happens in the worker to keep logic centralized.
    qids_to_resolve = set()
    artist_metadata_map = {}

    for qid in clean_qids:
        info = wikidata_entities.get(qid, {})
        
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
        context,
        list(qids_to_resolve),
        api_url=settings.WIKIDATA_ACTION_API_URL,
        cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
        timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
        rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
        headers=settings.DEFAULT_REQUEST_HEADERS,
        client=client
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

        # Resolve Country Label
        country_label = labels_map.get(meta.get("country_qid"))
        if country_label:
            country_label = normalize_and_clean_text(country_label)

        # Centralized Validation
        mbid = _validate_artist_data(wikidata_info, country_label)
        if not mbid:
            return None

        # Aliases
        aliases = [normalize_and_clean_text(a) for a in extract_wikidata_aliases(wikidata_info)]

        # Last.fm Strategy (Business Logic)
        lastfm_data = await async_fetch_lastfm_data_with_cache(
            context, {
                "method": "artist.getInfo",
                "mbid": mbid,
                "autocorrect": 1
            },
            mbid,
            api_key=lastfm.api_key,
            api_url=settings.LASTFM_API_URL,
            cache_dir=settings.LAST_FM_CACHE_DIRPATH,
            timeout=settings.LASTFM_REQUEST_TIMEOUT,
            rate_limit_delay=settings.LASTFM_RATE_LIMIT_DELAY,
            client=client
        )

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
            mbid=mbid,
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
    artist_index: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Enriches all artists from the merged index.
    Returns a Polars LazyFrame backed by a temporary JSONL file.
    """
    context.log.info("Starting artist enrichment for full index.")

    # Temp file for streaming results
    temp_file = settings.DATASETS_DIRPATH / ".temp" / "artists.jsonl"
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    await async_clear_file(temp_file)

    # 1. Get Total Count for Loop
    # We collect only the count, which is O(1) memory
    total_rows = artist_index.select(pl.len()).collect().item()
    context.log.info(f"Total artists to process: {total_rows}")

    if total_rows == 0:
        return pl.LazyFrame()

    batch_size = settings.WIKIDATA_ACTION_BATCH_SIZE

    async with wikidata.get_client(context) as client:
        # 2. Iterate Batches using Slicing
        for offset in range(0, total_rows, batch_size):
            context.log.info(f"Processing batch offset {offset}/{total_rows}")

            # Efficiently fetch only the current batch from source
            batch_df = artist_index.slice(offset, batch_size).collect()
            batch_items = batch_df.to_dicts()

            # Filter non-Latin names
            filtered_items = [
                item for item in batch_items
                if _is_latin_name(item.get("name", ""))
            ]
            dropped_count = len(batch_items) - len(filtered_items)
            if dropped_count > 0:
                context.log.info(
                    f"Dropped {dropped_count} non-Latin artists from batch {offset}"
                )

            if not filtered_items:
                continue

            # Enrich Batch
            enriched_batch = await _enrich_artist_batch(
                filtered_items, context, lastfm, client
            )

            # Filter metadata drops
            metadata_dropped_count = len(filtered_items) - len(enriched_batch)
            if metadata_dropped_count > 0:
                context.log.info(
                    f"Dropped {metadata_dropped_count} artists with missing metadata from batch {offset}"
                )

            # Write Batch to Disk (Stream)
            await async_append_jsonl(temp_file, enriched_batch)

    context.log.info(f"Enriched artists saved to {temp_file}")

    # Return LazyFrame pointing to the streamed file
    # Ensure we use scan_ndjson as we wrote JSONL
    return pl.scan_ndjson(str(temp_file))
