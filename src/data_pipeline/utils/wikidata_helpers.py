# -----------------------------------------------------------
# Wikidata API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Wikidata helpers
"""
import asyncio
import json
from pathlib import Path
from typing import Any, Callable, Optional, Dict, List

import httpx
import msgspec
from dagster import AssetExecutionContext

from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
    run_tasks_concurrently,
)
from data_pipeline.utils.io_helpers import JSONLWriter

WIKIDATA_CACHE_DIR = settings.wikidata_cache_dirpath


def execute_sparql_extraction(
    context: AssetExecutionContext,
    output_path: Path,
    get_query_function: Callable[..., str],
    record_processor: Callable[[dict[str, Any]], Optional[dict[str, Any]]],
    label: str,
    **query_params: Any,
) -> None:
    """
    Orchestrates fetching, processing, and saving data from a SPARQL endpoint.
    """
    asyncio.run(
        _run_extraction_pipeline(
            context=context,
            output_path=output_path,
            get_query_function=get_query_function,
            record_processor=record_processor,
            label=label,
            **query_params,
        )
    )


async def _run_extraction_pipeline(
    context: AssetExecutionContext,
    output_path: Path,
    get_query_function: Callable[..., str],
    record_processor: Callable[[dict[str, Any]], Optional[dict[str, Any]]],
    label: str,
    **query_params: Any,
) -> None:
    """
    Internal async function to run the extraction pipeline using concurrent pagination.
    """
    batch_size = settings.WIKIDATA_SPARQL_BATCH_SIZE
    concurrency_limit = settings.WIKIDATA_CONCURRENT_REQUESTS
    current_offset = 0
    total_written = 0
    has_more_data = True

    context.log.info(f"Starting extraction for {label}...")

    with JSONLWriter(output_path) as writer:
        while has_more_data:
            offsets_batch = [
                current_offset + (i * batch_size) for i in range(concurrency_limit)
            ]

            async def process_offset(offset: int) -> list[dict[str, Any]]:
                query = get_query_function(
                    **query_params, limit=batch_size, offset=offset
                )
                return await fetch_sparql_query_async(context, query)

            results_batches = await run_tasks_concurrently(
                items=offsets_batch,
                processor=process_offset,
                concurrency_limit=concurrency_limit,
                description=f"Fetching {label} (offsets {offsets_batch[0]}..{offsets_batch[-1]})",
            )

            batch_has_partial_page = False
            for results in results_batches:
                if len(results) < batch_size:
                    batch_has_partial_page = True

                for item in results:
                    processed_record = record_processor(item)
                    if processed_record:
                        writer.write(processed_record)
                        total_written += 1
            
            if batch_has_partial_page:
                has_more_data = False
            else:
                current_offset += (batch_size * concurrency_limit)

    context.log.info(f"Total records stored in {output_path.name}: {total_written}")


async def fetch_sparql_query_async(
    context: AssetExecutionContext,
    query: str,
    client: Optional[httpx.AsyncClient] = None,
) -> list[dict[str, Any]]:
    """
    Executes a SPARQL query against the Wikidata endpoint with retries asynchronously.
    """
    try:
        response = await make_async_request_with_retries(
            context=context,
            url=settings.WIKIDATA_SPARQL_ENDPOINT,
            method="POST",
            params={"query": query, "format": "json"},
            headers=settings.default_request_headers,
            client=client,
        )
        data = msgspec.json.decode(response.content)
        return data.get("results", {}).get("bindings", [])
    except httpx.HTTPError as e:
        context.log.error(f"An unrecoverable error occurred during SPARQL query: {e}")
        raise
    except msgspec.DecodeError as e:
        context.log.error(f"Error decoding JSON response from SPARQL query: {e}")
        raise


def get_sparql_binding_value(data: dict[str, Any], key: str) -> Any:
    return data.get(key, {}).get("value")


# --- Entity Fetching Helpers ---


async def async_fetch_wikidata_entities_batch(
    context: AssetExecutionContext,
    qids: List[str],
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Any]:
    """
    Fetches entity data for a batch of QIDs from the Wikidata API (wbgetentities).
    Implements a local file cache (one JSON per QID).
    """
    if not qids:
        return {}

    combined_entities = {}
    to_fetch = []

    # 1. Check Cache
    async def check_cache(qid: str) -> Optional[Dict[str, Any]]:
        cache_file = WIKIDATA_CACHE_DIR / f"{qid}.json"
        if await asyncio.to_thread(cache_file.exists):
            try:
                def read_json():
                    with open(cache_file, "rb") as f:
                        return msgspec.json.decode(f.read())
                data = await asyncio.to_thread(read_json)
                
                # REFETCH IF SITELINKS ARE MISSING (Stale Cache)
                if data and "sitelinks" in data:
                    return data
                else:
                    context.log.debug(f"Cache for {qid} is stale (missing sitelinks). Refetching.")
            except Exception as e:
                context.log.warning(f"Failed to read cache for {qid}: {e}")
        return None

    # Check cache for all QIDs concurrently
    cache_results = await asyncio.gather(*[check_cache(qid) for qid in qids])
    
    for qid, cached_data in zip(qids, cache_results):
        if cached_data:
            combined_entities[qid] = cached_data
        else:
            to_fetch.append(qid)

    if not to_fetch:
        return combined_entities

    context.log.info(f"Fetching {len(to_fetch)} entities from Wikidata API...")

    # 2. Fetch missing from API
    chunk_size = settings.WIKIDATA_ACTION_BATCH_SIZE
    chunks = [to_fetch[i : i + chunk_size] for i in range(0, len(to_fetch), chunk_size)]

    async def fetch_and_cache_chunk(chunk: List[str]) -> Dict[str, Any]:
        params = {
            "action": "wbgetentities",
            "ids": "|".join(chunk),
            "format": "json",
            "props": "claims|labels|aliases|descriptions|sitelinks",
            "languages": "en",
        }
        url = settings.WIKIDATA_ACTION_API_URL
        
        try:
            if settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY > 0:
                await asyncio.sleep(settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY)

            response = await make_async_request_with_retries(
                context=context,
                url=url,
                method="GET",
                params=params,
                headers=settings.default_request_headers,
                timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
                client=client,
            )
            data = response.json()
            entities = data.get("entities", {})
            
            # Cache individual entities
            for qid, entity_data in entities.items():
                if "missing" in entity_data:
                    continue
                
                cache_file = WIKIDATA_CACHE_DIR / f"{qid}.json"

                def save_json(d, p):
                    p.parent.mkdir(parents=True, exist_ok=True)
                    with open(p, "wb") as f:
                        f.write(msgspec.json.encode(d))
                
                await asyncio.to_thread(save_json, entity_data, cache_file)
            
            return entities
        except Exception as e:
            context.log.warning(f"Failed to fetch Wikidata entities batch: {e}")
            return {}

    results = await run_tasks_concurrently(
        items=chunks,
        processor=fetch_and_cache_chunk,
        concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
        description="Fetching & Caching Wikidata Entities",
    )

    for res in results:
        combined_entities.update(res)

    return combined_entities


async def async_resolve_qids_to_labels(
    context: AssetExecutionContext,
    qids: List[str],
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, str]:
    """
    Resolves a list of QIDs to their English labels.
    """
    entities = await async_fetch_wikidata_entities_batch(context, qids, client)
    labels = {}
    for qid, data in entities.items():
        label = extract_wikidata_label(data)
        if label:
            labels[qid] = label
    return labels


def extract_wikidata_label(entity_data: Dict[str, Any], lang: str = "en") -> Optional[str]:
    """Extracts the label for a given language."""
    return entity_data.get("labels", {}).get(lang, {}).get("value")


def extract_wikidata_aliases(entity_data: Dict[str, Any], lang: str = "en") -> List[str]:
    """Extracts aliases for a given language."""
    aliases = entity_data.get("aliases", {}).get(lang, [])
    return [a["value"] for a in aliases]


def extract_wikidata_wikipedia_url(entity_data: Dict[str, Any], lang: str = "en") -> Optional[str]:
    """
    Extracts the Wikipedia URL for a given language (e.g., 'enwiki').
    """
    wiki_key = f"{lang}wiki"
    sitelinks = entity_data.get("sitelinks", {})
    if wiki_key in sitelinks:
        title = sitelinks[wiki_key].get("title")
        if title:
            # Replace spaces with underscores for the URL
            encoded_title = title.replace(" ", "_")
            return f"https://{lang}.wikipedia.org/wiki/{encoded_title}"
    return None


def extract_wikidata_claim_value(
    entity_data: Dict[str, Any], property_id: str
) -> Optional[Any]:
    """
    Extracts the first claim value for a property (e.g., 'P495').
    Handles 'wikibase-entityid' (returns ID) and simple strings.
    """
    claims = entity_data.get("claims", {})
    if property_id not in claims:
        return None
    
    claim = claims[property_id][0]
    mainsnak = claim.get("mainsnak", {})
    
    if mainsnak.get("snaktype") != "value":
        return None
        
    datavalue = mainsnak.get("datavalue", {})
    dtype = datavalue.get("type")
    value = datavalue.get("value")

    if dtype == "wikibase-entityid":
        return value.get("id")
    
    return value


def extract_wikidata_claim_ids(
    entity_data: Dict[str, Any], property_id: str
) -> List[str]:
    """
    Extracts all claim values (IDs) for a property (e.g., 'P136' for Genres).
    """
    claims = entity_data.get("claims", {})
    if property_id not in claims:
        return []
    
    ids = []
    for claim in claims[property_id]:
        mainsnak = claim.get("mainsnak", {})
        if mainsnak.get("snaktype") == "value":
            datavalue = mainsnak.get("datavalue", {})
            if datavalue.get("type") == "wikibase-entityid":
                ids.append(datavalue.get("value", {}).get("id"))
    return [i for i in ids if i]


def extract_wikidata_country_id(entity_data: dict[str, Any]) -> str | None:
    """
    Extracts the country QID from Wikidata entity data.
    Tries P495 (Country of origin) first, then P27 (Country of citizenship).
    """
    return extract_wikidata_claim_value(
        entity_data, "P495"
    ) or extract_wikidata_claim_value(entity_data, "P27")


def extract_wikidata_genre_ids(entity_data: dict[str, Any]) -> list[str]:
    """
    Extracts genre QIDs from Wikidata entity data.
    Combines P136 (Genre) and P101 (Field of work) and returns unique sorted list.
    """
    ids = extract_wikidata_claim_ids(entity_data, "P136") + extract_wikidata_claim_ids(
        entity_data, "P101"
    )
    return sorted(list(set(ids)))
