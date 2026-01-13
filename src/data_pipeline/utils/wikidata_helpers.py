# -----------------------------------------------------------
# Wikidata API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
from pathlib import Path
from typing import Any, Callable, Optional, cast

from dagster import AssetExecutionContext

from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
    run_tasks_concurrently,
    AsyncClient,
    HTTPError,
)
from data_pipeline.utils.io_helpers import (
    async_read_json_file,
    async_write_json_file,
    decode_json,
    JSONDecodeError
)


async def run_extraction_pipeline(
    context: AssetExecutionContext,
    get_query_function: Callable[..., str],
    record_processor: Callable[[dict[str, Any]], Optional[dict[str, Any]]],
    label: str,
    sparql_endpoint: str,
    batch_size: int,
    concurrency_limit: int,
    timeout: int = 60,
    rate_limit_delay: float = 0.0,
    client: Optional[AsyncClient] = None,
    **query_params: Any,
) -> list[dict[str, Any]]:
    """
    Internal async function to run the extraction pipeline using concurrent pagination.
    Returns a list of processed records.
    """
    current_offset = 0
    all_records = []
    has_more_data = True

    context.log.info(f"Starting extraction for {label}...")

    while has_more_data:
        offsets_batch = [
            current_offset + (i * batch_size) for i in range(concurrency_limit)
        ]

        async def process_offset(offset: int) -> list[dict[str, Any]]:
            query = get_query_function(
                **query_params, limit=batch_size, offset=offset
            )
            # Fallback interpolation for literal placeholders like {start_year}
            # that might remain if the query function used double braces in an f-string.
            all_params = {**query_params, "limit": batch_size, "offset": offset}
            for key, value in all_params.items():
                placeholder = f"{{{key}}}"
                if placeholder in query:
                    query = query.replace(placeholder, str(value))

            return await _fetch_sparql_query_async(
                context,
                query,
                sparql_endpoint=sparql_endpoint,
                timeout=timeout,
                rate_limit_delay=rate_limit_delay,
                client=client
            )

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
                    all_records.append(processed_record)

        if batch_has_partial_page:
            has_more_data = False
        else:
            current_offset += (batch_size * concurrency_limit)

    context.log.info(f"Total records fetched for {label}: {len(all_records)}")
    return all_records


async def _fetch_sparql_query_async(
    context: AssetExecutionContext,
    query: str,
    sparql_endpoint: str,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 60,
    rate_limit_delay: float = 0.0,
    client: Optional[AsyncClient] = None,
) -> list[dict[str, Any]]:
    """
    Executes a SPARQL query against the Wikidata endpoint with retries asynchronously.
    """
    try:
        response = await make_async_request_with_retries(
            context=context,
            url=sparql_endpoint,
            method="POST",
            params={"query": query, "format": "json"},
            headers=headers,
            timeout=timeout,
            rate_limit_delay=rate_limit_delay,
            client=client,
        )
        data = decode_json(cast(Any, response.content))
        return (data.get("results") or {}).get("bindings") or []
    except HTTPError as e:
        if e.response is not None and e.response.status_code in [403, 429]:
            context.log.error(
                f"Wikidata API blocked the request (Status {e.response.status_code}). "
                "This usually indicates an IP ban or rate limit violation. "
                "Check 'bot-traffic@wikimedia.org' messages in the response body."
            )
        context.log.error(f"An unrecoverable error occurred during SPARQL query: {e}")
        raise
    except JSONDecodeError as e:
        context.log.error(f"Error decoding JSON response from SPARQL query: {e}")
        raise


def get_sparql_binding_value(data: dict[str, Any], key: str) -> Any:
    """Extracts a simple value from a SPARQL result binding."""
    return (data.get(key) or {}).get("value")


# --- Entity Fetching Helpers ---

async def async_fetch_wikidata_entities_batch(
    context: AssetExecutionContext,
    qids: list[str],
    api_url: str,
    cache_dir: Path,
    languages: Optional[list[str]] = None,
    timeout: int = 60,
    rate_limit_delay: float = 0.0,
    concurrency_limit: int = 5,
    action_batch_size: int = 50,
    headers: Optional[dict[str, str]] = None,
    client: Optional[AsyncClient] = None,
) -> dict[str, Any]:
    """
    Fetches entity data for a batch of QIDs from the Wikidata API (wbgetentities).
    Implements a local file cache (one JSON per QID).
    """
    if not qids:
        return {}

    combined_entities = {}
    to_fetch = []

    # 1. Check Cache
    async def check_cache(qid: str) -> Optional[dict[str, Any]]:
        cache_file = cache_dir / f"{qid}.json"
        data = await async_read_json_file(cache_file)
        if data and "sitelinks" in data:
            return data
        return None

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
    chunk_size = action_batch_size
    chunks = [to_fetch[i: i + chunk_size] for i in range(0, len(to_fetch), chunk_size)]

    async def fetch_and_cache_chunk(chunk: list[str]) -> dict[str, Any]:
        params = {
            "action": "wbgetentities",
            "ids": "|".join(chunk),
            "format": "json",
            "props": "claims|labels|aliases|descriptions|sitelinks",
            "languages": "|".join(languages) if languages else "en",
        }

        try:
            response = await make_async_request_with_retries(
                context=context,
                url=api_url,
                method="GET",
                params=params,
                headers=headers,
                timeout=timeout,
                rate_limit_delay=rate_limit_delay,
                client=client,
            )
            data = response.json()
            entities = data.get("entities", {})

            for qid, entity_data in entities.items():
                if "missing" in entity_data:
                    continue

                cache_file = cache_dir / f"{qid}.json"
                await async_write_json_file(cache_file, entity_data)

            return entities
        except Exception as e:
            context.log.warning(f"Failed to fetch Wikidata entities batch: {e}")
            return {}

    results = await run_tasks_concurrently(
        items=chunks,
        processor=fetch_and_cache_chunk,
        concurrency_limit=concurrency_limit,
        description="Fetching & Caching Wikidata Entities",
    )

    for res in results:
        combined_entities.update(res)

    return combined_entities


async def async_resolve_qids_to_labels(
    context: AssetExecutionContext,
    qids: list[str],
    api_url: str,
    cache_dir: Path,
    languages: Optional[list[str]] = None,
    timeout: int = 60,
    rate_limit_delay: float = 0.0,
    headers: Optional[dict[str, str]] = None,
    client: Optional[AsyncClient] = None,
) -> dict[str, str]:
    """Resolves a list of QIDs to their labels."""
    entities = await async_fetch_wikidata_entities_batch(
        context,
        qids,
        api_url=api_url,
        cache_dir=cache_dir,
        languages=languages,
        timeout=timeout,
        rate_limit_delay=rate_limit_delay,
        headers=headers,
        client=client
    )
    labels = {}
    for qid, data in entities.items():
        label = extract_wikidata_label(data, languages=languages)
        if label:
            labels[qid] = label
    return labels


def extract_wikidata_label(
    entity_data: dict[str, Any], 
    lang: str = "en", 
    languages: Optional[list[str]] = None
) -> Optional[str]:
    """
    Extracts the label for a given language.
    If the requested language is not found, tries the provided languages list.
    """
    labels = entity_data.get("labels") or {}

    # 1. Try requested language
    if lang in labels:
        return labels[lang].get("value")

    # 2. Try fallback languages
    if languages:
        for fallback in languages:
            if fallback in labels:
                return labels[fallback].get("value")

    return None


def extract_wikidata_aliases(
    entity_data: dict[str, Any], 
    lang: str = "en",
    languages: Optional[list[str]] = None
) -> list[str]:
    """
    Extracts aliases for a given language.
    If the requested language has no aliases, tries the provided languages list.
    """
    all_aliases = entity_data.get("aliases") or {}

    # 1. Try requested language
    if lang in all_aliases:
        return [a["value"] for a in all_aliases[lang]]

    # 2. Try fallback languages
    if languages:
        for fallback in languages:
            if fallback in all_aliases:
                return [a["value"] for a in all_aliases[fallback]]

    return []


def extract_wikidata_wikipedia_url(entity_data: dict[str, Any], lang: str = "en") -> Optional[str]:
    """Extracts the Wikipedia URL for a given language (e.g., 'enwiki')."""
    wiki_key = f"{lang}wiki"
    sitelinks = entity_data.get("sitelinks", {})
    if wiki_key in sitelinks:
        title = sitelinks[wiki_key].get("title")
        if title:
            encoded_title = title.replace(" ", "_")
            return f"https://{lang}.wikipedia.org/wiki/{encoded_title}"
    return None


def extract_wikidata_claim_value(
    entity_data: dict[str, Any], property_id: str
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
    entity_data: dict[str, Any], property_id: str
) -> list[str]:
    """Extracts all claim values (IDs) for a property."""
    claims = entity_data.get("claims", {})
    if property_id not in claims:
        return []

    ids = []
    for claim in claims[property_id]:
        mainsnak = claim.get("mainsnak", {})
        if mainsnak.get("snaktype") == "value":
            datavalue = mainsnak.get("datavalue", {})
            if datavalue.get("type") == "wikibase-entityid":
                ids.append((datavalue.get("value") or {}).get("id"))
    return [i for i in ids if i]
