# -----------------------------------------------------------
# Extract Genres Articles from Wikipedia
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
from typing import Any

import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Article, ArticleMetadata
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import yield_batches_concurrently, AsyncClient
from data_pipeline.utils.data_transformation_helpers import (
    normalize_and_clean_text,
    format_list_natural_language,
    create_rag_text_splitter,
)
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    async_resolve_qids_to_labels,
    extract_wikidata_wikipedia_url,
    extract_wikidata_claim_value,
)
from data_pipeline.utils.wikipedia_helpers import (
    async_fetch_wikipedia_article,
    parse_wikipedia_sections,
)
from data_pipeline.defs.resources import WikidataResource, WikipediaResource


WIKIPEDIA_EXCLUSION_HEADERS = [
    "References",
    "External links",
    "See also"
]

# Wikidata property IDs
WIKIDATA_PROP_COUNTRY_OF_ORIGIN = "P495"
WIKIDATA_PROP_INCEPTION = "P571"


def _extract_year_from_wikidata_time(time_value: Any) -> int | None:
    """
    Extracts the year from a Wikidata time value.

    Args:
        time_value: Wikidata time value dict with 'time' key like "+1988-00-00T00:00:00Z".

    Returns:
        The year as an integer, or None if parsing fails.
    """
    if not time_value or not isinstance(time_value, dict):
        return None
    time_str = time_value.get("time", "")
    if not time_str or len(time_str) < 5:
        return None
    try:
        return int(time_str[1:5])
    except (ValueError, IndexError):
        return None


@asset(
    name="genres_articles",
    description="Extract genre articles from Wikipedia, clean, split, and enrich with metadata for RAG.",
    io_manager_key="jsonl_io_manager"
)
async def extract_genres_articles(
    context: AssetExecutionContext,
    wikidata: WikidataResource,
    wikipedia: WikipediaResource,
    genres: pl.LazyFrame,
) -> list[list[Article]]:
    """
    Orchestrates the fetching, cleaning, chunking, and enrichment of genre articles
    from Wikipedia for all genres in the pipeline.

    Args:
        context: Dagster execution context for logging.
        wikidata: Wikidata resource for API access.
        wikipedia: Wikipedia resource for API access.
        genres: Polars LazyFrame containing genre data.

    Returns:
        A list of batches (lists of Article objects).
    """
    context.log.info("Loading genres from input.")

    # 1. Prepare Data
    genres_df = genres.collect().unique(subset=["id"], keep="first", maintain_order=True)
    rows_to_process = genres_df.to_dicts()
    total_rows = len(rows_to_process)
    context.log.info(f"Found {total_rows} genres to process for Wikipedia articles.")

    if total_rows == 0:
        return []

    # 2. Setup Splitter
    text_splitter = create_rag_text_splitter(
        model_name=settings.DEFAULT_EMBEDDINGS_MODEL_NAME,
        chunk_size=settings.TEXT_CHUNK_SIZE,
        chunk_overlap=settings.TEXT_CHUNK_OVERLAP,
    )

    # 3. Worker Function
    async def async_process_genre(
        genre_row: dict[str, Any],
        wiki_url: str,
        country_name: str | None,
        inception_year: int | None,
        client: AsyncClient
    ) -> list[Article]:

        genre_name = str(genre_row.get("name") or "")
        qid = str(genre_row.get("id") or "")
        aliases = genre_row.get("aliases") or []

        if not wiki_url:
            return []

        title = wiki_url.split("/")[-1]

        raw_text = await async_fetch_wikipedia_article(
            context,
            title,
            qid=qid,
            api_url=wikipedia.api_url,
            cache_dir=settings.WIKIPEDIA_CACHE_DIRPATH,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client,
            rate_limit_delay=wikipedia.rate_limit_delay
        )

        if not raw_text:
            return []

        # Build Context String: Aliases, Country (optional), Active since (optional)
        context_parts = []

        formatted_aliases = format_list_natural_language(aliases)
        if formatted_aliases:
            context_parts.append(f"Aliases: {formatted_aliases}")

        if country_name:
            context_parts.append(f"Country: {country_name}")

        if inception_year:
            context_parts.append(f"Active since: {inception_year}")

        context_str = "; ".join(context_parts)

        # Parse sections, clean, prepend context, and then chunk
        all_chunks_with_context = []
        for section in parse_wikipedia_sections(
            raw_text,
            exclusion_headers=WIKIPEDIA_EXCLUSION_HEADERS,
            min_content_length=settings.MIN_CONTENT_LENGTH,
        ):
            cleaned_content = normalize_and_clean_text(section.content)
            if not cleaned_content:
                continue

            # Construct Full Text for this section with Metadata prepended. Format:
            # "search_document: Topic: {Genre}. Context: {Metadata} | {Genre} (Section: {Section}) | {Content}"
            header_parts = [f"search_document: Topic: {title}."]
            if context_str:
                header_parts.append(f"Context: {context_str} |")
            else:
                header_parts.append("|")

            header_parts.append(f"{genre_name} (Section: {section.name}) |")
            full_header = " ".join(header_parts)

            # Combine header and content BEFORE splitting
            combined_text = f"{full_header} {cleaned_content}"

            section_chunks = text_splitter.split_text(combined_text)
            for chunk in section_chunks:
                all_chunks_with_context.append((section.name, chunk))

        total_chunks = len(all_chunks_with_context)

        results = []
        for i, (_, chunk_text) in enumerate(all_chunks_with_context):
            chunk_index = i + 1
            article_id = f"{qid}_chunk_{chunk_index}"

            meta = ArticleMetadata(
                title=title.replace("_", " "),
                name=genre_name,
                entity_type="genre",
                country=country_name,
                aliases=aliases or None,
                inception_year=inception_year,
                wikipedia_url=wiki_url,
                wikidata_uri=f"{settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX}{qid}",
                chunk_index=chunk_index,
                total_chunks=total_chunks
            )
            results.append(Article(id=article_id, metadata=meta, article=chunk_text))

        return results

    async def process_batch_wrapper(
        batch: list[dict[str, Any]],
        wikidata_client: AsyncClient,
        wikipedia_client: AsyncClient,
        sem: asyncio.Semaphore
    ) -> list[Article]:
        qids = [str(row.get("id") or "") for row in batch]

        # Fetch entity data for Wikipedia URLs and additional properties
        entities = await async_fetch_wikidata_entities_batch(
            context,
            qids,
            api_url=settings.WIKIDATA_ACTION_API_URL,
            cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
            languages=settings.WIKIDATA_FALLBACK_LANGUAGES,
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=wikidata_client
        )

        # Extract country QIDs that need resolution
        country_qids_to_resolve = set()
        genre_country_map: dict[str, str] = {}
        genre_year_map: dict[str, int] = {}

        for row in batch:
            qid = str(row.get("id") or "")
            entity_data = entities.get(qid)
            if not entity_data:
                continue

            # Extract country of origin (P495)
            country_qid = extract_wikidata_claim_value(entity_data, WIKIDATA_PROP_COUNTRY_OF_ORIGIN)
            if country_qid:
                country_qids_to_resolve.add(country_qid)
                genre_country_map[qid] = country_qid

            # Extract inception year (P571)
            inception_value = extract_wikidata_claim_value(entity_data, WIKIDATA_PROP_INCEPTION)
            year = _extract_year_from_wikidata_time(inception_value)
            if year:
                genre_year_map[qid] = year

        # Resolve country QIDs to labels
        country_labels: dict[str, str] = {}
        if country_qids_to_resolve:
            country_labels = await async_resolve_qids_to_labels(
                context,
                list(country_qids_to_resolve),
                api_url=settings.WIKIDATA_ACTION_API_URL,
                cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
                languages=settings.WIKIDATA_FALLBACK_LANGUAGES,
                timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
                rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
                headers=settings.DEFAULT_REQUEST_HEADERS,
                client=wikidata_client
            )

        async def bounded_process(
            row: dict[str, Any], url: str, country: str | None, year: int | None
        ) -> list[Article]:
            async with sem:
                return await async_process_genre(row, url, country, year, wikipedia_client)

        tasks = []
        for row in batch:
            qid = str(row.get("id") or "")
            entity_data = entities.get(qid)
            if not entity_data:
                continue
            wiki_url = extract_wikidata_wikipedia_url(entity_data)
            if wiki_url:
                # Resolve country name
                country_qid = genre_country_map.get(qid)
                country_name = country_labels.get(country_qid) if country_qid else None
                inception_year = genre_year_map.get(qid)
                tasks.append(bounded_process(row, wiki_url, country_name, inception_year))

        if not tasks:
            return []
        results_nested = await asyncio.gather(*tasks)
        return [item for sublist in results_nested for item in sublist]

    # 4. Execution Loop
    all_batches = []
    # Global semaphore shared across all batches to limit Wikipedia concurrent requests
    wikipedia_semaphore = asyncio.Semaphore(settings.WIKIPEDIA_CONCURRENT_REQUESTS)

    async with wikidata.get_client(context) as wikidata_client, wikipedia.get_client(context) as wikipedia_client:

        async def processor_with_two_clients(batch, _):
            return await process_batch_wrapper(batch, wikidata_client, wikipedia_client, wikipedia_semaphore)

        article_stream = yield_batches_concurrently(
            items=rows_to_process,
            batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
            processor_fn=processor_with_two_clients,
            concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
            description="Processing Genre Articles",
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            client=wikidata_client,
        )

        async for batch in article_stream:
            all_batches.append(batch)

    return all_batches
