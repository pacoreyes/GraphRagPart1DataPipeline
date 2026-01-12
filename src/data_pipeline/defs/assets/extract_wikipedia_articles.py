# -----------------------------------------------------------
# Extract Wikipedia Articles Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
import re
from typing import Dict, Any, List

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

from data_pipeline.models import Article, ArticleMetadata
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import async_append_jsonl, async_clear_file
from data_pipeline.utils.network_helpers import yield_batches_concurrently
from data_pipeline.utils.text_transformation_helpers import normalize_and_clean_text
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    extract_wikidata_wikipedia_url
)
from data_pipeline.utils.wikipedia_helpers import (
    async_fetch_wikipedia_article,
)
from data_pipeline.defs.resources import WikidataResource


WIKIPEDIA_EXCLUSION_HEADERS = [
    "References",
    "External links",
    "See also"
]


@asset(
    name="wikipedia_articles",
    description="Extract Wikipedia articles, clean, split, and enrich with metadata for RAG.",
)
async def extract_wikipedia_articles(
    context: AssetExecutionContext, 
    wikidata: WikidataResource,
    artists: pl.LazyFrame,
    genres: pl.LazyFrame,
    artist_index: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Orchestrates the fetching, cleaning, chunking, and enrichment of Wikipedia articles
    for all validated artists in the pipeline.
    Returns a Polars LazyFrame backed by a temporary JSONL file.
    """
    context.log.info("Loading validated artists, genres, and artist index from inputs.")

    # 1. Prepare Mappings (Materialize LazyFrames for lookup logic)
    # We collect here because we need random access / iteration for the API logic
    genres_df = genres.collect()
    artist_index_df = artist_index.collect()
    artists_df = artists.collect()

    genres_map: Dict[str, str] = {
        str(k): str(v) 
        for k, v in zip(genres_df["id"].to_list(), genres_df["name"].to_list()) 
        if k is not None and v is not None
    }
    inception_year_map = {}
    
    def extract_qid(uri: str) -> str:
        return uri.split("/")[-1] if "/" in uri else uri
        
    uris = artist_index_df["artist_uri"].to_list()
    dates = artist_index_df["start_date"].to_list()
    
    for uri, date in zip(uris, dates):
        qid = extract_qid(uri)
        try:
            year = int(date.split("-")[0]) if date else None
            if year:
                inception_year_map[qid] = year
        except (ValueError, AttributeError):
            continue

    rows_to_process = artists_df.to_dicts()
    total_rows = len(rows_to_process)
    context.log.info(f"Found {total_rows} artists to process for Wikipedia articles.")

    # Setup Temp File for Streaming Output
    temp_file = settings.DATASETS_DIRPATH / ".temp" / "wikipedia_articles.jsonl"
    temp_file.parent.mkdir(parents=True, exist_ok=True)
    await async_clear_file(temp_file)

    # 2. Setup Splitter
    tokenizer = AutoTokenizer.from_pretrained(
        settings.DEFAULT_EMBEDDINGS_MODEL_NAME, trust_remote_code=True
    )
    text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer,
        chunk_size=2048,
        chunk_overlap=512,
        separators=["\n\n", "\n", ". ", "? ", "! ", " ", ""],
    )

    # 3. Worker Function
    async def async_process_artist(
        artist_row: Dict[str, Any],
        wiki_url: str,
        client: httpx.AsyncClient
    ) -> List[Article]:
        
        artist_name = str(artist_row.get("name") or "")
        qid = str(artist_row.get("id") or "")
        
        if not wiki_url:
            return []
            
        title = wiki_url.split("/")[-1]
        
        raw_text = await async_fetch_wikipedia_article(
            context, 
            title, 
            qid=qid, 
            api_url=settings.WIKIPEDIA_API_URL,
            cache_dir=settings.WIKIPEDIA_CACHE_DIRPATH,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client,
            rate_limit_delay=settings.WIKIPEDIA_RATE_LIMIT_DELAY
        )
        
        if not raw_text:
            return []

        # 1. Section Parsing
        # Split by headers (e.g., "== Career ==")
        # Capturing group keeps delimiters in the list
        segments = re.split(r'(^={2,}[^=]+={2,}\s*$)', raw_text, flags=re.MULTILINE)

        current_section = "Introduction"
        all_chunks_with_context = []

        for segment in segments:
            segment = segment.strip()
            if not segment:
                continue

            # Check if header
            if segment.startswith("==") and segment.endswith("=="):
                header_clean = segment.strip("=").strip()
                
                # Check for exclusion headers (References, etc.)
                # If found, stop processing the rest of the document (standard Wikipedia cleaner behavior)
                if any(ex.lower() == header_clean.lower() for ex in WIKIPEDIA_EXCLUSION_HEADERS):
                    break
                
                current_section = header_clean
            else:
                # Content Block
                # We use normalize_and_clean_text directly on the section content
                cleaned_content = normalize_and_clean_text(segment)
                
                # Skip if content is empty or too short to be semantically useful (threshold: 30 chars)
                if not cleaned_content or len(cleaned_content) < 30:
                    continue

                section_chunks = text_splitter.split_text(cleaned_content)
                for chunk in section_chunks:
                    all_chunks_with_context.append((current_section, chunk))

        total_chunks = len(all_chunks_with_context)
        genre_ids = artist_row.get("genres") or []
        genre_names = [genres_map[gid] for gid in genre_ids if gid in genres_map]
        year = inception_year_map.get(qid)
        
        results = []
        for i, (section, chunk_text) in enumerate(all_chunks_with_context):
            # Prepend Section Context
            enriched_text = f"search_document: {artist_name} (Section: {section}) | {chunk_text}"
            
            meta = ArticleMetadata(
                title=title.replace("_", " "),
                artist_name=artist_name,
                country=artist_row.get("country") or "Unknown",
                aliases=artist_row.get("aliases"),
                tags=artist_row.get("tags"),
                similar_artists=artist_row.get("similar_artists"),
                genres=genre_names if genre_names else None,
                inception_year=year,
                wikipedia_url=wiki_url,
                wikidata_uri=f"{settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX}{qid}",
                chunk_index=i + 1,
                total_chunks=total_chunks
            )
            results.append(Article(metadata=meta, article=enriched_text))
            
        return results

    async def process_batch_wrapper(
        batch: list[Dict[str, Any]], client: httpx.AsyncClient
    ) -> list[Article]:
        qids = [str(row.get("id") or "") for row in batch]
        entities = await async_fetch_wikidata_entities_batch(
            context, 
            qids, 
            api_url=settings.WIKIDATA_ACTION_API_URL,
            cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client
        )
        
        # Limit concurrent requests to Wikipedia within this batch to avoid 429s
        # Total concurrency = (External Batches: 5) * (Internal Semaphore)
        sem = asyncio.Semaphore(settings.WIKIPEDIA_CONCURRENT_REQUESTS)

        async def bounded_process(row: Dict[str, Any], url: str) -> List[Article]:
            async with sem:
                return await async_process_artist(row, url, client)

        tasks = []
        for row in batch:
            qid = str(row.get("id") or "")
            entity_data = entities.get(qid)
            if not entity_data:
                continue
            wiki_url = extract_wikidata_wikipedia_url(entity_data)
            if wiki_url:
                tasks.append(bounded_process(row, wiki_url))
        
        if not tasks:
            return []
        results_nested = await asyncio.gather(*tasks)
        return [item for sublist in results_nested for item in sublist]

    # 4. Execution Loop
    async with wikidata.get_client(context) as client:
        article_stream = yield_batches_concurrently(
            items=rows_to_process,
            batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
            processor_fn=process_batch_wrapper,
            concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
            description="Processing Articles",
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            client=client,
        )

        # 5. Stream results to file
        context.log.info(f"Streaming Wikipedia articles to {temp_file}.")
        buffer = []
        chunk_count = 0
        async for article in article_stream:
            buffer.append(msgspec.to_builtins(article))
            if len(buffer) >= 50:
                await async_append_jsonl(temp_file, buffer)
                chunk_count += len(buffer)
                buffer = []
        
        if buffer:
            await async_append_jsonl(temp_file, buffer)
            chunk_count += len(buffer)

    context.log.info(f"Wikipedia extraction complete. Fetched {chunk_count} chunks.")
    
    context.add_output_metadata({
        "chunk_count": chunk_count,
        "artist_count": total_rows,
        "temp_path": str(temp_file)
    })
    
    return pl.scan_ndjson(str(temp_file))
