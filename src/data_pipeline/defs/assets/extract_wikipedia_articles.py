# -----------------------------------------------------------
# Extract Wikipedia Articles Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
from typing import Dict, Any, List

import httpx
import msgspec
import polars as pl
from dagster import asset, AssetExecutionContext
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

from data_pipeline.models import Article, ArticleMetadata
from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import yield_batches_concurrently
from data_pipeline.utils.transformation_helpers import normalize_and_clean_text
from data_pipeline.utils.wikidata_helpers import (
    async_fetch_wikidata_entities_batch,
    extract_wikidata_wikipedia_url
)
from data_pipeline.utils.wikipedia_helpers import (
    async_fetch_wikipedia_article,
    clean_wikipedia_text
)
from data_pipeline.defs.resources import WikidataResource

# --- Domain Constants ---
WIKIPEDIA_EXCLUSION_HEADERS = [
    "References",
    "External links",
    "See also",
    # "Further reading",
    # "Notes",
    # "Discography"
]


@asset(
    name="wikipedia_articles",
    description="Extract Wikipedia articles, clean, split, and enrich with metadata for RAG.",
)
async def extract_wikipedia_articles(
    context: AssetExecutionContext, 
    wikidata: WikidataResource,
    artists: pl.DataFrame,
    genres: pl.DataFrame,
    artist_index: pl.DataFrame
) -> pl.DataFrame:
    """
    Orchestrates the fetching, cleaning, chunking, and enrichment of Wikipedia articles
    for all validated artists in the pipeline.
    """
    context.log.info("Loading validated artists, genres, and artist index from inputs.")

    # 1. Prepare Mappings
    genres_map: Dict[str, str] = {
        str(k): str(v) 
        for k, v in zip(genres["id"].to_list(), genres["name"].to_list()) 
        if k is not None and v is not None
    }
    inception_year_map = {}
    
    def extract_qid(uri: str) -> str:
        return uri.split("/")[-1] if "/" in uri else uri
        
    uris = artist_index["artist_uri"].to_list()
    dates = artist_index["start_date"].to_list()
    
    for uri, date in zip(uris, dates):
        qid = extract_qid(uri)
        try:
            year = int(date.split("-")[0]) if date else None
            if year:
                inception_year_map[qid] = year
        except (ValueError, AttributeError):
            continue

    rows_to_process = artists.to_dicts()
    total_rows = len(rows_to_process)
    context.log.info(f"Found {total_rows} artists to process for Wikipedia articles.")

    # 2. Setup Splitter
    tokenizer = AutoTokenizer.from_pretrained(
        settings.DEFAULT_EMBEDDINGS_MODEL_NAME, trust_remote_code=True
    )
    text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer,
        chunk_size=2048,
        chunk_overlap=512,
        separators=["\n\n", "\n", " ", ""],
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
        
        raw_text = await async_fetch_wikipedia_article(context, title, qid=qid, client=client)
        
        if not raw_text:
            return []

        # Domain-specific text cleaning
        def process_text(text: str) -> List[str]:
            cleaned = normalize_and_clean_text(clean_wikipedia_text(text, WIKIPEDIA_EXCLUSION_HEADERS))
            return text_splitter.split_text(cleaned)

        chunks = await asyncio.to_thread(process_text, raw_text)
        
        total_chunks = len(chunks)
        genre_ids = artist_row.get("genres") or []
        genre_names = [genres_map[gid] for gid in genre_ids if gid in genres_map]
        year = inception_year_map.get(qid)
        
        results = []
        for i, chunk_text in enumerate(chunks):
            enriched_text = f"search_document: {artist_name} | {chunk_text}"
            meta = ArticleMetadata(
                title=title.replace("_", " "),
                artist_name=artist_name,
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
        entities = await async_fetch_wikidata_entities_batch(context, qids, client)
        
        tasks = []
        for row in batch:
            qid = str(row.get("id") or "")
            entity_data = entities.get(qid)
            if not entity_data:
                continue
            wiki_url = extract_wikidata_wikipedia_url(entity_data)
            if wiki_url:
                tasks.append(async_process_artist(row, wiki_url, client))
        
        if not tasks:
            return []
        results_nested = await asyncio.gather(*tasks)
        return [item for sublist in results_nested for item in sublist]

    # 4. Execution Loop
    async with wikidata.yield_for_execution(context) as client:
        article_stream = yield_batches_concurrently(
            items=rows_to_process,
            batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE,
            processor_fn=process_batch_wrapper,
            concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
            description="Processing Articles",
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            client=client,
        )

        # 5. Collect results
        context.log.info("Collecting Wikipedia articles.")
        articles_list = [msgspec.to_builtins(article) async for article in article_stream]

    context.log.info(f"Wikipedia extraction complete. Fetched {len(articles_list)} chunks.")
    
    context.add_output_metadata({
        "chunk_count": len(articles_list),
        "artist_count": total_rows
    })
    
    return pl.DataFrame(articles_list)
