# -----------------------------------------------------------
# Extract Wikipedia Articles Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
from pathlib import Path
from typing import Dict, Any, List, Callable

import httpx
import polars as pl
from dagster import asset, AssetExecutionContext
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

from data_pipeline.models import Article, ArticleMetadata
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import stream_to_jsonl
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


@asset(
    name="extract_wikipedia_articles",
    deps=["extract_artists", "extract_genres", "build_artist_index"],
    description="Extract Wikipedia articles, clean, split, and enrich with metadata for RAG.",
)
async def extract_wikipedia_articles(
    context: AssetExecutionContext,
) -> Path:
    """
    Orchestrates the fetching, cleaning, chunking, and enrichment of Wikipedia articles
    for all validated artists in the pipeline.
    """
    context.log.info("Loading validated artists, genres, and artist index.")

    # 1. Load Data
    # Artists (Source of Truth for QIDs and Wikipedia URLs)
    if not settings.artists_filepath.exists():
        context.log.warning(f"Artists file not found at {settings.artists_filepath}. Skipping.")
        return settings.wikipedia_articles_filepath
        
    artists_df = pl.read_ndjson(settings.artists_filepath)
    
    # Genres (Map QID -> Name)
    genres_map = {}
    if settings.genres_filepath.exists():
        genres_df = pl.read_ndjson(settings.genres_filepath)
        # Create a dict: {qid: name}
        genres_map = dict(zip(genres_df["id"].to_list(), genres_df["name"].to_list()))
    else:
        context.log.warning(f"Genres file not found at {settings.genres_filepath}. Metadata will miss genre names.")

    # Artist Index (Map QID -> Inception Year)
    inception_year_map = {}
    if settings.artist_index_filepath.exists():
        index_df = pl.read_ndjson(settings.artist_index_filepath)
        
        def extract_qid(uri: str) -> str:
            return uri.split("/")[-1] if "/" in uri else uri
            
        uris = index_df["artist_uri"].to_list()
        dates = index_df["start_date"].to_list()
        
        for uri, date in zip(uris, dates):
            qid = extract_qid(uri)
            try:
                year = int(date.split("-")[0]) if date else None
                if year:
                    inception_year_map[qid] = year
            except (ValueError, AttributeError):
                continue

    # Filter artists with Wikipedia URLs
    # artists_with_wiki = artists_df.filter(pl.col("wikipedia_url").is_not_null())
    rows_to_process = artists_df.to_dicts()
    
    total_rows = len(rows_to_process)
    context.log.info(f"Found {total_rows} artists to process for Wikipedia articles.")

    # 2. Setup Output
    output_path = settings.wikipedia_articles_filepath
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Ensure a fresh file
    output_path.unlink(missing_ok=True)

    # 3. Setup Splitter
    tokenizer = AutoTokenizer.from_pretrained(
        settings.DEFAULT_EMBEDDINGS_MODEL_NAME, trust_remote_code=True
    )
    text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer,
        chunk_size=2048,
        chunk_overlap=512,
        separators=["\n\n", "\n", " ", ""],
    )

    # 4. Worker Function
    async def async_process_artist(
        artist_row: Dict[str, Any],
        wiki_url: str,
        client: httpx.AsyncClient
    ) -> List[Article]:
        
        artist_name = artist_row.get("name")
        qid = artist_row.get("id")
        
        if not wiki_url:
            return []
            
        title = wiki_url.split("/")[-1]
        
        # Fetch
        raw_text = await async_fetch_wikipedia_article(context, title, qid=qid, client=client)
        
        if not raw_text:
            return []

        # Clean
        cleaned_text = clean_wikipedia_text(raw_text)
        if not cleaned_text:
            return []
            
        cleaned_text = normalize_and_clean_text(cleaned_text)

        # Split
        chunks = text_splitter.split_text(cleaned_text)
        total_chunks = len(chunks)
        
        # Metadata Enrichment
        genre_ids = artist_row.get("genres") or [] # List of QIDs
        genre_names = [genres_map.get(gid) for gid in genre_ids if gid in genres_map]
        year = inception_year_map.get(qid)
        
        results = []
        for i, chunk_text in enumerate(chunks):
            # Context Injection for RAG
            enriched_text = f"search_document: {artist_name} | {chunk_text}"
            
            meta = ArticleMetadata(
                title=title.replace("_", " "),
                artist_name=artist_name,
                genres=genre_names,
                inception_year=year,
                wikipedia_url=wiki_url,
                wikidata_uri=f"{settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX}{qid}",
                chunk_index=i + 1,
                total_chunks=total_chunks
            )
            
            article = Article(
                metadata=meta,
                article=enriched_text
            )
            
            results.append(article)
            
        return results

    async def process_batch_wrapper(
        batch: list[Dict[str, Any]], client: httpx.AsyncClient
    ) -> list[Article]:
        # 1. Collect QIDs
        qids = [row["id"] for row in batch]
        
        # 2. Fetch/Cache Entity Data
        entities = await async_fetch_wikidata_entities_batch(context, qids, client)
        
        tasks = []
        for row in batch:
            qid = row.get("id")
            entity_data = entities.get(qid)
            
            if not entity_data:
                continue
                
            # 3. Resolve Wikipedia URL
            wiki_url = extract_wikidata_wikipedia_url(entity_data)
            
            if wiki_url:
                tasks.append(async_process_artist(row, wiki_url, client))
        
        if not tasks:
            return []

        results_nested = await asyncio.gather(*tasks)
        # flatten
        return [item for sublist in results_nested for item in sublist]

    # 5. Execution Loop (Streaming)
    # Using batches to control concurrency
    article_stream = yield_batches_concurrently(
        items=rows_to_process,
        batch_size=settings.WIKIDATA_ACTION_BATCH_SIZE, # Can reuse this or defined another setting
        processor_fn=process_batch_wrapper,
        concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
        description="Processing Articles",
        timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
    )

    await stream_to_jsonl(article_stream, output_path)

    context.log.info(f"Wikipedia extraction complete. Saved to {output_path}")
    return output_path

