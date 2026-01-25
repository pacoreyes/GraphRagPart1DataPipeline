# -----------------------------------------------------------
# Unit Tests for extract_artists_articles
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_artists_articles import extract_artist_articles
from data_pipeline.utils.network_helpers import AsyncClient


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_artists_articles.settings")
async def test_extract_artists_articles_flow(
    mock_settings
):
    # Setup Mocks
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_ACTION_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY = 0
    mock_settings.WIKIPEDIA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIPEDIA_RATE_LIMIT_DELAY = 0
    mock_settings.WIKIDATA_ACTION_API_URL = "http://wd.api"
    mock_settings.MIN_CONTENT_LENGTH = 10
    mock_settings.ARTICLES_BUFFER_SIZE = 10
    mock_settings.WIKIPEDIA_CACHE_DIRPATH = Path("/tmp/wiki_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.DEFAULT_EMBEDDINGS_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
    mock_settings.TEXT_CHUNK_SIZE = 100
    mock_settings.TEXT_CHUNK_OVERLAP = 10
    mock_settings.WIKIDATA_FALLBACK_LANGUAGES = ["en"]
    mock_settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX = "http://wd.entity/"

    # Mock Data DataFrames (Input)
    artists_df = pl.DataFrame([
        {"id": "Q1", "name": "Artist One", "genres": ["QG1"]},
        {"id": "Q2", "name": "Artist Two", "genres": []}
    ])
    genres_df = pl.DataFrame([
        {"id": "QG1", "name": "Rock"}
    ])
    index_df = pl.DataFrame([
        {"artist_uri": "http://www.wikidata.org/entity/Q1", "start_date": "1991-01-01"}
    ])

    # Mock Wikidata Entity Fetch
    async def mock_fetch_entities(context, qids, client=None, **kwargs):
        result = {}
        if "Q1" in qids:
            result["Q1"] = {
                "sitelinks": {"enwiki": {"title": "Artist One"}}
            }
        if "Q2" in qids:
             result["Q2"] = {}  # No sitelinks
        return result

    # Mock Dependencies
    with patch("data_pipeline.defs.assets.extract_artists_articles.async_fetch_wikipedia_article", new_callable=AsyncMock) as mock_fetch, \
         patch("data_pipeline.defs.assets.extract_artists_articles.async_fetch_wikidata_entities_batch", side_effect=mock_fetch_entities), \
         patch("data_pipeline.defs.assets.extract_artists_articles.create_rag_text_splitter") as mock_splitter_factory:

        mock_fetch.return_value = "This is a sufficiently long text for Artist One to ensure it passes the minimal content filter of 50 characters."
        mock_splitter_instance = MagicMock()
        mock_splitter_instance.split_text.return_value = ["Chunk 1", "Chunk 2"]
        mock_splitter_factory.return_value = mock_splitter_instance

        from contextlib import asynccontextmanager

        context = build_asset_context()
        mock_client = MagicMock(spec=AsyncClient)

        mock_wikidata = MagicMock()
        @asynccontextmanager
        async def mock_yield_wd(context):
            yield mock_client
        mock_wikidata.get_client = mock_yield_wd

        mock_wikipedia = MagicMock()
        @asynccontextmanager
        async def mock_yield_wp(context):
            yield mock_client
        mock_wikipedia.get_client = mock_yield_wp
        mock_wikipedia.api_url = "http://wp.api"
        mock_wikipedia.rate_limit_delay = 0

        # Run Asset
        results = await extract_artist_articles(
            context,
            mock_wikidata,
            mock_wikipedia,
            artists_df.lazy(),
            genres_df.lazy(),
            index_df.lazy()
        )

        assert isinstance(results, list)
        assert len(results) >= 1
        first_batch = results[0]
        assert first_batch[0].id == "Q1_chunk_1"
        assert first_batch[0].metadata.name == "Artist One"
        assert first_batch[0].metadata.entity_type == "artist"


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_artists_articles.settings")
async def test_extract_artists_articles_deduplication(
    mock_settings
):
    # Setup Mocks
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_ACTION_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY = 0
    mock_settings.WIKIPEDIA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIPEDIA_RATE_LIMIT_DELAY = 0
    mock_settings.WIKIDATA_ACTION_API_URL = "http://wd.api"
    mock_settings.MIN_CONTENT_LENGTH = 10
    mock_settings.ARTICLES_BUFFER_SIZE = 10
    mock_settings.WIKIPEDIA_CACHE_DIRPATH = Path("/tmp/wiki_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.DEFAULT_EMBEDDINGS_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
    mock_settings.TEXT_CHUNK_SIZE = 100
    mock_settings.TEXT_CHUNK_OVERLAP = 10
    mock_settings.WIKIDATA_FALLBACK_LANGUAGES = ["en"]
    mock_settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX = "http://wd.entity/"

    # Mock Data DataFrames (Input) - DUPLICATES!
    artists_df = pl.DataFrame([
        {"id": "Q1", "name": "Artist One", "genres": ["QG1"]},
        {"id": "Q1", "name": "Artist One", "genres": ["QG1"]}
    ])
    genres_df = pl.DataFrame([
        {"id": "QG1", "name": "Rock"}
    ])
    index_df = pl.DataFrame([
        {"artist_uri": "http://www.wikidata.org/entity/Q1", "start_date": "1991-01-01"}
    ])

    # Mock Wikidata Entity Fetch
    async def mock_fetch_entities(context, qids, client=None, **kwargs):
        result = {}
        if "Q1" in qids:
            result["Q1"] = {
                "sitelinks": {"enwiki": {"title": "Artist One"}}
            }
        return result

    # Mock Dependencies
    with patch("data_pipeline.defs.assets.extract_artists_articles.async_fetch_wikipedia_article", new_callable=AsyncMock) as mock_fetch, \
         patch("data_pipeline.defs.assets.extract_artists_articles.async_fetch_wikidata_entities_batch", side_effect=mock_fetch_entities), \
         patch("data_pipeline.defs.assets.extract_artists_articles.create_rag_text_splitter") as mock_splitter_factory:

        mock_fetch.return_value = "This is a sufficiently long text for Artist One to ensure it passes the minimal content filter of 50 characters."
        mock_splitter_instance = MagicMock()
        mock_splitter_instance.split_text.return_value = ["Chunk 1"]
        mock_splitter_factory.return_value = mock_splitter_instance

        from contextlib import asynccontextmanager

        context = build_asset_context()
        mock_client = MagicMock(spec=AsyncClient)

        mock_wikidata = MagicMock()
        @asynccontextmanager
        async def mock_yield_wd(context):
            yield mock_client
        mock_wikidata.get_client = mock_yield_wd

        mock_wikipedia = MagicMock()
        @asynccontextmanager
        async def mock_yield_wp(context):
            yield mock_client
        mock_wikipedia.get_client = mock_yield_wp
        mock_wikipedia.api_url = "http://wp.api"
        mock_wikipedia.rate_limit_delay = 0

        # Run Asset
        results = await extract_artist_articles(
            context,
            mock_wikidata,
            mock_wikipedia,
            artists_df.lazy(),
            genres_df.lazy(),
            index_df.lazy()
        )

        assert isinstance(results, list)
        # Should contain only 1 chunk for Q1, despite 2 rows in input
        # results is list of batches, each batch is list of Article
        all_articles = [a for batch in results for a in batch]
        assert len(all_articles) == 1
        assert all_articles[0].id == "Q1_chunk_1"
        assert all_articles[0].metadata.entity_type == "artist"

        # Verify fetch was called only once (although cache might prevent 2nd call,
        # deduplication prevents the attempt)
        assert mock_fetch.call_count == 1
