# -----------------------------------------------------------
# Unit Tests for extract_genres_articles
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

from data_pipeline.defs.assets.extract_genres_articles import extract_genres_articles
from data_pipeline.utils.network_helpers import AsyncClient


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_genres_articles.settings")
async def test_extract_genres_articles_flow(
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
    mock_settings.WIKIPEDIA_CACHE_DIRPATH = Path("/tmp/wiki_cache")
    mock_settings.WIKIDATA_CACHE_DIRPATH = Path("/tmp/wd_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.DEFAULT_EMBEDDINGS_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
    mock_settings.TEXT_CHUNK_SIZE = 100
    mock_settings.TEXT_CHUNK_OVERLAP = 10
    mock_settings.WIKIDATA_FALLBACK_LANGUAGES = ["en"]
    mock_settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX = "http://wd.entity/"

    # Mock Data DataFrames (Input)
    genres_df = pl.DataFrame([
        {"id": "Q1298934", "name": "Techno", "aliases": ["techno music", "Detroit techno"]},
        {"id": "Q11401", "name": "House", "aliases": ["house music"]}
    ])

    # Mock Wikidata Entity Fetch
    async def mock_fetch_entities(context, qids, client=None, **kwargs):
        result = {}
        if "Q1298934" in qids:
            result["Q1298934"] = {
                "sitelinks": {"enwiki": {"title": "Techno"}},
                "claims": {
                    "P495": [{  # Country of origin
                        "mainsnak": {
                            "snaktype": "value",
                            "datavalue": {
                                "type": "wikibase-entityid",
                                "value": {"id": "Q30"}  # USA
                            }
                        }
                    }],
                    "P571": [{  # Inception
                        "mainsnak": {
                            "snaktype": "value",
                            "datavalue": {
                                "type": "time",
                                "value": {"time": "+1988-00-00T00:00:00Z", "precision": 9}
                            }
                        }
                    }]
                }
            }
        if "Q11401" in qids:
            result["Q11401"] = {
                "sitelinks": {"enwiki": {"title": "House_music"}}
                # No country or inception
            }
        return result

    # Mock QID to label resolution
    async def mock_resolve_labels(context, qids, **kwargs):
        result = {}
        if "Q30" in qids:
            result["Q30"] = "United States"
        return result

    # Mock Dependencies
    with patch("data_pipeline.defs.assets.extract_genres_articles.async_fetch_wikipedia_article", new_callable=AsyncMock) as mock_fetch, \
         patch("data_pipeline.defs.assets.extract_genres_articles.async_fetch_wikidata_entities_batch", side_effect=mock_fetch_entities), \
         patch("data_pipeline.defs.assets.extract_genres_articles.async_resolve_qids_to_labels", side_effect=mock_resolve_labels), \
         patch("data_pipeline.defs.assets.extract_genres_articles.create_rag_text_splitter") as mock_splitter_factory:

        mock_fetch.return_value = "This is a sufficiently long text for Techno genre to ensure it passes the minimal content filter of 50 characters."
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
        results = await extract_genres_articles(
            context,
            mock_wikidata,
            mock_wikipedia,
            genres_df.lazy()
        )

        assert isinstance(results, list)
        assert len(results) >= 1

        # Flatten results to find articles
        all_articles = [a for batch in results for a in batch]
        assert len(all_articles) >= 1

        # Find Techno article
        techno_articles = [a for a in all_articles if "Q1298934" in a.id]
        assert len(techno_articles) >= 1

        first_article = techno_articles[0]
        assert first_article.metadata.name == "Techno"
        assert first_article.metadata.entity_type == "genre"
        assert first_article.metadata.country == "United States"
        assert first_article.metadata.inception_year == 1988
        assert first_article.metadata.aliases == ["techno music", "Detroit techno"]


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_genres_articles.settings")
async def test_extract_genres_articles_empty_input(
    mock_settings
):
    # Setup Mocks
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.DEFAULT_EMBEDDINGS_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
    mock_settings.TEXT_CHUNK_SIZE = 100
    mock_settings.TEXT_CHUNK_OVERLAP = 10

    # Empty genres DataFrame
    genres_df = pl.DataFrame({"id": [], "name": [], "aliases": []})

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
    results = await extract_genres_articles(
        context,
        mock_wikidata,
        mock_wikipedia,
        genres_df.lazy()
    )

    assert isinstance(results, list)
    assert len(results) == 0


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_genres_articles.settings")
async def test_extract_genres_articles_no_wikipedia_url(
    mock_settings
):
    # Setup Mocks
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_ACTION_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY = 0
    mock_settings.WIKIPEDIA_CONCURRENT_REQUESTS = 2
    mock_settings.WIKIDATA_ACTION_API_URL = "http://wd.api"
    mock_settings.WIKIDATA_CACHE_DIRPATH = Path("/tmp/wd_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.DEFAULT_EMBEDDINGS_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
    mock_settings.TEXT_CHUNK_SIZE = 100
    mock_settings.TEXT_CHUNK_OVERLAP = 10
    mock_settings.WIKIDATA_FALLBACK_LANGUAGES = ["en"]

    # Genre without Wikipedia article
    genres_df = pl.DataFrame([
        {"id": "Q999999", "name": "Unknown Genre", "aliases": []}
    ])

    # Mock Wikidata Entity Fetch - returns entity without sitelinks
    async def mock_fetch_entities(context, qids, client=None, **kwargs):
        return {"Q999999": {"claims": {}}}  # No sitelinks

    with patch("data_pipeline.defs.assets.extract_genres_articles.async_fetch_wikidata_entities_batch", side_effect=mock_fetch_entities), \
         patch("data_pipeline.defs.assets.extract_genres_articles.create_rag_text_splitter"):

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
        results = await extract_genres_articles(
            context,
            mock_wikidata,
            mock_wikipedia,
            genres_df.lazy()
        )

        # Should return empty because genre has no Wikipedia URL
        all_articles = [a for batch in results for a in batch]
        assert len(all_articles) == 0
