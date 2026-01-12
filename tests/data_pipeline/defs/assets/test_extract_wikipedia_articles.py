# ----------------------------------------------------------- 
# Unit Tests for wikipedia_articles
# Dagster Data pipeline for Structured and Unstructured Data
# 
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# ----------------------------------------------------------- 

import pytest
import httpx
from unittest.mock import MagicMock, patch, AsyncMock
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_wikipedia_articles import extract_wikipedia_articles

@pytest.mark.asyncio
async def test_extract_wikipedia_articles_flow():
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
             result["Q2"] = {} # No sitelinks
        return result

    # Mock Dependencies
    with patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_fetch_wikipedia_article", new_callable=AsyncMock) as mock_fetch, \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_fetch_wikidata_entities_batch", side_effect=mock_fetch_entities), \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.AutoTokenizer"), \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.RecursiveCharacterTextSplitter") as mock_splitter_cls:
        # Setup specific mock returns
        mock_fetch.return_value = "This is a sufficiently long text for Artist One to ensure it passes the minimal content filter of 50 characters."

        # Setup Text Splitter Mock
        mock_splitter_instance = mock_splitter_cls.from_huggingface_tokenizer.return_value
        mock_splitter_instance.split_text.return_value = ["Chunk 1", "Chunk 2"]

        from contextlib import asynccontextmanager

        # Create Context
        context = build_asset_context()
        mock_client = MagicMock(spec=httpx.AsyncClient)

        mock_wikidata = MagicMock()
        @asynccontextmanager
        async def mock_yield(context):
            yield mock_client
        mock_wikidata.get_client = mock_yield

        # Run Asset
        result_lf = await extract_wikipedia_articles(
            context, 
            mock_wikidata, 
            artists_df.lazy(), 
            genres_df.lazy(), 
            index_df.lazy()
        )

        # Verifications
        assert isinstance(result_lf, pl.LazyFrame)
        
        result_df = result_lf.collect()
        assert len(result_df) == 2
        
        # Verify Fetch called for valid artist only
        mock_fetch.assert_called_once()
        
        first_row = result_df.to_dicts()[0]
        assert "metadata" in first_row
        assert first_row["metadata"]["artist_name"] == "Artist One"
        assert first_row["metadata"]["genres"] == ["Rock"]
        assert first_row["metadata"]["inception_year"] == 1991