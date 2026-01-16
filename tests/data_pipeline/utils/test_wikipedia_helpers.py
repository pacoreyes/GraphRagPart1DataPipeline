# -----------------------------------------------------------
# Unit Tests for wikipedia_helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from pathlib import Path
from data_pipeline.utils.wikipedia_helpers import async_fetch_wikipedia_article
from data_pipeline.utils.network_helpers import HTTPError

@pytest.mark.asyncio
async def test_async_fetch_wikipedia_article_cache_hit():
    context = MagicMock()
    title = "Test_Article"
    qid = "Q123"
    api_url = "http://test.api"
    cache_dir = Path("/tmp/cache")
    cached_content = "This is cached content."
    
    with patch("data_pipeline.utils.wikipedia_helpers.async_read_text_file", new_callable=AsyncMock) as mock_read:
        # Simulate cache hit
        mock_read.return_value = cached_content
        
        result = await async_fetch_wikipedia_article(context, title, qid=qid, api_url=api_url, cache_dir=cache_dir)
        
        assert result == cached_content
        mock_read.assert_called_once()

@pytest.mark.asyncio
async def test_async_fetch_wikipedia_article_cache_miss_fetch_success():
    context = MagicMock()
    title = "Test_Article"
    qid = "Q123"
    api_url = "http://test.api"
    cache_dir = Path("/tmp/cache")
    fetched_content = "This is fetched content."
    api_response = {
        "query": {
            "pages": {
                "123": {
                    "extract": fetched_content
                }
            }
        }
    }
    
    with patch("data_pipeline.utils.wikipedia_helpers.async_read_text_file", return_value=None), \
         patch("data_pipeline.utils.wikipedia_helpers.async_write_text_file", new_callable=AsyncMock) as mock_write, \
         patch("data_pipeline.utils.wikipedia_helpers.make_async_request_with_retries", new_callable=AsyncMock) as mock_request:
        
        mock_response = MagicMock()
        mock_response.json.return_value = api_response
        mock_request.return_value = mock_response
        
        result = await async_fetch_wikipedia_article(context, title, qid=qid, api_url=api_url, cache_dir=cache_dir)
        
        assert result == fetched_content
        # Verify write to cache
        mock_write.assert_called_once()
        args, _ = mock_write.call_args
        assert args[1] == fetched_content

@pytest.mark.asyncio
async def test_async_fetch_wikipedia_article_api_failure():
    context = MagicMock()
    title = "Test_Article"
    qid = "Q123"
    api_url = "http://test.api"
    cache_dir = Path("/tmp/cache")
    
    with patch("data_pipeline.utils.wikipedia_helpers.async_read_text_file", return_value=None), \
         patch("data_pipeline.utils.wikipedia_helpers.make_async_request_with_retries", side_effect=HTTPError("API Error")):
        
        result = await async_fetch_wikipedia_article(context, title, qid=qid, api_url=api_url, cache_dir=cache_dir)
        
        assert result is None
