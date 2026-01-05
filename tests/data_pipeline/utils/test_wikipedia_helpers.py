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
from data_pipeline.utils.wikipedia_helpers import async_fetch_wikipedia_article, clean_wikipedia_text, WIKIPEDIA_CACHE_DIR

@pytest.mark.asyncio
async def test_async_fetch_wikipedia_article_cache_hit():
    context = MagicMock()
    title = "Test_Article"
    qid = "Q123"
    cached_content = "This is cached content."
    
    with patch("pathlib.Path.exists", new_callable=MagicMock) as mock_exists, \
         patch("builtins.open", new_callable=MagicMock) as mock_open:
        
        # Simulate cache hit
        mock_exists.return_value = True
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_file.read.return_value = cached_content
        
        result = await async_fetch_wikipedia_article(context, title, qid=qid)
        
        assert result == cached_content
        mock_open.assert_called_with(WIKIPEDIA_CACHE_DIR / "Q123.txt", "r", encoding="utf-8")

@pytest.mark.asyncio
async def test_async_fetch_wikipedia_article_cache_miss_fetch_success():
    context = MagicMock()
    title = "Test_Article"
    qid = "Q123"
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
    
    with patch("pathlib.Path.exists", return_value=False), \
         patch("builtins.open", new_callable=MagicMock) as mock_open, \
         patch("data_pipeline.utils.wikipedia_helpers.make_async_request_with_retries", new_callable=AsyncMock) as mock_request, \
         patch("pathlib.Path.mkdir") as mock_mkdir:
        
        mock_response = MagicMock()
        mock_response.json.return_value = api_response
        mock_request.return_value = mock_response
        
        result = await async_fetch_wikipedia_article(context, title, qid=qid)
        
        assert result == fetched_content
        # Verify write to cache
        mock_open.assert_called_with(WIKIPEDIA_CACHE_DIR / "Q123.txt", "w", encoding="utf-8")
        mock_open.return_value.__enter__.return_value.write.assert_called_with(fetched_content)

@pytest.mark.asyncio
async def test_async_fetch_wikipedia_article_api_failure():
    context = MagicMock()
    title = "Test_Article"
    
    with patch("pathlib.Path.exists", return_value=False), \
         patch("data_pipeline.utils.wikipedia_helpers.make_async_request_with_retries", side_effect=Exception("API Error")):
        
        result = await async_fetch_wikipedia_article(context, title)
        
        assert result is None

def test_clean_wikipedia_text_removes_discography():
    raw_text = """
    Nick Warren is a DJ.
    
    == Biography ==
    He was born in Bristol.
    
    == Discography ==
    * Album 1
    * Album 2
    
    == References ==
    Ref 1
    """
    cleaned = clean_wikipedia_text(raw_text)
    
    assert "Nick Warren is a DJ." in cleaned
    assert "== Biography ==" in cleaned
    assert "He was born in Bristol." in cleaned
    assert "== Discography ==" not in cleaned
    assert "* Album 1" not in cleaned
    assert "== References ==" not in cleaned
