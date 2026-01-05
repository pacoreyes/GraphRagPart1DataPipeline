# -----------------------------------------------------------
# Unit Tests for extract_wikipedia_articles
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from pathlib import Path
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_wikipedia_articles import extract_wikipedia_articles
from data_pipeline.settings import settings

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_wikipedia_articles.stream_to_jsonl")
async def test_extract_wikipedia_articles_flow(mock_stream):
    # Mock Data
    artists_data = [
        {"id": "Q1", "name": "Artist One", "genres": ["QG1"]},
        {"id": "Q2", "name": "Artist Two", "genres": []} # Should be skipped if no wiki url
    ]
    genres_data = [
        {"id": "QG1", "name": "Rock"}
    ]
    index_data = [
        {"artist_uri": "http://www.wikidata.org/entity/Q1", "start_date": "1991-01-01"}
    ]

    # Mock Polars read_ndjson
    def mock_read_ndjson(path, **kwargs):
        if path == settings.artists_filepath:
            return pl.DataFrame(artists_data)
        if path == settings.genres_filepath:
            return pl.DataFrame(genres_data)
        if path == settings.artist_index_filepath:
            return pl.DataFrame(index_data)
        return pl.DataFrame([])

    # Mock Wikidata Entity Fetch
    async def mock_fetch_entities(context, qids, client=None):
        result = {}
        if "Q1" in qids:
            result["Q1"] = {
                "sitelinks": {
                    "enwiki": {
                        "title": "Artist One"
                    }
                }
            }
        if "Q2" in qids:
             result["Q2"] = {} # No sitelinks
        return result

    # Mock Dependencies
    with patch("polars.read_ndjson", side_effect=mock_read_ndjson), \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_fetch_wikipedia_article", new_callable=AsyncMock) as mock_fetch, \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.async_fetch_wikidata_entities_batch", side_effect=mock_fetch_entities) as mock_batch_fetch, \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.AutoTokenizer") as mock_tokenizer, \
         patch("data_pipeline.defs.assets.extract_wikipedia_articles.RecursiveCharacterTextSplitter") as mock_splitter_cls, \
         patch("builtins.open", new_callable=MagicMock), \
         patch("pathlib.Path.mkdir"), \
         patch("pathlib.Path.exists", return_value=True):
        
        # Setup specific mock returns
        mock_fetch.return_value = "This is the article text for Artist One.\n\nIt is a rock band."
        
        # Setup Text Splitter Mock
        mock_splitter_instance = mock_splitter_cls.from_huggingface_tokenizer.return_value
        mock_splitter_instance.split_text.return_value = ["This is the article text for Artist One.", "It is a rock band."]

        # Create Context
        context = build_asset_context()

        # Run Asset
        result_path = await extract_wikipedia_articles(context)

        # Verifications
        assert result_path == settings.wikipedia_articles_filepath
        
        # Verify Write called and consume generator to trigger execution
        mock_stream.assert_called_once()
        
        # Collect items
        items = [x async for x in mock_stream.call_args[0][0]]
        assert len(items) == 2
        
        # Verify Fetch called for valid artist only (After generator consumption)
        mock_fetch.assert_called_once()
        args, _ = mock_fetch.call_args
        assert "Artist_One" in args[1] # title
        
        saved_chunk = items[0]
        
        # saved_chunk is an Article struct
        assert saved_chunk.metadata.wikidata_uri == "http://www.wikidata.org/entity/Q1"
        assert saved_chunk.metadata.inception_year == 1991
        assert saved_chunk.metadata.genres == ["Rock"]
        assert "search_document: Artist One" in saved_chunk.article
