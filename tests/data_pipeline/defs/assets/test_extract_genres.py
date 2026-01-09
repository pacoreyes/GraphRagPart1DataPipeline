# -----------------------------------------------------------
# Unit Tests for genres
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
import httpx
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_genres import extract_genres

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_genres.fetch_sparql_query_async")
@patch("data_pipeline.defs.assets.extract_genres.async_fetch_wikidata_entities_batch")
@patch("data_pipeline.defs.assets.extract_genres.settings")
async def test_extract_genres(
    mock_settings,
    mock_fetch_entities,
    mock_fetch_sparql
):
    """
    Test the genres asset.
    """
    # Setup mock settings
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2

    # Mock Input DataFrames
    mock_artists_df = pl.DataFrame({
        "genres": [["Q101", "Q102"]]
    })
    mock_albums_df = pl.DataFrame({
        "genres": [["Q102", "Q103"]]
    })
    mock_tracks_df = pl.DataFrame({
        "genres": [["Q104"]]
    })

    # Unique Genres Expected: Q101, Q102, Q103, Q104
    
    from contextlib import asynccontextmanager

    # Mock Context and Resource
    context = build_asset_context()
    mock_client = MagicMock(spec=httpx.AsyncClient)

    mock_wikidata = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield mock_client
    mock_wikidata.yield_for_execution = mock_yield

    # Configure for batching test
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 2

    async def side_effect_fetch(context, qids, client=None):
        data = {
            "Q101": {"labels": {"en": {"value": "Genre A"}}, "aliases": {"en": [{"value": "Alias A1"}]}},
            "Q102": {"labels": {"en": {"value": "Genre B"}}, "aliases": {}},
            "Q103": {"labels": {"en": {"value": "Genre C"}}, "aliases": {}},
            "Q104": {"labels": {"en": {"value": "Genre D"}}, "aliases": {"en": [{"value": "Alias D1"}, {"value": "Alias D2"}]}}
        }
        return {k: v for k, v in data.items() if k in qids}

    mock_fetch_entities.side_effect = side_effect_fetch
    
    # Mock SPARQL (Parents)
    mock_fetch_sparql.return_value = [] # No parents for now

    # Execution
    result_df = await extract_genres(context, mock_wikidata, mock_artists_df, mock_albums_df, mock_tracks_df)

    # Assertions
    assert isinstance(result_df, pl.DataFrame)
    written_ids = result_df["id"].to_list()
    
    assert "Q101" in written_ids
    assert "Q102" in written_ids
    assert "Q103" in written_ids
    assert "Q104" in written_ids
