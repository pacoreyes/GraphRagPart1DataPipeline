# -----------------------------------------------------------
# Unit Tests for tracks
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

from data_pipeline.defs.assets.extract_tracks import extract_tracks

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_tracks.fetch_sparql_query_async")
@patch("data_pipeline.defs.assets.extract_tracks.settings")
async def test_extract_tracks(
    mock_settings,
    mock_fetch_sparql
):
    """
    Test the tracks asset.
    """
    # Setup mock settings
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2

    # Mock Input DataFrame (albums)
    mock_albums_df = pl.DataFrame({
        "id": ["Q100", "Q200"]
    })

    # Mock SPARQL Response
    mock_fetch_sparql.return_value = [
        # Track T1 (linked via Album->Track)
        {
            "album": {"value": "http://www.wikidata.org/entity/Q100"},
            "track": {"value": "http://www.wikidata.org/entity/T1"},
            "trackLabel": {"value": "Track One"},
        },
        # Track T2 (linked via Track->Album)
        {
            "album": {"value": "http://www.wikidata.org/entity/Q100"},
            "track": {"value": "http://www.wikidata.org/entity/T2"},
            "trackLabel": {"value": "Track Two"},
        },
        # Track T3 on Q200
        {
            "album": {"value": "http://www.wikidata.org/entity/Q200"},
            "track": {"value": "http://www.wikidata.org/entity/T3"},
            "trackLabel": {"value": "Track Three"},
        },
        # Track T1 on Q200 (Shared Track) - Should BE KEPT as a separate entry
        {
            "album": {"value": "http://www.wikidata.org/entity/Q200"},
            "track": {"value": "http://www.wikidata.org/entity/T1"},
            "trackLabel": {"value": "Track One"},
        }
    ]

    from contextlib import asynccontextmanager

    # Mock Context and Resource
    context = build_asset_context()
    mock_client = MagicMock(spec=httpx.AsyncClient)

    mock_wikidata = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield mock_client
    mock_wikidata.yield_for_execution = mock_yield

    # Execution
    result_df = await extract_tracks(context, mock_wikidata, mock_albums_df)

    # Assertions
    assert isinstance(result_df, pl.DataFrame)
    
    # Expected: 4 items (T1@Q100, T2@Q100, T3@Q200, T1@Q200)
    assert len(result_df) == 4
    
    # Verify T1 appears twice with different album_ids
    t1_instances = result_df.filter(pl.col("id") == "T1")
    assert len(t1_instances) == 2
    assert set(t1_instances["album_id"].to_list()) == {"Q100", "Q200"}

@pytest.mark.asyncio
async def test_extract_tracks_empty_albums():
    """
    Test tracks with empty albums DataFrame.
    """
    from contextlib import asynccontextmanager
    
    # Mock Empty DataFrame
    mock_albums_df = pl.DataFrame({"id": []})
    
    context = build_asset_context()
    mock_client = MagicMock(spec=httpx.AsyncClient)
    
    mock_wikidata = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield mock_client
    mock_wikidata.yield_for_execution = mock_yield
    
    result_df = await extract_tracks(context, mock_wikidata, mock_albums_df)
    
    # Should return empty DataFrame
    assert len(result_df) == 0
    assert isinstance(result_df, pl.DataFrame)
