# -----------------------------------------------------------
# Unit Tests for artist_index
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
from data_pipeline.defs.assets.build_artist_index import (
    build_artist_index_by_decade,
    build_artist_index,
)

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.build_artist_index.run_extraction_pipeline")
async def test_build_artist_index_by_decade(mock_execute):
    """Test the extraction asset for a specific decade."""
    # Create a mock context with a partition key
    context = build_asset_context(partition_key="1960s")
    mock_client = MagicMock(spec=httpx.AsyncClient)
    
    mock_execute.return_value = [{"artist_uri": "http://q1", "name": "Artist 1", "start_date": "1965"}]
    
    # Execute the asset
    result_df = await build_artist_index_by_decade(context, mock_client)
    
    assert isinstance(result_df, pl.DataFrame)
    assert len(result_df) == 1
    assert result_df["name"][0] == "Artist 1"
    
    # Verify run_extraction_pipeline was called
    mock_execute.assert_called_once()

@patch("data_pipeline.defs.assets.build_artist_index.deduplicate_by_priority")
def test_artist_index_merge_and_clean(mock_dedup):
    """Test the merge and deduplication asset."""
    # Mock Input (Partitions)
    mock_partitions = {
        "1960s": pl.DataFrame({"artist_uri": ["http://q1"], "name": ["Artist 1"], "start_date": ["1965"]}),
        "1970s": pl.DataFrame({"artist_uri": ["http://q2"], "name": ["Artist 2"], "start_date": ["1975"]})
    }
    
    mock_dedup.side_effect = lambda lf, **kwargs: lf
    
    context = build_asset_context()
    
    # Execute the asset
    result_df = build_artist_index(context, mock_partitions)
    
    assert isinstance(result_df, pl.DataFrame)
    assert len(result_df) == 2
    assert set(result_df["name"].to_list()) == {"Artist 1", "Artist 2"}
    mock_dedup.assert_called_once()
