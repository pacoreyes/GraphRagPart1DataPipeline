# -----------------------------------------------------------
# Unit Tests for artist_index
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context
from data_pipeline.defs.assets.build_artist_index import (
    build_artist_index_by_decade,
    build_artist_index,
)
from data_pipeline.utils.network_helpers import AsyncClient

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.build_artist_index.run_extraction_pipeline")
async def test_build_artist_index_by_decade(mock_execute):
    """Test the extraction asset for a specific decade."""
    from contextlib import asynccontextmanager

    # Create a mock context with a partition key
    context = build_asset_context(partition_key="1960s")
    mock_client = MagicMock(spec=AsyncClient)

    mock_wikidata = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield mock_client
    mock_wikidata.get_client = mock_yield

    mock_execute.return_value = [{"artist_uri": "http://q1", "name": "Artist 1", "start_date": "1965"}]

    # Execute the asset
    result_df = await build_artist_index_by_decade(context, mock_wikidata)

    assert isinstance(result_df, pl.LazyFrame)
    # Check data (must collect)
    df = result_df.collect()
    assert len(df) == 1
    assert df["name"][0] == "Artist 1"


@pytest.mark.asyncio


@patch("data_pipeline.defs.assets.build_artist_index.settings")


@patch("data_pipeline.defs.assets.build_artist_index.deduplicate_by_priority")


async def test_artist_index_merge_and_clean(


    mock_dedup,


    mock_settings


):


    """Test the merge and deduplication asset."""


    from dagster import MaterializeResult


    from pathlib import Path


    


    # Mock Settings


    mock_settings.TEMP_DIRPATH = MagicMock()


    mock_settings.DATASETS_DIRPATH = MagicMock()


    


    # Mock Input: Single LazyFrame (simulating IO manager combining partitions)


    combined_df = pl.DataFrame([


        {"artist_uri": "http://q1", "name": "Artist 1", "start_date": "1965"},


        {"artist_uri": "http://q2", "name": "Artist 2", "start_date": "1975"}


    ])


    mock_input_lf = combined_df.lazy()





    # Dedup just returns input for simplicity


    mock_dedup.side_effect = lambda lf, **kwargs: lf





    context = build_asset_context()





    # Execute the asset


    result_lf = build_artist_index(context, mock_input_lf)


    


    assert isinstance(result_lf, pl.LazyFrame)


    mock_dedup.assert_called_once()




