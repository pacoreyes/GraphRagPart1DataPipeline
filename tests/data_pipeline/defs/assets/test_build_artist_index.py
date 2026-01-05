# -----------------------------------------------------------
# Unit Tests for build_artist_index
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from unittest.mock import patch
from pathlib import Path
from dagster import build_asset_context
from data_pipeline.defs.assets.build_artist_index import (
    build_artist_index_by_decade,
    build_artist_index,
)

@patch("data_pipeline.defs.assets.build_artist_index.execute_sparql_extraction")
@patch("data_pipeline.defs.assets.build_artist_index.settings")
def test_build_artist_index_by_decade(mock_settings, mock_execute):
    """Test the extraction asset for a specific decade."""
    # Setup mock settings
    mock_settings.datasets_dirpath = Path("/tmp/datasets")
    
    # Create a mock context with a partition key
    context = build_asset_context(partition_key="1960s")
    
    # Execute the asset
    result = build_artist_index_by_decade(context)
    
    # Verify path construction
    expected_path = Path("/tmp/datasets/artist_index_1960s.jsonl")
    assert result == str(expected_path)
    
    # Verify execute_sparql_extraction was called with correct parameters
    mock_execute.assert_called_once()
    args, kwargs = mock_execute.call_args
    assert kwargs["output_path"] == expected_path
    assert kwargs["start_year"] == 1960
    assert kwargs["end_year"] == 1969

@patch("data_pipeline.defs.assets.build_artist_index.pl")
@patch("data_pipeline.defs.assets.build_artist_index.deduplicate_by_priority")
@patch("data_pipeline.defs.assets.build_artist_index.merge_jsonl_files")
@patch("data_pipeline.defs.assets.build_artist_index.settings")
def test_build_artist_index_merge_and_clean(mock_settings, mock_merge, mock_dedup, mock_pl):
    """Test the merge and deduplication asset."""
    # Setup mock settings
    mock_settings.datasets_dirpath = Path("/tmp/datasets")
    mock_settings.artist_index_filepath = Path("/tmp/datasets/artist_index.jsonl")
    
    # Mock Polars lazy frame and collection
    mock_lf = mock_pl.scan_ndjson.return_value
    mock_clean_lf = mock_dedup.return_value
    mock_df = mock_clean_lf.collect.return_value
    mock_df.__len__.return_value = 100 # Simulate 100 records
    
    context = build_asset_context()
    
    # Execute the asset
    result = build_artist_index(context)
    
    # Verify result path
    assert result == str(mock_settings.artist_index_filepath)
    
    # Verify merge was called FIRST
    mock_merge.assert_called_once()
    
    # Verify Polars scan was called on the merged file
    mock_pl.scan_ndjson.assert_called_once_with(mock_settings.artist_index_filepath)
    
    # Verify deduplication was called
    mock_dedup.assert_called_once_with(
        mock_lf,
        sort_col="start_date",
        unique_cols=["artist_uri", "name"],
        descending=False
    )
    
    # Verify write back to the same path
    mock_df.write_ndjson.assert_called_once_with(mock_settings.artist_index_filepath)
