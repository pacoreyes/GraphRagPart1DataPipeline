# -----------------------------------------------------------
# Unit Tests for extract_tracks
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
from unittest.mock import patch
from pathlib import Path
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_tracks import extract_tracks

@pytest.mark.asyncio
@patch("pathlib.Path.exists")
@patch("data_pipeline.defs.assets.extract_tracks.stream_to_jsonl")
@patch("data_pipeline.defs.assets.extract_tracks.fetch_sparql_query_async")
@patch("data_pipeline.defs.assets.extract_tracks.pl.read_ndjson")
@patch("data_pipeline.defs.assets.extract_tracks.settings")
async def test_extract_tracks(
    mock_settings,
    mock_read_ndjson,
    mock_fetch_sparql,
    mock_stream,
    mock_exists
):
    """
    Test the extract_tracks asset.
    """
    mock_exists.return_value = True
    # Setup mock settings
    mock_settings.albums_filepath = Path("/tmp/datasets/albums.jsonl")
    mock_settings.tracks_filepath = Path("/tmp/datasets/tracks.jsonl")
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2

    # Mock Albums Data
    mock_df = pl.DataFrame({
        "id": ["Q100", "Q200"]
    })
    mock_read_ndjson.return_value = mock_df

    # Mock SPARQL Response
    # Album Q100 has Track T1 (Forward P658) and Track T2 (Reverse P361)
    # Album Q200 has Track T3
    # Album Q200 ALSO has Track T1 (Shared track/compilation case)
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
        # Track T1 duplicate (found via Reverse P361 as well) - Should be deduplicated for Q100
        {
            "album": {"value": "http://www.wikidata.org/entity/Q100"},
            "track": {"value": "http://www.wikidata.org/entity/T1"},
            "trackLabel": {"value": "Track One"},
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

    # Mock Context
    context = build_asset_context()

    # Execution
    result = await extract_tracks(context)

    # Assertions
    assert result == Path("/tmp/datasets/tracks.jsonl")
    
    # Verify stream_to_jsonl calls
    mock_stream.assert_called_once()
    
    # Collect written items
    generator = mock_stream.call_args[0][0]
    written_tracks = [t async for t in generator]
    
    # Expected: 
    # 1. T1 on Q100
    # 2. T2 on Q100
    # 3. T3 on Q200
    # 4. T1 on Q200 (New)
    assert len(written_tracks) == 4
    
    # Verify T1 appears twice with different album_ids
    t1_instances = [t for t in written_tracks if t.id == "T1"]
    assert len(t1_instances) == 2
    assert {"Q100", "Q200"} == {t.album_id for t in t1_instances}
    
    # Check aliases for T1 on Q100
    track1_q100 = next(t for t in t1_instances if t.album_id == "Q100")

    track2 = next(t for t in written_tracks if t.id == "T2")
    assert track2.title == "Track Two"
    assert track2.album_id == "Q100"

    track3 = next(t for t in written_tracks if t.id == "T3")
    assert track3.title == "Track Three"
    assert track3.album_id == "Q200"

@pytest.mark.asyncio
@patch("pathlib.Path.exists")
@patch("data_pipeline.defs.assets.extract_tracks.stream_to_jsonl")
@patch("data_pipeline.defs.assets.extract_tracks.pl.read_ndjson")
@patch("data_pipeline.defs.assets.extract_tracks.settings")
async def test_extract_tracks_empty_albums(
    mock_settings,
    mock_read_ndjson,
    mock_stream,
    mock_exists
):
    """
    Test extract_tracks with empty albums file.
    """
    mock_exists.return_value = True
    mock_settings.albums_filepath = Path("/tmp/datasets/albums.jsonl")
    mock_settings.tracks_filepath = Path("/tmp/datasets/tracks.jsonl")
    
    # Mock Empty DataFrame
    mock_df = pl.DataFrame({"id": []})
    mock_read_ndjson.return_value = mock_df
    
    context = build_asset_context()
    
    result = await extract_tracks(context)
    
    # Should write empty list and return path
    mock_stream.assert_called_once_with([], mock_settings.tracks_filepath)
    assert result == mock_settings.tracks_filepath