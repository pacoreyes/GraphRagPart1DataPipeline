# -----------------------------------------------------------
# Unit Tests for extract_albums
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

from data_pipeline.defs.assets.extract_albums import extract_albums

@pytest.mark.asyncio
@patch("pathlib.Path.exists")
@patch("data_pipeline.defs.assets.extract_albums.stream_to_jsonl")
@patch("data_pipeline.defs.assets.extract_albums.fetch_sparql_query_async")
@patch("data_pipeline.defs.assets.extract_albums.pl.read_ndjson")
@patch("data_pipeline.defs.assets.extract_albums.settings")
async def test_extract_albums(
    mock_settings,
    mock_read_ndjson,
    mock_fetch_sparql,
    mock_stream,
    mock_exists
):
    """
    Test the extract_albums asset.
    """
    mock_exists.return_value = True
    # Setup mock settings
    mock_settings.artists_filepath = Path("/tmp/datasets/artists.jsonl")
    mock_settings.albums_filepath = Path("/tmp/datasets/albums.jsonl")
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2

    # Mock Artists Data
    mock_df = pl.DataFrame({
        "id": ["Q1", "Q2"]
    })
    mock_read_ndjson.return_value = mock_df

    # Mock SPARQL Response
    # Artist Q1 has Album A (2 dates)
    # Artist Q2 has Album B
    # Artist Q3 has duplicates by title: 
    #   - "Dup Title" (Q3A, 2015)
    #   - "Dup Title" (Q3B, 2005) -> Should be kept
    #   - "Dup Title" (Q3C, None)
    mock_fetch_sparql.return_value = [
        {
            "album": {"value": "http://www.wikidata.org/entity/QA"},
            "artist": {"value": "http://www.wikidata.org/entity/Q1"},
            "albumLabel": {"value": "Album A"},
            "releaseDate": {"value": "2010-01-01T00:00:00Z"},
        },
        {
            "album": {"value": "http://www.wikidata.org/entity/QA"},
            "artist": {"value": "http://www.wikidata.org/entity/Q1"},
            "albumLabel": {"value": "Album A"},
            "releaseDate": {"value": "2009-05-05T00:00:00Z"} # Earlier date
        },
        {
            "album": {"value": "http://www.wikidata.org/entity/QB"},
            "artist": {"value": "http://www.wikidata.org/entity/Q2"},
            "albumLabel": {"value": "Album B"},
            "releaseDate": {"value": "2020-10-10T00:00:00Z"}
        },
        # Duplicates for Q3
        {
            "album": {"value": "http://www.wikidata.org/entity/Q3A"},
            "artist": {"value": "http://www.wikidata.org/entity/Q3"},
            "albumLabel": {"value": "Dup Title"},
            "releaseDate": {"value": "2015-01-01T00:00:00Z"}
        },
        {
            "album": {"value": "http://www.wikidata.org/entity/Q3B"},
            "artist": {"value": "http://www.wikidata.org/entity/Q3"},
            "albumLabel": {"value": "Dup Title"},
            "releaseDate": {"value": "2005-01-01T00:00:00Z"}
        },
        {
            "album": {"value": "http://www.wikidata.org/entity/Q3C"},
            "artist": {"value": "http://www.wikidata.org/entity/Q3"},
            "albumLabel": {"value": "Dup Title"},
            "releaseDate": {"value": None}
        }
    ]

    # Mock Context
    context = build_asset_context()

    # Execution
    result = await extract_albums(context)

    # Assertions
    assert result == Path("/tmp/datasets/albums.jsonl")
    
    # Verify stream_to_jsonl calls
    mock_stream.assert_called_once()
    
    # Collect written items
    generator = mock_stream.call_args[0][0]
    written_albums = [a async for a in generator]
    
    # Expected: Q1(QA), Q2(QB), Q3(Q3B) -> 3 items
    assert len(written_albums) == 3
    
    # Check Album A (Earliest year 2009)
    album_a = next(a for a in written_albums if a.id == "QA")
    assert album_a.title == "Album A"
    assert album_a.year == 2009
    assert album_a.artist_id == "Q1"
    
    # Check Album B
    album_b = next(a for a in written_albums if a.id == "QB")
    assert album_b.title == "Album B"
    assert album_b.year == 2020
    assert album_b.artist_id == "Q2"

    # Check Dup Title (Earliest year 2005, ID Q3B)
    album_dup = next(a for a in written_albums if a.artist_id == "Q3")
    assert album_dup.title == "Dup Title"
    assert album_dup.year == 2005
    assert album_dup.id == "Q3B"