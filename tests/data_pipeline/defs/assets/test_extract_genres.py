# -----------------------------------------------------------
# Unit Tests for extract_genres
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

from data_pipeline.defs.assets.extract_genres import extract_genres

@pytest.mark.asyncio
@patch("pathlib.Path.exists")
@patch("data_pipeline.defs.assets.extract_genres.stream_to_jsonl")
@patch("data_pipeline.defs.assets.extract_genres.fetch_sparql_query_async")
@patch("data_pipeline.defs.assets.extract_genres.async_fetch_wikidata_entities_batch")
@patch("data_pipeline.defs.assets.extract_genres.pl.read_ndjson")
@patch("data_pipeline.defs.assets.extract_genres.settings")
async def test_extract_genres(
    mock_settings,
    mock_read_ndjson,
    mock_fetch_entities,
    mock_fetch_sparql,
    mock_stream,
    mock_exists
):
    """
    Test the extract_genres asset.
    """
    mock_exists.return_value = True
    # Setup mock settings
    mock_settings.artists_filepath = Path("/tmp/datasets/artists.jsonl")
    mock_settings.albums_filepath = Path("/tmp/datasets/albums.jsonl")
    mock_settings.tracks_filepath = Path("/tmp/datasets/tracks.jsonl")
    mock_settings.genres_filepath = Path("/tmp/datasets/genres.jsonl")
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_CONCURRENT_REQUESTS = 2

    # Mock Artists Data (Source)
    # 3 artists, overlapping genres
    mock_df = pl.DataFrame({
        "id": ["Q1", "Q2", "Q3"],
        "genres": [
            ["Q101", "Q102"],  # Artist 1
            ["Q102", "Q103"],  # Artist 2 (Overlap Q102)
            ["Q104"]           # Artist 3
        ]
    })
    # read_ndjson is called 3 times (artists, albums, tracks), returning same mock is fine for this test
    mock_read_ndjson.return_value = mock_df

    # Unique Genres Expected: Q101, Q102, Q103, Q104
    
    # Mock Context
    context = build_asset_context()

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
    result = await extract_genres(context)

    # Assertions
    assert result == Path("/tmp/datasets/genres.jsonl")

    # Verify stream_to_jsonl calls
    mock_stream.assert_called_once()
    
    # Collect written items
    generator = mock_stream.call_args[0][0]
    written_genres = [g async for g in generator]
    written_ids = [g.id for g in written_genres]
    
    assert "Q101" in written_ids
    assert "Q102" in written_ids
    assert "Q103" in written_ids
    assert "Q104" in written_ids
    
    # Check content of one
    genre_a = next(g for g in written_genres if g.id == "Q101")
    assert genre_a.name == "Genre A"
    assert genre_a.aliases == ["Alias A1"]