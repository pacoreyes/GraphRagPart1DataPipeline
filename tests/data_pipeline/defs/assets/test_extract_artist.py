# -----------------------------------------------------------
# Unit Tests for extract_artist
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

from data_pipeline.defs.assets.extract_artists import extract_artist
from data_pipeline.models import Artist
from data_pipeline.defs.resources import ApiConfiguration

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_artists.stream_to_jsonl")
@patch("data_pipeline.defs.assets.extract_artists.async_get_artist_info_with_fallback")
@patch("data_pipeline.defs.assets.extract_artists.async_resolve_qids_to_labels")
@patch("data_pipeline.defs.assets.extract_artists.async_fetch_wikidata_entities_batch")
@patch("data_pipeline.defs.assets.extract_artists.pl.read_ndjson")
@patch("data_pipeline.defs.assets.extract_artists.settings")
async def test_extract_artist(
    mock_settings,
    mock_read_ndjson,
    mock_fetch_entities,
    mock_resolve_labels,
    mock_lastfm,
    mock_stream
):
    """
    Test the extract_artist asset.
    """
    # Setup mock settings
    mock_settings.artist_index_filepath = Path("/tmp/dummy_artist_index.jsonl")
    mock_settings.datasets_dirpath = Path("/tmp/datasets")
    mock_settings.artists_filepath = Path("/tmp/datasets/artists.jsonl")
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.LASTFM_CONCURRENT_REQUESTS = 2
    mock_settings.LASTFM_API_URL = "http://mock.last.fm"

    # Mock Artist Index Data
    mock_df = pl.DataFrame({
        "artist_uri": [
            "http://www.wikidata.org/entity/Q1",
            "http://www.wikidata.org/entity/Q2",
            "http://www.wikidata.org/entity/Q3", # Artist 3: No Wikipedia URL
            "http://www.wikidata.org/entity/Q4"  # Artist 4: No Country
        ],
        "name": ["Artist 1", "Artist 2", "Artist 3", "Artist 4"],
        "wikipedia_url": [None, None, None, None]
    })
    mock_read_ndjson.return_value = mock_df

    # Mock Wikidata Entity Data
    mock_fetch_entities.return_value = {
        "Q1": {
            "sitelinks": {"enwiki": {"title": "Artist 1"}},
            "claims": {
                "P495": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q100"}}}}],
                "P136": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q200"}}}}],
                "P101": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q201"}}}}],
                "P434": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "mbid-1"}}}]
            },
            "aliases": {"en": [{"value": "Alias 1"}]}
        },
        "Q2": {
            "sitelinks": {"enwiki": {"title": "Artist 2"}},
            "claims": {
                "P27": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q101"}}}}],
            },
            "aliases": {}
        },
        "Q3": {
            "sitelinks": {}, # NO WIKIPEDIA URL
            "claims": {},
            "aliases": {}
        },
        "Q4": {
            "sitelinks": {"enwiki": {"title": "Artist 4"}},
            "claims": {}, # NO COUNTRY
            "aliases": {}
        }
    }

    # Mock Label Resolution (Countries)
    mock_resolve_labels.return_value = {
        "Q100": "Country A",
        "Q101": "Country B"
    }

    # Mock Last.fm Data
    mock_lastfm.side_effect = [
        {
            "artist": {
                "mbid": "mbid-1",
                "tags": {"tag": [{"name": "Tag 1"}, {"name": "Tag 2"}]},
                "similar": {"artist": [{"name": "Sim 1"}]}
            }
        },
        None,
        None,
        None
    ]

    # Mock Context
    api_config = ApiConfiguration(lastfm_api_key="dummy_key", nomic_api_key="dummy")
    context = build_asset_context(resources={"api_config": api_config})

    # Execution
    result = await extract_artist(context)

    # Assertions
    assert result == Path("/tmp/datasets/artists.jsonl")
    
    # Verify stream_to_jsonl was called once
    mock_stream.assert_called_once()
    
    # Consume the generator to trigger the logic and verify items
    generator = mock_stream.call_args[0][0] # First arg is items
    written_items = [i async for i in generator]
    
    assert len(written_items) == 4
    
    # Verify Last.fm calls (All 4 calls expected now)
    assert mock_lastfm.call_count == 4
    
    artist1: Artist = written_items[0]
    assert artist1.id == "Q1"
    assert artist1.name == "Artist 1"
    assert artist1.country == "Country A"
    assert "Q200" in artist1.genres
    
    artist2: Artist = written_items[1]
    assert artist2.id == "Q2"
    assert artist2.country == "Country B"
    
    artist3: Artist = written_items[2]
    assert artist3.id == "Q3"
    
    artist4: Artist = written_items[3]
    assert artist4.id == "Q4"
    assert artist4.country is None