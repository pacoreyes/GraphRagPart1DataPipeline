# -----------------------------------------------------------
# Unit Tests for artists
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
import httpx
from pathlib import Path
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_artists import extract_artists, _is_latin_name
from data_pipeline.defs.resources import LastFmResource


def test_is_latin_name():
    """Test the Latin name validator helper."""
    assert _is_latin_name("The Beatles") is True
    assert _is_latin_name("Björk") is True  # Latin-1
    assert _is_latin_name("Sigur Rós") is True  # Latin-1
    assert _is_latin_name("Dvořák") is True  # Latin Extended
    assert _is_latin_name("Beyoncé") is True
    assert _is_latin_name("Mötley Crüe") is True
    
    # Non-Latin
    assert _is_latin_name("Битлз") is False  # Cyrillic
    assert _is_latin_name("BTS (방탄소년단)") is False  # Korean
    assert _is_latin_name("坂本龍一") is False  # Japanese
    assert _is_latin_name("The Beatles (Битлз)") is False  # Mixed
    
    # Edge cases
    assert _is_latin_name("") is False
    assert _is_latin_name(None) is False


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_artists.pl.scan_ndjson")
@patch("data_pipeline.defs.assets.extract_artists.async_append_jsonl")
@patch("data_pipeline.defs.assets.extract_artists.async_fetch_lastfm_data_with_cache")
@patch("data_pipeline.defs.assets.extract_artists.async_resolve_qids_to_labels")
@patch("data_pipeline.defs.assets.extract_artists.async_fetch_wikidata_entities_batch")
@patch("data_pipeline.defs.assets.extract_artists.settings")
async def test_extract_artist(
    mock_settings,
    mock_fetch_entities,
    mock_resolve_labels,
    mock_lastfm,
    mock_append,
    mock_scan
):
    """
    Test the artists asset.
    """
    # Setup mock settings
    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10
    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10
    mock_settings.LASTFM_CONCURRENT_REQUESTS = 2
    mock_settings.LASTFM_API_URL = "http://mock.last.fm"
    mock_settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX = "http://www.wikidata.org/entity/"
    mock_settings.DATASETS_DIRPATH = Path("/tmp")

    # Mock Input DataFrame (artist_index)
    # Added "Non-Latin Artist" (Cyrillic) which should be filtered
    mock_index_df = pl.DataFrame({
        "artist_uri": [
            "http://www.wikidata.org/entity/Q1",
            "http://www.wikidata.org/entity/Q2",
            "http://www.wikidata.org/entity/Q3",
            "http://www.wikidata.org/entity/Q4",
            "http://www.wikidata.org/entity/Q5",
            "http://www.wikidata.org/entity/Q7"
        ],
        "name": [
            "Artist 1", 
            "Artist 2", 
            "Artist 3", 
            "Artist 4", 
            "Битлз",
            "No Country Artist"
        ]
    }).lazy()

    # Mock Wikidata Entity Data
    # Q7: Has MBID but NO Country claims
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
            "sitelinks": {},
            "claims": {},
            "aliases": {}
        },
        "Q4": {
            "sitelinks": {"enwiki": {"title": "Artist 4"}},
            "claims": {}, # NO COUNTRY
            "aliases": {}
        },
        "Q7": {
            "sitelinks": {"enwiki": {"title": "No Country Artist"}},
            "claims": {
                "P434": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "mbid-7"}}}]
            },
            "aliases": {}
        }
    }

    # Mock Label Resolution (Countries)
    mock_resolve_labels.return_value = {
        "Q100": "Country A",
        "Q101": "Country B"
    }

    # Mock Last.fm Data (Generic Side Effect)
    async def lastfm_side_effect(context, params, cache_key, api_key=None, client=None, **kwargs):
        if "mbid" in params and params["mbid"] == "mbid-1":
            return {"artist": {"mbid": "mbid-1", "tags": {"tag": [{"name": "Tag 1"}]}, "similar": {"artist": [{"name": "Sim 1"}]}}}
        if "artist" in params and params["artist"] == "Artist 1":
             return {"artist": {"mbid": "mbid-1", "tags": {"tag": [{"name": "Tag 1"}]}, "similar": {"artist": [{"name": "Sim 1"}]}}}
        return None

    mock_lastfm.side_effect = lastfm_side_effect
    
    # Mock Scan Result (This is what we assert on)
    expected_result = pl.DataFrame({"name": ["Artist 1"]}).lazy()
    mock_scan.return_value = expected_result

    from contextlib import asynccontextmanager

    # Mock Context and Resource
    lastfm = LastFmResource(api_key="dummy_key")
    context = build_asset_context()
    mock_client = MagicMock(spec=httpx.AsyncClient)

    mock_wikidata = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield mock_client
    mock_wikidata.get_client = mock_yield

    # Execution
    result_df = await extract_artists(context, mock_wikidata, lastfm, mock_index_df)

    assert isinstance(result_df, pl.LazyFrame)
    # Verify processing ran
    assert mock_append.called
    assert mock_scan.called
    
    # Verify that async_fetch_wikidata_entities_batch was called with correct QIDs
    # It should NOT contain Q5 (Битлз)
    call_args = mock_fetch_entities.call_args
    if call_args:
        qids_arg = call_args[0][1] # The second argument is the list of QIDs
        assert "Q5" not in qids_arg
        assert "Q1" in qids_arg

    # Verify MBID Filtering (Q2, Q3, Q4 have no P434/MBID, so they should be skipped)
    # Only Q1 has MBID.
    # Check that append was called with only 1 item
    append_args = mock_append.call_args[0]
    appended_data = append_args[1] # The list of dicts
    assert len(appended_data) == 1
    assert appended_data[0].name == "Artist 1"
    assert appended_data[0].id == "Q1"