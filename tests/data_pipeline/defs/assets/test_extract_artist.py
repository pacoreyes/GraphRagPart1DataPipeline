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
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_artists import extract_artists
from data_pipeline.defs.resources import ApiConfiguration

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_artists.async_fetch_lastfm_data_with_cache")
@patch("data_pipeline.defs.assets.extract_artists.async_resolve_qids_to_labels")
@patch("data_pipeline.defs.assets.extract_artists.async_fetch_wikidata_entities_batch")
@patch("data_pipeline.defs.assets.extract_artists.settings")
async def test_extract_artist(
    mock_settings,
    mock_fetch_entities,
    mock_resolve_labels,
    mock_lastfm
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

    # Mock Input DataFrame (artist_index)
    mock_index_df = pl.DataFrame({
        "artist_uri": [
            "http://www.wikidata.org/entity/Q1",
            "http://www.wikidata.org/entity/Q2",
            "http://www.wikidata.org/entity/Q3",
            "http://www.wikidata.org/entity/Q4"
        ],
        "name": ["Artist 1", "Artist 2", "Artist 3", "Artist 4"]
    })

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
            "sitelinks": {}, 
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

    # Mock Last.fm Data (Generic Side Effect)
    async def lastfm_side_effect(context, params, cache_key, client=None):
        if "mbid" in params and params["mbid"] == "mbid-1":
            return {"artist": {"mbid": "mbid-1", "tags": {"tag": [{"name": "Tag 1"}]}, "similar": {"artist": [{"name": "Sim 1"}]}}}
        if "artist" in params and params["artist"] == "Artist 1":
             return {"artist": {"mbid": "mbid-1", "tags": {"tag": [{"name": "Tag 1"}]}, "similar": {"artist": [{"name": "Sim 1"}]}}}
        return None

    mock_lastfm.side_effect = lastfm_side_effect

    # Mock Context and Resource
    api_config = ApiConfiguration(lastfm_api_key="dummy_key", nomic_api_key="dummy")
    context = build_asset_context()
    mock_client = MagicMock(spec=httpx.AsyncClient)

    # Execution
    result_df = await extract_artists(context, mock_client, api_config, mock_index_df)

    # Assertions
    assert isinstance(result_df, pl.DataFrame)
    assert len(result_df) == 3
    
    artist1 = result_df.filter(pl.col("id") == "Q1").to_dicts()[0]
    assert artist1["name"] == "Artist 1"
    assert artist1["country"] == "Country A"
    assert "Q200" in artist1["genres"]
