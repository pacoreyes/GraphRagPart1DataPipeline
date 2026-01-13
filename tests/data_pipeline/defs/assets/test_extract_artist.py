# -----------------------------------------------------------
# Unit Tests for artists
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
import polars as pl
from dagster import build_asset_context, MaterializeResult

from data_pipeline.defs.assets.extract_artists import extract_artists, _is_latin_name
from data_pipeline.defs.resources import LastFmResource
from data_pipeline.utils.network_helpers import AsyncClient


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


@patch("data_pipeline.defs.assets.extract_artists.async_fetch_lastfm_data_with_cache")


@patch("data_pipeline.defs.assets.extract_artists.async_resolve_qids_to_labels")


@patch("data_pipeline.defs.assets.extract_artists.async_fetch_wikidata_entities_batch")


@patch("data_pipeline.defs.assets.extract_artists.settings")


async def test_extract_artist(


    mock_settings,


    mock_fetch_entities,


    mock_resolve_labels,


    mock_lastfm,


):


    """


    Test the artists asset.


    """


    # Setup mock settings


    mock_settings.WIKIDATA_ACTION_BATCH_SIZE = 10


    mock_settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT = 10


    mock_settings.LASTFM_CONCURRENT_REQUESTS = 2


    mock_settings.LASTFM_API_URL = "http://mock.last.fm"


    mock_settings.LAST_FM_CACHE_DIRPATH = Path("/tmp/lastfm")


    mock_settings.LASTFM_REQUEST_TIMEOUT = 30


    mock_settings.LASTFM_RATE_LIMIT_DELAY = 0


    mock_settings.WIKIDATA_CONCEPT_BASE_URI_PREFIX = "http://www.wikidata.org/entity/"


    mock_settings.WIKIDATA_FALLBACK_LANGUAGES = ["en"]


    mock_settings.WIKIDATA_ACTION_API_URL = "http://wd.api"


    mock_settings.WIKIDATA_CACHE_DIRPATH = Path("/tmp/wd_cache")


    mock_settings.WIKIDATA_ACTION_REQUEST_TIMEOUT = 10


    mock_settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY = 0


    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}





    # Mock Input DataFrame (artist_index)


    mock_index_df = pl.DataFrame({


        "artist_uri": [


            "http://www.wikidata.org/entity/Q1",


            "http://www.wikidata.org/entity/Q5",


        ],


        "name": [


            "Artist 1", 


            "Битлз",


        ]


    }).lazy()





    # Mock Wikidata Entity Data


    mock_fetch_entities.return_value = {


        "Q1": {


            "sitelinks": {"enwiki": {"title": "Artist 1"}},


            "claims": {


                "P495": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q100"}}}}],


                "P434": [{"mainsnak": {"snaktype": "value", "datavalue": {"type": "string", "value": "mbid-1"}}}]


            },


            "aliases": {"en": [{"value": "Alias 1"}]}


        }


    }





    # Mock Label Resolution


    mock_resolve_labels.return_value = {"Q100": "Country A"}





    # Mock Last.fm Data


    mock_lastfm.return_value = {"artist": {"mbid": "mbid-1", "tags": {"tag": []}, "similar": {"artist": []}}}





    from contextlib import asynccontextmanager





    # Mock Resource


    lastfm = LastFmResource(api_key="dummy_key")


    context = build_asset_context()


    mock_client = MagicMock(spec=AsyncClient)





    mock_wikidata = MagicMock()


    @asynccontextmanager


    async def mock_yield(context):


        yield mock_client


        mock_wikidata.get_client = mock_yield


    


        # Execution


        results = await extract_artists(context, mock_wikidata, lastfm, mock_index_df)


    


        assert isinstance(results, list)


        assert len(results) == 1


        assert results[0].name == "Artist 1"


        assert results[0].id == "Q1"


        


        # Verify that non-Latin (Q5) was filtered


        mock_fetch_entities.assert_called_once()


        qids_called = mock_fetch_entities.call_args[0][1]


        assert "Q5" not in qids_called


    

