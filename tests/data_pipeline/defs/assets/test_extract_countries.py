# -----------------------------------------------------------
# Unit Tests for extract_countries
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

from data_pipeline.defs.assets.extract_countries import extract_countries


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_countries.async_resolve_labels_to_qids")
@patch("data_pipeline.defs.assets.extract_countries.settings")
async def test_extract_countries(mock_settings, mock_resolve):
    """
    Test the extract_countries asset.
    """
    # Setup mock settings
    mock_settings.WIKIDATA_ACTION_API_URL = "http://wd.api"
    mock_settings.WIKIDATA_CACHE_DIRPATH = "/tmp/wd_cache"
    mock_settings.WIKIDATA_ACTION_REQUEST_TIMEOUT = 10
    mock_settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY = 0
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}

    # Mock Input DataFrame (artists)
    # We use a mix of duplicates and nulls to test uniqueness/filtering
    artists_lf = pl.DataFrame({
        "country": ["US", "UK", "US", None, "Germany", ""]
    }).lazy()

    # Mock Resolution map
    mock_resolve.return_value = {
        "US": "Q30",
        "UK": "Q145",
        "Germany": "Q183"
    }

    # Mock Resource
    from contextlib import asynccontextmanager
    mock_client = MagicMock()
    mock_wikidata = MagicMock()
    
    @asynccontextmanager
    async def mock_get_client(context):
        yield mock_client
    
    mock_wikidata.get_client = mock_get_client

    context = build_asset_context()
    
    # Execution
    result = await extract_countries(context, mock_wikidata, artists_lf)

    # Verifications
    assert isinstance(result, pl.DataFrame)
    assert len(result) == 3
    
    # Check content
    rows = result.to_dicts()
    names = {r["name"] for r in rows}
    qids = {r["id"] for r in rows}
    
    assert names == {"US", "UK", "Germany"}
    assert qids == {"Q30", "Q145", "Q183"}
    
    # Verify helper was called with unique non-empty names
    called_labels = mock_resolve.call_args[0][1]
    assert set(called_labels) == {"US", "UK", "Germany"}
    assert "" not in called_labels
    assert None not in called_labels
