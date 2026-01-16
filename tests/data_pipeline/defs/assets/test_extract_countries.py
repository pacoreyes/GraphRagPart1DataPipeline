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
from data_pipeline.models import Country


@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_countries.async_resolve_labels_to_qids")
async def test_extract_countries(mock_resolve):
    """
    Test the extract_countries asset.
    """
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
    
    # Configure Resource Attributes
    mock_wikidata.api_url = "http://wd.api"
    mock_wikidata.cache_dir = "/tmp/wd_cache"
    mock_wikidata.timeout = 10
    mock_wikidata.rate_limit_delay = 0.0

    @asynccontextmanager
    async def mock_get_client(context):
        yield mock_client
    
    mock_wikidata.get_client = mock_get_client

    context = build_asset_context()
    
    # Execution
    result = await extract_countries(context, mock_wikidata, artists_lf)

    # Verifications
    assert isinstance(result, list)
    assert len(result) == 3
    assert all(isinstance(item, Country) for item in result)
    
    # Check content
    names = {c.name for c in result}
    qids = {c.id for c in result}
    
    assert names == {"US", "UK", "Germany"}
    assert qids == {"Q30", "Q145", "Q183"}
    
    # Verify helper was called with unique non-empty names
    called_labels = mock_resolve.call_args[0][1]
    assert set(called_labels) == {"US", "UK", "Germany"}
    assert "" not in called_labels
    assert None not in called_labels
    
    # Verify resource attributes were passed
    _, args, kwargs = mock_resolve.mock_calls[0]
    # args: (context, country_names)
    # kwargs: api_url, cache_dir, timeout, rate_limit_delay, client
    assert kwargs["api_url"] == "http://wd.api"
    assert str(kwargs["cache_dir"]) == "/tmp/wd_cache"
    assert kwargs["timeout"] == 10
    assert kwargs["rate_limit_delay"] == 0.0