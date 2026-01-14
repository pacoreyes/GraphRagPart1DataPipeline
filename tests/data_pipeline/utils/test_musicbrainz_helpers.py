import pytest
import json
from unittest.mock import MagicMock, AsyncMock
from dagster import build_asset_context

from data_pipeline.utils.musicbrainz_helpers import (
    fetch_artist_release_groups_async,
    fetch_releases_for_group_async,
    fetch_tracks_for_release_async,
)

# Mock the network helper
@pytest.fixture
def mock_make_request(mocker):
    return mocker.patch(
        "data_pipeline.utils.musicbrainz_helpers.make_async_request_with_retries",
        new_callable=AsyncMock
    )

@pytest.fixture
def mock_cache_path(tmp_path):
    return tmp_path

@pytest.mark.asyncio
async def test_fetch_artist_release_groups_async_uncached_success(mock_make_request, mock_cache_path):
    """
    Test fetching from API when not cached.
    Mocks 2 pages of results.
    """
    # 1. Mock API Responses
    page1_data = {
        "release-groups": [
            {"id": f"rg-{i}", "primary-type": "Album"} for i in range(100)
        ]
    }
    page2_data = {
        "release-groups": [
            {"id": "rg-100", "primary-type": "Single"}
        ]
    }
    
    resp1 = MagicMock()
    resp1.json.return_value = page1_data
    resp2 = MagicMock()
    resp2.json.return_value = page2_data
    
    mock_make_request.side_effect = [resp1, resp2]

    # 2. Run
    context = build_asset_context()
    client = AsyncMock() 
    
    results = await fetch_artist_release_groups_async(
        context=context, 
        artist_mbid="mbid-test", 
        client=client,
        cache_dirpath=mock_cache_path,
        api_url="http://mock-api",
        headers={"User-Agent": "test"},
        rate_limit_delay=0.1
    )

    # 3. Verify - Helper NO LONGER FILTERS
    assert len(results) == 101
    assert results[0]["id"] == "rg-0"
    assert results[-1]["id"] == "rg-100"

    # Verify calls
    assert mock_make_request.call_count == 2
    
    # Verify Cache File was written
    cache_file = mock_cache_path / "mbid-test_release.json"
    assert cache_file.exists()

@pytest.mark.asyncio
async def test_fetch_artist_release_groups_async_cached(mock_make_request, mock_cache_path):
    """
    Test loading from cache.
    """
    # 1. Setup Cache Hit
    cache_file = mock_cache_path / "mbid-test_release.json"
    cached_data = [{"id": "cached-1"}]
    with open(cache_file, "w") as f:
        json.dump(cached_data, f)
    
    # 2. Run
    context = build_asset_context()
    client = AsyncMock()
    
    results = await fetch_artist_release_groups_async(
        context=context, 
        artist_mbid="mbid-test", 
        client=client,
        cache_dirpath=mock_cache_path,
        api_url="http://mock-api",
        headers={"User-Agent": "test"},
        rate_limit_delay=0.1
    )
    
    # 3. Verify
    assert len(results) == 1
    assert results[0]["id"] == "cached-1"
    
    # API should NOT be called
    mock_make_request.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_releases_for_group_async(mock_make_request, mock_cache_path):
    """Test fetching releases for a release group."""
    rg_data = {
        "releases": [{"id": "rel-1"}, {"id": "rel-2"}]
    }
    resp = MagicMock()
    resp.json.return_value = rg_data
    mock_make_request.return_value = resp

    context = build_asset_context()
    client = AsyncMock()
    
    # Run 1: Fetch from API
    results = await fetch_releases_for_group_async(
        context=context,
        release_group_mbid="rg-123",
        client=client,
        cache_dirpath=mock_cache_path,
        api_url="http://mock-api",
        headers={"User-Agent": "test"}
    )
    
    assert len(results) == 2
    assert results[0]["id"] == "rel-1"
    mock_make_request.assert_called_once()
    
    # Verify Cache File
    cache_file = mock_cache_path / "rg-123_releases.json"
    assert cache_file.exists()
    
    # Run 2: Fetch from Cache
    mock_make_request.reset_mock()
    results_cached = await fetch_releases_for_group_async(
        context=context,
        release_group_mbid="rg-123",
        client=client,
        cache_dirpath=mock_cache_path,
        api_url="http://mock-api",
        headers={"User-Agent": "test"}
    )
    assert results_cached == results
    mock_make_request.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_tracks_for_release_async(mock_make_request, mock_cache_path):
    """Test fetching tracks for a specific release."""
    rel_data = {
        "media": [
            {
                "tracks": [
                    {"recording": {"id": "rec-1", "title": "Track One", "length": 100}}
                ]
            }
        ]
    }
    resp = MagicMock()
    resp.json.return_value = rel_data
    mock_make_request.return_value = resp

    context = build_asset_context()
    client = AsyncMock()
    
    results = await fetch_tracks_for_release_async(
        context=context,
        release_mbid="rel-123",
        client=client,
        cache_dirpath=mock_cache_path,
        api_url="http://mock-api",
        headers={"User-Agent": "test"}
    )
    
    assert len(results) == 1
    assert results[0]["id"] == "rec-1"
    
    # Verify Cache
    cache_file = mock_cache_path / "rel-123_tracks.json"
    assert cache_file.exists()