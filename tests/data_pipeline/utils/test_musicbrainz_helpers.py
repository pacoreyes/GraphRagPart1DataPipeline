import pytest
import json
from unittest.mock import MagicMock, AsyncMock
from dagster import build_asset_context

from data_pipeline.utils.musicbrainz_helpers import (
    fetch_artist_release_groups_async,
    fetch_tracks_for_release_group_async,
)

# Mock the network helper
@pytest.fixture
def mock_make_request(mocker):
    return mocker.patch(
        "data_pipeline.utils.musicbrainz_helpers.make_async_request_with_retries",
        new_callable=AsyncMock
    )

@pytest.fixture
def mock_settings_path(mocker, tmp_path):
    # Patch the settings object imported in the module
    mocker.patch("data_pipeline.utils.musicbrainz_helpers.settings.MUSICBRAINZ_CACHE_DIRPATH", tmp_path)
    return tmp_path

@pytest.mark.asyncio
async def test_fetch_artist_release_groups_async_uncached_success(mock_make_request, mock_settings_path):
    """
    Test fetching from API when not cached.
    Mocks 2 pages of results.
    """
    # 1. No setup needed for cache miss (dir is empty)

    # 2. Mock API Responses
    # Page 1: 100 items (full page), mixed types
    page1_data = {
        "release-groups": [
            {"id": f"alb-{i}", "primary-type": "Album", "secondary-types": None} for i in range(100)
        ]
    }
    # Page 2: 5 items, end of list
    page2_data = {
        "release-groups": [
            {"id": "sing-1", "primary-type": "Single", "secondary-types": []},
            {"id": "comp-1", "primary-type": "Album", "secondary-types": ["Compilation"]}, # Should be filtered out
            {"id": "other-1", "primary-type": "Other", "secondary-types": None} # Should be filtered out
        ]
    }
    
    # Setup side_effect for multiple calls
    resp1 = MagicMock()
    resp1.json.return_value = page1_data
    resp2 = MagicMock()
    resp2.json.return_value = page2_data
    
    mock_make_request.side_effect = [resp1, resp2]

    # 3. Run
    context = build_asset_context()
    client = AsyncMock() # Dummy client
    
    results = await fetch_artist_release_groups_async(context, "mbid-test", client)

    # 4. Verify
    # Page 1: 100 albums -> All kept
    # Page 2: 1 Single (kept), 1 Compilation (filtered), 1 Other (filtered) -> 1 kept
    # Total: 101
    assert len(results) == 101
    assert results[0]["id"] == "alb-0"
    assert results[-1]["id"] == "sing-1"

    # Verify calls
    assert mock_make_request.call_count == 2
    # Check args for pagination
    call1_args = mock_make_request.call_args_list[0]
    call2_args = mock_make_request.call_args_list[1]
    
    assert call1_args.kwargs["params"]["offset"] == 0
    assert call2_args.kwargs["params"]["offset"] == 100
    
    # Verify Cache File was written
    cache_file = mock_settings_path / "mbid-test_release.json"
    assert cache_file.exists()
    with open(cache_file, "r") as f:
        cached_data = json.load(f)
    assert len(cached_data) == 103 # Cached data is unfiltered (ALL items)

@pytest.mark.asyncio
async def test_fetch_artist_release_groups_async_cached(mock_make_request, mock_settings_path):
    """
    Test loading from cache.
    """
    # 1. Setup Cache Hit
    cache_file = mock_settings_path / "mbid-test_release.json"
    cached_data = [
        {"id": "cached-1", "primary-type": "Album", "secondary-types": None},
        {"id": "cached-2", "primary-type": "EP", "secondary-types": None} # Should be filtered out by logic
    ]
    with open(cache_file, "w") as f:
        json.dump(cached_data, f)
    
    # 2. Run
    context = build_asset_context()
    client = AsyncMock()
    
    results = await fetch_artist_release_groups_async(context, "mbid-test", client)
    
    # 3. Verify
    assert len(results) == 1 # Only cached-1 (Album)
    assert results[0]["id"] == "cached-1"
    
    # API should NOT be called
    mock_make_request.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_tracks_for_release_group_async_success(mock_make_request, mock_settings_path):
    """
    Test the two-step process to fetch tracks for a release group.
    """
    # 1. Mock API Responses
    # Response A: Release Group details with Release list
    rg_data = {
        "releases": [
            {"id": "rel-official", "status": "Official", "date": "2020-05-01"},
            {"id": "rel-older", "status": "Official", "date": "2020-01-01"},
            {"id": "rel-promo", "status": "Promotion", "date": "2019-12-01"}
        ]
    }
    # Response B: Release details with Tracklist
    rel_data = {
        "media": [
            {
                "tracks": [
                    {"recording": {"id": "rec-1", "title": "Track One", "length": 180000}},
                    {"recording": {"id": "rec-2", "title": "Track Two", "length": 200000}}
                ]
            }
        ]
    }

    resp_rg = MagicMock()
    resp_rg.json.return_value = rg_data
    resp_rel = MagicMock()
    resp_rel.json.return_value = rel_data

    # Step A should select 'rel-older' because it's Official and earliest
    mock_make_request.side_effect = [resp_rg, resp_rel]

    # 2. Run
    context = build_asset_context()
    client = AsyncMock()
    results = await fetch_tracks_for_release_group_async(context, "rg-123", client)

    # 3. Verify
    assert len(results) == 2
    assert results[0]["id"] == "rec-1"
    assert results[1]["title"] == "Track Two"

    assert mock_make_request.call_count == 2
    # Check that Step B used the correct release ID
    call2_args = mock_make_request.call_args_list[1]
    assert "release/rel-older" in call2_args.kwargs["url"]

    # Verify Cache
    cache_file = mock_settings_path / "rg-123_tracks.json"
    assert cache_file.exists()
