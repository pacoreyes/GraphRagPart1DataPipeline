import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from dagster import AssetExecutionContext, build_asset_context, MaterializeResult

from data_pipeline.defs.assets.extract_releases import extract_releases

# Mock the helper function to avoid network calls
@pytest.fixture
def mock_fetch_release_groups(mocker):
    return mocker.patch(
        "data_pipeline.defs.assets.extract_releases.fetch_artist_release_groups_async",
        new_callable=AsyncMock
    )

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_releases.settings")
async def test_extract_releases_success(
    mock_settings, mock_fetch_release_groups
):
    """
    Test that extract_releases correctly processes artists and extracts releases.
    """
    # Setup Mocks
    mock_settings.MUSICBRAINZ_CACHE_DIRPATH = Path("/tmp/mb_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.MUSICBRAINZ_API_URL = "http://mb.api"
    
    # 1. Setup Input Data
    artists_df = pl.DataFrame({
        "id": ["Q123"],
        "mbid": ["mbid-123"],
        "name": ["Test Artist"]
    }).lazy()

    # 2. Setup Mock Return Value
    mock_fetch_release_groups.return_value = [
        {
            "id": "rel-1",
            "title": "Test Album",
            "first-release-date": "2020-01-01",
            "primary-type": "Album"
        }
    ]

    from contextlib import asynccontextmanager

    # 3. Create Context & Resource
    context = build_asset_context()
    mock_musicbrainz = MagicMock()
    @asynccontextmanager
    async def mock_yield(context):
        yield MagicMock()
    mock_musicbrainz.get_client = mock_yield
    mock_musicbrainz.api_url = "http://mb.api"
    mock_musicbrainz.rate_limit_delay = 0

    # 4. Run Asset
    results = await extract_releases(context, mock_musicbrainz, artists_df)

    # 5. Verify Results
    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0].id == "rel-1"
    assert results[0].artist_id == "Q123"

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_releases.settings")
async def test_extract_releases_empty_input(mock_settings, mock_fetch_release_groups):
    """
    Test handling of empty input dataframe.
    """
    artists_df = pl.DataFrame(schema={"id": pl.Utf8, "mbid": pl.Utf8, "name": pl.Utf8}).lazy()

    context = build_asset_context()
    mock_musicbrainz = MagicMock()
    
    results = await extract_releases(context, mock_musicbrainz, artists_df)

    assert len(results) == 0
    mock_fetch_release_groups.assert_not_called()

