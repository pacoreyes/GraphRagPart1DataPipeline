import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from dagster import build_asset_context, MaterializeResult

from data_pipeline.defs.assets.extract_tracks import extract_tracks

@pytest.fixture

def mock_fetch_releases(mocker):

    return mocker.patch(

        "data_pipeline.defs.assets.extract_tracks.fetch_releases_for_group_async",

        new_callable=AsyncMock

    )



@pytest.fixture

def mock_fetch_tracks(mocker):

    return mocker.patch(

        "data_pipeline.defs.assets.extract_tracks.fetch_tracks_for_release_async",

        new_callable=AsyncMock

    )



@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_tracks.settings")
async def test_extract_tracks_success(
    mock_settings, mock_fetch_releases, mock_fetch_tracks
):
    """
    Test that extract_tracks correctly processes releases and extracts tracks from MusicBrainz.
    """
    # Setup Mocks
    mock_settings.MUSICBRAINZ_CACHE_DIRPATH = Path("/tmp/mb_cache")
    mock_settings.DEFAULT_REQUEST_HEADERS = {"User-Agent": "test"}
    mock_settings.MUSICBRAINZ_API_URL = "http://mb.api"
    
    # 1. Setup Input Data
    releases_df = pl.DataFrame({
        "id": ["rg-123"],
        "title": ["Test Album"]
    }).lazy()

    # 2. Setup Mock Return Values
    mock_fetch_releases.return_value = [
        {"id": "rel-1", "status": "Official", "date": "2020-01-01"}
    ]
    mock_fetch_tracks.return_value = [
        {"id": "rec-1", "title": "Song A", "length": 100}
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
    results = await extract_tracks(context, mock_musicbrainz, releases_df)

    # 5. Verify
    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0].id == "rec-1"
    assert results[0].album_id == "rg-123"

@pytest.mark.asyncio
@patch("data_pipeline.defs.assets.extract_tracks.settings")
async def test_extract_tracks_empty_input(mock_settings, mock_fetch_releases, mock_fetch_tracks):
    """
    Test handling of empty input dataframe.
    """
    releases_df = pl.DataFrame(schema={"id": pl.Utf8, "title": pl.Utf8}).lazy()

    context = build_asset_context()
    mock_musicbrainz = MagicMock()
    
    results = await extract_tracks(context, mock_musicbrainz, releases_df)

    assert len(results) == 0
    mock_fetch_releases.assert_not_called()
    mock_fetch_tracks.assert_not_called()
