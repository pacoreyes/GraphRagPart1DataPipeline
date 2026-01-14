import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock
from contextlib import asynccontextmanager
from dagster import build_asset_context

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


@pytest.fixture
def mock_select_best_release(mocker):
    return mocker.patch(
        "data_pipeline.defs.assets.extract_tracks.select_best_release"
    )


@pytest.fixture
def mock_musicbrainz():
    """Creates a mock MusicBrainzResource."""
    mock_resource = MagicMock()
    mock_resource.api_url = "http://mb.api"
    mock_resource.rate_limit_delay = 0
    mock_resource.cache_dir = "/tmp/mb_cache"

    @asynccontextmanager
    async def mock_get_client(context):
        yield MagicMock()

    mock_resource.get_client = mock_get_client
    return mock_resource


@pytest.mark.asyncio
async def test_extract_tracks_success(
    mock_fetch_releases, mock_fetch_tracks, mock_select_best_release, mock_musicbrainz
):
    """
    Test that extract_tracks correctly processes releases and extracts tracks from MusicBrainz.
    """
    # 1. Setup Input Data
    releases_df = pl.DataFrame({
        "id": ["rg-123"],
        "title": ["Test Album"]
    }).lazy()

    # 2. Setup Mock Return Values
    mock_fetch_releases.return_value = [
        {"id": "rel-1", "status": "Official", "date": "2020-01-01"}
    ]
    mock_select_best_release.return_value = {"id": "rel-1", "status": "Official", "date": "2020-01-01"}
    mock_fetch_tracks.return_value = [
        {"id": "rec-1", "title": "Song A", "length": 100}
    ]

    # 3. Create Context & Run Asset
    context = build_asset_context()
    results = await extract_tracks(context, mock_musicbrainz, releases_df)

    # 4. Verify
    assert isinstance(results, list)
    assert len(results) == 1
    assert results[0].id == "rec-1"
    assert results[0].album_id == "rg-123"


@pytest.mark.asyncio
async def test_extract_tracks_empty_input(
    mock_fetch_releases, mock_fetch_tracks, mock_select_best_release, mock_musicbrainz
):
    """
    Test handling of empty input dataframe.
    """
    releases_df = pl.DataFrame(schema={"id": pl.Utf8, "title": pl.Utf8}).lazy()

    context = build_asset_context()
    results = await extract_tracks(context, mock_musicbrainz, releases_df)

    assert len(results) == 0
    mock_fetch_releases.assert_not_called()
    mock_fetch_tracks.assert_not_called()
