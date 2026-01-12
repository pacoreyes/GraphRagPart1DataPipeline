import pytest
import polars as pl
from unittest.mock import MagicMock, AsyncMock
from dagster import build_asset_context

from data_pipeline.defs.assets.extract_tracks import extract_tracks

@pytest.fixture
def mock_fetch_tracks(mocker):
    return mocker.patch(
        "data_pipeline.defs.assets.extract_tracks.fetch_tracks_for_release_group_async",
        new_callable=AsyncMock
    )

@pytest.mark.asyncio
async def test_extract_tracks_success(mock_fetch_tracks):
    """
    Test that extract_tracks correctly processes releases and extracts tracks from MusicBrainz.
    """
    # 1. Setup Input Data
    releases_df = pl.DataFrame({
        "id": ["rg-123"],
        "title": ["Test Album"]
    }).lazy()

    # 2. Setup Mock Return Value
    mock_fetch_tracks.return_value = [
        {"id": "rec-1", "title": "Song A", "length": 100},
        {"id": "rec-2", "title": "Song B", "length": 200}
    ]

    # 3. Run Asset
    context = build_asset_context()
    result_lf = await extract_tracks(context, releases_df)
    result = result_lf.collect()

    # 4. Verify
    assert result.height == 2
    assert result.row(0, named=True)["id"] == "rec-1"
    assert result.row(0, named=True)["album_id"] == "rg-123"
    assert result.row(1, named=True)["title"] == "Song B"

    assert mock_fetch_tracks.call_count == 1
    args, _ = mock_fetch_tracks.call_args
    assert args[1] == "rg-123"

@pytest.mark.asyncio
async def test_extract_tracks_empty_input(mock_fetch_tracks):
    """
    Test handling of empty input dataframe.
    """
    releases_df = pl.DataFrame(schema={"id": pl.Utf8, "title": pl.Utf8}).lazy()

    context = build_asset_context()
    result_lf = await extract_tracks(context, releases_df)
    result = result_lf.collect()

    assert result.height == 0
    mock_fetch_tracks.assert_not_called()