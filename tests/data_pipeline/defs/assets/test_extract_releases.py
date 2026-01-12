import pytest
import polars as pl
from unittest.mock import MagicMock, AsyncMock
from dagster import AssetExecutionContext, build_asset_context

from data_pipeline.defs.assets.extract_releases import extract_releases

# Mock the helper function to avoid network calls
@pytest.fixture
def mock_fetch_release_groups(mocker):
    return mocker.patch(
        "data_pipeline.defs.assets.extract_releases.fetch_artist_release_groups_async",
        new_callable=AsyncMock
    )

@pytest.mark.asyncio
async def test_extract_releases_success(mock_fetch_release_groups):
    """
    Test that extract_releases correctly processes artists and extracts releases.
    """
    # 1. Setup Input Data
    # Artist with valid MBID
    artists_df = pl.DataFrame({
        "id": ["Q123"],
        "mbid": ["mbid-123"],
        "name": ["Test Artist"]
    }).lazy()

    # 2. Setup Mock Return Value
    # Return two releases: one valid Album, one Single (handled by logic)
    # The helper already filters for Album/Single, so we just return what the helper would return.
    mock_fetch_release_groups.return_value = [
        {
            "id": "rel-1",
            "title": "Test Album",
            "first-release-date": "2020-01-01",
            "primary-type": "Album"
        },
        {
            "id": "rel-2",
            "title": "Test Single",
            "first-release-date": "2021",
            "primary-type": "Single"
        }
    ]

    # 3. Create Context
    context = build_asset_context()

    # 4. Run Asset
    result_lf = await extract_releases(context, artists_df)
    result = result_lf.collect()

    # 5. Verify Results
    assert result.height == 2
    
    # Check Row 1
    row1 = result.row(0, named=True)
    assert row1["id"] == "rel-1"
    assert row1["title"] == "Test Album"
    assert row1["year"] == 2020
    assert row1["artist_id"] == "Q123"

    # Check Row 2
    row2 = result.row(1, named=True)
    assert row2["id"] == "rel-2"
    assert row2["year"] == 2021
    
    # Verify mock was called correctly
    # Note: We can't easily check the client instance passed, but we can check the context and mbid
    assert mock_fetch_release_groups.call_count == 1
    args, _ = mock_fetch_release_groups.call_args
    assert args[1] == "mbid-123"  # 2nd arg is artist_mbid


@pytest.mark.asyncio
async def test_extract_releases_empty_input(mock_fetch_release_groups):
    """
    Test handling of empty input dataframe.
    """
    artists_df = pl.DataFrame(schema={"id": pl.Utf8, "mbid": pl.Utf8, "name": pl.Utf8}).lazy()

    context = build_asset_context()
    result_lf = await extract_releases(context, artists_df)
    result = result_lf.collect()

    assert result.height == 0
    mock_fetch_release_groups.assert_not_called()
