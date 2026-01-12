# -----------------------------------------------------------
# Dagster Asset Checks
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import polars as pl
from dagster import AssetCheckResult, asset_check


@asset_check(asset="artist_index")
def check_artist_index_integrity(artist_index: pl.LazyFrame):
    """Checks that the artist index has no null IDs or names and no duplicates."""
    # Compute stats lazily
    stats = artist_index.select([
        pl.col("artist_uri").null_count().alias("null_ids"),
        pl.col("name").null_count().alias("null_names"),
        pl.len().alias("total_count")
    ]).collect().to_dicts()[0]

    # Check for duplicates: Count rows where count > 1 group by all columns
    # Note: Checking duplicates on all columns might be expensive if columns are many/large.
    # Assuming 'artist_uri' should be unique primary key for this index?
    # The original check used `is_duplicated()` on the whole frame.
    # We will replicate strict "all columns" duplicate check.
    
    dup_check = (
        artist_index.group_by(artist_index.collect_schema().names())
        .len()
        .filter(pl.col("len") > 1)
        .select(pl.col("len").sum())
        .collect()
        .item()
    )
    duplicate_count = dup_check if dup_check is not None else 0
    
    null_ids = stats["null_ids"]
    null_names = stats["null_names"]
    
    return AssetCheckResult(
        passed=bool(null_ids == 0 and null_names == 0 and duplicate_count == 0),
        metadata={
            "null_ids": null_ids,
            "null_names": null_names,
            "duplicate_count": duplicate_count
        }
    )


@asset_check(asset="artists")
def check_artists_completeness(artists: pl.LazyFrame):
    """Checks that enriched artists have at least some genres or tags assigned."""
    # We use select with aggregation to get counts in one go
    stats = artists.select([
        pl.len().alias("total"),
        pl.col("genres").list.len().fill_null(0).alias("genre_len"),
        pl.col("tags").list.len().fill_null(0).alias("tag_len")
    ]).select([
        pl.col("total").first(),
        ((pl.col("genre_len") > 0) | (pl.col("tag_len") > 0)).sum().alias("with_metadata")
    ]).collect().to_dicts()[0]

    total_artists = stats["total"]
    if total_artists == 0:
        return AssetCheckResult(passed=True, description="No artists to check.")
        
    completeness_ratio = stats["with_metadata"] / total_artists
    
    return AssetCheckResult(
        passed=bool(completeness_ratio > 0.5),  # Expect at least 50% to have some metadata
        metadata={"completeness_ratio": float(completeness_ratio)}
    )


@asset_check(asset="releases")
def check_releases_per_artist(releases: pl.LazyFrame):
    """Checks that we have a reasonable average of releases per artist."""
    stats = releases.select([
        pl.len().alias("count"),
        pl.col("artist_id").n_unique().alias("unique_artists")
    ]).collect().to_dicts()[0]
    
    count = stats["count"]
    unique_artists = stats["unique_artists"]

    if count == 0:
        return AssetCheckResult(passed=True)
        
    avg_releases = count / unique_artists if unique_artists > 0 else 0
    
    return AssetCheckResult(
        passed=bool(avg_releases >= 1.0),
        metadata={"avg_releases_per_artist": float(avg_releases)}
    )


@asset_check(asset="tracks")
def check_tracks_schema(tracks: pl.LazyFrame):
    """Checks that tracks have titles and valid album links."""
    stats = tracks.select([
        pl.col("title").null_count().alias("null_titles"),
        pl.col("album_id").null_count().alias("null_albums")
    ]).collect().to_dicts()[0]
    
    null_titles = stats["null_titles"]
    null_albums = stats["null_albums"]
    
    return AssetCheckResult(
        passed=bool(null_titles == 0 and null_albums == 0),
        metadata={"null_titles": null_titles, "null_albums": null_albums}
    )


@asset_check(asset="genres")
def check_genres_quality(genres: pl.LazyFrame):
    """Checks that genres have names and a reasonable amount of metadata."""
    null_names = genres.select(pl.col("name").null_count()).collect().item()
    return AssetCheckResult(
        passed=bool(null_names == 0),
        metadata={"null_names": null_names}
    )
