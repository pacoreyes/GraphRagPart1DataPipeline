# -----------------------------------------------------------
# Unit Tests for merge_wikipedia_articles
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import pytest
import polars as pl
from dagster import build_asset_context

from data_pipeline.defs.assets.merge_wikipedia_articles import merge_wikipedia_articles


def test_merge_wikipedia_articles_combines_both():
    """Test that merge combines artist and genre articles."""
    # Create mock artist articles
    artists_df = pl.DataFrame([
        {
            "id": "Q1_chunk_1",
            "article": "Artist article content",
            "metadata": {
                "title": "Artist One",
                "name": "Artist One",
                "entity_type": "artist",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Artist_One",
                "wikidata_uri": "http://www.wikidata.org/entity/Q1",
                "chunk_index": 1,
                "total_chunks": 1
            }
        }
    ])

    # Create mock genre articles
    genres_df = pl.DataFrame([
        {
            "id": "Q2_chunk_1",
            "article": "Genre article content",
            "metadata": {
                "title": "Techno",
                "name": "Techno",
                "entity_type": "genre",
                "wikipedia_url": "https://en.wikipedia.org/wiki/Techno",
                "wikidata_uri": "http://www.wikidata.org/entity/Q2",
                "chunk_index": 1,
                "total_chunks": 1
            }
        }
    ])

    context = build_asset_context()

    # Run merge
    result = merge_wikipedia_articles(
        context,
        artists_df.lazy(),
        genres_df.lazy()
    )

    # Verify result
    assert isinstance(result, pl.LazyFrame)
    collected = result.collect()
    assert len(collected) == 2

    # Verify both entity types are present
    ids = collected["id"].to_list()
    assert "Q1_chunk_1" in ids
    assert "Q2_chunk_1" in ids


def test_merge_wikipedia_articles_empty_artists():
    """Test merge when artist articles are empty."""
    # Empty artists
    artists_df = pl.DataFrame({
        "id": [],
        "article": [],
        "metadata": []
    })

    # Genre articles
    genres_df = pl.DataFrame([
        {
            "id": "Q2_chunk_1",
            "article": "Genre article content",
            "metadata": {"title": "Techno", "entity_type": "genre"}
        }
    ])

    context = build_asset_context()

    result = merge_wikipedia_articles(
        context,
        artists_df.lazy(),
        genres_df.lazy()
    )

    collected = result.collect()
    assert len(collected) == 1
    assert collected["id"][0] == "Q2_chunk_1"


def test_merge_wikipedia_articles_empty_genres():
    """Test merge when genre articles are empty."""
    # Artist articles
    artists_df = pl.DataFrame([
        {
            "id": "Q1_chunk_1",
            "article": "Artist article content",
            "metadata": {"title": "Artist One", "entity_type": "artist"}
        }
    ])

    # Empty genres
    genres_df = pl.DataFrame({
        "id": [],
        "article": [],
        "metadata": []
    })

    context = build_asset_context()

    result = merge_wikipedia_articles(
        context,
        artists_df.lazy(),
        genres_df.lazy()
    )

    collected = result.collect()
    assert len(collected) == 1
    assert collected["id"][0] == "Q1_chunk_1"


def test_merge_wikipedia_articles_both_empty():
    """Test merge when both inputs are empty."""
    artists_df = pl.DataFrame({
        "id": [],
        "article": [],
        "metadata": []
    })

    genres_df = pl.DataFrame({
        "id": [],
        "article": [],
        "metadata": []
    })

    context = build_asset_context()

    result = merge_wikipedia_articles(
        context,
        artists_df.lazy(),
        genres_df.lazy()
    )

    collected = result.collect()
    assert len(collected) == 0


def test_merge_wikipedia_articles_multiple_chunks():
    """Test merge with multiple chunks from both sources."""
    # Multiple artist chunks
    artists_df = pl.DataFrame([
        {"id": "Q1_chunk_1", "article": "Artist chunk 1", "metadata": {"entity_type": "artist"}},
        {"id": "Q1_chunk_2", "article": "Artist chunk 2", "metadata": {"entity_type": "artist"}},
        {"id": "Q3_chunk_1", "article": "Another artist", "metadata": {"entity_type": "artist"}},
    ])

    # Multiple genre chunks
    genres_df = pl.DataFrame([
        {"id": "Q2_chunk_1", "article": "Genre chunk 1", "metadata": {"entity_type": "genre"}},
        {"id": "Q2_chunk_2", "article": "Genre chunk 2", "metadata": {"entity_type": "genre"}},
    ])

    context = build_asset_context()

    result = merge_wikipedia_articles(
        context,
        artists_df.lazy(),
        genres_df.lazy()
    )

    collected = result.collect()
    assert len(collected) == 5  # 3 artist + 2 genre chunks
