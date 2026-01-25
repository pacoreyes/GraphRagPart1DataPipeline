# -----------------------------------------------------------
# Merge Wikipedia Articles Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import polars as pl
from dagster import asset, AssetExecutionContext


@asset(
    name="wikipedia_articles",
    deps=["artists_articles", "genres_articles"],
    description="Combines artist and genre articles into a unified JSONL dataset for RAG.",
    io_manager_key="jsonl_io_manager"
)
def merge_wikipedia_articles(
    context: AssetExecutionContext,
    artists_articles: pl.LazyFrame,
    genres_articles: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Merges artist and genre Wikipedia articles into a single dataset.

    Args:
        context: Dagster execution context for logging.
        artists_articles: LazyFrame containing artist article chunks.
        genres_articles: LazyFrame containing genre article chunks.

    Returns:
        A combined LazyFrame with all article chunks.
    """
    # Count inputs
    artists_count = artists_articles.select(pl.len()).collect().item()
    genres_count = genres_articles.select(pl.len()).collect().item()

    context.log.info(f"Merging articles: {artists_count} artist chunks, {genres_count} genre chunks")

    # Handle empty inputs
    if artists_count == 0 and genres_count == 0:
        context.log.warning("Both artist and genre articles are empty.")
        return pl.LazyFrame()

    if artists_count == 0:
        context.log.info("No artist articles, returning only genre articles.")
        return genres_articles

    if genres_count == 0:
        context.log.info("No genre articles, returning only artist articles.")
        return artists_articles

    # Concatenate both LazyFrames
    combined = pl.concat([artists_articles, genres_articles], how="vertical_relaxed")

    total_count = combined.select(pl.len()).collect().item()
    context.log.info(f"Combined total: {total_count} article chunks")

    return combined
