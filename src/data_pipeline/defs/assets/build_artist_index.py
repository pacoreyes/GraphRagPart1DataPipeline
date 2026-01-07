# -----------------------------------------------------------
# Building Artist Index
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from dagster import (
    asset, 
    AssetExecutionContext, 
    AssetIn, 
    AllPartitionMapping
)
from typing import Any, Optional
import polars as pl

from data_pipeline.utils.sparql_queries import get_artists_by_year_range_query
from data_pipeline.utils.wikidata_helpers import (
    run_extraction_pipeline,
    get_sparql_binding_value
)
from data_pipeline.utils.transformation_helpers import deduplicate_by_priority, normalize_and_clean_text
from data_pipeline.defs.partitions import decade_partitions, DECADES_TO_EXTRACT
from data_pipeline.defs.resources import WikidataResource


def _format_artist_record_from_sparql(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Processes a single artist record from the Wikidata SPARQL query result.

    Args:
        item (dict[str, Any]): A dictionary representing a single record from the API response.

    Returns:
        Optional[dict[str, Any]]: A dictionary containing the cleaned artist information,
            or None if the record is invalid.
    """
    artist_uri = get_sparql_binding_value(item, "artist")
    if not artist_uri:
        return None

    label_text = get_sparql_binding_value(item, "artistLabel")
    if not label_text:
        return None

    start_date = get_sparql_binding_value(item, "start_date")
    if not start_date:
        return None

    cleaned_label = normalize_and_clean_text(label_text)

    return {
        "artist_uri": artist_uri,
        "name": cleaned_label,
        "start_date": start_date
    }


@asset(
    name="build_artist_index_by_decade",
    partitions_def=decade_partitions,
    description="Extracts artist data for a specific decade."
)
async def build_artist_index_by_decade(context: AssetExecutionContext, wikidata: WikidataResource) -> pl.DataFrame:
    """
    Extracts artist data for a specific decade (partition).
    Returns a Polars DataFrame.
    """
    decade = context.partition_key
    start_year, end_year = DECADES_TO_EXTRACT[decade]

    records = await run_extraction_pipeline(
        context=context,
        get_query_function=get_artists_by_year_range_query,
        record_processor=_format_artist_record_from_sparql,
        label=f"artists_{decade}",
        client=wikidata,
        start_year=start_year,
        end_year=end_year,
    )

    context.log.info(f"Finished extraction for {decade}. Fetched {len(records)} records.")
    return pl.DataFrame(records)


@asset(
    name="artist_index",
    description="Merges all decade-specific artist data and deduplicates the result.",
    ins={
        "build_artist_index_by_decade": AssetIn(partition_mapping=AllPartitionMapping())
    }
)
def build_artist_index(context: AssetExecutionContext, build_artist_index_by_decade: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Merges all decade-specific artist DataFrames into a single one,
    then performs deduplication and cleaning using Polars.
    """
    # 1. Merge partitions
    context.log.info("Merging partitioned artist DataFrames.")
    dfs = list(build_artist_index_by_decade.values())
    
    if not dfs:
        context.log.warning("No data found in partitions. Returning empty DataFrame.")
        return pl.DataFrame()

    df = pl.concat(dfs)
    context.log.info(f"Merged raw artist index has {len(df)} records.")

    # 2. Deduplicate & Clean
    context.log.info("Preprocessing artist index.")

    lf = df.lazy()

    # Deduplicate by priority (URI and Name)
    clean_lf = deduplicate_by_priority(
        lf,
        sort_col="start_date",
        unique_cols=["artist_uri", "name"],
        descending=False
    )

    # Collect
    final_df = clean_lf.collect()
    record_count = len(final_df)
    
    context.log.info(f"Deduplicated and finalized artist index ({record_count} records).")
    return final_df