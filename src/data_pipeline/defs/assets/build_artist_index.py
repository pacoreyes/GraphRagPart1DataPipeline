# -----------------------------------------------------------
# Building Artist Index
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition
from typing import Any, Optional
import polars as pl

from data_pipeline.utils.sparql_queries import get_artists_by_year_range_query
from data_pipeline.utils.io_helpers import merge_jsonl_files
from data_pipeline.utils.wikidata_helpers import (
    execute_sparql_extraction,
    get_sparql_binding_value
)
from data_pipeline.utils.transformation_helpers import deduplicate_by_priority, normalize_and_clean_text
from data_pipeline.settings import settings

DECADES_TO_EXTRACT = {
    "1930s": (1930, 1939),
    "1940s": (1940, 1949),
    "1950s": (1950, 1959),
    "1960s": (1960, 1969),
    "1970s": (1970, 1979),
    "1980s": (1980, 1989),
    "1990s": (1990, 1999),
    "2000s": (2000, 2009),
    "2010s": (2010, 2019),
    "2020s": (2020, 2029),
}
decade_partitions = StaticPartitionsDefinition(list(DECADES_TO_EXTRACT.keys()))


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
    description="Extracts artist data for a specific decade and saves it to a JSONL file."
)
def build_artist_index_by_decade(context: AssetExecutionContext) -> str:
    """
    Extracts artist data for a specific decade (partition) and saves it to a JSONL file.

    Args:
        context (AssetExecutionContext): The Dagster execution context, providing partition keys and logging.

    Returns:
        str: The path to the generated JSONL file.
    """
    decade = context.partition_key
    start_year, end_year = DECADES_TO_EXTRACT[decade]
    output_filename = f"artist_index_{decade}.jsonl"
    output_path = settings.datasets_dirpath / output_filename

    execute_sparql_extraction(
        context=context,
        output_path=output_path,
        get_query_function=get_artists_by_year_range_query,
        record_processor=_format_artist_record_from_sparql,
        label=f"artists_{decade}",
        start_year=start_year,
        end_year=end_year,
    )

    context.log.info(f"Finished extraction for {decade}. Output at {output_path}")
    return str(output_path)


@asset(
    name="build_artist_index",
    deps=["build_artist_index_by_decade"],
    description="Merges all decade-specific artist JSONL files and deduplicates the result.",
)
def build_artist_index(context: AssetExecutionContext) -> str:
    """
    Merges all decade-specific artist JSONL files into a single artist_index.jsonl,
    then performs deduplication and cleaning using Polars.

    Args:
        context (AssetExecutionContext): The Dagster execution context for logging.

    Returns:
        str: The path to the merged and cleaned artist index file.
    """
    output_path = settings.artist_index_filepath

    # 1. Merge Files
    input_paths = [
        settings.datasets_dirpath / f"artist_index_{decade}.jsonl" for decade in DECADES_TO_EXTRACT
    ]
    merge_jsonl_files(input_paths, output_path)
    context.log.info(f"Merged raw artist index saved to {output_path}")

    # 2. Deduplicate & Clean (Post-processing)
    context.log.info(f"Preprocessing artist index from {output_path}")

    try:
        lf = pl.scan_ndjson(output_path)
    except Exception as e:
        context.log.error(f"Error scanning file {output_path}: {e}")
        raise e

    # Deduplicate by priority (URI and Name)
    clean_lf = deduplicate_by_priority(
        lf,
        sort_col="start_date",
        unique_cols=["artist_uri", "name"],
        descending=False
    )

    # Collect and Overwrite
    df = clean_lf.collect()
    record_count = len(df)
    
    df.write_ndjson(output_path)
    context.log.info(f"Deduplicated and finalized artist index ({record_count} records) saved to {output_path}")

    return str(output_path)
