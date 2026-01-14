# -----------------------------------------------------------
# Building Artist Index
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any, Optional

from dagster import (
    asset,
    AssetExecutionContext,
    AssetIn,
    AllPartitionMapping
)
import polars as pl

from data_pipeline.settings import settings
from data_pipeline.utils.wikidata_helpers import run_extraction_pipeline, get_sparql_binding_value
from data_pipeline.utils.data_transformation_helpers import deduplicate_by_priority, normalize_and_clean_text
from data_pipeline.defs.partitions import decade_partitions, DECADES_TO_EXTRACT
from data_pipeline.defs.resources import WikidataResource


def _get_artists_by_year_range_query(
    start_year: int,
    end_year: int,
    limit: int,
    offset: int
) -> str:
    """
    Generate a SPARQL query to fetch artists active within a specific year range.

    Args:
        start_year: The first year of the period (inclusive).
        end_year: The last year of the period (inclusive).
        limit: The maximum number of results to return.
        offset: The offset from which to start fetching results.

    Returns:
        A formatted SPARQL query string.
    """
    return f"""
SELECT ?artist ?artistLabel ?start_date
WHERE {{
  # --- INNER QUERY: Find the artists first (Pagination happens here) ---
  {{
    SELECT DISTINCT ?artist ?start_date

    # 1. Genre: Electronic & Subgenres, 'Genre' (P136) AND 'Field of Work' (P101)
    WHERE {{
      # 1. Genre: Electronic & Subgenres
      ?genre wdt:P279* wd:Q9778 .
      ?artist wdt:P136|wdt:P101 ?genre .

      # 2. Type: Human or Group
      {{ ?artist wdt:P31 wd:Q5 . }} UNION {{ ?artist wdt:P31/wdt:P279* wd:Q215380 . }}

      # 3. Date Filter
      ?artist wdt:P571|wdt:P2031 ?start_date .
      FILTER (YEAR(?start_date) >= {start_year} && YEAR(?start_date) <= {end_year})
    }}
    # Pagination applies only to the ID retrieval (Fast)
    ORDER BY ?start_date ?artist
    LIMIT {limit}
    OFFSET {offset}
  }}

  # --- OUTER QUERY: Fetch Labels for the 100 results ---

  # A. Try to find an English label specifically
  OPTIONAL {{ ?artist rdfs:label ?enLabel . FILTER(LANG(?enLabel) = "en") }}

  # B. Find ANY other label (Language is irrelevant)
  OPTIONAL {{ ?artist rdfs:label ?anyLabel . }}

  # C. Logic: "If English exists, use it. Otherwise, use the random fallback."
  BIND(COALESCE(?enLabel, ?anyLabel) AS ?artistLabel)
}}
# We group by the ID to merge the multiple labels into one line
GROUP BY ?artist ?artistLabel ?start_date
ORDER BY ?start_date
"""


def _format_artist_record_from_sparql(
    item: dict[str, Any]
) -> Optional[dict[str, Any]]:
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
async def build_artist_index_by_decade(
    context: AssetExecutionContext,
    wikidata: WikidataResource
) -> pl.LazyFrame:
    """
    Extracts artist data for a specific decade (partition).
    Returns a Polars LazyFrame.
    """
    decade = context.partition_key
    start_year, end_year = DECADES_TO_EXTRACT[decade]

    async with wikidata.get_client(context) as client:
        records = await run_extraction_pipeline(
            context=context,
            get_query_function=_get_artists_by_year_range_query,
            record_processor=_format_artist_record_from_sparql,
            label=f"artists_{decade}",
            sparql_endpoint=settings.WIKIDATA_SPARQL_ENDPOINT,
            batch_size=settings.WIKIDATA_SPARQL_BATCH_SIZE,
            concurrency_limit=settings.WIKIDATA_CONCURRENT_REQUESTS,
            timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
            rate_limit_delay=settings.WIKIDATA_SPARQL_RATE_LIMIT_DELAY,
            client=client,
            start_year=start_year,
            end_year=end_year,
        )

    context.log.info(f"Finished extraction for {decade}. Fetched {len(records)} records.")
    return pl.DataFrame(records).lazy()


@asset(
    name="artist_index",
    description="Merges all decade-specific artist data and deduplicates the result.",
    ins={
        "build_artist_index_by_decade": AssetIn(partition_mapping=AllPartitionMapping())
    }
)
def build_artist_index(
    context: AssetExecutionContext,
    build_artist_index_by_decade: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Merges all decade-specific artist DataFrames into a single one,
    then performs deduplication and cleaning using Polars.
    """
    context.log.info("Preprocessing artist index.")

    # Deduplicate by priority (URI and Name)
    clean_lf = deduplicate_by_priority(
        build_artist_index_by_decade,
        sort_col="start_date",
        unique_cols=["artist_uri", "name"],
        descending=False
    )

    return clean_lf
