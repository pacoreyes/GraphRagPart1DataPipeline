# -----------------------------------------------------------
# Extract Countries Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.settings import settings
from data_pipeline.utils.wikidata_helpers import async_resolve_labels_to_qids
from data_pipeline.defs.resources import WikidataResource


@asset(
    name="countries",
    description="Extract unique countries from artists and resolve them to Wikidata QIDs.",
)
async def extract_countries(
    context: AssetExecutionContext,
    wikidata: WikidataResource,
    artists: pl.LazyFrame
) -> pl.DataFrame:
    """
    Extracts unique countries from the enriched artists dataset and 
    resolves their Wikidata QIDs.
    """
    context.log.info("Extracting unique countries from artists.")

    # 1. Get unique country names
    unique_countries_df = artists.select("country").unique().collect()
    country_names = [
        c for c in unique_countries_df["country"].to_list() 
        if c and isinstance(c, str)
    ]

    context.log.info(f"Found {len(country_names)} unique countries to resolve.")

    if not country_names:
        return pl.DataFrame(schema={"id": pl.Utf8, "name": pl.Utf8})

    # 2. Resolve Labels to QIDs
    async with wikidata.get_client(context) as client:
        resolved_map = await async_resolve_labels_to_qids(
            context,
            country_names,
            api_url=settings.WIKIDATA_ACTION_API_URL,
            cache_dir=settings.WIKIDATA_CACHE_DIRPATH,
            timeout=settings.WIKIDATA_ACTION_REQUEST_TIMEOUT,
            rate_limit_delay=settings.WIKIDATA_ACTION_RATE_LIMIT_DELAY,
            headers=settings.DEFAULT_REQUEST_HEADERS,
            client=client
        )

    # 3. Construct DataFrame
    country_data = [
        {"id": qid, "name": name}
        for name, qid in resolved_map.items()
    ]

    # Log unresolved
    unresolved = set(country_names) - set(resolved_map.keys())
    if unresolved:
        context.log.warning(f"Could not resolve {len(unresolved)} countries: {unresolved}")

    return pl.DataFrame(country_data, schema={"id": pl.Utf8, "name": pl.Utf8})
