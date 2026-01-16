# -----------------------------------------------------------
# Extract Countries Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path

import polars as pl
from dagster import asset, AssetExecutionContext

from data_pipeline.models import Country
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
) -> list[Country]:
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
        return []

    # 2. Resolve Labels to QIDs
    async with wikidata.get_client(context) as client:
        resolved_map = await async_resolve_labels_to_qids(
            context,
            country_names,
            api_url=wikidata.api_url,
            cache_dir=Path(wikidata.cache_dir),
            timeout=wikidata.timeout,
            rate_limit_delay=wikidata.rate_limit_delay,
            client=client
        )

    # 3. Construct List of Country objects
    countries = []
    for name, qid in resolved_map.items():
        countries.append(Country(id=qid, name=name))

    # Log unresolved
    unresolved = set(country_names) - set(resolved_map.keys())
    if unresolved:
        context.log.warning(f"Could not resolve {len(unresolved)} countries: {unresolved}")

    return countries