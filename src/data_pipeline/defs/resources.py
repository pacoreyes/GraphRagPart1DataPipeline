# -----------------------------------------------------------
# Dagster Resources
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import os
from contextlib import contextmanager, asynccontextmanager
from typing import Any, AsyncGenerator, Generator

import httpx
from dagster import (
    ConfigurableResource,
    Definitions,
    EnvVar,
)
from neo4j import GraphDatabase, Driver

from data_pipeline.settings import settings
from .io_managers import PolarsJSONLIOManager


class ApiConfiguration(ConfigurableResource):
    """
    Configuration for external API keys.
    """
    lastfm_api_key: str
    nomic_api_key: str


class Neo4jResource(ConfigurableResource):
    """
    Resource for interacting with the Neo4j graph database.
    """
    uri: str = EnvVar("NEO4J_URI")
    username: str = EnvVar("NEO4J_USERNAME")
    password: str = EnvVar("NEO4J_PASSWORD")

    @contextmanager
    def yield_for_execution(self, context) -> Generator[Driver, None, None]:
        driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        try:
            yield driver
        finally:
            driver.close()


class WikidataResource(ConfigurableResource):
    """
    Resource for making HTTP requests to Wikidata (SPARQL).
    """
    user_agent: str
    timeout: int

    @asynccontextmanager
    async def yield_for_execution(self, context) -> AsyncGenerator[httpx.AsyncClient, None]:
        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout
        ) as client:
            yield client


# Environment Logic
is_prod = os.getenv("DAGSTER_ENV") == "PROD"

# Base resources
resource_defs: dict[str, Any] = {
    "api_config": ApiConfiguration(
        lastfm_api_key=EnvVar("LASTFM_API_KEY"),
        nomic_api_key=EnvVar("NOMIC_API_KEY"),
    ),
    "neo4j": Neo4jResource(),
    "wikidata": WikidataResource(
        user_agent=settings.USER_AGENT,
        timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT
    ),
    "io_manager": PolarsJSONLIOManager(base_dir=str(settings.datasets_dirpath))
}

# Export as Definitions for automatic loading
defs = Definitions(
    resources=resource_defs,
)
