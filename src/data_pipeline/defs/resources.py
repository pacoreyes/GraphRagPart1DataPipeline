# -----------------------------------------------------------
# Dagster Resources
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from contextlib import contextmanager, asynccontextmanager
from typing import Any, AsyncGenerator, Generator

from curl_cffi.requests import AsyncSession as AsyncClient
from dagster import (
    ConfigurableResource,
    Definitions,
    EnvVar,
)
from neo4j import GraphDatabase, Driver

from data_pipeline.settings import settings
from .io_managers import PolarsJSONLIOManager, PolarsParquetIOManager


class LastFmResource(ConfigurableResource):
    """
    Configuration for Last.fm API.
    """
    api_key: str

    @asynccontextmanager
    async def get_client(self, context) -> AsyncGenerator[AsyncClient, None]:
        context.log.debug(f"Initializing Last.fm client (Run ID: {context.run_id})")
        async with AsyncClient(
            headers={
                "Accept": "application/json",
                "X-Dagster-Run-Id": context.run_id
            },
            timeout=settings.LASTFM_REQUEST_TIMEOUT,
            impersonate="chrome"
        ) as client:
            yield client


class MusicBrainzResource(ConfigurableResource):
    """
    Configuration for MusicBrainz API.
    """
    api_url: str
    request_timeout: int
    rate_limit_delay: float

    @asynccontextmanager
    async def get_client(self, context) -> AsyncGenerator[AsyncClient, None]:
        context.log.debug(f"Initializing MusicBrainz client (Run ID: {context.run_id})")
        async with AsyncClient(
            headers={
                "User-Agent": settings.USER_AGENT, 
                "Accept": "application/json",
                "X-Dagster-Run-Id": context.run_id
            },
            timeout=self.request_timeout,
            impersonate="chrome"
        ) as client:
            yield client


class NomicResource(ConfigurableResource):
    """
    Configuration for Nomic API.
    """
    api_key: str


class Neo4jResource(ConfigurableResource):
    """
    Resource for interacting with the Neo4j graph database.
    """
    uri: str
    username: str
    password: str

    @contextmanager
    def get_driver(self, context) -> Generator[Driver, None, None]:
        context.log.debug(f"Initializing Neo4j driver (Run ID: {context.run_id})")
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
    impersonate: str = "chrome"

    @asynccontextmanager
    async def get_client(self, context) -> AsyncGenerator[AsyncClient, None]:
        context.log.debug(f"Initializing Wikidata client (Run ID: {context.run_id})")
        async with AsyncClient(
            headers={
                "User-Agent": self.user_agent, 
                "Accept": "application/json",
                "X-Dagster-Run-Id": context.run_id
            },
            timeout=self.timeout,
            impersonate=self.impersonate
        ) as client:
            yield client


class WikipediaResource(ConfigurableResource):
    """
    Resource for making HTTP requests to Wikipedia API.
    """
    api_url: str
    timeout: int
    rate_limit_delay: float

    @asynccontextmanager
    async def get_client(self, context) -> AsyncGenerator[AsyncClient, None]:
        context.log.debug(f"Initializing Wikipedia client (Run ID: {context.run_id})")
        async with AsyncClient(
            headers={
                "User-Agent": settings.USER_AGENT, 
                "Accept": "application/json",
                "X-Dagster-Run-Id": context.run_id
            },
            timeout=self.timeout,
            impersonate="chrome"
        ) as client:
            yield client


# Base resources
resource_defs: dict[str, Any] = {
    "lastfm": LastFmResource(
        api_key=EnvVar("LASTFM_API_KEY")
    ),
    "musicbrainz": MusicBrainzResource(
        api_url=settings.MUSICBRAINZ_API_URL,
        request_timeout=settings.MUSICBRAINZ_REQUEST_TIMEOUT,
        rate_limit_delay=settings.MUSICBRAINZ_RATE_LIMIT_DELAY,
    ),
    "nomic": NomicResource(
        api_key=EnvVar("NOMIC_API_KEY")
    ),
    "neo4j": Neo4jResource(
        uri=EnvVar("NEO4J_URI"),
        username=EnvVar("NEO4J_USERNAME"),
        password=EnvVar("NEO4J_PASSWORD"),
    ),
    "wikidata": WikidataResource(
        user_agent=settings.USER_AGENT,
        timeout=settings.WIKIDATA_SPARQL_REQUEST_TIMEOUT,
        impersonate="chrome"
    ),
    "wikipedia": WikipediaResource(
        api_url=settings.WIKIPEDIA_API_URL,
        timeout=60,  # Default timeout for Wikipedia
        rate_limit_delay=settings.WIKIPEDIA_RATE_LIMIT_DELAY
    ),
    "io_manager": PolarsParquetIOManager(base_dir=str(settings.DATASETS_DIRPATH)),
    "jsonl_io_manager": PolarsJSONLIOManager(base_dir=str(settings.DATASETS_DIRPATH))
}

# Export as Definitions for automatic loading
defs = Definitions(
    resources=resource_defs,
)
