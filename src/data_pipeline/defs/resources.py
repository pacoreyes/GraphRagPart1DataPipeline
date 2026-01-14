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

import chromadb
from chromadb.api.models.Collection import Collection
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
    cache_dir: str

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


class ChromaDBResource(ConfigurableResource):
    """
    Resource for interacting with ChromaDB vector database.

    Provides configuration for persistent ChromaDB storage and
    a factory method to obtain collections.
    """
    db_path: str
    collection_name: str
    model_name: str
    batch_size: int  # ChromaDB upsert batch size
    embedding_batch_size: int = 64  # GPU embedding batch size (optimal: 32-128 for MPS)

    @contextmanager
    def get_collection(
        self,
        context,
        embedding_function: Any = None
    ) -> Generator[Collection, None, None]:
        """
        Factory method to yield a ChromaDB collection.

        Args:
            context: Dagster execution context for logging.
            embedding_function: Custom embedding function for the collection.

        Yields:
            ChromaDB Collection instance.
        """
        context.log.debug(f"Initializing ChromaDB client at {self.db_path}")
        client = chromadb.PersistentClient(path=self.db_path)

        collection = client.get_or_create_collection(
            name=self.collection_name,
            embedding_function=embedding_function,
        )
        context.log.info(
            f"ChromaDB collection '{self.collection_name}' ready "
            f"(existing count: {collection.count()})"
        )
        yield collection


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
            driver.verify_connectivity()
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
        cache_dir=str(settings.MUSICBRAINZ_CACHE_DIRPATH),
    ),
    "nomic": NomicResource(
        api_key=EnvVar("NOMIC_API_KEY")
    ),
    "chromadb": ChromaDBResource(
        db_path=str(settings.VECTOR_DB_DIRPATH),
        collection_name=settings.DEFAULT_COLLECTION_NAME,
        model_name=settings.DEFAULT_EMBEDDINGS_MODEL_NAME,
        batch_size=settings.VECTOR_DB_BATCH_SIZE,
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
