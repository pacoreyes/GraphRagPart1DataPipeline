# -----------------------------------------------------------
# Pipeline Settings and Configuration
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ==============================================================================
    #  CORE PATHS
    # ==============================================================================

    # The 'app' directory, which is the root for Python imports
    APP_DIR: Path = Path(__file__).resolve().parent
    # For accessing top-level project resources (Repo Root)
    PROJECT_DIR: Path = APP_DIR.parent.parent

    # ==============================================================================
    #  LOCAL DATA PATHS
    # ==============================================================================

    # Top-level directory for all data, caches, temp, databases, and datasets
    DATA_DIR: Path = PROJECT_DIR / "data_volume"

    # Cache Directories
    @property
    def wikipedia_cache_dirpath(self) -> Path:
        return self.DATA_DIR / ".cache" / "wikipedia"

    @property
    def wikidata_cache_dirpath(self) -> Path:
        return self.DATA_DIR / ".cache" / "wikidata"

    @property
    def lastfm_cache_dirpath(self) -> Path:
        return self.DATA_DIR / ".cache" / "last_fm"

    # Temp Directory
    @property
    def temp_dirpath(self) -> Path:
        return self.DATA_DIR / ".temp"

    # ==============================================================================
    #  DATABASE PATHS & SETTINGS
    # ==============================================================================

    # Vector DB - Chroma
    @property
    def vector_db_dirpath(self) -> Path:
        return self.DATA_DIR / "vector_db"

    # ChromaDB
    DEFAULT_EMBEDDINGS_MODEL_NAME: str = "nomic-ai/nomic-embed-text-v1.5"
    DEFAULT_COLLECTION_NAME: str = "music_rag_collection"

    # ==============================================================================
    #  DATASETS PATHS
    # ==============================================================================

    # Datasets Directory
    @property
    def datasets_dirpath(self) -> Path:
        return self.DATA_DIR / "datasets"

    # Artist Index
    @property
    def artist_index_filepath(self) -> Path:
        return self.datasets_dirpath / "artist_index.jsonl"

    # Wikipedia Articles Dataset
    @property
    def wikipedia_articles_filepath(self) -> Path:
        return self.datasets_dirpath / "wikipedia_articles.jsonl"

    # Artists Dataset
    @property
    def artists_filepath(self) -> Path:
        return self.datasets_dirpath / "artists.jsonl"

    # Genres Dataset
    @property
    def genres_filepath(self) -> Path:
        return self.datasets_dirpath / "genres.jsonl"

    # Albums Dataset
    @property
    def albums_filepath(self) -> Path:
        return self.datasets_dirpath / "albums.jsonl"

    # Tracks Dataset
    @property
    def tracks_filepath(self) -> Path:
        return self.datasets_dirpath / "tracks.jsonl"

    # ==============================================================================
    #  API & SERVICE CONFIGURATION
    # ==============================================================================

    # Wikidata API URLs
    WIKIDATA_ACTION_API_URL: str = "https://www.wikidata.org/w/api.php"
    WIKIDATA_SPARQL_ENDPOINT: str = "https://query.wikidata.org/sparql"
    WIKIDATA_CONCEPT_BASE_URI_PREFIX: str = "http://www.wikidata.org/entity/"
    WIKIDATA_SITE_BASE_URL: str = "http://www.wikidata.org/wiki/"

    # Wikipedia API URL
    WIKIPEDIA_API_URL: str = "https://en.wikipedia.org/w/api.php"

    # LastFM API URL
    LASTFM_API_URL: str = "http://ws.audioscrobbler.com/2.0/"

    # Bot's public identity (Polite for APIs)
    USER_AGENT: str = "Nodes AI (info@nodesAI.de)"

    # HTTP requests headers
    @property
    def default_request_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "application/json",
        }

    # ==============================================================================
    #  API PROCESSING PARAMETERS
    # ==============================================================================

    # WIKIDATA API
    WIKIDATA_CONCURRENT_REQUESTS: int = 5

    # WIKIDATA SPARQL ENDPOINT API
    WIKIDATA_SPARQL_BATCH_SIZE: int = 500
    WIKIDATA_SPARQL_REQUEST_TIMEOUT: int = 60

    # WIKIDATA ACTION API
    WIKIDATA_ACTION_BATCH_SIZE: int = 50
    WIKIDATA_ACTION_REQUEST_TIMEOUT: int = 30
    WIKIDATA_ACTION_RATE_LIMIT_DELAY: float = 0.0

    # LASTFM API
    LASTFM_CONCURRENT_REQUESTS: int = 5
    LASTFM_REQUEST_TIMEOUT: int = 30
    LASTFM_RATE_LIMIT_DELAY: int = 1

    # ==============================================================================
    # MEMGRAPH CONFIGURATION
    # ==============================================================================
    MEMGRAPH_HOST: str = "127.0.0.1"
    MEMGRAPH_PORT: int = 7687
    MEMGRAPH_INGESTION_BATCH_SIZE: int = 1000

    # ==============================================================================
    #  AUTO-CREATION DIRS
    # ==============================================================================
    @model_validator(mode='after')
    def _create_directories(self):
        dirs_to_create = [
            self.wikipedia_cache_dirpath,
            self.wikidata_cache_dirpath,
            self.lastfm_cache_dirpath,
            self.temp_dirpath,
            self.datasets_dirpath,
            self.vector_db_dirpath
        ]
        for directory in dirs_to_create:
            directory.mkdir(parents=True, exist_ok=True)
        return self

    # ==============================================================================
    #  ENVIRONMENT VARIABLES
    # ==============================================================================
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # api_key: str
    # database_url: str
    debug_mode: bool = False  # Default value


settings = Settings()
