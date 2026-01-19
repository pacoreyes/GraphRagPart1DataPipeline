# -----------------------------------------------------------
# Pipeline Settings and Configuration
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path

from pydantic import Field, model_validator
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

    # Define fields for directories
    WIKIPEDIA_CACHE_DIRPATH: Path = Field(default_factory=lambda: Path("."), init=False)
    WIKIDATA_CACHE_DIRPATH: Path = Field(default_factory=lambda: Path("."), init=False)
    LAST_FM_CACHE_DIRPATH: Path = Field(default_factory=lambda: Path("."), init=False)
    MUSICBRAINZ_CACHE_DIRPATH: Path = Field(default_factory=lambda: Path("."), init=False)
    DATASETS_DIRPATH: Path = Field(default_factory=lambda: Path("."), init=False)

    # ==============================================================================
    #  VECTOR DATABASE SETTINGS
    # ==============================================================================

    # ChromaDB
    DEFAULT_EMBEDDINGS_MODEL_NAME: str = "nomic-ai/nomic-embed-text-v1.5"
    DEFAULT_COLLECTION_NAME: str = "music_rag_collection"
    VECTOR_DB_BATCH_SIZE: int = 128
    VECTOR_DB_DIRPATH: Path = Field(default_factory=lambda: Path("."), init=False)

    # ==============================================================================
    #  API & SERVICE CONFIGURATION
    # ==============================================================================

    # Wikidata API URLs
    WIKIDATA_ACTION_API_URL: str = "https://www.wikidata.org/w/api.php"
    WIKIDATA_SPARQL_ENDPOINT: str = "https://query.wikidata.org/sparql"
    WIKIDATA_CONCEPT_BASE_URI_PREFIX: str = "https://www.wikidata.org/entity/"
    WIKIDATA_SITE_BASE_URL: str = "https://www.wikidata.org/wiki/"

    # Wikipedia API URL
    WIKIPEDIA_API_URL: str = "https://en.wikipedia.org/w/api.php"

    # LastFM API URL
    LASTFM_API_URL: str = "https://ws.audioscrobbler.com/2.0/"

    # MusicBrainz API URL
    MUSICBRAINZ_API_URL: str = "https://musicbrainz.org/ws/2"

    # Bot's public identity (Polite for APIs)
    APP_NAME: str = "Nodes AI"
    APP_VERSION: str = "0.2.1"
    CONTACT_EMAIL: str = "info@nodesai.de"
    USER_AGENT: str = Field(default="", init=False)
    DEFAULT_REQUEST_HEADERS: dict[str, str] = Field(default_factory=dict, init=False)

    # ==============================================================================
    # WIKIDATA API
    # ==============================================================================
    WIKIDATA_CONCURRENT_REQUESTS: int = 5
    WIKIDATA_FALLBACK_LANGUAGES: list[str] = [
        "en", "de", "es", "fr", "it", "ja", "pt", "ru", "zh",
        "nl", "sv", "no", "da", "fi", "ko", "pl", "uk", "tr",
        "ro", "he"
    ]

    # WIKIDATA SPARQL ENDPOINT API
    WIKIDATA_SPARQL_BATCH_SIZE: int = 500
    WIKIDATA_SPARQL_REQUEST_TIMEOUT: int = 60
    WIKIDATA_SPARQL_RATE_LIMIT_DELAY: float = 5.0

    # WIKIDATA ACTION API
    WIKIDATA_ACTION_BATCH_SIZE: int = 30
    WIKIDATA_ACTION_REQUEST_TIMEOUT: int = 30
    WIKIDATA_ACTION_RATE_LIMIT_DELAY: int = 0

    # WIKIPEDIA API
    WIKIPEDIA_CONCURRENT_REQUESTS: int = 5
    WIKIPEDIA_RATE_LIMIT_DELAY: float = 0.2
    WIKIPEDIA_REQUEST_TIMEOUT: int = 60

    # LASTFM API
    LASTFM_CONCURRENT_REQUESTS: int = 5
    LASTFM_REQUEST_TIMEOUT: int = 30
    LASTFM_RATE_LIMIT_DELAY: int = 1

    # MUSICBRAINZ API
    MUSICBRAINZ_CONCURRENT_REQUESTS: int = 1
    MUSICBRAINZ_RATE_LIMIT_DELAY: float = 1.0
    MUSICBRAINZ_REQUEST_TIMEOUT: int = 60

    # ==============================================================================
    # NEO4J CONFIGURATION
    # ==============================================================================
    GRAPH_DB_INGESTION_BATCH_SIZE: int = 1000

    # ==============================================================================
    #  STREAMING BUFFER SIZES
    # ==============================================================================
    RELEASES_BUFFER_SIZE: int = 100
    TRACKS_BUFFER_SIZE: int = 200
    ARTICLES_BUFFER_SIZE: int = 50

    # ==============================================================================
    #  TEXT PROCESSING / RAG SETTINGS
    # ==============================================================================
    TEXT_CHUNK_SIZE: int = 2048
    TEXT_CHUNK_OVERLAP: int = 512
    MIN_CONTENT_LENGTH: int = 30

    # ==============================================================================
    #  AUTO-CREATION DIRS
    # ==============================================================================
    @model_validator(mode='after')
    def _compute_and_create_paths(self):
        # Assign directory values
        self.WIKIPEDIA_CACHE_DIRPATH = self.DATA_DIR / ".cache" / "wikipedia"
        self.WIKIDATA_CACHE_DIRPATH = self.DATA_DIR / ".cache" / "wikidata"
        self.LAST_FM_CACHE_DIRPATH = self.DATA_DIR / ".cache" / "last_fm"
        self.MUSICBRAINZ_CACHE_DIRPATH = self.DATA_DIR / ".cache" / "musicbrainz"
        self.DATASETS_DIRPATH = self.DATA_DIR / "datasets"
        self.VECTOR_DB_DIRPATH = self.DATA_DIR / "vector_db"

        # Create directories if they don't exist
        dirs_to_create = [
            self.WIKIPEDIA_CACHE_DIRPATH,
            self.WIKIDATA_CACHE_DIRPATH,
            self.LAST_FM_CACHE_DIRPATH,
            self.MUSICBRAINZ_CACHE_DIRPATH,
            self.DATASETS_DIRPATH,
            self.VECTOR_DB_DIRPATH,
        ]
        for directory in dirs_to_create:
            directory.mkdir(parents=True, exist_ok=True)

        self.USER_AGENT = f"{self.APP_NAME}/{self.APP_VERSION} ({self.CONTACT_EMAIL})"
        self.DEFAULT_REQUEST_HEADERS = {
            "User-Agent": self.USER_AGENT,
            "Accept": "application/json",
        }

        return self

    # ==============================================================================
    #  ENVIRONMENT VARIABLES
    # ==============================================================================
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()
