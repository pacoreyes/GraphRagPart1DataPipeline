# -----------------------------------------------------------
# Unit Tests for ingest_vector_db
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import build_asset_context, MaterializeResult

from data_pipeline.defs.assets.ingest_vector_db import (
    _prepare_chroma_metadata,
    _iter_batches,
    ingest_vector_db,
)


class TestPrepareChromaMetadata:
    """Tests for the _prepare_chroma_metadata helper function."""

    def test_prepare_chroma_metadata_full_artist_data(self):
        """Test metadata preparation with all artist fields present."""
        row_metadata = {
            "title": "Test Title",
            "name": "Test Artist",
            "entity_type": "artist",
            "country": "Germany",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Test",
            "wikidata_uri": "https://www.wikidata.org/entity/Q123",
            "inception_year": 1990,
            "chunk_index": 1,
            "total_chunks": 5,
            "aliases": ["Alias1", "Alias2"],
            "tags": ["electronic", "ambient"],
            "similar_artists": ["Artist A", "Artist B"],
            "genres": ["Techno", "House"],
        }
        row = {"metadata": row_metadata}

        result = _prepare_chroma_metadata(row)

        assert result["title"] == "Test Title"
        assert result["name"] == "Test Artist"
        assert result["entity_type"] == "artist"
        assert result["country"] == "Germany"
        assert result["inception_year"] == 1990
        assert result["chunk_index"] == 1
        assert result["total_chunks"] == 5
        # Verify lists are joined into strings
        assert result["aliases"] == "Alias1, Alias2"
        assert result["tags"] == "electronic, ambient"
        assert result["similar_artists"] == "Artist A, Artist B"
        assert result["genres"] == "Techno, House"

    def test_prepare_chroma_metadata_genre_data(self):
        """Test metadata preparation for genre articles."""
        row_metadata = {
            "title": "Techno",
            "name": "Techno",
            "entity_type": "genre",
            "country": "United States",
            "wikipedia_url": "https://en.wikipedia.org/wiki/Techno",
            "wikidata_uri": "https://www.wikidata.org/entity/Q1298934",
            "inception_year": 1988,
            "chunk_index": 1,
            "total_chunks": 3,
            "aliases": ["techno music", "Detroit techno"],
            # genres don't have tags, similar_artists, or genres fields
        }
        row = {"metadata": row_metadata}

        result = _prepare_chroma_metadata(row)

        assert result["title"] == "Techno"
        assert result["name"] == "Techno"
        assert result["entity_type"] == "genre"
        assert result["country"] == "United States"
        assert result["inception_year"] == 1988
        assert result["aliases"] == "techno music, Detroit techno"
        # Optional fields not present should not be in result
        assert "tags" not in result
        assert "similar_artists" not in result
        assert "genres" not in result

    def test_prepare_chroma_metadata_sparse_data(self):
        """Test metadata preparation with sparse/missing fields."""
        row_metadata = {
            "title": "Minimal Title",
            "name": "Minimal Genre",
            "entity_type": "genre",
            "wikipedia_url": "https://test.com",
            "wikidata_uri": "https://test.org",
            "chunk_index": 1,
            "total_chunks": 1,
            # Missing optional fields: country, inception_year, aliases, etc.
        }
        row = {"metadata": row_metadata}

        result = _prepare_chroma_metadata(row)

        assert result["title"] == "Minimal Title"
        assert result["name"] == "Minimal Genre"
        assert result["entity_type"] == "genre"
        assert "country" not in result
        assert "inception_year" not in result
        assert "aliases" not in result
        assert "tags" not in result

    def test_prepare_chroma_metadata_empty_lists(self):
        """Test that empty lists are not included in metadata."""
        row_metadata = {
            "title": "Test",
            "name": "Artist",
            "entity_type": "artist",
            "country": "US",
            "wikipedia_url": "url",
            "wikidata_uri": "uri",
            "chunk_index": 1,
            "total_chunks": 1,
            "aliases": [],
            "tags": [],
            "genres": [],
        }
        row = {"metadata": row_metadata}

        result = _prepare_chroma_metadata(row)

        # Empty lists should not be included
        assert "aliases" not in result
        assert "tags" not in result
        assert "genres" not in result

    def test_prepare_chroma_metadata_defaults_entity_type(self):
        """Test that entity_type defaults to 'artist' for backward compatibility."""
        row_metadata = {
            "title": "Test",
            "name": "Test",
            "wikipedia_url": "url",
            "wikidata_uri": "uri",
            "chunk_index": 1,
            "total_chunks": 1,
            # entity_type missing
        }
        row = {"metadata": row_metadata}

        result = _prepare_chroma_metadata(row)

        assert result["entity_type"] == "artist"


class TestIterBatches:
    """Tests for the _iter_batches generator function."""

    def test_iter_batches(self):
        """Test that batches are yielded correctly."""
        data = {"col": range(5)}
        lf = pl.DataFrame(data).lazy()

        batches = list(_iter_batches(lf, batch_size=2))

        assert len(batches) == 3
        assert len(batches[0]) == 2
        assert len(batches[1]) == 2
        assert len(batches[2]) == 1
        assert batches[0]["col"][0] == 0


class TestIngestVectorDbAsset:
    """Tests for the ingest_vector_db asset."""

    @pytest.fixture
    def mock_chromadb_resource(self):
        """Create a mock ChromaDB resource."""
        resource = MagicMock()
        resource.model_name = "nomic-ai/nomic-embed-text-v1.5"
        resource.batch_size = 2
        resource.embedding_batch_size = 32
        return resource

    @patch("data_pipeline.defs.assets.ingest_vector_db.NomicEmbeddingFunction")
    @patch("data_pipeline.defs.assets.ingest_vector_db.get_device")
    def test_ingest_vector_db_success(
        self,
        mock_get_device,
        mock_embedding_class,
        mock_chromadb_resource,
    ):
        """Test successful ingestion of documents."""
        mock_get_device.return_value = MagicMock(__str__=lambda self: "cpu")

        # Create input LazyFrame with mixed artist and genre articles
        data = {
            "id": ["Q1", "Q2", "Q3"],
            "article": ["Artist content", "Genre content", "Another artist"],
            "metadata": [
                {
                    "title": "T1", "name": "Artist One", "entity_type": "artist",
                    "country": "US", "wikipedia_url": "u", "wikidata_uri": "uri",
                    "chunk_index": 1, "total_chunks": 1, "tags": ["pop"]
                },
                {
                    "title": "Techno", "name": "Techno", "entity_type": "genre",
                    "country": "US", "wikipedia_url": "u", "wikidata_uri": "uri",
                    "chunk_index": 1, "total_chunks": 1, "aliases": ["techno music"]
                },
                {
                    "title": "T3", "name": "Artist Two", "entity_type": "artist",
                    "wikipedia_url": "u", "wikidata_uri": "uri",
                    "chunk_index": 1, "total_chunks": 1
                }
            ]
        }
        wikipedia_articles = pl.DataFrame(data).lazy()

        mock_embedding_fn = MagicMock()
        mock_embedding_class.return_value = mock_embedding_fn

        mock_collection = MagicMock()
        mock_collection.count.return_value = 3

        @contextmanager
        def mock_get_collection(context, embedding_function=None):
            yield mock_collection

        mock_chromadb_resource.get_collection = mock_get_collection

        context = build_asset_context()

        result = ingest_vector_db(
            context,
            mock_chromadb_resource,
            wikipedia_articles
        )

        assert isinstance(result, MaterializeResult)
        assert result.metadata["documents_processed"] == 3
        assert result.metadata["status"] == "success"
        # batch size is 2, total 3 articles -> 2 batches, so upsert called 2 times
        assert mock_collection.upsert.call_count == 2

    @patch("data_pipeline.defs.assets.ingest_vector_db.NomicEmbeddingFunction")
    @patch("data_pipeline.defs.assets.ingest_vector_db.get_device")
    def test_ingest_vector_db_empty_input(
        self,
        mock_get_device,
        mock_embedding_class,
        mock_chromadb_resource,
    ):
        """Test ingestion with empty input."""
        mock_get_device.return_value = MagicMock(__str__=lambda self: "cpu")

        # Empty input
        wikipedia_articles = pl.DataFrame({
            "id": [],
            "article": [],
            "metadata": []
        }).lazy()

        context = build_asset_context()

        result = ingest_vector_db(
            context,
            mock_chromadb_resource,
            wikipedia_articles
        )

        assert isinstance(result, MaterializeResult)
        assert result.metadata["documents_processed"] == 0
        assert result.metadata["status"] == "empty_input"
