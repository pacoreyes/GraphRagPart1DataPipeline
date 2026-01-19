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

    def test_prepare_chroma_metadata_full_data(self):
        """Test metadata preparation with all fields present."""
        row_metadata = {
            "title": "Test Title",
            "artist_name": "Test Artist",
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
        assert result["artist_name"] == "Test Artist"
        assert result["country"] == "Germany"
        assert result["inception_year"] == 1990
        assert result["chunk_index"] == 1
        assert result["total_chunks"] == 5
        # Verify lists are joined into strings
        assert result["aliases"] == "Alias1, Alias2"
        assert result["tags"] == "electronic, ambient"
        assert result["similar_artists"] == "Artist A, Artist B"
        assert result["genres"] == "Techno, House"

    def test_prepare_chroma_metadata_sparse_data(self):
        """Test metadata preparation with sparse/missing fields."""
        row_metadata = {
            "title": "Minimal Title",
            "artist_name": "Minimal Artist",
            "country": "Unknown",
            "wikipedia_url": "https://test.com",
            "wikidata_uri": "https://test.org",
            "chunk_index": 1,
            "total_chunks": 1,
            # Missing optional list fields
        }
        row = {"metadata": row_metadata}

        result = _prepare_chroma_metadata(row)

        assert result["title"] == "Minimal Title"
        assert result["artist_name"] == "Minimal Artist"
        assert result["country"] == "Unknown"
        assert "inception_year" not in result
        assert result["aliases"] == ""
        assert result["tags"] == ""

    def test_prepare_chroma_metadata_empty_lists(self):
        """Test that empty lists are present as empty strings in metadata."""
        row_metadata = {
            "title": "Test",
            "artist_name": "Artist",
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

        assert result["aliases"] == ""
        assert result["tags"] == ""
        assert result["genres"] == ""


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
        
        # Create input LazyFrame
        data = {
            "id": ["Q1", "Q2", "Q3"],
            "article": ["Content 1", "Content 2", "Content 3"],
            "metadata": [
                {
                    "title": "T1", "artist_name": "A1", "country": "US",
                    "wikipedia_url": "u", "wikidata_uri": "uri", "chunk_index": 1, "total_chunks": 1,
                    "tags": ["pop"]
                },
                {
                    "title": "T2", "artist_name": "A2", "country": "UK",
                    "wikipedia_url": "u", "wikidata_uri": "uri", "chunk_index": 1, "total_chunks": 1,
                    "tags": ["rock"]
                },
                {
                    "title": "T3", "artist_name": "A3", "country": "FR",
                    "wikipedia_url": "u", "wikidata_uri": "uri", "chunk_index": 1, "total_chunks": 1,
                    "tags": ["jazz"]
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