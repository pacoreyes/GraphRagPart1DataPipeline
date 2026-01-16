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
    _iter_batches,
    _process_batch,
    _prepare_chroma_metadata,
    ingest_vector_db,
)
from data_pipeline.utils.chroma_helpers import generate_doc_id


class TestPrepareChromaMetadata:
    """Tests for the _prepare_chroma_metadata helper function."""

    def test_prepare_chroma_metadata_full_data(self):
        """Test metadata preparation with all fields present."""
        row = {
            "metadata": {
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
        }

        result = _prepare_chroma_metadata(row)

        assert result["title"] == "Test Title"
        assert result["artist_name"] == "Test Artist"
        assert result["country"] == "Germany"
        assert result["inception_year"] == 1990
        assert result["chunk_index"] == 1
        assert result["total_chunks"] == 5
        assert result["aliases"] == "Alias1, Alias2"
        assert result["tags"] == "electronic, ambient"
        assert result["similar_artists"] == "Artist A, Artist B"
        assert result["genres"] == "Techno, House"

    def test_prepare_chroma_metadata_sparse_data(self):
        """Test metadata preparation with sparse/missing fields."""
        row = {
            "metadata": {
                "title": "Minimal Title",
                "artist_name": "Minimal Artist",
            }
        }

        result = _prepare_chroma_metadata(row)

        assert result["title"] == "Minimal Title"
        assert result["artist_name"] == "Minimal Artist"
        assert result["country"] == "Unknown"
        assert "inception_year" not in result
        assert "aliases" not in result
        assert "tags" not in result

    def test_prepare_chroma_metadata_empty_lists(self):
        """Test that empty lists are not included in metadata."""
        row = {
            "metadata": {
                "title": "Test",
                "artist_name": "Artist",
                "aliases": [],
                "tags": [],
                "genres": [],
            }
        }

        result = _prepare_chroma_metadata(row)

        assert "aliases" not in result
        assert "tags" not in result
        assert "genres" not in result

    def test_prepare_chroma_metadata_no_metadata(self):
        """Test handling of row with no metadata field."""
        row = {}

        result = _prepare_chroma_metadata(row)

        assert result["title"] == ""
        assert result["artist_name"] == ""
        assert result["country"] == "Unknown"


class TestGenerateDocId:
    """Tests for the generate_doc_id helper function."""

    def test_generate_doc_id_returns_hash(self):
        """Test that document ID is a valid SHA256 hash."""
        doc_id = generate_doc_id("Test article text", "row_0")

        assert len(doc_id) == 32
        assert all(c in "0123456789abcdef" for c in doc_id)

    def test_generate_doc_id_deterministic(self):
        """Test that the same input produces the same ID."""
        id1 = generate_doc_id("Same text", "row_5")
        id2 = generate_doc_id("Same text", "row_5")

        assert id1 == id2

    def test_generate_doc_id_unique_by_row_hash(self):
        """Test that different row hashes produce different IDs."""
        id1 = generate_doc_id("Same text", "row_0")
        id2 = generate_doc_id("Same text", "row_1")

        assert id1 != id2


class TestIterBatches:
    """Tests for the _iter_batches generator function."""

    def test_iter_batches_returns_correct_batch_count(self):
        """Test that the correct number of batches is yielded."""
        lf = pl.DataFrame({"a": range(10)}).lazy()

        batches = list(_iter_batches(lf, batch_size=3))

        assert len(batches) == 4
        assert len(batches[0]) == 3
        assert len(batches[1]) == 3
        assert len(batches[2]) == 3
        assert len(batches[3]) == 1

    def test_iter_batches_empty_lazyframe(self):
        """Test handling of empty LazyFrame."""
        lf = pl.DataFrame({"a": []}).lazy()

        batches = list(_iter_batches(lf, batch_size=10))

        assert len(batches) == 0

    def test_iter_batches_single_batch(self):
        """Test when all data fits in a single batch."""
        lf = pl.DataFrame({"a": range(5)}).lazy()

        batches = list(_iter_batches(lf, batch_size=10))

        assert len(batches) == 1
        assert len(batches[0]) == 5


class TestProcessBatch:
    """Tests for the _process_batch function."""

    def test_process_batch_valid_data(self):
        """Test processing a batch with valid articles."""
        batch_df = pl.DataFrame([
            {"id": "Q1", "article": "Article content 1", "metadata": {"title": "T1", "artist_name": "A1"}},
            {"id": "Q2", "article": "Article content 2", "metadata": {"title": "T2", "artist_name": "A2"}},
        ])

        documents, metadatas, ids = _process_batch(batch_df)

        assert len(documents) == 2
        assert len(metadatas) == 2
        assert len(ids) == 2
        assert documents[0] == "Article content 1"
        assert metadatas[0]["title"] == "T1"

    def test_process_batch_filters_empty_articles(self):
        """Test that empty articles are filtered out."""
        batch_df = pl.DataFrame([
            {"id": "Q1", "article": "Valid content", "metadata": {"title": "T1", "artist_name": "A1"}},
            {"id": "Q2", "article": "", "metadata": {"title": "T2", "artist_name": "A2"}},
            {"id": "Q3", "article": None, "metadata": {"title": "T3", "artist_name": "A3"}},
        ])

        documents, metadatas, ids = _process_batch(batch_df)

        assert len(documents) == 1
        assert documents[0] == "Valid content"

    def test_process_batch_all_empty(self):
        """Test handling of batch with all empty articles."""
        batch_df = pl.DataFrame([
            {"id": "Q1", "article": "", "metadata": {"title": "T1", "artist_name": "A1"}},
            {"id": "Q2", "article": None, "metadata": {"title": "T2", "artist_name": "A2"}},
        ])

        documents, metadatas, ids = _process_batch(batch_df)

        assert documents == []
        assert metadatas == []
        assert ids == []


class TestIngestVectorDbAsset:
    """Tests for the ingest_vector_db asset."""

    @pytest.fixture
    def mock_chromadb_resource(self):
        """Create a mock ChromaDB resource."""
        resource = MagicMock()
        resource.model_name = "nomic-ai/nomic-embed-text-v1.5"
        resource.batch_size = 2
        resource.embedding_batch_size = 64
        resource.db_path = "/tmp/test_db"
        resource.collection_name = "test_collection"
        return resource

    @pytest.fixture
    def sample_articles_lazyframe(self):
        """Create sample Wikipedia articles LazyFrame."""
        return pl.DataFrame([
            {
                "id": "Q1_chunk_1",
                "article": "search_document: Artist A | First chunk of text",
                "metadata": {
                    "title": "Artist A",
                    "artist_name": "Artist A",
                    "country": "Germany",
                    "wikipedia_url": "https://en.wikipedia.org/wiki/Artist_A",
                    "wikidata_uri": "https://www.wikidata.org/entity/Q1",
                    "inception_year": 1990,
                    "chunk_index": 1,
                    "total_chunks": 2,
                    "genres": ["Techno"],
                }
            },
            {
                "id": "Q1_chunk_2",
                "article": "search_document: Artist A | Second chunk of text",
                "metadata": {
                    "title": "Artist A",
                    "artist_name": "Artist A",
                    "country": "Germany",
                    "wikipedia_url": "https://en.wikipedia.org/wiki/Artist_A",
                    "wikidata_uri": "https://www.wikidata.org/entity/Q1",
                    "inception_year": 1990,
                    "chunk_index": 2,
                    "total_chunks": 2,
                    "genres": ["Techno"],
                }
            },
            {
                "id": "Q2_chunk_1",
                "article": "search_document: Artist B | Only chunk",
                "metadata": {
                    "title": "Artist B",
                    "artist_name": "Artist B",
                    "country": "UK",
                }
            },
        ]).lazy()

    @patch("data_pipeline.defs.assets.ingest_vector_db.NomicEmbeddingFunction")
    @patch("data_pipeline.defs.assets.ingest_vector_db.get_device")
    def test_ingest_vector_db_success(
        self,
        mock_get_device,
        mock_embedding_class,
        mock_chromadb_resource,
        sample_articles_lazyframe,
    ):
        """Test successful ingestion of documents."""
        mock_get_device.return_value = MagicMock(__str__=lambda self: "cpu")

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
            sample_articles_lazyframe
        )

        assert isinstance(result, MaterializeResult)

        def get_metadata_value(metadata, key):
            val = metadata.get(key)
            return val.value if hasattr(val, "value") else val

        assert get_metadata_value(result.metadata, "documents_processed") == 3
        assert get_metadata_value(result.metadata, "collection_total") == 3
        assert get_metadata_value(result.metadata, "status") == "success"
        assert get_metadata_value(result.metadata, "embedding_batch_size") == 64

        assert mock_collection.upsert.call_count == 2

    @patch("data_pipeline.defs.assets.ingest_vector_db.NomicEmbeddingFunction")
    @patch("data_pipeline.defs.assets.ingest_vector_db.get_device")
    def test_ingest_vector_db_empty_input(
        self,
        mock_get_device,
        mock_embedding_class,
        mock_chromadb_resource,
    ):
        """Test handling of empty input LazyFrame."""
        mock_get_device.return_value = MagicMock(__str__=lambda self: "cpu")
        mock_embedding_class.return_value = MagicMock()

        empty_lf = pl.DataFrame({
            "id": [],
            "article": [],
            "metadata": [],
        }).lazy()

        context = build_asset_context()

        result = ingest_vector_db(
            context,
            mock_chromadb_resource,
            empty_lf,
        )

        assert isinstance(result, MaterializeResult)

        def get_metadata_value(metadata, key):
            val = metadata.get(key)
            return val.value if hasattr(val, "value") else val

        assert get_metadata_value(result.metadata, "documents_processed") == 0
        assert get_metadata_value(result.metadata, "status") == "empty_input"

    @patch("data_pipeline.defs.assets.ingest_vector_db.NomicEmbeddingFunction")
    @patch("data_pipeline.defs.assets.ingest_vector_db.get_device")
    def test_ingest_vector_db_filters_empty_articles(
        self,
        mock_get_device,
        mock_embedding_class,
        mock_chromadb_resource,
    ):
        """Test that articles with empty content are filtered out."""
        mock_get_device.return_value = MagicMock(__str__=lambda self: "cpu")
        mock_embedding_class.return_value = MagicMock()

        mock_collection = MagicMock()
        mock_collection.count.return_value = 1

        @contextmanager
        def mock_get_collection(context, embedding_function=None):
            yield mock_collection

        mock_chromadb_resource.get_collection = mock_get_collection

        articles_with_empty = pl.DataFrame([
            {
                "id": "Q1_chunk_1",
                "article": "Valid article content",
                "metadata": {"title": "Valid", "artist_name": "Artist"},
            },
            {
                "id": "Q2_chunk_1",
                "article": "",
                "metadata": {"title": "Empty", "artist_name": "Artist"},
            },
            {
                "id": "Q3_chunk_1",
                "article": None,
                "metadata": {"title": "None", "artist_name": "Artist"},
            },
        ]).lazy()

        context = build_asset_context()

        result = ingest_vector_db(
            context,
            mock_chromadb_resource,
            articles_with_empty,
        )

        def get_metadata_value(metadata, key):
            val = metadata.get(key)
            return val.value if hasattr(val, "value") else val

        assert get_metadata_value(result.metadata, "documents_processed") == 1
        assert get_metadata_value(result.metadata, "documents_filtered") == 2
        assert mock_collection.upsert.call_count == 1

    @patch("data_pipeline.defs.assets.ingest_vector_db.NomicEmbeddingFunction")
    @patch("data_pipeline.defs.assets.ingest_vector_db.get_device")
    def test_ingest_vector_db_batch_processing(
        self,
        mock_get_device,
        mock_embedding_class,
        mock_chromadb_resource,
    ):
        """Test that documents are processed in correct batch sizes."""
        mock_get_device.return_value = MagicMock(__str__=lambda self: "cpu")
        mock_embedding_class.return_value = MagicMock()

        mock_collection = MagicMock()
        mock_collection.count.return_value = 5

        @contextmanager
        def mock_get_collection(context, embedding_function=None):
            yield mock_collection

        mock_chromadb_resource.get_collection = mock_get_collection
        mock_chromadb_resource.batch_size = 2

        five_articles = pl.DataFrame([
            {"id": f"Q{i}", "article": f"Article {i}", "metadata": {"title": f"T{i}", "artist_name": f"A{i}"}}
            for i in range(5)
        ]).lazy()

        context = build_asset_context()

        result = ingest_vector_db(
            context,
            mock_chromadb_resource,
            five_articles,
        )

        assert mock_collection.upsert.call_count == 3
