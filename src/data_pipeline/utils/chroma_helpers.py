# -----------------------------------------------------------
# ChromaDB Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import hashlib
from typing import cast, Union

import torch
from chromadb import Documents, Embeddings, EmbeddingFunction
from sentence_transformers import SentenceTransformer


def get_device() -> torch.device:
    """
    Detects the best available compute device.

    Returns:
        torch.device: CUDA if available, MPS for Apple Silicon, otherwise CPU.
    """
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


class NomicEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    ChromaDB-compatible embedding function using Nomic model.

    This function assumes documents are ALREADY prefixed with 'search_document:'
    from the upstream asset (extract_wikipedia_articles). It does NOT add
    prefixes during embedding generation.

    For query embedding, use `embed_query()` which adds the 'search_query:' prefix.

    Args:
        model_name: HuggingFace model identifier.
        device: Compute device string ('cuda', 'mps', 'cpu').
    """

    def __init__(self, model_name: str, device: str) -> None:  # type: ignore[override]
        super().__init__()  # EmbeddingFunction Protocol will require this in future versions
        self._model = SentenceTransformer(
            model_name, device=device, trust_remote_code=True
        )
        if device != "cpu":
            self._model.half()
        self._model.eval()

    def __call__(self, documents: Documents) -> Embeddings:
        """
        Generates embeddings for documents.

        Documents are expected to already contain the 'search_document:' prefix.

        Args:
            documents: List of document strings to embed.

        Returns:
            List of embedding vectors.
        """
        embeddings = self._model.encode(
            documents,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        # tolist() converts numpy array to list of floats, which matches Embeddings type alias
        return cast(Embeddings, embeddings.tolist())

    def embed_query(self, query: Union[Documents, str]) -> Embeddings:
        """
        Generates embedding for a search query.

        Adds the 'search_query:' prefix required by Nomic for retrieval.

        Args:
            query: Search query string or list of strings.

        Returns:
            Embedding vector for the query.
        """
        # Handle single string query which is common for query embedding
        if isinstance(query, str):
            texts = [query]
        else:
            texts = query

        prefixed = [f"search_query: {t}" for t in texts]
        
        embeddings = self._model.encode(
            prefixed,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return cast(Embeddings, embeddings.tolist())


def generate_doc_id(article_text: str, row_hash: str) -> str:
    """
    Generates a unique document ID using SHA256.

    Args:
        article_text: Article content.
        row_hash: Unique identifier for the row (e.g., 'row_0').

    Returns:
        SHA256 hex digest as document ID.
    """
    combined = f"{article_text}-{row_hash}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()
