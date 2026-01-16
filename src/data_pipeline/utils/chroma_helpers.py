# -----------------------------------------------------------
# ChromaDB Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import hashlib
from pathlib import Path
from typing import cast, Union

import chromadb
import torch
from chromadb.api.client import Client
from chromadb.api.models.Collection import Collection
from chromadb import Documents, Embeddings, EmbeddingFunction
from sentence_transformers import SentenceTransformer

from data_pipeline.settings import settings


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
    from the upstream asset (extract_articles). It does NOT add
    prefixes during embedding generation.

    For query embedding, use `embed_query()` which adds the 'search_query:' prefix.

    Args:
        model_name: HuggingFace model identifier.
        device: Compute device string ('cuda', 'mps', 'cpu').
    """

    def __init__(self, model_name: str, device: str) -> None:  # type: ignore
        # We ignore type checking here because EmbeddingFunction protocol might
        # have a different signature, but we need these specific args.
        self._model = SentenceTransformer(
            model_name, device=device, trust_remote_code=True
        )
        if device != "cpu":
            self._model.half()
        self._model.eval()

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generates embeddings for documents.

        Documents are expected to already contain the 'search_document:' prefix.

        Args:
            input: List of document strings to embed.

        Returns:
            List of embedding vectors.
        """
        embeddings = self._model.encode(
            input,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        # tolist() converts numpy array to list of floats, which matches Embeddings type alias
        return cast(Embeddings, embeddings.tolist())

    def embed_query(self, input: Union[Documents, str]) -> Embeddings:
        """
        Generates embedding for a search query.

        Adds the 'search_query:' prefix required by Nomic for retrieval.

        Args:
            input: Search query string or list of strings.

        Returns:
            Embedding vector for the query.
        """
        # Handle single string input which is common for query embedding
        if isinstance(input, str):
            texts = [input]
        else:
            texts = input

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
    # Truncate to 32 chars to satisfy Nomic Atlas limits (max 36)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()[:32]


def get_chroma_client(db_path: Path) -> Client:
    """
    Returns a PersistentClient for the given database path.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database path '{db_path}' does not exist.")
    # Cast because PersistentClient returns ClientAPI which is compatible with Client protocol
    return cast(Client, chromadb.PersistentClient(path=str(db_path)))


def get_collection_with_embedding(
    client: Client,
    collection_name: str,
    model_name: str = settings.DEFAULT_EMBEDDINGS_MODEL_NAME
) -> tuple[Collection, NomicEmbeddingFunction]:
    """
    Retrieves a collection with the initialized Nomic embedding function.
    
    Returns:
        tuple: (Collection, NomicEmbeddingFunction)
    """
    device = get_device()
    print(f"Initializing embedding model on {device}...")
    
    emb_fn = NomicEmbeddingFunction(model_name=model_name, device=str(device))
    
    try:
        collection = client.get_collection(
            name=collection_name, embedding_function=emb_fn
        )
        return collection, emb_fn
    except ValueError:
        raise ValueError(f"Collection '{collection_name}' not found.")
