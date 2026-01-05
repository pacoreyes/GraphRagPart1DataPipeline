# -----------------------------------------------------------
# Query Embeddings Script
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Standalone script to query a ChromaDB collection with a given text query.

Provides both a one-off query capability and an interactive mode to explore
the vector database. Reuses core embedding logic for consistency.

Usage:
    python scripts/query_embeddings.py "How many albums did Depeche Mode release?"
"""

import argparse
from pathlib import Path
from typing import Dict, Any

import chromadb

from music_rag_etl.settings import DEFAULT_MODEL_NAME, DEFAULT_COLLECTION_NAME
from music_rag_etl.utils.chroma_helpers import (
    NomicEmbeddingFunction,
    get_device,
)


def view_embeddings(collection: chromadb.Collection, limit: int) -> None:
    """
    Fetches and displays a sample of documents from the collection.

    Args:
        collection: The ChromaDB collection to inspect.
        limit: The maximum number of documents to display.
    """
    print(f"Fetching {limit} sample documents from '{collection.name}'...")
    try:
        results = collection.get(
            limit=limit, include=["embeddings", "documents", "metadatas"]
        )
        if not results["ids"]:
            print("No documents found in the collection.")
            return

        for i, doc_id in enumerate(results["ids"]):
            print(f"\n--- Document {i + 1} ---")
            print(f"ID: {doc_id}")
            print(f"Document (snippet): {results['documents'][i][:500]}...")
            print(f"Metadata: {results['metadatas'][i]}")
            emb_snippet = results["embeddings"][i][:10]
            print(f"Embedding (first 10): {emb_snippet}...")
            print(f"Embedding length: {len(results['embeddings'][i])}")
            print("-" * 30)
    except Exception as e:
        print(f"An error occurred while fetching documents: {e}")


def perform_query(
    collection: chromadb.Collection,
    emb_fn: NomicEmbeddingFunction,
    query_text: str,
    n_results: int,
    where_filter: Dict[str, Any],
) -> None:
    """
    Performs a query and prints the results.

    Args:
        collection: The ChromaDB collection to query.
        emb_fn: The embedding function instance.
        query_text: The user's query text.
        n_results: The number of results to retrieve.
        where_filter: A dictionary of metadata filters.
    """
    print(f"\nQuerying for: '{query_text}'")
    print("-" * 30)

    query_embedding = emb_fn.embed_query(query_text)
    query_kwargs = {"query_embeddings": [query_embedding], "n_results": n_results}
    if where_filter:
        query_kwargs["where"] = where_filter
        print(f"Applying filters: {where_filter}")

    results = collection.query(**query_kwargs)
    if not results or not results.get("ids")[0]:
        print("No results found.")
        return

    print("\nResults (sorted by distance - lower is better):")
    print("-" * 30)
    for i, doc_id in enumerate(results["ids"][0]):
        metadata = results["metadatas"][0][i]
        print(f"Result {i + 1}:")
        print(f"  - ID:       {doc_id}")
        print(f"  - Title:    {metadata.get('artist_name', 'N/A')}")
        print(f"  - URL:      {metadata.get('wikipedia_url', 'N/A')}")
        print(f"  - Distance: {results['distances'][0][i]:.4f}")
        print(f"  - Score:    {metadata.get('relevance_score', 'N/A')}")
        print(
            f"  - Chunks:   {metadata.get('chunk_index', 'N/A')} of "
            f"{metadata.get('total_chunks', 'N/A')}"
        )
        print(f"  - Document (snippet): {results['documents'][0][i][:600]}...")
        print("-" * 30)


def _setup_arg_parser() -> argparse.ArgumentParser:
    """
    Configures the command-line argument parser.

    Returns:
        argparse.ArgumentParser: The configured argument parser.
    """
    parser = argparse.ArgumentParser(
        description="Query or view a ChromaDB collection interactively."
    )
    parser.add_argument(
        "query_text",
        type=str,
        nargs="?",
        default=None,
        help="Optional initial query. Enters interactive mode after.",
    )
    parser.add_argument(
        "-n", "--n-results", type=int, default=5, help="Number of results to retrieve."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data_volume/vector_db"),
        help="Path to the ChromaDB directory.",
    )
    parser.add_argument(
        "--collection",
        type=str,
        default=DEFAULT_COLLECTION_NAME,
        help=f"Collection name to query (default: {DEFAULT_COLLECTION_NAME}).",
    )
    parser.add_argument(
        "--view-embeddings",
        action="store_true",
        help="If set, view a sample of embeddings and exit.",
    )
    parser.add_argument(
        "--filter-genre",
        type=str,
        help="Filter by genre (case-insensitive, partial match).",
    )
    parser.add_argument("--filter-min-year", type=int, help="Filter by minimum year.")
    parser.add_argument("--filter-max-year", type=int, help="Filter by maximum year.")
    return parser


def main() -> None:
    """
    Main function to handle arguments and run the query loop.
    """
    parser = _setup_arg_parser()
    args = parser.parse_args()

    if not args.db_path.exists():
        print(f"Error: DB path '{args.db_path}' not found. Ensure the database exists.")
        return

    if (
        args.filter_min_year is not None
        and args.filter_max_year is not None
        and args.filter_min_year > args.filter_max_year
    ):
        print("Error: --filter-min-year cannot exceed --filter-max-year.")
        return

    device = get_device()
    print(f"Using device: {device}")

    emb_fn = NomicEmbeddingFunction(model_name=DEFAULT_MODEL_NAME, device=device)
    client = chromadb.PersistentClient(path=str(args.db_path))

    try:
        collection = client.get_collection(
            name=args.collection, embedding_function=emb_fn
        )
    except ValueError:
        print(f"Error: Collection '{args.collection}' not found.")
        return

    if args.view_embeddings:
        view_embeddings(collection, args.n_results)
        return

    where_filter: Dict[str, Any] = {}
    if args.filter_genre:
        where_filter["genres"] = {"$contains": args.filter_genre}
    
    year_filter = {}
    if args.filter_min_year is not None:
        year_filter["$gte"] = args.filter_min_year
    if args.filter_max_year is not None:
        year_filter["$lte"] = args.filter_max_year
    
    if year_filter:
        where_filter["inception_year"] = year_filter

    if args.query_text:
        perform_query(collection, emb_fn, args.query_text, args.n_results, where_filter)

    while True:
        try:
            query_text = input("\nEnter your query (or 'quit' to exit): ")
            if query_text.lower().strip() in ["quit", "exit"]:
                print("Exiting interactive mode.")
                break
            if not query_text.strip():
                continue
            perform_query(collection, emb_fn, query_text, args.n_results, where_filter)
        except (KeyboardInterrupt, EOFError):
            print("\nExiting...")
            break


if __name__ == "__main__":
    main()

"""
DO NOT DELETE
python -m scripts.query_chromadb
"""