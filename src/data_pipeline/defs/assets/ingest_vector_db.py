# -----------------------------------------------------------
# Ingest Wikipedia article chunks into ChromaD Vector DB Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# This asset consumes pre-processed article chunks (already containing
# the 'search_document:' prefix from extract_wikipedia_articles) and
# ingests them into a ChromaDB collection with embeddings generated
# using the Nomic model.
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Iterator, Any

import polars as pl
from dagster import asset, AssetExecutionContext, MaterializeResult
from tqdm import tqdm

from data_pipeline.defs.resources import ChromaDBResource
from data_pipeline.utils.chroma_helpers import (
    NomicEmbeddingFunction,
    get_device,
    generate_doc_id
)


def _prepare_chroma_metadata(row: dict[str, Any]) -> dict[str, Any]:
    """
    Prepares metadata for ChromaDB from an article row.
    Handles sparse data by excluding empty/null values.
    
    Args:
        row: Dictionary containing article data with 'metadata' field.

    Returns:
        Flattened metadata dictionary suitable for ChromaDB.
    """
    metadata = row.get("metadata") or {}

    result = {
        "title": metadata.get("title") or "",
        "artist_name": metadata.get("artist_name") or "",
        "country": metadata.get("country") or "Unknown",
        "wikipedia_url": metadata.get("wikipedia_url") or "",
        "wikidata_uri": metadata.get("wikidata_uri") or "",
    }

    if metadata.get("inception_year"):
        result["inception_year"] = metadata["inception_year"]

    if metadata.get("chunk_index"):
        result["chunk_index"] = metadata["chunk_index"]

    if metadata.get("total_chunks"):
        result["total_chunks"] = metadata["total_chunks"]

    if metadata.get("aliases"):
        result["aliases"] = ", ".join(str(a) for a in metadata["aliases"])

    if metadata.get("tags"):
        result["tags"] = ", ".join(str(t) for t in metadata["tags"])

    if metadata.get("similar_artists"):
        result["similar_artists"] = ", ".join(
            str(s) for s in metadata["similar_artists"]
        )

    if metadata.get("genres"):
        result["genres"] = ", ".join(str(g) for g in metadata["genres"])

    return result


def _iter_batches(lf: pl.LazyFrame, batch_size: int) -> Iterator[pl.DataFrame]:
    """
    Yields batches from a LazyFrame using streaming.

    Args:
        lf: Polars LazyFrame to iterate.
        batch_size: Number of rows per batch.

    Yields:
        DataFrame batches.
    """
    offset = 0
    while True:
        batch = lf.slice(offset, batch_size).collect()
        if batch.is_empty():
            break
        yield batch
        offset += batch_size


def _process_batch(
    batch_df: pl.DataFrame,
) -> tuple[list[str], list[dict], list[str]]:
    """
    Processes a batch of articles for ChromaDB ingestion.

    Args:
        batch_df: DataFrame with article data.

    Returns:
        Tuple of (documents, metadatas, ids) lists.
    """
    documents = []
    metadatas = []
    ids = []

    for row in batch_df.to_dicts():
        article_text = row.get("article")
        if not article_text:
            continue

        doc_id = generate_doc_id(article_text, row.get("id", ""))
        metadata = _prepare_chroma_metadata(row)

        documents.append(article_text)
        metadatas.append(metadata)
        ids.append(doc_id)

    return documents, metadatas, ids


@asset(
    name="vector_db",
    description="Ingests Wikipedia article chunks into ChromaDB vector database.",
    deps=["wikipedia_articles"],
)
def ingest_vector_db(
    context: AssetExecutionContext,
    chromadb: ChromaDBResource,
    wikipedia_articles: pl.LazyFrame,
) -> MaterializeResult:
    """
    Ingests pre-processed Wikipedia article chunks into ChromaDB.

    This asset:
    1. Loads the embedding model (Nomic) on the best available device
    2. Iterates through article chunks in batches
    3. Generates embeddings and upserts to ChromaDB

    Documents are expected to already contain the 'search_document:' prefix
    from the upstream extract_wikipedia_articles asset.

    Args:
        context: Dagster execution context.
        chromadb: ChromaDB resource with collection configuration.
        wikipedia_articles: LazyFrame of pre-processed article chunks.

    Returns:
        MaterializeResult with ingestion statistics.
    """
    device = get_device()
    context.log.info(f"Using compute device: {device}")

    total_rows = wikipedia_articles.select(pl.len()).collect().item()
    if total_rows == 0:
        context.log.warning("No articles to ingest. Input is empty.")
        return MaterializeResult(
            metadata={
                "documents_processed": 0,
                "documents_filtered": 0,
                "status": "empty_input",
            }
        )

    context.log.info(f"Total articles to process: {total_rows}")

    embedding_fn = NomicEmbeddingFunction(
        model_name=chromadb.model_name,
        device=str(device),
    )

    documents_processed = 0
    documents_filtered = 0
    batch_count = 0

    with chromadb.get_collection(
        context, embedding_function=embedding_fn
    ) as collection:
        total_batches = (total_rows + chromadb.batch_size - 1) // chromadb.batch_size

        with tqdm(total=total_batches, desc="Ingesting batches in Chroma DB", unit="batch") as pbar:
            for batch_df in _iter_batches(wikipedia_articles, chromadb.batch_size):
                batch_count += 1
                batch_size_actual = len(batch_df)

                documents, metadatas, ids = _process_batch(batch_df)

                filtered_count = batch_size_actual - len(documents)
                documents_filtered += filtered_count

                if documents:
                    collection.upsert(
                        ids=ids,
                        documents=documents,
                        metadatas=metadatas,
                    )
                    documents_processed += len(documents)

                pbar.update(1)

        final_count = collection.count()
        context.log.info(
            f"Ingestion complete. Processed: {documents_processed}, "
            f"Filtered: {documents_filtered}, Collection total: {final_count}"
        )

    return MaterializeResult(
        metadata={
            "documents_processed": documents_processed,
            "documents_filtered": documents_filtered,
            "collection_total": final_count,
            "batches_processed": batch_count,
            "embedding_batch_size": chromadb.embedding_batch_size,
            "status": "success",
        }
    )
