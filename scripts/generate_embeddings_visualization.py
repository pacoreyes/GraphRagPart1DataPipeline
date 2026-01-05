# -----------------------------------------------------------
# Generate Embeddings Visualization Script
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Standalone script to generate an interactive Nomic map for ChromaDB embeddings.

This script connects to the local ChromaDB instance, fetches the stored 
embeddings and metadata, and uploads them to Nomic Atlas to create a 
visual, interactive map of the vector space.

Usage:
    python -m scripts.visualize_chroma_db
"""

import os
from typing import Optional

import chromadb
import nomic
import numpy as np
from nomic import atlas
from nomic.data_inference import NomicTopicOptions
from tqdm import tqdm

from src.data_pipeline.settings import settings


VECTOR_DB_DIRPATH = settings.vector_db_dirpath
COLLECTION_NAME = settings.DEFAULT_COLLECTION_NAME


def main() -> None:
    """
    Main execution function to generate the Nomic Atlas visualization.
    """
    print("--- ChromaDB Vector Space Visualization ---")

    # 1. Authenticate with Nomic
    api_key = os.getenv("NOMIC_API_KEY")
    if api_key:
        nomic.login(api_key)
    else:
        print("Warning: NOMIC_API_KEY not found in environment.")

    collection_name = COLLECTION_NAME
    project_name = "musicRAG ChromaDB Visualization"
    max_documents: Optional[int] = None

    # 2. Check Database Existence
    if not VECTOR_DB_DIRPATH.exists():
        print(f"Error: Database path '{VECTOR_DB_DIRPATH}' does not exist.")
        return

    # 3. Connect to ChromaDB
    print(f"Connecting to ChromaDB at '{VECTOR_DB_DIRPATH}'...")
    client = chromadb.PersistentClient(path=str(VECTOR_DB_DIRPATH))

    try:
        collection = client.get_collection(name=collection_name)
    except ValueError:
        print(f"Error: Collection '{collection_name}' not found.")
        return

    # 4. Fetch Data from Collection
    total_docs = collection.count()
    limit = max_documents if max_documents else total_docs

    if total_docs == 0:
        print("Error: The collection is empty.")
        return

    print(f"Fetching {limit} documents from collection '{collection_name}'...")

    # Fetch in batches for efficiency
    batch_size = 500
    all_results = {"ids": [], "embeddings": [], "metadatas": []}

    with tqdm(total=limit, desc="Fetching documents") as pbar:
        for offset in range(0, limit, batch_size):
            batch = collection.get(
                limit=min(batch_size, limit - offset),
                offset=offset,
                include=["embeddings", "metadatas"],
            )
            all_results["ids"].extend(batch["ids"])
            all_results["embeddings"].extend(batch["embeddings"])
            all_results["metadatas"].extend(batch["metadatas"])
            pbar.update(len(batch["ids"]))

    if not all_results["embeddings"]:
        print("Error: No documents with embeddings found to visualize.")
        return

    # 5. Prepare Data for Nomic Atlas
    print("Preparing data for Nomic Atlas...")
    embeddings = np.array(all_results["embeddings"])
    metadata = all_results["metadatas"]

    # Ensure each record has an ID for Nomic
    for i, doc_id in enumerate(all_results["ids"]):
        metadata[i]["id"] = doc_id

    # 6. Create Nomic Atlas Map
    print(f"Creating Nomic Atlas project '{project_name}'...")
    try:
        project = atlas.map_data(
            data=metadata,
            embeddings=embeddings,
            identifier=project_name,
            id_field="id",
            topic_model=NomicTopicOptions(
                build_topic_model=True,
                topic_label_field="artist_name",
            ),
        )
        print("\nSUCCESS: Interactive Nomic map created!")
        print(f"View your map at: {project.maps[0].map_link}")
    except ValueError as e:
        if "You have not configured your Nomic API token" in str(e):
            print("\nERROR: Nomic API token not configured.")
            print("Please run 'nomic login' in your terminal or set NOMIC_API_KEY.")
        else:
            print(f"\nERROR: Failed to create Nomic map: {e}")


if __name__ == "__main__":
    main()

"""
DO NOT DELETE
python -m scripts.visualize_chroma_db
"""
