# ----------------------------------------------------------- 
# Query Embeddings Script
# Standalone Script for Querying ChromaDB
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# ----------------------------------------------------------- 

"""
Standalone script to query a ChromaDB collection.
Reuses core embedding logic for consistency.
"""

from data_pipeline.settings import settings
from data_pipeline.utils.chroma_helpers import (
    get_chroma_client,
    get_collection_with_embedding
)


def main() -> None:
    # Configuration
    db_path = settings.VECTOR_DB_DIRPATH
    collection_name = settings.DEFAULT_COLLECTION_NAME
    n_results = 8

    print(f"Connecting to ChromaDB at {db_path}...")
    try:
        client = get_chroma_client(db_path)
        collection, emb_fn = get_collection_with_embedding(client, collection_name)
    except Exception as e:
        print(f"Error initializing DB: {e}")
        return

    print("\n--- Interactive Query Mode ---")
    print("Type your query and press Enter. Type 'exit' or 'quit' to stop.\n")

    while True:
        try:
            query_text = input("Query: ").strip()
            if not query_text:
                continue
            if query_text.lower() in ["exit", "quit"]:
                print("Goodbye!")
                break

            print(f"\nSearching for: '{query_text}'")
            print("-" * 30)

            # Perform query
            query_embedding = emb_fn.embed_query(query_text)
            
            results = collection.query(
                query_embeddings=query_embedding,
                n_results=n_results,
                include=["documents", "metadatas", "distances"]
            )

            if not results or not results.get("ids") or not results["ids"][0]:
                print("No results found.")
                continue

            print("\nResults (sorted by distance - lower is better):")
            print("-" * 60)
            
            # Iterate through the first query result list
            ids = results["ids"][0]

            metadatas = results["metadatas"][0] if results.get("metadatas") else [{}] * len(ids)
            distances = results["distances"][0] if results.get("distances") else ["N/A"] * len(ids)
            documents = results["documents"][0] if results.get("documents") else ["N/A"] * len(ids)

            for i, doc_id in enumerate(ids):
                meta = metadatas[i] or {}
                dist = distances[i]
                snippet = documents[i][:500].replace("\n", " ") + "..."

                print(f"Result {i + 1}:")
                print(f"  - ID:       {doc_id}")
                print(f"  - Chunk index: {meta['chunk_index']}  |  Total chunks: {meta['total_chunks']}")
                print(f"  - Distance: {dist:.4f}" if isinstance(dist, float) else f"  - Distance: {dist}")
                print(f"  - Title:    {meta['title']} | Artist:   {meta['artist_name']}")
                print(f"  - Aliases:  {meta.get('aliases', 'N/A')}")
                print(f"  - Similar:  {meta.get('similar_artists', 'N/A')}")
                print(f"  - Genres:   {meta.get('genres', 'N/A')}")
                print(f"  - Tags:     {meta.get('tags', 'N/A')}")
                print(f"  - Country:  {meta.get('country', 'N/A')}")
                print(f"  - Inception: {meta.get('inception_year', 'N/A')}")
                print(f". - Wikipedia URL: {meta['wikipedia_url']}  |  WikiData: {meta['wikidata_uri']}")
                print(f"  - Snippet:  {snippet}")
                print("-" * 30)
            print()  # Extra newline for readability

        except (KeyboardInterrupt, EOFError):
            print("\nExiting...")
            break


if __name__ == "__main__":
    main()
