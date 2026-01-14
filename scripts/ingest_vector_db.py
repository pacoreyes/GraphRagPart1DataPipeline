import os

# Enable MPS fallback for operations not yet implemented on Apple Silicon
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

import hashlib
import queue
import threading
from pathlib import Path
from typing import Tuple, List, Any

from data_pipeline.settings import settings

import polars as pl
import torch
import chromadb
from chromadb.api.models import Collection
from chromadb import Documents, Embeddings, EmbeddingFunction
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# --- CONFIGURATION for Colab ---
# Assumes the data file is uploaded to the root of the Colab environment
WIKIPEDIA_ARTICLES_FILE = Path(settings.DATA_DIR / "datasets" / "wikipedia_articles.jsonl")
CHROMA_DB_PATH = Path(settings.DATA_DIR / "vector_db")
DEFAULT_EMBEDDINGS_MODEL_NAME = "nomic-ai/nomic-embed-text-v1.5"
DEFAULT_COLLECTION_NAME = "electronic_music_collection"

# Optimized batch size for Apple M2 (Unified Memory)
# 64 provides a good balance between throughput and memory pressure.
BATCH_SIZE = 64

TEST_QUERY_TEXT = "Depeche Mode?"

# ---------------------


def get_device():
  """Returns the appropriate device available in the system: CUDA, MPS, or CPU"""
  if torch.backends.mps.is_available():
    return torch.device("mps")
  elif torch.cuda.is_available():
    return torch.device("cuda")
  else:
    return torch.device("cpu")


# --- CRITICAL: CUSTOM CLASS FOR NOMIC ---
class NomicEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    1. Sets trust_remote_code=True.
    2. Adds prefixes for Nomic model.
    3. Leverages batch processing for speed.
    4. Uses FP16 for performance on MPS/CUDA.
    """
    def __init__(self, model_name: str, device: str, *args: Any, **kwargs: Any) -> None:
        print(f"Loading model '{model_name}' on device '{device}'...")
        self.model = SentenceTransformer(
            model_name,
            device=device,
            trust_remote_code=True
        )
        
        # Optimization 1: Enable Half-Precision (FP16)
        if device != "cpu":
            print("Enabling FP16 (half-precision) inference.")
            self.model.half()
            
        self.model.eval()

    def __call__(self, input: Documents) -> Embeddings:
        # Nomic requires "search_document: " prefix for indexing
        processed_input = []
        for text in input:
            if not text.startswith("search_document:"):
                processed_input.append(f"search_document: {text}")
            else:
                processed_input.append(text)

        # The SentenceTransformer model will automatically handle truncation for inputs
        # longer than the model's max sequence length (8192 for nomic-embed-text).
        embeddings = self.model.encode(
            processed_input,
            convert_to_numpy=True,
            show_progress_bar=False,  # Progress bar is handled by the main ingestion loop
            normalize_embeddings=True,
        )

        return embeddings.tolist()

    def embed_query(self, query: str) -> List[float]:
        # Nomic requires "search_query: " prefix for searching
        prefixed_query = f"search_query: {query}"
        embedding = self.model.encode(
            [prefixed_query],
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        return embedding[0].tolist()


def get_chroma_collection(
    db_path: Path,
    collection_name: str,
    device: str,
    model_name: str = DEFAULT_EMBEDDINGS_MODEL_NAME,
) -> Tuple[Collection, 'NomicEmbeddingFunction']:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(db_path))

    emb_fn = NomicEmbeddingFunction(model_name=model_name, device=device)

    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=emb_fn,
    )
    return collection, emb_fn


def load_data_polars(file_path: Path) -> pl.DataFrame:
    """
    Optimization 3: Use Polars to read the JSONL file.
    This is significantly faster than python's json.loads loop.
    """
    print(f"Reading JSONL file with Polars from: {file_path}")
    if not file_path.exists():
        print(f"Error: The file '{file_path}' was not found.")
        raise SystemExit(1)
        
    try:
        # read_ndjson is eager but very fast. 
        # For M2 with Unified Memory, this efficiently loads data into compact Arrow format.
        df = pl.read_ndjson(str(file_path))
        return df
    except Exception as e:
        print(f"Error reading file with Polars: {e}")
        raise SystemExit(1)


def db_writer_worker(
    collection: Collection, 
    write_queue: queue.Queue,
    pbar: Any
) -> None:
    """
    Consumer thread: Reads from queue and writes to ChromaDB.
    """
    while True:
        item = write_queue.get()
        if item is None:
            write_queue.task_done()
            break
            
        batch_ids, batch_docs, batch_metas, batch_embeddings = item
        
        try:
            # We pass generated embeddings directly to save Chroma from calling the model again
            collection.upsert(
                ids=batch_ids,
                documents=batch_docs,
                metadatas=batch_metas,
                embeddings=batch_embeddings
            )
            pbar.update(1)
        except Exception as e:
            print(f"Error writing batch to DB: {e}")
            
        write_queue.task_done()


def ingest_data_parallel(
    collection: Collection,
    emb_fn: 'NomicEmbeddingFunction',
    df: pl.DataFrame,
    batch_size: int = BATCH_SIZE,
) -> None:
    """
    Optimization 2: Producer-Consumer pattern.
    Main thread: Prepares data + Generates Embeddings (GPU).
    Worker thread: Writes to ChromaDB (I/O).
    """
    total_docs = len(df)
    if total_docs == 0:
        print("No documents to ingest.")
        return

    print(f"Starting parallel ingestion of {total_docs} documents...")
    
    # Queue for communicating between GPU thread (producer) and Disk thread (consumer)
    # limit size to prevent memory explosion if writer is slow
    write_queue = queue.Queue(maxsize=4) 
    
    total_batches = (total_docs + batch_size - 1) // batch_size
    
    # Progress bar tracks BATCHES written
    pbar = tqdm(total=total_batches, desc="Ingesting batches (Parallel)", unit="batch")
    
    # Start Writer Thread
    writer_thread = threading.Thread(
        target=db_writer_worker, 
        args=(collection, write_queue, pbar),
        daemon=True
    )
    writer_thread.start()
    
    # Main Loop (Producer)
    # Iterate through Polars DataFrame in chunks
    for i in range(0, total_docs, batch_size):
        # Slicing Polars DF and converting to dicts is very fast
        slice_dicts = df[i: i + batch_size].to_dicts()
        
        batch_docs = []
        batch_metas = []
        batch_ids = []
        
        for index, row in enumerate(slice_dicts):
            # Maintain the robust parsing logic
            article_text = row.get("article")
            metadata_obj = row.get("metadata")
            
            # Robust checks
            if not article_text or not metadata_obj or not metadata_obj.get("title"):
                continue
                
            title = metadata_obj.get("title")
            
            # Prepare metadata
            chroma_metadata = {
                "title": title,
                "artist_name": metadata_obj.get("artist_name"),
                "inception_year": metadata_obj.get("inception_year", 0),
                "wikipedia_url": metadata_obj.get("wikipedia_url", "N/A"),
                "wikidata_uri": metadata_obj.get("wikidata_uri", "N/A"),
                "chunk_index": metadata_obj.get("chunk_index", "N/A"),
                "total_chunks": metadata_obj.get("total_chunks", "N/A"),
            }
            
            # Handle list fields
            if metadata_obj.get("aliases"):
                chroma_metadata["aliases"] = ", ".join(map(str, metadata_obj.get("aliases")))
            if metadata_obj.get("tags"):
                chroma_metadata["tags"] = ", ".join(map(str, metadata_obj.get("tags")))
            if metadata_obj.get("similar_artists"):
                chroma_metadata["similar_artists"] = ", ".join(map(str, metadata_obj.get("similar_artists")))
            if metadata_obj.get("genres"):
                chroma_metadata["genres"] = ", ".join(map(str, metadata_obj.get("genres")))
                
            # Generate ID (use global index i + local index)
            global_index = i + index
            doc_id = hashlib.sha256(f"{article_text}-{global_index}".encode("utf-8")).hexdigest()
            
            batch_docs.append(article_text)
            batch_metas.append(chroma_metadata)
            batch_ids.append(doc_id)
            
        if not batch_docs:
            pbar.update(1)  # Skip empty batch
            continue

        # GPU HEAVY WORK: Compute embeddings here on main thread
        batch_embeddings = emb_fn(batch_docs)
        
        # Put to queue for writer
        write_queue.put((batch_ids, batch_docs, batch_metas, batch_embeddings))
        
    # Signal completion
    write_queue.put(None)
    
    # Wait for writer to finish
    writer_thread.join()
    pbar.close()
    print("Success! Data ingestion complete.")


def test_query(collection: Collection, emb_fn: 'NomicEmbeddingFunction') -> None:
    print("\n--- Testing Query ---")
    if collection.count() == 0:
        print("Collection is empty. Cannot perform a query.")
        return

    query_vec = emb_fn.embed_query(TEST_QUERY_TEXT)

    results = collection.query(
        query_embeddings=[query_vec],
        n_results=1,
    )
    if results and results.get("documents"):
        print(f"Query: {TEST_QUERY_TEXT}")
        print("Found document:", results["documents"][0][0][:200], "...")
        print("Metadata:", results["metadatas"][0][0])
    else:
        print("Query returned no results.")


def main() -> None:
    """Main function to run the ingestion process."""
    device = get_device()
    print(f"Using device: {device}")

    # 1. Setup Collection
    collection, emb_fn = get_chroma_collection(
        CHROMA_DB_PATH, DEFAULT_COLLECTION_NAME, device
    )

    # 2. Load Data (Polars - Fast)
    df = load_data_polars(WIKIPEDIA_ARTICLES_FILE)
    
    # 3. Ingest (Parallel: GPU + I/O)
    ingest_data_parallel(collection, emb_fn, df, BATCH_SIZE)

    # 4. Verify
    test_query(collection, emb_fn)


if __name__ == "__main__":
    main()
