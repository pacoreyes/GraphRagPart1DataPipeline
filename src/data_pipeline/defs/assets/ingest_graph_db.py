# -----------------------------------------------------------
# Ingest Graph DB Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from itertools import islice
from typing import Iterable, TypeVar

import msgspec
from dagster import AssetExecutionContext, MaterializeResult, asset
from gqlalchemy import Memgraph

from data_pipeline.models import Album, Artist, Genre, Track
from data_pipeline.settings import settings
from data_pipeline.utils.io_helpers import read_items_from_jsonl
from data_pipeline.utils.memgraph_helpers import (
    MemgraphConfig,
    clear_database,
    create_indexes,
    get_memgraph_client,
)

T = TypeVar("T")


def chunked_iterable(iterable: Iterable[T], size: int) -> Iterable[list[T]]:
    """Yield successive chunks from an iterable."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def execute_cypher(memgraph: Memgraph, query: str, params: dict | None = None) -> None:
    """
    Executes a Cypher query with optional parameters using the underlying pymgclient cursor.
    This bypasses GQLAlchemy's execute() which does not support parameters.
    """
    # Create a new connection for this transaction
    conn = memgraph.new_connection()
    try:
        # Access the raw pymgclient connection
        # Note: _connection is a protected member of MemgraphConnection, 
        # but required because GQLAlchemy wrappers don't expose param binding.
        cursor = conn._connection.cursor()  # type: ignore
        cursor.execute(query, params or {})
        
        # Ensure execution is flushed/consumed
        # We perform a fetchall to ensure the driver sends the query, 
        # even for non-returning queries (like CREATE).
        cursor.fetchall()
            
        conn._connection.commit()
    except Exception as e:
        # Ensure we roll back on error if possible, though commit won't have happened
        raise e
    finally:
        # Explicitly close to prevent pool exhaustion if pooling is used
        # (Though new_connection() usually creates a fresh one)
        pass


@asset(
    name="load_graph_db",
    deps=["extract_genres"],
    description="Ingests in Memgraph Artists, Albums, Tracks, and Genres",
)
def load_graph_db(
    context: AssetExecutionContext, config: MemgraphConfig
) -> MaterializeResult:
    """
    Dagster asset that ingests music data into the Memgraph database efficiently.

    Updates:
    - Added Album -> Genre and Track -> Genre relationships.
    - Added Genre -> Genre (SUBGENRE_OF) hierarchy.
    - Implemented self-loop prevention for SIMILAR_TO and SUBGENRE_OF.
    """
    memgraph: Memgraph = get_memgraph_client(config)
    batch_size = settings.MEMGRAPH_INGESTION_BATCH_SIZE

    # --- Step 1: Clear Database & Prepare ---
    clear_database(memgraph, context)

    # --- Step 2: Node Ingestion ---
    context.log.info("Starting Stage 1: Node Ingestion")

    # 1. Genres
    genres_path = settings.genres_filepath
    genre_count = 0
    genre_query = """
    UNWIND $batch AS row
    CREATE (:Genre {
        id: row.id, 
        name: row.name, 
        aliases: row.aliases,
        parent_ids: row.parent_ids
    });
    """
    genre_stream = read_items_from_jsonl(genres_path, Genre)
    for batch in chunked_iterable(genre_stream, batch_size):
        batch_data = [msgspec.to_builtins(item) for item in batch]
        execute_cypher(memgraph, genre_query, {"batch": batch_data})
        genre_count += len(batch)
    context.log.info(f"Loaded {genre_count} genres.")

    # 2. Artists
    artists_path = settings.artists_filepath
    artist_count = 0
    artist_query = """
    UNWIND $batch AS row
    CREATE (:Artist {
        id: row.id, 
        name: row.name, 
        country: row.country, 
        aliases: row.aliases,
        tags: row.tags,
        genres: row.genres,
        similar_artists: row.similar_artists
    });
    """
    artist_stream = read_items_from_jsonl(artists_path, Artist)
    for batch in chunked_iterable(artist_stream, batch_size):
        batch_data = [msgspec.to_builtins(item) for item in batch]
        execute_cypher(memgraph, artist_query, {"batch": batch_data})
        artist_count += len(batch)
    context.log.info(f"Loaded {artist_count} artists.")

    # 3. Albums
    albums_path = settings.albums_filepath
    album_count = 0
    album_query = """
    UNWIND $batch AS row
    CREATE (:Album {
        id: row.id, 
        title: row.title, 
        year: row.year,
        artist_id: row.artist_id,
        genres: row.genres
    });
    """
    album_stream = read_items_from_jsonl(albums_path, Album)
    for batch in chunked_iterable(album_stream, batch_size):
        batch_data = [msgspec.to_builtins(item) for item in batch]
        execute_cypher(memgraph, album_query, {"batch": batch_data})
        album_count += len(batch)
    context.log.info(f"Loaded {album_count} albums.")

    # 4. Tracks
    tracks_path = settings.tracks_filepath
    track_count = 0
    track_query = """
    UNWIND $batch AS row
    CREATE (:Track {
        id: row.id, 
        title: row.title,
        album_id: row.album_id,
        genres: row.genres
    });
    """
    track_stream = read_items_from_jsonl(tracks_path, Track)
    for batch in chunked_iterable(track_stream, batch_size):
        batch_data = [msgspec.to_builtins(item) for item in batch]
        execute_cypher(memgraph, track_query, {"batch": batch_data})
        track_count += len(batch)
    context.log.info(f"Loaded {track_count} tracks.")

    # --- Step 3: Index Creation ---
    create_indexes(memgraph, context)

    # --- Step 4: Relationship Ingestion ---
    context.log.info("Starting Stage 3: Relationship Ingestion")

    # 1. Artist -> Genre
    context.log.info("Creating (Artist)-[:HAS_GENRE]->(Genre)...")
    execute_cypher(memgraph, """
    MATCH (a:Artist) WHERE a.genres IS NOT NULL
    UNWIND a.genres AS gid
    MATCH (g:Genre {id: gid})
    MERGE (a)-[:HAS_GENRE]->(g);
    """)

    # 2. Artist -> Artist (SIMILAR_TO) with Self-Loop Prevention
    context.log.info("Creating (Artist)-[:SIMILAR_TO]->(Artist) with validation...")
    execute_cypher(memgraph, """
    MATCH (a:Artist) WHERE a.similar_artists IS NOT NULL
    UNWIND a.similar_artists AS sim_name
    MATCH (target:Artist {name: sim_name})
    WHERE a.id <> target.id AND NOT target.name IN a.aliases
    MERGE (a)-[:SIMILAR_TO]->(target);
    """)

    # 3. Album -> Artist
    context.log.info("Creating (Album)-[:PERFORMED_BY]->(Artist)...")
    execute_cypher(memgraph, """
    MATCH (alb:Album) WHERE alb.artist_id IS NOT NULL
    MATCH (art:Artist {id: alb.artist_id})
    MERGE (alb)-[:PERFORMED_BY]->(art);
    """)

    # 4. Album -> Track (CONTAINS_TRACK)
    context.log.info("Creating (Album)-[:CONTAINS_TRACK]->(Track)...")
    execute_cypher(memgraph, """
    MATCH (t:Track) WHERE t.album_id IS NOT NULL
    MATCH (alb:Album {id: t.album_id})
    MERGE (alb)-[:CONTAINS_TRACK]->(t);
    """)

    # 5. Genre -> Genre (SUBGENRE_OF) with Self-Loop Prevention
    context.log.info("Creating (Genre)-[:SUBGENRE_OF]->(Genre)...")
    execute_cypher(memgraph, """
    MATCH (g:Genre) WHERE g.parent_ids IS NOT NULL
    UNWIND g.parent_ids AS pid
    MATCH (parent:Genre {id: pid})
    WHERE g.id <> parent.id
    MERGE (g)-[:SUBGENRE_OF]->(parent);
    """)

    # 6. Album -> Genre
    context.log.info("Creating (Album)-[:HAS_GENRE]->(Genre)...")
    execute_cypher(memgraph, """
    MATCH (alb:Album) WHERE alb.genres IS NOT NULL
    UNWIND alb.genres AS gid
    MATCH (g:Genre {id: gid})
    MERGE (alb)-[:HAS_GENRE]->(g);
    """)

    # 7. Track -> Genre
    context.log.info("Creating (Track)-[:HAS_GENRE]->(Genre)...")
    execute_cypher(memgraph, """
    MATCH (t:Track) WHERE t.genres IS NOT NULL
    UNWIND t.genres AS gid
    MATCH (g:Genre {id: gid})
    MERGE (t)-[:HAS_GENRE]->(g);
    """)

    # --- Step 5: Cleanup ---
    context.log.info("Starting Stage 4: Cleanup")
    cleanup_queries = [
        "MATCH (n:Artist) REMOVE n.genres, n.similar_artists;",
        "MATCH (n:Album) REMOVE n.artist_id, n.genres;",
        "MATCH (n:Track) REMOVE n.album_id, n.genres;",
        "MATCH (n:Genre) REMOVE n.parent_ids;"
    ]
    for q in cleanup_queries:
        execute_cypher(memgraph, q)

    context.log.info("Graph population complete.")
    return MaterializeResult(
        metadata={
            "nodes_loaded": {
                "genres": genre_count,
                "artists": artist_count,
                "albums": album_count,
                "tracks": track_count
            },
            "status": "success"
        }
    )
