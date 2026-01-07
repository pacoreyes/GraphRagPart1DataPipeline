# -----------------------------------------------------------
# Ingest Graph DB Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import polars as pl
from dagster import AssetExecutionContext, MaterializeResult, asset

from data_pipeline.settings import settings
from data_pipeline.utils.graph_db_helpers import (
    clear_database,
    create_indexes,
    execute_cypher,
)
from data_pipeline.defs.resources import Neo4jResource


@asset(
    name="ingest_graph_db",
    description="Ingests in Neo4j Artists, Albums, Tracks, and Genres.",
)
def ingest_graph_db(
    context: AssetExecutionContext, 
    neo4j: Neo4jResource,
    artists: pl.DataFrame,
    albums: pl.DataFrame,
    tracks: pl.DataFrame,
    genres: pl.DataFrame
) -> MaterializeResult:
    """
    Dagster asset that ingests music data into the Neo4j database.
    """
    driver = neo4j
    batch_size = settings.GRAPH_DB_INGESTION_BATCH_SIZE

    # --- Step 1: Clear Database & Prepare ---
    clear_database(driver, context)

    # --- Step 2: Node Ingestion ---
    context.log.info("Starting Stage 1: Node Ingestion")

    # 1. Genres
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
    if not genres.is_empty():
        for batch_df in genres.iter_slices(n_rows=batch_size):
            batch_data = batch_df.to_dicts()
            execute_cypher(driver, genre_query, {"batch": batch_data})
            genre_count += len(batch_data)
    context.log.info(f"Loaded {genre_count} genres.")

    # 2. Artists
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
    if not artists.is_empty():
        for batch_df in artists.iter_slices(n_rows=batch_size):
            batch_data = batch_df.to_dicts()
            execute_cypher(driver, artist_query, {"batch": batch_data})
            artist_count += len(batch_data)
    context.log.info(f"Loaded {artist_count} artists.")

    # 3. Albums
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
    if not albums.is_empty():
        for batch_df in albums.iter_slices(n_rows=batch_size):
            batch_data = batch_df.to_dicts()
            execute_cypher(driver, album_query, {"batch": batch_data})
            album_count += len(batch_data)
    context.log.info(f"Loaded {album_count} albums.")

    # 4. Tracks
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
    if not tracks.is_empty():
        for batch_df in tracks.iter_slices(n_rows=batch_size):
            batch_data = batch_df.to_dicts()
            execute_cypher(driver, track_query, {"batch": batch_data})
            track_count += len(batch_data)
    context.log.info(f"Loaded {track_count} tracks.")

    # --- Step 3: Index Creation ---
    create_indexes(driver, context)

    # --- Step 4: Relationship Ingestion ---
    context.log.info("Starting Stage 3: Relationship Ingestion")

    # 1. Artist -> Genre
    execute_cypher(driver, """
    MATCH (a:Artist) WHERE a.genres IS NOT NULL
    UNWIND a.genres AS gid
    MATCH (g:Genre {id: gid})
    MERGE (a)-[:HAS_GENRE]->(g);
    """)

    # 2. Artist -> Artist (SIMILAR_TO)
    execute_cypher(driver, """
    MATCH (a:Artist) WHERE a.similar_artists IS NOT NULL
    UNWIND a.similar_artists AS sim_name
    MATCH (target:Artist {name: sim_name})
    WHERE a.id <> target.id AND NOT target.name IN a.aliases
    MERGE (a)-[:SIMILAR_TO]->(target);
    """)

    # 3. Album -> Artist
    execute_cypher(driver, """
    MATCH (alb:Album) WHERE alb.artist_id IS NOT NULL
    MATCH (art:Artist {id: alb.artist_id})
    MERGE (alb)-[:PERFORMED_BY]->(art);
    """)

    # 4. Album -> Track
    execute_cypher(driver, """
    MATCH (t:Track) WHERE t.album_id IS NOT NULL
    MATCH (alb:Album {id: t.album_id})
    MERGE (alb)-[:CONTAINS_TRACK]->(t);
    """)

    # 5. Genre -> Genre
    execute_cypher(driver, """
    MATCH (g:Genre) WHERE g.parent_ids IS NOT NULL
    UNWIND g.parent_ids AS pid
    MATCH (parent:Genre {id: pid})
    WHERE g.id <> parent.id
    MERGE (g)-[:SUBGENRE_OF]->(parent);
    """)

    # 6. Album -> Genre
    execute_cypher(driver, """
    MATCH (alb:Album) WHERE alb.genres IS NOT NULL
    UNWIND alb.genres AS gid
    MATCH (g:Genre {id: gid})
    MERGE (alb)-[:HAS_GENRE]->(g);
    """)

    # 7. Track -> Genre
    execute_cypher(driver, """
    MATCH (t:Track) WHERE t.genres IS NOT NULL
    UNWIND t.genres AS gid
    MATCH (g:Genre {id: gid})
    MERGE (t)-[:HAS_GENRE]->(g);
    """)

    # --- Step 5: Cleanup ---
    cleanup_queries = [
        "MATCH (n:Artist) REMOVE n.genres, n.similar_artists;",
        "MATCH (n:Album) REMOVE n.artist_id, n.genres;",
        "MATCH (n:Track) REMOVE n.album_id, n.genres;",
        "MATCH (n:Genre) REMOVE n.parent_ids;"
    ]
    for q in cleanup_queries:
        execute_cypher(driver, q)

    context.log.info("Graph population complete.")
    return MaterializeResult(
        metadata={
            "total_records_input": {
                "genres": len(genres),
                "artists": len(artists),
                "albums": len(albums),
                "tracks": len(tracks)
            },
            "nodes_ingested": {
                "genres": genre_count,
                "artists": artist_count,
                "albums": album_count,
                "tracks": track_count
            },
            "status": "success"
        }
    )