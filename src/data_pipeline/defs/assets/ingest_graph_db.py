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
from neo4j import Driver

from data_pipeline.settings import settings
from data_pipeline.utils.neo4j_helpers import (
    clear_database,
    execute_cypher,
)
from data_pipeline.defs.resources import Neo4jResource


def _create_indexes(driver: Driver, context: AssetExecutionContext) -> None:
    """
    Creates necessary indexes in Neo4j to optimize query performance.
    """
    context.log.info("Creating indexes...")
    
    # noinspection SqlNoDataSourceInspection
    index_commands = [
        "CREATE INDEX artist_id_idx IF NOT EXISTS FOR (n:Artist) ON (n.id)",
        "CREATE INDEX artist_name_idx IF NOT EXISTS FOR (n:Artist) ON (n.name)",
        "CREATE INDEX album_id_idx IF NOT EXISTS FOR (n:Album) ON (n.id)",
        "CREATE INDEX album_artist_id_idx IF NOT EXISTS FOR (n:Album) ON (n.artist_id)",
        "CREATE INDEX track_id_idx IF NOT EXISTS FOR (n:Track) ON (n.id)",
        "CREATE INDEX track_album_id_idx IF NOT EXISTS FOR (n:Track) ON (n.album_id)",
        "CREATE INDEX genre_id_idx IF NOT EXISTS FOR (n:Genre) ON (n.id)",
        "CREATE INDEX genre_name_idx IF NOT EXISTS FOR (n:Genre) ON (n.name)",
    ]

    for cmd in index_commands:
        try:
            execute_cypher(driver, cmd)
            context.log.info(f"Executed: {cmd}")
        except Exception as e:
            context.log.error(f"Failed to execute '{cmd}': {e}")
            raise e


@asset(
    name="ingest_graph_db",
    description="Ingests in Neo4j Artists, Releases, Tracks, and Genres.",
)
def ingest_graph_db(
    context: AssetExecutionContext, 
    neo4j: Neo4jResource,
    artists: pl.LazyFrame,
    releases: pl.LazyFrame,
    tracks: pl.LazyFrame,
    genres: pl.LazyFrame
) -> MaterializeResult:
    """
    Dagster asset that ingests music data into the Neo4j database.
    """
    # Materialize LazyFrames to DataFrames for iteration
    # We must collect because we need to iterate in batches for Neo4j
    artists_df = artists.collect()
    releases_df = releases.collect()
    tracks_df = tracks.collect()
    genres_df = genres.collect()

    batch_size = settings.GRAPH_DB_INGESTION_BATCH_SIZE

    with neo4j.get_driver(context) as driver:
        # --- Step 1: Clear Database & Prepare ---
        clear_database(driver, context)

        # --- Step 2: Node Ingestion ---
        context.log.info("Starting Stage 1: Node Ingestion")

        # 1. Genres
        genre_count = 0
        # noinspection SqlNoDataSourceInspection
        genre_query = """
        UNWIND $batch AS row
        CREATE (:Genre {
            id: row.id, 
            name: row.name, 
            aliases: row.aliases,
            parent_ids: row.parent_ids
        });
        """
        if not genres_df.is_empty():
            for batch_df in genres_df.iter_slices(n_rows=batch_size):
                batch_data = batch_df.to_dicts()
                execute_cypher(driver, genre_query, {"batch": batch_data})
                genre_count += len(batch_data)
        context.log.info(f"Loaded {genre_count} genres.")

        # 2. Artists
        artist_count = 0
        # noinspection SqlNoDataSourceInspection
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
        if not artists_df.is_empty():
            for batch_df in artists_df.iter_slices(n_rows=batch_size):
                batch_data = batch_df.to_dicts()
                execute_cypher(driver, artist_query, {"batch": batch_data})
                artist_count += len(batch_data)
        context.log.info(f"Loaded {artist_count} artists.")

        # 3. Releases
        release_count = 0
        # noinspection SqlNoDataSourceInspection
        release_query = """
        UNWIND $batch AS row
        CREATE (:Album {
            id: row.id, 
            title: row.title, 
            year: row.year,
            artist_id: row.artist_id,
            genres: row.genres
        });
        """
        if not releases_df.is_empty():
            for batch_df in releases_df.iter_slices(n_rows=batch_size):
                batch_data = batch_df.to_dicts()
                execute_cypher(driver, release_query, {"batch": batch_data})
                release_count += len(batch_data)
        context.log.info(f"Loaded {release_count} releases.")

        # 4. Tracks
        track_count = 0
        # noinspection SqlNoDataSourceInspection
        track_query = """
        UNWIND $batch AS row
        CREATE (:Track {
            id: row.id, 
            title: row.title,
            album_id: row.album_id,
            genres: row.genres
        });
        """
        if not tracks_df.is_empty():
            for batch_df in tracks_df.iter_slices(n_rows=batch_size):
                batch_data = batch_df.to_dicts()
                execute_cypher(driver, track_query, {"batch": batch_data})
                track_count += len(batch_data)
        context.log.info(f"Loaded {track_count} tracks.")

        # --- Step 3: Index Creation ---
        _create_indexes(driver, context)

        # --- Step 4: Relationship Ingestion ---
        context.log.info("Starting Stage 3: Relationship Ingestion")

        # 1. Artist -> Genre
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, """
        MATCH (a:Artist) WHERE a.genres IS NOT NULL
        UNWIND a.genres AS gid
        MATCH (g:Genre {id: gid})
        MERGE (a)-[:HAS_GENRE]->(g);
        """)

        # 2. Artist -> Artist (SIMILAR_TO)
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, """
        MATCH (a:Artist) WHERE a.similar_artists IS NOT NULL
        UNWIND a.similar_artists AS sim_name
        MATCH (target:Artist {name: sim_name})
        WHERE a.id <> target.id AND NOT target.name IN a.aliases
        MERGE (a)-[:SIMILAR_TO]->(target);
        """)

        # 3. Album -> Artist
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, """
        MATCH (alb:Album) WHERE alb.artist_id IS NOT NULL
        MATCH (art:Artist {id: alb.artist_id})
        MERGE (alb)-[:PERFORMED_BY]->(art);
        """)

        # 4. Album -> Track
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, """
        MATCH (t:Track) WHERE t.album_id IS NOT NULL
        MATCH (alb:Album {id: t.album_id})
        MERGE (alb)-[:CONTAINS_TRACK]->(t);
        """)

        # 5. Genre -> Genre
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, """
        MATCH (g:Genre) WHERE g.parent_ids IS NOT NULL
        UNWIND g.parent_ids AS pid
        MATCH (parent:Genre {id: pid})
        WHERE g.id <> parent.id
        MERGE (g)-[:SUBGENRE_OF]->(parent);
        """)

        # 6. Album -> Genre
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, """
        MATCH (alb:Album) WHERE alb.genres IS NOT NULL
        UNWIND alb.genres AS gid
        MATCH (g:Genre {id: gid})
        MERGE (alb)-[:HAS_GENRE]->(g);
        """)

        # 7. Track -> Genre
        # noinspection SqlNoDataSourceInspection
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
                "genres": len(genres_df),
                "artists": len(artists_df),
                "releases": len(releases_df),
                "tracks": len(tracks_df)
            },
            "nodes_ingested": {
                "genres": genre_count,
                "artists": artist_count,
                "releases": release_count,
                "tracks": track_count
            },
            "status": "success"
        }
    )
