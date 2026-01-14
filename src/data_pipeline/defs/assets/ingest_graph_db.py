# -----------------------------------------------------------
# Ingest Graph DB Asset
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import cast, LiteralString
import polars as pl
from dagster import AssetExecutionContext, MaterializeResult, asset
from neo4j import Driver

from data_pipeline.settings import settings
from data_pipeline.utils.neo4j_helpers import (
    clear_database,
    execute_cypher,
    execute_cypher_with_progress,
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
        "CREATE INDEX release_id_idx IF NOT EXISTS FOR (n:Release) ON (n.id)",
        "CREATE INDEX release_artist_id_idx IF NOT EXISTS FOR (n:Release) ON (n.artist_id)",
        "CREATE INDEX genre_id_idx IF NOT EXISTS FOR (n:Genre) ON (n.id)",
        "CREATE INDEX genre_name_idx IF NOT EXISTS FOR (n:Genre) ON (n.name)",
    ]

    for cmd in index_commands:
        try:
            execute_cypher(driver, cmd, transactional=False)
            context.log.info(f"Executed: {cmd}")
        except Exception as e:
            context.log.error(f"Failed to execute '{cmd}': {e}")
            raise e


@asset(
    name="graph_db",
    description="Ingests in Neo4j Artists, Releases, and Genres.",
)
def ingest_graph_db(
    context: AssetExecutionContext, 
    neo4j: Neo4jResource,
    artists: pl.LazyFrame,
    releases: pl.LazyFrame,
    genres: pl.LazyFrame
) -> MaterializeResult:
    """
    Dagster asset that ingests music data into the Neo4j database.
    """
    # Materialize LazyFrames to DataFrames for iteration
    # We must collect because we need to iterate in batches for Neo4j
    artists_df = artists.collect()
    releases_df = releases.collect()
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
            mbid: row.mbid,
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
        CREATE (:Release {
            id: row.id, 
            title: row.title, 
            year: row.year,
            artist_id: row.artist_id
        });
        """
        if not releases_df.is_empty():
            for batch_df in releases_df.iter_slices(n_rows=batch_size):
                batch_data = batch_df.to_dicts()
                execute_cypher(driver, release_query, {"batch": batch_data})
                release_count += len(batch_data)
        context.log.info(f"Loaded {release_count} releases.")

        # --- Step 3: Index Creation ---
        _create_indexes(driver, context)

        # --- Step 4: Relationship Ingestion ---
        context.log.info("Starting Stage 3: Relationship Ingestion")

        # 1. Artist -> Genre
        # noinspection SqlNoDataSourceInspection
        execute_cypher_with_progress(
            driver,
            """
            MATCH (a:Artist) WHERE a.genres IS NOT NULL
            CALL (a) {
                UNWIND a.genres AS gid
                MATCH (g:Genre {id: gid})
                MERGE (a)-[:HAS_GENRE]->(g)
            } IN TRANSACTIONS OF 1000 ROWS;
            """,
            total_count=len(artists_df),
            batch_size=1000,
            description="Ingesting Artist -> Genre"
        )

        # 2. Artist -> Artist (SIMILAR_TO)
        # Matches against name OR aliases
        # noinspection SqlNoDataSourceInspection
        execute_cypher_with_progress(
            driver,
            """
            MATCH (a:Artist) WHERE a.similar_artists IS NOT NULL
            CALL (a) {
                UNWIND a.similar_artists AS sim_name
                MATCH (target:Artist)
                WHERE (target.name = sim_name OR sim_name IN target.aliases)
                  AND a.id <> target.id
                MERGE (a)-[:SIMILAR_TO]->(target)
            } IN TRANSACTIONS OF 1000 ROWS;
            """,
            total_count=len(artists_df),
            batch_size=1000,
            description="Ingesting Artist -> Artist"
        )

        # 3. Release -> Artist
        # noinspection SqlNoDataSourceInspection
        execute_cypher_with_progress(
            driver,
            """
            MATCH (rel:Release) WHERE rel.artist_id IS NOT NULL
            CALL (rel) {
                MATCH (art:Artist {id: rel.artist_id})
                MERGE (rel)-[:PERFORMED_BY]->(art)
            } IN TRANSACTIONS OF 1000 ROWS;
            """,
            total_count=len(releases_df),
            batch_size=1000,
            description="Ingesting Release -> Artist"
        )

        # 4. Genre -> Genre
        # noinspection SqlNoDataSourceInspection
        execute_cypher_with_progress(
            driver,
            """
            MATCH (g:Genre) WHERE g.parent_ids IS NOT NULL
            CALL (g) {
                UNWIND g.parent_ids AS pid
                MATCH (parent:Genre {id: pid})
                WHERE g.id <> parent.id
                MERGE (g)-[:SUBGENRE_OF]->(parent)
            } IN TRANSACTIONS OF 1000 ROWS;
            """,
            total_count=len(genres_df),
            batch_size=1000,
            description="Ingesting Genre -> Genre"
        )

        # --- Step 5: Cleanup ---
        cleanup_queries = [
            "MATCH (n:Artist) REMOVE n.genres, n.similar_artists;",
            "MATCH (n:Release) REMOVE n.artist_id;",
            "MATCH (n:Genre) REMOVE n.parent_ids;"
        ]
        for q in cleanup_queries:
            execute_cypher(driver, q)

        # --- Step 6: Validation ---
        _validate_graph_counts(driver, context, {
            "Artist": len(artists_df),
            "Release": len(releases_df),
            "Genre": len(genres_df)
        })

    context.log.info("Graph population complete.")
    return MaterializeResult(
        metadata={
            "total_records_input": {
                "genres": len(genres_df),
                "artists": len(artists_df),
                "releases": len(releases_df)
            },
            "nodes_ingested": {
                "genres": genre_count,
                "artists": artist_count,
                "releases": release_count
            },
            "status": "success"
        }
    )


def _validate_graph_counts(driver: Driver, context: AssetExecutionContext, expected_counts: dict[str, int]) -> None:
    """
    Validates that the number of nodes in the graph matches the expected counts.
    """
    context.log.info("Validating graph node counts...")
    
    for label, expected in expected_counts.items():
        # noinspection SqlNoDataSourceInspection
        query = f"MATCH (n:{label}) RETURN count(n) AS count"
        try:
            with driver.session() as session:
                result = session.run(cast(LiteralString, query)).single()
                actual = result["count"] if result else 0
                
            if actual != expected:
                context.log.warning(
                    f"Count mismatch for {label}: Expected {expected}, found {actual}. "
                    "This might be due to duplicates in source or merge logic."
                )
            else:
                context.log.info(f"Validation passed for {label}: {actual} nodes found.")
        except Exception as e:
            context.log.error(f"Validation failed for {label}: {e}")


"""
MATCH (target:Artist {name: 'Depeche Mode'})

// 1. Get Depeche Mode's own genres (This is the new part)
MATCH (target)-[r3:HAS_GENRE]->(targetGenre:Genre)

// 2. Get Similar Artists and their connections
MATCH (target)-[r1:SIMILAR_TO]-(similar:Artist)
MATCH (similar)-[r2:HAS_GENRE]->(simGenre:Genre)

// 3. Return everything, including the new relationships (r3)
RETURN target, targetGenre, similar, simGenre, r1, r2, r3
"""
