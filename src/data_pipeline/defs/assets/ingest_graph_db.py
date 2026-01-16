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
        "CREATE INDEX genre_id_idx IF NOT EXISTS FOR (n:Genre) ON (n.id)",
        "CREATE INDEX genre_name_idx IF NOT EXISTS FOR (n:Genre) ON (n.name)",
        "CREATE INDEX country_id_idx IF NOT EXISTS FOR (n:Country) ON (n.id)",
        "CREATE INDEX country_name_idx IF NOT EXISTS FOR (n:Country) ON (n.name)",
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
    genres: pl.LazyFrame,
    countries: pl.LazyFrame
) -> MaterializeResult:
    """
    Dagster asset that ingests music data into the Neo4j database.
    """
    # Materialize LazyFrames to DataFrames for iteration
    # We must collect because we need to iterate in batches for Neo4j
    artists_df = artists.collect()
    releases_df = releases.collect()
    genres_df = genres.collect()
    countries_df = countries.collect()

    batch_size = settings.GRAPH_DB_INGESTION_BATCH_SIZE

    with neo4j.get_driver(context) as driver:
        # --- Step 1: Clear Database & Prepare ---
        clear_database(driver, context)

        # --- Step 2: Node Ingestion ---
        context.log.info("Starting Stage 1: Node Ingestion")

        # 0. Countries
        country_count = 0
        # noinspection SqlNoDataSourceInspection
        country_query = """
        UNWIND $batch AS row
        CREATE (:Country {
            id: row.id, 
            name: row.name
        });
        """
        if not countries_df.is_empty():
            for batch_df in countries_df.iter_slices(n_rows=batch_size):
                batch_data = batch_df.to_dicts()
                execute_cypher(driver, country_query, {"batch": batch_data})
                country_count += len(batch_data)
        context.log.info(f"Loaded {country_count} countries.")

        # 1. Genres
        genre_count = 0
        # noinspection SqlNoDataSourceInspection
        genre_query = """
        UNWIND $batch AS row
        CREATE (:Genre {
            id: row.id, 
            name: row.name, 
            aliases: row.aliases
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
            aliases: row.aliases,
            tags: row.tags
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
            year: row.year
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
        ag_df = artists_df.filter(pl.col("genres").is_not_null())
        if not ag_df.is_empty():
            ag_query = """
            UNWIND $batch AS row
            MATCH (a:Artist {id: row.id})
            UNWIND row.genres AS gid
            MATCH (g:Genre {id: gid})
            MERGE (a)-[:PLAYS_GENRE]->(g)
            """
            # Using smaller batch for relationships to be safe, though execute_cypher is auto-commit
            # But here we use explicit batches in python loop.
            for batch_df in ag_df.iter_slices(n_rows=1000):
                batch_data = batch_df.select(["id", "genres"]).to_dicts()
                execute_cypher(driver, ag_query, {"batch": batch_data})
        context.log.info("Ingested Artist -> Genre relationships.")

        # 2. Artist -> Artist (SIMILAR_TO)
        # Matches against name OR aliases
        aa_df = artists_df.filter(pl.col("similar_artists").is_not_null())
        if not aa_df.is_empty():
            aa_query = """
            UNWIND $batch AS row
            MATCH (a:Artist {id: row.id})
            UNWIND row.similar_artists AS sim_name
            MATCH (target:Artist)
            WHERE (target.name = sim_name OR sim_name IN target.aliases)
              AND a.id <> target.id
            MERGE (a)-[:SIMILAR_TO]->(target)
            """
            for batch_df in aa_df.iter_slices(n_rows=1000):
                batch_data = batch_df.select(["id", "similar_artists"]).to_dicts()
                execute_cypher(driver, aa_query, {"batch": batch_data})
        context.log.info("Ingested Artist -> Artist relationships.")

        # 3. Release -> Artist
        ra_df = releases_df.filter(pl.col("artist_id").is_not_null())
        if not ra_df.is_empty():
            ra_query = """
            UNWIND $batch AS row
            MATCH (rel:Release {id: row.id})
            MATCH (art:Artist {id: row.artist_id})
            MERGE (rel)-[:PERFORMED_BY]->(art)
            """
            for batch_df in ra_df.iter_slices(n_rows=1000):
                batch_data = batch_df.select(["id", "artist_id"]).to_dicts()
                execute_cypher(driver, ra_query, {"batch": batch_data})
        context.log.info("Ingested Release -> Artist relationships.")

        # 4. Genre -> Genre
        gg_df = genres_df.filter(pl.col("parent_ids").is_not_null())
        if not gg_df.is_empty():
            gg_query = """
            UNWIND $batch AS row
            MATCH (g:Genre {id: row.id})
            UNWIND row.parent_ids AS pid
            MATCH (parent:Genre {id: pid})
            WHERE g.id <> parent.id
            MERGE (g)-[:SUBGENRE_OF]->(parent)
            """
            for batch_df in gg_df.iter_slices(n_rows=1000):
                batch_data = batch_df.select(["id", "parent_ids"]).to_dicts()
                execute_cypher(driver, gg_query, {"batch": batch_data})
        context.log.info("Ingested Genre -> Genre relationships.")

        # 5. Genre -> Genre (Aliases)
        ga_df = genres_df.filter(pl.col("aliases").is_not_null())
        if not ga_df.is_empty():
            ga_query = """
            UNWIND $batch AS row
            MATCH (source:Genre {id: row.id})
            UNWIND row.aliases AS alias
            MATCH (target:Genre {name: alias})
            WHERE source.id <> target.id
            MERGE (source)-[:RELATED_TO]->(target)
            """
            for batch_df in ga_df.iter_slices(n_rows=1000):
                batch_data = batch_df.select(["id", "aliases"]).to_dicts()
                execute_cypher(driver, ga_query, {"batch": batch_data})
        context.log.info("Ingested Genre -> Genre (Aliases) relationships.")

        # 6. Artist -> Country
        ac_df = artists_df.filter(pl.col("country").is_not_null())
        if not ac_df.is_empty():
            ac_query = """
            UNWIND $batch AS row
            MATCH (a:Artist {id: row.id})
            MATCH (c:Country {name: row.country})
            MERGE (a)-[:FROM_COUNTRY]->(c)
            """
            for batch_df in ac_df.iter_slices(n_rows=1000):
                batch_data = batch_df.select(["id", "country"]).to_dicts()
                execute_cypher(driver, ac_query, {"batch": batch_data})
        context.log.info("Ingested Artist -> Country relationships.")

        # --- Step 5: Validation ---
        _validate_graph_counts(driver, context, {
            "Artist": len(artists_df),
            "Release": len(releases_df),
            "Genre": len(genres_df),
            "Country": len(countries_df)
        })

    context.log.info("Graph population complete.")
    return MaterializeResult(
        metadata={
            "total_records_input": {
                "genres": len(genres_df),
                "artists": len(artists_df),
                "releases": len(releases_df),
                "countries": len(countries_df)
            },
            "nodes_ingested": {
                "genres": genre_count,
                "artists": artist_count,
                "releases": release_count,
                "countries": country_count
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
MATCH (target)-[r3:PLAYS_GENRE]->(targetGenre:Genre)

// 2. Get Similar Artists and their connections
MATCH (target)-[r1:SIMILAR_TO]-(similar:Artist)
MATCH (similar)-[r2:PLAYS_GENRE]->(simGenre:Genre)

// 3. Return everything, including the new relationships (r3)
RETURN target, targetGenre, similar, simGenre, r1, r2, r3
"""
