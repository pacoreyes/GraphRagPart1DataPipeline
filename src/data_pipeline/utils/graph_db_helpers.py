# -----------------------------------------------------------
# Graph DB Helpers (Neo4j)
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Any, Optional, LiteralString, cast

from dagster import Config, AssetExecutionContext, EnvVar
from neo4j import GraphDatabase, Driver
from pydantic import Field


class Neo4jConfig(Config):
    """Configuration for Neo4j connection."""
    uri: str = Field(
        default=EnvVar("NEO4J_URI"), 
        description="Neo4j URI (e.g., neo4j+s://...)."
    )
    username: str = Field(
        default=EnvVar("NEO4J_USERNAME"), 
        description="Neo4j username."
    )
    password: str = Field(
        default=EnvVar("NEO4J_PASSWORD"), 
        description="Neo4j password."
    )


def get_neo4j_driver(config: Optional[Neo4jConfig] = None) -> Driver:
    """
    Initializes and returns a Neo4j driver based on the configuration.

    Args:
        config: The Neo4j configuration object. If None, uses defaults from EnvVars.

    Returns:
        A Neo4j Driver instance.
    """
    if config:
        uri = config.uri
        username = config.username
        password = config.password
    else:
        # Fallback to manual EnvVar resolution if no config provided (for scripts)
        # Dagster EnvVar objects resolve to their value when cast to string or accessed,
        # but here we should handle them or let the driver handle the string values.
        # Actually, Neo4jConfig already has EnvVar defaults. 
        # If we instantiate it without args, it resolves them.
        resolved_config = Neo4jConfig()
        uri = resolved_config.uri
        username = resolved_config.username
        password = resolved_config.password
    
    return GraphDatabase.driver(uri, auth=(username, password))


def execute_cypher(driver: Driver, query: str, params: dict[str, Any] | None = None, database: str = "neo4j") -> None:
    """
    Executes a Cypher query with optional parameters using the Neo4j driver.
    """
    try:
        with driver.session(database=database) as session:
            # We cast to LiteralString because schema-altering queries cannot be parameterized,
            # and the driver uses LiteralString as a safety hint.
            session.run(cast(LiteralString, query), params or {}).consume()
    except Exception as e:
        raise e


def clear_database(driver: Driver, context: AssetExecutionContext) -> None:
    """
    Clears all nodes, relationships, and indexes from the database.
    """
    context.log.info("Starting database cleanup...")

    try:
        # 1. Delete all nodes and relationships
        # Using simple DETACH DELETE for smaller datasets. 
        # For huge datasets, batched deletion (CALL { ... } IN TRANSACTIONS) is preferred.
        # noinspection SqlNoDataSourceInspection
        execute_cypher(driver, "MATCH (n) DETACH DELETE n;")
        context.log.info("Deleted all nodes and relationships.")

        # 2. Drop all indexes
        with driver.session() as session:
            # noinspection SqlNoDataSourceInspection
            indexes = session.run("SHOW INDEXES").data()
            for idx in indexes:
                # Filter system indexes or those we shouldn't touch if necessary
                name = idx.get("name")
                type_ = idx.get("type", "").lower()
                
                # We only drop explicitly created indexes (RANGE, POINT, TEXT)
                if name and type_ in ["range", "point", "text", "btree"]: 
                    try:
                        # noinspection SqlNoDataSourceInspection
                        session.run(cast(LiteralString, f"DROP INDEX {name}"))
                        context.log.info(f"Dropped index: {name}")
                    except Exception as e:
                        context.log.warning(f"Failed to drop index {name}: {e}")

            # Drop constraints as well if they exist
            # noinspection SqlNoDataSourceInspection
            constraints = session.run("SHOW CONSTRAINTS").data()
            for const in constraints:
                name = const.get("name")
                if name:
                    try:
                        # noinspection SqlNoDataSourceInspection
                        session.run(cast(LiteralString, f"DROP CONSTRAINT {name}"))
                        context.log.info(f"Dropped constraint: {name}")
                    except Exception as e:
                        context.log.warning(f"Failed to drop constraint {name}: {e}")

    except Exception as e:
        context.log.error(f"Error during database cleanup: {e}")
        raise e


def create_indexes(driver: Driver, context: AssetExecutionContext) -> None:
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
