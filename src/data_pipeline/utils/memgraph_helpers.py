# -----------------------------------------------------------
# Memgraph DB Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from dagster import Config, AssetExecutionContext
from pydantic import Field
from gqlalchemy import Memgraph

from data_pipeline.settings import settings


class MemgraphConfig(Config):
    """Configuration for Memgraph connection."""
    host: str = Field(settings.MEMGRAPH_HOST, description="Memgraph host address.")
    port: int = Field(settings.MEMGRAPH_PORT, description="Memgraph port number.")


def get_memgraph_client(config: MemgraphConfig) -> Memgraph:
    """
    Initializes and returns a Memgraph client based on the provided configuration.

    Args:
        config: The Memgraph configuration object containing host and port.

    Returns:
        A Memgraph client instance connected to the specified host and port.
    """
    return Memgraph(host=config.host, port=config.port)


def clear_database(memgraph: Memgraph, context: AssetExecutionContext) -> None:
    """
    Clears all nodes, relationships, and indexes from the database.
    
    This function performs a complete cleanup of the Memgraph database instance,
    removing all data and dropping all indexes. It is typically used as a 
    preparatory step before a fresh data ingestion.

    Args:
        memgraph: The Memgraph client instance to execute queries.
        context: The Dagster asset execution context for logging.
    """
    context.log.info("Starting database cleanup...")

    try:
        # 1. Delete all nodes and relationships
        memgraph.execute("MATCH (n) DETACH DELETE n;")
        context.log.info("Deleted all nodes and relationships.")

        # 2. Drop all indexes
        # Fetch existing indexes
        indexes = list(memgraph.execute_and_fetch("SHOW INDEX INFO;"))
        for idx in indexes:
            label = idx.get("label")
            property_name = idx.get("property")
            if label and property_name:
                query = f"DROP INDEX ON :{label}({property_name});"
                try:
                    memgraph.execute(query)
                    context.log.info(f"Dropped index: :{label}({property_name})")
                except Exception as e:
                    context.log.warning(
                        f"Failed to drop index :{label}({property_name}). Error: {e}"
                    )
    except Exception as e:
        context.log.error(f"Error during database cleanup: {e}")
        raise e


def create_indexes(memgraph: Memgraph, context: AssetExecutionContext) -> None:
    """
    Creates necessary indexes in Memgraph to optimize query performance.

    Args:
        memgraph: The Memgraph client instance.
        context: The Dagster asset execution context for logging.
    """
    context.log.info("Creating indexes...")
    index_commands = [
        "CREATE INDEX ON :Artist(id);",
        "CREATE INDEX ON :Artist(name);",
        "CREATE INDEX ON :Album(id);",
        "CREATE INDEX ON :Album(artist_id);",
        "CREATE INDEX ON :Track(id);",
        "CREATE INDEX ON :Track(album_id);",
        "CREATE INDEX ON :Genre(id);",
        "CREATE INDEX ON :Genre(name);",
    ]

    for cmd in index_commands:
        try:
            memgraph.execute(cmd)
            context.log.info(f"Executed: {cmd}")
        except Exception as e:
            context.log.error(f"Failed to execute '{cmd}': {e}")
            raise e
