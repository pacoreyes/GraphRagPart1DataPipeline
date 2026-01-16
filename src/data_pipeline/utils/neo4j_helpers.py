# -----------------------------------------------------------
# Neo4j Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import time
from typing import Any, LiteralString, cast

from dagster import AssetExecutionContext
from neo4j import Driver
from neo4j.exceptions import ServiceUnavailable, SessionExpired


def execute_cypher(
    driver: Driver,
    query: str,
    params: dict[str, Any] | None = None,
    database: str = "neo4j",
    transactional: bool = True
) -> None:
    """
    Executes a Cypher query with optional parameters using the Neo4j driver.
    
    Args:
        driver: Neo4j Driver instance.
        query: Cypher query string.
        params: Dictionary of query parameters.
        database: Target database name.
        transactional: If True, uses execute_write (managed transaction with retries).
                       If False, uses session.run (auto-commit transaction), required for
                       schema operations (CREATE INDEX) or batched transactions (CALL ... IN TRANSACTIONS).
    """
    try:
        with driver.session(database=database) as session:
            if transactional:
                def _work(tx, q, p):
                    tx.run(q, p).consume()
                session.execute_write(_work, cast(LiteralString, query), params or {})
            else:
                # We cast to LiteralString because schema-altering queries cannot be parameterized
                session.run(cast(LiteralString, query), params or {}).consume()
    except Exception as e:
        raise e


def _execute_with_retry(
    driver: Driver,
    query: str,
    max_retries: int = 3,
    base_delay: float = 2.0,
) -> Any:
    """
    Executes a Cypher query with retry logic for transient connection failures.

    Args:
        driver: Neo4j Driver instance.
        query: Cypher query to execute.
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds for exponential backoff.

    Returns:
        The single result from the query, or None if no results.

    Raises:
        The last exception if all retries are exhausted.
    """
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            with driver.session() as session:
                # noinspection SqlNoDataSourceInspection
                result = session.run(cast(LiteralString, query)).single()
                return result
        except (ServiceUnavailable, SessionExpired) as e:
            last_exception = e
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                time.sleep(delay)
            else:
                raise last_exception
    
    if last_exception:
        raise last_exception
    raise ServiceUnavailable("Failed to execute query after retries with no captured exception.")


def clear_database(
    driver: Driver,
    context: AssetExecutionContext,
    batch_size: int = 500
) -> None:
    """
    Clears all nodes, relationships, and indexes from the database.

    Uses an iterative batch deletion approach with fresh sessions for each batch
    and retry logic, which is required for Neo4j Aura cloud instances that have
    connection timeouts.

    Args:
        driver: Neo4j Driver instance.
        context: Dagster execution context for logging.
        batch_size: Number of nodes to delete per batch (default 500 for Aura).
    """
    context.log.info("Starting database cleanup...")

    try:
        # 1. Delete relationships first (faster than DETACH DELETE on nodes)
        total_rels_deleted = 0
        while True:
            # noinspection SqlNoDataSourceInspection
            result = _execute_with_retry(
                driver,
                f"MATCH ()-[r]->() WITH r LIMIT {batch_size} DELETE r RETURN count(*) AS deleted"
            )
            deleted = result["deleted"] if result else 0
            if deleted == 0:
                break
            total_rels_deleted += deleted
            context.log.debug(f"Deleted {deleted} relationships (total: {total_rels_deleted})")

        if total_rels_deleted > 0:
            context.log.info(f"Deleted {total_rels_deleted} relationships.")

        # 2. Delete nodes (now without relationships, much faster)
        total_nodes_deleted = 0
        while True:
            # noinspection SqlNoDataSourceInspection
            result = _execute_with_retry(
                driver,
                f"MATCH (n) WITH n LIMIT {batch_size} DELETE n RETURN count(*) AS deleted"
            )
            deleted = result["deleted"] if result else 0
            if deleted == 0:
                break
            total_nodes_deleted += deleted
            context.log.debug(f"Deleted {deleted} nodes (total: {total_nodes_deleted})")

        context.log.info(f"Deleted {total_nodes_deleted} nodes.")

        # 3. Drop all indexes
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
