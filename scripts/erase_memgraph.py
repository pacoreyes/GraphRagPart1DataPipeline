# -----------------------------------------------------------
# Erase Memgraph Script
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

"""
Standalone script to completely erase the Memgraph database content.

This script provides a convenient way to clear all nodes, relationships, and 
indexes from the Memgraph database. It reuses the core logic from the 
ETL application to ensure consistency.

Usage:
    python -m scripts.erase_memgraph
"""

import sys
from typing import Any

from dagster import build_asset_context
from data_pipeline.utils.memgraph_helpers import (
    MemgraphConfig,
    clear_database,
    get_memgraph_client,
)


def main() -> None:
    """
    Main execution function for the database erasure script.
    """
    print("--- Memgraph Database Erasure Tool ---")
    
    # 1. Setup Configuration
    config = MemgraphConfig()
    
    # 2. Initialize Client and Check Connectivity
    try:
        memgraph = get_memgraph_client(config)
        # Check connectivity
        memgraph.execute("RETURN 1;")
    except Exception as e:
        print(f"Error: Could not connect to Memgraph at {config.host}:{config.port}.")
        print(f"Details: {e}")
        sys.exit(1)

    print(f"Connected to Memgraph at {config.host}:{config.port}")
    
    # 3. Confirmation Prompt
    confirm = input(
        "\nWARNING: This will delete ALL data and indexes in the database.\n"
        "Are you sure you want to proceed? (yes/no): "
    )
    
    if confirm.lower() != "yes":
        print("Operation cancelled.")
        return

    # 4. Create Context and Execute Cleanup
    # We use build_asset_context() to satisfy the requirement for an 
    # AssetExecutionContext used for logging within clear_database.
    context: Any = build_asset_context()

    print("\nStarting database erasure...")
    try:
        clear_database(memgraph, context)
        print("\nSUCCESS: Database successfully erased.")
    except Exception as e:
        print(f"\nERROR: Failed to erase database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


""" DO NOT DELETE
uv run --env-file .env scripts/erase_memgraph.py

MATCH path = (art:Artist {name: "Arcade Fire"})-[:SIMILAR_TO]->(similar:Artist)
RETURN path;
"""
