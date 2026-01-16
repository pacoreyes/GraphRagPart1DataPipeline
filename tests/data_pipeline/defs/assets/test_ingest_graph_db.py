import pytest
from unittest.mock import MagicMock
import polars as pl
from dagster import build_asset_context
from contextlib import contextmanager

from data_pipeline.defs.assets.ingest_graph_db import ingest_graph_db
from data_pipeline.defs.resources import Neo4jResource

# Global mock storage to bridge between fixture and class
_MOCK_DRIVER = None

class MockNeo4jResource(Neo4jResource):
    @contextmanager
    def get_driver(self, context):
        yield _MOCK_DRIVER

@pytest.fixture
def mock_neo4j_resource():
    global _MOCK_DRIVER
    
    # Create mocks for driver and session
    mock_driver = MagicMock()
    mock_session = MagicMock()
    _MOCK_DRIVER = mock_driver
    
    # Instantiate the subclass
    resource = MockNeo4jResource(uri="bolt://localhost:7687", username="neo4j", password="password")
    
    # Setup session
    mock_driver.session.return_value.__enter__.return_value = mock_session
    mock_driver.verify_connectivity.return_value = None
    
    # FIX: Setup default return values to prevent infinite loops in clear_database
    # clear_database expects {"deleted": 0} to exit loops.
    mock_record = {"deleted": 0, "count": 0}
    
    mock_result = MagicMock()
    mock_result.single.return_value = mock_record
    mock_result.data.return_value = [] # For SHOW INDEXES
    mock_result.consume.return_value = None
    
    mock_session.run.return_value = mock_result
    
    # Return tuple: (resource_instance, mock_driver, mock_session)
    yield resource, mock_driver, mock_session
    
    # Cleanup
    _MOCK_DRIVER = None

def test_ingest_graph_db_executes_related_to_query(mock_neo4j_resource):
    resource, mock_driver, mock_session = mock_neo4j_resource

    # Create dummy input data
    artists_lf = pl.LazyFrame({"id": ["A1"], "name": ["Artist1"], "mbid": ["m1"], "country": ["US"], "aliases": [None], "tags": [None], "genres": [None], "similar_artists": [None]})
    releases_lf = pl.LazyFrame({"id": ["R1"], "title": ["Album1"], "year": [2020], "artist_id": ["A1"]})
    tracks_lf = pl.LazyFrame({"id": ["T1", "T2"], "title": ["Track1", "Track2"], "album_id": ["R1", "R1"]})
    genres_lf = pl.LazyFrame({"id": ["G1"], "name": ["Rock"], "aliases": [["Hard Rock"]], "parent_ids": [None]})
    countries_lf = pl.LazyFrame({"id": ["Q30"], "name": ["US"]})

    context = build_asset_context()

    # Run the asset
    result = ingest_graph_db(context, resource, artists_lf, releases_lf, tracks_lf, genres_lf, countries_lf)
    
    # Verify the result status
    assert result.metadata["status"] == "success"
    
    # Verify that execute_cypher_with_progress was called with our new query
    session_run_calls = mock_session.run.call_args_list
    session_write_calls = mock_session.execute_write.call_args_list
    # driver_calls = mock_driver.execute_query.call_args_list
    
    found_related_to_query = False
    expected_snippet = "MERGE (source)-[:RELATED_TO]->(target)"
    
    # Helper to check a query string
    def check_query(q):
        return q and expected_snippet in q

    # Check session.run calls (args[0] is query)
    for c in session_run_calls:
        query_arg = None
        if c.args:
            query_arg = c.args[0]
        elif 'query_' in c.kwargs:
             query_arg = c.kwargs['query_']
        elif 'query' in c.kwargs:
             query_arg = c.kwargs['query']
        
        if check_query(query_arg):
            found_related_to_query = True
            break
            
    # Check session.execute_write calls (args[1] is query)
    if not found_related_to_query:
        for c in session_write_calls:
            # execute_write(work_fn, query, params)
            if len(c.args) >= 2:
                if check_query(c.args[1]):
                    found_related_to_query = True
                    break
            
    assert found_related_to_query, "The 'RELATED_TO' Cypher query was not executed."
