# -----------------------------------------------------------
# Tests for Neo4j Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from unittest.mock import MagicMock

import pytest
from neo4j.exceptions import ServiceUnavailable, SessionExpired

from data_pipeline.utils.neo4j_helpers import (
    _execute_with_retry,
    clear_database,
    execute_cypher,
)


@pytest.fixture
def mock_driver():
    """Creates a mock Neo4j driver."""
    return MagicMock()


@pytest.fixture
def mock_context():
    """Creates a mock Dagster AssetExecutionContext."""
    context = MagicMock()
    context.log = MagicMock()
    return context


class TestExecuteCypher:
    """Tests for execute_cypher function."""

    def test_execute_cypher_transactional_success(self, mock_driver):
        """Test execute_cypher with transactional=True uses execute_write."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        execute_cypher(mock_driver, "CREATE (n:Test)", {"name": "test"})

        mock_driver.session.assert_called_once_with(database="neo4j")
        mock_session.execute_write.assert_called_once()

    def test_execute_cypher_non_transactional_success(self, mock_driver):
        """Test execute_cypher with transactional=False uses session.run."""
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        execute_cypher(
            mock_driver,
            "CREATE INDEX test_idx FOR (n:Test) ON (n.name)",
            transactional=False
        )

        mock_driver.session.assert_called_once_with(database="neo4j")
        mock_session.run.assert_called_once()
        mock_result.consume.assert_called_once()

    def test_execute_cypher_raises_on_error(self, mock_driver):
        """Test that exceptions are propagated correctly."""
        mock_session = MagicMock()
        mock_session.execute_write.side_effect = Exception("Connection failed")
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(Exception, match="Connection failed"):
            execute_cypher(mock_driver, "CREATE (n:Test)")


class TestExecuteWithRetry:
    """Tests for _execute_with_retry function."""

    def test_execute_with_retry_success_first_attempt(self, mock_driver):
        """Test successful execution on first attempt."""
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.single.return_value = {"count": 10}
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = _execute_with_retry(mock_driver, "MATCH (n) RETURN count(n) AS count")

        assert result == {"count": 10}
        assert mock_driver.session.call_count == 1

    def test_execute_with_retry_recovers_from_transient_failure(self, mock_driver):
        """Test retry logic recovers from SessionExpired."""
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.single.return_value = {"deleted": 100}

        # First call fails, second succeeds
        call_count = [0]
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise SessionExpired("Connection lost")
            return mock_result

        mock_session.run.side_effect = side_effect
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        result = _execute_with_retry(
            mock_driver,
            "MATCH (n) DELETE n RETURN count(*) AS deleted",
            max_retries=3,
            base_delay=0.01  # Fast retry for tests
        )

        assert result == {"deleted": 100}
        assert call_count[0] == 2

    def test_execute_with_retry_exhausts_retries(self, mock_driver):
        """Test that exception is raised after max retries."""
        mock_session = MagicMock()
        mock_session.run.side_effect = ServiceUnavailable("Service unavailable")
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(ServiceUnavailable, match="Service unavailable"):
            _execute_with_retry(
                mock_driver,
                "MATCH (n) RETURN n",
                max_retries=2,
                base_delay=0.01
            )

        # Should have tried 3 times (initial + 2 retries)
        assert mock_session.run.call_count == 3


class TestClearDatabase:
    """Tests for clear_database function."""

    def test_clear_database_deletes_relationships_then_nodes(self, mock_driver, mock_context):
        """Test that clear_database deletes relationships first, then nodes."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate: relationships deleted, then nodes deleted
        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 50})),   # rels batch 1
            MagicMock(single=MagicMock(return_value={"deleted": 0})),    # rels done
            MagicMock(single=MagicMock(return_value={"deleted": 100})),  # nodes batch 1
            MagicMock(single=MagicMock(return_value={"deleted": 0})),    # nodes done
            MagicMock(data=MagicMock(return_value=[])),  # SHOW INDEXES
            MagicMock(data=MagicMock(return_value=[])),  # SHOW CONSTRAINTS
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context, batch_size=100)

        # Verify logging
        mock_context.log.info.assert_any_call("Starting database cleanup...")
        mock_context.log.info.assert_any_call("Deleted 50 relationships.")
        mock_context.log.info.assert_any_call("Deleted 100 nodes.")

    def test_clear_database_handles_empty_database(self, mock_driver, mock_context):
        """Test clear_database handles an already empty database."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        # Database is already empty
        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No rels
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No nodes
            MagicMock(data=MagicMock(return_value=[])),  # SHOW INDEXES
            MagicMock(data=MagicMock(return_value=[])),  # SHOW CONSTRAINTS
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context)

        mock_context.log.info.assert_any_call("Deleted 0 nodes.")

    def test_clear_database_drops_indexes(self, mock_driver, mock_context):
        """Test that clear_database drops existing indexes."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_indexes_result = MagicMock()
        mock_indexes_result.data.return_value = [
            {"name": "artist_idx", "type": "RANGE"},
            {"name": "genre_idx", "type": "BTREE"},
        ]

        mock_constraints_result = MagicMock()
        mock_constraints_result.data.return_value = []

        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No rels
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No nodes
            mock_indexes_result,  # SHOW INDEXES
            MagicMock(),  # DROP INDEX artist_idx
            MagicMock(),  # DROP INDEX genre_idx
            mock_constraints_result,  # SHOW CONSTRAINTS
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context)

        # Verify indexes were dropped
        mock_context.log.info.assert_any_call("Dropped index: artist_idx")
        mock_context.log.info.assert_any_call("Dropped index: genre_idx")

    def test_clear_database_drops_constraints(self, mock_driver, mock_context):
        """Test that clear_database drops existing constraints."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_indexes_result = MagicMock()
        mock_indexes_result.data.return_value = []

        mock_constraints_result = MagicMock()
        mock_constraints_result.data.return_value = [
            {"name": "artist_unique"},
        ]

        mock_results = [
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No rels
            MagicMock(single=MagicMock(return_value={"deleted": 0})),  # No nodes
            mock_indexes_result,  # SHOW INDEXES
            mock_constraints_result,  # SHOW CONSTRAINTS
            MagicMock(),  # DROP CONSTRAINT
        ]
        mock_session.run.side_effect = mock_results

        clear_database(mock_driver, mock_context)

        mock_context.log.info.assert_any_call("Dropped constraint: artist_unique")

    def test_clear_database_raises_on_persistent_error(self, mock_driver, mock_context):
        """Test that errors are raised after retries are exhausted."""
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.run.side_effect = ServiceUnavailable("Persistent failure")

        with pytest.raises(ServiceUnavailable, match="Persistent failure"):
            clear_database(mock_driver, mock_context)

        mock_context.log.error.assert_called_once()
