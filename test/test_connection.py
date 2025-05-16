"""
Tests for doris_cmd.connection module.
"""
import pytest
from doris_cmd.connection import DorisConnection
from test.config import DORIS_CONFIG, TEST_DATABASE


def test_connection_successful(doris_connection):
    """Test that we can successfully connect to Doris."""
    assert doris_connection.connection is not None
    assert doris_connection.query_id is not None
    assert doris_connection.version is not None


def test_get_current_database(doris_connection, test_database):
    """Test getting current database name."""
    # Switch to test database
    doris_connection.use_database(test_database)
    
    # Get current database
    current_db = doris_connection.get_current_database()
    
    # Verify it's our test database
    assert current_db == TEST_DATABASE


def test_execute_query(doris_connection, test_database):
    """Test executing a simple query."""
    # Switch to test database
    doris_connection.use_database(test_database)
    
    # Execute a simple query
    column_names, results = doris_connection.execute_query("SELECT 1 AS test_value")
    
    # Verify results
    assert column_names == ['test_value']
    assert len(results) == 1
    assert results[0]['test_value'] == 1


def test_use_database(doris_connection, test_database):
    """Test switching databases."""
    # Use the default database first
    doris_connection.use_database(DORIS_CONFIG.get("database", "information_schema"))
    
    # Verify we're on the default database
    current_db = doris_connection.get_current_database()
    assert current_db == DORIS_CONFIG.get("database", "information_schema")
    
    # Switch to test database
    success = doris_connection.use_database(test_database)
    assert success is True
    
    # Verify we're now on the test database
    current_db = doris_connection.get_current_database()
    assert current_db == TEST_DATABASE 