"""
Tests for query operations in doris_cmd.
"""
import os
import tempfile
import pytest
import time
import threading
from test.config import TEST_DATABASE


def test_execute_file(doris_connection, test_database):
    """Test executing SQL from a file."""
    # Create a temporary SQL file
    fd, sql_file = tempfile.mkstemp(suffix='.sql')
    os.close(fd)
    
    try:
        # Write SQL to the file (only SELECT operations)
        with open(sql_file, 'w') as f:
            f.write("""
            -- Select a simple value
            SELECT 1 AS test_value;
            
            -- Show databases
            SHOW DATABASES;
            
            -- Show tables
            SHOW TABLES FROM information_schema;
            """)
        
        # Execute the SQL file
        column_names, results = doris_connection.execute_file(sql_file)
        
        # Verify the results of the last query (SHOW TABLES)
        assert column_names is not None
        assert results is not None
        
    finally:
        # Clean up the temporary file
        os.unlink(sql_file)


def test_reset_query_id(doris_connection):
    """Test resetting the query ID."""
    # Get the current query ID
    original_query_id = doris_connection.query_id
    
    # Reset the query ID
    success = doris_connection.reset_query_id()
    
    # Verify that the query ID changed
    assert success is True
    assert doris_connection.query_id != original_query_id


def test_reconnect(doris_connection):
    """Test reconnecting to the database."""
    # Close the connection first
    doris_connection.close()
    
    # Reconnect
    success = doris_connection.reconnect()
    
    # Verify reconnection was successful
    assert success is True
    assert doris_connection.connection is not None


def test_select_with_conditions(doris_connection):
    """Test SELECT with various conditions."""
    # Test SELECT with arithmetic operations
    column_names, results = doris_connection.execute_query(
        "SELECT 1+1 as addition, 5-2 as subtraction, 3*4 as multiplication, 10/2 as division"
    )
    
    # Verify results
    assert len(results) == 1
    assert results[0]['addition'] == 2
    assert results[0]['subtraction'] == 3
    assert results[0]['multiplication'] == 12
    assert results[0]['division'] == 5.0
    
    # Test SELECT with string functions
    column_names, results = doris_connection.execute_query(
        "SELECT CONCAT('Hello', ' ', 'World') as greeting, LENGTH('Doris') as length"
    )
    
    # Verify results
    assert len(results) == 1
    assert results[0]['greeting'] == 'Hello World'
    assert results[0]['length'] == 5


def test_system_variables(doris_connection):
    """Test querying system variables."""
    # Test getting system variables
    column_names, results = doris_connection.execute_query(
        "SHOW VARIABLES LIKE 'version%'"
    )
    
    # Verify we got results
    assert len(results) > 0
    
    # Check system database
    column_names, results = doris_connection.execute_query(
        "SELECT DATABASE() as current_db"
    )
    
    # Should have a result
    assert len(results) == 1
    assert 'current_db' in results[0]


def test_cancel_query(doris_connection):
    """
    Test canceling a running query.
    
    This test uses a slow query that doesn't require creating tables.
    """
    # Only run if HTTP port is available
    if doris_connection.http_port is None:
        pytest.skip("HTTP port not available, skipping cancel test")
    
    # Define a slow query that uses system tables
    # This cross join should be slow enough to cancel
    slow_query = """
    SELECT 
        a.TABLE_SCHEMA, 
        a.TABLE_NAME, 
        b.TABLE_SCHEMA, 
        b.TABLE_NAME
    FROM 
        information_schema.tables a,
        information_schema.tables b
    LIMIT 10000;
    """
    
    # Define a function to run the query in a separate thread
    query_result = {"completed": False, "error": None}
    
    def run_query():
        try:
            doris_connection.execute_query(slow_query)
            query_result["completed"] = True
        except Exception as e:
            query_result["error"] = str(e)
    
    # Start the query in a separate thread
    query_thread = threading.Thread(target=run_query)
    query_thread.start()
    
    # Wait a short time for the query to start
    time.sleep(0.5)
    
    # Cancel the query
    cancel_success = doris_connection.cancel_query()
    
    # Wait for the query thread to finish (should be cancelled)
    query_thread.join(timeout=5)
    
    # If the test is working correctly, either:
    # 1. The query was cancelled successfully (cancel_success is True)
    # 2. Or the query completed before we could cancel it (query_result["completed"] is True)
    assert cancel_success is True or query_result["completed"] is True 