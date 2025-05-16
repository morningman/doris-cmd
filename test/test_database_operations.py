"""
Tests for database operations in doris_cmd.
"""
import pytest
from test.config import TEST_DATABASE


def test_show_tables(doris_connection, test_database):
    """Test showing tables in the database."""
    # Execute the SHOW TABLES query
    column_names, results = doris_connection.execute_query(f"SHOW TABLES FROM {test_database}")
    
    # Check that we got results in the expected format
    assert column_names is not None
    assert isinstance(results, list)
    
    # Find the table name column
    table_name_col = None
    for col in column_names:
        if "table" in col.lower():
            table_name_col = col
            break
    
    # Verify we found a column that likely contains table names
    assert table_name_col is not None


def test_show_columns(doris_connection, test_database):
    """Test showing columns of a table."""
    # First get a table from the database
    column_names, results = doris_connection.execute_query(f"SHOW TABLES FROM {test_database}")
    
    # Skip the test if no tables exist
    if not results:
        pytest.skip(f"No tables found in {test_database} to test SHOW COLUMNS")
    
    # Find the table name column
    table_name_col = None
    for col in column_names:
        if "table" in col.lower():
            table_name_col = col
            break
    
    assert table_name_col is not None
    
    # Get the first table
    table_name = results[0][table_name_col]
    
    # Show columns for this table
    column_names, results = doris_connection.execute_query(f"SHOW COLUMNS FROM {table_name}")
    
    # Check that we got results in the expected format
    assert column_names is not None
    assert isinstance(results, list)
    
    # There should be information about at least one column
    assert "Field" in column_names or "COLUMN_NAME" in column_names


def test_select_system_tables(doris_connection):
    """Test querying system tables."""
    # Query information_schema.tables
    column_names, results = doris_connection.execute_query(
        "SELECT * FROM information_schema.tables LIMIT 10"
    )
    
    # Check that we got results in the expected format
    assert column_names is not None
    assert isinstance(results, list)
    assert len(results) <= 10  # We limited to 10 rows


def test_query_id_changes(doris_connection):
    """Test that query_id changes for each query."""
    # Execute first query and capture the query ID
    doris_connection.execute_query("SELECT 1")
    first_query_id = doris_connection.query_id
    
    # Execute second query and capture the query ID
    doris_connection.execute_query("SELECT 2")
    second_query_id = doris_connection.query_id
    
    # Verify the query IDs are different
    assert first_query_id != second_query_id


def test_catalog_operations(doris_connection):
    """Test catalog operations."""
    # Get current catalog
    current_catalog = doris_connection.get_current_catalog()
    
    # The default should be "internal"
    assert current_catalog is not None
    
    # Try to switch back to the same catalog (should work)
    success = doris_connection.switch_catalog(current_catalog)
    assert success is True 