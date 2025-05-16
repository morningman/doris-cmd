"""
Pytest configuration for doris-cmd tests.
"""
import os
import sys
import pytest

# Add the parent directory to sys.path so we can import doris_cmd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from doris_cmd.connection import DorisConnection
from test.config import DORIS_CONFIG, TEST_DATABASE


@pytest.fixture(scope="session")
def doris_connection():
    """
    Create a connection to Doris for testing.
    
    This fixture is session-scoped, meaning it will be created once per test session.
    """
    # Create connection using config
    connection = DorisConnection(
        host=DORIS_CONFIG["host"],
        port=DORIS_CONFIG["port"],
        user=DORIS_CONFIG["user"],
        password=DORIS_CONFIG["password"],
        database=DORIS_CONFIG.get("database")
    )
    
    # Connect to Doris
    connected = connection.connect()
    if not connected:
        pytest.skip("Could not connect to Doris. Skipping tests.")
    
    # Return the connection for tests to use
    yield connection
    
    # Close the connection after tests
    connection.close()


@pytest.fixture(scope="session")
def test_database(doris_connection):
    """
    Use an existing database for testing.
    
    This fixture is session-scoped.
    """
    # Use existing database from config
    database = TEST_DATABASE
    
    # Check if database exists
    column_names, results = doris_connection.execute_query("SHOW DATABASES")
    
    # Find the database name column
    db_name_col = None
    for col in column_names:
        if "database" in col.lower():
            db_name_col = col
            break
    
    if db_name_col:
        db_names = [row[db_name_col] for row in results]
        if database not in db_names:
            pytest.skip(f"Test database {database} does not exist. Skipping tests.")
    
    # Switch to the test database
    success = doris_connection.use_database(database)
    if not success:
        pytest.skip(f"Could not switch to test database {database}. Skipping tests.")
    
    # Return the database name for tests to use
    yield database 