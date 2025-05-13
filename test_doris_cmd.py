#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python test script for doris-cmd.
This script tests core functionalities of doris-cmd, such as connection, query execution, and progress reporting.
"""
import os
import time
import argparse
import threading
from doris_cmd.connection import DorisConnection
from doris_cmd.progress import ProgressTracker


def test_connection(host, port, user, password, database):
    """Test connection to Apache Doris."""
    print("\n==== Testing Connection ====")
    connection = DorisConnection(host, port, user, password, database)
    success = connection.connect()
    
    if success:
        print("✅ Connection successful")
        print(f"Initial Query ID: {connection.query_id}")
        print(f"HTTP Port: {connection.http_port}")
        
        # Test getting current database
        current_db = connection.get_current_database()
        print(f"Current database: {current_db}")
        print(f"Set Query ID: {connection.query_id}")
        
        # Close connection
        connection.close()
        return connection
    else:
        print("❌ Connection failed")
        return None


def test_query_id_generation(connection):
    """Test that each query generates a new query_id."""
    print("\n==== Testing Query ID Generation ====")
    
    # Reconnect because the connection was closed earlier
    success = connection.connect()
    if not success:
        print("❌ Connection failed")
        return
    
    try:
        # Execute first query and record query_id
        print("Executing first query: SELECT 1")
        _, _ = connection.execute_query("SELECT 1")
        first_query_id = connection.query_id
        print(f"First query's Query ID: {first_query_id}")
        
        # Execute second query and record query_id
        print("Executing second query: SELECT 2")
        _, _ = connection.execute_query("SELECT 2")
        second_query_id = connection.query_id
        print(f"Second query's Query ID: {second_query_id}")
        
        # Check if the two query_ids are different
        if first_query_id != second_query_id:
            print("✅ Verification successful: Each query generated a new Query ID")
        else:
            print("❌ Verification failed: Both queries used the same Query ID")
    finally:
        # Close connection
        connection.close()


def display_results(column_names, results, query_id=None):
    """Display query results.
    
    Args:
        column_names (list): List of column names
        results (list): List of result dictionaries
        query_id (str, optional): Query ID
    """
    if not column_names or not results:
        return
        
    print(f"Column names: {column_names}")
    print(f"Number of result rows: {len(results)}")
    print("Results preview:")
    for i, row in enumerate(results[:5]):  # Only show the first 5 rows
        print(f"  {i+1}. {row}")
    if len(results) > 5:
        print(f"  ... Total {len(results)} rows")
        
    # Display query ID
    if query_id:
        print(f"Query ID: {query_id}")


def test_http_port_detection(connection):
    """Test HTTP port automatic detection."""
    print("\n==== Testing HTTP Port Auto-detection ====")
    
    # Reconnect because the connection was closed earlier
    success = connection.connect()
    if not success:
        print("❌ Connection failed")
        return
    
    try:
        # Check if HTTP port was obtained
        http_port = connection.get_http_port()
        if http_port:
            print(f"✅ Successfully detected HTTP port: {http_port}")
        else:
            print("❌ Failed to detect HTTP port")
        
    finally:
        # Close connection
        connection.close()


def test_simple_query(connection, mock_mode=False):
    """Test simple query and progress reporting."""
    print("\n==== Testing Simple Query and Progress Reporting ====")
    
    # Reconnect because the connection was closed earlier
    success = connection.connect()
    if not success:
        print("❌ Connection failed")
        return
    
    try:
        # Execute simple query - SHOW DATABASES
        print("Executing query: SHOW DATABASES")
        
        # Create progress tracker
        progress_tracker = ProgressTracker(
            host=connection.host,
            connection=connection,
            query_id=None,
            mock_mode=mock_mode
        )
        
        # Execute query (this will set a new query_id)
        column_names, results = connection.execute_query("SHOW DATABASES")
        query_id = connection.query_id
        
        # Update progress tracker's query_id and start tracking
        progress_tracker.query_id = query_id
        progress_tracker.start_tracking()
        
        # Wait a short time to let progress tracking display
        time.sleep(0.5)
        
        # Stop progress tracking
        progress_tracker.stop_tracking()
        print()  # New line
        
        if column_names and results:
            print("✅ Query successful")
            display_results(column_names, results, query_id)
        else:
            print("❌ Query returned no results or failed")
    finally:
        # Close connection
        connection.close()


def test_long_running_query(connection, mock_mode=False):
    """Test long-running query and progress reporting."""
    print("\n==== Testing Long-Running Query and Progress Reporting ====")
    
    # Reconnect because the connection was closed earlier
    success = connection.connect()
    if not success:
        print("❌ Connection failed")
        return
    
    try:
        # Create progress tracker
        progress_tracker = ProgressTracker(
            host=connection.host,
            connection=connection,
            query_id=None,
            mock_mode=mock_mode
        )
        query_id = None
        
        # Execute a long-running query, here using SELECT SLEEP(5) as an example
        print("Executing long-running query: SELECT SLEEP(5)")
        
        # Execute query in a separate thread
        def execute_query():
            nonlocal query_id
            # Execute query (will set a new query_id)
            connection.execute_query("SELECT SLEEP(5)")
            query_id = connection.query_id
            
        query_thread = threading.Thread(target=execute_query)
        query_thread.daemon = True
        query_thread.start()
        
        # Wait a short time for the query to start
        time.sleep(0.2)
        
        # Set progress tracker's query_id and start tracking
        progress_tracker.query_id = connection.query_id
        progress_tracker.start_tracking()
        
        # Wait a few seconds to let progress display
        time.sleep(2)
        
        # Simulate query cancellation
        print("\nSimulating query cancellation...")
        connection.cancel_query()
        
        # Wait for query thread to finish
        query_thread.join(timeout=2)
        
        # Stop progress tracking
        progress_tracker.stop_tracking()
        print()  # New line
        
        print("✅ Long-running query test completed")
        if query_id:
            print(f"Query ID: {query_id}")
        
    finally:
        # Close connection
        connection.close()


def test_query_with_results(connection, database, mock_mode=False):
    """Test query returning results."""
    print("\n==== Testing Query with Results ====")
    
    # Reconnect because the connection was closed earlier
    success = connection.connect()
    if not success:
        print("❌ Connection failed")
        return
    
    # If a database is specified, switch to that database
    if database:
        print(f"Switching to database: {database}")
        connection.use_database(database)
    
    try:
        # Execute query - simple SELECT query
        print("Executing query: SELECT 1 AS test_col, 'Hello Doris' AS message")
        
        # Create progress tracker
        progress_tracker = ProgressTracker(
            host=connection.host,
            connection=connection,
            query_id=None,
            mock_mode=mock_mode
        )
        
        # Execute query
        column_names, results = connection.execute_query(
            "SELECT 1 AS test_col, 'Hello Doris' AS message"
        )
        query_id = connection.query_id
        
        # Update progress tracker's query_id and start tracking
        progress_tracker.query_id = query_id
        progress_tracker.start_tracking()
        
        # Wait a short time
        time.sleep(0.5)
        
        # Stop progress tracking
        progress_tracker.stop_tracking()
        print()  # New line
        
        if column_names and results:
            print("✅ Query successful")
            display_results(column_names, results, query_id)
        else:
            print("❌ Query returned no results or failed")
    finally:
        # Close connection
        connection.close()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="doris-cmd test script")
    parser.add_argument("--host", default="localhost", help="Apache Doris host")
    parser.add_argument("--port", type=int, default=9030, help="Apache Doris MySQL port")
    parser.add_argument("--http_port", type=int, default=None, help="Apache Doris HTTP port (optional, auto-detect)")
    parser.add_argument("--user", default="root", help="Username")
    parser.add_argument("--password", default="", help="Password")
    parser.add_argument("--database", default=None, help="Database name")
    parser.add_argument("--mock", action="store_true", help="Enable mock mode for testing")
    
    args = parser.parse_args()
    
    print("doris-cmd test script")
    print(f"Connection information: {args.host}:{args.port}")
    print(f"User: {args.user}, Database: {args.database or '(not specified)'}")
    if args.http_port:
        print(f"HTTP port: {args.http_port} (user specified)")
    else:
        print("HTTP port: auto-detect")
    print(f"Mock mode: {'Enabled' if args.mock else 'Disabled'}")
    
    # Test connection
    connection = test_connection(args.host, args.port, args.user, args.password, args.database)
    if connection is None:
        print("Cannot continue testing, connection failed")
        return
    
    # If the user specified an HTTP port, use that port
    if args.http_port:
        print(f"Using user-specified HTTP port: {args.http_port}")
        connection = DorisConnection(args.host, args.port, args.user, args.password, args.database)
        connection.http_port = args.http_port
        connection.connect()
    
    # Test HTTP port auto-detection
    test_http_port_detection(connection)
    
    # Test query_id generation
    test_query_id_generation(connection)
    
    # Test simple query
    test_simple_query(connection, args.mock)
    
    # Test query with results
    test_query_with_results(connection, args.database, args.mock)
    
    # Test long-running query
    test_long_running_query(connection, args.mock)
    
    # If mock mode is enabled, test progress tracking in pure mock mode
    if args.mock:
        print("\n==== Testing Pure Mock Mode Progress Tracking ====")
        mock_tracker = ProgressTracker(
            host=connection.host,
            mock_mode=True
        )
        mock_tracker.query_id = "mock_query_test_123"
        mock_tracker.start_tracking()
        print("Mock mode progress tracking started, waiting 5 seconds...")
        time.sleep(5)
        mock_tracker.stop_tracking()
        print("\n✅ Mock mode test completed")
    
    print("\nAll tests completed!")


if __name__ == "__main__":
    main() 