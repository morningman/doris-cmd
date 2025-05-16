#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Query handling utilities for doris-cmd.
"""
import os
import sys
import signal
import time

from doris_cmd.progress import ProgressTracker


def handle_query(connection, query):
    """Handle query execution and progress tracking.
    
    Args:
        connection (DorisConnection): Database connection
        query (str): SQL query to execute
        
    Returns:
        tuple: (column_names, results, query_id)
    """
    # Special commands handling
    if query.lower().startswith('use '):
        # Handle USE command to change database
        db_name = query[4:].strip()
        if connection.use_database(db_name):
            print(f"Database changed to {db_name}")
        return None, None, None
    
    if query.lower().startswith('switch '):
        # Handle SWITCH command to change catalog
        catalog_name = query[7:].strip()
        if connection.switch_catalog(catalog_name):
            print(f"Catalog changed to {catalog_name}")
        return None, None, None
        
    if query.lower().startswith('source '):
        # Handle SOURCE command to execute SQL from file
        file_path = query[7:].strip().strip('"\'')
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return None, None, None
            
        print(f"Executing SQL from file: {file_path}")
        
        # Execute file will set new query_id for each query in the file
        # and connection.query_id will have the last query's ID
        column_names, results = connection.execute_file(file_path)
        
        return column_names, results, connection.query_id
    
    # Regular query execution
    # The execute_query method will set a new query_id before executing the query
    column_names, results = connection.execute_query(query)
    
    return column_names, results, connection.query_id


def handle_query_with_progress(connection, query, mock_mode=False, output_file=None):
    """Handle query execution with progress tracking.
    
    This function executes the query with progress tracking.
    
    For multiple statements (separated by semicolons):
    - Each statement is executed separately
    - Results for each statement are displayed immediately
    - Only the last result is returned to the caller
    
    For single statements:
    - Results are not displayed, the caller is responsible for displaying them
    
    Args:
        connection (DorisConnection): Database connection
        query (str): SQL query to execute
        mock_mode (bool): Whether to use mock mode for progress tracking
        output_file (str, optional): Path to output CSV file
        
    Returns:
        tuple: (column_names, results, query_id, progress_tracker, runtime) 
              of the last executed statement
    """
    # Import here to avoid circular imports
    from doris_cmd.display import display_results
    
    # Special commands handling
    if query.lower().startswith('use '):
        # Handle USE command to change database
        db_name = query[4:].strip()
        if connection.use_database(db_name):
            print(f"Database changed to {db_name}")
        return None, None, None, None, None
    
    if query.lower().startswith('switch '):
        # Handle SWITCH command to change catalog
        catalog_name = query[7:].strip()
        if connection.switch_catalog(catalog_name):
            print(f"Catalog changed to {catalog_name}")
        return None, None, None, None, None
        
    if query.lower().startswith('source '):
        # Handle SOURCE command to execute SQL from file
        file_path = query[7:].strip().strip('"\'')
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return None, None, None, None, None
            
        print(f"Executing SQL from file: {file_path}")
        
        # Set up signal handler for Ctrl+C (SIGINT)
        original_handler = signal.getsignal(signal.SIGINT)

        def sigint_handler(sig, frame):
            # Restore original handler
            signal.signal(signal.SIGINT, original_handler)
            # Cancel query
            connection.cancel_query()
            # Raise KeyboardInterrupt to be caught by outer try/except
            raise KeyboardInterrupt()

        # Set custom handler
        signal.signal(signal.SIGINT, sigint_handler)
        
        try:
            # Read the file content
            with open(file_path, 'r') as f:
                sql = f.read()
                
            # Split the file into individual queries
            queries = [q.strip() for q in sql.split(';') if q.strip()]
            
            column_names, results, query_id, progress_tracker, runtime = None, None, None, None, None
            for i, query in enumerate(queries):
                print(f"Executing query: {query}")
                # Each query will get a new query_id
                column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress_single(
                    connection, query, mock_mode
                )
                
                # Display results for each query
                if column_names and results:
                    # First query overwrites, subsequent queries append
                    append_mode = i > 0 if output_file else False 
                    display_results(column_names, results, query_id, runtime, output_file, append_csv=append_mode)
                    print()  # Add empty line for readability
            
            return column_names, results, query_id, progress_tracker, runtime
        finally:
            # Restore original handler
            signal.signal(signal.SIGINT, original_handler)
    
    # Check for multiple statements in the query (split by semicolon)
    if ';' in query:
        # Split by semicolon, but preserve semicolons in quoted strings
        # This is a simplified approach - a proper SQL parser would be better
        statements = []
        current = ""
        in_quotes = False
        quote_char = None
        
        for char in query:
            if char in ["'", '"'] and (not in_quotes or char == quote_char):
                in_quotes = not in_quotes
                if in_quotes:
                    quote_char = char
                current += char
            elif char == ';' and not in_quotes:
                current = current.strip()
                if current:  # Only add non-empty statements
                    statements.append(current)
                current = ""
            else:
                current += char
        
        # Add the last statement if it's not empty
        current = current.strip()
        if current:
            statements.append(current)
            
        # Check if we have multiple statements
        if len(statements) > 1:
            # Execute each statement separately
            last_results = None, None, None, None, None
            had_error = False
            
            for idx, stmt in enumerate(statements):
                if not stmt.strip():
                    continue  # Skip empty statements
                    
                print(f"Executing statement {idx+1}/{len(statements)}: {stmt}")
                
                try:
                    # Always use a fresh connection state for each statement
                    if had_error or idx > 0:
                        # Ping or reconnect to ensure clean connection state
                        if not connection._check_connection():
                            print("Connection lost. Reconnecting...")
                            if not connection.reconnect():
                                print("Failed to reconnect. Skipping remaining statements.")
                                break
                                
                    # Execute the statement
                    results = handle_query_with_progress_single(connection, stmt, mock_mode)
                    column_names, rows, query_id, progress_tracker, runtime = results
                    
                    # Display results for each statement
                    if column_names and rows:
                        # First statement overwrites, subsequent statements append
                        append_mode = idx > 0 if output_file else False
                        display_results(column_names, rows, query_id, runtime, output_file, append_csv=append_mode)
                        print()  # Add empty line for readability
                    
                    # Remember the last successful results
                    last_results = results
                    had_error = False
                    
                except Exception as e:
                    print(f"Error executing statement {idx+1}: {e}")
                    had_error = True
                    
                    # Try to reconnect to clean up the connection state
                    try:
                        print("Attempting to reconnect...")
                        if connection.reconnect():
                            print("Reconnection successful.")
                        else:
                            print("Reconnection failed. Skipping remaining statements.")
                            break
                    except Exception as reconnect_error:
                        print(f"Failed to reconnect: {reconnect_error}")
                        print("Skipping remaining statements.")
                        break
            
            # Return the last successful results
            return last_results
            
    # Regular query execution with progress (single statement)
    return handle_query_with_progress_single(connection, query, mock_mode)


def handle_query_with_progress_single(connection, query, mock_mode=False):
    """Handle single query execution with progress tracking.
    
    This function executes a single query with progress tracking but does not display results.
    Results display should be handled by the caller.
    
    Args:
        connection (DorisConnection): Database connection
        query (str): SQL query to execute
        mock_mode (bool): Whether to use mock mode for progress tracking
        
    Returns:
        tuple: (column_names, results, query_id, progress_tracker, runtime)
    """
    # Set up signal handler for Ctrl+C (SIGINT)
    original_handler = signal.getsignal(signal.SIGINT)

    def sigint_handler(sig, frame):
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        # Stop progress tracking if active
        if progress_tracker and progress_tracker.tracking:
            progress_tracker.stop_tracking()
        # Cancel query
        connection.cancel_query()
        # Raise KeyboardInterrupt to be caught by outer try/except
        raise KeyboardInterrupt()

    # Set custom handler
    signal.signal(signal.SIGINT, sigint_handler)
    
    # Create progress tracker before executing the query
    progress_tracker = None
    column_names, results = None, None
    runtime = None
    
    try:
        # Make sure connection is alive
        if not connection._check_connection():
            print("Connection lost. Reconnecting...")
            if not connection.reconnect():
                print("Failed to reconnect.")
                return None, None, None, None, None
        
        # Set a new query_id before executing the query
        connection._set_query_id()  # Set a new query_id now
        query_id = connection.query_id  # Get the query_id
        
        # Create and start the progress tracker with the query_id already set
        progress_tracker = ProgressTracker(
            host=connection.host,
            connection=connection,
            query_id=query_id,  # Use the already set query_id
            mock_mode=mock_mode,
            auth_user=connection.user,
            auth_password=connection.password
        )
        
        # Start tracking progress before executing the query
        progress_tracker.start_tracking()
        
        # Execute query in the same thread
        # For better experience with long-running queries
        column_names, results = connection.execute_query(query, set_query_id=False)  # Don't set a new query_id
        
        # Stop tracking progress after query completes
        progress_tracker.stop_tracking()
        
        # Get total runtime
        runtime = progress_tracker.get_total_runtime()
        
        print()  # Print newline after progress tracking
    except Exception as e:
        print(f"Error during query execution: {e}")
        # Try to reconnect if connection was lost
        if not connection._check_connection():
            try:
                print("Attempting to reconnect...")
                connection.reconnect()
            except Exception as reconnect_error:
                print(f"Failed to reconnect: {reconnect_error}")
        return None, None, None, None, None
    finally:
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        # Stop progress tracking if active
        if progress_tracker and progress_tracker.tracking:
            progress_tracker.stop_tracking()
            # Make sure we get the runtime even when exiting early
            if runtime is None:
                runtime = progress_tracker.get_total_runtime()
        
    # Return query_id along with results and runtime
    return column_names, results, connection.query_id, progress_tracker, runtime 