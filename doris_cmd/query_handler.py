#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Query handling utilities for doris-cmd.
"""
import os
import sys
import signal
import time
import requests
import json

from doris_cmd.progress import ProgressTracker


def _handle_special_commands(connection, query):
    """Handle special commands like USE, SWITCH, and SOURCE.
    
    Args:
        connection (DorisConnection): Database connection
        query (str): SQL query to execute
        
    Returns:
        tuple: (result, is_special_command)
            where result is the command result (varies by command type)
            and is_special_command is a boolean indicating if it was a special command
    """
    # Handle USE command to change database
    if query.lower().startswith('use '):
        db_name = query[4:].strip()
        if connection.use_database(db_name):
            print(f"Database changed to {db_name}")
        return (None, None, None), True
    
    # Handle SWITCH command to change catalog
    if query.lower().startswith('switch '):
        catalog_name = query[7:].strip()
        if connection.switch_catalog(catalog_name):
            print(f"Catalog changed to {catalog_name}")
        return (None, None, None), True
    
    # Not a special command
    return None, False


def _split_statements(query):
    """Split a query into multiple statements by semicolons, preserving quoted strings.
    
    Args:
        query (str): SQL query to split
        
    Returns:
        list: List of individual SQL statements
    """
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
        
    return statements


def handle_query(connection, query):
    """Handle query execution.
    
    Args:
        connection (DorisConnection): Database connection
        query (str): SQL query to execute
        
    Returns:
        tuple: (column_names, results, trace_id)
    """
    # Handle special commands
    result, is_special = _handle_special_commands(connection, query)
    if is_special:
        return result
    
    # Handle SOURCE command (file execution)
    if query.lower().startswith('source '):
        file_path = query[7:].strip().strip('"\'')
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return None, None, None
            
        print(f"Executing SQL from file: {file_path}")
        
        # Execute file will set new trace_id for each query in the file
        # and connection.trace_id will have the last query's ID
        column_names, results = connection.execute_file(file_path)
        
        return column_names, results, connection.trace_id
    
    # Regular query execution
    # The execute_query method will set a new trace_id before executing the query
    column_names, results = connection.execute_query(query)
    
    return column_names, results, connection.trace_id


def _setup_sigint_handler(connection, progress_tracker=None):
    """Set up a signal handler for SIGINT (Ctrl+C).
    
    Args:
        connection (DorisConnection): Database connection
        progress_tracker (ProgressTracker, optional): Progress tracker to stop
        
    Returns:
        tuple: (original_handler, new_handler_function)
    """
    original_handler = signal.getsignal(signal.SIGINT)
    
    def sigint_handler(sig, frame):
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        # Stop progress tracking if active
        if progress_tracker and hasattr(progress_tracker, 'tracking') and progress_tracker.tracking:
            progress_tracker.stop_tracking()
        # Cancel query
        connection.cancel_query()
        # Raise KeyboardInterrupt to be caught by outer try/except
        raise KeyboardInterrupt()
    
    # Set custom handler
    signal.signal(signal.SIGINT, sigint_handler)
    
    return original_handler, sigint_handler


def _handle_source_file(connection, file_path, handler_func, output_file=None, **kwargs):
    """Handle execution of SQL from a file.
    
    Args:
        connection (DorisConnection): Database connection
        file_path (str): Path to the SQL file
        handler_func (callable): Function to handle each query
        output_file (str, optional): Path to output CSV file
        **kwargs: Additional arguments to pass to handler_func
        
    Returns:
        tuple: The result of the last query execution
    """
    from doris_cmd.display import display_results
    
    # Set up signal handler for Ctrl+C (SIGINT)
    original_handler, _ = _setup_sigint_handler(connection)
    
    try:
        # Read the file content
        with open(file_path, 'r') as f:
            sql = f.read()
            
        # Split the file into individual queries
        queries = [q.strip() for q in sql.split(';') if q.strip()]
        
        last_result = None
        for i, query in enumerate(queries):
            print(f"Executing query: {query}")
            # Execute each query
            result = handler_func(connection, query, **kwargs)
            
            # Display results for each query if output_file is provided
            if output_file and len(result) >= 2 and result[0] and result[1]:
                # For profile mode, result is (column_names, results, trace_id, runtime)
                # For progress mode, result is (column_names, results, trace_id, progress_tracker, runtime)
                column_names, rows = result[0], result[1]
                trace_id = result[2] if len(result) > 2 else None
                runtime = result[-1] if len(result) > 3 else None
                
                # First query overwrites, subsequent queries append
                append_mode = i > 0 if output_file else False 
                display_results(column_names, rows, trace_id, runtime, output_file, append_csv=append_mode)
                print()  # Add empty line for readability
            
            last_result = result
        
        return last_result
    finally:
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)


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
        tuple: (column_names, results, trace_id, progress_tracker, runtime) 
              of the last executed statement
    """
    # Import here to avoid circular imports
    from doris_cmd.display import display_results
    
    # Handle special commands
    result, is_special = _handle_special_commands(connection, query)
    if is_special:
        return None, None, None, None, None
    
    # Handle SOURCE command (file execution)
    if query.lower().startswith('source '):
        file_path = query[7:].strip().strip('"\'')
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return None, None, None, None, None
            
        print(f"Executing SQL from file: {file_path}")
        
        return _handle_source_file(
            connection, 
            file_path, 
            handle_query_with_progress_single,
            output_file=output_file,
            mock_mode=mock_mode
        )
    
    # Check for multiple statements in the query
    statements = _split_statements(query)
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
                column_names, rows, trace_id, progress_tracker, runtime = results
                
                # Display results for each statement
                if column_names and rows:
                    # First statement overwrites, subsequent statements append
                    append_mode = idx > 0 if output_file else False
                    display_results(column_names, rows, trace_id, runtime, output_file, append_csv=append_mode)
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
        tuple: (column_names, results, trace_id, progress_tracker, runtime)
    """
    # Set up signal handler for Ctrl+C (SIGINT)
    progress_tracker = None
    original_handler, _ = _setup_sigint_handler(connection, progress_tracker)
    
    # Create progress tracker before executing the query
    column_names, results = None, None
    runtime = None
    
    try:
        # Make sure connection is alive
        if not connection._check_connection():
            print("Connection lost. Reconnecting...")
            if not connection.reconnect():
                print("Failed to reconnect.")
                return None, None, None, None, None
        
        # Set a new trace_id before executing the query
        connection._set_trace_id()  # Set a new trace_id now
        trace_id = connection.trace_id  # Get the trace_id
        
        # Create and start the progress tracker with the trace_id already set
        progress_tracker = ProgressTracker(
            host=connection.host,
            connection=connection,
            trace_id=trace_id,  # Use the already set trace_id
            mock_mode=mock_mode,
            auth_user=connection.user,
            auth_password=connection.password
        )
        
        # Start tracking progress before executing the query
        progress_tracker.start_tracking()
        
        # Execute query in the same thread
        # For better experience with long-running queries
        column_names, results = connection.execute_query(query, set_trace_id=False)  # Don't set a new trace_id
        
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
        
    # Return trace_id along with results and runtime
    return column_names, results, connection.trace_id, progress_tracker, runtime 


def handle_query_with_profile(connection, query, output_file=None):
    """Handle query execution with profile collection.
    
    This function executes the query and collects the profile information.
    
    For multiple statements (separated by semicolons):
    - Each statement is executed separately
    - Results for each statement are displayed immediately
    - Only the last result is returned to the caller
    
    For single statements:
    - Results are not displayed, the caller is responsible for displaying them
    
    Args:
        connection (DorisConnection): Database connection
        query (str): SQL query to execute
        output_file (str, optional): Path to output CSV file
        
    Returns:
        tuple: (column_names, results, trace_id, runtime) of the last executed statement
    """
    # Import here to avoid circular imports
    from doris_cmd.display import display_results
    
    # Handle special commands
    result, is_special = _handle_special_commands(connection, query)
    if is_special:
        return None, None, None, None
    
    # Handle SOURCE command (file execution)
    if query.lower().startswith('source '):
        file_path = query[7:].strip().strip('"\'')
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return None, None, None, None
            
        print(f"Executing SQL from file: {file_path}")
        
        return _handle_source_file(
            connection, 
            file_path, 
            handle_query_with_profile_single,
            output_file=output_file
        )
    
    # Check for multiple statements in the query
    statements = _split_statements(query)
    if len(statements) > 1:
        # Execute each statement separately
        last_results = None, None, None, None
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
                results = handle_query_with_profile_single(connection, stmt)
                column_names, rows, trace_id, runtime = results
                
                # Display results for each statement
                if column_names and rows:
                    # First statement overwrites, subsequent statements append
                    append_mode = idx > 0 if output_file else False
                    display_results(column_names, rows, trace_id, runtime, output_file, append_csv=append_mode)
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
        
    # Regular query execution with profile (single statement)
    return handle_query_with_profile_single(connection, query)


def handle_query_with_profile_single(connection, query):
    """Handle single query execution with profile collection.
    
    This function executes a single query and collects the profile but does not display results.
    Results display should be handled by the caller.
    
    Args:
        connection (DorisConnection): Database connection
        query (str): SQL query to execute
        
    Returns:trace_id
        tuple: (column_names, results, query_id, runtime)
    """
    # Set up signal handler for Ctrl+C (SIGINT)
    original_handler, _ = _setup_sigint_handler(connection)
    
    column_names, results = None, None
    runtime = None
    start_time = time.time()
    
    try:
        # Make sure connection is alive
        if not connection._check_connection():
            print("Connection lost. Reconnecting...")
            if not connection.reconnect():
                print("Failed to reconnect.")
                return None, None, None, None
        
        # Set a new trace_id before executing the query
        connection._set_trace_id()
        trace_id = connection.trace_id
        
        try:
            # Execute the actual query
            print(f"Executing query and collecting profile...")
            column_names, results = connection.execute_query(query, set_trace_id=False)
            
            # Calculate runtime
            runtime = time.time() - start_time
            
            # Fetch the query profile
            print(f"Query completed in {runtime:.2f}s. Fetching profile...")
            try:
                # First get the actual query ID using last_query_id()
                query_id_col, query_id_result = connection.execute_query("SELECT last_query_id()")
                
                if query_id_result and len(query_id_result) > 0:
                    # Get the query ID from the result
                    actual_query_id = list(query_id_result[0].values())[0]
                    print(f"Found last query ID: {actual_query_id}")
                    
                    # Create profile directory if it doesn't exist
                    profile_dir = "/tmp/.doris_profile"
                    os.makedirs(profile_dir, exist_ok=True)
                    
                    # Get the HTTP port
                    http_port = connection.get_http_port()
                    if http_port:
                        # Get profile from REST API
                        profile_url = f"http://{connection.host}:{http_port}/rest/v2/manager/query/profile/text/{actual_query_id}"
                        print(f"Profile URL: {profile_url}")
                        
                        # Make HTTP request with authentication if needed
                        auth = None
                        if connection.user:
                            # Always pass auth even if password is empty string
                            auth = (connection.user, connection.password if connection.password is not None else '')
                            print(f"Using authentication with user: '{connection.user}', password length: {len(connection.password) if connection.password is not None else 0}")
                        else:
                            print("Warning: No authentication credentials available (username is None)")
                        
                        try:
                            response = requests.get(profile_url, auth=auth, timeout=10)
                            
                            if response.status_code == 200:
                                # Parse response JSON
                                try:
                                    response_json = response.json()
                                    if response_json.get('msg') == 'success':
                                        # Save profile to file
                                        profile_data = response_json.get('data', '')
                                        profile_file = f"{profile_dir}/doris_profile_{actual_query_id}.txt"
                                        with open(profile_file, 'w') as f:
                                            f.write(str(profile_data))
                                        print(f"Profile saved to {profile_file}")
                                    else:
                                        print(f"Failed to get profile: API returned msg={response_json.get('msg')}")
                                except ValueError:
                                    # Not a JSON response
                                    print(f"Failed to parse profile response as JSON, saving raw response")
                                    profile_file = f"{profile_dir}/doris_profile_{actual_query_id}.txt"
                                    with open(profile_file, 'w') as f:
                                        f.write(response.text)
                                    print(f"Raw response saved to {profile_file}")
                            else:
                                print(f"Failed to get profile: HTTP {response.status_code}")
                                if response.text:
                                    print(f"Error response: {response.text}")
                        except Exception as profile_err:
                            print(f"Error fetching profile: {profile_err}")
                    else:
                        print("Failed to get HTTP port. Cannot fetch profile.")
                else:
                    print("Could not get last query ID. Profile collection skipped.")
                    
            except Exception as e:
                print(f"Error collecting profile: {e}")
                # Continue without profile - query was executed successfully
            
        except Exception as e:
            print(f"Error during query execution: {e}")
            return None, None, None, None
            
    except Exception as e:
        print(f"Error during query execution: {e}")
        # Try to reconnect if connection was lost
        if not connection._check_connection():
            try:
                print("Attempting to reconnect...")
                connection.reconnect()
            except Exception as reconnect_error:
                print(f"Failed to reconnect: {reconnect_error}")
        return None, None, None, None
    finally:
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        
    # Return query results, ID and runtime
    return column_names, results, connection.trace_id, runtime 