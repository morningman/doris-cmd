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
        # Remove trailing semicolon if present
        if db_name.endswith(';'):
            db_name = db_name[:-1].strip()
        if connection.use_database(db_name):
            print(f"Database changed to {db_name}")
        return (None, None, None, None), True
    
    # Handle SWITCH command to change catalog
    if query.lower().startswith('switch '):
        catalog_name = query[7:].strip()
        # Remove trailing semicolon if present
        if catalog_name.endswith(';'):
            catalog_name = catalog_name[:-1].strip()
        if connection.switch_catalog(catalog_name):
            print(f"Catalog changed to {catalog_name}")
        return (None, None, None, None), True
    
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
        tuple: (column_names, results, trace_id, query_id)
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
            return None, None, None, None
            
        print(f"Executing SQL from file: {file_path}")
        
        # Execute file will set new trace_id for each query in the file
        # and connection.trace_id will have the last query's ID
        column_names, results, query_id = connection.execute_file(file_path)
        
        return column_names, results, connection.trace_id, query_id
    
    # Regular query execution
    # The execute_query method will set a new trace_id before executing the query
    column_names, results, query_id = connection.execute_query(query)
    
    return column_names, results, connection.trace_id, query_id


def _handle_source_file(connection, file_path, handler_func, output_file=None, **kwargs):
    """Execute SQL from a file.
    
    Args:
        connection (DorisConnection): Database connection
        file_path (str): Path to the SQL file
        handler_func (function): Query handler function to use
        output_file (str, optional): Path to output CSV file
        **kwargs: Additional arguments for the handler function
        
    Returns:
        tuple: The result of the last query execution
    """
    from doris_cmd.display import display_results
    
    # Set up signal handler for Ctrl+C (SIGINT)
    original_handler = signal.getsignal(signal.SIGINT)
    
    def sigint_handler(sig, frame):
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        # Cancel query
        connection.cancel_query()
        # Raise KeyboardInterrupt to be caught by outer try/except
        raise KeyboardInterrupt()
    
    signal.signal(signal.SIGINT, sigint_handler)
    
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
                # For progress mode, result is (column_names, results, trace_id, query_id, progress_tracker, runtime)
                column_names, rows = result[0], result[1]
                trace_id = result[2] if len(result) > 2 else None
                
                # Get query_id if it exists (progress mode)
                query_id = result[3] if len(result) > 3 and not isinstance(result[3], (float, int)) else None
                
                # Get runtime (it's the last element for profile mode, or second-to-last for progress mode)
                runtime = result[-1] if len(result) > 3 else None
                
                # First query overwrites, subsequent queries append
                append_mode = i > 0 if output_file else False 
                display_results(column_names, rows, trace_id, query_id, runtime, output_file, append_csv=append_mode)
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
        tuple: (column_names, results, trace_id, query_id, progress_tracker, runtime) 
              of the last executed statement
    """
    # Import here to avoid circular imports
    from doris_cmd.display import display_results
    
    # Handle special commands
    result, is_special = _handle_special_commands(connection, query)
    if is_special:
        return None, None, None, None, None, None
    
    # Handle SOURCE command (file execution)
    if query.lower().startswith('source '):
        file_path = query[7:].strip().strip('"\'')
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            return None, None, None, None, None, None
            
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
        last_results = None, None, None, None, None, None
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
                        if not connection.reconnect(preserve_state=True):
                            print("Failed to reconnect. Skipping remaining statements.")
                            break
                            
                # Execute the statement
                results = handle_query_with_progress_single(connection, stmt, mock_mode)
                column_names, rows, trace_id, query_id, progress_tracker, runtime = results
                
                # Display results for each statement
                if column_names and rows:
                    # First statement overwrites, subsequent statements append
                    append_mode = idx > 0 if output_file else False
                    display_results(column_names, rows, trace_id, query_id, runtime, output_file, append_csv=append_mode)
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
                    if connection.reconnect(preserve_state=True):
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
        tuple: (column_names, results, trace_id, query_id, progress_tracker, runtime)
    """
    # Create progress tracker and set up signal handler after creating it
    column_names, results = None, None
    query_id = None
    runtime = None
    progress_tracker = None
    original_handler = signal.getsignal(signal.SIGINT)
    
    try:
        # Make sure connection is alive
        if not connection._check_connection():
            print("Connection lost. Reconnecting...")
            if not connection.reconnect(preserve_state=True):
                print("Failed to reconnect.")
                return None, None, None, None, None, None
        
        # Set a new trace_id before executing the query
        connection._set_trace_id()  # Set a new trace_id now
        trace_id = connection.trace_id  # Get the trace_id
        
        # Create the progress tracker with the trace_id already set
        progress_tracker = ProgressTracker(
            host=connection.host,
            connection=connection,
            trace_id=trace_id,  # Use the already set trace_id
            mock_mode=mock_mode,
            auth_user=connection.user,
            auth_password=connection.password
        )
        
        # NOW set up signal handler with the actual progress tracker
        def sigint_handler(sig, frame):
            print("\n[INFO] Query interrupted (Ctrl+C)")
            # Restore original handler
            signal.signal(signal.SIGINT, original_handler)
            
            # Save current state BEFORE stopping progress and canceling query
            saved_state = {'catalog': None, 'database': None}
            try:
                # Try to get current state while connection might still be alive
                saved_state['catalog'] = connection.get_current_catalog()
                saved_state['database'] = connection.get_current_database()
            except Exception:
                # If we can't get state, that's okay - we'll use defaults
                pass
            
            # Stop progress tracking if active
            if progress_tracker and hasattr(progress_tracker, 'tracking') and progress_tracker.tracking:
                progress_tracker.stop_tracking()
            # Cancel query (this will work even if connection becomes dead)
            connection.cancel_query()
            # Mark connection as needing reset due to PyMySQL behavior on interruption
            connection._connection_needs_reset = True
            # Save the state for reconnection
            connection._saved_state = saved_state
            # Raise KeyboardInterrupt to be caught by outer try/except
            raise KeyboardInterrupt()
        
        def sigquit_handler(sig, frame):
            print("\n[INFO] Soft query cancellation (Ctrl+\\) - preserving connection")
            # Stop progress tracking if active
            if progress_tracker and hasattr(progress_tracker, 'tracking') and progress_tracker.tracking:
                progress_tracker.stop_tracking()
            # Cancel query only - don't trigger KeyboardInterrupt
            success = connection.cancel_query()
            if success:
                print("[INFO] Query cancelled successfully. Connection preserved.")
            else:
                print("[WARN] Query cancellation may have failed.")
            # Don't raise any exception - just return to let query complete normally
        
        # Set up both signal handlers
        signal.signal(signal.SIGINT, sigint_handler)   # Ctrl+C - full interruption
        signal.signal(signal.SIGQUIT, sigquit_handler) # Ctrl+\ - soft cancellation
        
        # Start tracking progress before executing the query
        progress_tracker.start_tracking()
        
        # Execute query in the same thread
        # For better experience with long-running queries
        column_names, results, query_id = connection.execute_query(query, set_trace_id=False)  # Don't set a new trace_id
        
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
                connection.reconnect(preserve_state=True)
            except Exception as reconnect_error:
                print(f"Failed to reconnect: {reconnect_error}")
        return None, None, None, None, None, None
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
    return column_names, results, connection.trace_id, query_id, progress_tracker, runtime


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
        tuple: (column_names, results, trace_id, query_id, runtime) of the last executed statement
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
            handle_query_with_profile_single,
            output_file=output_file
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
                        if not connection.reconnect(preserve_state=True):
                            print("Failed to reconnect. Skipping remaining statements.")
                            break
                            
                # Execute the statement
                results = handle_query_with_profile_single(connection, stmt)
                column_names, rows, trace_id, query_id, runtime = results
                
                # Display results for each statement
                if column_names and rows:
                    # First statement overwrites, subsequent statements append
                    append_mode = idx > 0 if output_file else False
                    display_results(column_names, rows, trace_id, query_id, runtime, output_file, append_csv=append_mode)
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
                    if connection.reconnect(preserve_state=True):
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
        
    Returns:
        tuple: (column_names, results, trace_id, query_id, runtime)
    """
    # Set up signal handler for Ctrl+C (SIGINT)
    original_handler = signal.getsignal(signal.SIGINT)
    
    def sigint_handler(sig, frame):
        print("\n[INFO] Query interrupted (Ctrl+C)")
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        
        # Save current state BEFORE canceling query
        saved_state = {'catalog': None, 'database': None}
        try:
            # Try to get current state while connection might still be alive
            saved_state['catalog'] = connection.get_current_catalog()
            saved_state['database'] = connection.get_current_database()
        except Exception:
            # If we can't get state, that's okay - we'll use defaults
            pass
        
        # Cancel query
        connection.cancel_query()
        # Mark connection as needing reset due to PyMySQL behavior on interruption
        connection._connection_needs_reset = True
        # Save the state for reconnection
        connection._saved_state = saved_state
        # Raise KeyboardInterrupt to be caught by outer try/except
        raise KeyboardInterrupt()
    
    signal.signal(signal.SIGINT, sigint_handler)
    
    column_names, results = None, None
    query_id = None
    runtime = None
    start_time = time.time()
    
    try:
        # Make sure connection is alive
        if not connection._check_connection():
            print("Connection lost. Reconnecting...")
            if not connection.reconnect(preserve_state=True):
                print("Failed to reconnect.")
                return None, None, None, None, None
        
        # Set a new trace_id before executing the query
        connection._set_trace_id()
        trace_id = connection.trace_id
        
        try:
            # Execute the actual query
            print(f"Executing query and collecting profile...")
            column_names, results, query_id = connection.execute_query(query, set_trace_id=False)
            
            # Calculate runtime
            runtime = time.time() - start_time
            
            # Fetch the query profile
            print(f"Query completed in {runtime:.2f}s. Fetching profile...")
            try:
                # If we already have the query_id from execute_query, use it
                actual_query_id = query_id
                
                # If we don't have a query_id yet, try to get it
                if not actual_query_id:
                    query_id_col, query_id_result, _ = connection.execute_query("SELECT last_query_id()")
                    
                    if query_id_result and len(query_id_result) > 0:
                        # Get the query ID from the result
                        actual_query_id = list(query_id_result[0].values())[0]
                        query_id = actual_query_id
                
                if actual_query_id:
                    print(f"Found query ID: {actual_query_id}")
                    
                    # Create profile directory if it doesn't exist
                    profile_dir = "/tmp/.doris_profile"
                    os.makedirs(profile_dir, exist_ok=True)
                    
                    # Get the HTTP port
                    http_port = connection.get_http_port()
                    if http_port:
                        # Use the correct HTTP API URL to fetch the profile
                        profile_url = f"http://{connection.host}:{http_port}/rest/v2/manager/query/profile/text/{actual_query_id}"
                        
                        # Set up basic authentication
                        auth = None
                        if connection.user:
                            # Always pass auth even if password is empty string
                            auth = (connection.user, connection.password if connection.password is not None else '')
                        
                        try:
                            response = requests.get(profile_url, auth=auth, timeout=10)
                            
                            if response.status_code == 200:
                                # Parse response JSON
                                try:
                                    response_json = response.json()
                                    if response_json.get('msg') == 'success':
                                        # Extract profile data from the response
                                        # The data field is a JSON string with profile information
                                        profile_data = response_json.get('data', '')
                                        
                                        # Parse the profile data which is a JSON string
                                        try:
                                            if isinstance(profile_data, str):
                                                profile_json = json.loads(profile_data)
                                                # Extract the actual profile content
                                                profile_content = profile_json.get('profile', '')
                                                # Convert "\n" strings to actual newlines
                                                profile_content = profile_content.replace('\\n', '\n')
                                            else:
                                                # If not a string, try to get profile directly
                                                profile_content = profile_data.get('profile', str(profile_data))
                                                profile_content = profile_content.replace('\\n', '\n')
                                                
                                            # Save the processed profile to file
                                            profile_file = f"{profile_dir}/doris_profile_{actual_query_id}.txt"
                                            with open(profile_file, 'w') as f:
                                                f.write(profile_content)
                                            print(f"Profile saved to {profile_file}")
                                        except Exception as parse_err:
                                            print(f"Failed to parse profile data: {parse_err}")
                                            # Fall back to saving the raw data
                                            profile_file = f"{profile_dir}/doris_profile_{actual_query_id}.txt"
                                            with open(profile_file, 'w') as f:
                                                f.write(str(profile_data))
                                            print(f"Raw profile data saved to {profile_file}")
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
                    print("Warning: Could not find query ID, profile collection skipped")
            except Exception as e:
                print(f"Warning: Failed to collect profile: {e}")
        except Exception as e:
            print(f"Error during query execution: {e}")
            # Try to reconnect if connection was lost
            if not connection._check_connection():
                try:
                    print("Attempting to reconnect...")
                    connection.reconnect(preserve_state=True)
                except Exception as reconnect_error:
                    print(f"Failed to reconnect: {reconnect_error}")
            return None, None, None, None, None
    finally:
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        
    return column_names, results, trace_id, query_id, runtime 