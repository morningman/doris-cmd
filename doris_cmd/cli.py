"""
Command line interface for Apache Doris.
"""
import os
import sys
import signal
import time
import statistics
from pathlib import Path
import click
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from pygments.lexers.sql import SqlLexer
from rich.console import Console
from rich.table import Table
from rich.style import Style as RichStyle
import csv

from doris_cmd.connection import DorisConnection
from doris_cmd.progress import ProgressTracker


def get_history_file():
    """Get history file path."""
    home_dir = os.path.expanduser("~")
    history_dir = os.path.join(home_dir, ".doris_cmd")
    os.makedirs(history_dir, exist_ok=True)
    return os.path.join(history_dir, "history")


def display_results(column_names, results, query_id=None, runtime=None, output_file=None, append_csv=False):
    """Display query results in tabular format using rich.
    
    Args:
        column_names (list): List of column names
        results (list): List of dictionaries containing results
        query_id (str, optional): The query ID associated with this result
        runtime (float, optional): Query runtime in seconds
        output_file (str, optional): Path to output CSV file
        append_csv (bool): Whether to append to the CSV file if it exists
    """
    if not column_names or not results:
        return

    # Create console for output
    console = Console()
    
    # Create a table
    table = Table(show_header=True, header_style="bold magenta")
    
    # Add columns
    for col in column_names:
        table.add_column(col)
    
    # Add rows
    for row in results:
        table_row = [str(row.get(col, "")) for col in column_names]
        table.add_row(*table_row)
    
    # Display the table
    console.print(table)
    
    # Display the number of rows
    console.print(f"\nRows: {len(results)}")
    
    # Display the query ID and runtime if provided
    if query_id:
        console.print(f"Query ID: {query_id}")
        console.print(f"Query Time: {runtime:.2f}s")
        
    # Export to CSV if output_file is provided
    if output_file:
        if export_query_results_to_csv(column_names, results, output_file, append=append_csv):
            if not append_csv:
                console.print(f"\nQuery results exported to: {output_file}")
            else:
                console.print(f"\nQuery results appended to: {output_file}")
        else:
            console.print(f"\nFailed to export query results to: {output_file}")


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
                    
                    # 显示每条语句的结果
                    if column_names and rows:
                        # First statement overwrites, subsequent statements append
                        append_mode = idx > 0 if output_file else False
                        display_results(column_names, rows, query_id, runtime, output_file, append_csv=append_mode)
                        print()  # 添加空行提高可读性
                    
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


def run_benchmark(connection, sql_path, times=1, mock_mode=False, output_file=None):
    """Run benchmark for SQL queries in a file or all SQL files in a directory.
    
    This function executes SQL queries for benchmarking. It supports:
    1. A single file containing multiple SQL queries (separated by semicolons)
    2. A directory containing multiple .sql files:
       - Each .sql file can contain a single SQL query
       - Or multiple SQL queries separated by semicolons
    
    Args:
        connection (DorisConnection): Database connection
        sql_path (str): Path to the SQL file or directory
        times (int): Number of times to run each query
        mock_mode (bool): Whether to use mock mode for progress tracking
        output_file (str, optional): Path to output CSV file
        
    Returns:
        bool: True if all queries executed successfully, False otherwise
    """
    # Check if the path is a directory or a file
    is_directory = os.path.isdir(sql_path)
    
    if is_directory:
        print(f"Running benchmark on all .sql files in directory: {sql_path}")
        # Get all .sql files in the directory
        sql_files = [os.path.join(sql_path, f) for f in os.listdir(sql_path) 
                     if f.endswith('.sql') and os.path.isfile(os.path.join(sql_path, f))]
        
        if not sql_files:
            print(f"No .sql files found in directory: {sql_path}")
            return False
            
        # Sort SQL files by filename to ensure consistent execution order
        sql_files.sort()
            
        print(f"Found {len(sql_files)} SQL files")
        print(f"Each SQL statement will be executed {times} time(s)")
        print("Files will be processed in alphabetical order by filename")
        print("Multiple SQL statements in a single file (separated by semicolons) will be executed individually")
        print()
        
        # Display the files that will be processed
        for i, sql_file in enumerate(sql_files, 1):
            filename = os.path.basename(sql_file)
            print(f"  {i}. {filename}")
        print()
        
        # Read all SQL files, each containing a single SQL query
        queries = []
        for sql_file in sql_files:
            try:
                with open(sql_file, 'r') as f:
                    sql_content = f.read().strip()
                    if sql_content:
                        # Split the file content into individual queries
                        sql_queries = [q.strip() for q in sql_content.split(';') if q.strip()]
                        filename = os.path.basename(sql_file)
                        
                        if len(sql_queries) > 1:
                            # Multiple SQL statements in the file
                            for i, query in enumerate(sql_queries):
                                queries.append((query, f"{filename}:{i+1}"))
                        else:
                            # Single SQL statement in the file
                            queries.append((sql_content, filename))
            except Exception as e:
                print(f"Error reading file {sql_file}: {e}")
    else:
        # Traditional single file with multiple queries
        if not os.path.exists(sql_path):
            print(f"Error: File not found: {sql_path}")
            return False
            
        print(f"Running benchmark using SQL file: {sql_path}")
        print(f"Each query will be executed {times} time(s)")
        print()
        
        # Read the file content
        try:
            with open(sql_path, 'r') as f:
                sql_content = f.read()
        except Exception as e:
            print(f"Error reading file: {e}")
            return False
            
        # Split the file into individual queries
        sql_queries = [q.strip() for q in sql_content.split(';') if q.strip()]
        
        if not sql_queries:
            print("No valid SQL queries found in the file.")
            return False
            
        # Store queries with a default filename (for consistent handling)
        filename = os.path.basename(sql_path)
        queries = [(q, f"{filename}:{i+1}") for i, q in enumerate(sql_queries)]
    
    if not queries:
        print("No valid SQL queries found.")
        return False
    
    console = Console()
    
    # Set up signal handler for Ctrl+C (SIGINT)
    original_handler = signal.getsignal(signal.SIGINT)

    def sigint_handler(sig, frame):
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)
        # Cancel any running query
        connection.cancel_query()
        print("\nBenchmark cancelled by user")
        # Raise KeyboardInterrupt to be caught by outer try/except
        raise KeyboardInterrupt()

    # Set custom handler
    signal.signal(signal.SIGINT, sigint_handler)
    
    # Store execution times for each query and run
    benchmark_results = []
    total_start_time = time.time()
    
    try:
        # Process each query
        for i, (query, source) in enumerate(queries, 1):
            # Skip special commands like USE, SWITCH but execute them once
            if query.lower().startswith(('use ', 'switch ')):
                # Execute the command once to change context
                print(f"Executing setup command: {query}")
                _, _ = connection.execute_query(query)
                print("Skipping benchmark for setup command")
                continue
                
            # Store results for this query
            query_results = {
                'query_num': i,
                'query_text': query,
                'query_source': source,
                'times': []
            }
            
            print(f"Benchmarking Query #{i} from {source}... ", end='', flush=True)
            
            for run in range(1, times + 1):
                try:
                    # Execute query and measure time - no need for query_id or progress tracking in benchmark mode
                    start_time = time.time()
                    column_names, results = connection.execute_query(query, set_query_id=False)
                    end_time = time.time()
                    
                    # Calculate execution time
                    execution_time = end_time - start_time
                    
                    # Store result with a placeholder query_id (not needed for benchmark)
                    query_results['times'].append({
                        'run': run,
                        'time': execution_time,
                        'query_id': 'benchmark_run'  # Placeholder - not actually used
                    })
                    
                    # Print a dot to indicate progress
                    print(".", end='', flush=True)
                    
                except Exception as e:
                    print(f"\nError during run #{run}: {e}")
                    
                    # Try to reconnect if connection was lost
                    if not connection._check_connection():
                        try:
                            print("Attempting to reconnect...")
                            if not connection.reconnect():
                                print("Failed to reconnect")
                                return False
                            print("Reconnection successful")
                        except Exception as reconnect_error:
                            print(f"Failed to reconnect: {reconnect_error}")
                            return False
            
            print(" Done")
            benchmark_results.append(query_results)
            
        total_end_time = time.time()
        total_runtime = total_end_time - total_start_time
        
        # Print final benchmark results
        print("\n=== BENCHMARK RESULTS ===\n")
        
        # Store run times by run number for later statistics
        run_times_by_number = [[] for _ in range(times)]
        
        # Prepare data for the consolidated table
        for result in benchmark_results:
            times_list = [t['time'] for t in result['times']]
            
            # Add times to run_times_by_number for summary statistics
            for run_idx, run_time in enumerate(times_list):
                if run_idx < len(run_times_by_number):
                    run_times_by_number[run_idx].append(run_time)
        
        # 1. Create a consolidated table with execution times and statistics
        table = Table(title="Query Execution Times (seconds)", show_header=True, header_style="bold magenta")
        table.add_column("No.", style="dim", justify="right")
        table.add_column("Query #", style="dim")
        table.add_column("Source", style="dim")
        
        # Add columns for each run
        for run in range(1, times + 1):
            table.add_column(f"Run {run}")
        
        # Add statistics columns    
        table.add_column("Min", style="green")
        table.add_column("Max", style="red")
        table.add_column("Avg", style="blue")
        
        # Add rows for each query (preserving the original order)
        for idx, result in enumerate(benchmark_results, 1):
            query_num = result['query_num']
            query_source = result['query_source']
            times_list = [t['time'] for t in result['times']]
            
            # Calculate statistics
            min_time = min(times_list) if times_list else 0
            max_time = max(times_list) if times_list else 0
            avg_time = sum(times_list) / len(times_list) if times_list else 0
            
            # Format row data
            row = [str(idx), f"Query {query_num}", query_source]
            
            # Add times for each run
            for run in range(times):
                if run < len(times_list):
                    run_time = times_list[run]
                    row.append(f"{run_time:.4f}")
                else:
                    row.append("N/A")
            
            # Add statistics
            row.append(f"{min_time:.4f}")
            row.append(f"{max_time:.4f}")
            row.append(f"{avg_time:.4f}")
            
            table.add_row(*row)
        
        # Calculate summary statistics for each run
        summary_row = ["", "Average", ""]
        for run_times in run_times_by_number:
            if run_times:
                avg_for_run = sum(run_times) / len(run_times)
                summary_row.append(f"{avg_for_run:.4f}")
            else:
                summary_row.append("N/A")
        
        # Add placeholders for min/max/avg columns in the summary row
        summary_row.extend(["", "", ""])
        table.add_row(*summary_row, style="bold")
        
        # Add P50, P95 percentile row
        p50_row = ["", "P50", ""]
        p95_row = ["", "P95", ""]
        
        for run_times in run_times_by_number:
            if run_times:
                # Sort for percentile calculation
                sorted_times = sorted(run_times)
                p50_idx = int(len(sorted_times) * 0.5)
                p95_idx = int(len(sorted_times) * 0.95)
                
                # Get the values (or last value if index out of range)
                p50 = sorted_times[p50_idx] if p50_idx < len(sorted_times) else sorted_times[-1]
                p95 = sorted_times[p95_idx] if p95_idx < len(sorted_times) else sorted_times[-1]
                
                p50_row.append(f"{p50:.4f}")
                p95_row.append(f"{p95:.4f}")
            else:
                p50_row.append("N/A")
                p95_row.append("N/A")
                
        # Add placeholders for min/max/avg columns
        p50_row.extend(["", "", ""])
        p95_row.extend(["", "", ""])
        
        table.add_row(*p50_row, style="bold yellow")
        table.add_row(*p95_row, style="bold red")
        
        # Print the consolidated table
        console.print(table)
        
        # 2. Create a simplified table for overall statistics
        stats_table = Table(title="Overall Statistics", show_header=True, header_style="bold magenta")
        stats_table.add_column("Metric", style="dim")
        stats_table.add_column("Value")
        
        # Add only the required statistics
        stats_table.add_row("Total Runtime", f"{total_runtime:.2f} seconds")
        stats_table.add_row("Number of Queries", str(len(benchmark_results)))
        stats_table.add_row("Total Executions", str(len(benchmark_results) * times))
        
        # Print the statistics table
        console.print(stats_table)
        
        # 3. Print the SQL queries for reference
        queries_table = Table(title="SQL Queries", show_header=True, header_style="bold magenta")
        queries_table.add_column("No.", style="dim", justify="right")
        queries_table.add_column("Query #", style="dim")
        queries_table.add_column("Source", style="dim")  
        queries_table.add_column("SQL")
        
        for idx, result in enumerate(benchmark_results, 1):
            # Truncate long queries for display
            query_text = result['query_text']
            if len(query_text) > 100:
                query_text = query_text[:97] + "..."
            queries_table.add_row(
                str(idx),
                f"Query {result['query_num']}", 
                result['query_source'], 
                query_text
            )
        
        console.print(queries_table)
        
        # Export results to CSV if output_file is provided
        if output_file:
            # Create a dictionary of overall statistics for CSV export
            stats_dict = {
                "Total Runtime": f"{total_runtime:.2f} seconds",
                "Number of Queries": str(len(benchmark_results)),
                "Total Executions": str(len(benchmark_results) * times)
            }
            
            # Export to CSV
            if export_benchmark_results_to_csv(benchmark_results, run_times_by_number, stats_dict, output_file):
                print(f"\nBenchmark results exported to: {output_file}")
            else:
                print(f"\nFailed to export benchmark results to: {output_file}")
        
        return True
    finally:
        # Restore original handler
        signal.signal(signal.SIGINT, original_handler)


def export_query_results_to_csv(column_names, results, output_file, append=False):
    """Export query results to a CSV file.
    
    Args:
        column_names (list): List of column names
        results (list): List of dictionaries containing results
        output_file (str): Path to the output CSV file
        append (bool): Whether to append to an existing file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if the file exists when appending
        file_exists = os.path.exists(output_file) if append else False
        
        # Open in append mode if requested and file exists
        mode = 'a' if append and file_exists else 'w'
        
        with open(output_file, mode, newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Add a separator if appending
            if append and file_exists:
                writer.writerow([])  # Empty line as separator
                writer.writerow([f"# SQL Query Results"])
            
            # Write header
            writer.writerow(column_names)
            
            # Write rows
            for row in results:
                writer.writerow([row.get(col, "") for col in column_names])
                
        return True
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return False
        
        
def export_benchmark_results_to_csv(benchmark_results, run_times_by_number, stats, output_file):
    """Export benchmark results to a CSV file.
    
    This exports the Query Execution Times table and the Overall Statistics table.
    
    Args:
        benchmark_results (list): List of query results
        run_times_by_number (list): List of run times grouped by run number
        stats (dict): Dictionary containing overall statistics
        output_file (str): Path to the output CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write Query Execution Times table
            writer.writerow(["# Query Execution Times (seconds)"])
            
            # Determine number of runs
            times = len(run_times_by_number)
            
            # Write header row
            header = ["No.", "Query #", "Source"]
            for run in range(1, times + 1):
                header.append(f"Run {run}")
            header.extend(["Min", "Max", "Avg"])
            writer.writerow(header)
            
            # Write data rows
            for idx, result in enumerate(benchmark_results, 1):
                query_num = result['query_num']
                query_source = result['query_source']
                times_list = [t['time'] for t in result['times']]
                
                # Calculate statistics
                min_time = min(times_list) if times_list else 0
                max_time = max(times_list) if times_list else 0
                avg_time = sum(times_list) / len(times_list) if times_list else 0
                
                # Format row data
                row = [idx, f"Query {query_num}", query_source]
                
                # Add times for each run
                for run in range(times):
                    if run < len(times_list):
                        row.append(f"{times_list[run]:.4f}")
                    else:
                        row.append("N/A")
                
                # Add statistics
                row.append(f"{min_time:.4f}")
                row.append(f"{max_time:.4f}")
                row.append(f"{avg_time:.4f}")
                
                writer.writerow(row)
            
            # Add summary row
            summary_row = ["", "Average", ""]
            for run_times in run_times_by_number:
                if run_times:
                    avg_for_run = sum(run_times) / len(run_times)
                    summary_row.append(f"{avg_for_run:.4f}")
                else:
                    summary_row.append("N/A")
            
            # Add placeholders for min/max/avg columns
            summary_row.extend(["", "", ""])
            writer.writerow(summary_row)
            
            # Add empty row as separator
            writer.writerow([])
            
            # Write Overall Statistics table
            writer.writerow(["# Overall Statistics"])
            writer.writerow(["Metric", "Value"])
            
            # Write individual statistics
            for key, value in stats.items():
                writer.writerow([key, value])
                
        return True
    except Exception as e:
        print(f"Error exporting benchmark results to CSV: {e}")
        return False


@click.command()
@click.option("--host", default="localhost", help="Apache Doris host")
@click.option("--port", default=9030, type=int, help="Apache Doris MySQL port")
@click.option("--http_port", default=None, type=int, help="Apache Doris HTTP port for progress tracking (auto-detected if not specified)")
@click.option("--user", default="root", help="Username")
@click.option("--password", default="", help="Password")
@click.option("--database", default=None, help="Default database")
@click.option("--execute", "-e", help="Execute query and exit")
@click.option("--file", "-f", help="Execute queries from file and exit")
@click.option("--benchmark", help="Run benchmark on SQL queries from a file or all .sql files in a directory")
@click.option("--times", type=int, default=1, help="Number of times to run each query in benchmark mode")
@click.option("--mock", is_flag=True, help="Enable mock mode for progress tracking")
@click.option("--output", help="Output results to a CSV file (e.g., res.csv)")
def main(host, port, http_port, user, password, database, execute, file, benchmark, times, mock, output):
    """Apache Doris Command Line Interface with query progress reporting."""
    
    # Connect to Apache Doris
    connection = DorisConnection(host, port, user, password, database)
    if not connection.connect():
        sys.exit(1)
    
    # Set HTTP port if provided by user
    if http_port is not None:
        connection.http_port = http_port
    
    # Benchmark mode
    if benchmark:
        try:
            success = run_benchmark(connection, benchmark, times, mock, output)
            if not success:
                sys.exit(1)
        except KeyboardInterrupt:
            print("\nBenchmark cancelled by user.")
        except EOFError:
            print("\nReceived exit signal (Ctrl+D)")
        finally:
            connection.close()
        return
    
    # Non-interactive mode: execute query from command line
    if execute:
        try:
            # 检查是否是多语句查询
            is_multi_statement = False
            in_quotes = False
            quote_char = None
            stmt_count = 0
            
            for char in execute:
                if char in ["'", '"'] and (not in_quotes or char == quote_char):
                    in_quotes = not in_quotes
                    if in_quotes:
                        quote_char = char
                elif char == ';' and not in_quotes:
                    stmt_count += 1
                    if stmt_count > 1:
                        is_multi_statement = True
                        break
            
            # 执行查询
            column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress(
                connection, execute, mock, output
            )
            
            # 只有在单语句查询时才需要在这里显示结果
            # 多语句查询的结果已经在 handle_query_with_progress 中显示了
            if column_names and results and not is_multi_statement:
                display_results(column_names, results, query_id, runtime, output)
        except KeyboardInterrupt:
            print("Query cancelled by user.")
        except EOFError:
            print("\nReceived exit signal (Ctrl+D)")
            # Stop any active progress tracking
            if progress_tracker and progress_tracker.tracking:
                progress_tracker.stop_tracking()
            # Cancel any running query
            connection.cancel_query()
        finally:
            connection.close()
        return
        
    # Non-interactive mode: execute queries from file
    if file:
        # 从文件执行总是按多语句处理，结果会在handle_query_with_progress中显示
        try:
            column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress(
                connection, f"source {file}", mock, output
            )
            # 结果已经在处理过程中显示和导出了
        except KeyboardInterrupt:
            print("Query cancelled by user.")
        except EOFError:
            print("\nReceived exit signal (Ctrl+D)")
            # Stop any active progress tracking
            if 'progress_tracker' in locals() and progress_tracker and progress_tracker.tracking:
                progress_tracker.stop_tracking()
            # Cancel any running query
            connection.cancel_query()
        finally:
            connection.close()
        return
    
    # Interactive mode
    history = FileHistory(get_history_file())
    session = PromptSession(
        history=history,
        auto_suggest=AutoSuggestFromHistory(),
        lexer=PygmentsLexer(SqlLexer),
        style=Style.from_dict({
            'prompt': '#00aa00 bold',
        }),
    )
    
    # Print welcome message
    print("""
    _                     _            ____             _     
   / \\   _ __   __ _  ___| |__   ___  |  _ \\  ___  _ __(_)___ 
  / _ \\ | '_ \\ / _` |/ __| '_ \\ / _ \\ | | | |/ _ \\| '__| / __|
 / ___ \\| |_) | (_| | (__| | | |  __/ | |_| | (_) | |  | \\__ \\
/_/   \\_\\ .__/ \\__,_|\\___|_| |_|\\___| |____/ \\___/|_|  |_|___/
        |_|                                                   
    """)
    # Get version from connection
    doris_version = connection.version or "Apache Doris"
    print(f"Server version: {doris_version}\n")
    print(f"Type 'help' or '\\h' for help, '\\q' to quit.")
    print(f"Press Ctrl+D to cancel any running query and exit")
    print(f"Use semicolon (;) followed by Enter to execute a query")
    
    # Variable to track active query progress tracker
    active_progress_tracker = None
    
    # Main loop
    try:
        while True:
            try:
                # Get current database and catalog for prompt
                current_db = connection.get_current_database() or "(none)"
                current_catalog = connection.get_current_catalog() or "internal"
                
                # Display prompt with catalog and database name
                prompt_text = f"doris-cmd [{current_catalog}][{current_db}]> "
                
                # Collect multi-line SQL until semicolon + newline is encountered
                query_buffer = []
                continue_collecting = True
                
                while continue_collecting:
                    # Get user input, use main prompt for first input, continuation prompt otherwise
                    if not query_buffer:
                        try:
                            line = session.prompt(prompt_text)
                        except Exception as e:
                            raise
                    else:
                        try:
                            line = session.prompt('          -> ')
                        except Exception as e:
                            raise
                    
                    # Special command handling - only check in the first line
                    if not query_buffer:
                        # Check exit command
                        if line.strip().lower() in ('exit', 'quit', '\\q'):
                            return
                        
                        # Check help command
                        if line.strip().lower() in ('help', '\\h'):
                            print_help()
                            break
                            
                        # Check special command shortcuts
                        if line.strip().lower() in ('\\d', 'show databases'):
                            line = "SHOW DATABASES;"
                        elif line.strip().lower() in ('\\t', 'show tables'):
                            line = "SHOW TABLES;"
                        
                        # Automatically add semicolon to use and source commands
                        first_word = line.strip().split()[0].lower() if line.strip() else ""
                        if first_word in ('use', 'source', 'switch') and not line.rstrip().endswith(';'):
                            line = f"{line.rstrip()};"
                    
                    # Add current line to buffer
                    query_buffer.append(line)
                    
                    # Check if the line ends with a semicolon outside of quotes
                    # This is a simple approach - for a real parser we'd need a more robust method
                    # to handle quoted strings and comments
                    if line.rstrip().endswith(';'):
                        continue_collecting = False
                
                # If no valid SQL was collected, continue the loop
                if not query_buffer:
                    continue
                
                # Merge all lines to create the complete query
                query = '\n'.join(query_buffer)
                
                # Process empty query
                query = query.strip()
                if not query:
                    continue
                
                # Process query
                try:
                    column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress(
                        connection, query, mock, output
                    )
                    
                    # Save progress tracker reference
                    active_progress_tracker = progress_tracker
                    
                    # 显示查询结果 - 但对于多语句，结果已在 handle_query_with_progress 中显示了
                    # 检查是否是多语句查询（通过分号判断，但需要排除引号内的分号）
                    is_multi_statement = False
                    in_quotes = False
                    quote_char = None
                    stmt_count = 0
                    
                    for char in query:
                        if char in ["'", '"'] and (not in_quotes or char == quote_char):
                            in_quotes = not in_quotes
                            if in_quotes:
                                quote_char = char
                        elif char == ';' and not in_quotes:
                            stmt_count += 1
                            if stmt_count > 1:
                                is_multi_statement = True
                                break
                    
                    # 仅对单语句查询显示结果，多语句结果已在执行过程中显示
                    if column_names and results and not is_multi_statement:
                        display_results(column_names, results, query_id, runtime, output)
                except Exception as e:
                    print(f"Error: {e}")
                    # Try to reconnect to clean up connection state after an error
                    try:
                        print("Attempting to reconnect to clean up connection state...")
                        success = connection.reconnect()
                        if success:
                            print("Reconnection successful.")
                        else:
                            print("Failed to reconnect. Some features may not work correctly.")
                    except Exception as reconnect_error:
                        print(f"Failed to reconnect: {reconnect_error}")
                        print("You may need to restart doris-cmd if you experience further issues.")
                
            except KeyboardInterrupt:
                # Handle Ctrl+C
                print("\nQuery cancelled")
                continue
            except EOFError:
                # Handle Ctrl+D (EOF)
                print("\nReceived exit signal (Ctrl+D)")
                # Stop any active progress tracking
                if active_progress_tracker and active_progress_tracker.tracking:
                    active_progress_tracker.stop_tracking()
                # Cancel any running query
                connection.cancel_query()
                # Break out of the loop to exit
                break
            except Exception as e:
                print(f"Error: {e}")
    finally:
        # Close connection
        connection.close()


def print_help():
    """Print help information."""
    help_text = """
    General Commands:
      \\q, exit, quit                   Exit doris-cmd
      \\h, help                         Show this help
      
    Query Commands:
      \\d, show databases              Show all databases
      \\t, show tables                 Show tables in current database
      use <database>                   Switch to a different database
      switch <catalog>                 Switch to a different catalog
      source <file>                    Execute SQL statements from a file
      
    Any other input will be treated as a SQL query.
    
    Special Features:
      - Press Ctrl+C to cancel a running query
      - Press Ctrl+D to cancel any running query and exit
      - Query progress is displayed in real-time
      - Each query gets a new query ID for tracking
      - Query ID and runtime are displayed with results
      - HTTP port is auto-detected from Doris FE
      
    Command Line Options:
      --benchmark <path>               Run benchmark on SQL queries from a file or directory:
                                       - If path is a file: execute all SQL statements in the file
                                       - If path is a directory: execute all SQL statements 
                                         from each .sql file (supports multiple statements per file)
      --times N                        Execute each query N times in benchmark mode (default: 1)
      --file, -f <file>                Execute queries from file and exit
      --execute, -e <query>            Execute a single query and exit
      --output <file.csv>              Export results to a CSV file:
                                       - In benchmark mode: exports execution times and statistics only
                                       - With --file or --execute: exports query results
                                         (for multiple SQL statements, all results are included)
    """
    print(help_text)


if __name__ == "__main__":
    main() 