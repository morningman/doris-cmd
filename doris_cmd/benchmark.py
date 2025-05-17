#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Benchmark utilities for doris-cmd.
"""
import os
import time
import signal
import statistics
from rich.console import Console
from rich.table import Table
from rich.style import Style as RichStyle


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
    # Import here to avoid circular imports
    from doris_cmd.export import export_benchmark_results_to_csv
    
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
                    column_names, results = connection.execute_query(query, set_trace_id=False)
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