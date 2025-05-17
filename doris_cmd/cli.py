#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Command line interface for Apache Doris.
"""
import os
import sys
import signal
import time
import click
import configparser
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from pygments.lexers.sql import SqlLexer

from doris_cmd.connection import DorisConnection
from doris_cmd.progress import ProgressTracker
from doris_cmd.utils import get_history_file, print_help
from doris_cmd.display import display_results
from doris_cmd.query_handler import handle_query, handle_query_with_progress, handle_query_with_progress_single
from doris_cmd.benchmark import run_benchmark


@click.command()
@click.option("--config", help="Path to configuration file")
@click.option("--host", default=None, help="Apache Doris host")
@click.option("--port", default=None, type=int, help="Apache Doris MySQL port")
@click.option("--user", default=None, help="Username")
@click.option("--password", default=None, help="Password")
@click.option("--database", default=None, help="Default database")
@click.option("--execute", "-e", help="Execute query and exit")
@click.option("--file", "-f", help="Execute queries from file and exit")
@click.option("--benchmark", help="Run benchmark on SQL queries from a file or all .sql files in a directory")
@click.option("--times", type=int, default=1, help="Number of times to run each query in benchmark mode")
@click.option("--mock", is_flag=True, help="Enable mock mode for progress tracking")
@click.option("--output", help="Output results to a CSV file (e.g., res.csv)")
@click.option("--profile", is_flag=True, help="Enable profile mode to collect query profiles")
def main(config, host, port, user, password, database, execute, file, benchmark, times, mock, output, profile):
    """Apache Doris Command Line Interface with query progress reporting."""
    
    # Default values
    default_host = "localhost"
    default_port = 9030
    default_user = "root"
    default_password = ""
    
    # Load config file if specified
    config_params = {}
    if config:
        config_params = load_config(config)
    
    # Apply values with precedence: command line > config file > defaults
    host = host or config_params.get('host', default_host)
    port = port or int(config_params.get('port', default_port))
    user = user or config_params.get('user', default_user)
    password = password or config_params.get('password', default_password)
    database = database or config_params.get('database')
    
    # Connect to Apache Doris
    connection = DorisConnection(host, port, user, password, database)
    if not connection.connect():
        sys.exit(1)
    
    # Check for incompatible options
    if benchmark and profile:
        print("Error: --benchmark and --profile cannot be used together")
        sys.exit(1)
    
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
    
    # Set up profile mode if enabled
    if profile:
        # Enable profile by setting enable_profile=true
        try:
            connection.execute_query("SET enable_profile=true")
            print("Profile mode enabled")
        except Exception as e:
            print(f"Failed to enable profile mode: {e}")
            sys.exit(1)

    # Non-interactive mode: execute query from command line
    if execute:
        try:
            # Check if this is a multi-statement query
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
            
            # Execute query with progress or profiling
            if profile:
                from doris_cmd.query_handler import handle_query_with_profile
                column_names, results, query_id, runtime = handle_query_with_profile(
                    connection, execute, output
                )
            else:
                column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress(
                    connection, execute, mock, output
                )
            
            # Only display results here for single-statement queries
            # For multi-statement queries, results are already displayed in handler
            if column_names and results and not is_multi_statement:
                display_results(column_names, results, query_id, runtime, output)
        except KeyboardInterrupt:
            print("Query cancelled by user.")
        except EOFError:
            print("\nReceived exit signal (Ctrl+D)")
            # Cancel any running query
            connection.cancel_query()
        finally:
            connection.close()
        return
        
    # Non-interactive mode: execute queries from file
    if file:
        # File execution is always handled as multi-statement queries
        # Results are displayed in handler
        try:
            if profile:
                from doris_cmd.query_handler import handle_query_with_profile
                column_names, results, query_id, runtime = handle_query_with_profile(
                    connection, f"source {file}", output
                )
            else:
                column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress(
                    connection, f"source {file}", mock, output
                )
            # Results have already been displayed and exported during processing
        except KeyboardInterrupt:
            print("Query cancelled by user.")
        except EOFError:
            print("\nReceived exit signal (Ctrl+D)")
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
    
    if profile:
        print(f"Profile mode is enabled. Profiles will be saved to /tmp/.doris_profile/")
    
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
                    if profile:
                        from doris_cmd.query_handler import handle_query_with_profile
                        column_names, results, query_id, runtime = handle_query_with_profile(
                            connection, query, output
                        )
                    else:
                        column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress(
                            connection, query, mock, output
                        )
                    
                    # Check if this is a multi-statement query (by semicolons outside of quotes)
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
                    
                    # Only display results for single-statement queries
                    # Multi-statement results are already displayed during processing
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
                # Cancel any running query
                connection.cancel_query()
                # Break out of the loop to exit
                break
            except Exception as e:
                print(f"Error: {e}")
    finally:
        # Close connection
        connection.close()


def load_config(config_path):
    """Load configuration from a file.
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        dict: Dictionary containing configuration parameters
    """
    config_params = {}
    
    try:
        if not os.path.exists(config_path):
            print(f"Config file not found: {config_path}")
            return config_params
            
        config = configparser.ConfigParser()
        config.read(config_path)
        
        if 'doris' in config:
            doris_section = config['doris']
            
            if 'host' in doris_section:
                config_params['host'] = doris_section['host']
            if 'port' in doris_section:
                config_params['port'] = doris_section['port']
            if 'user' in doris_section:
                config_params['user'] = doris_section['user']
            if 'password' in doris_section:
                config_params['password'] = doris_section['password']
            if 'database' in doris_section:
                config_params['database'] = doris_section['database']
    except Exception as e:
        print(f"Error reading config file: {e}")
    
    return config_params


if __name__ == "__main__":
    main() 