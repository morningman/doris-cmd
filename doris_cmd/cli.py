"""
Command line interface for Apache Doris.
"""
import os
import sys
import signal
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

from doris_cmd.connection import DorisConnection
from doris_cmd.progress import ProgressTracker


def get_history_file():
    """Get history file path."""
    home_dir = os.path.expanduser("~")
    history_dir = os.path.join(home_dir, ".doris_cmd")
    os.makedirs(history_dir, exist_ok=True)
    return os.path.join(history_dir, "history")


def display_results(column_names, results, query_id=None, runtime=None):
    """Display query results in tabular format using rich.
    
    Args:
        column_names (list): List of column names
        results (list): List of dictionaries containing results
        query_id (str, optional): The query ID associated with this result
        runtime (float, optional): Query runtime in seconds
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


def handle_query_with_progress(connection, query, mock_mode=False):
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
            for query in queries:
                print(f"Executing query: {query}")
                # Each query will get a new query_id
                column_names, results, query_id, progress_tracker, runtime = handle_query_with_progress_single(
                    connection, query, mock_mode
                )
                
                # Display results for each query
                if column_names and results:
                    display_results(column_names, results, query_id, runtime)
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
                        display_results(column_names, rows, query_id, runtime)
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


@click.command()
@click.option("--host", default="localhost", help="Apache Doris host")
@click.option("--port", default=9030, type=int, help="Apache Doris MySQL port")
@click.option("--http_port", default=None, type=int, help="Apache Doris HTTP port for progress tracking (auto-detected if not specified)")
@click.option("--user", default="root", help="Username")
@click.option("--password", default="", help="Password")
@click.option("--database", default=None, help="Default database")
@click.option("--execute", "-e", help="Execute query and exit")
@click.option("--file", "-f", help="Execute queries from file and exit")
@click.option("--mock", is_flag=True, help="Enable mock mode for progress tracking")
def main(host, port, http_port, user, password, database, execute, file, mock):
    """Apache Doris Command Line Interface with query progress reporting."""
    
    # Connect to Apache Doris
    connection = DorisConnection(host, port, user, password, database)
    if not connection.connect():
        sys.exit(1)
    
    # Set HTTP port if provided by user
    if http_port is not None:
        connection.http_port = http_port
    
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
                connection, execute, mock
            )
            
            # 只有在单语句查询时才需要在这里显示结果
            # 多语句查询的结果已经在 handle_query_with_progress 中显示了
            if column_names and results and not is_multi_statement:
                display_results(column_names, results, query_id, runtime)
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
            handle_query_with_progress(connection, f"source {file}", mock)
            # 不需要在这里显示结果，因为结果已经在执行过程中显示了
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
                        connection, query, mock
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
                        display_results(column_names, results, query_id, runtime)
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
    """
    print(help_text)


if __name__ == "__main__":
    main() 