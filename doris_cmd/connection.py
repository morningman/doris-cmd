#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Connection module for Apache Doris CLI.
"""
import uuid
import pymysql
import requests
from pymysql.cursors import DictCursor
import time


class DorisConnection:
    """Connection manager for Apache Doris."""

    def __init__(self, host, port, user, password, database=None):
        """Initialize a connection to Apache Doris.
        
        Args:
            host (str): The host of Apache Doris server
            port (int): The port of Apache Doris server
            user (str): Username for authentication
            password (str): Password for authentication
            database (str, optional): Default database to use
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.trace_id = None
        self.http_port = None
        self.version = None
        self.connection_id = None
        self._last_known_database = None
        self._last_known_catalog = None
        
    def connect(self):
        """Establish connection to Apache Doris."""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                cursorclass=DictCursor
            )
            print("Connected to Apache Doris at {}:{}".format(self.host, self.port))
            
            # Generate a new trace ID (but don't set it here)
            self.trace_id = self._generate_trace_id()
            
            # Get Doris version (but don't print it here - it will be printed by CLI)
            version = self._get_doris_version()
            self.version = version
            
            # Get the HTTP port
            self.http_port = self._get_http_port()
            
            # Get the connection ID
            self._get_connection_id()
            
            return True
        except Exception as e:
            print("Connection failed: {}".format(e))
            return False
        
    def _generate_trace_id(self):
        """Generate a unique trace ID."""
        return "doris_cmd_{}".format(uuid.uuid4().hex)
    
    def _set_trace_id(self):
        """Set a trace ID for the current session using session_context.
        
        This sets the trace_id in session_context which Doris uses to track queries.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            return False
        
        # Generate a new trace ID for this query
        self.trace_id = self._generate_trace_id()
        
        cursor = None
        try:
            # Check if connection is alive first
            if not self._check_connection():
                # Try to reconnect
                if not self.reconnect(preserve_state=True):
                    return False
            
            cursor = self.connection.cursor()
            
            # Set the trace ID using session_context
            session_sql = "SET session_context = 'trace_id:{}'".format(self.trace_id)
            cursor.execute(session_sql)
            
            return True
        except Exception as e:
            print("Failed to set trace ID: {}".format(e))
            
            # Try to clean up the connection and reconnect
            self._cleanup_after_error()
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    # Ignore errors when closing cursor
                    pass
                    
    def _check_connection(self, retry_count=3):
        """Check if the connection is still alive with retry logic"""
        if not self.connection:
            return False
            
        for attempt in range(retry_count):
            try:
                # Use ping() as the most reliable connection check
                self.connection.ping()
                return True
            except Exception as e:
                if attempt == retry_count - 1:  # Last attempt
                    return False
                time.sleep(0.1)  # Short delay between retries
        return False
            
    def _cleanup_after_error(self):
        """Clean up connection state after an error occurs."""
        try:
            # Only cleanup and reconnect if the connection appears to be dead
            if self.connection and not self._check_connection():
                try:
                    self.connection.close()
                except Exception:
                    pass
                    
                # Reset connection
                self.connection = None
                
                # Try to reconnect immediately
                self.reconnect(preserve_state=True)
        except Exception:
            # If cleanup fails, at least make sure connection is None
            self.connection = None
    
    def _get_http_port(self):
        """Get the HTTP port of the Doris FE server.
        
        This method executes "SHOW FRONTENDS" and finds the row where
        "CurrentConnected" is "Yes", then returns the "HttpPort" value.
        
        Returns:
            int: The HTTP port, or None if not found
        """
        if not self.connection:
            return None
            
        cursor = self.connection.cursor()
        try:
            # Set a new query ID for this operation
            self._set_trace_id()
            
            # Execute SHOW FRONTENDS
            cursor.execute("SHOW FRONTENDS")
            results = cursor.fetchall()
            
            # Find the row where CurrentConnected is Yes
            for row in results:
                if 'CurrentConnected' in row and row['CurrentConnected'].lower() == 'yes':
                    if 'HttpPort' in row:
                        try:
                            return int(row['HttpPort'])
                        except (ValueError, TypeError):
                            print(f"Invalid HTTP port value: {row['HttpPort']}")
                            return None
            
            # If we got here, we didn't find a connected frontend
            print("Warning: Could not find a connected Doris FE. HTTP API features may not work.")
            return None
        except Exception as e:
            print(f"Failed to get HTTP port: {e}")
            return None
        finally:
            cursor.close()
    
    def get_http_port(self):
        """Get the HTTP port of the Doris FE server.
        
        If the HTTP port is not already known, this method will try to get it.
        
        Returns:
            int: The HTTP port, or None if not found
        """
        if self.http_port is None:
            self.http_port = self._get_http_port()
        return self.http_port
    
    def execute_query(self, sql, set_trace_id=True):
        """Execute a SQL query.
        
        Args:
            sql (str): SQL query to execute
            set_trace_id (bool): Whether to set a new trace ID before executing
            
        Returns:
            tuple: (column_names, results, query_id)
        """
        if not self.connection:
            print("Not connected to Apache Doris")
            # Try to reconnect automatically
            if not self.reconnect(preserve_state=True):
                return None, None, None
        
        # Check connection health first
        if not self._check_connection():
            print("Connection lost. Attempting to reconnect...")
            if not self.reconnect(preserve_state=True):
                print("Reconnection failed")
                return None, None, None
        
        # Set a new query ID before executing the query if requested
        if set_trace_id:
            if not self._set_trace_id():
                print("Warning: Failed to set trace ID. Query tracking may not work properly.")
        
        cursor = None
        query_id = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            
            # Get column names and results
            column_names = None
            results = None
            if cursor.description:
                column_names = [col[0] for col in cursor.description]
                results = cursor.fetchall()
            
            # Get the last query ID using the same connection
            try:
                # Create a new cursor to ensure we don't interfere with any results
                id_cursor = self.connection.cursor()
                id_cursor.execute("SELECT last_query_id()")
                id_result = id_cursor.fetchone()
                if id_result and 'last_query_id()' in id_result:
                    query_id = id_result['last_query_id()']
                id_cursor.close()
            except Exception as e:
                print(f"Failed to get query ID: {e}")
            
            return column_names, results, query_id
        except pymysql.Error as e:
            error_msg = f"Query execution failed: {e}"
            print(error_msg)
            
            # Check what type of error this is
            if isinstance(e, (pymysql.OperationalError, pymysql.InterfaceError)):
                # These errors typically indicate connection problems
                self._cleanup_after_error()
            
            return None, None, None
        except Exception as e:
            print(f"Unexpected error during query execution: {e}")
            
            # Special handling for KeyboardInterrupt
            if isinstance(e, KeyboardInterrupt):
                # Don't check connection state immediately after KeyboardInterrupt
                # The connection might be in an unstable state due to query cancellation
                return None, None, None
            
            # Check if connection is still alive before cleanup
            if not self._check_connection(retry_count=1):
                self._cleanup_after_error()
            return None, None, None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    # Ignore errors when closing cursor
                    pass
    
    def execute_file(self, file_path, set_trace_id=True):
        """Execute SQL from a file.
        
        Args:
            file_path (str): Path to the SQL file
            set_trace_id (bool): Whether to set a new trace ID for each query
            
        Returns:
            tuple: (column_names, results, query_id) of the last query in the file
        """
        if not self.connection:
            print("Not connected to Apache Doris")
            return None, None, None
            
        try:
            with open(file_path, 'r') as f:
                sql = f.read()
                
            # Split the file into individual queries (naive implementation)
            # In a production environment, a proper SQL parser would be better
            queries = [q.strip() for q in sql.split(';') if q.strip()]
            
            column_names, results, query_id = None, None, None
            for query in queries:
                print(f"Executing query: {query}")
                # Each query gets a new trace ID if set_trace_id is True
                column_names, results, query_id = self.execute_query(query, set_trace_id=set_trace_id)
                
            return column_names, results, query_id
        except Exception as e:
            print(f"Failed to execute SQL file: {e}")
            return None, None, None
    
    def get_current_database(self, silent_on_error=False):
        """Get the current database name.
        
        Args:
            silent_on_error (bool): If True, don't print error messages
        
        Returns:
            str: Current database name or None
        """
        if not self.connection:
            # Return cached value if connection is down
            return getattr(self, '_last_known_database', None)
            
        # Check if connection is still alive before proceeding
        if not self._check_connection():
            # Return cached value if connection check fails
            return getattr(self, '_last_known_database', None)
            
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT DATABASE()")
            result = cursor.fetchone()
            
            db_name = result['DATABASE()'] if result else None
            
            # Cache the last known database
            if db_name:
                self._last_known_database = db_name
            return db_name
        except Exception as e:
            if not silent_on_error:
                # Only print error if not in silent mode
                pass
            # Return cached value if query fails
            return getattr(self, '_last_known_database', None)
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
    
    def get_current_catalog(self, silent_on_error=False):
        """Get the current catalog name.
        
        Args:
            silent_on_error (bool): If True, don't print error messages
        
        Returns:
            str: Current catalog name or None
        """
        if not self.connection:
            # Return cached value if connection is down
            return getattr(self, '_last_known_catalog', 'internal')
            
        # Check if connection is still alive before proceeding
        if not self._check_connection():
            # Return cached value if connection check fails
            return getattr(self, '_last_known_catalog', 'internal')
            
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW CATALOGS")
            results = cursor.fetchall()
            
            # Find the row where IsCurrent is Yes
            for row in results:
                if 'IsCurrent' in row and row['IsCurrent'].lower() == 'yes':
                    if 'CatalogName' in row:
                        catalog_name = row['CatalogName']
                        # Cache the last known catalog
                        self._last_known_catalog = catalog_name
                        return catalog_name
            
            # If we got here, we didn't find a current catalog
            return "internal"  # Default to internal catalog if not found
        except Exception as e:
            if not silent_on_error:
                # Only print error if not in silent mode
                pass
            # Return cached value if query fails, default to internal
            return getattr(self, '_last_known_catalog', 'internal')
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_current_connection_id(self):
        """Get the current connection ID.
        
        Returns:
            int: Current connection ID or None if not available
        """
        return self.connection_id
    
    def use_database(self, database):
        """Change the current database.
        
        Args:
            database (str): Database name to use
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            return False
            
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"USE {database}")
            self.database = database
            return True
        except Exception as e:
            print(f"Failed to use database {database}: {e}")
            return False
        finally:
            cursor.close()
    
    def switch_catalog(self, catalog):
        """Change the current catalog.
        
        Args:
            catalog (str): Catalog name to switch to
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            return False
            
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SWITCH {catalog}")
            return True
        except Exception as e:
            print(f"Failed to switch to catalog {catalog}: {e}")
            return False
        finally:
            cursor.close()
    
    def cancel_query(self):
        """Cancel the current running query"""
        if not self.connection_id:
            return False
        
        try:
            # Create a new connection to send KILL QUERY
            with self._create_connection() as kill_conn:
                with kill_conn.cursor() as cursor:
                    cursor.execute(f"KILL QUERY {self.connection_id}")
                    return True
        except Exception as e:
            return False
            
    def reset_trace_id(self):
        """Generate a new trace ID and set it for the current session."""
        return self._set_trace_id()
    
    def close(self):
        """Close the connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed")
            self.connection = None
    
    def reconnect(self, preserve_state=True):
        """Reconnect to the database"""
        old_catalog = None
        old_database = None
        
        # Use saved state from signal handler if available
        if preserve_state and hasattr(self, '_saved_state') and self._saved_state:
            old_catalog = self._saved_state.get('catalog')
            old_database = self._saved_state.get('database')
        elif preserve_state:
            # Fallback: try to get current state if connection is still alive, or use cached values
            try:
                if self.connection and self._check_connection():
                    old_catalog = self.get_current_catalog(silent_on_error=True)
                    old_database = self.get_current_database(silent_on_error=True)
                else:
                    # Connection is dead, use cached values
                    old_catalog = getattr(self, '_last_known_catalog', None)
                    old_database = getattr(self, '_last_known_database', None)
            except Exception:
                # If we can't get current state, use cached values
                old_catalog = getattr(self, '_last_known_catalog', None)
                old_database = getattr(self, '_last_known_database', None)
        
        try:
            self._close_connection()
            self._establish_connection()
            
            # Restore previous state if requested and we have it
            if preserve_state and old_catalog and old_catalog != 'internal':
                try:
                    print(f"[INFO] Restoring catalog: {old_catalog}")
                    
                    if self.switch_catalog(old_catalog):
                        # Update cache after successful switch
                        self._last_known_catalog = old_catalog
                        
                        if old_database:
                            print(f"[INFO] Restoring database: {old_database}")
                            
                            if self.use_database(old_database):
                                # Update cache after successful switch
                                self._last_known_database = old_database
                                print(f"[INFO] State successfully restored: {old_catalog}.{old_database}")
                            else:
                                # Database restore failed, clear cached database but keep catalog
                                print(f"[WARN] Database '{old_database}' does not exist in catalog '{old_catalog}', clearing database context")
                                self._last_known_database = None
                                
                                # Try to provide helpful information about available databases
                                try:
                                    available_dbs = self.get_available_databases()
                                    if available_dbs:
                                        print(f"[INFO] Available databases in catalog '{old_catalog}': {', '.join(available_dbs[:5])}")
                                        if len(available_dbs) > 5:
                                            print(f"[INFO] ... and {len(available_dbs) - 5} more databases")
                                    else:
                                        print(f"[INFO] No databases found in catalog '{old_catalog}' or unable to retrieve database list")
                                except Exception:
                                    # Don't let database listing errors affect reconnection
                                    pass
                                    
                                print(f"[INFO] Partial state restored: {old_catalog}.(none)")
                    else:
                        print(f"[WARN] Failed to switch to catalog '{old_catalog}', using default catalog")
                        self._last_known_catalog = 'internal'
                        self._last_known_database = None
                        print(f"[INFO] Using default state: internal.(none)")
                        
                except Exception as e:
                    print(f"[WARN] Could not restore state - catalog: {old_catalog}, database: {old_database}: {e}")
                    # Reset to safe defaults if state restoration fails completely
                    self._last_known_catalog = 'internal'
                    self._last_known_database = None
                    print(f"[INFO] Reset to default state: internal.(none)")
            
            # Clear saved state after use, but keep cached values for future use
            if hasattr(self, '_saved_state'):
                delattr(self, '_saved_state')
            
            # Update cache with final state after successful reconnection
            try:
                final_catalog = self.get_current_catalog(silent_on_error=True)
                final_database = self.get_current_database(silent_on_error=True)
                if final_catalog:
                    self._last_known_catalog = final_catalog
                if final_database:
                    self._last_known_database = final_database
            except Exception:
                # Don't let cache update errors affect the reconnection result
                pass
            
            return True
        except Exception as e:
            print(f"[ERROR] Reconnection failed: {e}")
            return False
    
    def get_persistent_state(self):
        """Get the current or last known state for persistence.
        
        This method tries to get current state first, but falls back to cached values
        if the connection is unavailable. This is specifically designed for use in
        signal handlers where connection may be unstable.
        
        Returns:
            dict: Dictionary with 'catalog' and 'database' keys
        """
        state = {'catalog': None, 'database': None}
        
        # Get cached values first
        cached_catalog = getattr(self, '_last_known_catalog', None)
        cached_database = getattr(self, '_last_known_database', None)
        
        try:
            # Try to get current state if connection is alive
            if self.connection and self._check_connection():
                catalog = self.get_current_catalog(silent_on_error=True)
                database = self.get_current_database(silent_on_error=True)
                
                # Check for Doris catalog reset corruption
                # If live state shows 'internal' but we have a different cached catalog,
                # it likely means Doris reset the session during query cancellation
                if (catalog == 'internal' and cached_catalog and cached_catalog != 'internal'):
                    # Use cached state to preserve user context
                    state['catalog'] = cached_catalog
                    state['database'] = cached_database
                else:
                    # Live state appears reliable, use it
                    state['catalog'] = catalog
                    state['database'] = database
            else:
                # Connection is dead, use cached values
                state['catalog'] = cached_catalog
                state['database'] = cached_database
        except Exception as e:
            # If all fails, use cached values
            state['catalog'] = cached_catalog
            state['database'] = cached_database
        
        return state
    
    def _create_connection(self):
        """Create a new PyMySQL connection"""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=None,  # Don't specify database during connection, we'll set it manually later
            charset='utf8mb4',
            cursorclass=DictCursor
        )
    
    def _set_connection_id(self):
        """Set the connection ID for the current connection"""
        if self.connection:
            self._get_connection_id()
    
    def _close_connection(self):
        """Close the current connection"""
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass  # Ignore errors when closing
            finally:
                self.connection = None
                self.connection_id = None
                if hasattr(self, '_connection_needs_reset'):
                    delattr(self, '_connection_needs_reset')
    
    def _establish_connection(self):
        """Establish a new connection and set up basic state"""
        self.connection = self._create_connection()
        if self.connection:
            self._set_connection_id()
            # Get version and HTTP port
            self.version = self._get_doris_version()
            self.http_port = self._get_http_port()
            return True
        return False
    
    def _get_doris_version(self):
        """Get Apache Doris version.
        
        Returns:
            str: Doris version string or None if not available
        """
        if not self.connection:
            return None
            
        cursor = self.connection.cursor()
        try:
            cursor.execute("SHOW VARIABLES LIKE 'version_comment'")
            result = cursor.fetchone()
            if result and 'Value' in result:
                return result['Value']
            return None
        except Exception as e:
            print(f"Failed to get Doris version: {e}")
            return None
        finally:
            cursor.close()
    
    def _get_connection_id(self):
        """Get the current connection ID from Doris.
        
        Returns:
            int: Current connection ID or None if not available
        """
        if not self.connection:
            return None
            
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT CONNECTION_ID()")
            result = cursor.fetchone()
            if result and 'CONNECTION_ID()' in result:
                self.connection_id = result['CONNECTION_ID()']
                return self.connection_id
            return None
        except Exception as e:
            print(f"Failed to get connection ID: {e}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass 
    
    def get_available_databases(self):
        """Get list of available databases in current catalog.
        
        Returns:
            list: List of database names, or empty list if query fails
        """
        if not self.connection:
            return []
            
        # Check if connection is still alive before proceeding
        if not self._check_connection():
            return []
            
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW DATABASES")
            results = cursor.fetchall()
            
            # Extract database names from results
            databases = []
            for row in results:
                # SHOW DATABASES returns different column names in different Doris versions
                if 'Database' in row:
                    databases.append(row['Database'])
                elif 'DatabaseName' in row:
                    databases.append(row['DatabaseName'])
                # Add other potential column names as needed
                
            return databases
        except Exception as e:
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass 