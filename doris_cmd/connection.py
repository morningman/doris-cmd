#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Connection module for Apache Doris CLI.
"""
import uuid
import pymysql
import requests
from pymysql.cursors import DictCursor


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
                if not self.reconnect():
                    return False
            
            cursor = self.connection.cursor()
            # Set the trace ID using session_context
            cursor.execute("SET session_context = 'trace_id:{}'".format(self.trace_id))
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
                    
    def _check_connection(self):
        """Check if the connection is still alive.
        
        Returns:
            bool: True if connection is alive, False otherwise
        """
        if not self.connection:
            return False
            
        try:
            # Try a simple query to check connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            # Connection is dead
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
                self.reconnect()
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
            if not self.reconnect():
                return None, None, None
        
        # Check connection health first
        if not self._check_connection():
            print("Connection lost. Attempting to reconnect...")
            if not self.reconnect():
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
            
            # Only try to clean up connection state if it appears to be a connection issue
            if isinstance(e, (pymysql.OperationalError, pymysql.InterfaceError)):
                # These errors typically indicate connection problems
                self._cleanup_after_error()
            
            return None, None, None
        except Exception as e:
            print(f"Unexpected error during query execution: {e}")
            # Check if connection is still alive before cleanup
            if not self._check_connection():
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
    
    def get_current_database(self):
        """Get the current database name.
        
        Returns:
            str: Current database name or None
        """
        if not self.connection:
            return None
            
        # Set a new query ID for this operation
        self._set_trace_id()
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT DATABASE()")
            result = cursor.fetchone()
            return result['DATABASE()'] if result else None
        except Exception:
            return None
        finally:
            cursor.close()
    
    def get_current_catalog(self):
        """Get the current catalog name.
        
        Returns:
            str: Current catalog name or None
        """
        if not self.connection:
            return None
            
        # Set a new query ID for this operation
        self._set_trace_id()
        
        cursor = self.connection.cursor()
        try:
            cursor.execute("SHOW CATALOGS")
            results = cursor.fetchall()
            
            # Find the row where IsCurrent is Yes
            for row in results:
                if 'IsCurrent' in row and row['IsCurrent'].lower() == 'yes':
                    if 'CatalogName' in row:
                        return row['CatalogName']
            
            # If we got here, we didn't find a current catalog
            return "internal"  # Default to internal catalog if not found
        except Exception as e:
            # If the command fails (old version of Doris), default to internal
            return "internal"
        finally:
            cursor.close()
    
    def use_database(self, database):
        """Change the current database.
        
        Args:
            database (str): Database name to use
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection:
            return False
            
        # Set a new query ID for this operation
        self._set_trace_id()
        
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
            
        # Set a new query ID for this operation
        self._set_trace_id()
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SWITCH {catalog}")
            return True
        except Exception as e:
            print(f"Failed to switch to catalog {catalog}: {e}")
            return False
        finally:
            cursor.close()
    
    def cancel_query(self, http_port=None):
        """Cancel the current running query using Doris HTTP API.
        
        Args:
            http_port (int, optional): The HTTP port of Apache Doris server.
                                     If not provided, it will try to get the port.
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.trace_id:
            return False
            
        # Get the HTTP port if not provided
        if http_port is None:
            http_port = self.get_http_port()
            if http_port is None:
                print("Cannot cancel query: HTTP port not available")
                return False
            
        try:
            # Use Doris HTTP API to cancel the query
            cancel_url = f"http://{self.host}:{http_port}/api/cancel_query"
            params = {
                "query_id": self.trace_id
            }
            response = requests.get(cancel_url, params=params, timeout=5)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('status') == 'OK':
                    print(f"Query cancelled: {self.trace_id}")
                    return True
                else:
                    print(f"Failed to cancel query: {result.get('msg', 'Unknown error')}")
            else:
                print(f"Failed to cancel query: HTTP {response.status_code}")
                
            return False
        except Exception as e:
            print(f"Failed to cancel query: {e}")
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
    
    def reconnect(self):
        """Close the current connection and establish a new one.
        
        This is useful for recovering from connection errors or broken pipe errors.
        
        Returns:
            bool: True if reconnection was successful, False otherwise
        """
        # Close existing connection if it exists
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                # Ignore errors while closing a possibly broken connection
                pass
            self.connection = None
        
        # Try to establish a new connection
        return self.connect()
    
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