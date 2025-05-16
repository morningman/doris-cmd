#!/usr/bin/env python
"""List all available databases in Doris."""

import sys
import os

# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from doris_cmd.connection import DorisConnection

# Doris connection params - adjust as needed
HOST = 'localhost'
PORT = 9030
USER = 'root'
PASSWORD = ''

def main():
    """Connect to Doris and list all databases."""
    # Create connection
    conn = DorisConnection(host=HOST, port=PORT, user=USER, password=PASSWORD)
    
    # Connect
    print("Connecting to Doris at {}:{}".format(HOST, PORT))
    if not conn.connect():
        print("Failed to connect!")
        return 1
    
    # Query databases
    print("Querying available databases...")
    col_names, results = conn.execute_query("SHOW DATABASES")
    
    # Find database name column
    db_col = None
    for col in col_names:
        if "database" in col.lower():
            db_col = col
            break
    
    # Print results
    if results and db_col:
        print("\nAvailable databases:")
        for row in results:
            print("- {}".format(row[db_col]))
    else:
        print("No databases found or couldn't determine database column name")
    
    # Close connection
    conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main()) 