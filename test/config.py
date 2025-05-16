"""
Test configuration for doris-cmd.

This file contains configuration for testing doris-cmd against a real Doris cluster.
DO NOT commit this file with real credentials to version control.
"""

# Doris connection information
DORIS_CONFIG = {
    "host": "172.20.32.136",  # Replace with actual Doris host
    "port": 9033,         # Replace with actual Doris port
    "user": "root",       # Replace with actual Doris username
    "password": "",       # Replace with actual Doris password
    "database": "information_schema"    # Default database to use
}

# HTTP port of the Doris FE server
HTTP_PORT = 8033  # Replace with actual HTTP port if known

# Test database name - will be created and dropped during tests
TEST_DATABASE = "information_schema" 
