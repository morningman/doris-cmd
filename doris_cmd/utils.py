#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility functions for doris-cmd.
"""
import os


def get_history_file():
    """Get history file path."""
    home_dir = os.path.expanduser("~")
    history_dir = os.path.join(home_dir, ".doris_cmd")
    os.makedirs(history_dir, exist_ok=True)
    return os.path.join(history_dir, "history")


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
      - Query progress is displayed in real-time (when not in profile mode)
      - Each query gets a new query ID for tracking
      - Query ID and runtime are displayed with results
      - HTTP port is auto-detected from Doris FE
      - Profile mode: when enabled, collects and saves query profiles
      
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
      --profile                        Enable profile mode to collect and save query profiles.
                                       Profiles are saved to /tmp/.doris_profile/ directory.
                                       Note: Incompatible with --benchmark option.
    """
    print(help_text) 