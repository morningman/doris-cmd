#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
doris-cmd - A command line client for Apache Doris with query progress reporting.

Features:
- Real-time progress tracking of query execution
- Authentication support for REST API
- Display of runtime alongside query ID
- Cancel queries with Ctrl+C or exit with Ctrl+D
- Mock mode for testing without a real Doris connection
"""

__version__ = "0.2.0"

__all__ = [
    "DorisConnection",
    "ProgressTracker",
    "main",
    "display_results",
    "handle_query",
    "handle_query_with_progress",
    "run_benchmark",
    "export_query_results_to_csv",
    "export_benchmark_results_to_csv",
]

from doris_cmd.connection import DorisConnection
from doris_cmd.progress import ProgressTracker
from doris_cmd.cli import main
from doris_cmd.display import display_results
from doris_cmd.query_handler import handle_query, handle_query_with_progress
from doris_cmd.benchmark import run_benchmark
from doris_cmd.export import export_query_results_to_csv, export_benchmark_results_to_csv 