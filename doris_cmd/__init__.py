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
]

from doris_cmd.connection import DorisConnection
from doris_cmd.progress import ProgressTracker
from doris_cmd.cli import main 