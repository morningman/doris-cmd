#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Display utilities for doris-cmd.
"""
from rich.console import Console
from rich.table import Table
from rich.style import Style as RichStyle


def display_results(column_names, results, trace_id=None, runtime=None, output_file=None, append_csv=False):
    """Display query results in tabular format using rich.
    
    Args:
        column_names (list): List of column names
        results (list): List of dictionaries containing results
        trace_id (str, optional): The trace ID associated with this result
        runtime (float, optional): Query runtime in seconds
        output_file (str, optional): Path to output CSV file
        append_csv (bool): Whether to append to the CSV file if it exists
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
    if trace_id:
        console.print(f"Trace ID: {trace_id}")
        console.print(f"Query Time: {runtime:.2f}s")
        
    # Export to CSV if output_file is provided
    if output_file:
        # Import here to avoid circular imports
        from doris_cmd.export import export_query_results_to_csv

        if export_query_results_to_csv(column_names, results, output_file, append=append_csv):
            if not append_csv:
                console.print(f"\nQuery results exported to: {output_file}")
            else:
                console.print(f"\nQuery results appended to: {output_file}")
        else:
            console.print(f"\nFailed to export query results to: {output_file}") 