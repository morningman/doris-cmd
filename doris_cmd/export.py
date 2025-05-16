#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export utilities for doris-cmd.
"""
import os
import csv


def export_query_results_to_csv(column_names, results, output_file, append=False):
    """Export query results to a CSV file.
    
    Args:
        column_names (list): List of column names
        results (list): List of dictionaries containing results
        output_file (str): Path to the output CSV file
        append (bool): Whether to append to an existing file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check if the file exists when appending
        file_exists = os.path.exists(output_file) if append else False
        
        # Open in append mode if requested and file exists
        mode = 'a' if append and file_exists else 'w'
        
        with open(output_file, mode, newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Add a separator if appending
            if append and file_exists:
                writer.writerow([])  # Empty line as separator
                writer.writerow([f"# SQL Query Results"])
            
            # Write header
            writer.writerow(column_names)
            
            # Write rows
            for row in results:
                writer.writerow([row.get(col, "") for col in column_names])
                
        return True
    except Exception as e:
        print(f"Error exporting to CSV: {e}")
        return False
        
        
def export_benchmark_results_to_csv(benchmark_results, run_times_by_number, stats, output_file):
    """Export benchmark results to a CSV file.
    
    This exports the Query Execution Times table and the Overall Statistics table.
    
    Args:
        benchmark_results (list): List of query results
        run_times_by_number (list): List of run times grouped by run number
        stats (dict): Dictionary containing overall statistics
        output_file (str): Path to the output CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write Query Execution Times table
            writer.writerow(["# Query Execution Times (seconds)"])
            
            # Determine number of runs
            times = len(run_times_by_number)
            
            # Write header row
            header = ["No.", "Query #", "Source"]
            for run in range(1, times + 1):
                header.append(f"Run {run}")
            header.extend(["Min", "Max", "Avg"])
            writer.writerow(header)
            
            # Write data rows
            for idx, result in enumerate(benchmark_results, 1):
                query_num = result['query_num']
                query_source = result['query_source']
                times_list = [t['time'] for t in result['times']]
                
                # Calculate statistics
                min_time = min(times_list) if times_list else 0
                max_time = max(times_list) if times_list else 0
                avg_time = sum(times_list) / len(times_list) if times_list else 0
                
                # Format row data
                row = [idx, f"Query {query_num}", query_source]
                
                # Add times for each run
                for run in range(times):
                    if run < len(times_list):
                        row.append(f"{times_list[run]:.4f}")
                    else:
                        row.append("N/A")
                
                # Add statistics
                row.append(f"{min_time:.4f}")
                row.append(f"{max_time:.4f}")
                row.append(f"{avg_time:.4f}")
                
                writer.writerow(row)
            
            # Add summary row
            summary_row = ["", "Average", ""]
            for run_times in run_times_by_number:
                if run_times:
                    avg_for_run = sum(run_times) / len(run_times)
                    summary_row.append(f"{avg_for_run:.4f}")
                else:
                    summary_row.append("N/A")
            
            # Add placeholders for min/max/avg columns
            summary_row.extend(["", "", ""])
            writer.writerow(summary_row)
            
            # Add empty row as separator
            writer.writerow([])
            
            # Write Overall Statistics table
            writer.writerow(["# Overall Statistics"])
            writer.writerow(["Metric", "Value"])
            
            # Write individual statistics
            for key, value in stats.items():
                writer.writerow([key, value])
                
        return True
    except Exception as e:
        print(f"Error exporting benchmark results to CSV: {e}")
        return False 