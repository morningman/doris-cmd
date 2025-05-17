"""
Module for fetching and displaying query progress information.
"""
import time
import threading
import requests
import json
import random


class ProgressTracker:
    """Track and display query progress."""

    def __init__(self, host, port=None, trace_id=None, connection=None, mock_mode=False, 
                 auth_user=None, auth_password=None, auth_headers=None, auth_cookies=None):
        """Initialize a progress tracker.
        
        Args:
            host (str): The host of Apache Doris server
            port (int, optional): The HTTP port of Apache Doris server. If not provided,
                                 connection must be provided to get the port.
            trace_id (str, optional): The trace ID to track, can be set later
            connection (DorisConnection, optional): Connection to Doris for getting HTTP port
            mock_mode (bool, optional): Whether to use mock data for testing
            auth_user (str, optional): Username for Basic Authentication
            auth_password (str, optional): Password for Basic Authentication
            auth_headers (dict, optional): Custom headers for authentication
            auth_cookies (dict, optional): Cookies for authentication
        """
        self.host = host
        self.port = port
        self.trace_id = trace_id
        self.connection = connection
        self.tracking = False
        self.thread = None
        self.progress_data = {}
        self.last_print_time = 0
        self.mock_mode = mock_mode
        self.last_error = None
        self.ever_received_data = False
        self.start_time = None
        self.total_runtime = 0
        self.progress_tracking_started = False
        self.silent_mode = False
        
        # Authentication parameters
        self.auth_user = auth_user
        self.auth_password = auth_password
        self.auth_headers = auth_headers or {}
        self.auth_cookies = auth_cookies or {}
        
        # Mock data counters
        self.mock_scan_rows = 0
        self.mock_scan_bytes = 0
        self.mock_start_time = None
        
    def start_tracking(self, silent=False):
        """Start tracking query progress in a separate thread.
        
        Args:
            silent (bool): If True, progress will not be displayed to the console
        """
        if self.tracking:
            return
            
        # Record start time
        self.start_time = time.time()
        
        # Set silent mode
        self.silent_mode = silent
            
        # Try to get HTTP port if not set and connection is available
        if not self.mock_mode and self.port is None and self.connection is not None:
            self.port = self.connection.get_http_port()
            if self.port is None and not self.mock_mode:
                if not self.silent_mode:
                    print("\rWarning: Could not determine HTTP port. Using mock mode for progress tracking.")
                self.mock_mode = True
        
        # In mock mode, initialize mock data
        if self.mock_mode:
            self.mock_scan_rows = 0
            self.mock_scan_bytes = 0
            self.mock_start_time = time.time()
        
        self.tracking = True
        self.thread = threading.Thread(target=self._track_progress)
        self.thread.daemon = True
        self.thread.start()
        
    def stop_tracking(self):
        """Stop tracking query progress."""
        self.tracking = False
        if self.thread:
            self.thread.join(timeout=1.0)
            self.thread = None
        
        # Calculate total runtime
        self.total_runtime = 0
        if self.start_time:
            self.total_runtime = time.time() - self.start_time
            
        # In mock mode, display final progress data (only if not in silent mode)
        if self.mock_mode and not self.silent_mode:
            progress = self._fetch_progress_mock(final=True)
            if progress:
                self.progress_data = progress
                self._display_progress()
        
    def get_total_runtime(self):
        """Get the total runtime in seconds.
        
        Returns:
            float: Total runtime in seconds, or 0 if tracking hasn't started
        """
        if hasattr(self, 'total_runtime') and self.total_runtime > 0:
            return self.total_runtime
        elif self.start_time:
            return time.time() - self.start_time
        return 0
        
    def _track_progress(self):
        """Track progress in a loop until stopped."""
        # Initially wait until query has been running for at least 2 seconds
        # before actually starting to track progress
        wait_until = self.start_time + 2.0
        
        while self.tracking and time.time() < wait_until:
            # Sleep in short intervals to check if tracking is still needed
            time.sleep(0.1)
            
            # If tracking has been explicitly stopped, exit early
            if not self.tracking:
                return
        
        # Only set this flag once we've waited the initial period
        self.progress_tracking_started = True
        
        # Now begin actual progress tracking
        while self.tracking:
            try:
                # Skip if trace_id is not set
                if not self.trace_id and not self.mock_mode:
                    time.sleep(0.1)
                    continue
                    
                # In mock mode, we don't need a port or trace_id
                if not self.mock_mode:
                    # Skip if port is not set
                    if self.port is None:
                        if self.connection:
                            self.port = self.connection.get_http_port()
                            if self.port is None:
                                # If still can't get the port, use mock mode
                                if not self.silent_mode:
                                    print("\rInfo: Using mock data for progress tracking")
                                self.mock_mode = True
                        else:
                            if not self.silent_mode:
                                print("\rInfo: Using mock data for progress tracking")
                            self.mock_mode = True
                    
                # Get progress data
                if self.mock_mode:
                    progress = self._fetch_progress_mock()
                else:
                    progress = self._fetch_progress()
                    
                if progress:
                    self.progress_data = progress
                    self.ever_received_data = True
                    self.last_error = None
                else:
                    # If unable to get progress, retain previous data but add error flag
                    if not self.ever_received_data:
                        # If we've never received data, set default values
                        self.progress_data = {
                            'scanned_rows': 'N/A',
                            'scanned_bytes': 'N/A',
                            'cpu_ms': 'N/A',
                            'memory_bytes': 'N/A',
                            'state': 'UNKNOWN'
                        }
                    
                    # Always add current runtime even when progress info retrieval fails
                    if self.start_time:
                        runtime_ms = int((time.time() - self.start_time) * 1000)
                        self.progress_data['runtime_ms'] = runtime_ms
                    
                    # Add error information if available
                    if self.last_error:
                        self.progress_data['error'] = self.last_error
                        
                # Display the progress (if not in silent mode)
                if not self.silent_mode:
                    self._display_progress()
            except Exception as e:
                # Only print errors occasionally to avoid cluttering the terminal
                if not self.silent_mode:
                    current_time = time.time()
                    if current_time - self.last_print_time > 5.0:
                        print(f"\rProgress tracking error: {e}", end="")
                        self.last_print_time = current_time
                        self.last_error = str(e)
                        
                        # Always add current runtime to progress data even when exceptions occur
                        if self.start_time and self.progress_data:
                            runtime_ms = int((time.time() - self.start_time) * 1000)
                            self.progress_data['runtime_ms'] = runtime_ms
                            self._display_progress()
                else:
                    # In silent mode, just record the error but don't print anything
                    self.last_error = str(e)
                    if self.start_time and self.progress_data:
                        runtime_ms = int((time.time() - self.start_time) * 1000)
                        self.progress_data['runtime_ms'] = runtime_ms
                
            # Sleep for 1 second between progress checks
            time.sleep(1.0)
            
    def _fetch_progress_mock(self, final=False):
        """Generate mock progress data for testing.
        
        Args:
            final (bool): Whether this is the final progress update
            
        Returns:
            dict: Mock progress information
        """
        # Increment mock counters
        if final:
            # For final update, simulate completion
            self.mock_scan_rows += random.randint(500, 1000)  # Final burst of rows
            self.mock_scan_bytes += random.randint(5, 10) * 1024 * 1024  # Final burst of bytes
        else:
            self.mock_scan_rows += random.randint(80, 120)  # ~100 rows per call
            self.mock_scan_bytes += random.randint(900000, 1100000)  # ~1MB per call
        
        # Calculate elapsed time
        elapsed_time = time.time() - self.mock_start_time if self.mock_start_time else 0
        
        # Generate mock CPU and memory usage
        # CPU time grows with elapsed time
        mock_cpu_ms = int(elapsed_time * 1000 * random.uniform(0.8, 1.2))
        
        # Memory usage grows with scan bytes (with some randomness)
        mock_memory_bytes = int(self.mock_scan_bytes * random.uniform(1.5, 2.5))
        
        # Calculate actual runtime
        runtime_ms = 0
        if self.start_time:
            runtime_ms = int((time.time() - self.start_time) * 1000)
        
        # Generate mock progress data
        progress = {
            'state': 'RUNNING' if not final else 'FINISHED',
            'scanned_rows': self.mock_scan_rows,
            'scanned_bytes': self.mock_scan_bytes,
            'elapsed_time': elapsed_time,
            'cpu_ms': mock_cpu_ms,
            'memory_bytes': mock_memory_bytes,
            'runtime_ms': runtime_ms
        }
        
        return progress
            
    def _fetch_progress(self):
        """Fetch progress information from Apache Doris.
        
        Returns:
            dict: Progress information
        """
        try:
            # New Doris FE HTTP API for query progress
            url = f"http://{self.host}:{self.port}/rest/v2/manager/query/progres/query/{self.trace_id}"
            
            try:
                # Setup authentication
                auth = None
                if self.auth_user is not None:
                    auth = (self.auth_user, self.auth_password or '')
                
                # Make the request with only Basic Authentication
                response = requests.get(
                    url, 
                    timeout=5,
                    auth=auth
                )
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        # Check if the API call was successful
                        if result.get('msg') == 'success' and 'data' in result:
                            # Extract specified fields from the data
                            data = result['data']
                            
                            # Calculate runtime
                            runtime_ms = 0
                            if self.start_time:
                                runtime_ms = int((time.time() - self.start_time) * 1000)
                                
                            progress = {
                                'scanned_rows': data.get('scanRows', 0),
                                'scanned_bytes': data.get('scanBytes', 0),
                                'cpu_ms': data.get('cpuMs', 0),
                                'memory_bytes': data.get('currentUsedMemoryBytes', 0),
                                'state': 'RUNNING',  # Assume running if we get data
                                'runtime_ms': runtime_ms
                            }
                            
                            return progress
                        else:
                            # If API call was not successful, store error message
                            error_msg = f"API Error: {result.get('msg', 'Unknown error')}"
                            if 'data' in result:
                                error_data = result['data']
                                if isinstance(error_data, dict):
                                    error_msg += f" | {json.dumps(error_data)}"
                                else:
                                    error_msg += f" | {error_data}"
                            self.last_error = error_msg
                            return None
                    except json.JSONDecodeError:
                        self.last_error = f"Invalid JSON response: {response.text[:100]}"
                        return None
                else:
                    if response.status_code == 401 or response.status_code == 403:
                        self.last_error = f"Authentication failed (HTTP {response.status_code}). Please check username and password."
                    else:
                        self.last_error = f"HTTP Error: {response.status_code}"
                    if response.text:
                        self.last_error += f" | {response.text[:100]}"
                    return None
            except requests.exceptions.Timeout:
                self.last_error = "Request timeout"
                return None
            except requests.exceptions.ConnectionError:
                self.last_error = f"Connection error to {self.host}:{self.port}"
                return None
        except Exception as e:
            self.last_error = f"Exception: {str(e)}"
            return None
            
    def _display_progress(self):
        """Display current progress information."""
        # Don't display anything in silent mode
        if self.silent_mode:
            return
            
        # Always calculate runtime
        runtime_ms = 0
        if self.start_time:
            runtime_ms = int((time.time() - self.start_time) * 1000)
            
        # Only proceed if we're in the progress tracking phase (after 2s)
        if not self.progress_tracking_started:
            return
            
        if not self.progress_data:
            # If there's no progress data, at least show runtime
            mock_indicator = "[Mock] " if self.mock_mode else ""
            runtime_str = f"Runtime: {runtime_ms/1000:.2f}s"
            progress_str = f"\r{mock_indicator}ID: {self.trace_id} | {runtime_str} | Waiting for progress data..."
            # Clear the entire line, then print new progress information
            # Use a string of spaces long enough to cover the whole line, then return to line start to print new information
            print("\r" + " " * 150 + "\r" + progress_str, end="")
            self.last_print_time = time.time()
            return
            
        # Extract relevant information from progress_data
        scanned_rows = self.progress_data.get('scanned_rows', 'N/A')
        scanned_bytes = self.progress_data.get('scanned_bytes', 'N/A')
        state = self.progress_data.get('state', 'UNKNOWN')
        elapsed_time = self.progress_data.get('elapsed_time')
        cpu_ms = self.progress_data.get('cpu_ms', 'N/A')
        memory_bytes = self.progress_data.get('memory_bytes', 'N/A')
        # Use existing runtime if available, otherwise use calculated value
        runtime_ms = self.progress_data.get('runtime_ms', runtime_ms)
        error = self.progress_data.get('error')
        
        # Format rows with commas if it's a number
        formatted_rows = scanned_rows
        if isinstance(scanned_rows, (int, float)):
            formatted_rows = f"{scanned_rows:,}"
        
        # Convert bytes to human-readable format only if it's a number
        if isinstance(scanned_bytes, (int, float)):
            readable_bytes = self._format_bytes(scanned_bytes)
        else:
            readable_bytes = scanned_bytes
            
        if isinstance(memory_bytes, (int, float)):
            readable_memory = self._format_bytes(memory_bytes)
        else:
            readable_memory = memory_bytes
        
        # Format CPU time only if it's a number
        if isinstance(cpu_ms, (int, float)):
            cpu_time = f"{cpu_ms/1000:.2f}s"
        else:
            cpu_time = cpu_ms
        
        # Always display runtime
        runtime_str = f"Runtime: {runtime_ms/1000:.2f}s"
            
        # Format time if available
        time_str = ""
        if elapsed_time is not None:
            time_str = f" | Time: {elapsed_time:.1f}s"
        
        # Indicate if using mock data
        mock_indicator = "[Mock] " if self.mock_mode else ""
        
        # Add error information if available
        error_str = ""
        if error:
            # Truncate error message if too long
            if len(error) > 50:
                error = error[:47] + "..."
            error_str = f" | Error: {error}"
        
        # Display progress information with trace_id and new metrics
        progress_str = f"{mock_indicator}State: {state} | Trace ID: {self.trace_id} | {runtime_str} | ScannedRows: {formatted_rows} | ScannedBytes: {readable_bytes} | CPU: {cpu_time} | Mem: {readable_memory}{time_str}{error_str}"
        
        # Clear the entire line, then print new progress information
        # Use a string of spaces long enough to cover the whole line, then return to line start to print new information
        print("\r" + " " * 150 + "\r" + progress_str, end="")
        self.last_print_time = time.time()
        
    @staticmethod
    def _format_bytes(bytes_num):
        """Format bytes into human-readable string.
        
        Args:
            bytes_num (int): Number of bytes
            
        Returns:
            str: Human-readable string
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_num < 1024:
                return f"{bytes_num:.2f} {unit}"
            bytes_num /= 1024
        return f"{bytes_num:.2f} PB" 