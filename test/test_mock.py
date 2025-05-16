"""
Mock tests for doris_cmd that don't require a real Doris connection.
"""
import unittest
from unittest.mock import MagicMock, patch, ANY
import os
import sys
import tempfile

# Add the parent directory to sys.path so we can import doris_cmd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from doris_cmd.connection import DorisConnection


class TestConnectionInitialization(unittest.TestCase):
    """Test initialization of the DorisConnection class."""
    
    def test_initialization(self):
        """Test that DorisConnection initializes correctly."""
        conn = DorisConnection(
            host="localhost",
            port=9030,
            user="test_user",
            password="test_password",
            database="test_db"
        )
        
        self.assertEqual(conn.host, "localhost")
        self.assertEqual(conn.port, 9030)
        self.assertEqual(conn.user, "test_user")
        self.assertEqual(conn.password, "test_password")
        self.assertEqual(conn.database, "test_db")
        self.assertIsNone(conn.connection)
        self.assertIsNone(conn.query_id)
        self.assertIsNone(conn.http_port)
        self.assertIsNone(conn.version)


@patch('pymysql.connect')
class TestConnectionWithMock(unittest.TestCase):
    """Test DorisConnection methods with mocked pymysql."""
    
    def setUp(self):
        """Set up test environment."""
        self.conn = DorisConnection(
            host="localhost",
            port=9030,
            user="test_user",
            password="test_password",
            database="test_db"
        )
    
    def test_connect(self, mock_connect):
        """Test connect method."""
        # Set up the mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Call the method
        result = self.conn.connect()
        
        # Assertions
        self.assertTrue(result)
        mock_connect.assert_called_once_with(
            host="localhost",
            port=9030,
            user="test_user",
            password="test_password",
            database="test_db",
            charset='utf8mb4',
            cursorclass=ANY
        )
        self.assertEqual(self.conn.connection, mock_connection)
        self.assertIsNotNone(self.conn.query_id)
    
    def test_connect_exception(self, mock_connect):
        """Test connect method with exception."""
        # Set up the mock to raise an exception
        mock_connect.side_effect = Exception("Connection failed")
        
        # Call the method
        result = self.conn.connect()
        
        # Assertions
        self.assertFalse(result)
        self.assertIsNone(self.conn.connection)
    
    def test_execute_query(self, mock_connect):
        """Test execute_query method."""
        # Set up mocks
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Set up cursor description and fetchall results
        mock_cursor.description = [("column1", None, None, None, None, None, None)]
        mock_cursor.fetchall.return_value = [{"column1": "value1"}]
        
        # Need to connect first
        self.conn.connect()
        
        # Call the method - this will actually call execute multiple times
        # due to version checks and other internal operations
        column_names, results = self.conn.execute_query("SELECT 1")
        
        # Only verify that the SQL we're interested in was executed at some point
        mock_cursor.execute.assert_any_call("SELECT 1")
        
        # Rest of assertions
        self.assertEqual(column_names, ["column1"])
        self.assertEqual(results, [{"column1": "value1"}])
    
    def test_close(self, mock_connect):
        """Test close method."""
        # Set up mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Need to connect first
        self.conn.connect()
        
        # Call the method
        self.conn.close()
        
        # Assertions
        mock_connection.close.assert_called_once()
        self.assertIsNone(self.conn.connection)
    
    def test_reconnect(self, mock_connect):
        """Test reconnect method."""
        # Set up mock
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Call the method
        result = self.conn.reconnect()
        
        # Assertions
        self.assertTrue(result)
        # We don't assert call count because internal implementation might call it multiple times
        self.assertEqual(self.conn.connection, mock_connection)


class TestFileExecution(unittest.TestCase):
    """Test file execution with mocked execute_query."""
    
    def setUp(self):
        """Set up test environment."""
        self.conn = DorisConnection(
            host="localhost",
            port=9030,
            user="test_user",
            password="test_password"
        )
        self.conn.connection = MagicMock()  # Fake a connection
    
    @patch.object(DorisConnection, 'execute_query')
    def test_execute_file(self, mock_execute_query):
        """Test execute_file method."""
        # Set up mock
        mock_execute_query.return_value = (["column1"], [{"column1": "value1"}])
        
        # Create a temporary file
        fd, sql_file = tempfile.mkstemp(suffix='.sql')
        os.close(fd)
        
        try:
            # Write SQL to the file
            with open(sql_file, 'w') as f:
                f.write("""
                -- First query
                SELECT 1;
                
                -- Second query
                SELECT 2;
                """)
            
            # Call the method
            column_names, results = self.conn.execute_file(sql_file)
            
            # We should have at least 2 calls for the 2 queries in the file
            # We use assertGreaterEqual instead of assertEqual because the method might
            # perform additional calls for query IDs etc.
            self.assertGreaterEqual(mock_execute_query.call_count, 2)
            self.assertEqual(column_names, ["column1"])
            self.assertEqual(results, [{"column1": "value1"}])
            
        finally:
            # Clean up
            os.unlink(sql_file)


if __name__ == '__main__':
    unittest.main() 