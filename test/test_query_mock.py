"""
Mock tests for query operations in doris_cmd.
"""
import unittest
from unittest.mock import MagicMock, patch, ANY
import os
import sys
import tempfile
import uuid

# Add the parent directory to sys.path so we can import doris_cmd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from doris_cmd.connection import DorisConnection


class TestQueryGeneration(unittest.TestCase):
    """Test query ID generation and manipulation."""
    
    def test_generate_query_id(self):
        """Test query ID generation."""
        conn = DorisConnection(
            host="localhost",
            port=9030,
            user="test_user",
            password="test_password"
        )
        
        # Generate a query ID
        query_id = conn._generate_query_id()
        
        # Verify it has the expected format
        self.assertTrue(query_id.startswith("doris_cmd_"))
        self.assertEqual(len(query_id), len("doris_cmd_") + 32)  # 32 is the length of a hex UUID
    
    @patch.object(uuid, 'uuid4')
    def test_generate_query_id_with_mock_uuid(self, mock_uuid4):
        """Test query ID generation with a mocked UUID."""
        # Set up mock
        mock_uuid4.return_value = MagicMock(hex='1234567890abcdef1234567890abcdef')
        
        conn = DorisConnection(
            host="localhost",
            port=9030,
            user="test_user",
            password="test_password"
        )
        
        # Generate a query ID
        query_id = conn._generate_query_id()
        
        # Verify the query ID has the expected format with our mocked UUID
        self.assertEqual(query_id, "doris_cmd_1234567890abcdef1234567890abcdef")


@patch('pymysql.connect')
class TestQueryOperations(unittest.TestCase):
    """Test query operations with mocked database connection."""
    
    def setUp(self):
        """Set up test environment."""
        self.conn = DorisConnection(
            host="localhost",
            port=9030,
            user="test_user",
            password="test_password",
            database="test_db"
        )
    
    def test_set_query_id(self, mock_connect):
        """Test setting a query ID."""
        # Set up mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Connect first
        self.conn.connect()
        
        # Get the initial query ID
        original_query_id = self.conn.query_id
        
        # Set a new query ID
        result = self.conn._set_query_id()
        
        # Verify result and query ID changed
        self.assertTrue(result)
        self.assertNotEqual(self.conn.query_id, original_query_id)
        
        # Verify the session_context was set with the new query ID
        mock_cursor.execute.assert_any_call(f"SET session_context = 'trace_id:{self.conn.query_id}'")
    
    def test_check_connection(self, mock_connect):
        """Test checking if connection is alive."""
        # Set up mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Connect first
        self.conn.connect()
        
        # Test with a healthy connection
        result = self.conn._check_connection()
        self.assertTrue(result)
        mock_cursor.execute.assert_any_call("SELECT 1")
        
        # Test with a dead connection
        mock_cursor.execute.side_effect = Exception("Connection is dead")
        result = self.conn._check_connection()
        self.assertFalse(result)
    
    def test_get_database(self, mock_connect):
        """Test getting current database."""
        # Set up mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Set up cursor fetchone result
        mock_cursor.fetchone.return_value = {"DATABASE()": "test_db"}
        
        # Connect first
        self.conn.connect()
        
        # Get current database
        db_name = self.conn.get_current_database()
        
        # Verify
        self.assertEqual(db_name, "test_db")
        mock_cursor.execute.assert_any_call("SELECT DATABASE()")
    
    def test_use_database(self, mock_connect):
        """Test using a database."""
        # Set up mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        # Connect first
        self.conn.connect()
        
        # Use database
        result = self.conn.use_database("new_db")
        
        # Verify
        self.assertTrue(result)
        self.assertEqual(self.conn.database, "new_db")
        mock_cursor.execute.assert_any_call("USE new_db")
    
    def test_cancel_query(self, mock_connect):
        """Test canceling a query."""
        # Set up mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        # Connect first
        self.conn.connect()
        
        # Set query ID and HTTP port
        self.conn.query_id = "doris_cmd_test_query_id"
        self.conn.http_port = 8033
        
        # Mock requests.get
        with patch('requests.get') as mock_get:
            # Set up mock response
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"status": "OK"}
            mock_get.return_value = mock_response
            
            # Cancel query
            result = self.conn.cancel_query()
            
            # Verify
            self.assertTrue(result)
            mock_get.assert_called_once_with(
                "http://localhost:8033/api/cancel_query",
                params={"query_id": "doris_cmd_test_query_id"},
                timeout=5
            )


if __name__ == '__main__':
    unittest.main() 