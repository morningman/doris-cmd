#!/bin/bash
set -e

echo "===== doris-cmd Installation and Test Script ====="

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Install doris-cmd
echo "Installing doris-cmd..."
pip install -e .

echo "Installation completed!"

# Display help information
echo -e "\n===== doris-cmd Command Line Help ====="
doris-cmd --help

echo -e "\n===== Testing doris-cmd ====="
echo "Please modify the following variables according to your Apache Doris environment:"

# Apache Doris connection information (user should modify based on actual environment)
DORIS_HOST="localhost"
DORIS_PORT="9030"
DORIS_USER="root"
DORIS_PASSWORD=""
DORIS_DATABASE="test"

echo "Apache Doris Connection Information:"
echo "  Host: $DORIS_HOST"
echo "  MySQL Port: $DORIS_PORT"
echo "  Username: $DORIS_USER"
echo "  Password: $DORIS_PASSWORD"
echo "  Database: $DORIS_DATABASE"
echo "  HTTP Port: Auto-detect (from 'SHOW FRONTENDS' query)"

# Create a test SQL file
echo "Creating test SQL file..."
cat > test_query.sql << EOF
SHOW DATABASES;
USE $DORIS_DATABASE;
SHOW TABLES;
SELECT 1 AS test_col;
EOF

echo -e "\nTest SQL File Content:"
cat test_query.sql

echo -e "\n===== Test Cases ====="

echo -e "\n1. Execute a single query:"
echo "doris-cmd --host $DORIS_HOST --port $DORIS_PORT --user $DORIS_USER --password \"$DORIS_PASSWORD\" --execute \"SHOW DATABASES\""
echo -e "\nYou can run the above command to test executing a single query\n"

echo "2. Execute SQL file:"
echo "doris-cmd --host $DORIS_HOST --port $DORIS_PORT --user $DORIS_USER --password \"$DORIS_PASSWORD\" --file test_query.sql"
echo -e "\nYou can run the above command to test executing a SQL file\n"

echo "3. Interactive mode:"
echo "doris-cmd --host $DORIS_HOST --port $DORIS_PORT --user $DORIS_USER --password \"$DORIS_PASSWORD\" --database $DORIS_DATABASE"
echo -e "\nYou can run the above command to enter interactive mode\n"

echo "Note: Please ensure the Apache Doris service is running and can be connected"
echo "      If the connection information is incorrect, please modify the variables in this script"
echo -e "\nFeatures:"
echo "- HTTP port will be automatically obtained from the 'SHOW FRONTENDS' query"
echo "- Each query will generate a new query_id"
echo "- Query progress will be displayed in real-time"
echo "- After query execution, the query_id will be displayed below the results"

echo -e "\nTo manually specify HTTP port, you can use the --http_port parameter:"
echo "doris-cmd --host $DORIS_HOST --port $DORIS_PORT --http_port 8030 --user $DORIS_USER --password \"$DORIS_PASSWORD\""

echo -e "\nTo directly execute the test, please uncomment the following command:"
echo "# doris-cmd --host $DORIS_HOST --port $DORIS_PORT --user $DORIS_USER --password \"$DORIS_PASSWORD\" --execute \"SHOW DATABASES\""

echo -e "\n===== Test Script End =====" 