#!/bin/bash
# Helper script for running doris-cmd tests

USAGE="Usage: $0 [--mock-only]

Options:
  --mock-only    Only run mock tests, don't attempt to connect to a real Doris server
"

# Default is to run all tests
RUN_MOCK_ONLY=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --mock-only)
      RUN_MOCK_ONLY=true
      shift
      ;;
    --help)
      echo -e "$USAGE"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo -e "$USAGE"
      exit 1
      ;;
  esac
done

# Switch to project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Check if virtual environment exists, create if not
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install test dependencies
echo "Installing test dependencies..."
pip install -r test/requirements.txt

# Install project (development mode)
pip install -e .

# Run tests
if [ "$RUN_MOCK_ONLY" = true ]; then
    echo "Running mock tests..."
    python -m unittest test/test_mock.py test/test_query_mock.py
else
    echo "Running all tests..."
    echo "Note: If no Doris server is available, use the --mock-only option to run only mock tests."
    pytest test/
fi

# Exit virtual environment
deactivate

echo "Tests completed" 