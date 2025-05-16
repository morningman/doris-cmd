# doris-cmd Test Framework

This directory contains the regression test framework for the doris-cmd project, used for testing interactions with Apache Doris.

## Test Characteristics

These tests are designed to be **read-only**, without creating or deleting any objects in the database. The main features include:

- Tests only use SELECT queries and system metadata tables
- Tests do not create or delete databases or tables
- Tests use existing databases instead of creating temporary databases
- Tests do not write data

## Configuration

Before running the tests, please configure the database connection information in the `config.py` file:

```python
DORIS_CONFIG = {
    "host": "localhost",  # Replace with actual Doris host
    "port": 9030,         # Replace with actual Doris port
    "user": "root",       # Replace with actual Doris username
    "password": "",       # Replace with actual Doris password
    "database": "test"    # Default database to use
}
```

Note: Do **NOT** commit the config.py file with real credentials to the version control system.

Important: Please ensure that the database referenced by the TEST_DATABASE variable already exists. The tests will not automatically create databases.

## Test Structure

- `conftest.py` - Contains shared pytest fixtures
- `test_connection.py` - Tests for connection functionality
- `test_database_operations.py` - Tests for database query operations
- `test_query_operations.py` - Tests for query-related functionality

## Running Tests

Make sure pytest is installed:

```bash
pip install pytest
```

Run tests from the project root directory:

```bash
pytest test/
```

To view detailed output:

```bash
pytest test/ -v
```

Run tests using the script:

```bash
./test/run_tests.sh
```

## Adding New Tests

When adding new tests, please follow these conventions:

1. Create separate test files for new functionality, with names in the format `test_<feature_name>.py`
2. Use existing test fixtures `doris_connection` and `test_database`
3. Only use read-only operations, do not use operations that modify the database 