# Doris Command Line Interface (doris-cmd)

A modern command line interface for Apache Doris with query progress reporting.

## Features

- Interactive SQL command line interface
- Query progress tracking
- Syntax highlighting
- Command history
- Auto-suggestions
- Benchmark mode
- Formatted tabular output
- CSV export

## Installation

### Quick Install

Use the provided installation script to automatically check for dependencies and install doris-cmd:

```bash
# Clone the repository
git clone https://github.com/morningman/doris-cmd.git
cd doris-cmd

# Run the installation script
./install.sh
```

### Manual Installation

If you prefer to install manually:

```bash
# Clone the repository
git clone https://github.com/morningman/doris-cmd.git
cd doris-cmd

# Install with pip
pip install -e .
```

## Requirements

- Python 3.6 or higher
- Required packages (automatically installed):
  - mysql-connector-python
  - pymysql
  - requests
  - tabulate
  - prompt-toolkit
  - click
  - pygments
  - rich

## Usage

### Basic Usage

```bash
# Connect to local Doris instance
doris-cmd

# Connect to specific host and port
doris-cmd --host <hostname> --port <port> --user <username> --password <password>

# Specify a database
doris-cmd --database <database>
```

### Execute Query

```bash
# Execute a single query
doris-cmd -e "SELECT * FROM my_table LIMIT 10"

# Execute queries from a file
doris-cmd -f my_queries.sql
```

### Benchmark Mode

```bash
# Run benchmark on SQL queries from a file
doris-cmd --benchmark path/to/queries.sql --times 3
```

### Output to CSV

```bash
# Output results to a CSV file
doris-cmd -e "SELECT * FROM my_table" --output results.csv
```

## Interactive Mode Commands

Inside the interactive shell:

- Use semicolon (`;`) followed by Enter to execute a query
- Type `help` or `\h` to display help
- Type `exit`, `quit`, or `\q` to exit
- Use `\d` or `show databases` to show databases
- Use `\t` or `show tables` to show tables
- Press Ctrl+D to cancel any running query and exit

## Troubleshooting

If you encounter any issues:

1. Make sure all dependencies are installed:
   ```bash
   pip install -r requirements.txt
   ```

2. Check if Python 3.6+ is available:
   ```bash
   python3 --version
   ```

3. If you experience connection issues, check if your Doris server is running and accessible.

## License

Apache License 2.0

