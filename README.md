# Doris Command Line Interface (doris-cmd)

A modern command line interface for Apache Doris

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

Use the provided build-install script to automatically check for dependencies, build and install doris-cmd:

```bash
# Clone the repository
git clone https://github.com/morningman/doris-cmd.git
cd doris-cmd

# Run the build-install script
./build-install.sh
```

## Requirements

For source installation only (standalone binary has no requirements):

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

# Use a configuration file
doris-cmd --config /path/to/config.ini
```

### Configuration File

You can specify connection settings in a configuration file:

```ini
[doris]
host = your_doris_host
port = 9030
user = root
password = your_password
database = your_database
```

Command line arguments take precedence over configuration file values.

### Execute Query

```bash
# Execute a single query
doris-cmd -e "SELECT * FROM my_table LIMIT 10"

# Execute queries from a file
doris-cmd -f my_queries.sql

# Use a configuration file and execute a query
doris-cmd --config /path/to/config.ini -e "SHOW DATABASES"
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

## Building Standalone Binaries

doris-cmd includes a packaging script to build standalone executables that don't require Python or any other dependencies:

```bash
# Run the packaging script to build a standalone binary for your platform
./package.sh
```

The resulting binaries will be placed in the `dist/` directory, named according to your platform:
- `doris-cmd-linux-0.2.0` - When built on Linux
- `doris-cmd-macos-0.2.0` - When built on macOS

### Build Requirements

- Python 3.6+ and PyInstaller

## Testing

To quickly test the installation:

```bash
# Show help
doris-cmd --help

# Test connection to a Doris instance
doris-cmd --host localhost --port 9030 --user root --execute "SHOW DATABASES"
```

## Troubleshooting

If you encounter any issues:

1. Make sure all dependencies are installed (if using source installation):
   ```bash
   pip install -r requirements.txt
   ```

2. Check if Python 3.6+ is available (if using source installation):
   ```bash
   python3 --version
   ```

3. If you experience connection issues, check if your Doris server is running and accessible.

## License

Apache License 2.0

