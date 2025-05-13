# Doris-Cmd

A command line tool for connecting to and operating Doris database.

## Features

- Connect to Doris via MySQL protocol
- Execute SQL queries and display results in table format
- Display real-time query progress during query execution
  - Show scanned rows, data size, CPU and memory usage
  - Show query runtime
- Display query ID and total runtime with results
- Support query cancellation (Ctrl+C) and program exit (Ctrl+D)

## Installation

```bash
pip install doris-cmd
```

## Usage

### Command Line Options

```
doris-cmd --host <host> --port <port> --user <user> --password <password> --database <database>
```

Parameters:

- `--host`: Doris server address, default is localhost
- `--port`: Doris MySQL port, default is 9030
- `--user`: Username, default is root
- `--password`: Password, default is empty
- `--database`: Default database (optional)
- `--execute`, `-e`: Execute query and exit
- `--file`, `-f`: Execute query from file and exit

### Special Commands in Interactive Mode

- `\q`, `exit`, `quit`: Exit the program
- `\h`, `help`: Display help information
- `\d`, `show databases`: Show all databases
- `\t`, `show tables`: Show tables in the current database
- `use <database>`: Switch to specified database
- `source <file>`: Execute SQL statements from file

### Keyboard Shortcuts

- `Ctrl+C`: Cancel the currently running query
- `Ctrl+D`: Cancel the current query and exit the program

## Progress Display

During query execution, doris-cmd will display the following real-time progress information:

- Status: Query status (RUNNING/FINISHED)
- Query ID: Used to identify and track the query
- Runtime: Actual runtime of the query
- Scanned rows: Number of data rows scanned
- Data volume: Size of processed data
- CPU time: CPU time used by the query
- Memory usage: Memory amount used by the query

## Development

### Installing Development Dependencies

```bash
pip install -e ".[dev]"
```

### Running Tests

```bash
python test_doris_cmd.py --host <host> --port <port> --user <user> --password <password>
```

## License

Apache License 2.0

