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
- Run benchmark on SQL queries and measure performance

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
- `--benchmark`: Run benchmark on SQL queries from a file or directory
- `--times`: Number of times to run each query in benchmark mode (default: 1)

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

## Benchmark Mode

The benchmark mode allows you to measure the performance of SQL queries:

```bash
# Benchmark a single file with multiple SQL statements
doris-cmd --host <host> --port <port> --benchmark "queries.sql" --times 3

# Benchmark all .sql files in a directory (each file = one SQL statement)
doris-cmd --host <host> --port <port> --benchmark "sql_queries_dir/" --times 3
```

This command will:
1. Execute each SQL query the specified number of times
2. Show progress indicators during execution
3. Display comprehensive statistical results at the end

When using a directory, each .sql file is treated as a single SQL query. This is ideal for organizing complex queries into separate files.

The benchmark results include three tables:

1. **Query Execution Times**: Shows each query's execution time for each run, plus min/max/avg statistics
   ```
   ┏━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━┓
   ┃ Query # ┃ Source     ┃ Run 1   ┃ Run 2   ┃ Run 3   ┃ Min    ┃ Max    ┃ Avg    ┃
   ┡━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━┩
   │ Query 1 │ query1.sql │ 1.2345  │ 1.1234  │ 1.3456  │ 1.1234 │ 1.3456 │ 1.2345 │
   │ Query 2 │ query2.sql │ 0.5678  │ 0.4567  │ 0.6789  │ 0.4567 │ 0.6789 │ 0.5678 │
   │ Average │            │ 0.9012  │ 0.7901  │ 1.0123  │        │        │        │
   │ P50     │            │ 0.9012  │ 0.7901  │ 1.0123  │        │        │        │
   │ P95     │            │ 1.2345  │ 1.1234  │ 1.3456  │        │        │        │
   └─────────┴────────────┴─────────┴─────────┴─────────┴────────┴────────┴────────┘
   ```

2. **Overall Statistics**: Simple summary of the benchmark run
   ```
   ┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
   ┃ Metric          ┃ Value          ┃
   ┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
   │ Total Runtime   │ 5.67 seconds   │
   │ Number of Queries │ 2            │
   │ Total Executions │ 6             │
   └─────────────────┴────────────────┘
   ```

3. **SQL Queries**: Lists all benchmarked queries for reference
   ```
   ┏━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
   ┃ Query # ┃ Source     ┃ SQL                                                     ┃
   ┡━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
   │ Query 1 │ query1.sql │ SELECT * FROM my_table WHERE id > 1000                  │
   │ Query 2 │ query2.sql │ SELECT COUNT(*) FROM my_other_table                     │
   └─────────┴────────────┴─────────────────────────────────────────────────────────┘
   ```

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

