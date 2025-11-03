# TrinoQ

A convenient CLI tool to query data from Trino with built-in caching and Google authentication support.

## Features

- Execute SQL queries directly from command line
- Read queries from files or stdin
- Built-in caching using Parquet format for faster repeated queries
- Google JWT authentication
- Environment variable substitution in queries
- DataFrame evaluation with Python expressions

## Installation

```bash
uv tool install git+https://github.com/mmngreco/trinoq
```

## Configuration

Set up your Trino connection URL with the required environment variable:

```bash
export TRINO_URL="https://host:443"
```

The URL format supports query parameters for additional configuration:

```bash
export TRINO_URL="https://host:443?user=user@google.com&catalog=my_catalog&schema=my_schema"
```

## Usage

### Basic Query

```bash
trinoq "select 1"
```

### Read Query from File

```bash
trinoq -f query.sql
```

### Read Query from Stdin

```bash
echo "select * from my_table limit 10" | trinoq -
```

### Disable Cache

By default, query results are cached in `/tmp/druidq/`. To disable caching:

```bash
trinoq --no-cache "select * from my_table"
```

### Evaluate DataFrame

Execute Python code on the resulting DataFrame using the `-e` or `--eval-df` flag:

```bash
trinoq "select * from my_table" -e "print(df.describe())"
```

### Quiet Mode

Suppress informational output (useful for piping results):

```bash
trinoq --quiet "select * from my_table" -e "print(df.head())"
```

### Environment Variable Substitution

Use environment variables in your queries with `{VAR_NAME}` syntax:

```bash
export TABLE_NAME="my_table"
trinoq "select * from {TABLE_NAME}"
```

## Command-Line Options

```
trinoq [-h] [-f] [-n] [-q] [-e EVAL_DF] [--pdb] query

positional arguments:
  query                 SQL query string, or use '-' for stdin, or use with -f for file

optional arguments:
  -h, --help            show this help message and exit
  -f, --file            Read query from file
  -n, --no-cache        Do not use cache
  -q, --quiet           Do not print the output except the code you use in eval-df
  -e EVAL_DF, --eval-df EVAL_DF
                        Evaluate 'df' using string or filename
  --pdb                 Run pdb on start
```

## Dependencies

- pandas
- trino
- google-auth
- pyarrow

Requires Python >=3.6
