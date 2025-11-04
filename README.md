# TrinoQ

A convenient CLI tool to query data from Trino with built-in caching and Google authentication support.

## Features

- Execute SQL queries directly from command line
- Read queries from files or stdin
- Built-in caching using Parquet format for faster repeated queries
- Google JWT authentication
- Parameter substitution with `@param` annotations
- Environment variable substitution in queries
- DataFrame evaluation with Python expressions using `@eval` annotations
- Dry-run mode to preview rendered queries
- Query execution timing
- Export results to JSON, CSV, or Parquet formats

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

## Query Annotations

TrinoQ supports special annotations in SQL comments to parameterize and enhance your queries.

### Parameter Substitution with @param

Define parameters directly in your SQL files using `@param` annotations:

```sql
-- @param start_date 2024-01-01
-- @param end_date 2024-12-31
-- @param table_name sales_data

SELECT * 
FROM {table_name}
WHERE date >= '{start_date}' 
  AND date <= '{end_date}'
```

**Syntax Options:**
- Single braces: `{param_name}` - Standard Python format syntax
- Double braces: `{{param_name}}` - Jinja2-style syntax

**Important:** Parameter values are used exactly as written. If you need quotes in the SQL, include them in the parameter value:

```sql
-- @param token '7739-9592-01'
SELECT * FROM table WHERE id = {{token}}
-- Result: SELECT * FROM table WHERE id = '7739-9592-01'
```

### Python Evaluation with @eval

Execute Python code on the resulting DataFrame using annotations:

**Inline code:**
```sql
-- @eval print(df.describe())
SELECT * FROM my_table
```

**External file:**
```sql
-- @eval-file analysis.py
SELECT * FROM my_table
```

**Legacy syntax (still supported):**
```sql
-- eval: analysis.py
SELECT * FROM my_table
```

The DataFrame is available as `df` in the evaluation context.

### Environment Variable Substitution

Use environment variables in your queries with `{VAR_NAME}` syntax:

```bash
export TABLE_NAME="my_table"
trinoq "select * from {TABLE_NAME}"
```

**Note:** `@param` values take precedence over environment variables.

## Command-Line Options

### Dry Run

Preview the rendered query without executing it:

```bash
trinoq --dry-run -f query.sql
```

This is useful for debugging template substitutions and verifying your parameters.

### Execution Timing

Measure query execution time:

```bash
trinoq -t "select * from large_table"
# Output: Execution time: 2.345s
```

### Output Formats

Export results in different formats:

**JSON to stdout:**
```bash
trinoq -o json "select * from my_table"
trinoq -o json "select * from my_table" > output.json
```

**CSV to stdout:**
```bash
trinoq -o csv "select * from my_table"
trinoq -o csv "select * from my_table" > output.csv
```

**Parquet file:**
```bash
trinoq -o parquet "select * from my_table"
# Creates: output.parquet
```

### Disable Cache

By default, query results are cached in `/tmp/druidq/`. To disable caching:

```bash
trinoq --no-cache "select * from my_table"
```

### Quiet Mode

Suppress informational output (useful for piping results):

```bash
trinoq --quiet "select * from my_table" -e "print(df.head())"
```

### Command-Line Evaluation

Execute Python code on the resulting DataFrame using the `-e` or `--eval-df` flag:

```bash
trinoq "select * from my_table" -e "print(df.describe())"
```

## Complete Examples

### Example 1: Parameterized Query with Timing

```sql
-- query.sql
-- @param region US
-- @param min_sales 1000

SELECT 
  product_name,
  SUM(sales) as total_sales
FROM sales_table
WHERE region = '{{region}}'
  AND sales > {{min_sales}}
GROUP BY product_name
ORDER BY total_sales DESC
```

```bash
trinoq -t -f query.sql
```

### Example 2: Export with Analysis

```sql
-- report.sql
-- @param month 2024-11
-- @eval print(f"Total rows: {len(df)}\nTotal revenue: ${df['revenue'].sum():,.2f}")

SELECT 
  DATE(order_date) as day,
  SUM(revenue) as revenue
FROM orders
WHERE DATE_FORMAT(order_date, '%Y-%m') = '{{month}}'
GROUP BY DATE(order_date)
ORDER BY day
```

```bash
trinoq -t -o csv -f report.sql > monthly_report.csv
```

### Example 3: Combining Annotations

```sql
-- analysis.sql
-- @param dataset_name production_data
-- @param threshold 100
-- @eval-file complex_analysis.py

SELECT *
FROM {CATALOG}.{{dataset_name}}
WHERE metric > {{threshold}}
```

```bash
export CATALOG="my_catalog"
trinoq --dry-run -f analysis.sql  # Preview first
trinoq -t -f analysis.sql         # Then execute
```

## Command-Line Reference

```
trinoq [-h] [-f] [-n] [-q] [-e EVAL_DF] [-t] [-o {json,csv,parquet}] [--dry-run] [--pdb] query
```

### Positional Arguments

**`query`**
- SQL query string, or use `-` for stdin, or use with `-f` for file
- Supports SQL annotations (see Query Annotations section)

### Optional Arguments

**`-h, --help`**
- Show help message and exit

**`-f, --file`**
- Read query from file
- When used, the `query` argument is treated as a file path
- Example: `trinoq -f query.sql`

**`-n, --no-cache`**
- Do not use query result cache
- By default, results are cached in `/tmp/druidq/` using Parquet format
- Use this flag to force fresh query execution

**`-q, --quiet`**
- Suppress informational output except for eval-df code output
- Useful for piping results or in scripts

**`-e EVAL_DF, --eval-df EVAL_DF`**
- Evaluate Python code on the result DataFrame
- Can be inline code or a file path
- The DataFrame is available as `df` variable
- Example: `trinoq "SELECT * FROM table" -e "print(df.describe())"`
- Note: Can also be specified in SQL using `@eval` or `@eval-file` annotations

**`-t, --timing`**
- Measure and display query execution time
- Outputs: `Execution time: X.XXXs`

**`-o {json,csv,parquet}, --output {json,csv,parquet}`**
- Export results in specified format
- `json`: Output to stdout in JSON format
- `csv`: Output to stdout in CSV format  
- `parquet`: Save to `output.parquet` file

**`--dry-run`**
- Show rendered query without executing it
- Useful for debugging parameter substitution and template rendering
- Displays the final SQL after all `@param` and environment variable substitutions

**`--pdb`**
- Start Python debugger (pdb) on start
- For development/debugging purposes

### SQL Annotations

These annotations can be embedded in SQL comments within your query files:

**`-- @param <key> <value>`**
- Define parameters for query substitution
- Use `{key}` or `{{key}}` in your SQL to reference the parameter
- Example: `-- @param table_name my_table`

**`-- @eval <python_code>`**
- Execute inline Python code on the result DataFrame
- Example: `-- @eval print(df.head())`

**`-- @eval-file <file_path>`**
- Execute Python code from a file on the result DataFrame
- Example: `-- @eval-file analysis.py`
- Legacy syntax also supported: `-- eval: analysis.py`

## Development

### Running Tests

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=term-missing
```

See [tests/README.md](tests/README.md) for more details.

## Dependencies

### Core
- pandas
- trino
- google-auth
- pyarrow

### Development
- pytest>=7.0
- pytest-cov

Requires Python >=3.9
