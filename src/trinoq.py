"""
You need to define a TRINO_URL environment variable with the connection string to the trino server.

$ export TRINO_URL="https://host:443"

Usage:
$ trinoq "select 1"              # Direct query string
$ trinoq -f query.sql            # Read from file
$ echo "select 1" | trinoq -     # Read from stdin
"""

import pandas as pd
import os

from trino.dbapi import Connection


def printer(*args, quiet=False, **kwargs):
    if not quiet:
        print(*args, **kwargs)


def create_connection() -> Connection:
    import warnings
    import google.auth
    from google.auth.transport.requests import Request
    from trino.auth import JWTAuthentication
    from trino.dbapi import connect

    # parse url
    from urllib.parse import urlparse, parse_qs

    # parse url
    trino_url = os.environ["TRINO_URL"]
    parsed_url = urlparse(trino_url)

    host = parsed_url.hostname
    port = parsed_url.port
    http_scheme = parsed_url.scheme

    params = parse_qs(parsed_url.query)
    user = params.get("user", None)
    catalog = params.get("catalog", [None])[0]
    schema = params.get("schema", [None])[0]

    # auth
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        credentials, _ = google.auth.default()
        credentials.refresh(Request())
        auth = JWTAuthentication(credentials.token)

    # connect
    conn = connect(
        auth=auth,
        user=user,
        host=host,
        port=port,
        http_scheme=http_scheme,
        catalog=catalog,
        schema=schema,
    )
    return conn


def extract_params(query: str) -> dict[str, str]:
    """Extract parameters from SQL comments like '-- @param key value'"""
    import re
    
    pattern = r"--\s*@param\s+(\S+)\s+(.+?)(?:\n|$)"
    matches = re.findall(pattern, query, re.IGNORECASE | re.MULTILINE)
    params = {}
    for key, value in matches:
        params[key] = value.strip()
    return params


def extract_eval_code(query: str) -> str | None:
    """Extract eval code from SQL comment like '-- @eval code'"""
    import re
    
    pattern = r"--\s*@eval\s+(.+?)(?:\n|$)"
    match = re.search(pattern, query, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def extract_eval_file(query: str) -> str | None:
    """Extract eval file path from SQL comment like '-- @eval-file file.py' or '-- eval: file.py'"""
    import re
    
    # Try new syntax first: -- @eval-file file.py
    pattern = r"--\s*@eval-file\s+(.+?)(?:\n|$)"
    match = re.search(pattern, query, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fall back to old syntax: -- eval: file.py
    pattern = r"--\s*eval:\s*(.+?)(?:\n|$)"
    match = re.search(pattern, query, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def get_query(args):
    import re

    query_in = args.query
    
    # Read from stdin if query is "-"
    if query_in == "-":
        import sys
        out = sys.stdin.read()
    # Read from file if --file flag is set
    elif args.file:
        with open(query_in, "r") as f:
            out = f.read()
    # Otherwise treat as query string
    else:
        out = query_in

    # Extract @param values from query
    params = extract_params(out)
    
    # format {{{
    # First check for double braces {{key}}
    pattern_double = r"{{([^}]+)}}"
    matches_double = re.findall(pattern_double, out)
    
    # Then check for single braces {key} (only if no double braces found)
    pattern_single = r"(?<!\{){([^}]+)}(?!\})"
    matches_single = re.findall(pattern_single, out) if not matches_double else []
    
    if matches_double:
        # Handle double braces {{key}}
        fmt_values = {}
        for k in matches_double:
            k = k.strip()
            # Try params first, then environment variables
            if k in params:
                fmt_values[k] = params[k]
            else:
                fmt_values[k] = os.environ[k]
        
        # Replace {{key}} with values
        for key, value in fmt_values.items():
            out = re.sub(r'{{\s*' + re.escape(key) + r'\s*}}', value, out)
    
    elif matches_single:
        # Handle single braces {key}
        fmt_values = {}
        for k in matches_single:
            # Try params first, then environment variables
            if k in params:
                fmt_values[k] = params[k]
            else:
                fmt_values[k] = os.environ[k]
        
        # Use standard format for single braces
        out = out.format(**fmt_values)
    # }}}
    return out


def get_args():
    import argparse

    epilog_text = """
SQL Annotations (embed in SQL comments):
  -- @param <key> <value>     Define parameters for query substitution
                              Use {key} or {{key}} in SQL to reference
                              Example: -- @param table_name my_table

  -- @eval <python_code>      Execute inline Python code on result DataFrame
                              Example: -- @eval print(df.head())

  -- @eval-file <file_path>   Execute Python code from file on result DataFrame
                              Example: -- @eval-file analysis.py
                              Legacy: -- eval: analysis.py

Examples:
  trinoq "SELECT 1"                        # Direct query
  trinoq -f query.sql                      # Read from file
  echo "SELECT 1" | trinoq -               # Read from stdin
  trinoq --dry-run -f query.sql            # Preview rendered query
  trinoq -t -o json "SELECT * FROM table"  # Time and export to JSON
"""

    parser = argparse.ArgumentParser(
        description="Query Trino database with built-in caching and parameter support",
        epilog=epilog_text,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("query", help="SQL query string, or use '-' for stdin, or use with -f for file")
    parser.add_argument(
        "-f",
        "--file",
        help="Read query from file",
        action="store_true",
    )
    parser.add_argument(
        "-n",
        "--no-cache",
        help="Do not use cache",
        action="store_true",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        help="Do not printer the output except the code you use in eval-df",
        action="store_true",
    )
    parser.add_argument(
        "-e",
        "--eval-df",
        help="Evaluate 'df' using string or filename",
        default="",
    )

    parser.add_argument(
        "--pdb",
        help="Run pdb on start",
        action="store_true",
    )
    parser.add_argument(
        "--dry-run",
        help="Show rendered query without executing it",
        action="store_true",
    )
    parser.add_argument(
        "-t",
        "--timing",
        help="Measure and display query execution time",
        action="store_true",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output format: json, csv, or parquet",
        choices=["json", "csv", "parquet"],
        default=None,
    )
    return parser.parse_args()


def get_eval_df(args):
    eval_df_in = args.eval_df
    try:
        with open(eval_df_in, "r") as f:
            out = f.read()
    except FileNotFoundError:
        out = eval_df_in
    return out


def get_temp_file(query):
    from hashlib import sha1
    from pathlib import Path

    qhash = sha1(query.encode()).hexdigest()
    temp_file = Path(f"/tmp/druidq/{qhash}.parquet")
    if not temp_file.parent.exists():
        temp_file.parent.mkdir(parents=True, exist_ok=True)

    return temp_file


def read_sql(*args, **kwargs):
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pd.read_sql(*args, **kwargs)


def execute(
    query: str,
    engine: Connection | None = None,
    no_cache: bool = True,
    quiet: bool = True,
):

    if engine is None:
        engine = create_connection()
    if no_cache:
        return read_sql(query, engine)

    # cache {{
    temp_file = get_temp_file(query)
    if temp_file.exists():
        printer(f"Loading cache: {temp_file}", quiet=quiet)
        return pd.read_parquet(temp_file)
    # }}

    df = read_sql(query, engine)

    # cache {{
    try:
        df.to_parquet(temp_file, engine="pyarrow")
    except Exception as e:
        printer(f"Error caching:\n{e}", quiet=quiet)
    else:
        printer(f"Saving cache: {temp_file}", quiet=quiet)
    # }}

    return df


def app():
    import sys
    import time
    
    args = get_args()
    query = get_query(args)
    quiet = args.quiet

    if args.pdb:
        breakpoint()

    # Handle --dry-run flag
    if args.dry_run:
        print(query)
        return

    # Check for eval code in SQL comment (new syntax)
    eval_code_from_query = extract_eval_code(query)
    
    # Check for eval file in SQL comment
    eval_file_from_query = extract_eval_file(query)
    
    # Priority: CLI flag > @eval > @eval-file
    if not args.eval_df:
        if eval_code_from_query:
            args.eval_df = eval_code_from_query
        elif eval_file_from_query:
            args.eval_df = eval_file_from_query

    printer(f"In[query]:\n{query}", quiet=quiet)

    # Measure execution time if --timing flag is set
    start_time = time.time()
    
    df = execute(query=query, no_cache=args.no_cache, quiet=quiet)
    
    if args.timing:
        elapsed_time = time.time() - start_time
        printer(f"\nExecution time: {elapsed_time:.3f}s", quiet=quiet)

    # Handle output formats
    if args.output:
        if args.output == "json":
            print(df.to_json(orient="records", indent=2))
        elif args.output == "csv":
            print(df.to_csv(index=False))
        elif args.output == "parquet":
            output_file = "output.parquet"
            df.to_parquet(output_file, engine="pyarrow")
            printer(f"\nSaved to: {output_file}", quiet=False)
    else:
        printer(f"\nOut[df]:\n{df.to_string()}", quiet=quiet)

    if args.eval_df:
        eval_df = get_eval_df(args)
        printer(f"\nIn[eval]:\n{eval_df}", quiet=quiet)

        printer("Out[eval]:", quiet=quiet)
        exec(eval_df, globals(), locals())


if __name__ == "__main__":
    app()
