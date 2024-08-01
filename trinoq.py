"""
You need to define a TRINO_URL environment variable with the connection string to the trino server.

$ export TRINO_URL="https://host:443?user=user@google.com"
$ trinoq "select 1"
"""
import pandas as pd
from rich import print as rprint
import os

def printer(*args, quiet=False, **kwargs):
    if not quiet:
        rprint(*args, **kwargs)

def create_connection():
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
    user = params["user"][0]
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


def get_query(args):

    def find_fmt_keys(s: str) -> list[str] | None:
        import re
        pattern = r"{[^}]+}"
        matches = re.findall(pattern, s)
        return matches

    query_in = args.query
    try:
        with open(query_in, "r") as f:
            out = f.read()
    except FileNotFoundError:
        out = query_in

    # format {{{
    fmt_keys = find_fmt_keys(out)
    if fmt_keys:
        fmt_values = {}
        for key in fmt_keys:
            k = key[1:-1]
            fmt_values[k] = os.environ[k]
        out = out.format(**fmt_values)
    # }}}
    return out


def get_args():
    import argparse

    parser = argparse.ArgumentParser(description="Query")
    parser.add_argument("query", help="query or filename with query")
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


def execute(query, engine, no_cache=False, quiet=False):
    import warnings
    if no_cache:
        return pd.read_sql(query, engine)

    # cache {{
    temp_file = get_temp_file(query)
    if temp_file.exists():
        printer(f"Loading cache: {temp_file}", quiet=quiet)
        return pd.read_parquet(temp_file)
    # }}

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_sql(query, engine)

    # cache {{
    try:
        df.to_parquet(temp_file)
    except Exception as e:
        printer(f"Error caching:\n{e}", quiet=quiet)
    else:
        printer(f"Saving cache: {temp_file}", quiet=quiet)
    # }}

    return df


def app():
    args = get_args()
    query = get_query(args)
    quiet = args.quiet

    if args.pdb:
        breakpoint()

    printer("In[query]:", quiet=quiet)
    printer(query, quiet=quiet)

    engine = create_connection()
    df = execute(query, engine, args.no_cache, quiet=quiet)

    printer(quiet=quiet)
    printer("Out[df]:", quiet=quiet)
    printer(df, quiet=quiet)

    if args.eval_df:
        eval_df = get_eval_df(args)
        printer(quiet=quiet)
        printer("In[eval]:", quiet=quiet)
        printer(eval_df, quiet=quiet)

        printer("Out[eval]:", quiet=quiet)
        exec(eval_df, globals(), locals())


if __name__ == "__main__":
    app()
