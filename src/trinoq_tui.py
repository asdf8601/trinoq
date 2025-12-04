"""TrinoQ TUI - A terminal user interface for querying Trino databases.

Requires the 'tui' extra:
    pip install trinoq[tui]

Usage:
    trinoq-tui
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive, var
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    OptionList,
    Static,
    TextArea,
    Tree,
)
from textual.widgets.option_list import Option
from textual.widgets.tree import TreeNode

# Cache file for tables
CACHE_DIR = Path("/tmp/trinoq")
TABLES_CACHE_FILE = CACHE_DIR / "tables_cache.json"
QUERIES_FILE = CACHE_DIR / "saved_queries.json"
CACHE_MAX_AGE_SECONDS = 3600  # Refresh cache if older than 1 hour


def load_tables_cache() -> list[dict]:
    """Load tables from cache file."""
    try:
        if TABLES_CACHE_FILE.exists():
            with open(TABLES_CACHE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return []


def save_tables_cache(tables: list[dict]) -> None:
    """Save tables to cache file."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(TABLES_CACHE_FILE, "w") as f:
            json.dump(tables, f)
    except Exception:
        pass


def cache_needs_refresh() -> bool:
    """Check if cache is stale and needs refresh."""
    try:
        if not TABLES_CACHE_FILE.exists():
            return True
        age = time.time() - TABLES_CACHE_FILE.stat().st_mtime
        return age > CACHE_MAX_AGE_SECONDS
    except Exception:
        return True


def load_saved_queries() -> list[dict]:
    """Load saved queries from file."""
    try:
        if QUERIES_FILE.exists():
            with open(QUERIES_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return []


def save_queries(queries: list[dict]) -> None:
    """Save queries to file."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(QUERIES_FILE, "w") as f:
            json.dump(queries, f, indent=2)
    except Exception:
        pass


def fuzzy_match(pattern: str, text: str) -> tuple[bool, int]:
    """Fuzzy matching - returns (matched, score). Higher score = better match."""
    pattern = pattern.lower()
    text = text.lower()

    if not pattern:
        return True, 0

    p_idx = 0
    score = 0
    prev_match = -1

    for i, char in enumerate(text):
        if p_idx < len(pattern) and char == pattern[p_idx]:
            # Bonus for consecutive matches
            if prev_match == i - 1:
                score += 10
            # Bonus for matching at start or after separator
            if i == 0 or text[i - 1] in "._-":
                score += 5
            score += 1
            prev_match = i
            p_idx += 1

    matched = p_idx == len(pattern)
    return matched, score if matched else 0


class SchemaTree(Tree):
    """A tree widget for browsing database schema (catalogs/schemas/tables)."""

    def __init__(self) -> None:
        super().__init__("Catalogs", id="schema-tree")
        self.show_root = True
        self._all_tables: list[dict] = []  # Store all tables for filtering

    def populate_catalogs(self, catalogs: list[str]) -> None:
        """Populate the tree with catalog names."""
        self.clear()
        for catalog in sorted(catalogs):
            node = self.root.add(
                f"[bold]{catalog}[/bold]", data={"type": "catalog", "name": catalog}
            )
            # Add placeholder for lazy loading
            node.add_leaf("[dim]Loading...[/dim]", data={"type": "placeholder"})

    def populate_schemas(self, catalog_node: TreeNode, schemas: list[str]) -> None:
        """Populate a catalog node with schema names."""
        # Remove placeholder
        catalog_node.remove_children()
        for schema in sorted(schemas):
            catalog_name = catalog_node.data["name"]
            node = catalog_node.add(
                f"[cyan]{schema}[/cyan]",
                data={"type": "schema", "name": schema, "catalog": catalog_name},
            )
            # Add placeholder for lazy loading
            node.add_leaf("[dim]Loading...[/dim]", data={"type": "placeholder"})

    def populate_tables(self, schema_node: TreeNode, tables: list[str]) -> None:
        """Populate a schema node with table names."""
        # Remove placeholder
        schema_node.remove_children()
        catalog = schema_node.data["catalog"]
        schema_name = schema_node.data["name"]
        for table in sorted(tables):
            table_data = {
                "type": "table",
                "name": table,
                "schema": schema_name,
                "catalog": catalog,
            }
            schema_node.add_leaf(f"[green]{table}[/green]", data=table_data)
            # Store for filtering
            self._all_tables.append(table_data)

    def get_all_tables(self) -> list[dict]:
        """Get all loaded tables."""
        return self._all_tables

    def find_table_node(self, catalog: str, schema: str, table: str) -> TreeNode | None:
        """Find a table node by its full path."""
        for catalog_node in self.root.children:
            if catalog_node.data and catalog_node.data.get("name") == catalog:
                for schema_node in catalog_node.children:
                    if schema_node.data and schema_node.data.get("name") == schema:
                        for table_node in schema_node.children:
                            if table_node.data and table_node.data.get("name") == table:
                                return table_node
        return None


class QueryEditor(TextArea):
    """A TextArea configured for SQL editing."""

    def __init__(self) -> None:
        super().__init__(
            language="sql",
            show_line_numbers=True,
            tab_behavior="indent",
            id="query-editor",
        )
        self.text = "-- Enter your SQL query here\nSELECT 1 AS test"


class ResultsTable(DataTable):
    """A DataTable for displaying query results."""

    def __init__(self) -> None:
        super().__init__(id="results-table", zebra_stripes=True)
        self.cursor_type = "row"

    def display_results(self, columns: list[str], rows: list[tuple]) -> None:
        """Display query results in the table."""
        self.clear(columns=True)
        if columns:
            self.add_columns(*columns)
        for row in rows:
            # Convert all values to strings for display
            self.add_row(*[str(v) if v is not None else "NULL" for v in row])

    def display_error(self, error: str) -> None:
        """Display an error message in the table."""
        self.clear(columns=True)
        self.add_column("Error")
        self.add_row(str(error))


class SearchPopup(Container):
    """Floating search popup with input and results list."""

    BINDINGS = [
        Binding("escape", "cancel_search", "Cancel", show=False, priority=True),
    ]

    def __init__(self) -> None:
        super().__init__(id="search-popup")
        self.matches: list[dict] = []
        self.all_tables: list[dict] = []  # Cache of all tables

    def compose(self) -> ComposeResult:
        yield Input(placeholder="Search tables (fuzzy)...", id="search-input")
        yield OptionList(id="search-results")

    def action_cancel_search(self) -> None:
        self.app.action_hide_search()

    def update_results(self, matches: list[dict]) -> None:
        """Update the results list."""
        self.matches = matches
        results = self.query_one("#search-results", OptionList)
        results.clear_options()
        for match in matches:
            full_name = f"{match['catalog']}.{match['schema']}.{match['name']}"
            results.add_option(Option(full_name, id=full_name))
        if matches:
            results.highlighted = 0

    def filter_tables(self, pattern: str) -> list[dict]:
        """Filter cached tables using fuzzy matching."""
        if not pattern:
            return self.all_tables[:50]

        scored = []
        for table in self.all_tables:
            # Match against table name and full path
            full_name = f"{table['schema']}.{table['name']}"
            matched, score = fuzzy_match(pattern, table["name"])
            if matched:
                scored.append((score, table))
            else:
                matched, score = fuzzy_match(pattern, full_name)
                if matched:
                    scored.append((score, table))

        # Sort by score descending
        scored.sort(key=lambda x: -x[0])
        return [t for _, t in scored[:50]]


class QueriesPopup(Container):
    """Floating popup for saved queries."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False, priority=True),
        Binding("delete", "delete_query", "Delete", show=False, priority=True),
    ]

    def __init__(self) -> None:
        super().__init__(id="queries-popup")
        self.queries: list[dict] = []

    def compose(self) -> ComposeResult:
        yield Static("Saved Queries (Enter=load, Del=delete)", id="queries-title")
        yield Input(placeholder="Filter queries...", id="queries-filter")
        yield OptionList(id="queries-list")

    def action_cancel(self) -> None:
        self.app.action_hide_queries()

    def action_delete_query(self) -> None:
        self.app.action_delete_selected_query()

    def load_queries(self) -> None:
        """Load and display saved queries."""
        self.queries = load_saved_queries()
        self._update_list("")

    def _update_list(self, filter_text: str) -> None:
        """Update the queries list with optional filter."""
        results = self.query_one("#queries-list", OptionList)
        results.clear_options()

        for i, q in enumerate(self.queries):
            name = q.get("name", f"Query {i + 1}")
            sql_preview = q.get("sql", "")[:50].replace("\n", " ")
            if filter_text and filter_text.lower() not in name.lower():
                continue
            results.add_option(Option(f"{name}: {sql_preview}...", id=str(i)))

        if self.queries and results.option_count > 0:
            results.highlighted = 0


class SaveQueryPopup(Container):
    """Floating popup for naming a query before saving."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False, priority=True),
    ]

    def __init__(self) -> None:
        super().__init__(id="save-query-popup")
        self.sql_to_save: str = ""

    def compose(self) -> ComposeResult:
        yield Static("Save Query", id="save-query-title")
        yield Input(placeholder="Enter query name...", id="save-query-name")

    def action_cancel(self) -> None:
        self.app.action_hide_save_query()

    def set_default_name(self, sql: str) -> None:
        """Set a default name based on the SQL."""
        self.sql_to_save = sql
        first_line = sql.split("\n")[0].strip()
        if first_line.startswith("--"):
            default_name = first_line[2:].strip()[:50]
        else:
            default_name = sql[:30].replace("\n", " ")
        self.query_one("#save-query-name", Input).value = default_name


class StatusBar(Static):
    """A status bar widget to display query status with spinner."""

    status = reactive("Ready")
    is_running = reactive(False)
    _spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    _spinner_idx = 0

    def on_mount(self) -> None:
        """Start spinner animation."""
        self.set_interval(0.1, self._update_spinner)

    def _update_spinner(self) -> None:
        """Update spinner frame."""
        if self.is_running:
            self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_frames)
            self.refresh()

    def render(self) -> str:
        if self.is_running:
            spinner = self._spinner_frames[self._spinner_idx]
            return f" {spinner} {self.status}"
        return f" {self.status}"


class TrinoQApp(App):
    """A TUI for running Trino queries."""

    TITLE = "TrinoQ"
    SUB_TITLE = "Trino Query Browser"

    CSS = """
    Screen {
        layout: vertical;
        layers: base popup;
    }

    #content-area {
        width: 100%;
        height: 1fr;
    }

    #sidebar {
        width: 30;
        height: 100%;
        border-right: solid $primary;
    }

    #schema-tree {
        width: 100%;
        height: 100%;
        scrollbar-gutter: stable;
    }

    #main-area {
        width: 1fr;
        height: 100%;
        layout: grid;
        grid-size: 1 2;
        grid-rows: 2fr 3fr;
    }

    #query-editor {
        height: 100%;
        border: round $primary;
    }

    #results-container {
        height: 100%;
        border: round $secondary;
    }

    #results-table {
        height: 100%;
    }

    #status-bar {
        width: 100%;
        height: 1;
        background: $surface;
        color: $text-muted;
        padding: 0 1;
    }

    #results-container:focus-within {
        border: round $accent;
    }

    #query-editor:focus-within {
        border: round $accent;
    }

    #search-popup {
        display: none;
        layer: popup;
        width: 70%;
        height: auto;
        max-height: 20;
        background: $surface;
        border: round $surface-lighten-2;
        padding: 1 2;
        align: center top;
        margin: 3 0 0 0;
    }

    #search-popup.visible {
        display: block;
    }

    #search-input {
        width: 100%;
        border: none;
        background: $surface;
    }

    #search-results {
        width: 100%;
        height: auto;
        max-height: 15;
        border: none;
        background: $surface;
        margin-top: 1;
    }

    #queries-popup {
        display: none;
        layer: popup;
        width: 70%;
        height: auto;
        max-height: 22;
        background: $surface;
        border: round $surface-lighten-2;
        padding: 1 2;
        align: center top;
        margin: 3 0 0 0;
    }

    #queries-popup.visible {
        display: block;
    }

    #queries-title {
        width: 100%;
        text-align: center;
        color: $text-muted;
        margin-bottom: 1;
    }

    #queries-filter {
        width: 100%;
        border: none;
        background: $surface;
    }

    #queries-list {
        width: 100%;
        height: auto;
        max-height: 15;
        border: none;
        background: $surface;
        margin-top: 1;
    }

    #save-query-popup {
        display: none;
        layer: popup;
        width: 50%;
        height: auto;
        background: $surface;
        border: round $surface-lighten-2;
        padding: 1 2;
        align: center top;
        margin: 5 0 0 0;
    }

    #save-query-popup.visible {
        display: block;
    }

    #save-query-title {
        width: 100%;
        text-align: center;
        color: $text-muted;
        margin-bottom: 1;
    }

    #save-query-name {
        width: 100%;
        border: solid $primary;
        background: $surface;
    }
    """

    BINDINGS = [
        Binding("ctrl+e", "execute_query", "Run", show=True, priority=True),
        Binding("ctrl+s", "save_query", "Save", show=True, priority=True),
        Binding("ctrl+o", "show_queries", "Open", show=True, priority=True),
        Binding("ctrl+r", "refresh_schema", "Refresh", show=True, priority=True),
        Binding("ctrl+l", "clear_results", "Clear", show=True, priority=True),
        Binding("ctrl+b", "toggle_sidebar", "Sidebar", show=True, priority=True),
        Binding("ctrl+q", "quit", "Quit", show=True, priority=True),
        Binding("ctrl+w", "focus_next_tab", "Next", show=True, priority=True),
        Binding("slash", "show_search", "/ Search", show=True, priority=True),
    ]

    show_sidebar = var(True)
    _connection: Any = None
    _focus_order = ["schema-tree", "query-editor", "results-table"]
    _current_focus_idx = 1  # Start on editor

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="content-area"):
            with Container(id="sidebar"):
                yield SchemaTree()
            with Vertical(id="main-area"):
                yield QueryEditor()
                with Container(id="results-container"):
                    yield ResultsTable()
        yield StatusBar(id="status-bar")
        yield SearchPopup()
        yield QueriesPopup()
        yield SaveQueryPopup()
        yield Footer()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self.query_one(QueryEditor).focus()
        self._load_catalogs()

    def watch_show_sidebar(self, show_sidebar: bool) -> None:
        """Toggle the sidebar visibility."""
        self.query_one("#sidebar").display = show_sidebar

    def _get_connection(self) -> Any:
        """Get or create a Trino connection."""
        if self._connection is None:
            from trinoq import create_connection

            self._connection = create_connection()
        return self._connection

    @work(thread=True, exclusive=True, group="schema")
    def _load_catalogs(self) -> None:
        """Load catalogs in a background thread."""
        import pandas as pd

        status = self.query_one(StatusBar)
        tree = self.query_one(SchemaTree)

        self.call_from_thread(setattr, status, "status", "Loading catalogs...")

        try:
            conn = self._get_connection()
            df = pd.read_sql("SHOW CATALOGS", conn)
            catalogs = df.iloc[:, 0].tolist()

            self.call_from_thread(tree.populate_catalogs, catalogs)
            self.call_from_thread(
                setattr, status, "status", f"Loaded {len(catalogs)} catalogs"
            )
        except Exception as e:
            self.call_from_thread(setattr, status, "status", f"Error: {e}")
            self.call_from_thread(
                self.notify, f"Failed to load catalogs: {e}", severity="error"
            )

    @work(thread=True, group="fetch")
    def _load_schemas(self, node: TreeNode, catalog: str) -> None:
        """Load schemas for a catalog in a background thread."""
        import pandas as pd

        status = self.query_one(StatusBar)
        tree = self.query_one(SchemaTree)

        self.call_from_thread(
            setattr, status, "status", f"Loading schemas for {catalog}..."
        )

        try:
            conn = self._get_connection()
            df = pd.read_sql(f"SHOW SCHEMAS FROM {catalog}", conn)
            schemas = df.iloc[:, 0].tolist()

            self.call_from_thread(tree.populate_schemas, node, schemas)
            self.call_from_thread(
                setattr, status, "status", f"Loaded {len(schemas)} schemas"
            )
        except Exception as e:
            self.call_from_thread(setattr, status, "status", f"Error: {e}")

    @work(thread=True, group="fetch")
    def _load_tables(self, node: TreeNode, catalog: str, schema: str) -> None:
        """Load tables for a schema in a background thread."""
        import pandas as pd

        status = self.query_one(StatusBar)
        tree = self.query_one(SchemaTree)

        self.call_from_thread(
            setattr, status, "status", f"Loading tables for {catalog}.{schema}..."
        )

        try:
            conn = self._get_connection()
            df = pd.read_sql(f"SHOW TABLES FROM {catalog}.{schema}", conn)
            tables = df.iloc[:, 0].tolist()

            self.call_from_thread(tree.populate_tables, node, tables)
            self.call_from_thread(
                setattr, status, "status", f"Loaded {len(tables)} tables"
            )
        except Exception as e:
            self.call_from_thread(setattr, status, "status", f"Error: {e}")

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        """Handle tree node expansion for lazy loading."""
        node = event.node
        if node.data is None:
            return

        node_type = node.data.get("type")

        # Check if it has a placeholder child (needs loading)
        has_placeholder = (
            node.children
            and node.children[0].data
            and node.children[0].data.get("type") == "placeholder"
        )

        if has_placeholder:
            if node_type == "catalog":
                self._load_schemas(node, node.data["name"])
            elif node_type == "schema":
                self._load_tables(node, node.data["catalog"], node.data["name"])

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle tree node selection - insert table name into editor."""
        node = event.node
        if node.data is None:
            return

        if node.data.get("type") == "table":
            catalog = node.data["catalog"]
            schema = node.data["schema"]
            table = node.data["name"]
            full_name = f"{catalog}.{schema}.{table}"

            editor = self.query_one(QueryEditor)
            editor.insert(full_name)
            editor.focus()

    @work(thread=True, exclusive=True, group="query")
    def _execute_query(self, sql: str) -> None:
        """Execute the SQL query in a background thread."""
        import pandas as pd

        status = self.query_one(StatusBar)
        results_table = self.query_one(ResultsTable)

        self.call_from_thread(setattr, status, "is_running", True)
        self.call_from_thread(setattr, status, "status", "Running query...")

        start_time = time.time()

        try:
            conn = self._get_connection()
            df = pd.read_sql(sql, conn)
            columns = df.columns.tolist()
            rows = [tuple(row) for row in df.values]

            elapsed = time.time() - start_time

            self.call_from_thread(results_table.display_results, columns, rows)
            self.call_from_thread(setattr, status, "is_running", False)
            self.call_from_thread(
                setattr,
                status,
                "status",
                f"Query completed: {len(rows)} rows in {elapsed:.2f}s",
            )
            self.call_from_thread(
                self.notify, f"Query returned {len(rows)} rows", severity="information"
            )

        except Exception as e:
            self.call_from_thread(setattr, status, "is_running", False)
            self.call_from_thread(results_table.display_error, str(e))
            self.call_from_thread(setattr, status, "status", f"Query failed: {e}")
            self.call_from_thread(self.notify, f"Query failed: {e}", severity="error")

    def action_execute_query(self) -> None:
        """Execute the current query."""
        editor = self.query_one(QueryEditor)
        sql = editor.selected_text if editor.selected_text else editor.text

        if not sql or not sql.strip():
            self.notify("No query to execute", severity="warning")
            return

        self._execute_query(sql.strip())

    def action_clear_results(self) -> None:
        """Clear the results table."""
        results_table = self.query_one(ResultsTable)
        results_table.clear(columns=True)
        self.query_one(StatusBar).status = "Results cleared"

    def action_toggle_sidebar(self) -> None:
        """Toggle the sidebar visibility."""
        self.show_sidebar = not self.show_sidebar

    def action_focus_tree(self) -> None:
        """Focus the schema tree."""
        self.query_one(SchemaTree).focus()

    def action_focus_editor(self) -> None:
        """Focus the query editor."""
        self.query_one(QueryEditor).focus()

    def action_focus_results(self) -> None:
        """Focus the results table."""
        self.query_one(ResultsTable).focus()

    def action_focus_next_tab(self) -> None:
        """Focus the next tab."""
        self._current_focus_idx = (self._current_focus_idx + 1) % len(self._focus_order)
        widget_id = self._focus_order[self._current_focus_idx]
        self.query_one(f"#{widget_id}").focus()
        self.notify(f"Focused: {widget_id}")

    def action_focus_prev_tab(self) -> None:
        """Focus the previous tab (gT vim-style)."""
        self._current_focus_idx = (self._current_focus_idx - 1) % len(self._focus_order)
        widget_id = self._focus_order[self._current_focus_idx]
        self.query_one(f"#{widget_id}").focus()

    def action_refresh_schema(self) -> None:
        """Trigger schema refresh."""
        self._load_catalogs()

    def action_show_search(self) -> None:
        """Show the table search popup."""
        popup = self.query_one(SearchPopup)
        popup.add_class("visible")
        popup.query_one("#search-input", Input).value = ""
        popup.query_one("#search-results", OptionList).clear_options()
        popup.matches = []
        popup.query_one("#search-input", Input).focus()

        # Load from cache first (instant)
        if not popup.all_tables:
            cached = load_tables_cache()
            if cached:
                popup.all_tables = cached
                popup.update_results(cached[:50])
                self.query_one(
                    StatusBar
                ).status = f"Loaded {len(cached)} tables from cache"

        # Refresh in background if cache is stale
        if cache_needs_refresh():
            self._load_all_tables()

    def action_hide_search(self) -> None:
        """Hide the table search popup."""
        popup = self.query_one(SearchPopup)
        popup.remove_class("visible")
        self.query_one(QueryEditor).focus()

    def action_save_query(self) -> None:
        """Show the save query popup."""
        editor = self.query_one(QueryEditor)
        sql = editor.text.strip()

        if not sql:
            self.notify("No query to save", severity="warning")
            return

        popup = self.query_one(SaveQueryPopup)
        popup.set_default_name(sql)
        popup.add_class("visible")
        popup.query_one("#save-query-name", Input).focus()

    def action_hide_save_query(self) -> None:
        """Hide the save query popup."""
        popup = self.query_one(SaveQueryPopup)
        popup.remove_class("visible")
        self.query_one(QueryEditor).focus()

    def _do_save_query(self, name: str, sql: str) -> None:
        """Actually save the query with the given name."""
        queries = load_saved_queries()
        queries.insert(0, {"name": name, "sql": sql, "saved_at": time.time()})
        # Keep only last 50 queries
        queries = queries[:50]
        save_queries(queries)
        self.notify(f"Query saved: {name}", severity="information")

    def action_show_queries(self) -> None:
        """Show the saved queries popup."""
        popup = self.query_one(QueriesPopup)
        popup.add_class("visible")
        popup.load_queries()
        popup.query_one("#queries-filter", Input).value = ""
        popup.query_one("#queries-filter", Input).focus()

    def action_hide_queries(self) -> None:
        """Hide the queries popup."""
        popup = self.query_one(QueriesPopup)
        popup.remove_class("visible")
        self.query_one(QueryEditor).focus()

    def action_delete_selected_query(self) -> None:
        """Delete the selected query."""
        popup = self.query_one(QueriesPopup)
        results = popup.query_one("#queries-list", OptionList)

        if results.highlighted is not None and results.highlighted < len(popup.queries):
            idx = results.highlighted
            deleted = popup.queries.pop(idx)
            save_queries(popup.queries)
            popup._update_list(popup.query_one("#queries-filter", Input).value)
            self.notify(f"Deleted: {deleted.get('name', 'query')}", severity="warning")

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        """Handle selection from popup lists."""
        if event.option_list.id == "search-results":
            popup = self.query_one(SearchPopup)
            if event.option_index < len(popup.matches):
                match = popup.matches[event.option_index]
                full_name = f"{match['catalog']}.{match['schema']}.{match['name']}"
                editor = self.query_one(QueryEditor)
                editor.insert(full_name)
            self.action_hide_search()

        elif event.option_list.id == "queries-list":
            popup = self.query_one(QueriesPopup)
            if event.option_index < len(popup.queries):
                query = popup.queries[event.option_index]
                editor = self.query_one(QueryEditor)
                editor.text = query.get("sql", "")
                self.notify(f"Loaded: {query.get('name', 'query')}")
            self.action_hide_queries()

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle input changes in popups."""
        if event.input.id == "search-input":
            popup = self.query_one(SearchPopup)
            pattern = event.value.strip()

            status = self.query_one(StatusBar)

            if not popup.all_tables:
                status.status = "Loading tables, please wait..."
                return

            # Filter locally using fuzzy match
            matches = popup.filter_tables(pattern)
            popup.update_results(matches)

            if matches:
                status.status = (
                    f"Found {len(matches)} of {len(popup.all_tables)} tables"
                )
            else:
                status.status = f"No matches in {len(popup.all_tables)} tables"

        elif event.input.id == "queries-filter":
            popup = self.query_one(QueriesPopup)
            popup._update_list(event.value.strip())

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter key in input fields."""
        if event.input.id == "save-query-name":
            popup = self.query_one(SaveQueryPopup)
            name = event.value.strip()
            if name and popup.sql_to_save:
                self._do_save_query(name, popup.sql_to_save)
                self.action_hide_save_query()
            elif not name:
                self.notify("Please enter a name", severity="warning")

    @work(thread=True, exclusive=True, group="load_tables")
    def _load_all_tables(self) -> None:
        """Load all tables from Trino for fuzzy search."""
        import pandas as pd

        popup = self.query_one(SearchPopup)
        status = self.query_one(StatusBar)

        self.call_from_thread(setattr, status, "status", "Refreshing tables...")

        try:
            conn = self._get_connection()

            # Get all catalogs
            catalogs_df = pd.read_sql("SHOW CATALOGS", conn)
            catalogs = catalogs_df.iloc[:, 0].tolist()

            all_tables = []

            for catalog in catalogs:
                if catalog == "system":
                    continue
                try:
                    query = f"""
                        SELECT table_catalog, table_schema, table_name 
                        FROM {catalog}.information_schema.tables 
                        LIMIT 500
                    """
                    df = pd.read_sql(query, conn)
                    for row in df.itertuples():
                        all_tables.append(
                            {"catalog": row[1], "schema": row[2], "name": row[3]}
                        )
                except Exception:
                    pass

            # Save to cache file
            save_tables_cache(all_tables)

            def update_cache():
                popup.all_tables = all_tables
                # Only update results if popup is visible and no filter applied
                if popup.has_class("visible"):
                    search_input = popup.query_one("#search-input", Input)
                    if not search_input.value.strip():
                        popup.update_results(all_tables[:50])
                status.status = f"Refreshed {len(all_tables)} tables"

            self.call_from_thread(update_cache)

        except Exception as e:
            self.call_from_thread(setattr, status, "status", f"Load error: {e}")


def main() -> None:
    """Entry point for the TUI application."""
    app = TrinoQApp()
    app.run()


if __name__ == "__main__":
    main()
