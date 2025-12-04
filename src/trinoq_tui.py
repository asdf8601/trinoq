"""TrinoQ TUI - A terminal user interface for querying Trino databases.

Requires the 'tui' extra:
    pip install trinoq[tui]

Usage:
    trinoq-tui
"""

from __future__ import annotations

import time
from typing import Any

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Vertical
from textual.reactive import reactive, var
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Static,
    TextArea,
    Tree,
)
from textual.widgets.tree import TreeNode


class SchemaTree(Tree):
    """A tree widget for browsing database schema (catalogs/schemas/tables)."""

    def __init__(self) -> None:
        super().__init__("Catalogs", id="schema-tree")
        self.show_root = True

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
        for table in sorted(tables):
            schema_node.add_leaf(
                f"[green]{table}[/green]",
                data={
                    "type": "table",
                    "name": table,
                    "schema": schema_node.data["name"],
                    "catalog": schema_node.data["catalog"],
                },
            )


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


class StatusBar(Static):
    """A status bar widget to display query status."""

    status = reactive("Ready")

    def render(self) -> str:
        return f" {self.status}"


class TrinoQApp(App):
    """A TUI for running Trino queries."""

    TITLE = "TrinoQ"
    SUB_TITLE = "Trino Query Browser"

    CSS = """
    #sidebar {
        width: 30;
        height: 100%;
        dock: left;
        border-right: solid $primary;
    }

    #schema-tree {
        width: 100%;
        height: 100%;
        scrollbar-gutter: stable;
    }

    #main-area {
        width: 100%;
        height: 100%;
        layout: grid;
        grid-size: 1 3;
        grid-rows: 2fr 3fr 1;
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
    """

    BINDINGS = [
        Binding("ctrl+e", "execute_query", "Run Query", show=True, priority=True),
        Binding("ctrl+r", "refresh_schema", "Refresh Schema", show=True, priority=True),
        Binding("ctrl+l", "clear_results", "Clear Results", show=True, priority=True),
        Binding("ctrl+b", "toggle_sidebar", "Toggle Sidebar", show=True, priority=True),
        Binding("ctrl+q", "quit", "Quit", show=True, priority=True),
        Binding("ctrl+w", "focus_next_tab", "Next", show=True, priority=True),
    ]

    show_sidebar = var(True)
    _connection: Any = None
    _focus_order = ["schema-tree", "query-editor", "results-table"]
    _current_focus_idx = 1  # Start on editor

    def compose(self) -> ComposeResult:
        yield Header()
        with Container(id="sidebar"):
            yield SchemaTree()
        with Vertical(id="main-area"):
            yield QueryEditor()
            with Container(id="results-container"):
                yield ResultsTable()
            yield StatusBar(id="status-bar")
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

        self.call_from_thread(setattr, status, "status", "Running query...")

        start_time = time.time()

        try:
            conn = self._get_connection()
            df = pd.read_sql(sql, conn)
            columns = df.columns.tolist()
            rows = [tuple(row) for row in df.values]

            elapsed = time.time() - start_time

            self.call_from_thread(results_table.display_results, columns, rows)
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


def main() -> None:
    """Entry point for the TUI application."""
    app = TrinoQApp()
    app.run()


if __name__ == "__main__":
    main()
