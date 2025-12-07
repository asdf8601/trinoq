"""TrinoQ TUI - A terminal user interface for querying Trino databases.

Requires the 'tui' extra:
    pip install trinoq[tui]

Usage:
    trinoq-tui
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import pty
import struct
import termios
import time
from pathlib import Path
from typing import Any

import pyte
from rich.text import Text
from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.command import Hit, Hits, Provider
from textual.containers import Container, Horizontal, Vertical
from textual.message import Message
from textual.reactive import reactive, var
from textual.widget import Widget
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    OptionList,
    Static,
)
from textual.widgets.option_list import Option

# Cache file for tables
CACHE_DIR = Path("/tmp/trinoq")
TABLES_CACHE_FILE = CACHE_DIR / "tables_cache.json"
QUERIES_FILE = CACHE_DIR / "saved_queries.json"
CACHE_MAX_AGE_SECONDS = 3600  # Refresh cache if older than 1 hour

# Key mappings for terminal
CTRL_KEYS = {
    "left": "\x1b[D",
    "right": "\x1b[C",
    "up": "\x1b[A",
    "down": "\x1b[B",
    "home": "\x1b[H",
    "end": "\x1b[F",
    "pageup": "\x1b[5~",
    "pagedown": "\x1b[6~",
    "delete": "\x1b[3~",
    "escape": "\x1b",
    "enter": "\r",
    "backspace": "\x7f",
    "tab": "\t",
}


def _pyte_color_to_rich(color: str) -> str:
    """Convert pyte color to Rich color format."""
    if color == "default":
        return ""
    if color == "brown":
        return "yellow"
    # If it looks like a hex color (6 chars, all hex), add #
    if len(color) == 6 and all(c in "0123456789abcdefABCDEF" for c in color):
        return f"#{color}"
    return color


class PyteDisplay:
    """Rich-compatible display for pyte screen content."""

    def __init__(self, lines: list[Text]) -> None:
        self.lines = lines

    def __rich_console__(self, console, options):
        for line in self.lines:
            yield line


class VimEditor(Widget, can_focus=True):
    """Embedded vim/nvim editor using pyte terminal emulation."""

    DEFAULT_CSS = """
    VimEditor {
        width: 100%;
        height: 100%;
        background: #1e1e1e;
        color: #f0f0f0;
    }
    """

    class Closed(Message):
        """Message sent when vim exits."""

        def __init__(self, content: str, editor_id: str | None = None) -> None:
            self.content = content
            self.editor_id = editor_id
            super().__init__()

    class AreaSelectRequested(Message):
        """Message sent when double-escape is detected."""

        pass

    def __init__(
        self,
        *,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
        auto_start: bool = False,
        initial_content: str = "",
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self._display = PyteDisplay([Text()])
        self._screen: pyte.Screen | None = None
        self._stream: pyte.Stream | None = None
        self._fd: int | None = None
        self._p_out = None
        self._temp_file: Path | None = None
        self._vim_running = False
        self._size_set = asyncio.Event()
        self._data_or_disconnect = None
        self._event = asyncio.Event()
        self._background_tasks: set = set()
        self._last_escape_time: float = 0  # For detecting double-escape
        self._initial_content: str = initial_content
        self._auto_start = auto_start
        self._content: str = initial_content  # Store current content for access
        self._started = False  # Track if vim has been started
        self._vim_paused = False  # Pause vim input for area selection mode

    @property
    def content(self) -> str:
        """Get current editor content."""
        # If vim is running, read from temp file (auto-saved on InsertLeave)
        if self._vim_running and self._temp_file and self._temp_file.exists():
            return self._temp_file.read_text()
        return self._content

    @content.setter
    def content(self, value: str) -> None:
        """Set editor content (will be used on next vim start)."""
        self._content = value
        self._initial_content = value

    def append_text(self, text: str) -> None:
        """Append text to vim buffer directly."""
        if self._vim_running and self._p_out is not None:
            # Send vim commands: Esc, Go (go to end, new line, insert mode), text, Esc
            try:
                # Escape to ensure normal mode, then Go to append at end
                commands = f"\x1bGo{text}\x1b"
                self._p_out.write(commands.encode())
            except Exception:
                pass
        # Also update internal content
        if self._content and not self._content.endswith("\n"):
            self._content += "\n"
        self._content += text
        self._initial_content = self._content

    def _start_vim(self) -> None:
        """Start vim with current content."""
        if self._vim_running:
            return
        self._initial_content = self._content
        self._vim_running = True
        self.focus()
        self._run_vim_worker()

    @work(exclusive=False)
    async def _run_vim_worker(self) -> None:
        """Worker to run both vim tasks concurrently."""
        await asyncio.gather(self._run(), self._send())

    def render(self):
        return self._display

    def on_mount(self) -> None:
        """Called when widget is mounted - start vim if auto_start is set."""
        if self._auto_start and not self._started:
            self._started = True
            # Start vim in a worker - it will wait for _size_set internally
            self._start_vim()

    def on_resize(self, event: events.Resize) -> None:
        """Handle resize events."""
        # Subtract 2 for border (top + bottom) from height
        # Subtract 2 for border (left + right) from width
        ncol = max(1, event.size.width - 2)
        nrow = max(1, event.size.height - 2)
        if ncol > 0 and nrow > 0:
            self._screen = pyte.Screen(ncol, nrow)
            self._stream = pyte.Stream(self._screen)
            self._size_set.set()
            # Update pty size if running
            if self._fd is not None:
                try:
                    winsize = struct.pack("HH", nrow, ncol)
                    fcntl.ioctl(self._fd, termios.TIOCSWINSZ, winsize)
                except OSError:
                    pass

    async def on_key(self, event: events.Key) -> None:
        """Handle key events and forward to vim."""
        if not self._vim_running or self._p_out is None:
            return

        # Don't capture keys when paused (area selection mode)
        if self._vim_paused:
            return

        # Detect double-escape for area selection mode
        if event.key == "escape":
            current_time = time.time()
            if current_time - self._last_escape_time < 0.4:
                # Double escape detected - request area selection mode
                self._last_escape_time = 0
                self.post_message(self.AreaSelectRequested())
                event.stop()
                return
            self._last_escape_time = current_time

        event.stop()

        char = None

        # Handle ctrl+key combinations
        if event.key.startswith("ctrl+"):
            key_char = event.key[-1]
            if key_char.isalpha():
                char = chr(ord(key_char.lower()) - ord("a") + 1)
        else:
            # Handle mapped special keys
            char = CTRL_KEYS.get(event.key) or event.character

        if char:
            try:
                self._p_out.write(char.encode())
            except Exception:
                pass

    def open_with_content(self, content: str) -> None:
        """Open vim with the given content."""
        self._content = content
        self._start_vim()

    def _open_vim(self) -> int:
        """Fork and exec vim."""
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        # Use unique temp file per editor based on widget id
        editor_id = self.id or "default"
        ext = ".py" if "python" in editor_id else ".sql"
        self._temp_file = CACHE_DIR / f"vim_edit_{editor_id}{ext}"
        self._temp_file.write_text(self._initial_content)

        pid, fd = pty.fork()
        if pid == 0:
            # Child process
            editor = os.environ.get("EDITOR", "vim")
            ncol = self._screen.columns if self._screen else 80
            nrow = self._screen.lines if self._screen else 24
            env = dict(
                TERM="xterm-256color",
                LC_ALL="en_US.UTF-8",
                COLUMNS=str(ncol),
                LINES=str(nrow),
            )
            env.update(os.environ)
            # Add autocommand to save on leaving insert mode
            vim_args = [
                editor,
                "-c",
                "autocmd InsertLeave * silent! write",
                str(self._temp_file),
            ]
            os.execvpe(editor, vim_args, env)
        return fd

    async def _run(self) -> None:
        """Main run loop for vim process."""
        await self._size_set.wait()

        self._fd = self._open_vim()
        self._p_out = os.fdopen(self._fd, "w+b", 0)

        # Set pty size after fork to ensure vim gets correct dimensions
        if self._screen:
            nrow = self._screen.lines
            ncol = self._screen.columns
            try:
                winsize = struct.pack("HH", nrow, ncol)
                fcntl.ioctl(self._fd, termios.TIOCSWINSZ, winsize)
            except OSError:
                pass

        loop = asyncio.get_running_loop()

        def on_output():
            try:
                data = self._p_out.read(65536)
                if data:
                    self._data_or_disconnect = data.decode("utf-8", errors="replace")
                else:
                    self._data_or_disconnect = None
                self._event.set()
            except Exception:
                loop.remove_reader(self._p_out)
                self._data_or_disconnect = None
                self._event.set()

        loop.add_reader(self._p_out, on_output)

    async def _send(self) -> None:
        """Process vim output and update display."""
        while self._vim_running:
            await self._event.wait()
            self._event.clear()

            if self._data_or_disconnect is None:
                # Vim exited
                self._vim_running = False
                content = ""
                if self._temp_file and self._temp_file.exists():
                    content = self._temp_file.read_text()
                self._content = content  # Update stored content
                self._cleanup()
                self.post_message(self.Closed(content, editor_id=self.id))
                break
            else:
                # Update display
                if self._stream and self._screen:
                    try:
                        self._stream.feed(self._data_or_disconnect)
                    except (TypeError, KeyError, ValueError):
                        # pyte can fail on some terminal codes, ignore
                        pass
                    lines = []
                    for row in range(self._screen.lines):
                        text = Text()
                        line_buffer = self._screen.buffer[row]
                        for col in range(self._screen.columns):
                            char_data = line_buffer[col]
                            char = char_data.data or " "
                            # Build style from pyte character attributes
                            style_parts = []
                            fg = _pyte_color_to_rich(char_data.fg)
                            bg = _pyte_color_to_rich(char_data.bg)

                            # Force visible text color if default
                            if not fg:
                                fg = "#f0f0f0"

                            if char_data.reverse:
                                # For reverse, swap fg/bg
                                fg, bg = bg or "black", fg or "white"
                            if fg:
                                style_parts.append(fg)
                            if bg:
                                style_parts.append(f"on {bg}")
                            if char_data.bold:
                                style_parts.append("bold")
                            if char_data.italics:
                                style_parts.append("italic")
                            if char_data.underscore:
                                style_parts.append("underline")
                            style = " ".join(style_parts) if style_parts else None
                            text.append(char, style)
                        # Add cursor with reverse video
                        if row == self._screen.cursor.y:
                            x = self._screen.cursor.x
                            if x < len(text):
                                cursor = text[x]
                                cursor.stylize("reverse")
                                new_text = text[:x]
                                new_text.append(cursor)
                                new_text.append(text[x + 1 :])
                                text = new_text
                        lines.append(text)
                    self._display = PyteDisplay(lines)
                    self.refresh()

    def _cleanup(self) -> None:
        """Clean up resources."""
        if self._p_out is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.remove_reader(self._p_out)
            except Exception:
                pass
            try:
                self._p_out.close()
            except Exception:
                pass
            self._p_out = None
        self._fd = None
        self._vim_running = False


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


class ResultsTable(DataTable):
    """A DataTable for displaying query results."""

    def __init__(self) -> None:
        super().__init__(id="results-table", zebra_stripes=True)
        self.cursor_type = "row"

    def display_results(self, columns: list[str], rows: list[tuple]) -> None:
        """Display query results in the table."""
        self.clear(columns=True)
        if columns:
            # Convert column names to strings (needed for df.T which uses numeric indices)
            self.add_columns(*[str(c) for c in columns])
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

    def on_key(self, event: events.Key) -> None:
        """Handle navigation keys."""
        if event.key == "up":
            self.query_one("#search-results", OptionList).action_cursor_up()
            event.stop()
        elif event.key == "down":
            self.query_one("#search-results", OptionList).action_cursor_down()
            event.stop()
        elif event.key == "enter":
            # Propagate to allow on_input_submitted if focused on input,
            # but we will handle selection in on_input_submitted
            pass

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

    def on_key(self, event: events.Key) -> None:
        """Handle navigation keys."""
        if event.key == "up":
            self.query_one("#queries-list", OptionList).action_cursor_up()
            event.stop()
        elif event.key == "down":
            self.query_one("#queries-list", OptionList).action_cursor_down()
            event.stop()
        elif event.key == "enter":
            pass

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


class TrinoQCommands(Provider):
    """Command provider for TrinoQ."""

    @property
    def _commands(self) -> list[tuple[str, Any, str]]:
        return [
            ("Run Query", self.app.action_execute_query, "Execute current SQL query"),
            (
                "Toggle Python Editor",
                self.app.action_toggle_python,
                "Show/hide Python script editor",
            ),
            ("Save Query", self.app.action_save_query, "Save current query"),
            ("Open Queries", self.app.action_show_queries, "Open saved queries"),
            ("Clear Results", self.app.action_clear_results, "Clear results table"),
            (
                "Toggle Maximize",
                self.app.action_toggle_maximize,
                "Maximize/restore panel",
            ),
            ("Quit", self.app.action_quit, "Quit application"),
            ("Search Tables", self.app.action_show_search, "Search database tables"),
        ]

    async def discover(self) -> Hits:
        for name, callback, help_text in self._commands:
            yield Hit(
                1.0,
                name,
                callback,
                help=help_text,
            )

    async def search(self, query: str) -> Hits:
        matcher = self.matcher(query)
        for name, callback, help_text in self._commands:
            score = matcher.match(name)
            if score > 0:
                yield Hit(
                    score,
                    matcher.highlight(name),
                    callback,
                    help=help_text,
                )


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

    #main-area {
        width: 1fr;
        height: 100%;
        layout: grid;
        grid-size: 1 2;
        grid-rows: 2fr 3fr;
    }

    #sql-editor {
        height: 100%;
        width: 100%;
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

    #sql-editor:focus {
        border: round $accent;
    }

    #python-editor {
        display: none;
        height: 100%;
        width: 1fr;
        border: round $success;
    }

    #python-editor.visible {
        display: block;
    }

    #python-editor.hidden {
        display: none;
    }

    #python-editor:focus-within {
        border: round $accent;
    }

    #editors-row {
        height: 100%;
        width: 100%;
    }

    #editor-container {
        height: 100%;
        width: 1fr;
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

    #sql-editor.hidden {
        display: none;
    }

    #editor-container.hidden {
        display: none;
    }

    #results-container.hidden {
        display: none;
    }

    #main-area.maximized {
        grid-size: 1 1;
        grid-rows: 1fr;
    }

    .area-selected {
        border: heavy yellow !important;
    }
    """

    BINDINGS = [
        Binding("f5", "execute_query", "Run", show=True, priority=True),
        Binding("ctrl+p", "command_palette", "Menu", show=True, priority=True),
    ]

    COMMANDS = {TrinoQCommands}

    show_python_editor = var(True)
    _maximized_panel: str | None = None  # Track which panel is maximized
    _connection: Any = None
    _area_select_mode: bool = False  # Area selection mode
    _selected_area: int = 0  # 0=sql, 1=python, 2=results
    _last_escape_time: float = 0  # For detecting double-escape
    _areas: list[str] = ["sql-editor", "python-editor", "results-container"]
    _areas_focus: list[str] = ["sql-editor", "python-editor", "results-table"]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="content-area"):
            with Vertical(id="main-area"):
                with Horizontal(id="editors-row"):
                    with Container(id="editor-container"):
                        yield VimEditor(
                            id="sql-editor",
                            auto_start=True,
                            initial_content="-- SQL query\nselect 1",
                        )
                    yield VimEditor(
                        id="python-editor",
                        auto_start=True,
                        classes="visible",
                        initial_content="# python script\n# df = df * 100",
                    )
                with Container(id="results-container"):
                    yield ResultsTable()
        yield StatusBar(id="status-bar")
        yield SearchPopup()
        yield QueriesPopup()
        yield SaveQueryPopup()
        yield Footer()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self.query_one("#sql-editor", VimEditor).focus()

    def on_key(self, event: events.Key) -> None:
        """Handle key events for area selection mode."""
        # Detect double-escape to enter area selection mode
        if event.key == "escape":
            current_time = time.time()
            if self._area_select_mode:
                # Exit area selection mode
                self._exit_area_select_mode()
                event.stop()
                return
            elif current_time - self._last_escape_time < 0.4:
                # Double escape detected - enter area selection mode
                self._enter_area_select_mode()
                event.stop()
                return
            self._last_escape_time = current_time
            return

        # Handle navigation in area selection mode
        if self._area_select_mode:
            if event.key in ("left", "up", "h", "k"):
                self._move_area_selection(-1)
                event.stop()
            elif event.key in ("right", "down", "l", "j"):
                self._move_area_selection(1)
                event.stop()
            elif event.key == "enter":
                self._exit_area_select_mode()
                event.stop()

    def _enter_area_select_mode(self) -> None:
        """Enter area selection mode."""
        self._area_select_mode = True
        # Determine current area based on focus
        focused = self.focused
        if focused:
            if focused.id == "sql-editor":
                self._selected_area = 0
            elif focused.id == "python-editor":
                self._selected_area = 1
            elif focused.id == "results-table":
                self._selected_area = 2
        # Pause vim editors so they don't capture keys
        self._set_vim_editors_paused(True)
        self._update_area_highlight()
        self.query_one(
            StatusBar
        ).status = "Area select: ←↑↓→ to move, Enter/Esc to select"

    def on_vim_editor_area_select_requested(
        self, event: VimEditor.AreaSelectRequested
    ) -> None:
        """Handle area selection request from VimEditor."""
        self._enter_area_select_mode()

    def _set_vim_editors_paused(self, paused: bool) -> None:
        """Pause or resume all vim editors."""
        try:
            self.query_one("#sql-editor", VimEditor)._vim_paused = paused
        except Exception:
            pass
        try:
            self.query_one("#python-editor", VimEditor)._vim_paused = paused
        except Exception:
            pass

    def _exit_area_select_mode(self) -> None:
        """Exit area selection mode and focus selected area."""
        self._area_select_mode = False
        # Resume vim editors
        self._set_vim_editors_paused(False)
        # Remove all highlights
        for area_id in self._areas:
            try:
                widget = self.query_one(f"#{area_id}")
                widget.remove_class("area-selected")
            except Exception:
                pass
        # Focus the selected area (use _areas_focus for focusable widgets)
        area_id = self._areas_focus[self._selected_area]
        try:
            widget = self.query_one(f"#{area_id}")
            widget.focus()
        except Exception:
            pass
        self.query_one(StatusBar).status = "Ready"

    def _move_area_selection(self, direction: int) -> None:
        """Move area selection by direction (-1 or +1)."""
        self._selected_area = (self._selected_area + direction) % len(self._areas)
        self._update_area_highlight()

    def _update_area_highlight(self) -> None:
        """Update visual highlight for selected area."""
        for i, area_id in enumerate(self._areas):
            try:
                widget = self.query_one(f"#{area_id}")
                if i == self._selected_area:
                    widget.add_class("area-selected")
                else:
                    widget.remove_class("area-selected")
            except Exception:
                pass

    def watch_show_python_editor(self, show_python_editor: bool) -> None:
        """Toggle the Python editor visibility."""
        python_editor = self.query_one("#python-editor", VimEditor)
        if show_python_editor:
            python_editor.add_class("visible")
            # Start vim if not already started
            if not python_editor._started:
                python_editor._started = True
                python_editor._start_vim()
        else:
            python_editor.remove_class("visible")

    def _get_connection(self) -> Any:
        """Get or create a Trino connection."""
        if self._connection is None:
            from trinoq import create_connection

            self._connection = create_connection()
        return self._connection

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

    @work(thread=True, exclusive=True, group="query")
    def _execute_query_with_python(self, sql: str, python_script: str) -> None:
        """Execute the SQL query with Python post-processing."""
        import pandas as pd

        from trinoq import execute

        status = self.query_one(StatusBar)
        results_table = self.query_one(ResultsTable)

        self.call_from_thread(setattr, status, "is_running", True)
        self.call_from_thread(setattr, status, "status", "Running query with Python...")

        start_time = time.time()

        try:
            # Execute the SQL query
            conn = self._get_connection()
            df = execute(sql, engine=conn, no_cache=True, quiet=True)

            # Execute the Python script with df in scope
            # The script can modify df or create a new one
            # Use same dict for globals and locals so assignments work correctly
            exec_globals = {"df": df, "pd": pd, "__builtins__": __builtins__}
            exec(python_script, exec_globals, exec_globals)

            # Get the resulting df (script may have modified it)
            df = exec_globals.get("df", df)

            # Always reset index to include it as a column if it has meaningful data
            # This is useful for df.T, groupby, etc.
            try:
                # Check if index has meaningful data (not just 0,1,2...)
                if not (df.index.equals(pd.RangeIndex(len(df)))):
                    df = df.reset_index()
            except Exception:
                pass

            elapsed = time.time() - start_time

            # Display results
            columns = df.columns.tolist()
            rows = [tuple(row) for row in df.values]

            self.call_from_thread(results_table.display_results, columns, rows)
            self.call_from_thread(setattr, status, "is_running", False)
            self.call_from_thread(
                setattr,
                status,
                "status",
                f"Python completed: {len(rows)} rows in {elapsed:.2f}s",
            )
            self.call_from_thread(
                self.notify, f"Query returned {len(rows)} rows", severity="information"
            )

        except Exception as e:
            self.call_from_thread(setattr, status, "is_running", False)
            self.call_from_thread(results_table.display_error, str(e))
            self.call_from_thread(setattr, status, "status", f"Error: {e}")
            self.call_from_thread(self.notify, f"Error: {e}", severity="error")

    def action_execute_query(self) -> None:
        """Execute the current query."""
        editor = self.query_one("#sql-editor", VimEditor)
        sql = editor.content

        if not sql or not sql.strip():
            self.notify("No query to execute", severity="warning")
            return

        # Check if Python editor is visible and has content
        if self.show_python_editor:
            python_editor = self.query_one("#python-editor", VimEditor)
            python_script = python_editor.content.strip()
            # Always run with Python if the editor is visible
            if python_script:
                self._execute_query_with_python(sql.strip(), python_script)
                return

        self._execute_query(sql.strip())

    def action_clear_results(self) -> None:
        """Clear the results table."""
        results_table = self.query_one(ResultsTable)
        results_table.clear(columns=True)
        self.query_one(StatusBar).status = "Results cleared"

    def action_toggle_python(self) -> None:
        """Toggle the Python editor visibility."""
        self.show_python_editor = not self.show_python_editor
        if self.show_python_editor:
            self.query_one("#python-editor", VimEditor).focus()

    def on_vim_editor_closed(self, event: VimEditor.Closed) -> None:
        """Handle vim editor closing - restart vim for the editor."""
        # Restart vim for whichever editor was closed
        if event.editor_id == "sql-editor":
            sql_editor = self.query_one("#sql-editor", VimEditor)
            sql_editor._start_vim()
        elif event.editor_id == "python-editor":
            python_editor = self.query_one("#python-editor", VimEditor)
            python_editor._start_vim()
        self.query_one(StatusBar).status = "Ready"

    def action_focus_editor(self) -> None:
        """Focus the query editor."""
        self.query_one("#sql-editor", VimEditor).focus()

    def action_focus_results(self) -> None:
        """Focus the results table."""
        self.query_one(ResultsTable).focus()

    def action_toggle_maximize(self) -> None:
        """Toggle maximize for the focused panel (editor or results)."""
        editor = self.query_one("#editor-container")
        results = self.query_one("#results-container")
        main_area = self.query_one("#main-area")

        # Determine which panel is focused
        focused = self.focused
        if focused is None:
            return

        # Find if we're in editor or results
        current_panel = None
        if focused.id == "sql-editor" or focused.has_class("text-area"):
            current_panel = "editor"
        elif focused.id == "results-table" or focused.id == "results-container":
            current_panel = "results"
        else:
            # Check ancestors
            node = focused
            while node is not None:
                if node.id == "sql-editor" or node.id == "editor-container":
                    current_panel = "editor"
                    break
                elif node.id == "results-container":
                    current_panel = "results"
                    break
                node = node.parent

        if current_panel is None:
            self.notify("Focus editor or results to maximize")
            return

        # If already maximized, restore
        if self._maximized_panel is not None:
            editor.remove_class("hidden")
            results.remove_class("hidden")
            main_area.remove_class("maximized")
            self._maximized_panel = None
            self.query_one(StatusBar).status = "Restored layout"
        else:
            # Maximize current panel
            main_area.add_class("maximized")
            if current_panel == "editor":
                results.add_class("hidden")
                self._maximized_panel = "editor"
                self.query_one(
                    StatusBar
                ).status = "Editor maximized (ctrl+m to restore)"
            else:
                editor.add_class("hidden")
                self._maximized_panel = "results"
                self.query_one(
                    StatusBar
                ).status = "Results maximized (ctrl+m to restore)"

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
        self.query_one("#sql-editor", VimEditor).focus()

    def action_save_query(self) -> None:
        """Show the save query popup."""
        editor = self.query_one("#sql-editor", VimEditor)
        sql = editor.content.strip()

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
        self.query_one("#sql-editor", VimEditor).focus()

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
        self.query_one("#sql-editor", VimEditor).focus()

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
                editor = self.query_one("#sql-editor", VimEditor)
                editor.append_text(full_name)
            self.action_hide_search()

        elif event.option_list.id == "queries-list":
            popup = self.query_one(QueriesPopup)
            if event.option_index < len(popup.queries):
                query = popup.queries[event.option_index]
                editor = self.query_one("#sql-editor", VimEditor)
                editor.content = query.get("sql", "")
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

        elif event.input.id == "search-input":
            # Select from search results
            popup = self.query_one(SearchPopup)
            results = popup.query_one("#search-results", OptionList)
            if results.highlighted is not None and results.highlighted < len(
                popup.matches
            ):
                match = popup.matches[results.highlighted]
                full_name = f"{match['catalog']}.{match['schema']}.{match['name']}"
                editor = self.query_one("#sql-editor", VimEditor)
                editor.append_text(full_name)
            self.action_hide_search()

        elif event.input.id == "queries-filter":
            # Select from queries
            popup = self.query_one(QueriesPopup)
            results = popup.query_one("#queries-list", OptionList)

            if results.highlighted is not None:
                option = results.get_option_at_index(results.highlighted)
                if option and option.id:
                    query_idx = int(option.id)
                    if query_idx < len(popup.queries):
                        query = popup.queries[query_idx]
                        editor = self.query_one("#sql-editor", VimEditor)
                        editor.content = query.get("sql", "")
                        self.notify(f"Loaded: {query.get('name', 'query')}")
            self.action_hide_queries()

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
