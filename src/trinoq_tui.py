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
    Button,
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
HISTORY_FILE = CACHE_DIR / "query_history.json"
CACHE_MAX_AGE_SECONDS = 3600  # Refresh cache if older than 1 hour
HISTORY_MAX_ENTRIES = 100  # Keep last 100 queries in history

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


class Splitter(Widget):
    """Draggable splitter between panels for resizing."""

    DEFAULT_CSS = """
    Splitter {
        background: $surface-lighten-1;
    }
    
    Splitter:hover {
        background: $accent;
    }
    
    Splitter.dragging {
        background: $accent;
    }
    
    Splitter.hidden {
        display: none;
    }
    
    Splitter.horizontal {
        height: 1;
        width: 100%;
        min-height: 1;
    }
    
    Splitter.vertical {
        width: 1;
        height: 100%;
        min-width: 1;
    }
    """

    class Dragged(Message):
        """Message sent when splitter is dragged."""

        def __init__(self, splitter: "Splitter", delta: int) -> None:
            self.splitter = splitter
            self.delta = delta
            super().__init__()

    def __init__(
        self,
        orientation: str = "horizontal",
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self.orientation = orientation
        self._dragging = False
        self._last_pos = 0
        self.add_class(orientation)

    def on_mouse_down(self, event: events.MouseDown) -> None:
        """Start dragging."""
        self._dragging = True
        self._last_pos = (
            event.screen_y if self.orientation == "horizontal" else event.screen_x
        )
        self.add_class("dragging")
        self.capture_mouse()
        event.stop()

    def on_mouse_move(self, event: events.MouseMove) -> None:
        """Handle mouse movement during drag."""
        if self._dragging:
            current = (
                event.screen_y if self.orientation == "horizontal" else event.screen_x
            )
            delta = current - self._last_pos
            if delta != 0:
                self.post_message(self.Dragged(self, delta))
                self._last_pos = current
        event.stop()

    def on_mouse_up(self, event: events.MouseUp) -> None:
        """Stop dragging."""
        if self._dragging:
            self._dragging = False
            self.remove_class("dragging")
            self.release_mouse()
        event.stop()

    def render(self) -> str:
        """Render empty content - splitter is just a colored bar."""
        return ""


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
        """Set editor content - updates vim buffer if running."""
        self._content = value
        self._initial_content = value
        if self._vim_running and self._p_out is not None and self._temp_file:
            try:
                # Write new content to temp file
                self._temp_file.write_text(value)
                # Send vim command to reload file: Esc, :e! (reload), Enter
                commands = "\x1b:e!\r"
                self._p_out.write(commands.encode())
            except Exception:
                pass

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

        # Ctrl+C to enter area selection mode
        if event.key == "ctrl+c":
            self.post_message(self.AreaSelectRequested())
            event.stop()
            return

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


def load_query_history() -> list[dict]:
    """Load query history from file."""
    try:
        if HISTORY_FILE.exists():
            with open(HISTORY_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return []


def save_query_to_history(sql: str, python_script: str | None = None) -> None:
    """Save a successfully executed query to history.

    Avoids duplicates by checking if the same SQL already exists at the top.
    """
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        history = load_query_history()

        # Create history entry
        entry = {
            "sql": sql,
            "executed_at": time.time(),
        }
        if python_script:
            entry["python"] = python_script

        # Avoid duplicate if same SQL is at the top
        if history and history[0].get("sql") == sql:
            # Update timestamp and python script if present
            history[0] = entry
        else:
            # Insert at the beginning
            history.insert(0, entry)

        # Keep only last N entries
        history = history[:HISTORY_MAX_ENTRIES]

        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)
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


def render_query(query: str) -> str:
    """Render SQL query with Jinja-style template substitution.

    Supports:
    - -- @param key value (define parameter in SQL comment)
    - {{key}} or {key} to reference parameters or environment variables
    """
    import re

    from trinoq import extract_params

    # Extract @param values from query
    params = extract_params(query)

    # First check for double braces {{key}}
    pattern_double = r"{{([^}]+)}}"
    matches_double = re.findall(pattern_double, query)

    # Then check for single braces {key} (only if no double braces found)
    pattern_single = r"(?<!\{){([^}]+)}(?!\})"
    matches_single = re.findall(pattern_single, query) if not matches_double else []

    if matches_double:
        # Handle double braces {{key}}
        fmt_values = {}
        for k in matches_double:
            k = k.strip()
            # Try params first, then environment variables
            if k in params:
                fmt_values[k] = params[k]
            else:
                fmt_values[k] = os.environ.get(k, f"{{{{MISSING:{k}}}}}")

        # Replace {{key}} with values
        for key, value in fmt_values.items():
            query = re.sub(r"{{\s*" + re.escape(key) + r"\s*}}", value, query)

    elif matches_single:
        # Handle single braces {key}
        fmt_values = {}
        for k in matches_single:
            # Try params first, then environment variables
            if k in params:
                fmt_values[k] = params[k]
            else:
                fmt_values[k] = os.environ.get(k, f"{{MISSING:{k}}}")

        # Use standard format for single braces
        query = query.format(**fmt_values)

    return query


class ResultsTable(DataTable):
    """A DataTable for displaying query results with vim-style visual selection."""

    BINDINGS = [
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("h", "cursor_left", "Left", show=False),
        Binding("l", "cursor_right", "Right", show=False),
        Binding("v", "toggle_visual", "Visual", show=False),
        Binding("V", "toggle_visual_line", "Visual Line", show=False),
        Binding("ctrl+v", "toggle_visual_block", "Visual Block", show=False),
        Binding("y", "yank_selection", "Yank", show=False),
        Binding("escape", "exit_mode", "Exit", show=False),
        Binding("slash", "start_search", "Search", show=False),
        Binding("n", "next_match", "Next", show=False),
        Binding("N", "prev_match", "Prev", show=False),
    ]

    def __init__(self) -> None:
        super().__init__(id="results-table", zebra_stripes=True)
        self.cursor_type = "cell"
        self._visual_mode = False
        self._visual_line_mode = False  # V: select entire rows
        self._visual_block_mode = False  # Ctrl+V: select entire columns
        self._selection_start: tuple[int, int] | None = None  # (row, col)
        self._selected_cells: set[tuple[int, int]] = set()
        self._original_values: dict[tuple[int, int], str] = {}  # Store original values
        # Search state
        self._search_mode = False
        self._search_query = ""
        self._search_matches: list[tuple[int, int]] = []  # List of (row, col) matches
        self._current_match_idx = -1
        self._search_highlighted: dict[
            tuple[int, int], str
        ] = {}  # Store original values for highlights

    def action_toggle_visual(self) -> None:
        """Enter visual selection mode (cell-based)."""
        if not self._visual_mode:
            self._visual_mode = True
            self._visual_line_mode = False
            self._visual_block_mode = False
            self.cursor_type = "none"  # Hide native cursor
            row_idx = self.cursor_row
            col_idx = self.cursor_column
            self._selection_start = (row_idx, col_idx)
            self._update_selection()
            self.app.query_one(
                "StatusBar"
            ).status = "-- VISUAL -- (hjkl to extend, y to copy, Esc to cancel)"
        else:
            self._exit_visual_mode()

    def action_toggle_visual_line(self) -> None:
        """Enter visual line mode (select entire rows)."""
        if not self._visual_mode:
            self._visual_mode = True
            self._visual_line_mode = True
            self._visual_block_mode = False
            self.cursor_type = "none"  # Hide native cursor
            row_idx = self.cursor_row
            col_idx = self.cursor_column
            self._selection_start = (row_idx, col_idx)
            self._update_selection()
            self.app.query_one(
                "StatusBar"
            ).status = "-- VISUAL LINE -- (jk to extend rows, y to copy, Esc to cancel)"
        else:
            self._exit_visual_mode()

    def action_toggle_visual_block(self) -> None:
        """Enter visual block mode (select entire columns)."""
        if not self._visual_mode:
            self._visual_mode = True
            self._visual_line_mode = False
            self._visual_block_mode = True
            self.cursor_type = "none"  # Hide native cursor
            row_idx = self.cursor_row
            col_idx = self.cursor_column
            self._selection_start = (row_idx, col_idx)
            self._update_selection()
            self.app.query_one(
                "StatusBar"
            ).status = (
                "-- VISUAL BLOCK -- (hl to extend cols, y to copy, Esc to cancel)"
            )
        else:
            self._exit_visual_mode()

    def action_exit_mode(self) -> None:
        """Exit visual mode or search mode."""
        if self._visual_mode:
            self._exit_visual_mode()
        elif self._search_mode:
            self._exit_search_mode()
        else:
            self._clear_search_highlights()

    def _exit_visual_mode(self) -> None:
        """Clear visual mode state and restore original cell values."""
        # Restore original values (remove highlighting)
        for (row_idx, col_idx), original_value in self._original_values.items():
            try:
                row_key = self._row_locations.get_key(row_idx)
                col_key = self._column_locations.get_key(col_idx)
                if row_key and col_key:
                    self.update_cell(row_key, col_key, original_value)
            except Exception:
                pass
        self._visual_mode = False
        self._visual_line_mode = False
        self._visual_block_mode = False
        self._selection_start = None
        self._selected_cells.clear()
        self._original_values.clear()
        self.cursor_type = "cell"  # Restore native cursor
        self.refresh()
        self.app.query_one("StatusBar").status = ""

    def _update_selection(self) -> None:
        """Update selected cells based on cursor position and visual mode type."""
        if not self._visual_mode or self._selection_start is None:
            return

        # First restore any previously highlighted cells that are no longer selected
        old_selected = self._selected_cells.copy()

        start_row, start_col = self._selection_start
        end_row, end_col = self.cursor_row, self.cursor_column

        # Get range bounds based on visual mode type
        min_row, max_row = min(start_row, end_row), max(start_row, end_row)
        min_col, max_col = min(start_col, end_col), max(start_col, end_col)

        num_columns = len(list(self.columns))
        num_rows = self.row_count

        if self._visual_line_mode:
            # V: Select entire rows from start_row to end_row
            min_col, max_col = 0, num_columns - 1
        elif self._visual_block_mode:
            # Ctrl+V: Select entire columns from start_col to end_col
            min_row, max_row = 0, num_rows - 1

        # Build new selection set
        new_selected = {
            (r, c)
            for r in range(min_row, max_row + 1)
            for c in range(min_col, max_col + 1)
        }

        # Restore cells that are no longer selected
        for coord in old_selected - new_selected:
            if coord in self._original_values:
                try:
                    row_key = self._row_locations.get_key(coord[0])
                    col_key = self._column_locations.get_key(coord[1])
                    if row_key and col_key:
                        self.update_cell(row_key, col_key, self._original_values[coord])
                except Exception:
                    pass

        # Highlight new cells
        cursor_pos = (self.cursor_row, self.cursor_column)
        for coord in new_selected - old_selected:
            try:
                row_key = self._row_locations.get_key(coord[0])
                col_key = self._column_locations.get_key(coord[1])
                if row_key and col_key:
                    value = self.get_cell(row_key, col_key)
                    # Store original value
                    if coord not in self._original_values:
                        self._original_values[coord] = str(value)
                    # Use brighter style for cursor position, dimmer for rest of selection
                    if coord == cursor_pos:
                        highlighted = Text(str(value), style="bold white on blue")
                    else:
                        highlighted = Text(str(value), style="white on dark_blue")
                    self.update_cell(row_key, col_key, highlighted)
            except Exception:
                pass

        # Update cursor cell style (may have moved within selection)
        old_cursor = getattr(self, "_last_cursor_pos", None)
        if (
            old_cursor
            and old_cursor in self._selected_cells
            and old_cursor != cursor_pos
        ):
            # Restore old cursor to normal selection style
            try:
                row_key = self._row_locations.get_key(old_cursor[0])
                col_key = self._column_locations.get_key(old_cursor[1])
                if row_key and col_key:
                    value = self._original_values.get(old_cursor, "")
                    highlighted = Text(str(value), style="white on dark_blue")
                    self.update_cell(row_key, col_key, highlighted)
            except Exception:
                pass
        if cursor_pos in self._selected_cells:
            # Highlight new cursor position
            try:
                row_key = self._row_locations.get_key(cursor_pos[0])
                col_key = self._column_locations.get_key(cursor_pos[1])
                if row_key and col_key:
                    value = self._original_values.get(cursor_pos, "")
                    highlighted = Text(str(value), style="bold white on blue")
                    self.update_cell(row_key, col_key, highlighted)
            except Exception:
                pass
        self._last_cursor_pos = cursor_pos

        self._selected_cells = new_selected
        self.refresh()

    def on_data_table_cell_highlighted(self, event) -> None:
        """Update selection when cursor moves."""
        if self._visual_mode:
            self._update_selection()

    def action_yank_selection(self) -> None:
        """Copy selected cells to clipboard."""
        if not self._visual_mode or not self._selected_cells:
            # If not in visual mode, copy current cell
            row_idx = self.cursor_row
            col_idx = self.cursor_column
            try:
                cell_value = self.get_cell_at((row_idx, col_idx))
                self._copy_to_clipboard(str(cell_value))
                self.app.query_one("StatusBar").status = "Copied cell"
            except Exception:
                pass
            return

        # Get selection bounds
        rows = sorted(set(r for r, c in self._selected_cells))
        cols = sorted(set(c for r, c in self._selected_cells))

        # Build text from selected cells (TSV format)
        lines = []
        for row_idx in rows:
            row_values = []
            for col_idx in cols:
                if (row_idx, col_idx) in self._selected_cells:
                    try:
                        cell_value = self.get_cell_at((row_idx, col_idx))
                        row_values.append(str(cell_value))
                    except Exception:
                        row_values.append("")
            lines.append("\t".join(row_values))

        text = "\n".join(lines)
        self._copy_to_clipboard(text)
        cell_count = len(self._selected_cells)
        self.app.query_one("StatusBar").status = f"Copied {cell_count} cells"
        self._exit_visual_mode()

    def _copy_to_clipboard(self, text: str) -> None:
        """Copy text to system clipboard."""
        import subprocess
        import sys

        try:
            if sys.platform == "darwin":
                # macOS
                subprocess.run(["pbcopy"], input=text.encode(), check=True)
            elif sys.platform.startswith("linux"):
                # Linux with xclip or xsel
                try:
                    subprocess.run(
                        ["xclip", "-selection", "clipboard"],
                        input=text.encode(),
                        check=True,
                    )
                except FileNotFoundError:
                    subprocess.run(
                        ["xsel", "--clipboard", "--input"],
                        input=text.encode(),
                        check=True,
                    )
            else:
                # Fallback to OSC 52
                import base64

                encoded = base64.b64encode(text.encode()).decode()
                print(f"\033]52;c;{encoded}\a", end="", flush=True)
        except Exception:
            # Silent fail
            pass

    def get_cell_at(self, coordinate: tuple[int, int]) -> str:
        """Get cell value at (row, col) coordinate."""
        row_idx, col_idx = coordinate
        row_key = self._row_locations.get_key(row_idx)
        col_key = self._column_locations.get_key(col_idx)
        return self.get_cell(row_key, col_key)

    def display_results(
        self, columns: list[str], rows: list[tuple], max_rows: int = 10000
    ) -> None:
        """Display query results in the table.

        Args:
            columns: Column names
            rows: Row data
            max_rows: Maximum rows to display (default 10000) to prevent UI blocking
        """
        self._exit_visual_mode() if self._visual_mode else None
        self.clear(columns=True)

        if columns:
            # Convert column names to strings (needed for df.T which uses numeric indices)
            self.add_columns(*[str(c) for c in columns])

        # Limit rows to prevent UI blocking
        display_rows = rows[:max_rows]
        truncated = len(rows) > max_rows

        # Convert all values to strings and add all rows at once (more efficient)
        str_rows = [
            tuple(str(v) if v is not None else "NULL" for v in row)
            for row in display_rows
        ]
        self.add_rows(str_rows)

        if truncated:
            # Add indicator row showing truncation
            self.add_row(
                *[
                    f"... ({len(rows) - max_rows} more rows)" if i == 0 else ""
                    for i in range(len(columns))
                ]
            )

    def display_error(self, error: str) -> None:
        """Display an error message in the table."""
        self._exit_visual_mode() if self._visual_mode else None
        self.clear(columns=True)
        self.add_column("Error")
        self.add_row(str(error))

    # Search methods
    def action_start_search(self) -> None:
        """Start search mode - show input in status bar."""
        self._search_mode = True
        self._search_query = ""
        self.app.query_one("StatusBar").status = "/"

    def action_next_match(self) -> None:
        """Go to next search match."""
        if not self._search_matches:
            return
        self._current_match_idx = (self._current_match_idx + 1) % len(
            self._search_matches
        )
        self._goto_current_match()

    def action_prev_match(self) -> None:
        """Go to previous search match."""
        if not self._search_matches:
            return
        self._current_match_idx = (self._current_match_idx - 1) % len(
            self._search_matches
        )
        self._goto_current_match()

    def _goto_current_match(self) -> None:
        """Move cursor to current match and update status."""
        if not self._search_matches or self._current_match_idx < 0:
            return
        row_idx, col_idx = self._search_matches[self._current_match_idx]
        self.move_cursor(row=row_idx, column=col_idx)
        total = len(self._search_matches)
        current = self._current_match_idx + 1
        self.app.query_one(
            "StatusBar"
        ).status = f"/{self._search_query} [{current}/{total}]"

    def _perform_search(self, query: str) -> None:
        """Search all cells for query and highlight matches."""
        self._clear_search_highlights()
        self._search_query = query
        self._search_matches = []
        self._current_match_idx = -1

        if not query:
            self.app.query_one("StatusBar").status = ""
            return

        query_lower = query.lower()

        # Search all cells
        for row_idx in range(self.row_count):
            for col_idx in range(len(list(self.columns))):
                try:
                    row_key = self._row_locations.get_key(row_idx)
                    col_key = self._column_locations.get_key(col_idx)
                    if row_key and col_key:
                        value = str(self.get_cell(row_key, col_key))
                        if query_lower in value.lower():
                            self._search_matches.append((row_idx, col_idx))
                            # Store original and highlight
                            if (row_idx, col_idx) not in self._search_highlighted:
                                self._search_highlighted[(row_idx, col_idx)] = value
                            # Highlight match with yellow background
                            highlighted = Text(value, style="black on yellow")
                            self.update_cell(row_key, col_key, highlighted)
                except Exception:
                    pass

        if self._search_matches:
            self._current_match_idx = 0
            self._goto_current_match()
        else:
            self.app.query_one("StatusBar").status = f"/{query} [no matches]"

    def _clear_search_highlights(self) -> None:
        """Remove search highlights from cells."""
        for (row_idx, col_idx), original_value in self._search_highlighted.items():
            try:
                row_key = self._row_locations.get_key(row_idx)
                col_key = self._column_locations.get_key(col_idx)
                if row_key and col_key:
                    self.update_cell(row_key, col_key, original_value)
            except Exception:
                pass
        self._search_highlighted.clear()
        self._search_matches = []
        self._current_match_idx = -1

    def _exit_search_mode(self) -> None:
        """Exit search input mode."""
        self._search_mode = False
        if self._search_query:
            self._perform_search(self._search_query)
        else:
            self.app.query_one("StatusBar").status = ""

    def on_key(self, event: events.Key) -> None:
        """Handle key events for search input."""
        if not self._search_mode:
            return

        if event.key == "enter":
            # Execute search
            self._search_mode = False
            self._perform_search(self._search_query)
            event.stop()
        elif event.key == "escape":
            # Cancel search
            self._search_mode = False
            self._search_query = ""
            self.app.query_one("StatusBar").status = ""
            event.stop()
        elif event.key == "backspace":
            # Delete last char
            self._search_query = self._search_query[:-1]
            self.app.query_one("StatusBar").status = f"/{self._search_query}"
            event.stop()
        elif event.character and event.character.isprintable():
            # Add character to search
            self._search_query += event.character
            self.app.query_one("StatusBar").status = f"/{self._search_query}"
            event.stop()


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
        with Horizontal(id="queries-content"):
            yield OptionList(id="queries-list")
            yield Static("", id="queries-preview")

    def on_key(self, event: events.Key) -> None:
        """Handle navigation keys."""
        if event.key == "up":
            self.query_one("#queries-list", OptionList).action_cursor_up()
            self._update_preview()
            event.stop()
        elif event.key == "down":
            self.query_one("#queries-list", OptionList).action_cursor_down()
            self._update_preview()
            event.stop()
        elif event.key == "enter":
            # Load the selected query directly
            self._load_selected()
            event.stop()

    def _load_selected(self) -> None:
        """Load the currently highlighted query into editor."""
        results = self.query_one("#queries-list", OptionList)
        if results.highlighted is None:
            return
        option = results.get_option_at_index(results.highlighted)
        if option and option.id:
            query_idx = int(option.id)
            if query_idx < len(self.queries):
                query = self.queries[query_idx]
                sql_editor = self.app.query_one("#sql-editor")
                sql_editor.content = query.get("sql", "")
                self.app.notify(f"Loaded: {query.get('name', 'query')}")
        self.app.action_hide_queries()

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted
    ) -> None:
        """Update preview when option is highlighted."""
        self._update_preview()

    def _update_preview(self) -> None:
        """Update the preview panel with the selected query."""
        results = self.query_one("#queries-list", OptionList)
        preview = self.query_one("#queries-preview", Static)

        if (
            results.highlighted is not None
            and results.highlighted < results.option_count
        ):
            option = results.get_option_at_index(results.highlighted)
            if option and option.id:
                query_idx = int(option.id)
                if query_idx < len(self.queries):
                    sql = self.queries[query_idx].get("sql", "")
                    preview.update(sql)
                    return
        preview.update("")

    def action_cancel(self) -> None:
        self.app.action_hide_queries()

    def action_delete_query(self) -> None:
        self.app.action_delete_selected_query()

    def load_queries(self) -> None:
        """Load and display saved queries."""
        self.queries = load_saved_queries()
        self._update_list("")
        # Update preview for first item
        self.call_after_refresh(self._update_preview)

    def _update_list(self, filter_text: str) -> None:
        """Update the queries list with optional filter."""
        results = self.query_one("#queries-list", OptionList)
        results.clear_options()

        for i, q in enumerate(self.queries):
            name = q.get("name", f"Query {i + 1}")
            if filter_text and filter_text.lower() not in name.lower():
                continue
            results.add_option(Option(name, id=str(i)))

        if self.queries and results.option_count > 0:
            results.highlighted = 0


class HistoryPopup(Container):
    """Floating popup for query history (auto-saved successful queries)."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False, priority=True),
    ]

    def __init__(self) -> None:
        super().__init__(id="history-popup")
        self.history: list[dict] = []

    def compose(self) -> ComposeResult:
        yield Static("Query History (Enter=load)", id="history-title")
        yield Input(placeholder="Filter history...", id="history-filter")
        with Horizontal(id="history-content"):
            yield OptionList(id="history-list")
            yield Static("", id="history-preview")

    def on_key(self, event: events.Key) -> None:
        """Handle navigation keys."""
        if event.key == "up":
            self.query_one("#history-list", OptionList).action_cursor_up()
            self._update_preview()
            event.stop()
        elif event.key == "down":
            self.query_one("#history-list", OptionList).action_cursor_down()
            self._update_preview()
            event.stop()
        elif event.key == "enter":
            # Load the selected query directly
            self._load_selected()
            event.stop()

    def _load_selected(self) -> None:
        """Load the currently highlighted query into editors."""
        results = self.query_one("#history-list", OptionList)
        if results.highlighted is None:
            return
        option = results.get_option_at_index(results.highlighted)
        if option and option.id:
            history_idx = int(option.id)
            if history_idx < len(self.history):
                entry = self.history[history_idx]
                sql_editor = self.app.query_one("#sql-editor")
                sql_editor.content = entry.get("sql", "")
                # Also load Python script if present
                python_script = entry.get("python", "")
                if python_script:
                    python_editor = self.app.query_one("#python-editor")
                    python_editor.content = python_script
                self.app.notify("Loaded query from history")
        self.app.action_hide_history()

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted
    ) -> None:
        """Update preview when option is highlighted."""
        self._update_preview()

    def _update_preview(self) -> None:
        """Update the preview panel with the selected query."""
        results = self.query_one("#history-list", OptionList)
        preview = self.query_one("#history-preview", Static)

        if (
            results.highlighted is not None
            and results.highlighted < results.option_count
        ):
            option = results.get_option_at_index(results.highlighted)
            if option and option.id:
                query_idx = int(option.id)
                if query_idx < len(self.history):
                    entry = self.history[query_idx]
                    sql = entry.get("sql", "")
                    python = entry.get("python", "")
                    if python:
                        preview.update(f"-- SQL:\n{sql}\n\n-- Python:\n{python}")
                    else:
                        preview.update(sql)
                    return
        preview.update("")

    def action_cancel(self) -> None:
        self.app.action_hide_history()

    def load_history(self) -> None:
        """Load and display query history."""
        self.history = load_query_history()
        self._update_list("")
        # Update preview for first item
        self.call_after_refresh(self._update_preview)

    def _update_list(self, filter_text: str) -> None:
        """Update the history list with optional filter."""
        from datetime import datetime

        results = self.query_one("#history-list", OptionList)
        results.clear_options()

        for i, entry in enumerate(self.history):
            sql = entry.get("sql", "")
            # Create a display name from first line of SQL + timestamp
            first_line = sql.strip().split("\n")[0][:50]
            if len(sql.strip().split("\n")[0]) > 50:
                first_line += "..."

            # Format timestamp
            executed_at = entry.get("executed_at", 0)
            if executed_at:
                dt = datetime.fromtimestamp(executed_at)
                time_str = dt.strftime("%m/%d %H:%M")
            else:
                time_str = ""

            display = f"{time_str} {first_line}"

            # Filter matches SQL content and Python script
            if filter_text:
                python_script = entry.get("python", "")
                searchable = f"{sql} {python_script}".lower()
                if filter_text.lower() not in searchable:
                    continue
            results.add_option(Option(display, id=str(i)))

        if self.history and results.option_count > 0:
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


class ExportPopup(Container):
    """Floating popup for export file path."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False, priority=True),
    ]

    def __init__(self) -> None:
        super().__init__(id="export-popup")

    def compose(self) -> ComposeResult:
        yield Static("Export Results", id="export-title")
        yield Input(placeholder="trinoq_export_YYYYMMDD_HHMMSS.csv", id="export-path")
        with Horizontal(id="export-buttons"):
            yield Button("Save", id="btn-export-save", variant="primary")
            yield Button("Cancel", id="btn-export-cancel", variant="default")

    def action_cancel(self) -> None:
        self.app.action_hide_export()

    def set_default_path(self) -> None:
        """Set default export path using cwd and timestamp."""
        from datetime import datetime

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_path = str(Path.cwd() / f"trinoq_export_{timestamp}.csv")
        input_widget = self.query_one("#export-path", Input)
        input_widget.clear()
        input_widget.insert_text_at_cursor(default_path)
        input_widget.focus()


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


class HelpPopup(Container):
    """Floating popup showing keybindings help."""

    BINDINGS = [
        Binding("escape", "close", "Close", show=False, priority=True),
        Binding("q", "close", "Close", show=False, priority=True),
    ]

    HELP_TEXT = """
[bold cyan]TrinoQ Keybindings[/]

[bold yellow]Navigation[/]
  [green]Ctrl+C[/]       Enter area selection mode
  [green]h/j/k/l[/]      Move between areas (vim-style)
  [green]Enter/i[/]      Select area and enter
  [green]Esc[/]          Exit selection / close popups

[bold yellow]Query Execution[/]
  [green]F5[/]           Run query
  [green]Ctrl+P[/]       Open command palette

[bold yellow]Layout[/]
  [green]F11[/]          Maximize/restore focused panel
  [green]Drag splitters[/] Resize panels

[bold yellow]Results Table[/]
  [green]h/j/k/l[/]      Navigate cells
  [green]v[/]            Visual mode (cells)
  [green]V[/]            Visual Line mode (entire rows)
  [green]Ctrl+V[/]       Visual Block mode (entire columns)
  [green]y[/]            Yank (copy) selection
  [green]/[/]            Search in results
  [green]n/N[/]          Next/Previous search match
  [green]Esc[/]          Exit visual/search mode

[bold yellow]SQL Templates[/]
  [dim]-- @param key value[/]   Define a parameter
  [dim]{{key}}[/]               Use parameter or env var

[bold yellow]Commands (Ctrl+P)[/]
  Run Query, Save Query, Open Queries
  Clear Results, Search Tables, Toggle Cache
  Toggle Python Panel, Quit

[bold yellow]Caching[/]
  Results are cached to [dim]/tmp/trinoq/[/]
  Use [green]Toggle Cache[/] in command palette to disable

[dim]Press Esc or q to close[/]
"""

    def compose(self) -> ComposeResult:
        yield Static(self.HELP_TEXT, id="help-content")

    def action_close(self) -> None:
        self.app.action_hide_help()


class TrinoQCommands(Provider):
    """Command provider for TrinoQ."""

    @property
    def _commands(self) -> list[tuple[str, Any, str]]:
        return [
            ("Run Query", self.app.action_execute_query, "Execute current SQL query"),
            ("Save Query", self.app.action_save_query, "Save current query"),
            ("Open Queries", self.app.action_show_queries, "Open saved queries"),
            (
                "Query History",
                self.app.action_show_history,
                "Browse executed query history",
            ),
            ("Clear Results", self.app.action_clear_results, "Clear results table"),
            (
                "Toggle Maximize",
                self.app.action_toggle_maximize,
                "Maximize/restore panel",
            ),
            (
                "Toggle Cache",
                self.app.action_toggle_cache,
                "Enable/disable query result caching",
            ),
            (
                "Toggle Python Panel",
                self.app.action_toggle_python_panel,
                "Show/hide Python editor panel",
            ),
            ("Help", self.app.action_show_help, "Show keybindings help"),
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
        layout: vertical;
    }

    #sql-editor {
        height: 100%;
        width: 60%;
        border: round $primary;
    }

    #results-container {
        height: 50%;
        border: round $secondary;
    }

    #results-toolbar {
        height: 1;
        width: 100%;
        align: left middle;
    }

    #results-toolbar Button {
        min-width: 8;
        height: 1;
        margin-right: 1;
        border: none;
        padding: 0 1;
    }

    #results-table {
        height: 1fr;
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
        height: 100%;
        width: 40%;
        border: round $success;
    }

    #python-editor:focus-within {
        border: round $accent;
    }

    #editors-row {
        height: 50%;
        width: 100%;
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
        offset: 15% 30%;
    }

    #search-popup.visible {
        display: block;
    }

    #search-input {
        width: 100%;
        height: 1;
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
        width: 100%;
        height: 1fr;
        max-height: 100%;
        background: $surface;
        border: none;
        padding: 1 2;
        dock: top;
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
        height: 1;
        border: none;
        background: $surface;
    }

    #queries-content {
        width: 100%;
        height: 1fr;
    }

    #queries-list {
        width: 30%;
        height: 100%;
        border: round $surface-lighten-2;
        background: $surface;
        margin-top: 1;
    }

    #queries-preview {
        width: 70%;
        height: 100%;
        border: round $surface-lighten-2;
        background: $surface-darken-1;
        margin-top: 1;
        margin-left: 1;
        padding: 1;
        overflow: auto;
    }

    #history-popup {
        display: none;
        layer: popup;
        width: 100%;
        height: 1fr;
        max-height: 100%;
        background: $surface;
        border: none;
        padding: 1 2;
        dock: top;
    }

    #history-popup.visible {
        display: block;
    }

    #history-title {
        width: 100%;
        text-align: center;
        color: $text-muted;
        margin-bottom: 1;
    }

    #history-filter {
        width: 100%;
        height: 1;
        border: none;
        background: $surface;
    }

    #history-content {
        width: 100%;
        height: 1fr;
    }

    #history-list {
        width: 30%;
        height: 100%;
        border: round $surface-lighten-2;
        background: $surface;
        margin-top: 1;
    }

    #history-preview {
        width: 70%;
        height: 100%;
        border: round $surface-lighten-2;
        background: $surface-darken-1;
        margin-top: 1;
        margin-left: 1;
        padding: 1;
        overflow: auto;
    }

    #save-query-popup {
        display: none;
        layer: popup;
        width: 50%;
        height: auto;
        background: $surface;
        border: round $surface-lighten-2;
        padding: 1 2;
        offset: 25% 40%;
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

    #export-popup {
        display: none;
        layer: popup;
        width: 80%;
        height: auto;
        background: $surface;
        border: round $surface-lighten-2;
        padding: 1 2;
        offset: 10% 40%;
    }

    #export-popup.visible {
        display: block;
    }

    #export-title {
        width: 100%;
        text-align: center;
        color: $text-muted;
        margin-bottom: 1;
    }

    #export-path {
        width: 100%;
        height: 3;
        border: solid $primary;
        background: $surface;
    }

    #export-buttons {
        width: 100%;
        height: 3;
        margin-top: 1;
        align: center middle;
    }

    #export-buttons Button {
        min-width: 10;
        margin: 0 1;
    }

    #sql-editor.hidden {
        display: none;
    }

    #python-editor.hidden {
        display: none;
    }

    #editors-row.hidden {
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

    #help-popup {
        display: none;
        layer: popup;
        width: 60%;
        height: auto;
        max-height: 80%;
        background: $surface;
        border: round $primary;
        padding: 1 2;
        offset: 20% 10%;
    }

    #help-popup.visible {
        display: block;
    }

    #help-content {
        width: 100%;
        height: auto;
    }
    """

    BINDINGS = [
        Binding("f5", "execute_query", "Run", show=True, priority=True),
        Binding("ctrl+p", "command_palette", "Menu", show=True, priority=True),
        Binding("f11", "toggle_maximize", "Maximize", show=False, priority=True),
    ]

    COMMANDS = {TrinoQCommands}

    _maximized_panel: str | None = None  # Track which panel is maximized
    _connection: Any = None
    _connection_created_at: float = 0  # Timestamp when connection was created
    _connection_max_age: int = (
        55 * 60
    )  # Refresh token after 55 minutes (before 1hr expiry)
    _area_select_mode: bool = False  # Area selection mode
    _selected_area: int = 0  # 0=sql, 1=python, 2=results
    _areas: list[str] = ["sql-editor", "python-editor", "results-container"]
    _areas_focus: list[str] = ["sql-editor", "python-editor", "results-table"]
    _cache_enabled: bool = True  # Enable query result caching by default
    _python_panel_visible: bool = False  # Python panel hidden by default

    # Layout ratios for splitter resize
    _editors_ratio: float = 0.5  # 50% editors, 50% results
    _sql_ratio: float = 0.6  # 60% SQL, 40% Python
    _layout_config_path: Path = Path.home() / ".config" / "trinoq" / "layout.json"

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="content-area"):
            with Vertical(id="main-area"):
                with Horizontal(id="editors-row"):
                    yield VimEditor(
                        id="sql-editor",
                        auto_start=True,
                        initial_content="-- SQL query\nselect 1",
                    )
                    yield Splitter(id="vertical-splitter", orientation="vertical")
                    yield VimEditor(
                        id="python-editor",
                        auto_start=True,
                        initial_content="# python script\n# df = df * 100",
                    )
                yield Splitter(id="horizontal-splitter", orientation="horizontal")
                with Container(id="results-container"):
                    with Horizontal(id="results-toolbar"):
                        yield Button("Copy", id="btn-copy", variant="default")
                        yield Button("Export", id="btn-export", variant="default")
                    yield ResultsTable()
        yield StatusBar(id="status-bar")
        yield SearchPopup()
        yield QueriesPopup()
        yield HistoryPopup()
        yield SaveQueryPopup()
        yield ExportPopup()
        yield HelpPopup(id="help-popup")
        yield Footer()

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        self._load_layout()
        self._apply_layout()

        # Hide Python panel by default
        if not self._python_panel_visible:
            self.query_one("#python-editor").add_class("hidden")
            self.query_one("#vertical-splitter").add_class("hidden")
            self.query_one("#sql-editor").styles.width = "100%"

        self.query_one("#sql-editor", VimEditor).focus()

        # Load tables cache immediately and refresh in background
        self._load_tables_cache_on_startup()

    def _load_tables_cache_on_startup(self) -> None:
        """Load tables from cache on startup and refresh in background."""
        popup = self.query_one(SearchPopup)

        # Load from disk cache immediately
        cached = load_tables_cache()
        if cached:
            popup.all_tables = cached
            self.query_one(StatusBar).status = f"Loaded {len(cached)} tables from cache"

        # Always refresh in background on startup (if cache is stale or empty)
        if cache_needs_refresh() or not cached:
            self._load_all_tables()

    def _load_layout(self) -> None:
        """Load layout ratios from config file."""
        try:
            if self._layout_config_path.exists():
                data = json.loads(self._layout_config_path.read_text())
                self._editors_ratio = data.get("editors_ratio", 0.5)
                self._sql_ratio = data.get("sql_ratio", 0.6)
        except Exception:
            pass

    def _save_layout(self) -> None:
        """Save layout ratios to config file."""
        try:
            self._layout_config_path.parent.mkdir(parents=True, exist_ok=True)
            self._layout_config_path.write_text(
                json.dumps(
                    {
                        "editors_ratio": self._editors_ratio,
                        "sql_ratio": self._sql_ratio,
                    }
                )
            )
        except Exception:
            pass

    def _apply_layout(self) -> None:
        """Apply current ratios to layout."""
        # Clamp values between 10% and 90%
        self._editors_ratio = max(0.1, min(0.9, self._editors_ratio))
        self._sql_ratio = max(0.1, min(0.9, self._sql_ratio))

        try:
            # Apply editors vs results ratio
            editors_row = self.query_one("#editors-row")
            results = self.query_one("#results-container")
            editors_row.styles.height = f"{int(self._editors_ratio * 100)}%"
            results.styles.height = f"{int((1 - self._editors_ratio) * 100)}%"

            # Apply SQL vs Python ratio
            sql_editor = self.query_one("#sql-editor")
            python_editor = self.query_one("#python-editor")
            sql_editor.styles.width = f"{int(self._sql_ratio * 100)}%"
            python_editor.styles.width = f"{int((1 - self._sql_ratio) * 100)}%"
        except Exception:
            pass

    def on_splitter_dragged(self, event: Splitter.Dragged) -> None:
        """Handle splitter drag events."""
        splitter_id = event.splitter.id

        if splitter_id == "horizontal-splitter":
            # Vertical resize: editors vs results
            try:
                main_area = self.query_one("#main-area")
                total_height = main_area.size.height
                if total_height > 0:
                    delta_ratio = event.delta / total_height
                    self._editors_ratio += delta_ratio
                    self._apply_layout()
                    self._save_layout()
            except Exception:
                pass

        elif splitter_id == "vertical-splitter":
            # Horizontal resize: SQL vs Python
            try:
                editors_row = self.query_one("#editors-row")
                total_width = editors_row.size.width
                if total_width > 0:
                    delta_ratio = event.delta / total_width
                    self._sql_ratio += delta_ratio
                    self._apply_layout()
                    self._save_layout()
            except Exception:
                pass

    def on_key(self, event: events.Key) -> None:
        """Handle key events for area selection mode."""
        # Ctrl+C to enter area selection mode (from non-vim areas like results)
        if event.key == "ctrl+c" and not self._area_select_mode:
            self._enter_area_select_mode()
            event.stop()
            return

        # Escape to exit area selection mode
        if event.key == "escape" and self._area_select_mode:
            self._exit_area_select_mode()
            event.stop()
            return

        # Handle navigation in area selection mode (positional)
        if self._area_select_mode:
            # Layout when Python visible:
            # [0: SQL] [1: Python]
            # [2: Results        ]
            # Layout when Python hidden:
            # [0: SQL]
            # [2: Results]
            if event.key in ("left", "h"):
                # Move left: Python -> SQL (only if Python visible)
                if self._python_panel_visible and self._selected_area == 1:
                    self._selected_area = 0
                    self._update_area_highlight()
                event.stop()
            elif event.key in ("right", "l"):
                # Move right: SQL -> Python (only if Python visible)
                if self._python_panel_visible and self._selected_area == 0:
                    self._selected_area = 1
                    self._update_area_highlight()
                event.stop()
            elif event.key in ("up", "k"):
                # Move up: Results -> SQL
                if self._selected_area == 2:
                    self._selected_area = 0
                    self._update_area_highlight()
                event.stop()
            elif event.key in ("down", "j"):
                # Move down: SQL/Python -> Results
                if self._selected_area in (0, 1):
                    self._selected_area = 2
                    self._update_area_highlight()
                event.stop()
            elif event.key in ("enter", "i"):
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

    def _get_connection(self) -> Any:
        """Get or create a Trino connection. Refreshes token if older than 55 minutes."""
        from trinoq import create_connection

        current_time = time.time()
        connection_age = current_time - self._connection_created_at

        # Create new connection if none exists or token is about to expire
        if self._connection is None or connection_age > self._connection_max_age:
            self._connection = create_connection()
            self._connection_created_at = current_time

        return self._connection

    @work(thread=True, exclusive=True, group="query")
    def _execute_query(self, sql: str) -> None:
        """Execute the SQL query in a background thread."""
        from trinoq import execute

        status = self.query_one(StatusBar)
        results_table = self.query_one(ResultsTable)

        self.call_from_thread(setattr, status, "is_running", True)
        cache_status = "cached" if self._cache_enabled else "no cache"
        self.call_from_thread(
            setattr, status, "status", f"Running query ({cache_status})..."
        )

        start_time = time.time()

        try:
            # Render template variables ({{var}} with @param values)
            rendered_sql = render_query(sql)
            conn = self._get_connection()
            df = execute(
                rendered_sql, engine=conn, no_cache=not self._cache_enabled, quiet=True
            )
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

            # Auto-save to query history
            save_query_to_history(sql)

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
        cache_status = "cached" if self._cache_enabled else "no cache"
        self.call_from_thread(
            setattr, status, "status", f"Running query with Python ({cache_status})..."
        )

        start_time = time.time()

        try:
            # Render template variables ({{var}} with @param values)
            rendered_sql = render_query(sql)
            # Execute the SQL query
            conn = self._get_connection()
            df = execute(
                rendered_sql, engine=conn, no_cache=not self._cache_enabled, quiet=True
            )

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

            # Auto-save to query history (with Python script)
            save_query_to_history(sql, python_script)

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

        # Check if Python editor has content (always visible now)
        python_editor = self.query_one("#python-editor", VimEditor)
        python_script = python_editor.content.strip()
        # Check if there's actual Python code (not just comments)
        python_lines = [
            line
            for line in python_script.split("\n")
            if line.strip() and not line.strip().startswith("#")
        ]
        if python_lines:
            self._execute_query_with_python(sql.strip(), python_script)
            return

        self._execute_query(sql.strip())

    def action_clear_results(self) -> None:
        """Clear the results table."""
        results_table = self.query_one(ResultsTable)
        results_table.clear(columns=True)
        self.query_one(StatusBar).status = "Results cleared"

    def action_toggle_cache(self) -> None:
        """Toggle query result caching."""
        self._cache_enabled = not self._cache_enabled
        status = "enabled" if self._cache_enabled else "disabled"
        self.query_one(StatusBar).status = f"Cache {status}"
        self.notify(f"Query caching {status}", severity="information")

    def action_toggle_python_panel(self) -> None:
        """Toggle Python editor panel visibility."""
        self._python_panel_visible = not self._python_panel_visible
        python_editor = self.query_one("#python-editor")
        v_splitter = self.query_one("#vertical-splitter")
        sql_editor = self.query_one("#sql-editor")

        if self._python_panel_visible:
            python_editor.remove_class("hidden")
            v_splitter.remove_class("hidden")
            self._apply_layout()
            self.query_one(StatusBar).status = "Python panel shown"
            self.notify("Python panel visible", severity="information")
        else:
            python_editor.add_class("hidden")
            v_splitter.add_class("hidden")
            sql_editor.styles.width = "100%"
            self.query_one(StatusBar).status = "Python panel hidden"
            self.notify("Python panel hidden", severity="information")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        if event.button.id == "btn-copy":
            self._copy_results_to_clipboard()
        elif event.button.id == "btn-export":
            self._export_results()
        elif event.button.id == "btn-export-save":
            path = self.query_one("#export-path", Input).value.strip()
            if path:
                self._do_export(path)
                self.action_hide_export()
            else:
                self.notify("Please enter a file path", severity="warning")
        elif event.button.id == "btn-export-cancel":
            self.action_hide_export()

    def _copy_results_to_clipboard(self) -> None:
        """Copy all results to clipboard as formatted table (df.to_string style)."""
        import pandas as pd

        results_table = self.query_one(ResultsTable)
        if results_table.row_count == 0:
            self.query_one(StatusBar).status = "No results to copy"
            return

        # Get column headers
        columns = [str(col.label) for col in results_table.columns.values()]

        # Get all rows
        rows = []
        for row_idx in range(results_table.row_count):
            row_values = []
            for col_idx in range(len(columns)):
                try:
                    row_key = results_table._row_locations.get_key(row_idx)
                    col_key = results_table._column_locations.get_key(col_idx)
                    if row_key and col_key:
                        value = results_table.get_cell(row_key, col_key)
                        row_values.append(value)
                except Exception:
                    row_values.append("")
            rows.append(row_values)

        # Create DataFrame and use to_string()
        df = pd.DataFrame(rows, columns=columns)
        text = df.to_string(index=False)

        results_table._copy_to_clipboard(text)
        self.query_one(StatusBar).status = f"Copied {results_table.row_count} rows"

    def _export_results(self) -> None:
        """Show export popup to get file path from user."""
        self.action_show_export()

    def _do_export(self, export_path: str) -> None:
        """Export results to CSV file at the given path."""
        import csv

        results_table = self.query_one(ResultsTable)
        if results_table.row_count == 0:
            self.query_one(StatusBar).status = "No results to export"
            return

        path = Path(export_path)

        # Get column headers
        columns = [str(col.label) for col in results_table.columns.values()]

        # Write CSV
        try:
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                for row_idx in range(results_table.row_count):
                    row_values = []
                    for col_idx in range(len(columns)):
                        try:
                            row_key = results_table._row_locations.get_key(row_idx)
                            col_key = results_table._column_locations.get_key(col_idx)
                            if row_key and col_key:
                                value = results_table.get_cell(row_key, col_key)
                                row_values.append(str(value))
                        except Exception:
                            row_values.append("")
                    writer.writerow(row_values)
            self.query_one(StatusBar).status = f"Exported to {path}"
        except Exception as e:
            self.query_one(StatusBar).status = f"Export failed: {e}"

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
        """Toggle maximize for the focused panel (sql, python, or results)."""
        sql_editor = self.query_one("#sql-editor")
        python_editor = self.query_one("#python-editor")
        editors_row = self.query_one("#editors-row")
        results = self.query_one("#results-container")
        h_splitter = self.query_one("#horizontal-splitter")
        v_splitter = self.query_one("#vertical-splitter")

        # Determine which panel is focused
        focused = self.focused
        if focused is None:
            return

        # Find which specific panel we're in
        current_panel = None
        if focused.id == "sql-editor":
            current_panel = "sql"
        elif focused.id == "python-editor":
            current_panel = "python"
        elif focused.id in ("results-table", "results-container"):
            current_panel = "results"
        else:
            # Check ancestors
            node = focused
            while node is not None:
                if node.id == "sql-editor":
                    current_panel = "sql"
                    break
                elif node.id == "python-editor":
                    current_panel = "python"
                    break
                elif node.id == "results-container":
                    current_panel = "results"
                    break
                node = node.parent

        if current_panel is None:
            self.notify("Focus a panel to maximize")
            return

        # If already maximized, restore
        if self._maximized_panel is not None:
            # Restore all panels
            sql_editor.remove_class("hidden")
            python_editor.remove_class("hidden")
            editors_row.remove_class("hidden")
            results.remove_class("hidden")
            h_splitter.remove_class("hidden")
            v_splitter.remove_class("hidden")
            self._maximized_panel = None
            self._apply_layout()  # Restore splitter ratios
            self.query_one(StatusBar).status = "Restored layout"
        else:
            # Maximize current panel
            h_splitter.add_class("hidden")
            v_splitter.add_class("hidden")

            if current_panel == "sql":
                python_editor.add_class("hidden")
                results.add_class("hidden")
                editors_row.styles.height = "100%"
                sql_editor.styles.width = "100%"
                self._maximized_panel = "sql"
                self.query_one(StatusBar).status = "SQL maximized (F11 to restore)"
            elif current_panel == "python":
                sql_editor.add_class("hidden")
                results.add_class("hidden")
                editors_row.styles.height = "100%"
                python_editor.styles.width = "100%"
                self._maximized_panel = "python"
                self.query_one(StatusBar).status = "Python maximized (F11 to restore)"
            else:  # results
                editors_row.add_class("hidden")
                results.styles.height = "100%"
                self._maximized_panel = "results"
                self.query_one(StatusBar).status = "Results maximized (F11 to restore)"

    def action_show_search(self) -> None:
        """Show the table search popup."""
        popup = self.query_one(SearchPopup)
        popup.add_class("visible")
        popup.query_one("#search-input", Input).value = ""
        popup.query_one("#search-results", OptionList).clear_options()
        popup.matches = []
        popup.query_one("#search-input", Input).focus()

        # Show cached tables immediately (already loaded on startup)
        if popup.all_tables:
            popup.update_results(popup.all_tables[:50])
            self.query_one(
                StatusBar
            ).status = f"{len(popup.all_tables)} tables available"

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

    def action_show_export(self) -> None:
        """Show the export popup."""
        results_table = self.query_one(ResultsTable)
        if results_table.row_count == 0:
            self.query_one(StatusBar).status = "No results to export"
            return
        popup = self.query_one(ExportPopup)
        popup.add_class("visible")
        self.call_after_refresh(popup.set_default_path)

    def action_hide_export(self) -> None:
        """Hide the export popup."""
        popup = self.query_one(ExportPopup)
        popup.remove_class("visible")
        self.query_one(ResultsTable).focus()

    def action_show_help(self) -> None:
        """Show the help popup with keybindings."""
        popup = self.query_one(HelpPopup)
        popup.add_class("visible")
        popup.focus()

    def action_hide_help(self) -> None:
        """Hide the help popup."""
        popup = self.query_one(HelpPopup)
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

    def action_show_history(self) -> None:
        """Show the query history popup."""
        popup = self.query_one(HistoryPopup)
        popup.add_class("visible")
        popup.load_history()
        popup.query_one("#history-filter", Input).value = ""
        popup.query_one("#history-filter", Input).focus()

    def action_hide_history(self) -> None:
        """Hide the history popup."""
        popup = self.query_one(HistoryPopup)
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
            # Use option.id (the real index) not option_index (filtered list position)
            if event.option and event.option.id:
                query_idx = int(event.option.id)
                if query_idx < len(popup.queries):
                    query = popup.queries[query_idx]
                    editor = self.query_one("#sql-editor", VimEditor)
                    editor.content = query.get("sql", "")
                    self.notify(f"Loaded: {query.get('name', 'query')}")
            self.action_hide_queries()

        elif event.option_list.id == "history-list":
            popup = self.query_one(HistoryPopup)
            # Use option.id (the real index) not option_index (filtered list position)
            if event.option and event.option.id:
                history_idx = int(event.option.id)
                if history_idx < len(popup.history):
                    entry = popup.history[history_idx]
                    sql_editor = self.query_one("#sql-editor", VimEditor)
                    sql_editor.content = entry.get("sql", "")
                    # Also load Python script if present
                    python_script = entry.get("python", "")
                    if python_script:
                        python_editor = self.query_one("#python-editor", VimEditor)
                        python_editor.content = python_script
                    self.notify("Loaded query from history")
            self.action_hide_history()

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

        elif event.input.id == "history-filter":
            popup = self.query_one(HistoryPopup)
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

        elif event.input.id == "export-path":
            path = event.value.strip()
            if path:
                self._do_export(path)
                self.action_hide_export()
            else:
                self.notify("Please enter a file path", severity="warning")

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
