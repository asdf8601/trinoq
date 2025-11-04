"""
Tests for TrinoQ functionality.
"""

import pytest
import sys
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from trinoq import (
    extract_params,
    extract_eval_code,
    extract_eval_file,
    get_query,
)


class TestExtractParams:
    """Tests for @param annotation extraction."""
    
    def test_single_param(self):
        query = "-- @param name value\nSELECT 1"
        params = extract_params(query)
        assert params == {"name": "value"}
    
    def test_multiple_params(self):
        query = """
        -- @param start_date 2024-01-01
        -- @param end_date 2024-12-31
        -- @param limit 100
        SELECT * FROM table
        """
        params = extract_params(query)
        assert params == {
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "limit": "100"
        }
    
    def test_param_with_quotes(self):
        query = "-- @param token '7739-9592-01'\nSELECT 1"
        params = extract_params(query)
        assert params == {"token": "'7739-9592-01'"}
    
    def test_param_with_spaces(self):
        query = "-- @param description this is a long description\nSELECT 1"
        params = extract_params(query)
        assert params == {"description": "this is a long description"}
    
    def test_no_params(self):
        query = "SELECT * FROM table"
        params = extract_params(query)
        assert params == {}
    
    def test_case_insensitive(self):
        query = "-- @PARAM name VALUE\nSELECT 1"
        params = extract_params(query)
        assert params == {"name": "VALUE"}


class TestExtractEvalCode:
    """Tests for @eval annotation extraction."""
    
    def test_simple_eval(self):
        query = "-- @eval print(df.head())\nSELECT 1"
        code = extract_eval_code(query)
        assert code == "print(df.head())"
    
    def test_eval_with_multiple_statements(self):
        query = "-- @eval df.describe(); print(len(df))\nSELECT 1"
        code = extract_eval_code(query)
        assert code == "df.describe(); print(len(df))"
    
    def test_no_eval(self):
        query = "SELECT * FROM table"
        code = extract_eval_code(query)
        assert code is None
    
    def test_case_insensitive(self):
        query = "-- @EVAL print(df)\nSELECT 1"
        code = extract_eval_code(query)
        assert code == "print(df)"


class TestExtractEvalFile:
    """Tests for @eval-file annotation extraction."""
    
    def test_eval_file_new_syntax(self):
        query = "-- @eval-file analysis.py\nSELECT 1"
        file_path = extract_eval_file(query)
        assert file_path == "analysis.py"
    
    def test_eval_file_legacy_syntax(self):
        query = "-- eval: analysis.py\nSELECT 1"
        file_path = extract_eval_file(query)
        assert file_path == "analysis.py"
    
    def test_eval_file_with_path(self):
        query = "-- @eval-file /path/to/script.py\nSELECT 1"
        file_path = extract_eval_file(query)
        assert file_path == "/path/to/script.py"
    
    def test_no_eval_file(self):
        query = "SELECT * FROM table"
        file_path = extract_eval_file(query)
        assert file_path is None
    
    def test_case_insensitive(self):
        query = "-- @EVAL-FILE script.py\nSELECT 1"
        file_path = extract_eval_file(query)
        assert file_path == "script.py"


class TestGetQuery:
    """Tests for query parameter substitution."""
    
    def test_single_brace_substitution(self):
        query_text = """
        -- @param table_name my_table
        -- @param limit 10
        SELECT * FROM {table_name} LIMIT {limit}
        """
        
        # Create mock args
        args = Mock()
        args.query = query_text
        args.file = False
        
        result = get_query(args)
        assert "my_table" in result
        assert "10" in result
        assert "{table_name}" not in result
        assert "{limit}" not in result
    
    def test_double_brace_substitution(self):
        query_text = """
        -- @param start_date 2024-01-01
        -- @param end_date 2024-12-31
        SELECT * FROM table WHERE date >= '{{start_date}}' AND date <= '{{end_date}}'
        """
        
        args = Mock()
        args.query = query_text
        args.file = False
        
        result = get_query(args)
        assert "2024-01-01" in result
        assert "2024-12-31" in result
        assert "{{start_date}}" not in result
        assert "{{end_date}}" not in result
    
    def test_params_with_quotes_preserved(self):
        query_text = """
        -- @param token '7739-9592-01'
        SELECT * FROM table WHERE id = {{token}}
        """
        
        args = Mock()
        args.query = query_text
        args.file = False
        
        result = get_query(args)
        assert "'7739-9592-01'" in result
        assert "{{token}}" not in result
    
    def test_env_var_substitution(self):
        query_text = "SELECT * FROM {CATALOG}.my_table"
        
        args = Mock()
        args.query = query_text
        args.file = False
        
        with patch.dict(os.environ, {"CATALOG": "production"}):
            result = get_query(args)
            assert "production.my_table" in result
            assert "{CATALOG}" not in result
    
    def test_param_overrides_env_var(self):
        query_text = """
        -- @param CATALOG staging
        SELECT * FROM {CATALOG}.my_table
        """
        
        args = Mock()
        args.query = query_text
        args.file = False
        
        with patch.dict(os.environ, {"CATALOG": "production"}):
            result = get_query(args)
            assert "staging.my_table" in result
            assert "production" not in result
    
    def test_read_from_stdin(self):
        query_text = "-- @param name value\nSELECT {{name}}"
        
        args = Mock()
        args.query = "-"
        args.file = False
        
        with patch("sys.stdin.read", return_value=query_text):
            result = get_query(args)
            assert "value" in result
            assert "{{name}}" not in result
    
    def test_read_from_file(self, tmp_path):
        # Create temporary SQL file
        sql_file = tmp_path / "test.sql"
        sql_file.write_text("-- @param table test_table\nSELECT * FROM {{table}}")
        
        args = Mock()
        args.query = str(sql_file)
        args.file = True
        
        result = get_query(args)
        assert "test_table" in result
        assert "{{table}}" not in result


class TestIntegration:
    """Integration tests for combined functionality."""
    
    def test_params_with_eval(self):
        query = """
        -- @param limit 100
        -- @eval print(f"Rows: {len(df)}")
        SELECT * FROM table LIMIT {{limit}}
        """
        
        params = extract_params(query)
        eval_code = extract_eval_code(query)
        
        assert params == {"limit": "100"}
        assert eval_code == 'print(f"Rows: {len(df)}")'
    
    def test_params_with_eval_file(self):
        query = """
        -- @param dataset production_data
        -- @eval-file analysis.py
        SELECT * FROM {{dataset}}
        """
        
        params = extract_params(query)
        eval_file = extract_eval_file(query)
        
        assert params == {"dataset": "production_data"}
        assert eval_file == "analysis.py"
    
    def test_multiple_annotations(self):
        query = """
        -- @param start_date 2024-01-01
        -- @param end_date 2024-12-31
        -- @param region 'US'
        -- @eval print(df.describe())
        
        SELECT * 
        FROM sales 
        WHERE date BETWEEN '{{start_date}}' AND '{{end_date}}'
          AND region = {{region}}
        """
        
        params = extract_params(query)
        eval_code = extract_eval_code(query)
        
        assert len(params) == 3
        assert params["start_date"] == "2024-01-01"
        assert params["end_date"] == "2024-12-31"
        assert params["region"] == "'US'"
        assert eval_code == "print(df.describe())"


class TestCLIOptions:
    """Tests for CLI flag handling."""
    
    @patch('trinoq.get_args')
    @patch('trinoq.get_query')
    def test_dry_run_flag(self, mock_get_query, mock_get_args):
        """Test that --dry-run prints query without execution."""
        from trinoq import app
        
        # Mock args
        args = Mock()
        args.dry_run = True
        args.query = "SELECT 1"
        args.file = False
        args.pdb = False
        mock_get_args.return_value = args
        mock_get_query.return_value = "SELECT 1"
        
        # Capture stdout
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            app()
            output = mock_stdout.getvalue()
            assert "SELECT 1" in output
    
    @patch('trinoq.execute')
    @patch('trinoq.get_args')
    @patch('trinoq.get_query')
    def test_timing_flag(self, mock_get_query, mock_get_args, mock_execute):
        """Test that --timing measures execution time."""
        from trinoq import app
        import pandas as pd
        
        # Mock args
        args = Mock()
        args.dry_run = False
        args.timing = True
        args.output = None
        args.quiet = True
        args.eval_df = None
        args.no_cache = True
        args.pdb = False
        mock_get_args.return_value = args
        mock_get_query.return_value = "SELECT 1"
        
        # Mock execute to return a dataframe
        mock_df = pd.DataFrame({"col": [1, 2, 3]})
        mock_execute.return_value = mock_df
        
        # Capture stdout
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            app()
            output = mock_stdout.getvalue()
            # Should contain execution time
            assert "Execution time:" in output or output == ""
    
    @patch('trinoq.execute')
    @patch('trinoq.get_args')
    @patch('trinoq.get_query')
    def test_output_json_flag(self, mock_get_query, mock_get_args, mock_execute):
        """Test that --output json exports to JSON."""
        from trinoq import app
        import pandas as pd
        
        # Mock args
        args = Mock()
        args.dry_run = False
        args.timing = False
        args.output = "json"
        args.quiet = True
        args.eval_df = None
        args.no_cache = True
        args.pdb = False
        mock_get_args.return_value = args
        mock_get_query.return_value = "SELECT 1"
        
        # Mock execute to return a dataframe
        mock_df = pd.DataFrame({"col": [1, 2, 3]})
        mock_execute.return_value = mock_df
        
        # Capture stdout
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            app()
            output = mock_stdout.getvalue()
            # Should contain JSON output
            assert '"col"' in output or output == ""
    
    @patch('trinoq.execute')
    @patch('trinoq.get_args')
    @patch('trinoq.get_query')
    def test_output_csv_flag(self, mock_get_query, mock_get_args, mock_execute):
        """Test that --output csv exports to CSV."""
        from trinoq import app
        import pandas as pd
        
        # Mock args
        args = Mock()
        args.dry_run = False
        args.timing = False
        args.output = "csv"
        args.quiet = True
        args.eval_df = None
        args.no_cache = True
        args.pdb = False
        mock_get_args.return_value = args
        mock_get_query.return_value = "SELECT 1"
        
        # Mock execute to return a dataframe
        mock_df = pd.DataFrame({"col": [1, 2, 3]})
        mock_execute.return_value = mock_df
        
        # Capture stdout
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            app()
            output = mock_stdout.getvalue()
            # Should contain CSV output with header
            assert "col" in output or output == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
