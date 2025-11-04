# Tests for TrinoQ

This directory contains the test suite for TrinoQ.

## Running Tests

### Run all tests
```bash
pytest tests/
```

### Run with verbose output
```bash
pytest tests/ -v
```

### Run with coverage
```bash
pytest tests/ --cov=src --cov-report=term-missing
```

### Run specific test class
```bash
pytest tests/test_trinoq.py::TestExtractParams -v
```

### Run specific test
```bash
pytest tests/test_trinoq.py::TestExtractParams::test_single_param -v
```

## Test Coverage

Current test coverage focuses on:

### Core Functionality (100% covered)
- ✅ Parameter extraction (`@param`)
- ✅ Eval code extraction (`@eval`)
- ✅ Eval file extraction (`@eval-file`)
- ✅ Query parameter substitution (single and double braces)
- ✅ Environment variable substitution
- ✅ Parameter precedence over env vars

### CLI Options (Mocked)
- ✅ `--dry-run` flag
- ✅ `-t / --timing` flag
- ✅ `-o / --output` flag (json, csv)

### Integration Tests
- ✅ Multiple annotations combined
- ✅ Params with eval
- ✅ Params with eval-file

### Not Covered (Requires Trino Connection)
- Database connection and authentication
- Actual query execution
- Caching mechanism
- Parquet file operations

## Test Structure

```
tests/
├── test_trinoq.py    # Main test suite
└── README.md         # This file
```

### Test Classes

- **TestExtractParams**: Tests for `@param` annotation extraction
- **TestExtractEvalCode**: Tests for `@eval` annotation extraction
- **TestExtractEvalFile**: Tests for `@eval-file` annotation extraction
- **TestGetQuery**: Tests for query parameter substitution
- **TestIntegration**: Integration tests combining multiple features
- **TestCLIOptions**: Tests for CLI flags (mocked)

## Writing New Tests

When adding new functionality:

1. Add tests to the appropriate test class or create a new one
2. Use descriptive test names: `test_<feature>_<scenario>`
3. Use mocks for external dependencies (database, filesystem)
4. Test both success and failure cases
5. Run tests with coverage to ensure new code is tested

Example:
```python
def test_new_feature_with_valid_input(self):
    # Arrange
    input_data = "test input"
    
    # Act
    result = new_function(input_data)
    
    # Assert
    assert result == expected_output
```

## Dependencies

Tests require:
- pytest>=7.0
- pytest-cov

Install with:
```bash
uv pip install -e ".[dev]"
```
