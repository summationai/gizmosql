# GizmoSQL Test Suite

This comprehensive test suite validates the PIVOT fix implementation (GitHub issue #44) and ensures no regressions in existing functionality.

## Test Structure

### Core Test Modules

- **`test_pivot_issues.py`** - Tests for PIVOT functionality and GitHub issue #44 fix
- **`test_statement_execution.py`** - Tests for statement execution modes and fallback logic  
- **`test_regression.py`** - Regression tests to prevent breaking existing functionality
- **`test_performance.py`** - Performance tests to ensure no significant performance impact
- **`test_integration.py`** - End-to-end integration tests covering full workflows

### Supporting Files

- **`conftest.py`** - Pytest configuration and shared fixtures
- **`run_tests.py`** - Test runner with various execution options
- **`test_requirements.txt`** - Python dependencies for testing
- **`README.md`** - This documentation

## Quick Start

### Prerequisites

1. **GizmoSQL Server Built**: Ensure `gizmosql_server` binary is built and in PATH
2. **Python Dependencies**: Install test requirements
   ```bash
   pip install -r tests/test_requirements.txt
   ```

### Running Tests

#### Quick Smoke Test
```bash
python tests/run_tests.py --smoke
```

#### PIVOT-Specific Tests
```bash
python tests/run_tests.py --suite pivot
```

#### Full Test Suite
```bash
python tests/run_tests.py --suite all
```

#### With Coverage
```bash
python tests/run_tests.py --suite all --coverage
```

## Test Categories

### 1. PIVOT Issue Tests (`test_pivot_issues.py`)

Tests the specific PIVOT functionality that was failing in GitHub issue #44:

- **`test_pivot_github_issue_44_original_query`** - Tests the exact problematic query from the issue
- **`test_pivot_standard_syntax_still_works`** - Ensures standard PIVOT syntax continues to work
- **`test_pivot_with_different_aggregations`** - Tests PIVOT with various aggregation functions
- **`test_pivot_with_complex_where_conditions`** - Tests complex WHERE clauses in PIVOT
- **`test_pivot_with_subqueries`** - Tests PIVOT with subqueries that could cause multiple statement issues
- **`test_multiple_pivot_operations`** - Tests multiple PIVOT operations in sequence

### 2. Statement Execution Tests (`test_statement_execution.py`)

Tests the internal statement execution logic and fallback mechanism:

- **`test_simple_prepared_statements`** - Ensures simple queries use prepared statements
- **`test_multiple_statement_detection`** - Tests detection of queries requiring multiple statements
- **`test_prepared_statement_parameters`** - Tests parameter handling in prepared statements
- **`test_direct_execution_fallback_preserves_results`** - Ensures fallback produces correct results
- **`test_error_handling_in_direct_execution`** - Tests error handling in fallback mode

### 3. Regression Tests (`test_regression.py`)

Comprehensive tests to prevent breaking existing functionality:

- **`test_basic_select_queries`** - Basic SELECT operations
- **`test_table_operations`** - CREATE, INSERT, UPDATE, DELETE operations
- **`test_join_operations`** - Various JOIN types
- **`test_aggregate_functions`** - SUM, COUNT, AVG, etc.
- **`test_group_by_operations`** - GROUP BY with various conditions
- **`test_order_by_operations`** - Sorting and ordering
- **`test_where_conditions`** - Filtering with WHERE clauses
- **`test_subqueries`** - Nested query support
- **`test_data_types`** - Various data type handling
- **`test_window_functions`** - Window function support
- **`test_cte_queries`** - Common Table Expression support

### 4. Performance Tests (`test_performance.py`)

Performance validation to ensure the fix doesn't negatively impact performance:

- **`test_prepared_statement_performance`** - Performance of prepared statement queries
- **`test_direct_execution_performance`** - Performance of direct execution (PIVOT) queries
- **`test_mixed_query_performance`** - Performance when mixing execution modes
- **`test_parameter_binding_performance`** - Parameter binding efficiency
- **`test_large_result_set_performance`** - Large result set handling
- **`test_memory_usage_stability`** - Memory leak detection

### 5. Integration Tests (`test_integration.py`)

End-to-end tests covering realistic usage scenarios:

- **`test_end_to_end_pivot_workflow`** - Complete PIVOT workflow from data to results
- **`test_multi_backend_compatibility`** - DuckDB-specific feature compatibility
- **`test_real_world_analytics_scenarios`** - Realistic analytics use cases
- **`test_error_recovery_and_robustness`** - Error recovery and system robustness
- **`test_data_integrity_across_execution_modes`** - Data integrity validation

## Test Configuration

### Environment Variables

- **`GIZMOSQL_SERVER_BINARY`** - Path to gizmosql_server binary (auto-detected)
- **`GIZMOSQL_PASSWORD`** - Password for test server (defaults to "test_password_123")

### Pytest Markers

Tests are organized with pytest markers for selective execution:

- `@pytest.mark.pivot` - PIVOT-related tests
- `@pytest.mark.regression` - Regression prevention tests  
- `@pytest.mark.performance` - Performance tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.slow` - Slow-running tests

### Test Data

Tests use both:
1. **Static test data** - Predefined datasets for consistent testing
2. **Generated test data** - Randomly generated data for performance and scale testing

## Running Specific Test Scenarios

### Test the Original GitHub Issue

```bash
pytest tests/test_pivot_issues.py::TestPivotIssues::test_pivot_github_issue_44_original_query -v
```

### Test Only Fast Tests (Exclude Slow Ones)

```bash
pytest -m "not slow" tests/
```

### Test Performance Impact

```bash
python tests/run_tests.py --suite performance
```

### Test with Parallel Execution

```bash
python tests/run_tests.py --parallel 4
```

## Continuous Integration

The test suite is integrated with GitHub Actions (`.github/workflows/test-pivot-fix.yml`) and runs:

1. **Multi-Python Version Testing** - Tests on Python 3.9, 3.10, 3.11
2. **Build Verification** - Ensures code builds successfully
3. **Functionality Testing** - Runs all test suites
4. **Performance Validation** - Checks for performance regressions
5. **Docker Integration** - Tests Docker build and functionality
6. **Backwards Compatibility** - Ensures existing functionality isn't broken

## Test Development Guidelines

### Adding New Tests

1. **Choose the Right Module**: Add tests to the appropriate test module based on functionality
2. **Use Fixtures**: Leverage the shared fixtures in `conftest.py`
3. **Follow Naming**: Use descriptive test function names starting with `test_`
4. **Add Markers**: Use appropriate pytest markers for categorization
5. **Document Complex Tests**: Add docstrings explaining complex test scenarios

### Test Data Management

- Use the `duckdb_test_file` fixture for consistent test data
- Create specific test tables for complex scenarios
- Clean up test data in test teardown to avoid interference

### Performance Test Guidelines

- Use reasonable iteration counts (balance coverage vs. execution time)
- Set realistic performance expectations based on hardware
- Include both prepared statement and direct execution performance tests
- Test memory usage stability with repeated executions

## Troubleshooting

### Common Issues

1. **Server Binary Not Found**
   ```
   ERROR: Cannot find or run gizmosql_server binary
   ```
   **Solution**: Build GizmoSQL and ensure `gizmosql_server` is in PATH

2. **Connection Refused**
   ```
   Connection refused to localhost:31338
   ```
   **Solution**: Check if another process is using the port, or wait longer for server startup

3. **Import Errors**
   ```
   ModuleNotFoundError: No module named 'adbc_driver_flightsql'
   ```
   **Solution**: Install test requirements: `pip install -r tests/test_requirements.txt`

### Debug Mode

Run tests with verbose output and no capture for debugging:

```bash
pytest -v -s tests/test_pivot_issues.py::TestPivotIssues::test_pivot_github_issue_44_original_query
```

### Test Isolation

If tests interfere with each other, run them in isolation:

```bash
pytest --forked tests/
```

## Coverage Reports

Generate HTML coverage reports:

```bash
python tests/run_tests.py --coverage
# Open htmlcov/index.html in browser
```

## Contributing

When contributing new tests:

1. Ensure tests are deterministic and don't depend on external resources
2. Add appropriate documentation and comments
3. Follow existing test patterns and conventions
4. Test both success and failure scenarios
5. Consider performance implications of new tests

## Test Results Interpretation

### Success Criteria

- ✅ All PIVOT issue tests pass (validates the fix)
- ✅ All regression tests pass (no existing functionality broken)
- ✅ Performance tests complete within reasonable bounds
- ✅ Integration tests demonstrate end-to-end functionality

### Failure Investigation

1. **PIVOT Test Failures**: Indicates the fix may not be working correctly
2. **Regression Test Failures**: Suggests existing functionality was broken
3. **Performance Test Failures**: May indicate performance regression
4. **Integration Test Failures**: Could indicate system-level issues

The test suite provides comprehensive validation that the PIVOT fix resolves GitHub issue #44 while maintaining full backwards compatibility and acceptable performance.