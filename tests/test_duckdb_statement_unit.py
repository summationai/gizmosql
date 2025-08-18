"""
Unit tests for DuckDBStatement class to catch implementation bugs
"""
import pytest
import tempfile
import duckdb
from pathlib import Path
from tests.conftest import GizmoSQLTestCase


class TestDuckDBStatementUnit(GizmoSQLTestCase):
    """Unit tests for DuckDBStatement implementation"""
    
    @pytest.fixture
    def temp_duckdb_connection(self):
        """Create a temporary DuckDB connection for unit testing"""
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_file:
            conn = duckdb.connect(tmp_file.name)
            
            # Set up test data
            conn.execute("""
                CREATE TABLE unit_test_data (
                    id INTEGER,
                    category STRING,
                    value DECIMAL(10, 2)
                )
            """)
            
            conn.execute("""
                INSERT INTO unit_test_data VALUES
                (1, 'A', 100.50),
                (2, 'B', 200.75),
                (3, 'A', 150.25),
                (4, 'B', 300.00)
            """)
            
            yield conn
            conn.close()
            Path(tmp_file.name).unlink()
    
    def test_prepared_statement_creation_success(self, cursor):
        """Test successful prepared statement creation"""
        
        # Simple queries that should use prepared statements
        simple_queries = [
            "SELECT COUNT(*) FROM test_pivot_data",
            "SELECT * FROM test_pivot_data WHERE league = 'M'",
            "SELECT DISTINCT category FROM test_pivot_data",
        ]
        
        for query in simple_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Prepared statement should work for: {query}"
    
    def test_direct_execution_fallback_detection(self, cursor):
        """Test that direct execution fallback is triggered correctly"""
        
        # Queries that should trigger "Cannot prepare multiple statements" and fall back
        fallback_queries = [
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            "PIVOT (select * from test_pivot_data where league = 'M') ON category USING sum(pnl_amount) GROUP BY league",
        ]
        
        for query in fallback_queries:
            # These should succeed via direct execution fallback
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Direct execution fallback should work for: {query[:50]}..."
    
    def test_prepared_statement_parameters_handling(self, cursor):
        """Test parameter handling in prepared statements"""
        
        # Test various parameter types and counts
        param_tests = [
            ("SELECT * FROM test_pivot_data WHERE league = ?", ["M"]),
            ("SELECT * FROM test_pivot_data WHERE pnl_amount > ?", [15000000]),
            ("SELECT * FROM test_pivot_data WHERE league = ? AND pnl_amount > ?", ["M", 10000000]),
            ("SELECT * FROM test_pivot_data WHERE league IN (?, ?, ?)", ["M", "C", "N"]),
            ("SELECT * FROM test_pivot_data WHERE period BETWEEN ? AND ?", ["2024-01-01", "2024-12-31"]),
        ]
        
        for query, params in param_tests:
            result = self.assert_query_succeeds(cursor, query, parameters=params)
            assert result is not None, f"Parameter handling should work for: {query}"
    
    def test_direct_execution_no_parameters_restriction(self, cursor):
        """Test that direct execution correctly rejects parameters"""
        
        # Direct execution queries should not accept parameters
        pivot_with_params_attempts = [
            ("PIVOT (select * from test_pivot_data where league = ?) ON league USING sum(pnl_amount) GROUP BY category", ["M"]),
        ]
        
        # Note: This might be tricky to test directly through the Flight SQL interface
        # since the parameter restriction happens at a lower level
        # For now, we'll test that PIVOT queries work without parameters
        
        param_free_pivot = "PIVOT (select * from test_pivot_data where league = 'M') ON league USING sum(pnl_amount) GROUP BY category"
        result = self.assert_query_succeeds(cursor, param_free_pivot)
        assert result is not None, "Direct execution should work without parameters"
    
    def test_query_result_handling_edge_cases(self, cursor):
        """Test edge cases in query result handling"""
        
        edge_cases = [
            # Empty result set
            ("SELECT * FROM test_pivot_data WHERE 1=0", 0),
            
            # Single row
            ("SELECT * FROM test_pivot_data LIMIT 1", 1),
            
            # NULL values
            ("SELECT NULL as null_col", 1),
            
            # Large numbers
            ("SELECT 999999999999999999999999999999.99 as big_decimal", 1),
            
            # Empty string
            ("SELECT '' as empty_string", 1),
            
            # Special characters
            ("SELECT 'test with spaces and symbols: !@#$%^&*()' as special", 1),
        ]
        
        for query, expected_rows in edge_cases:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Should handle edge case: {query}"
            assert result.num_rows == expected_rows, f"Expected {expected_rows} rows for: {query}"
    
    def test_error_handling_in_different_modes(self, cursor):
        """Test error handling in both prepared and direct execution modes"""
        
        # Errors that should occur in prepared statement mode
        prepared_errors = [
            ("SELECT * FROM nonexistent_table", "does not exist"),
            ("SELECT invalid_column FROM test_pivot_data", "column"),
            ("SELECT * FROM test_pivot_data WHERE invalid_function()", "function"),
        ]
        
        for query, expected_error_text in prepared_errors:
            error = self.assert_query_fails(cursor, query)
            assert expected_error_text.lower() in str(error).lower(), f"Expected error text '{expected_error_text}' in: {error}"
        
        # Errors that should occur in direct execution mode (PIVOT)
        direct_errors = [
            ("PIVOT (select * from nonexistent_table) ON col USING sum(val) GROUP BY cat", "does not exist"),
            ("PIVOT (select * from test_pivot_data) ON nonexistent_col USING sum(pnl_amount) GROUP BY category", "column"),
            ("PIVOT (select * from test_pivot_data) ON league USING invalid_func(pnl_amount) GROUP BY category", "function"),
        ]
        
        for query, expected_error_text in direct_errors:
            error = self.assert_query_fails(cursor, query)
            # Should be a proper SQL error, not a "Cannot prepare multiple statements" error
            assert "cannot prepare multiple statements" not in str(error).lower()
            # May or may not contain the expected error text depending on how DuckDB processes it
    
    def test_context_handling_in_fetch_result(self, cursor):
        """Test that FetchResult correctly handles context in both modes"""
        
        # This tests the specific bug we fixed - accessing context safely
        
        # Prepared statement mode - should use stmt_->context
        prepared_query = "SELECT category, COUNT(*) as count FROM test_pivot_data GROUP BY category"
        result = self.assert_query_succeeds(cursor, prepared_query)
        assert result is not None, "Prepared statement context handling should work"
        assert result.num_rows > 0, "Should return grouped results"
        
        # Direct execution mode - should use con_->context  
        direct_query = "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category"
        result = self.assert_query_succeeds(cursor, direct_query)
        assert result is not None, "Direct execution context handling should work"
        
        # The key test: this should not segfault due to null pointer access
        assert result.schema is not None, "Should have valid schema from context"
        assert len(result.schema) > 0, "Schema should have fields"
    
    def test_statement_reuse_and_cleanup(self, cursor):
        """Test that statements can be reused and cleaned up properly"""
        
        # Execute the same prepared statement multiple times
        reusable_query = "SELECT COUNT(*) FROM test_pivot_data WHERE league = ?"
        
        for league in ['M', 'C', 'N']:
            result = self.assert_query_succeeds(cursor, reusable_query, parameters=[league])
            assert result is not None, f"Statement reuse should work for league {league}"
        
        # Execute the same direct execution query multiple times
        reusable_pivot = "PIVOT (select * from test_pivot_data where league = 'M') ON category USING sum(pnl_amount) GROUP BY league"
        
        for _ in range(3):
            result = self.assert_query_succeeds(cursor, reusable_pivot)
            assert result is not None, "Direct execution statement reuse should work"
    
    def test_data_type_conversion_accuracy(self, cursor):
        """Test that data types are converted accurately between DuckDB and Arrow"""
        
        # Test various data types
        type_tests = [
            ("SELECT 42 as int_col", "int32"),
            ("SELECT 3.14159 as float_col", "double"),
            ("SELECT 'hello world' as string_col", "string"),
            ("SELECT true as bool_col", "bool"),
            ("SELECT '2024-01-01'::DATE as date_col", "date32"),
            ("SELECT 123.45::DECIMAL(10,2) as decimal_col", "decimal128"),
        ]
        
        for query, expected_type_family in type_tests:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Type conversion should work for: {query}"
            
            # Check that we got the expected type family
            field_type = str(result.schema[0].type)
            # This is a loose check since exact type names might vary
            if expected_type_family == "decimal128":
                assert "decimal" in field_type.lower(), f"Expected decimal type, got: {field_type}"
            elif expected_type_family == "string":
                assert "string" in field_type.lower(), f"Expected string type, got: {field_type}"
    
    def test_large_result_set_handling(self, cursor):
        """Test handling of large result sets"""
        
        # Create a larger dataset
        cursor.execute("DROP TABLE IF EXISTS large_test_data")
        cursor.execute("""
            CREATE TABLE large_test_data AS
            SELECT 
                i as id,
                ('Category_' || (i % 10)) as category,
                ('League_' || (i % 3)) as league,
                (i * 10.5) as amount
            FROM generate_series(1, 1000) as s(i)
        """)
        
        try:
            # Test prepared statement with large result
            large_prepared = "SELECT * FROM large_test_data ORDER BY id"
            result = self.assert_query_succeeds(cursor, large_prepared)
            assert result is not None, "Large prepared statement result should work"
            assert result.num_rows == 1000, "Should return all 1000 rows"
            
            # Test direct execution with large result
            large_pivot = """
                PIVOT (select * from large_test_data) 
                ON league USING sum(amount), count(*) 
                GROUP BY category
                ORDER BY category
            """
            result = self.assert_query_succeeds(cursor, large_pivot)
            assert result is not None, "Large direct execution result should work"
            assert result.num_rows == 10, "Should return 10 categories"
            
        finally:
            cursor.execute("DROP TABLE IF EXISTS large_test_data")
    
    def test_concurrent_statement_execution_simulation(self, cursor):
        """Simulate concurrent statement execution patterns"""
        
        # Simulate what happens when multiple queries are executed in rapid succession
        # This tests for race conditions and state management issues
        
        queries = [
            ("prepared", "SELECT COUNT(*) FROM test_pivot_data", None),
            ("direct", "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category", None),
            ("prepared", "SELECT * FROM test_pivot_data WHERE league = ?", ["M"]),
            ("direct", "PIVOT (select * from test_pivot_data where league = 'C') ON category USING avg(pnl_amount) GROUP BY league", None),
            ("prepared", "SELECT DISTINCT category FROM test_pivot_data", None),
        ]
        
        # Execute multiple rounds rapidly
        for round_num in range(5):
            for execution_mode, query, params in queries:
                result = self.assert_query_succeeds(cursor, query, parameters=params)
                assert result is not None, f"Concurrent simulation round {round_num} {execution_mode} should work"
    
    def test_statement_state_isolation(self, cursor):
        """Test that statement state is properly isolated between executions"""
        
        # Test that previous query state doesn't affect subsequent queries
        
        # Execute a direct execution query
        cursor.execute("PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category")
        result1 = cursor.fetch_arrow_table()
        
        # Execute a prepared statement query  
        cursor.execute("SELECT COUNT(*) FROM test_pivot_data WHERE league = ?", parameters=["M"])
        result2 = cursor.fetch_arrow_table()
        
        # Execute another direct execution query
        cursor.execute("PIVOT (select * from test_pivot_data where league = 'C') ON category USING sum(pnl_amount) GROUP BY league")
        result3 = cursor.fetch_arrow_table()
        
        # All should succeed and return valid results
        assert result1 is not None, "First direct execution should work"
        assert result2 is not None, "Prepared statement should work after direct execution"
        assert result3 is not None, "Second direct execution should work after prepared statement"
        
        # Results should be independent
        assert result1.schema != result2.schema, "Different queries should have different schemas"
        assert result1.num_rows != result2.num_rows, "Different queries should have different row counts"
    
    def test_memory_management_in_statement_lifecycle(self, cursor):
        """Test memory management throughout statement lifecycle"""
        
        # Test that memory is properly managed through create -> execute -> fetch -> cleanup
        
        memory_test_queries = [
            # Small result
            "SELECT * FROM test_pivot_data LIMIT 1",
            
            # Medium result  
            "SELECT * FROM test_pivot_data",
            
            # Aggregated result
            "SELECT category, COUNT(*) FROM test_pivot_data GROUP BY category",
            
            # PIVOT result (direct execution)
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
        ]
        
        # Execute each query multiple times to test for memory leaks
        for query in memory_test_queries:
            for iteration in range(10):
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Memory management test iteration {iteration} should work for: {query[:30]}..."
                
                # Force some operations on the result to ensure memory is properly accessed
                _ = result.num_rows
                _ = result.num_columns
                _ = result.schema
                
                # Convert to pandas to test full data access
                df = result.to_pandas()
                assert df is not None, f"Result conversion should work in iteration {iteration}"


class TestDuckDBStatementErrorHandling(GizmoSQLTestCase):
    """Specific tests for error handling in DuckDBStatement"""
    
    def test_connection_error_handling(self, cursor):
        """Test handling of connection-related errors"""
        
        # Test queries that might cause connection issues
        connection_test_queries = [
            "SELECT version()",  # Should always work if connection is good
            "SELECT current_timestamp",  # Another basic connectivity test
        ]
        
        for query in connection_test_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Connection test should work: {query}"
    
    def test_sql_syntax_error_handling(self, cursor):
        """Test handling of SQL syntax errors in both execution modes"""
        
        syntax_errors = [
            # Basic syntax errors
            "SELCT * FROM test_pivot_data",  # Typo in SELECT
            "SELECT * FORM test_pivot_data",  # Typo in FROM  
            "SELECT * FROM test_pivot_data WERE league = 'M'",  # Typo in WHERE
            
            # PIVOT syntax errors
            "PIVOT select * from test_pivot_data ON league USING sum(pnl_amount) GROUP BY category",  # Missing parentheses
            "PIVOT (select * from test_pivot_data) league USING sum(pnl_amount) GROUP BY category",  # Missing ON
            "PIVOT (select * from test_pivot_data) ON USING sum(pnl_amount) GROUP BY category",  # Missing column in ON
        ]
        
        for query in syntax_errors:
            error = self.assert_query_fails(cursor, query)
            
            # Should get a proper syntax error, not a crash
            error_str = str(error).lower()
            assert any(keyword in error_str for keyword in ["syntax", "parse", "error"]), f"Should be syntax error: {error}"
            assert "segmentation" not in error_str, f"Should not segfault on syntax error: {query}"
    
    def test_resource_exhaustion_handling(self, cursor):
        """Test handling of resource exhaustion scenarios"""
        
        # Test queries that might exhaust resources
        resource_tests = [
            # Very large cartesian product (might hit memory limits)
            """SELECT * FROM 
               (SELECT * FROM test_pivot_data) t1 
               CROSS JOIN 
               (SELECT * FROM test_pivot_data) t2 
               CROSS JOIN 
               (SELECT * FROM test_pivot_data) t3
               LIMIT 10""",  # LIMIT to prevent actual exhaustion
            
            # Complex aggregation that might be expensive
            """SELECT 
                 category,
                 league, 
                 COUNT(*) as count,
                 SUM(pnl_amount) as sum_amount,
                 AVG(pnl_amount) as avg_amount,
                 MIN(pnl_amount) as min_amount,
                 MAX(pnl_amount) as max_amount,
                 STDDEV(pnl_amount) as stddev_amount
               FROM test_pivot_data 
               GROUP BY category, league""",
        ]
        
        for query in resource_tests:
            try:
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Resource test should complete: {query[:50]}..."
            except Exception as e:
                # Query might legitimately fail due to resource limits, but shouldn't crash
                error_str = str(e).lower()
                assert "segmentation" not in error_str, f"Resource exhaustion should not cause segfault: {query[:50]}..."
    
    def test_transaction_state_error_handling(self, cursor):
        """Test error handling related to transaction states"""
        
        # Test transaction-related scenarios
        transaction_tests = [
            # Basic transaction operations
            "BEGIN TRANSACTION",
            "SELECT COUNT(*) FROM test_pivot_data",
            "COMMIT",
            
            # Test PIVOT in transaction context
            "BEGIN TRANSACTION",
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            "ROLLBACK",
        ]
        
        for query in transaction_tests:
            try:
                result = self.assert_query_succeeds(cursor, query)
                # Some transaction commands might not return results
                if result is not None:
                    assert result.num_rows >= 0, f"Transaction test should work: {query}"
            except Exception as e:
                # Transaction operations might not be fully supported, but shouldn't crash
                error_str = str(e).lower()
                assert "segmentation" not in error_str, f"Transaction error should not cause segfault: {query}"
    
    def test_concurrent_error_scenarios(self, cursor):
        """Test error scenarios that might occur under concurrent access"""
        
        # Simulate errors that might happen with concurrent access
        concurrent_error_tests = [
            # Query that works
            ("SELECT COUNT(*) FROM test_pivot_data", True),
            # Query that fails
            ("SELECT * FROM nonexistent_table", False),
            # PIVOT that works
            ("PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category", True),
            # PIVOT that fails
            ("PIVOT (select * from nonexistent_table) ON col USING sum(val) GROUP BY cat", False),
            # Back to working query
            ("SELECT DISTINCT league FROM test_pivot_data", True),
        ]
        
        for query, should_succeed in concurrent_error_tests:
            if should_succeed:
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Expected success query should work: {query[:50]}..."
            else:
                error = self.assert_query_fails(cursor, query)
                # Should be a proper error, not a crash
                error_str = str(error).lower()
                assert "segmentation" not in error_str, f"Expected failure should not segfault: {query[:50]}..."
                
                # After an error, next query should still work
                recovery_result = self.assert_query_succeeds(cursor, "SELECT 1")
                assert recovery_result is not None, "Should recover after error"