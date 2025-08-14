"""
Tests for statement execution modes and the DuckDBStatement fallback logic
"""
import pytest
from tests.conftest import GizmoSQLTestCase


class TestStatementExecution(GizmoSQLTestCase):
    """Test cases for statement execution modes and fallback logic"""
    
    def test_simple_prepared_statements(self, cursor):
        """Test that simple queries use prepared statements successfully"""
        
        # These queries should use prepared statements (the normal path)
        simple_queries = [
            "SELECT 1",
            "SELECT COUNT(*) FROM nation",
            "SELECT * FROM nation WHERE n_nationkey = ?",
            "SELECT n_name FROM nation WHERE n_name LIKE ?",
            "INSERT INTO test_pivot_data VALUES ('2024-06-01', 'Test', 'T', 1000)"
        ]
        
        for query in simple_queries:
            if "?" in query:
                # Parameterized query
                if "n_nationkey" in query:
                    result = self.assert_query_succeeds(cursor, query, parameters=[1])
                elif "n_name" in query:
                    result = self.assert_query_succeeds(cursor, query, parameters=['ALGERIA%'])
            else:
                result = self.assert_query_succeeds(cursor, query)
            
            assert result is not None, f"Simple query should succeed: {query}"
    
    def test_multiple_statement_detection(self, cursor):
        """Test detection of queries that require multiple statements"""
        
        # These queries should trigger the "multiple statements" error detection
        # and fall back to direct execution
        multiple_statement_queries = [
            # PIVOT queries that DuckDB rewrites internally
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            
            # Complex subquery PIVOT
            """PIVOT (
                select * from test_pivot_data 
                where league in (select distinct league from test_pivot_data)
            ) ON league USING sum(pnl_amount) GROUP BY category""",
            
            # PIVOT with window functions (if they cause multiple statements)
            """PIVOT (
                select category, league, pnl_amount,
                       row_number() over (partition by category order by pnl_amount) as rn
                from test_pivot_data
            ) ON league USING sum(pnl_amount) GROUP BY category"""
        ]
        
        for query in multiple_statement_queries:
            # These should succeed with our fallback logic
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Multiple statement query should fall back to direct execution: {query[:50]}..."
    
    def test_prepared_statement_parameters(self, cursor):
        """Test that prepared statements handle parameters correctly"""
        
        # Test various parameter types
        param_tests = [
            ("SELECT * FROM nation WHERE n_nationkey = ?", [24], 1),
            ("SELECT * FROM nation WHERE n_nationkey IN (?, ?, ?)", [0, 1, 24], 3),
            ("SELECT * FROM test_pivot_data WHERE pnl_amount > ?", [15000000], None),  # Variable results
            ("SELECT * FROM test_pivot_data WHERE period >= ? AND period <= ?", ['2024-01-01', '2024-12-31'], None),
            ("SELECT * FROM test_pivot_data WHERE category LIKE ?", ['%Revenue%'], None)
        ]
        
        for query, params, expected_rows in param_tests:
            result = self.assert_query_succeeds(cursor, query, parameters=params)
            assert result is not None
            if expected_rows is not None:
                assert result.num_rows == expected_rows, f"Expected {expected_rows} rows for query: {query}"
    
    def test_direct_execution_fallback_preserves_results(self, cursor):
        """Test that direct execution fallback produces correct results"""
        
        # Create a simple PIVOT that we can verify manually
        cursor.execute("DELETE FROM test_pivot_data")  # Clean slate
        
        # Insert known test data
        test_data = [
            ('2024-01-01', 'Category A', 'M', 100),
            ('2024-01-01', 'Category A', 'C', 200),
            ('2024-01-01', 'Category B', 'M', 300),
            ('2024-01-01', 'Category B', 'C', 400)
        ]
        
        for row in test_data:
            cursor.execute(
                "INSERT INTO test_pivot_data VALUES (?, ?, ?, ?)",
                parameters=list(row)
            )
        
        # Test PIVOT that should use direct execution
        pivot_query = """
            PIVOT (select * from test_pivot_data) 
            ON league USING sum(pnl_amount) 
            GROUP BY category
            ORDER BY category
        """
        
        result = self.assert_query_succeeds(cursor, pivot_query)
        
        # Verify the results are correct
        assert result.num_rows == 2, "Should have 2 categories"
        
        # Convert to pandas for easier verification
        df = result.to_pandas()
        
        # Should have category column plus columns for each league
        assert 'category' in df.columns
        assert len(df.columns) >= 2  # category + at least one league column
        
        # Verify the categories
        categories = sorted(df['category'].tolist())
        assert categories == ['Category A', 'Category B']
    
    def test_error_handling_in_direct_execution(self, cursor):
        """Test that errors in direct execution mode are properly reported"""
        
        # Query that should fail in direct execution mode
        invalid_pivot = """
            PIVOT (select * from nonexistent_table) 
            ON some_column USING sum(some_value) 
            GROUP BY other_column
        """
        
        # Should fail with appropriate error (not "Cannot prepare multiple statements")
        error = self.assert_query_fails(cursor, invalid_pivot)
        
        # The error should be about the missing table, not about multiple statements
        assert "nonexistent_table" in str(error) or "does not exist" in str(error).lower()
        assert "Cannot prepare multiple statements" not in str(error)
    
    def test_statement_execution_mode_consistency(self, cursor):
        """Test that the same query produces consistent results across execution modes"""
        
        # Use a query that should work in both modes for comparison
        base_query = "SELECT category, league, sum(pnl_amount) as total FROM test_pivot_data GROUP BY category, league ORDER BY category, league"
        
        # Execute the base query (should use prepared statements)
        base_result = self.assert_query_succeeds(cursor, base_query)
        
        # Now execute a PIVOT version that should use direct execution
        pivot_query = """
            PIVOT (select * from test_pivot_data) 
            ON league USING sum(pnl_amount) 
            GROUP BY category
            ORDER BY category
        """
        
        pivot_result = self.assert_query_succeeds(cursor, pivot_query)
        
        # Both should succeed and return data
        assert base_result is not None
        assert pivot_result is not None
        
        # The pivot should have the same or fewer rows (due to pivoting)
        assert pivot_result.num_rows <= base_result.num_rows
    
    def test_concurrent_statement_execution(self, cursor):
        """Test that mixed prepared and direct execution queries work correctly"""
        
        queries = [
            # Should use prepared statements
            ("SELECT COUNT(*) FROM nation", None),
            ("SELECT * FROM nation WHERE n_nationkey = ?", [1]),
            
            # Should use direct execution
            ("PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category", None),
            
            # Back to prepared statements
            ("SELECT DISTINCT category FROM test_pivot_data", None),
            ("SELECT * FROM test_pivot_data WHERE pnl_amount > ?", [10000000]),
            
            # Another direct execution
            ("PIVOT (select * from test_pivot_data where league = 'M') ON category USING avg(pnl_amount) GROUP BY league", None)
        ]
        
        # Execute all queries in sequence
        for query, params in queries:
            result = self.assert_query_succeeds(cursor, query, parameters=params)
            assert result is not None, f"Query should succeed: {query[:50]}..."
    
    def test_performance_impact_measurement(self, cursor):
        """Basic performance test to ensure fallback doesn't significantly impact simple queries"""
        
        import time
        
        # Test prepared statement performance (should be fast)
        simple_query = "SELECT COUNT(*) FROM nation"
        
        start_time = time.time()
        for _ in range(10):
            result = self.assert_query_succeeds(cursor, simple_query)
        prepared_time = time.time() - start_time
        
        # Test a query that uses direct execution
        pivot_query = "PIVOT (select * from test_pivot_data) ON league USING count(*) GROUP BY category"
        
        start_time = time.time()
        for _ in range(5):  # Fewer iterations since these might be slower
            result = self.assert_query_succeeds(cursor, pivot_query)
        direct_time = time.time() - start_time
        
        # Both should complete in reasonable time (this is a basic sanity check)
        assert prepared_time < 10.0, f"Prepared statements took too long: {prepared_time}s"
        assert direct_time < 30.0, f"Direct execution took too long: {direct_time}s"
        
        print(f"Performance - Prepared: {prepared_time:.3f}s, Direct: {direct_time:.3f}s")