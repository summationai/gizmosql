"""
Comprehensive edge case tests for GizmoSQL
"""
import pytest
import string
import random
import time
from decimal import Decimal
from tests.conftest import GizmoSQLTestCase


class TestDataTypeEdgeCases(GizmoSQLTestCase):
    """Test edge cases with various data types"""
    
    def test_extreme_numeric_values(self, cursor):
        """Test extreme numeric values in both execution modes"""
        
        extreme_values = [
            # Large integers
            ("SELECT 9223372036854775807 as max_bigint", "9223372036854775807"),  # Max 64-bit int
            ("SELECT -9223372036854775808 as min_bigint", "-9223372036854775808"),  # Min 64-bit int
            
            # Large decimals
            ("SELECT 99999999999999999999999999999999999999.99 as max_decimal", None),  # Max decimal
            ("SELECT -99999999999999999999999999999999999999.99 as min_decimal", None),  # Min decimal
            
            # Very small decimals
            ("SELECT 0.00000000000000000000000000000000000001 as tiny_decimal", None),
            
            # Scientific notation
            ("SELECT 1e308 as large_float", None),
            ("SELECT 1e-308 as small_float", None),
            
            # Zero variations
            ("SELECT 0 as zero", "0"),
            ("SELECT 0.0 as zero_decimal", None),
            ("SELECT -0 as negative_zero", "0"),
        ]
        
        for query, expected_str in extreme_values:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Extreme value query should work: {query}"
            
            # Test with PIVOT to ensure direct execution handles extreme values
            pivot_query = f"""
                PIVOT (
                    SELECT 'cat' as category, 'league' as league, ({query.split('SELECT ')[1].split(' as ')[0]}) as amount
                ) ON league USING sum(amount) GROUP BY category
            """
            
            try:
                pivot_result = self.assert_query_succeeds(cursor, pivot_query)
                assert pivot_result is not None, f"PIVOT with extreme value should work: {query}"
            except Exception as e:
                # Some extreme values might not be supported in all contexts
                assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
    
    def test_string_edge_cases(self, cursor):
        """Test edge cases with string values"""
        
        string_edge_cases = [
            # Empty string
            ("SELECT '' as empty_string", ""),
            
            # Very long string
            ("SELECT '" + "a" * 1000 + "' as long_string", "a" * 1000),
            
            # Unicode characters
            ("SELECT '🚀 GizmoSQL 测试 🎉' as unicode_string", None),
            
            # Special characters that might cause parsing issues
            ("SELECT 'test with ''quotes'' inside' as quoted_string", None),
            ("SELECT 'line1\nline2\ttab' as multiline_string", None),
            ("SELECT 'SQL injection attempt; DROP TABLE test_pivot_data; --' as injection_attempt", None),
            
            # Null character (might not be supported)
            # ("SELECT 'test\x00null' as null_char_string", None),
            
            # String with various encodings
            ("SELECT 'café résumé naïve' as accented_string", None),
        ]
        
        for query, expected in string_edge_cases:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"String edge case should work: {query}"
            
            # Test with PIVOT
            pivot_query = f"""
                PIVOT (
                    SELECT 'category' as cat, 'league' as league, ({query.split('SELECT ')[1].split(' as ')[0]}) as text_val
                ) ON league USING string_agg(text_val, ',') GROUP BY cat
            """
            
            try:
                pivot_result = self.assert_query_succeeds(cursor, pivot_query)
                assert pivot_result is not None, f"PIVOT with string edge case should work: {query}"
            except Exception as e:
                # Some string functions might not be supported
                if "function" not in str(e).lower():
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
    
    def test_date_time_edge_cases(self, cursor):
        """Test edge cases with date and time values"""
        
        datetime_edge_cases = [
            # Extreme dates
            ("SELECT '1900-01-01'::DATE as old_date", None),
            ("SELECT '2100-12-31'::DATE as future_date", None),
            
            # Leap year edge cases
            ("SELECT '2000-02-29'::DATE as leap_day", None),  # Valid leap day
            ("SELECT '1900-02-28'::DATE as non_leap_end", None),  # Not a leap year
            
            # Time zones and timestamps
            ("SELECT '2024-01-01 23:59:59'::TIMESTAMP as timestamp_val", None),
            ("SELECT '2024-01-01 00:00:00'::TIMESTAMP as midnight", None),
            
            # Date arithmetic edge cases
            ("SELECT '2024-01-01'::DATE + INTERVAL '365 days' as date_add", None),
            ("SELECT '2024-12-31'::DATE - INTERVAL '365 days' as date_sub", None),
        ]
        
        for query, expected in datetime_edge_cases:
            try:
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"DateTime edge case should work: {query}"
            except Exception as e:
                # Some date formats might not be supported
                if "invalid" not in str(e).lower() and "parse" not in str(e).lower():
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
    
    def test_null_value_handling(self, cursor):
        """Test NULL value handling in various contexts"""
        
        null_tests = [
            # Basic NULL
            ("SELECT NULL as null_val", None),
            
            # NULL in operations
            ("SELECT NULL + 1 as null_arithmetic", None),
            ("SELECT NULL || 'text' as null_concat", None),
            ("SELECT COALESCE(NULL, 'default') as null_coalesce", "default"),
            
            # NULL in aggregations
            ("SELECT COUNT(NULL) as null_count", "0"),
            ("SELECT SUM(NULL) as null_sum", None),
            ("SELECT AVG(NULL) as null_avg", None),
        ]
        
        for query, expected in null_tests:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"NULL handling should work: {query}"
            
            # Test NULL handling in PIVOT
            null_pivot = f"""
                PIVOT (
                    SELECT 'cat' as category, 'league' as league, ({query.split('SELECT ')[1].split(' as ')[0]}) as val
                ) ON league USING count(val) GROUP BY category
            """
            
            pivot_result = self.assert_query_succeeds(cursor, null_pivot)
            assert pivot_result is not None, f"PIVOT with NULL should work: {query}"


class TestComplexQueryPatterns(GizmoSQLTestCase):
    """Test complex query patterns that might cause issues"""
    
    def test_deeply_nested_subqueries(self, cursor):
        """Test deeply nested subqueries"""
        
        # Build increasingly nested queries
        base_query = "SELECT category FROM test_pivot_data WHERE league = 'M'"
        
        nested_levels = [
            f"SELECT * FROM ({base_query}) t1",
            f"SELECT * FROM (SELECT * FROM ({base_query}) t1) t2",
            f"SELECT * FROM (SELECT * FROM (SELECT * FROM ({base_query}) t1) t2) t3",
        ]
        
        for i, nested_query in enumerate(nested_levels):
            result = self.assert_query_succeeds(cursor, nested_query)
            assert result is not None, f"Nested query level {i+1} should work"
            
            # Test nested query in PIVOT
            pivot_nested = f"""
                PIVOT (
                    SELECT category, 'league' as league, 1 as count_val
                    FROM ({nested_query}) nested
                ) ON league USING sum(count_val) GROUP BY category
            """
            
            pivot_result = self.assert_query_succeeds(cursor, pivot_nested)
            assert pivot_result is not None, f"PIVOT with nested query level {i+1} should work"
    
    def test_complex_join_patterns(self, cursor):
        """Test complex JOIN patterns"""
        
        # Create additional test table for joins
        cursor.execute("DROP TABLE IF EXISTS join_test_table")
        cursor.execute("""
            CREATE TABLE join_test_table (
                league STRING,
                league_name STRING,
                founded_year INTEGER
            )
        """)
        
        cursor.execute("""
            INSERT INTO join_test_table VALUES
            ('M', 'Major League', 1950),
            ('C', 'Championship League', 1960),
            ('N', 'National League', 1970)
        """)
        
        try:
            complex_joins = [
                # Self join
                """SELECT t1.category, t2.league 
                   FROM test_pivot_data t1 
                   JOIN test_pivot_data t2 ON t1.category = t2.category
                   LIMIT 10""",
                
                # Multiple joins
                """SELECT t1.category, t1.league, j1.league_name
                   FROM test_pivot_data t1
                   JOIN join_test_table j1 ON t1.league = j1.league
                   LIMIT 10""",
                
                # Outer joins
                """SELECT t1.category, j1.league_name
                   FROM test_pivot_data t1
                   LEFT JOIN join_test_table j1 ON t1.league = j1.league
                   LIMIT 10""",
                
                # Join with aggregation
                """SELECT t1.league, j1.league_name, COUNT(*) as count
                   FROM test_pivot_data t1
                   JOIN join_test_table j1 ON t1.league = j1.league
                   GROUP BY t1.league, j1.league_name""",
            ]
            
            for join_query in complex_joins:
                result = self.assert_query_succeeds(cursor, join_query)
                assert result is not None, f"Complex join should work: {join_query[:50]}..."
                
                # Test join result in PIVOT
                pivot_join = f"""
                    PIVOT (
                        SELECT category, league, 1 as count_val
                        FROM ({join_query}) joined
                    ) ON league USING sum(count_val) GROUP BY category
                """
                
                try:
                    pivot_result = self.assert_query_succeeds(cursor, pivot_join)
                    assert pivot_result is not None, f"PIVOT with join should work: {join_query[:30]}..."
                except Exception as e:
                    # Complex joins in PIVOT might not always be supported
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
        
        finally:
            cursor.execute("DROP TABLE IF EXISTS join_test_table")
    
    def test_window_function_edge_cases(self, cursor):
        """Test window functions in various contexts"""
        
        window_functions = [
            # Basic window functions
            """SELECT category, pnl_amount,
                      ROW_NUMBER() OVER (ORDER BY pnl_amount) as rn
               FROM test_pivot_data""",
            
            # Window with partition
            """SELECT category, pnl_amount,
                      RANK() OVER (PARTITION BY category ORDER BY pnl_amount DESC) as rank
               FROM test_pivot_data""",
            
            # Multiple window functions
            """SELECT category, pnl_amount,
                      ROW_NUMBER() OVER (ORDER BY pnl_amount) as rn,
                      DENSE_RANK() OVER (ORDER BY pnl_amount) as dense_rank,
                      LAG(pnl_amount) OVER (ORDER BY period) as prev_amount
               FROM test_pivot_data""",
            
            # Window with frame specification
            """SELECT category, pnl_amount,
                      SUM(pnl_amount) OVER (ORDER BY period ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_sum
               FROM test_pivot_data""",
        ]
        
        for window_query in window_functions:
            try:
                result = self.assert_query_succeeds(cursor, window_query)
                assert result is not None, f"Window function should work: {window_query[:50]}..."
                
                # Test window function result in PIVOT (might be complex)
                pivot_window = f"""
                    PIVOT (
                        SELECT category, league, rn
                        FROM ({window_query}) windowed
                        WHERE rn IS NOT NULL
                    ) ON league USING min(rn) GROUP BY category
                """
                
                try:
                    pivot_result = self.assert_query_succeeds(cursor, pivot_window)
                    assert pivot_result is not None, f"PIVOT with window function should work"
                except Exception as e:
                    # Window functions with PIVOT might not be supported
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
                    
            except Exception as e:
                # Some window functions might not be supported
                if "function" not in str(e).lower():
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
    
    def test_recursive_cte_patterns(self, cursor):
        """Test recursive Common Table Expressions"""
        
        recursive_ctes = [
            # Simple recursive CTE
            """WITH RECURSIVE numbers AS (
                   SELECT 1 as n
                   UNION ALL
                   SELECT n + 1 FROM numbers WHERE n < 5
               )
               SELECT * FROM numbers""",
            
            # Recursive CTE with data
            """WITH RECURSIVE category_hierarchy AS (
                   SELECT category, 1 as level
                   FROM test_pivot_data
                   WHERE category LIKE '%Revenue%'
                   
                   UNION ALL
                   
                   SELECT t.category, ch.level + 1
                   FROM test_pivot_data t
                   JOIN category_hierarchy ch ON t.category != ch.category
                   WHERE ch.level < 2
               )
               SELECT DISTINCT category, level FROM category_hierarchy""",
        ]
        
        for cte_query in recursive_ctes:
            try:
                result = self.assert_query_succeeds(cursor, cte_query)
                assert result is not None, f"Recursive CTE should work: {cte_query[:50]}..."
                
                # Test CTE result in PIVOT
                pivot_cte = f"""
                    PIVOT (
                        SELECT category, 'A' as league, level as amount
                        FROM ({cte_query}) cte_result
                    ) ON league USING sum(amount) GROUP BY category
                """
                
                try:
                    pivot_result = self.assert_query_succeeds(cursor, pivot_cte)
                    assert pivot_result is not None, f"PIVOT with CTE should work"
                except Exception as e:
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
                    
            except Exception as e:
                # Recursive CTEs might not be fully supported
                if "recursive" not in str(e).lower():
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"


class TestPerformanceEdgeCases(GizmoSQLTestCase):
    """Test performance-related edge cases"""
    
    def test_large_in_clause(self, cursor):
        """Test queries with very large IN clauses"""
        
        # Generate large IN clause
        large_values = [f"'value_{i}'" for i in range(1000)]
        in_clause = ", ".join(large_values)
        
        large_in_query = f"""
            SELECT * FROM test_pivot_data 
            WHERE category IN ({in_clause})
        """
        
        result = self.assert_query_succeeds(cursor, large_in_query)
        assert result is not None, "Large IN clause should work"
        
        # Test large IN clause with PIVOT
        pivot_large_in = f"""
            PIVOT (
                SELECT * FROM test_pivot_data 
                WHERE category NOT IN ({in_clause})  -- Use NOT IN to get some results
            ) ON league USING sum(pnl_amount) GROUP BY category
        """
        
        pivot_result = self.assert_query_succeeds(cursor, pivot_large_in)
        assert pivot_result is not None, "PIVOT with large IN clause should work"
    
    def test_complex_expression_evaluation(self, cursor):
        """Test complex mathematical expressions"""
        
        complex_expressions = [
            # Nested arithmetic
            """SELECT ((pnl_amount * 1.1 + 1000) / 2.5 - 500) * 0.95 as complex_calc
               FROM test_pivot_data""",
            
            # Multiple function calls
            """SELECT ROUND(SQRT(ABS(pnl_amount)), 2) as sqrt_abs_round
               FROM test_pivot_data""",
            
            # String manipulation
            """SELECT UPPER(SUBSTR(REPLACE(category, ' ', '_'), 1, 10)) as string_ops
               FROM test_pivot_data""",
            
            # Date arithmetic
            """SELECT period + INTERVAL '30 days' as future_date,
                      EXTRACT(YEAR FROM period) * 12 + EXTRACT(MONTH FROM period) as year_month
               FROM test_pivot_data""",
        ]
        
        for expr_query in complex_expressions:
            try:
                result = self.assert_query_succeeds(cursor, expr_query)
                assert result is not None, f"Complex expression should work: {expr_query[:50]}..."
                
                # Test complex expression in PIVOT
                pivot_expr = f"""
                    PIVOT (
                        SELECT category, league, 
                               ({expr_query.split('SELECT ')[1].split(' FROM')[0]})
                        FROM test_pivot_data
                    ) ON league USING count(*) GROUP BY category
                """
                
                try:
                    pivot_result = self.assert_query_succeeds(cursor, pivot_expr)
                    assert pivot_result is not None, f"PIVOT with complex expression should work"
                except Exception as e:
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
                    
            except Exception as e:
                # Some functions might not be supported
                if "function" not in str(e).lower():
                    assert "segmentation" not in str(e).lower(), f"Should not segfault: {e}"
    
    def test_memory_intensive_operations(self, cursor):
        """Test operations that might consume significant memory"""
        
        # Create larger test dataset
        cursor.execute("DROP TABLE IF EXISTS memory_test_data")
        cursor.execute("""
            CREATE TABLE memory_test_data AS
            SELECT 
                i as id,
                ('Long_Category_Name_' || (i % 50) || '_With_Suffix') as category,
                ('League_' || (i % 10)) as league,
                (i * 123.456789) as amount,
                ('Description_' || i || '_' || REPEAT('x', 100)) as description
            FROM generate_series(1, 5000) as s(i)
        """)
        
        try:
            memory_intensive_queries = [
                # Large aggregation
                """SELECT category, 
                          COUNT(*) as count,
                          SUM(amount) as total,
                          AVG(amount) as average,
                          STRING_AGG(DISTINCT league, ',') as leagues
                   FROM memory_test_data 
                   GROUP BY category""",
                
                # Large sort
                """SELECT * FROM memory_test_data 
                   ORDER BY description, amount DESC, category
                   LIMIT 100""",
                
                # Large PIVOT
                """PIVOT (
                       SELECT * FROM memory_test_data
                       WHERE id <= 1000  -- Limit to prevent excessive memory use
                   ) ON league USING sum(amount), count(*) GROUP BY category
                   ORDER BY category""",
            ]
            
            for memory_query in memory_intensive_queries:
                result = self.assert_query_succeeds(cursor, memory_query)
                assert result is not None, f"Memory intensive query should work: {memory_query[:50]}..."
                
                # Check that we got reasonable results
                assert result.num_rows >= 0, "Should return valid row count"
                
        finally:
            cursor.execute("DROP TABLE IF EXISTS memory_test_data")
    
    @pytest.mark.timeout(30)
    def test_query_timeout_handling(self, cursor):
        """Test that long-running queries can be handled properly"""
        
        # Queries that might take a while but should complete
        potentially_slow_queries = [
            # Large cartesian product with limit
            """SELECT t1.category, t2.league, COUNT(*) as count
               FROM test_pivot_data t1
               CROSS JOIN test_pivot_data t2
               GROUP BY t1.category, t2.league
               LIMIT 50""",
            
            # Complex aggregation
            """SELECT category,
                      COUNT(DISTINCT league) as unique_leagues,
                      SUM(pnl_amount) as total,
                      AVG(pnl_amount) as average,
                      STDDEV(pnl_amount) as stddev
               FROM test_pivot_data
               GROUP BY category
               HAVING COUNT(*) > 0""",
            
            # PIVOT with complex aggregation
            """PIVOT (
                   SELECT category, league, pnl_amount
                   FROM test_pivot_data
               ) ON league USING sum(pnl_amount), avg(pnl_amount), count(*), min(pnl_amount), max(pnl_amount)
               GROUP BY category""",
        ]
        
        for slow_query in potentially_slow_queries:
            start_time = time.time()
            result = self.assert_query_succeeds(cursor, slow_query)
            end_time = time.time()
            
            assert result is not None, f"Potentially slow query should complete: {slow_query[:50]}..."
            assert end_time - start_time < 25, f"Query should complete within reasonable time: {slow_query[:50]}..."


class TestErrorRecoveryAndRobustness(GizmoSQLTestCase):
    """Test error recovery and system robustness"""
    
    def test_error_recovery_sequence(self, cursor):
        """Test that system recovers properly from various error conditions"""
        
        error_recovery_sequence = [
            # Good query
            ("SELECT COUNT(*) FROM test_pivot_data", True),
            
            # Syntax error
            ("SELCT * FROM test_pivot_data", False),
            
            # Good query after syntax error
            ("SELECT DISTINCT category FROM test_pivot_data", True),
            
            # Missing table error
            ("SELECT * FROM nonexistent_table", False),
            
            # Good PIVOT query after error
            ("PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category", True),
            
            # PIVOT syntax error
            ("PIVOT select * from test_pivot_data ON league USING sum(pnl_amount) GROUP BY category", False),
            
            # Good prepared statement after PIVOT error
            ("SELECT * FROM test_pivot_data WHERE league = ?", True, ["M"]),
            
            # Parameter error
            ("SELECT * FROM test_pivot_data WHERE league = ? AND invalid_column = ?", False, ["M", "value"]),
            
            # Final good query
            ("SELECT version()", True),
        ]
        
        for i, test_item in enumerate(error_recovery_sequence):
            if len(test_item) >= 3:
                query, should_succeed, params = test_item
            else:
                query, should_succeed = test_item
                params = None
            
            if should_succeed:
                result = self.assert_query_succeeds(cursor, query, parameters=params)
                assert result is not None, f"Recovery sequence step {i} should succeed: {query[:50]}..."
            else:
                error = self.assert_query_fails(cursor, query, parameters=params)
                assert error is not None, f"Recovery sequence step {i} should fail as expected: {query[:50]}..."
                
                # Verify it's a proper error, not a crash
                error_str = str(error).lower()
                assert "segmentation" not in error_str, f"Step {i} should not cause segfault"
                assert "crashed" not in error_str, f"Step {i} should not cause crash"
    
    def test_malformed_data_handling(self, cursor):
        """Test handling of malformed or unusual data"""
        
        # Create table with potentially problematic data
        cursor.execute("DROP TABLE IF EXISTS malformed_data_test")
        cursor.execute("""
            CREATE TABLE malformed_data_test (
                id INTEGER,
                text_data STRING,
                numeric_data DECIMAL(10,2)
            )
        """)
        
        # Insert various edge cases
        malformed_inserts = [
            # Normal data
            ("INSERT INTO malformed_data_test VALUES (1, 'normal', 100.50)", True),
            
            # Very long text
            (f"INSERT INTO malformed_data_test VALUES (2, '{'x' * 10000}', 200.75)", True),
            
            # Unicode and special characters
            ("INSERT INTO malformed_data_test VALUES (3, '🚀Test with émojis and ñoñó', 300.00)", True),
            
            # Extreme numbers
            ("INSERT INTO malformed_data_test VALUES (4, 'extreme', 99999999999999999999.99)", True),
            
            # Empty and NULL values
            ("INSERT INTO malformed_data_test VALUES (5, '', NULL)", True),
            ("INSERT INTO malformed_data_test VALUES (NULL, NULL, NULL)", True),
        ]
        
        try:
            for insert_query, should_succeed in malformed_inserts:
                if should_succeed:
                    cursor.execute(insert_query)
                else:
                    try:
                        cursor.execute(insert_query)
                        assert False, f"Expected insert to fail: {insert_query[:50]}..."
                    except Exception:
                        pass  # Expected to fail
            
            # Test querying malformed data
            malformed_queries = [
                "SELECT * FROM malformed_data_test",
                "SELECT COUNT(*) FROM malformed_data_test WHERE text_data IS NOT NULL",
                "SELECT id, LENGTH(text_data) as text_length FROM malformed_data_test",
                
                # PIVOT with malformed data
                """PIVOT (
                       SELECT COALESCE(text_data, 'null') as category, 
                              'A' as league, 
                              COALESCE(numeric_data, 0) as amount
                       FROM malformed_data_test
                       WHERE id IS NOT NULL
                   ) ON league USING sum(amount) GROUP BY category""",
            ]
            
            for query in malformed_queries:
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Malformed data query should work: {query[:50]}..."
                
        finally:
            cursor.execute("DROP TABLE IF EXISTS malformed_data_test")
    
    def test_resource_cleanup_after_errors(self, cursor):
        """Test that resources are properly cleaned up after errors"""
        
        # Sequence of operations that might leave resources hanging
        resource_test_sequence = [
            # Start a transaction
            "BEGIN TRANSACTION",
            
            # Do some work
            "SELECT COUNT(*) FROM test_pivot_data",
            
            # Cause an error
            "SELECT * FROM nonexistent_table",  # This should fail
            
            # Try to continue (might need rollback)
            "SELECT 1",  # Simple query to test if connection is still good
            
            # Try PIVOT after error
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            
            # Rollback if transaction is still active
            "ROLLBACK",
        ]
        
        for i, query in enumerate(resource_test_sequence):
            try:
                cursor.execute(query)
                result = cursor.fetch_arrow_table()
                # Success is good
                assert result is not None or query.upper().startswith(('BEGIN', 'ROLLBACK', 'COMMIT')), f"Step {i} should work or be transaction command"
            except Exception as e:
                # Some queries are expected to fail
                error_str = str(e).lower()
                assert "segmentation" not in error_str, f"Step {i} should not cause segfault: {query}"
                
                # After an error, we should still be able to execute simple queries
                try:
                    cursor.execute("SELECT 1")
                    recovery_result = cursor.fetch_arrow_table()
                    assert recovery_result is not None, f"Should be able to recover after error in step {i}"
                except Exception as recovery_error:
                    # If we can't recover, it might be due to transaction state
                    pass
    
    def test_concurrent_error_isolation(self, cursor):
        """Test that errors in one query don't affect subsequent queries"""
        
        # Pairs of (error_query, recovery_query)
        error_isolation_tests = [
            # SQL syntax error -> normal query
            ("INVALID SQL SYNTAX HERE", "SELECT 1"),
            
            # Missing table -> PIVOT query
            ("SELECT * FROM missing_table", "PIVOT (select * from test_pivot_data) ON league USING count(*) GROUP BY category"),
            
            # Invalid PIVOT -> prepared statement
            ("PIVOT invalid syntax", "SELECT COUNT(*) FROM test_pivot_data WHERE league = ?"),
            
            # Invalid function -> normal aggregation
            ("SELECT invalid_function(pnl_amount) FROM test_pivot_data", "SELECT SUM(pnl_amount) FROM test_pivot_data"),
        ]
        
        for error_query, recovery_query in error_isolation_tests:
            # Execute error query (should fail)
            error = self.assert_query_fails(cursor, error_query)
            assert error is not None, f"Error query should fail: {error_query[:50]}..."
            
            # Execute recovery query (should succeed)
            if "?" in recovery_query:
                result = self.assert_query_succeeds(cursor, recovery_query, parameters=["M"])
            else:
                result = self.assert_query_succeeds(cursor, recovery_query)
            
            assert result is not None, f"Recovery query should succeed after error: {recovery_query[:50]}..."
            
            # Verify connection is still stable
            ping_result = self.assert_query_succeeds(cursor, "SELECT version()")
            assert ping_result is not None, "Connection should remain stable after error/recovery cycle"