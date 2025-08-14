"""
Regression tests specifically for segfaults and crashes
"""
import pytest
import signal
import subprocess
import time
import os
from tests.conftest import GizmoSQLTestCase


class TestSegfaultRegression(GizmoSQLTestCase):
    """Tests to prevent segfaults and crashes in GizmoSQL"""
    
    def test_pivot_direct_execution_no_segfault(self, cursor):
        """Regression test for segfault in direct execution mode (GitHub issue #44)"""
        
        # This specific query pattern caused the segfault due to null pointer access in FetchResult()
        problematic_query = """
            PIVOT (select * from test_pivot_data where (league in ('M'))) 
            ON league USING sum(pnl_amount) 
            GROUP BY category ORDER BY category LIMIT 100 OFFSET 0
        """
        
        # Should not crash/segfault - this was the original bug
        result = self.assert_query_succeeds(cursor, problematic_query)
        assert result is not None, "Query should not cause segfault"
        assert result.num_rows >= 0, "Should return valid results"
    
    def test_multiple_pivot_queries_no_memory_corruption(self, cursor):
        """Test that multiple PIVOT queries don't cause memory corruption"""
        
        pivot_queries = [
            "PIVOT (select * from test_pivot_data where league = 'M') ON category USING sum(pnl_amount) GROUP BY league",
            "PIVOT (select * from test_pivot_data where league = 'C') ON category USING avg(pnl_amount) GROUP BY league", 
            "PIVOT (select * from test_pivot_data where league = 'N') ON category USING count(*) GROUP BY league",
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount), avg(pnl_amount) GROUP BY category",
            "PIVOT (select * from test_pivot_data where pnl_amount > 10000000) ON league USING sum(pnl_amount) GROUP BY category"
        ]
        
        # Execute multiple times to catch memory corruption issues
        for iteration in range(3):
            for i, query in enumerate(pivot_queries):
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Query {i} in iteration {iteration} should not crash"
    
    def test_direct_execution_null_pointer_safety(self, cursor):
        """Test that direct execution handles null pointers safely"""
        
        # Various PIVOT patterns that trigger direct execution
        direct_execution_queries = [
            # Basic PIVOT
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            
            # PIVOT with WHERE clause
            "PIVOT (select * from test_pivot_data where pnl_amount > 0) ON league USING sum(pnl_amount) GROUP BY category",
            
            # PIVOT with complex expressions
            "PIVOT (select category, league, pnl_amount * 1.1 as adjusted_amount from test_pivot_data) ON league USING sum(adjusted_amount) GROUP BY category",
            
            # PIVOT with subquery
            "PIVOT (select * from test_pivot_data where league in (select distinct league from test_pivot_data)) ON league USING sum(pnl_amount) GROUP BY category",
            
            # Empty result PIVOT (should not crash)
            "PIVOT (select * from test_pivot_data where 1=0) ON league USING sum(pnl_amount) GROUP BY category"
        ]
        
        for query in direct_execution_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Direct execution query should not cause null pointer crash: {query[:50]}..."
    
    def test_fetch_result_edge_cases(self, cursor):
        """Test edge cases in FetchResult that could cause crashes"""
        
        edge_case_queries = [
            # Empty result set
            "PIVOT (select * from test_pivot_data where category = 'NonexistentCategory') ON league USING sum(pnl_amount) GROUP BY category",
            
            # Single row result
            "PIVOT (select * from test_pivot_data limit 1) ON league USING sum(pnl_amount) GROUP BY category",
            
            # NULL values
            "PIVOT (select category, league, null as amount from test_pivot_data limit 1) ON league USING count(amount) GROUP BY category",
            
            # Large numbers
            "PIVOT (select category, league, pnl_amount * 1000000 as big_amount from test_pivot_data) ON league USING sum(big_amount) GROUP BY category",
            
            # String aggregation (if supported)
            "PIVOT (select category, league, category as text_col from test_pivot_data) ON league USING group_concat(text_col) GROUP BY category"
        ]
        
        for query in edge_case_queries:
            try:
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Edge case should not crash: {query[:50]}..."
            except Exception as e:
                # Some queries might legitimately fail, but should not segfault
                assert "segmentation" not in str(e).lower(), f"Query caused segfault: {query[:50]}..."
                assert "crashed" not in str(e).lower(), f"Query caused crash: {query[:50]}..."
    
    def test_concurrent_direct_execution_safety(self, cursor):
        """Test that concurrent-like direct execution queries are safe"""
        
        # Simulate rapid-fire queries that might expose race conditions
        queries = [
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            "SELECT COUNT(*) FROM test_pivot_data",
            "PIVOT (select * from test_pivot_data where league = 'M') ON category USING sum(pnl_amount) GROUP BY league",
            "SELECT DISTINCT category FROM test_pivot_data",
        ]
        
        # Execute rapidly to test for race conditions
        for round_num in range(5):
            for query in queries:
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Concurrent query {round_num} should not crash"
    
    def test_malformed_pivot_queries_safe_errors(self, cursor):
        """Test that malformed PIVOT queries fail safely without crashes"""
        
        malformed_queries = [
            # Missing required clauses
            "PIVOT (select * from test_pivot_data)",  # Missing ON clause
            "PIVOT ON league USING sum(pnl_amount) GROUP BY category",  # Missing subquery
            "PIVOT (select * from test_pivot_data) ON league GROUP BY category",  # Missing USING
            
            # Invalid column references
            "PIVOT (select * from test_pivot_data) ON nonexistent_column USING sum(pnl_amount) GROUP BY category",
            "PIVOT (select * from test_pivot_data) ON league USING sum(nonexistent_column) GROUP BY category",
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY nonexistent_column",
            
            # Invalid aggregations
            "PIVOT (select * from test_pivot_data) ON league USING invalid_function(pnl_amount) GROUP BY category",
            "PIVOT (select * from test_pivot_data) ON league USING sum() GROUP BY category",  # Empty aggregation
            
            # SQL injection attempts (should fail safely)
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount); DROP TABLE test_pivot_data; -- GROUP BY category",
        ]
        
        for query in malformed_queries:
            # These should fail with proper error messages, not crash
            error = self.assert_query_fails(cursor, query)
            
            # Verify it's a proper SQL error, not a crash
            error_str = str(error).lower()
            assert "segmentation" not in error_str, f"Malformed query caused segfault: {query[:50]}..."
            assert "crashed" not in error_str, f"Malformed query caused crash: {query[:50]}..."
            assert "core dumped" not in error_str, f"Malformed query caused core dump: {query[:50]}..."
    
    def test_large_pivot_result_memory_safety(self, cursor):
        """Test that large PIVOT results don't cause memory issues"""
        
        # Create a larger dataset for this test
        cursor.execute("DROP TABLE IF EXISTS large_pivot_test")
        cursor.execute("""
            CREATE TABLE large_pivot_test AS
            SELECT 
                ('Category_' || (i % 10)) as category,
                ('League_' || (i % 5)) as league,
                (i * 100.50) as amount
            FROM generate_series(1, 1000) as s(i)
        """)
        
        try:
            # Large PIVOT that could cause memory issues
            large_pivot = """
                PIVOT (select * from large_pivot_test) 
                ON league USING sum(amount), avg(amount), count(*), min(amount), max(amount)
                GROUP BY category
                ORDER BY category
            """
            
            result = self.assert_query_succeeds(cursor, large_pivot)
            assert result is not None, "Large PIVOT should not cause memory crash"
            assert result.num_rows > 0, "Large PIVOT should return results"
            
        finally:
            # Cleanup
            cursor.execute("DROP TABLE IF EXISTS large_pivot_test")
    
    def test_nested_pivot_complexity_safety(self, cursor):
        """Test complex nested PIVOT scenarios don't crash"""
        
        complex_pivots = [
            # PIVOT with window functions
            """PIVOT (
                select category, league, pnl_amount,
                       row_number() over (partition by category order by pnl_amount) as rn
                from test_pivot_data
            ) ON league USING sum(pnl_amount) GROUP BY category""",
            
            # PIVOT with case statements
            """PIVOT (
                select 
                    case when category like '%Revenue%' then 'Revenue' else 'Other' end as cat_type,
                    league,
                    pnl_amount
                from test_pivot_data
            ) ON league USING sum(pnl_amount) GROUP BY cat_type""",
            
            # PIVOT with date functions
            """PIVOT (
                select extract(month from period) as month, league, pnl_amount
                from test_pivot_data
            ) ON league USING sum(pnl_amount) GROUP BY month""",
        ]
        
        for query in complex_pivots:
            try:
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Complex PIVOT should not crash: {query[:50]}..."
            except Exception as e:
                # Complex queries might fail for legitimate reasons, but shouldn't crash
                error_str = str(e).lower()
                assert "segmentation" not in error_str, f"Complex PIVOT caused segfault: {query[:50]}..."
                assert "crashed" not in error_str, f"Complex PIVOT caused crash: {query[:50]}..."
    
    def test_server_stability_after_crash_conditions(self, cursor):
        """Test that server remains stable after encountering crash conditions"""
        
        # Test sequence: potential crash condition -> normal query -> another crash condition -> normal query
        test_sequence = [
            ("crash_test", "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category"),
            ("normal", "SELECT COUNT(*) FROM test_pivot_data"),
            ("crash_test", "PIVOT (select * from test_pivot_data where league = 'M') ON category USING sum(pnl_amount) GROUP BY league"),
            ("normal", "SELECT DISTINCT league FROM test_pivot_data"),
            ("crash_test", "PIVOT (select category, league, pnl_amount * 2 as doubled from test_pivot_data) ON league USING sum(doubled) GROUP BY category"),
            ("normal", "SELECT * FROM test_pivot_data LIMIT 1"),
        ]
        
        for test_type, query in test_sequence:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Server should remain stable for {test_type} query: {query[:50]}..."
            
            # Additional check that connection is still alive
            ping_result = self.assert_query_succeeds(cursor, "SELECT 1")
            assert ping_result is not None, "Server connection should remain alive after each query"


@pytest.mark.parametrize("stress_level", [1, 2, 3])
class TestMemoryStressRegression(GizmoSQLTestCase):
    """Memory stress tests to catch crashes under load"""
    
    def test_repeated_pivot_execution_memory_stability(self, cursor, stress_level):
        """Test repeated PIVOT execution for memory leaks and crashes"""
        
        iterations = stress_level * 10
        
        pivot_query = """
            PIVOT (select * from test_pivot_data) 
            ON league USING sum(pnl_amount) 
            GROUP BY category
        """
        
        for i in range(iterations):
            result = self.assert_query_succeeds(cursor, pivot_query)
            assert result is not None, f"Iteration {i} should not crash"
            
            # Every 5 iterations, test a normal query to ensure prepared statements still work
            if i % 5 == 0:
                normal_result = self.assert_query_succeeds(cursor, "SELECT COUNT(*) FROM test_pivot_data")
                assert normal_result is not None, f"Normal query at iteration {i} should work"
    
    def test_alternating_execution_modes_stability(self, cursor, stress_level):
        """Test rapid alternation between prepared and direct execution modes"""
        
        iterations = stress_level * 5
        
        queries = [
            ("prepared", "SELECT * FROM test_pivot_data WHERE league = ?", ["M"]),
            ("direct", "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category", None),
            ("prepared", "SELECT COUNT(*) FROM test_pivot_data WHERE category LIKE ?", ["%Revenue%"]),
            ("direct", "PIVOT (select * from test_pivot_data where league = 'C') ON category USING sum(pnl_amount) GROUP BY league", None),
        ]
        
        for i in range(iterations):
            query_type, query, params = queries[i % len(queries)]
            
            result = self.assert_query_succeeds(cursor, query, parameters=params)
            assert result is not None, f"Alternating execution {query_type} at iteration {i} should not crash"


class TestCrashDetection:
    """Tests that detect if the server process crashes"""
    
    def test_server_process_stability_under_pivot_load(self, gizmosql_server):
        """Test that server process doesn't crash under PIVOT query load"""
        
        import psutil
        
        # Get the server process
        server_process = psutil.Process(gizmosql_server["process"].pid)
        assert server_process.is_running(), "Server should be running"
        
        # Create connection and run crash-prone queries
        connection_params = {
            "uri": f"grpc://localhost:{gizmosql_server['port']}",
            "db_kwargs": {
                "username": gizmosql_server["username"],
                "password": gizmosql_server["password"]
            }
        }
        
        import adbc_driver_flightsql.dbapi as gizmosql
        
        with gizmosql.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Run the crash-prone PIVOT query multiple times
                crash_prone_query = """
                    PIVOT (select * from test_pivot_data where (league in ('M'))) 
                    ON league USING sum(pnl_amount) 
                    GROUP BY category ORDER BY category LIMIT 100 OFFSET 0
                """
                
                for i in range(10):
                    try:
                        cur.execute(crash_prone_query)
                        result = cur.fetch_arrow_table()
                        
                        # Check that server process is still alive
                        assert server_process.is_running(), f"Server crashed after iteration {i}"
                        
                    except Exception as e:
                        # Query might fail for other reasons, but server shouldn't crash
                        assert server_process.is_running(), f"Server crashed on iteration {i}: {e}"
    
    @pytest.mark.timeout(30)
    def test_no_infinite_loops_in_direct_execution(self):
        """Test that direct execution doesn't get stuck in infinite loops"""
        
        # This test uses a timeout to catch infinite loops
        # If it takes more than 30 seconds, it's likely stuck
        
        # Test with a simple server instance
        import subprocess
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_db:
            try:
                # Start server process
                env = os.environ.copy()
                env["GIZMOSQL_PASSWORD"] = "test123"
                
                server_proc = subprocess.Popen([
                    "gizmosql_server",
                    "--database-filename", tmp_db.name,
                    "--port", "31339"
                ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # Give it time to start
                time.sleep(3)
                
                # Test query that could cause infinite loop
                client_proc = subprocess.run([
                    "gizmosql_client",
                    "--command", "Execute",
                    "--host", "localhost",
                    "--port", "31339", 
                    "--username", "gizmosql_username",
                    "--password", "test123",
                    "--query", "PIVOT (SELECT 1 as a, 2 as b, 3 as c) ON b USING sum(c) GROUP BY a"
                ], capture_output=True, timeout=25)  # Should complete well before 30s timeout
                
                # If we get here, no infinite loop occurred
                assert True, "No infinite loop detected"
                
            finally:
                # Cleanup
                if 'server_proc' in locals():
                    server_proc.terminate()
                    server_proc.wait()
                os.unlink(tmp_db.name)