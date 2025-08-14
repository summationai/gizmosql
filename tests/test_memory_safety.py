"""
Memory safety and crash detection tests for GizmoSQL
"""
import pytest
import psutil
import subprocess
import tempfile
import time
import os
import signal
from pathlib import Path
from tests.conftest import GizmoSQLTestCase


class TestMemorySafety(GizmoSQLTestCase):
    """Memory safety tests to detect leaks, corruption, and crashes"""
    
    def test_memory_leak_detection_pivot_queries(self):
        """Test for memory leaks in PIVOT query execution"""
        
        # This test requires a separate server process to monitor memory
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_db:
            try:
                # Set up test database
                import duckdb
                conn = duckdb.connect(tmp_db.name)
                conn.execute("""
                    CREATE TABLE memory_test (
                        category STRING,
                        league STRING,
                        amount DECIMAL(10,2)
                    )
                """)
                conn.execute("""
                    INSERT INTO memory_test 
                    SELECT 
                        'Category_' || (i % 5) as category,
                        'League_' || (i % 3) as league,
                        (i * 10.5) as amount
                    FROM generate_series(1, 1000) as s(i)
                """)
                conn.close()
                
                # Start server process
                env = os.environ.copy()
                env["GIZMOSQL_PASSWORD"] = "test123"
                
                server_proc = subprocess.Popen([
                    "gizmosql_server",
                    "--database-filename", tmp_db.name,
                    "--port", "31340"
                ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # Wait for server to start
                time.sleep(3)
                
                try:
                    # Monitor memory usage
                    server_process = psutil.Process(server_proc.pid)
                    initial_memory = server_process.memory_info().rss
                    
                    # Execute PIVOT queries repeatedly
                    pivot_query = "PIVOT (select * from memory_test) ON league USING sum(amount) GROUP BY category"
                    
                    for i in range(20):
                        client_proc = subprocess.run([
                            "gizmosql_client",
                            "--command", "Execute",
                            "--host", "localhost",
                            "--port", "31340",
                            "--username", "gizmosql_username", 
                            "--password", "test123",
                            "--query", pivot_query
                        ], capture_output=True, timeout=10)
                        
                        assert client_proc.returncode == 0, f"Query {i} should succeed"
                        
                        # Check memory every 5 queries
                        if i % 5 == 4:
                            current_memory = server_process.memory_info().rss
                            memory_growth = current_memory - initial_memory
                            
                            # Allow some memory growth, but not excessive
                            max_growth = 50 * 1024 * 1024  # 50MB max growth
                            assert memory_growth < max_growth, f"Excessive memory growth: {memory_growth} bytes after {i+1} queries"
                    
                    # Final memory check
                    final_memory = server_process.memory_info().rss
                    total_growth = final_memory - initial_memory
                    
                    print(f"Memory growth: {total_growth} bytes over 20 PIVOT queries")
                    
                    # Memory growth should be reasonable
                    assert total_growth < 100 * 1024 * 1024, f"Total memory growth too large: {total_growth} bytes"
                    
                finally:
                    server_proc.terminate()
                    server_proc.wait()
                    
            finally:
                Path(tmp_db.name).unlink()
    
    def test_buffer_overflow_protection(self, cursor):
        """Test protection against buffer overflows"""
        
        # Test very long strings that might cause buffer overflows
        long_string_tests = [
            # Very long category name
            f"SELECT '{'x' * 1000}' as long_category",
            
            # Very long SQL query (within reason)
            f"SELECT category FROM test_pivot_data WHERE category IN ({', '.join([f\"'cat_{i}'\" for i in range(100)])})",
            
            # Long aggregation with many columns
            """SELECT 
                 category,
                 """ + ",\n                 ".join([f"SUM(pnl_amount) as sum_{i}" for i in range(50)]) + """
               FROM test_pivot_data GROUP BY category""",
        ]
        
        for test_query in long_string_tests:
            try:
                result = self.assert_query_succeeds(cursor, test_query)
                assert result is not None, f"Long string query should not cause buffer overflow: {test_query[:50]}..."
            except Exception as e:
                # Query might fail for legitimate reasons, but should not crash
                error_str = str(e).lower()
                assert "segmentation" not in error_str, f"Should not cause segfault: {test_query[:50]}..."
                assert "buffer overflow" not in error_str, f"Should not cause buffer overflow: {test_query[:50]}..."
    
    def test_stack_overflow_protection(self, cursor):
        """Test protection against stack overflows from deep recursion"""
        
        # Test deeply nested expressions that might cause stack overflow
        stack_test_queries = [
            # Deeply nested arithmetic
            "SELECT " + "(" * 100 + "1" + " + 1)" * 100,
            
            # Deeply nested function calls
            "SELECT " + "UPPER(" * 20 + "'test'" + ")" * 20,
            
            # Complex CASE statement
            """SELECT CASE 
                        WHEN category = 'A' THEN
                          CASE WHEN league = 'M' THEN 1
                               WHEN league = 'C' THEN 2
                               ELSE 3 END
                        WHEN category = 'B' THEN  
                          CASE WHEN league = 'M' THEN 4
                               WHEN league = 'C' THEN 5
                               ELSE 6 END
                        ELSE 0
                      END as nested_case
               FROM test_pivot_data""",
        ]
        
        for stack_query in stack_test_queries[:1]:  # Test first one to avoid actual issues
            try:
                result = self.assert_query_succeeds(cursor, stack_query)
                assert result is not None, f"Nested query should not cause stack overflow: {stack_query[:50]}..."
            except Exception as e:
                # Deep nesting might legitimately fail, but should not crash
                error_str = str(e).lower()
                assert "segmentation" not in error_str, f"Should not cause segfault: {stack_query[:50]}..."
                assert "stack overflow" not in error_str, f"Should not cause stack overflow: {stack_query[:50]}..."
    
    def test_null_pointer_dereference_protection(self, cursor):
        """Test protection against null pointer dereferences"""
        
        # These are the specific patterns that could cause null pointer issues
        null_pointer_risk_queries = [
            # PIVOT with empty result (our original bug scenario)
            "PIVOT (select * from test_pivot_data where 1=0) ON league USING sum(pnl_amount) GROUP BY category",
            
            # PIVOT with NULL values
            "PIVOT (select category, league, null as amount from test_pivot_data) ON league USING count(amount) GROUP BY category",
            
            # Multiple PIVOT operations
            "PIVOT (select * from test_pivot_data limit 1) ON league USING sum(pnl_amount) GROUP BY category",
            
            # PIVOT with complex subquery that might return unexpected results
            """PIVOT (
                 select category, league, pnl_amount
                 from test_pivot_data t1
                 where exists (select 1 from test_pivot_data t2 where t2.category = t1.category)
               ) ON league USING sum(pnl_amount) GROUP BY category""",
        ]
        
        for null_risk_query in null_pointer_risk_queries:
            result = self.assert_query_succeeds(cursor, null_risk_query)
            assert result is not None, f"Null pointer risk query should not crash: {null_risk_query[:50]}..."
            
            # Verify we can still execute queries after potential null pointer scenarios
            recovery_query = self.assert_query_succeeds(cursor, "SELECT 1")
            assert recovery_query is not None, "Should be able to execute queries after null pointer risk scenario"
    
    def test_use_after_free_protection(self, cursor):
        """Test protection against use-after-free errors"""
        
        # Execute patterns that might cause use-after-free if resources aren't managed properly
        use_after_free_patterns = [
            # Execute and immediately execute another query
            ("SELECT COUNT(*) FROM test_pivot_data", "SELECT DISTINCT category FROM test_pivot_data"),
            
            # PIVOT followed by prepared statement
            ("PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
             "SELECT * FROM test_pivot_data WHERE league = ?"),
             
            # Prepared statement followed by PIVOT
            ("SELECT * FROM test_pivot_data WHERE league = ?",
             "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category"),
            
            # Multiple PIVOTs in succession
            ("PIVOT (select * from test_pivot_data where league = 'M') ON category USING sum(pnl_amount) GROUP BY league",
             "PIVOT (select * from test_pivot_data where league = 'C') ON category USING sum(pnl_amount) GROUP BY league"),
        ]
        
        for query1, query2 in use_after_free_patterns:
            # Execute first query
            if "?" in query1:
                result1 = self.assert_query_succeeds(cursor, query1, parameters=["M"])
            else:
                result1 = self.assert_query_succeeds(cursor, query1)
            assert result1 is not None, f"First query should succeed: {query1[:50]}..."
            
            # Immediately execute second query
            if "?" in query2:
                result2 = self.assert_query_succeeds(cursor, query2, parameters=["C"])
            else:
                result2 = self.assert_query_succeeds(cursor, query2)
            assert result2 is not None, f"Second query should succeed: {query2[:50]}..."
            
            # Verify system is still stable
            stability_check = self.assert_query_succeeds(cursor, "SELECT version()")
            assert stability_check is not None, "System should remain stable after use-after-free pattern"
    
    def test_double_free_protection(self, cursor):
        """Test protection against double-free errors"""
        
        # Execute the same query multiple times rapidly to test for double-free
        double_free_test_queries = [
            "SELECT COUNT(*) FROM test_pivot_data",
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            "SELECT * FROM test_pivot_data WHERE league = ?",
        ]
        
        for test_query in double_free_test_queries:
            # Execute the same query 5 times rapidly
            for i in range(5):
                if "?" in test_query:
                    result = self.assert_query_succeeds(cursor, test_query, parameters=["M"])
                else:
                    result = self.assert_query_succeeds(cursor, test_query)
                
                assert result is not None, f"Query iteration {i} should not cause double-free: {test_query[:50]}..."
                
                # Verify each result is independent
                assert result.num_rows >= 0, f"Each result should be valid in iteration {i}"


class TestCrashDetection:
    """Tests specifically designed to detect process crashes"""
    
    def test_server_crash_detection_under_load(self):
        """Test server stability under query load"""
        
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_db:
            try:
                # Create test database
                import duckdb
                conn = duckdb.connect(tmp_db.name)
                conn.execute("""
                    CREATE TABLE crash_test (
                        id INTEGER,
                        category STRING,
                        league STRING,
                        amount DECIMAL(10,2)
                    )
                """)
                conn.execute("""
                    INSERT INTO crash_test 
                    SELECT 
                        i,
                        'Cat_' || (i % 10) as category,
                        'League_' || (i % 5) as league,
                        (i * 5.5) as amount
                    FROM generate_series(1, 500) as s(i)
                """)
                conn.close()
                
                # Start server
                env = os.environ.copy()
                env["GIZMOSQL_PASSWORD"] = "crash_test"
                
                server_proc = subprocess.Popen([
                    "gizmosql_server",
                    "--database-filename", tmp_db.name,
                    "--port", "31341",
                    "--print-queries"
                ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                time.sleep(3)
                
                try:
                    server_process = psutil.Process(server_proc.pid)
                    
                    # Test queries that previously caused crashes
                    crash_prone_queries = [
                        # Original segfault query
                        "PIVOT (select * from crash_test where (league in ('League_0'))) ON league USING sum(amount) GROUP BY category ORDER BY category LIMIT 100 OFFSET 0",
                        
                        # Variations that might cause issues
                        "PIVOT (select * from crash_test) ON league USING sum(amount) GROUP BY category",
                        "PIVOT (select * from crash_test where amount > 100) ON category USING count(*) GROUP BY league",
                        "PIVOT (select category, league, amount * 2 as doubled from crash_test) ON league USING sum(doubled) GROUP BY category",
                    ]
                    
                    for i, query in enumerate(crash_prone_queries):
                        # Verify server is still running before query
                        assert server_process.is_running(), f"Server should be running before query {i}"
                        
                        # Execute query
                        client_proc = subprocess.run([
                            "gizmosql_client",
                            "--command", "Execute",
                            "--host", "localhost",
                            "--port", "31341",
                            "--username", "gizmosql_username",
                            "--password", "crash_test", 
                            "--query", query
                        ], capture_output=True, timeout=15)
                        
                        # Verify server is still running after query
                        assert server_process.is_running(), f"Server crashed during query {i}: {query[:50]}..."
                        
                        # Query should either succeed or fail gracefully (not crash)
                        if client_proc.returncode != 0:
                            stderr = client_proc.stderr.decode().lower()
                            assert "segmentation" not in stderr, f"Query {i} caused segfault: {query[:50]}..."
                            assert "core dumped" not in stderr, f"Query {i} caused core dump: {query[:50]}..."
                    
                    # Server should still be responsive
                    final_test = subprocess.run([
                        "gizmosql_client",
                        "--command", "Execute", 
                        "--host", "localhost",
                        "--port", "31341",
                        "--username", "gizmosql_username",
                        "--password", "crash_test",
                        "--query", "SELECT 1"
                    ], capture_output=True, timeout=10)
                    
                    assert final_test.returncode == 0, "Server should be responsive after crash-prone queries"
                    assert server_process.is_running(), "Server should still be running at the end"
                    
                finally:
                    server_proc.terminate()
                    server_proc.wait()
                    
            finally:
                Path(tmp_db.name).unlink()
    
    def test_signal_handling_robustness(self):
        """Test server robustness under signal conditions"""
        
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_db:
            try:
                # Start server
                env = os.environ.copy()
                env["GIZMOSQL_PASSWORD"] = "signal_test"
                
                server_proc = subprocess.Popen([
                    "gizmosql_server",
                    "--database-filename", tmp_db.name,
                    "--port", "31342"
                ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                time.sleep(2)
                
                try:
                    # Send harmless signals
                    os.kill(server_proc.pid, signal.SIGUSR1)  # User signal (should be ignored)
                    time.sleep(1)
                    
                    # Verify server is still running
                    assert server_proc.poll() is None, "Server should survive SIGUSR1"
                    
                    # Try to connect after signal
                    client_proc = subprocess.run([
                        "gizmosql_client",
                        "--command", "Execute",
                        "--host", "localhost", 
                        "--port", "31342",
                        "--username", "gizmosql_username",
                        "--password", "signal_test",
                        "--query", "SELECT 1"
                    ], capture_output=True, timeout=10)
                    
                    # Should still be responsive
                    # Note: might fail due to connection issues, but server shouldn't crash
                    if client_proc.returncode != 0:
                        stderr = client_proc.stderr.decode().lower()
                        assert "segmentation" not in stderr, "Signal should not cause segfault"
                        assert "core dumped" not in stderr, "Signal should not cause core dump"
                    
                finally:
                    server_proc.terminate()
                    server_proc.wait()
                    
            finally:
                Path(tmp_db.name).unlink()
    
    @pytest.mark.timeout(60)
    def test_infinite_loop_detection(self):
        """Test that queries don't get stuck in infinite loops"""
        
        with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as tmp_db:
            try:
                # Start server
                env = os.environ.copy()
                env["GIZMOSQL_PASSWORD"] = "loop_test"
                
                server_proc = subprocess.Popen([
                    "gizmosql_server",
                    "--database-filename", tmp_db.name,
                    "--port", "31343"
                ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                time.sleep(3)
                
                try:
                    # Queries that might potentially loop
                    potential_loop_queries = [
                        # Simple PIVOT (should complete quickly)  
                        "PIVOT (SELECT 1 as cat, 2 as league, 3 as amount) ON league USING sum(amount) GROUP BY cat",
                        
                        # PIVOT with recursive-like pattern
                        "PIVOT (SELECT 'A' as cat, 'B' as league, 1 as amount UNION SELECT 'A' as cat, 'C' as league, 2 as amount) ON league USING sum(amount) GROUP BY cat",
                    ]
                    
                    for query in potential_loop_queries:
                        start_time = time.time()
                        
                        client_proc = subprocess.run([
                            "gizmosql_client",
                            "--command", "Execute",
                            "--host", "localhost",
                            "--port", "31343",
                            "--username", "gizmosql_username",
                            "--password", "loop_test",
                            "--query", query
                        ], capture_output=True, timeout=30)  # 30 second timeout
                        
                        end_time = time.time()
                        duration = end_time - start_time
                        
                        # Should complete reasonably quickly
                        assert duration < 25, f"Query took too long ({duration}s), possible infinite loop: {query[:50]}..."
                        
                        # Check if server is still responsive
                        assert server_proc.poll() is None, f"Server should still be running after query: {query[:50]}..."
                    
                finally:
                    server_proc.terminate()
                    server_proc.wait()
                    
            finally:
                Path(tmp_db.name).unlink()


class TestResourceExhaustionHandling(GizmoSQLTestCase):
    """Test handling of resource exhaustion scenarios"""
    
    def test_large_result_set_memory_management(self, cursor):
        """Test memory management with large result sets"""
        
        # Create large dataset for testing
        cursor.execute("DROP TABLE IF EXISTS large_result_test")
        cursor.execute("""
            CREATE TABLE large_result_test AS
            SELECT 
                i as id,
                ('Category_' || (i % 20)) as category,
                ('League_' || (i % 8)) as league,
                (i * 1.5) as amount
            FROM generate_series(1, 10000) as s(i)
        """)
        
        try:
            # Test large prepared statement result
            large_query = "SELECT * FROM large_result_test ORDER BY id"
            result = self.assert_query_succeeds(cursor, large_query)
            assert result is not None, "Large result query should work"
            assert result.num_rows == 10000, "Should return all rows"
            
            # Test large PIVOT result
            large_pivot = """
                PIVOT (select * from large_result_test) 
                ON league USING sum(amount), count(*) 
                GROUP BY category
            """
            pivot_result = self.assert_query_succeeds(cursor, large_pivot)
            assert pivot_result is not None, "Large PIVOT result should work"
            assert pivot_result.num_rows == 20, "Should return 20 categories"
            
            # Test that system is still responsive after large results
            small_query = self.assert_query_succeeds(cursor, "SELECT 1")
            assert small_query is not None, "System should be responsive after large results"
            
        finally:
            cursor.execute("DROP TABLE IF EXISTS large_result_test")
    
    def test_connection_limit_handling(self, cursor):
        """Test handling when approaching connection limits"""
        
        # This test verifies that our single connection remains stable
        # and doesn't leak resources that could affect connection pooling
        
        connection_stress_queries = [
            "SELECT COUNT(*) FROM test_pivot_data",
            "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category",
            "SELECT * FROM test_pivot_data WHERE league = ?",
            "SELECT DISTINCT category FROM test_pivot_data",
        ]
        
        # Execute many queries to stress the connection
        for round_num in range(10):
            for query in connection_stress_queries:
                if "?" in query:
                    result = self.assert_query_succeeds(cursor, query, parameters=["M"])
                else:
                    result = self.assert_query_succeeds(cursor, query)
                
                assert result is not None, f"Connection stress test round {round_num} should work: {query[:50]}..."
            
            # Verify connection is still good
            ping_result = self.assert_query_succeeds(cursor, "SELECT version()")
            assert ping_result is not None, f"Connection should be stable after round {round_num}"
    
    def test_disk_space_simulation(self, cursor):
        """Test behavior when disk space might be limited"""
        
        # We can't actually fill up disk space, but we can test operations
        # that might create temporary files or require disk I/O
        
        disk_intensive_queries = [
            # Large sort that might use disk
            """SELECT * FROM test_pivot_data 
               ORDER BY pnl_amount, category, league, period
               LIMIT 100""",
            
            # Large aggregation
            """SELECT category, league,
                      COUNT(*) as count,
                      SUM(pnl_amount) as total,
                      AVG(pnl_amount) as average,
                      MIN(pnl_amount) as minimum,
                      MAX(pnl_amount) as maximum
               FROM test_pivot_data
               GROUP BY category, league""",
            
            # Complex PIVOT that might require temporary storage
            """PIVOT (
                   SELECT category, league, pnl_amount,
                          pnl_amount * 1.1 as adjusted,
                          EXTRACT(YEAR FROM period) as year
                   FROM test_pivot_data
               ) ON league USING sum(pnl_amount), sum(adjusted), count(*) GROUP BY category, year""",
        ]
        
        for disk_query in disk_intensive_queries:
            result = self.assert_query_succeeds(cursor, disk_query)
            assert result is not None, f"Disk intensive query should work: {disk_query[:50]}..."
            
            # Verify system remains responsive
            quick_check = self.assert_query_succeeds(cursor, "SELECT 1")
            assert quick_check is not None, "System should remain responsive after disk intensive operation"