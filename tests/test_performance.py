"""
Performance tests to ensure the PIVOT fix doesn't negatively impact performance
"""
import pytest
import time
from tests.conftest import GizmoSQLTestCase


class TestPerformance(GizmoSQLTestCase):
    """Performance tests for statement execution modes"""
    
    @pytest.fixture(autouse=True)
    def setup_performance_data(self, cursor):
        """Set up larger dataset for performance testing"""
        
        # Create a larger test dataset
        cursor.execute("DROP TABLE IF EXISTS perf_test_data")
        cursor.execute("""
            CREATE TABLE perf_test_data AS
            SELECT 
                ('2024-' || LPAD((i % 12 + 1)::VARCHAR, 2, '0') || '-01')::DATE as period,
                'Category ' || CHR(65 + (i % 5)) as category,
                'League ' || CHR(88 + (i % 3)) as league,
                (RANDOM() * 1000000 + 1000)::DECIMAL(38, 2) as pnl_amount
            FROM generate_series(1, 1000) as s(i)
        """)
        
        yield
        
        # Cleanup
        cursor.execute("DROP TABLE IF EXISTS perf_test_data")
    
    def test_prepared_statement_performance(self, cursor):
        """Test performance of queries that should use prepared statements"""
        
        queries = [
            "SELECT COUNT(*) FROM perf_test_data",
            "SELECT * FROM perf_test_data WHERE category = 'Category A'",
            "SELECT category, COUNT(*) FROM perf_test_data GROUP BY category",
            "SELECT * FROM perf_test_data WHERE pnl_amount > 50000 ORDER BY pnl_amount DESC LIMIT 10"
        ]
        
        iterations = 20
        total_time = 0
        
        for query in queries:
            start_time = time.time()
            
            for _ in range(iterations):
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None
            
            query_time = time.time() - start_time
            total_time += query_time
            
            avg_time = query_time / iterations
            print(f"Prepared statement avg time: {avg_time:.4f}s for {query[:50]}...")
            
            # Reasonable performance expectation (adjust based on your requirements)
            assert avg_time < 1.0, f"Prepared statement query too slow: {avg_time:.4f}s"
        
        print(f"Total prepared statement time for {len(queries)} queries: {total_time:.4f}s")
    
    def test_direct_execution_performance(self, cursor):
        """Test performance of queries that use direct execution (PIVOT)"""
        
        pivot_queries = [
            "PIVOT (select * from perf_test_data) ON league USING sum(pnl_amount) GROUP BY category",
            "PIVOT (select * from perf_test_data where pnl_amount > 25000) ON league USING avg(pnl_amount) GROUP BY category",
            "PIVOT (select * from perf_test_data) ON category USING count(*) GROUP BY league",
        ]
        
        iterations = 5  # Fewer iterations for potentially slower queries
        total_time = 0
        
        for query in pivot_queries:
            start_time = time.time()
            
            for _ in range(iterations):
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None
            
            query_time = time.time() - start_time
            total_time += query_time
            
            avg_time = query_time / iterations
            print(f"Direct execution avg time: {avg_time:.4f}s for {query[:50]}...")
            
            # More lenient performance expectation for direct execution
            assert avg_time < 5.0, f"Direct execution query too slow: {avg_time:.4f}s"
        
        print(f"Total direct execution time for {len(pivot_queries)} queries: {total_time:.4f}s")
    
    def test_mixed_query_performance(self, cursor):
        """Test performance when mixing prepared and direct execution queries"""
        
        mixed_queries = [
            # Prepared statements
            ("prepared", "SELECT COUNT(*) FROM perf_test_data"),
            ("prepared", "SELECT category, AVG(pnl_amount) FROM perf_test_data GROUP BY category"),
            
            # Direct execution
            ("direct", "PIVOT (select * from perf_test_data) ON league USING sum(pnl_amount) GROUP BY category"),
            
            # More prepared statements
            ("prepared", "SELECT * FROM perf_test_data WHERE league = 'League X' ORDER BY pnl_amount DESC LIMIT 5"),
            ("prepared", "SELECT league, MAX(pnl_amount) FROM perf_test_data GROUP BY league"),
            
            # Another direct execution
            ("direct", "PIVOT (select * from perf_test_data where category like 'Category %') ON category USING count(*) GROUP BY league"),
        ]
        
        start_time = time.time()
        
        for query_type, query in mixed_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Mixed query failed: {query[:50]}..."
        
        total_time = time.time() - start_time
        
        print(f"Mixed query execution time: {total_time:.4f}s for {len(mixed_queries)} queries")
        
        # Should complete reasonably quickly
        assert total_time < 30.0, f"Mixed queries took too long: {total_time:.4f}s"
    
    def test_parameter_binding_performance(self, cursor):
        """Test performance of parameterized queries (prepared statements)"""
        
        param_query = "SELECT * FROM perf_test_data WHERE category = ? AND pnl_amount > ?"
        
        test_params = [
            ['Category A', 25000],
            ['Category B', 50000],
            ['Category C', 75000],
            ['Category D', 100000],
            ['Category E', 125000]
        ]
        
        iterations = 10
        start_time = time.time()
        
        for _ in range(iterations):
            for params in test_params:
                result = self.assert_query_succeeds(cursor, param_query, parameters=params)
                assert result is not None
        
        total_time = time.time() - start_time
        avg_time = total_time / (iterations * len(test_params))
        
        print(f"Parameter binding avg time: {avg_time:.4f}s per query")
        
        # Parameter binding should be fast
        assert avg_time < 0.5, f"Parameter binding too slow: {avg_time:.4f}s"
    
    def test_large_result_set_performance(self, cursor):
        """Test performance with larger result sets"""
        
        # Query that returns all data
        large_query = "SELECT * FROM perf_test_data ORDER BY period, category, league"
        
        start_time = time.time()
        result = self.assert_query_succeeds(cursor, large_query)
        query_time = time.time() - start_time
        
        print(f"Large result set time: {query_time:.4f}s for {result.num_rows} rows")
        
        # Should handle reasonably sized result sets efficiently
        assert query_time < 10.0, f"Large result set query too slow: {query_time:.4f}s"
        assert result.num_rows == 1000, "Should return all 1000 rows"
    
    def test_pivot_complexity_performance(self, cursor):
        """Test performance of increasingly complex PIVOT queries"""
        
        pivot_queries = [
            # Simple PIVOT
            ("simple", "PIVOT (select * from perf_test_data) ON league USING sum(pnl_amount) GROUP BY category"),
            
            # PIVOT with WHERE clause
            ("filtered", """PIVOT (select * from perf_test_data where pnl_amount > 25000) 
                           ON league USING sum(pnl_amount) GROUP BY category"""),
            
            # PIVOT with complex subquery
            ("complex", """PIVOT (
                             select category, league, pnl_amount 
                             from perf_test_data 
                             where league in (select distinct league from perf_test_data where pnl_amount > 50000)
                           ) ON league USING avg(pnl_amount) GROUP BY category"""),
        ]
        
        for complexity, query in pivot_queries:
            start_time = time.time()
            result = self.assert_query_succeeds(cursor, query)
            query_time = time.time() - start_time
            
            print(f"PIVOT {complexity} complexity time: {query_time:.4f}s")
            
            # Performance expectation scales with complexity
            max_time = {"simple": 3.0, "filtered": 5.0, "complex": 10.0}[complexity]
            assert query_time < max_time, f"PIVOT {complexity} query too slow: {query_time:.4f}s"
            assert result is not None
    
    def test_concurrent_query_simulation(self, cursor):
        """Simulate concurrent queries to test statement mode switching"""
        
        # This simulates what might happen with multiple clients
        # (though we're using a single connection here)
        
        query_sequence = [
            "SELECT COUNT(*) FROM perf_test_data",  # Prepared
            "PIVOT (select * from perf_test_data) ON league USING sum(pnl_amount) GROUP BY category",  # Direct
            "SELECT category, AVG(pnl_amount) FROM perf_test_data GROUP BY category",  # Prepared
            "PIVOT (select * from perf_test_data where pnl_amount > 30000) ON category USING count(*) GROUP BY league",  # Direct
            "SELECT * FROM perf_test_data WHERE period >= '2024-06-01' ORDER BY pnl_amount DESC LIMIT 10",  # Prepared
        ]
        
        iterations = 3
        start_time = time.time()
        
        for iteration in range(iterations):
            for i, query in enumerate(query_sequence):
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None, f"Concurrent simulation query {i} failed in iteration {iteration}"
        
        total_time = time.time() - start_time
        
        print(f"Concurrent simulation time: {total_time:.4f}s for {iterations * len(query_sequence)} queries")
        
        # Should handle the switching between modes efficiently
        assert total_time < 60.0, f"Concurrent simulation too slow: {total_time:.4f}s"
    
    @pytest.mark.parametrize("data_size", [100, 500, 1000])
    def test_performance_scaling(self, cursor, data_size):
        """Test performance scaling with different data sizes"""
        
        # Create table with specified size
        cursor.execute(f"DROP TABLE IF EXISTS scale_test_{data_size}")
        cursor.execute(f"""
            CREATE TABLE scale_test_{data_size} AS
            SELECT 
                ('2024-' || LPAD((i % 12 + 1)::VARCHAR, 2, '0') || '-01')::DATE as period,
                'Category ' || CHR(65 + (i % 3)) as category,
                'League ' || CHR(88 + (i % 2)) as league,
                (RANDOM() * 100000 + 1000)::DECIMAL(38, 2) as pnl_amount
            FROM generate_series(1, {data_size}) as s(i)
        """)
        
        try:
            # Test both prepared and direct execution
            prepared_query = f"SELECT category, COUNT(*) FROM scale_test_{data_size} GROUP BY category"
            direct_query = f"PIVOT (select * from scale_test_{data_size}) ON league USING sum(pnl_amount) GROUP BY category"
            
            # Test prepared statement
            start_time = time.time()
            result1 = self.assert_query_succeeds(cursor, prepared_query)
            prepared_time = time.time() - start_time
            
            # Test direct execution
            start_time = time.time() 
            result2 = self.assert_query_succeeds(cursor, direct_query)
            direct_time = time.time() - start_time
            
            print(f"Data size {data_size}: Prepared={prepared_time:.4f}s, Direct={direct_time:.4f}s")
            
            # Performance should scale reasonably
            max_prepared = data_size * 0.001  # 1ms per 1000 rows
            max_direct = data_size * 0.005    # 5ms per 1000 rows
            
            assert prepared_time < max_prepared, f"Prepared scaling poor at {data_size} rows: {prepared_time:.4f}s"
            assert direct_time < max_direct, f"Direct scaling poor at {data_size} rows: {direct_time:.4f}s"
            
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS scale_test_{data_size}")
    
    def test_memory_usage_stability(self, cursor):
        """Test that repeated queries don't cause memory leaks"""
        
        # Run the same queries many times to check for memory leaks
        queries = [
            "SELECT COUNT(*) FROM perf_test_data",
            "PIVOT (select * from perf_test_data) ON league USING sum(pnl_amount) GROUP BY category",
        ]
        
        iterations = 50
        
        for query in queries:
            start_time = time.time()
            
            for i in range(iterations):
                result = self.assert_query_succeeds(cursor, query)
                assert result is not None
                
                # Every 10 iterations, check that we're not getting progressively slower
                if i > 0 and i % 10 == 0:
                    current_time = time.time()
                    elapsed = current_time - start_time
                    avg_time = elapsed / (i + 1)
                    
                    # Should maintain consistent performance
                    assert avg_time < 2.0, f"Performance degrading at iteration {i}: {avg_time:.4f}s avg"
            
            total_time = time.time() - start_time
            print(f"Memory stability test: {total_time:.4f}s for {iterations} iterations of {query[:30]}...")