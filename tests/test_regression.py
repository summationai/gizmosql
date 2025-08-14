"""
Regression tests to ensure existing functionality continues to work
"""
import pytest
from tests.conftest import GizmoSQLTestCase


class TestRegression(GizmoSQLTestCase):
    """Regression tests to prevent breaking existing functionality"""
    
    def test_basic_select_queries(self, cursor):
        """Test basic SELECT queries still work"""
        
        queries = [
            "SELECT 1",
            "SELECT 1 + 1 as result",
            "SELECT 'hello' as greeting",
            "SELECT current_date",
            "SELECT version()"
        ]
        
        for query in queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result.num_rows == 1, f"Query should return 1 row: {query}"
    
    def test_table_operations(self, cursor):
        """Test table operations work correctly"""
        
        # CREATE TABLE
        cursor.execute("CREATE TEMP TABLE test_regression (id INT, name VARCHAR)")
        
        # INSERT
        cursor.execute("INSERT INTO test_regression VALUES (1, 'test1'), (2, 'test2')")
        
        # SELECT
        result = self.assert_query_succeeds(cursor, "SELECT * FROM test_regression ORDER BY id")
        assert result.num_rows == 2
        
        # UPDATE
        cursor.execute("UPDATE test_regression SET name = 'updated' WHERE id = 1")
        
        # Verify update
        result = self.assert_query_succeeds(cursor, "SELECT name FROM test_regression WHERE id = 1")
        df = result.to_pandas()
        assert df.iloc[0]['name'] == 'updated'
        
        # DELETE
        cursor.execute("DELETE FROM test_regression WHERE id = 2")
        
        # Verify delete
        result = self.assert_query_succeeds(cursor, "SELECT COUNT(*) as count FROM test_regression")
        df = result.to_pandas()
        assert df.iloc[0]['count'] == 1
        
        # DROP TABLE
        cursor.execute("DROP TABLE test_regression")
    
    def test_join_operations(self, cursor):
        """Test JOIN operations work correctly"""
        
        # Create test tables
        cursor.execute("""
            CREATE TEMP TABLE customers (
                id INT, 
                name VARCHAR, 
                nation_key INT
            )
        """)
        
        cursor.execute("""
            INSERT INTO customers VALUES 
            (1, 'Alice', 0),
            (2, 'Bob', 1),
            (3, 'Charlie', 24)
        """)
        
        # Test INNER JOIN
        join_query = """
            SELECT c.name, n.n_name 
            FROM customers c 
            INNER JOIN nation n ON c.nation_key = n.n_nationkey
            ORDER BY c.name
        """
        
        result = self.assert_query_succeeds(cursor, join_query)
        assert result.num_rows == 3
        
        df = result.to_pandas()
        assert 'Alice' in df['name'].values
        assert 'ALGERIA' in df['n_name'].values
        
        # Test LEFT JOIN
        left_join_query = """
            SELECT c.name, n.n_name 
            FROM customers c 
            LEFT JOIN nation n ON c.nation_key = n.n_nationkey
            ORDER BY c.name
        """
        
        result = self.assert_query_succeeds(cursor, left_join_query)
        assert result.num_rows == 3
        
        # Cleanup
        cursor.execute("DROP TABLE customers")
    
    def test_aggregate_functions(self, cursor):
        """Test aggregate functions work correctly"""
        
        aggregate_queries = [
            ("SELECT COUNT(*) as count FROM test_pivot_data", "count"),
            ("SELECT SUM(pnl_amount) as total FROM test_pivot_data", "total"),
            ("SELECT AVG(pnl_amount) as average FROM test_pivot_data", "average"),
            ("SELECT MIN(pnl_amount) as minimum FROM test_pivot_data", "minimum"),
            ("SELECT MAX(pnl_amount) as maximum FROM test_pivot_data", "maximum"),
            ("SELECT COUNT(DISTINCT category) as unique_categories FROM test_pivot_data", "unique_categories")
        ]
        
        for query, column_name in aggregate_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result.num_rows == 1, f"Aggregate query should return 1 row: {query}"
            df = result.to_pandas()
            assert column_name in df.columns, f"Result should have {column_name} column"
            # Check that we got a numeric result (not null)
            assert df.iloc[0][column_name] is not None, f"Aggregate result should not be null for {query}"
    
    def test_group_by_operations(self, cursor):
        """Test GROUP BY operations work correctly"""
        
        group_by_queries = [
            "SELECT category, COUNT(*) as count FROM test_pivot_data GROUP BY category",
            "SELECT league, SUM(pnl_amount) as total FROM test_pivot_data GROUP BY league",
            "SELECT category, league, AVG(pnl_amount) as avg_amount FROM test_pivot_data GROUP BY category, league",
            "SELECT extract(year from period) as year, COUNT(*) FROM test_pivot_data GROUP BY extract(year from period)"
        ]
        
        for query in group_by_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result.num_rows > 0, f"GROUP BY query should return results: {query}"
    
    def test_order_by_operations(self, cursor):
        """Test ORDER BY operations work correctly"""
        
        order_queries = [
            "SELECT * FROM test_pivot_data ORDER BY period",
            "SELECT * FROM test_pivot_data ORDER BY pnl_amount DESC",
            "SELECT * FROM test_pivot_data ORDER BY category, league, period",
            "SELECT category, SUM(pnl_amount) as total FROM test_pivot_data GROUP BY category ORDER BY total DESC"
        ]
        
        for query in order_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result.num_rows > 0, f"ORDER BY query should return results: {query}"
    
    def test_where_conditions(self, cursor):
        """Test WHERE conditions work correctly"""
        
        where_queries = [
            "SELECT * FROM test_pivot_data WHERE league = 'M'",
            "SELECT * FROM test_pivot_data WHERE pnl_amount > 15000000",
            "SELECT * FROM test_pivot_data WHERE period >= '2024-01-01'",
            "SELECT * FROM test_pivot_data WHERE category LIKE '%Revenue%'",
            "SELECT * FROM test_pivot_data WHERE league IN ('M', 'C')",
            "SELECT * FROM test_pivot_data WHERE pnl_amount BETWEEN 10000000 AND 20000000"
        ]
        
        for query in where_queries:
            result = self.assert_query_succeeds(cursor, query)
            # Some queries might return 0 rows due to filtering, that's OK
            assert result is not None, f"WHERE query should execute: {query}"
    
    def test_subqueries(self, cursor):
        """Test subqueries work correctly"""
        
        subquery_tests = [
            """SELECT * FROM test_pivot_data 
               WHERE pnl_amount > (SELECT AVG(pnl_amount) FROM test_pivot_data)""",
            
            """SELECT category, 
                      (SELECT COUNT(*) FROM test_pivot_data t2 WHERE t2.category = t1.category) as count
               FROM test_pivot_data t1 
               GROUP BY category""",
            
            """SELECT * FROM test_pivot_data 
               WHERE league IN (SELECT DISTINCT league FROM test_pivot_data WHERE pnl_amount > 15000000)""",
            
            """SELECT category, league, pnl_amount
               FROM test_pivot_data t1
               WHERE pnl_amount = (SELECT MAX(pnl_amount) FROM test_pivot_data t2 WHERE t2.category = t1.category)"""
        ]
        
        for query in subquery_tests:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Subquery should execute: {query[:50]}..."
    
    def test_data_types(self, cursor):
        """Test various data types work correctly"""
        
        # Create table with various data types
        cursor.execute("""
            CREATE TEMP TABLE data_type_test (
                int_col INTEGER,
                float_col DOUBLE,
                string_col VARCHAR,
                date_col DATE,
                bool_col BOOLEAN,
                decimal_col DECIMAL(10,2)
            )
        """)
        
        # Insert test data
        cursor.execute("""
            INSERT INTO data_type_test VALUES 
            (42, 3.14159, 'hello world', '2024-01-01', true, 123.45),
            (-1, -2.71828, 'goodbye', '2023-12-31', false, -67.89),
            (null, null, null, null, null, null)
        """)
        
        # Test selecting different data types
        result = self.assert_query_succeeds(cursor, "SELECT * FROM data_type_test ORDER BY int_col")
        assert result.num_rows == 3
        
        # Test type-specific operations
        type_queries = [
            "SELECT int_col * 2 FROM data_type_test WHERE int_col IS NOT NULL",
            "SELECT ROUND(float_col, 2) FROM data_type_test WHERE float_col IS NOT NULL", 
            "SELECT UPPER(string_col) FROM data_type_test WHERE string_col IS NOT NULL",
            "SELECT extract(year from date_col) FROM data_type_test WHERE date_col IS NOT NULL",
            "SELECT NOT bool_col FROM data_type_test WHERE bool_col IS NOT NULL",
            "SELECT decimal_col + 10.5 FROM data_type_test WHERE decimal_col IS NOT NULL"
        ]
        
        for query in type_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result.num_rows > 0, f"Type-specific query should return results: {query}"
        
        # Cleanup
        cursor.execute("DROP TABLE data_type_test")
    
    def test_window_functions(self, cursor):
        """Test window functions work correctly"""
        
        window_queries = [
            """SELECT category, pnl_amount, 
                      ROW_NUMBER() OVER (PARTITION BY category ORDER BY pnl_amount) as rn
               FROM test_pivot_data""",
            
            """SELECT category, pnl_amount,
                      RANK() OVER (ORDER BY pnl_amount DESC) as rank
               FROM test_pivot_data""",
            
            """SELECT category, pnl_amount,
                      SUM(pnl_amount) OVER (PARTITION BY category) as category_total
               FROM test_pivot_data""",
            
            """SELECT period, pnl_amount,
                      LAG(pnl_amount) OVER (ORDER BY period) as prev_amount
               FROM test_pivot_data"""
        ]
        
        for query in window_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result.num_rows > 0, f"Window function query should return results: {query[:50]}..."
    
    def test_cte_queries(self, cursor):
        """Test Common Table Expressions (CTE) work correctly"""
        
        cte_queries = [
            """WITH category_totals AS (
                   SELECT category, SUM(pnl_amount) as total
                   FROM test_pivot_data 
                   GROUP BY category
               )
               SELECT * FROM category_totals ORDER BY total DESC""",
            
            """WITH ranked_data AS (
                   SELECT *, ROW_NUMBER() OVER (ORDER BY pnl_amount DESC) as rn
                   FROM test_pivot_data
               )
               SELECT * FROM ranked_data WHERE rn <= 3""",
            
            """WITH RECURSIVE date_series AS (
                   SELECT '2024-01-01'::DATE as date
                   UNION ALL
                   SELECT date + INTERVAL '1 month'
                   FROM date_series
                   WHERE date < '2024-06-01'
               )
               SELECT * FROM date_series"""
        ]
        
        for query in cte_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result.num_rows > 0, f"CTE query should return results: {query[:50]}..."
    
    def test_parameterized_queries_still_work(self, cursor):
        """Test that parameterized queries (prepared statements) still work properly"""
        
        param_queries = [
            ("SELECT * FROM nation WHERE n_nationkey = ?", [0]),
            ("SELECT * FROM nation WHERE n_name = ?", ["ALGERIA"]),
            ("SELECT * FROM test_pivot_data WHERE pnl_amount > ?", [15000000]),
            ("SELECT * FROM test_pivot_data WHERE league IN (?, ?)", ["M", "C"]),
            ("SELECT * FROM test_pivot_data WHERE period BETWEEN ? AND ?", ["2024-01-01", "2024-12-31"])
        ]
        
        for query, params in param_queries:
            result = self.assert_query_succeeds(cursor, query, parameters=params)
            assert result is not None, f"Parameterized query should work: {query}"
    
    def test_transaction_behavior(self, cursor):
        """Test basic transaction behavior"""
        
        # Note: ADBC might handle transactions differently, this is a basic test
        
        # Create a test table
        cursor.execute("CREATE TEMP TABLE trans_test (id INT, value VARCHAR)")
        
        # Insert some data
        cursor.execute("INSERT INTO trans_test VALUES (1, 'original')")
        
        # Verify data exists
        result = self.assert_query_succeeds(cursor, "SELECT * FROM trans_test")
        assert result.num_rows == 1
        
        # Update data
        cursor.execute("UPDATE trans_test SET value = 'modified' WHERE id = 1")
        
        # Verify update
        result = self.assert_query_succeeds(cursor, "SELECT value FROM trans_test WHERE id = 1")
        df = result.to_pandas()
        assert df.iloc[0]['value'] == 'modified'
        
        # Cleanup
        cursor.execute("DROP TABLE trans_test")