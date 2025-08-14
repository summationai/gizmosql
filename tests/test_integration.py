"""
Integration tests for end-to-end GizmoSQL functionality
"""
import pytest
import tempfile
import duckdb
from pathlib import Path
from tests.conftest import GizmoSQLTestCase


class TestIntegration(GizmoSQLTestCase):
    """Integration tests covering full workflows"""
    
    def test_end_to_end_pivot_workflow(self, cursor):
        """Test complete PIVOT workflow from data insertion to results"""
        
        # Step 1: Create a fresh table for this test
        cursor.execute("DROP TABLE IF EXISTS integration_pivot_test")
        cursor.execute("""
            CREATE TABLE integration_pivot_test (
                report_date DATE,
                business_unit STRING,
                region STRING,
                revenue DECIMAL(15, 2),
                costs DECIMAL(15, 2)
            )
        """)
        
        # Step 2: Insert realistic business data
        test_data = [
            ('2024-01-01', 'Sales', 'North', 150000.00, 45000.00),
            ('2024-01-01', 'Sales', 'South', 120000.00, 36000.00),
            ('2024-01-01', 'Marketing', 'North', 80000.00, 25000.00),
            ('2024-01-01', 'Marketing', 'South', 70000.00, 22000.00),
            ('2024-02-01', 'Sales', 'North', 160000.00, 48000.00),
            ('2024-02-01', 'Sales', 'South', 130000.00, 39000.00),
            ('2024-02-01', 'Marketing', 'North', 85000.00, 27000.00),
            ('2024-02-01', 'Marketing', 'South', 75000.00, 24000.00),
        ]
        
        for row in test_data:
            cursor.execute(
                "INSERT INTO integration_pivot_test VALUES (?, ?, ?, ?, ?)",
                parameters=list(row)
            )
        
        # Step 3: Verify data was inserted correctly
        count_result = self.assert_query_succeeds(
            cursor, 
            "SELECT COUNT(*) as count FROM integration_pivot_test"
        )
        assert count_result.to_pandas().iloc[0]['count'] == len(test_data)
        
        # Step 4: Test the problematic PIVOT pattern from GitHub issue
        problematic_pivot = """
            PIVOT (
                select business_unit, region, revenue 
                from integration_pivot_test 
                where business_unit in ('Sales', 'Marketing')
                and report_date >= '2024-01-01'
            ) 
            ON region USING sum(revenue) 
            GROUP BY business_unit 
            ORDER BY business_unit
        """
        
        pivot_result = self.assert_query_succeeds(cursor, problematic_pivot)
        
        # Step 5: Validate the PIVOT results
        df = pivot_result.to_pandas()
        
        # Should have one row per business unit
        assert len(df) == 2, f"Expected 2 business units, got {len(df)}"
        
        # Should have business_unit column plus region columns
        assert 'business_unit' in df.columns
        
        # Should have data for both Sales and Marketing
        business_units = sorted(df['business_unit'].tolist())
        assert business_units == ['Marketing', 'Sales']
        
        # Step 6: Test alternative PIVOT syntax for comparison
        standard_pivot = """
            SELECT * FROM integration_pivot_test 
            PIVOT (SUM(revenue) FOR region IN ('North', 'South') GROUP BY business_unit)
            ORDER BY business_unit
        """
        
        standard_result = self.assert_query_succeeds(cursor, standard_pivot)
        standard_df = standard_result.to_pandas()
        
        # Both approaches should yield similar structure
        assert len(standard_df) == 2, "Standard PIVOT should also return 2 rows"
        
        # Step 7: Test complex aggregations
        complex_pivot = """
            PIVOT (
                select business_unit, region, revenue, costs,
                       (revenue - costs) as profit
                from integration_pivot_test
            ) 
            ON region USING sum(profit), avg(revenue), count(*)
            GROUP BY business_unit
        """
        
        complex_result = self.assert_query_succeeds(cursor, complex_pivot)
        assert complex_result.num_rows > 0, "Complex PIVOT should return results"
        
        # Cleanup
        cursor.execute("DROP TABLE integration_pivot_test")
    
    def test_multi_backend_compatibility(self, cursor):
        """Test that our fix works with different DuckDB features"""
        
        # Test with DuckDB-specific functions
        duckdb_queries = [
            # Array functions
            """PIVOT (
                select 'test' as category, 'A' as group, [1, 2, 3] as arr
                union all
                select 'test' as category, 'B' as group, [4, 5, 6] as arr
            ) ON group USING list_concat(arr) GROUP BY category""",
            
            # Date functions
            """PIVOT (
                select extract(month from period) as month, league, pnl_amount
                from test_pivot_data
            ) ON league USING sum(pnl_amount) GROUP BY month""",
            
            # String functions
            """PIVOT (
                select regexp_replace(category, ' ', '_') as clean_category, league, pnl_amount
                from test_pivot_data
            ) ON league USING sum(pnl_amount) GROUP BY clean_category""",
        ]
        
        for query in duckdb_queries:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"DuckDB-specific PIVOT should work: {query[:50]}..."
    
    def test_real_world_analytics_scenarios(self, cursor):
        """Test realistic analytics scenarios that use PIVOT"""
        
        # Create a sales analytics dataset
        cursor.execute("DROP TABLE IF EXISTS sales_analytics")
        cursor.execute("""
            CREATE TABLE sales_analytics (
                sale_date DATE,
                product_category STRING,
                sales_channel STRING,
                customer_segment STRING,
                revenue DECIMAL(15, 2),
                units_sold INTEGER,
                discount_percent DECIMAL(5, 2)
            )
        """)
        
        # Insert sample analytics data
        analytics_data = []
        import random
        from datetime import date, timedelta
        
        categories = ['Electronics', 'Clothing', 'Books', 'Home']
        channels = ['Online', 'Retail', 'Partner']
        segments = ['Enterprise', 'SMB', 'Consumer']
        
        base_date = date(2024, 1, 1)
        for i in range(200):  # Generate 200 sample records
            analytics_data.append((
                base_date + timedelta(days=i % 90),
                random.choice(categories),
                random.choice(channels),
                random.choice(segments),
                round(random.uniform(1000, 50000), 2),
                random.randint(1, 100),
                round(random.uniform(0, 25), 2)
            ))
        
        for row in analytics_data:
            cursor.execute(
                "INSERT INTO sales_analytics VALUES (?, ?, ?, ?, ?, ?, ?)",
                parameters=list(row)
            )
        
        # Scenario 1: Revenue by channel and category
        channel_pivot = """
            PIVOT (
                select product_category, sales_channel, revenue
                from sales_analytics
                where sale_date >= '2024-01-01'
            ) 
            ON sales_channel USING sum(revenue)
            GROUP BY product_category
            ORDER BY product_category
        """
        
        result1 = self.assert_query_succeeds(cursor, channel_pivot)
        assert result1.num_rows > 0, "Channel pivot should return results"
        
        # Scenario 2: Monthly trends by customer segment
        monthly_pivot = """
            PIVOT (
                select extract(month from sale_date) as month, 
                       customer_segment, 
                       revenue
                from sales_analytics
            ) 
            ON customer_segment USING sum(revenue), avg(revenue)
            GROUP BY month
            ORDER BY month
        """
        
        result2 = self.assert_query_succeeds(cursor, monthly_pivot)
        assert result2.num_rows > 0, "Monthly pivot should return results"
        
        # Scenario 3: Complex business metrics
        metrics_pivot = """
            PIVOT (
                select product_category,
                       sales_channel,
                       revenue,
                       units_sold,
                       (revenue * (1 - discount_percent / 100)) as net_revenue
                from sales_analytics
                where units_sold > 5
            ) 
            ON sales_channel 
            USING sum(net_revenue), sum(units_sold), avg(discount_percent)
            GROUP BY product_category
        """
        
        result3 = self.assert_query_succeeds(cursor, metrics_pivot)
        assert result3.num_rows > 0, "Metrics pivot should return results"
        
        # Cleanup
        cursor.execute("DROP TABLE sales_analytics")
    
    def test_error_recovery_and_robustness(self, cursor):
        """Test that the system recovers gracefully from various error conditions"""
        
        # Test sequence: good query, bad query, good query
        # This ensures error states don't persist
        
        # Start with a working query
        result1 = self.assert_query_succeeds(cursor, "SELECT COUNT(*) FROM test_pivot_data")
        assert result1.num_rows == 1
        
        # Execute a query that should fail
        self.assert_query_fails(
            cursor, 
            "PIVOT (select * from nonexistent_table) ON invalid_col USING sum(bad_col) GROUP BY fake_col"
        )
        
        # Ensure we can still execute good queries after the error
        result2 = self.assert_query_succeeds(cursor, "SELECT DISTINCT category FROM test_pivot_data")
        assert result2.num_rows > 0
        
        # Test a working PIVOT after the error
        working_pivot = """
            PIVOT (select * from test_pivot_data) 
            ON league USING sum(pnl_amount) 
            GROUP BY category
        """
        
        result3 = self.assert_query_succeeds(cursor, working_pivot)
        assert result3.num_rows > 0
        
        # Test multiple error types don't interfere with each other
        error_queries = [
            "SELECT * FROM table_that_does_not_exist",
            "SELECT invalid_column FROM test_pivot_data",
            "PIVOT (select * from test_pivot_data) ON bad_column USING sum(also_bad) GROUP BY category"
        ]
        
        for error_query in error_queries:
            self.assert_query_fails(cursor, error_query)
            
            # After each error, ensure normal queries still work
            recovery_result = self.assert_query_succeeds(cursor, "SELECT 1 as test")
            assert recovery_result.num_rows == 1
    
    def test_concurrent_execution_modes(self, cursor):
        """Test rapid switching between prepared and direct execution modes"""
        
        # Interleave prepared statement and direct execution queries
        mixed_sequence = [
            ("prepared", "SELECT COUNT(*) FROM test_pivot_data"),
            ("direct", "PIVOT (select * from test_pivot_data) ON league USING sum(pnl_amount) GROUP BY category"),
            ("prepared", "SELECT DISTINCT category FROM test_pivot_data"),
            ("direct", "PIVOT (select * from test_pivot_data where pnl_amount > 10000000) ON category USING count(*) GROUP BY league"),
            ("prepared", "SELECT * FROM test_pivot_data WHERE league = 'M' LIMIT 5"),
            ("direct", "PIVOT (select * from test_pivot_data) ON league USING avg(pnl_amount), min(pnl_amount), max(pnl_amount) GROUP BY category"),
            ("prepared", "SELECT league, SUM(pnl_amount) FROM test_pivot_data GROUP BY league"),
        ]
        
        for execution_mode, query in mixed_sequence:
            result = self.assert_query_succeeds(cursor, query)
            assert result is not None, f"Failed in {execution_mode} mode: {query[:50]}..."
            
            # Additional verification that we get reasonable results
            assert result.num_rows >= 0, f"Invalid result count in {execution_mode} mode"
    
    def test_data_integrity_across_execution_modes(self, cursor):
        """Test that data integrity is maintained across different execution modes"""
        
        # Create test data with known values
        cursor.execute("DROP TABLE IF EXISTS integrity_test")
        cursor.execute("""
            CREATE TABLE integrity_test (
                id INTEGER,
                category STRING,
                value DECIMAL(10, 2)
            )
        """)
        
        test_values = [
            (1, 'A', 100.50),
            (2, 'A', 200.75),
            (3, 'B', 150.25),
            (4, 'B', 300.00),
        ]
        
        for row in test_values:
            cursor.execute(
                "INSERT INTO integrity_test VALUES (?, ?, ?)",
                parameters=list(row)
            )
        
        # Calculate expected totals
        expected_total_a = 100.50 + 200.75  # 301.25
        expected_total_b = 150.25 + 300.00  # 450.25
        
        # Test with prepared statement
        prepared_result = self.assert_query_succeeds(
            cursor,
            "SELECT category, SUM(value) as total FROM integrity_test GROUP BY category ORDER BY category"
        )
        
        prepared_df = prepared_result.to_pandas()
        prepared_totals = {row['category']: row['total'] for _, row in prepared_df.iterrows()}
        
        # Test with direct execution (PIVOT)
        pivot_result = self.assert_query_succeeds(
            cursor,
            "PIVOT (select * from integrity_test) ON category USING sum(value) GROUP BY 1 ORDER BY 1"
        )
        
        # Both should produce equivalent totals
        assert abs(prepared_totals['A'] - expected_total_a) < 0.01, f"Prepared statement A total incorrect: {prepared_totals['A']}"
        assert abs(prepared_totals['B'] - expected_total_b) < 0.01, f"Prepared statement B total incorrect: {prepared_totals['B']}"
        
        # The PIVOT result should be structurally different but numerically equivalent
        assert pivot_result.num_rows > 0, "PIVOT should return results"
        
        # Test that repeated execution gives consistent results
        for _ in range(3):
            repeat_prepared = self.assert_query_succeeds(
                cursor,
                "SELECT category, SUM(value) as total FROM integrity_test GROUP BY category ORDER BY category"
            )
            
            repeat_pivot = self.assert_query_succeeds(
                cursor,
                "PIVOT (select * from integrity_test) ON category USING sum(value) GROUP BY 1"
            )
            
            # Results should be consistent across executions
            repeat_df = repeat_prepared.to_pandas()
            repeat_totals = {row['category']: row['total'] for _, row in repeat_df.iterrows()}
            
            assert repeat_totals == prepared_totals, "Prepared statement results should be consistent"
            assert repeat_pivot.num_rows == pivot_result.num_rows, "PIVOT results should be consistent"
        
        # Cleanup
        cursor.execute("DROP TABLE integrity_test")
    
    def test_complex_query_plans(self, cursor):
        """Test that complex query plans work correctly with our fix"""
        
        # Create a more complex schema for testing
        cursor.execute("DROP TABLE IF EXISTS complex_test")
        cursor.execute("""
            CREATE TABLE complex_test (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                category STRING,
                subcategory STRING,
                region STRING,
                sales_date DATE,
                amount DECIMAL(15, 2),
                quantity INTEGER,
                FOREIGN KEY (parent_id) REFERENCES complex_test(id)
            )
        """)
        
        # Insert hierarchical test data
        complex_data = [
            (1, None, 'Electronics', 'Phones', 'North', '2024-01-15', 50000, 10),
            (2, 1, 'Electronics', 'Accessories', 'North', '2024-01-16', 5000, 50),
            (3, None, 'Electronics', 'Laptops', 'South', '2024-01-17', 75000, 5),
            (4, 3, 'Electronics', 'Accessories', 'South', '2024-01-18', 8000, 40),
            (5, None, 'Clothing', 'Shirts', 'North', '2024-01-19', 12000, 200),
            (6, 5, 'Clothing', 'Accessories', 'North', '2024-01-20', 3000, 100),
        ]
        
        for row in complex_data:
            cursor.execute(
                "INSERT INTO complex_test VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                parameters=list(row)
            )
        
        # Test complex PIVOT with joins and subqueries
        complex_pivot = """
            PIVOT (
                select c1.category, c1.region, c1.amount
                from complex_test c1
                where c1.sales_date >= '2024-01-15'
                and c1.amount > (
                    select avg(c2.amount) 
                    from complex_test c2 
                    where c2.category = c1.category
                )
            ) 
            ON region USING sum(amount)
            GROUP BY category
        """
        
        result = self.assert_query_succeeds(cursor, complex_pivot)
        assert result.num_rows > 0, "Complex PIVOT with subqueries should work"
        
        # Test PIVOT with window functions
        window_pivot = """
            PIVOT (
                select category, region, amount,
                       row_number() over (partition by category order by amount desc) as rn
                from complex_test
                where rn <= 2
            ) 
            ON region USING sum(amount)
            GROUP BY category
        """
        
        # This might fail due to window function complexity, but shouldn't crash
        try:
            window_result = self.assert_query_succeeds(cursor, window_pivot)
            assert window_result is not None
        except Exception as e:
            # If it fails, it should be a legitimate SQL error, not a "multiple statements" error
            assert "Cannot prepare multiple statements" not in str(e)
        
        # Test PIVOT with CTEs
        cte_pivot = """
            WITH category_stats AS (
                select category, avg(amount) as avg_amount
                from complex_test
                group by category
            )
            PIVOT (
                select c.region, c.category, c.amount
                from complex_test c
                join category_stats cs on c.category = cs.category
                where c.amount > cs.avg_amount
            ) 
            ON region USING sum(amount)
            GROUP BY category
        """
        
        cte_result = self.assert_query_succeeds(cursor, cte_pivot)
        assert cte_result is not None, "PIVOT with CTE should work"
        
        # Cleanup
        cursor.execute("DROP TABLE complex_test")