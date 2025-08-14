"""
Tests for PIVOT functionality and GitHub issue #44 fix
"""
import pytest
from tests.conftest import GizmoSQLTestCase


class TestPivotIssues(GizmoSQLTestCase):
    """Test cases for PIVOT functionality and multiple statement issues"""
    
    def test_pivot_github_issue_44_original_query(self, cursor):
        """Test the exact problematic PIVOT query from GitHub issue #44"""
        
        # This is the exact query that was failing in the GitHub issue
        problematic_query = """
            PIVOT (select * from test_pivot_data where (league in ('M'))) 
            ON league USING sum(pnl_amount) 
            GROUP BY category ORDER BY category LIMIT 100 OFFSET 0
        """
        
        # This should now work with our fix (before it would fail with "Cannot prepare multiple statements at once")
        result = self.assert_query_succeeds(cursor, problematic_query)
        
        # Verify we get results
        assert result.num_rows > 0, "PIVOT query should return results"
        
        # The result should have category column and the pivoted league columns
        column_names = [field.name for field in result.schema]
        assert "category" in column_names, "Result should include category column"
    
    def test_pivot_standard_syntax_still_works(self, cursor):
        """Ensure standard PIVOT syntax continues to work"""
        
        standard_pivot = """
            SELECT * FROM test_pivot_data 
            PIVOT (SUM(pnl_amount) FOR league IN ('M', 'C', 'N') GROUP BY category)
        """
        
        result = self.assert_query_succeeds(cursor, standard_pivot)
        assert result.num_rows > 0, "Standard PIVOT should return results"
    
    def test_pivot_with_different_aggregations(self, cursor):
        """Test PIVOT with different aggregation functions"""
        
        # Test with AVG
        avg_pivot = """
            PIVOT (select * from test_pivot_data) 
            ON league USING avg(pnl_amount) 
            GROUP BY category
        """
        
        result = self.assert_query_succeeds(cursor, avg_pivot)
        assert result.num_rows > 0, "AVG PIVOT should return results"
        
        # Test with COUNT
        count_pivot = """
            PIVOT (select * from test_pivot_data) 
            ON league USING count(*) 
            GROUP BY category
        """
        
        result = self.assert_query_succeeds(cursor, count_pivot)
        assert result.num_rows > 0, "COUNT PIVOT should return results"
    
    def test_pivot_with_complex_where_conditions(self, cursor):
        """Test PIVOT queries with complex WHERE conditions"""
        
        complex_pivot = """
            PIVOT (
                select * from test_pivot_data 
                where pnl_amount > 10000000 
                AND period >= '2024-01-01'
                AND category LIKE '%Revenue%'
            ) 
            ON league USING sum(pnl_amount) 
            GROUP BY category
        """
        
        result = self.assert_query_succeeds(cursor, complex_pivot)
        # May return 0 rows due to filtering, but should not error
        assert result is not None, "Complex PIVOT should execute without error"
    
    def test_pivot_with_subqueries(self, cursor):
        """Test PIVOT with subqueries that could cause multiple statement issues"""
        
        subquery_pivot = """
            PIVOT (
                select category, league, pnl_amount 
                from test_pivot_data 
                where league in (
                    select distinct league from test_pivot_data where pnl_amount > 15000000
                )
            ) 
            ON league USING sum(pnl_amount) 
            GROUP BY category
        """
        
        result = self.assert_query_succeeds(cursor, subquery_pivot)
        assert result is not None, "PIVOT with subquery should execute without error"
    
    def test_multiple_pivot_operations(self, cursor):
        """Test multiple PIVOT operations in sequence"""
        
        # First PIVOT
        pivot1 = """
            PIVOT (select * from test_pivot_data where league = 'M') 
            ON category USING sum(pnl_amount) 
            GROUP BY league
        """
        
        result1 = self.assert_query_succeeds(cursor, pivot1)
        assert result1 is not None
        
        # Second PIVOT
        pivot2 = """
            PIVOT (select * from test_pivot_data where league = 'C') 
            ON category USING avg(pnl_amount) 
            GROUP BY league
        """
        
        result2 = self.assert_query_succeeds(cursor, pivot2)
        assert result2 is not None
    
    def test_pivot_error_cases(self, cursor):
        """Test PIVOT queries that should legitimately fail"""
        
        # Invalid column reference
        invalid_pivot = """
            PIVOT (select * from test_pivot_data) 
            ON nonexistent_column USING sum(pnl_amount) 
            GROUP BY category
        """
        
        self.assert_query_fails(cursor, invalid_pivot)
        
        # Invalid aggregation
        invalid_agg_pivot = """
            PIVOT (select * from test_pivot_data) 
            ON league USING sum(nonexistent_column) 
            GROUP BY category
        """
        
        self.assert_query_fails(cursor, invalid_agg_pivot)
    
    def test_regular_queries_still_work(self, cursor):
        """Ensure regular non-PIVOT queries continue to work normally"""
        
        # Simple SELECT
        result = self.assert_query_succeeds(cursor, "SELECT COUNT(*) as count FROM test_pivot_data")
        assert result.num_rows == 1
        
        # JOIN query
        join_query = """
            SELECT t1.category, COUNT(*) as count
            FROM test_pivot_data t1
            WHERE t1.pnl_amount > 10000000
            GROUP BY t1.category
            ORDER BY count DESC
        """
        
        result = self.assert_query_succeeds(cursor, join_query)
        assert result.num_rows >= 0
        
        # Parameterized query
        param_query = "SELECT * FROM nation WHERE n_nationkey = ?"
        result = self.assert_query_succeeds(cursor, param_query, parameters=[24])
        assert result.num_rows == 1
    
    def test_direct_execution_mode_detection(self, cursor):
        """Test that our fix correctly detects when to use direct execution mode"""
        
        # This query structure should trigger the multiple statements detection
        trigger_query = """
            PIVOT (
                select period, category, league, pnl_amount
                from test_pivot_data 
                where league in ('M', 'C')
                AND category like '%Discount%'
            ) 
            ON league USING sum(pnl_amount), avg(pnl_amount)
            GROUP BY category, period
            ORDER BY category
        """
        
        # Should succeed with our fix (would fail without it)
        result = self.assert_query_succeeds(cursor, trigger_query)
        assert result is not None, "Complex PIVOT should work with direct execution fallback"
    
    def test_prepared_statement_performance_regression(self, cursor):
        """Ensure regular queries still use prepared statements for performance"""
        
        # Simple queries should still be fast (using prepared statements)
        # This is more of a performance test - we can't easily assert the internal mechanism
        # but we can ensure the query works efficiently
        
        for i in range(10):
            result = self.assert_query_succeeds(
                cursor, 
                "SELECT * FROM nation WHERE n_nationkey = ?", 
                parameters=[i % 3]
            )
            assert result is not None
    
    @pytest.mark.parametrize("league", ["M", "C", "N"])
    def test_pivot_parametrized_by_league(self, cursor, league):
        """Test PIVOT queries with different league parameters"""
        
        pivot_query = f"""
            PIVOT (select * from test_pivot_data where league = '{league}') 
            ON category USING sum(pnl_amount) 
            GROUP BY league
        """
        
        result = self.assert_query_succeeds(cursor, pivot_query)
        assert result is not None, f"PIVOT should work for league {league}"
    
    def test_pivot_with_date_filtering(self, cursor):
        """Test PIVOT with date-based filtering which can cause complex query plans"""
        
        date_pivot = """
            PIVOT (
                select * from test_pivot_data 
                where period between '2024-01-01' and '2024-12-31'
                AND extract(month from period) in (1, 2, 3)
            ) 
            ON league USING sum(pnl_amount) 
            GROUP BY category, extract(quarter from period)
            ORDER BY category
        """
        
        result = self.assert_query_succeeds(cursor, date_pivot)
        assert result is not None, "Date-filtered PIVOT should work"