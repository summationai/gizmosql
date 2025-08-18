"""
Pytest configuration and fixtures for GizmoSQL tests
"""
import os
import pytest
import subprocess
import time
import tempfile
import shutil
from pathlib import Path
from typing import Generator, Optional
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions


@pytest.fixture(scope="session")
def gizmosql_password():
    """Password for test GizmoSQL server"""
    return "test_password_123"


@pytest.fixture(scope="session")
def test_data_dir():
    """Directory for test data files"""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir


@pytest.fixture(scope="session")
def duckdb_test_file(test_data_dir):
    """Create a temporary DuckDB file for testing"""
    db_file = test_data_dir / "test.duckdb"
    
    # Create test database with sample data
    import duckdb
    conn = duckdb.connect(str(db_file))
    
    # Create test tables
    conn.execute("""
        CREATE TABLE test_pivot_data (
            period DATE,
            category STRING,
            league STRING,
            pnl_amount DECIMAL(38, 2)
        )
    """)
    
    # Insert test data from GitHub issue #44
    conn.execute("""
        INSERT INTO test_pivot_data VALUES
        ('2024-01-01', 'Other Sales Revenue', 'C', 16304900),
        ('2024-02-01', 'Discount', 'M', 17918200),
        ('2024-03-01', 'Discount', 'C', 18693200),
        ('2024-04-01', 'Other Sales Revenue', 'N', 7374843),
        ('2024-05-01', 'Discount', 'M', 17918200)
    """)
    
    # Create table for general testing
    conn.execute("""
        CREATE TABLE nation AS 
        SELECT * FROM (VALUES 
            (0, 'ALGERIA'),
            (1, 'ARGENTINA'), 
            (24, 'UNITED STATES')
        ) AS t(n_nationkey, n_name)
    """)
    
    conn.close()
    return db_file


@pytest.fixture(scope="session")
def gizmosql_server(gizmosql_password, duckdb_test_file):
    """Start a GizmoSQL server for testing"""
    server_process = None
    try:
        # Start server
        env = os.environ.copy()
        env["GIZMOSQL_PASSWORD"] = gizmosql_password
        
        server_process = subprocess.Popen([
            "gizmosql_server",
            "--database-filename", str(duckdb_test_file),
            "--port", "31338",  # Use different port to avoid conflicts
            "--print-queries"
        ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Wait for server to start
        time.sleep(5)
        
        # Check if server is running
        if server_process.poll() is not None:
            stdout, stderr = server_process.communicate()
            pytest.fail(f"Server failed to start: {stderr.decode()}")
        
        yield {
            "host": "localhost",
            "port": 31338,
            "username": "gizmosql_username", 
            "password": gizmosql_password,
            "process": server_process
        }
        
    finally:
        if server_process:
            server_process.terminate()
            server_process.wait()


@pytest.fixture
def gizmosql_connection(gizmosql_server):
    """Create a connection to the test GizmoSQL server"""
    connection_params = {
        "uri": f"grpc://localhost:{gizmosql_server['port']}",
        "db_kwargs": {
            "username": gizmosql_server["username"],
            "password": gizmosql_server["password"]
        }
    }
    
    with gizmosql.connect(**connection_params) as conn:
        yield conn


@pytest.fixture
def cursor(gizmosql_connection):
    """Create a cursor for the GizmoSQL connection"""
    with gizmosql_connection.cursor() as cur:
        yield cur


class GizmoSQLTestCase:
    """Base test case class with utilities for GizmoSQL testing"""
    
    @staticmethod
    def execute_query(cursor, query: str, parameters=None):
        """Execute a query and return results"""
        cursor.execute(query, parameters=parameters)
        return cursor.fetch_arrow_table()
    
    @staticmethod
    def assert_query_succeeds(cursor, query: str, parameters=None):
        """Assert that a query executes successfully"""
        try:
            result = GizmoSQLTestCase.execute_query(cursor, query, parameters)
            return result
        except Exception as e:
            pytest.fail(f"Query failed unexpectedly: {query}\nError: {e}")
    
    @staticmethod
    def assert_query_fails(cursor, query: str, expected_error: str = None, parameters=None):
        """Assert that a query fails with expected error"""
        with pytest.raises(Exception) as exc_info:
            GizmoSQLTestCase.execute_query(cursor, query, parameters)
        
        if expected_error:
            assert expected_error in str(exc_info.value), f"Expected error '{expected_error}' not found in '{exc_info.value}'"
        
        return exc_info.value