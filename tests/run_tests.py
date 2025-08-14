#!/usr/bin/env python3
"""
GizmoSQL Test Runner

This script provides various ways to run the GizmoSQL test suite with different configurations.
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and handle errors"""
    print(f"\n{'='*60}")
    print(f"Running: {description or ' '.join(cmd)}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode != 0:
        print(f"\n❌ FAILED: {description or ' '.join(cmd)}")
        return False
    else:
        print(f"\n✅ PASSED: {description or ' '.join(cmd)}")
        return True


def main():
    parser = argparse.ArgumentParser(description="GizmoSQL Test Runner")
    parser.add_argument("--suite", choices=["all", "pivot", "regression", "performance", "integration"], 
                       default="all", help="Test suite to run")
    parser.add_argument("--coverage", action="store_true", help="Run with coverage reporting")
    parser.add_argument("--parallel", "-j", type=int, help="Number of parallel workers")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--slow", action="store_true", help="Include slow tests")
    parser.add_argument("--smoke", action="store_true", help="Run only smoke tests (quick validation)")
    parser.add_argument("--server-binary", help="Path to gizmosql_server binary (auto-detected if not provided)")
    
    args = parser.parse_args()
    
    # Check if gizmosql_server is available
    server_binary = args.server_binary or "gizmosql_server"
    try:
        result = subprocess.run([server_binary, "--help"], capture_output=True, timeout=5)
        if result.returncode != 0:
            print(f"❌ ERROR: Cannot find or run gizmosql_server binary: {server_binary}")
            print("Please ensure gizmosql_server is built and in your PATH, or specify --server-binary")
            return 1
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print(f"❌ ERROR: Cannot find or run gizmosql_server binary: {server_binary}")
        print("Please ensure gizmosql_server is built and in your PATH, or specify --server-binary")
        return 1
    
    print(f"✅ Found gizmosql_server binary: {server_binary}")
    
    # Set environment variable for tests
    os.environ["GIZMOSQL_SERVER_BINARY"] = server_binary
    
    # Build pytest command
    cmd = ["python", "-m", "pytest"]
    
    if args.verbose:
        cmd.append("-v")
    
    if args.coverage:
        cmd.extend(["--cov=src", "--cov-report=html", "--cov-report=term"])
    
    if args.parallel:
        cmd.extend(["-n", str(args.parallel)])
    
    # Select test suite
    if args.smoke:
        # Quick smoke tests
        cmd.extend([
            "tests/test_pivot_issues.py::TestPivotIssues::test_pivot_github_issue_44_original_query",
            "tests/test_regression.py::TestRegression::test_basic_select_queries",
            "tests/test_statement_execution.py::TestStatementExecution::test_simple_prepared_statements"
        ])
    elif args.suite == "pivot":
        cmd.extend(["-m", "pivot", "tests/test_pivot_issues.py", "tests/test_statement_execution.py"])
    elif args.suite == "regression":
        cmd.extend(["-m", "regression", "tests/test_regression.py"])
    elif args.suite == "performance":
        cmd.extend(["-m", "performance", "tests/test_performance.py"])
    elif args.suite == "integration":
        cmd.extend(["-m", "integration", "tests/test_integration.py"])
    else:  # all
        cmd.append("tests/")
    
    # Handle slow tests
    if not args.slow:
        cmd.extend(["-m", "not slow"])
    
    success = run_command(cmd, f"GizmoSQL {args.suite} test suite")
    
    if success:
        print(f"\n🎉 All tests passed!")
        if args.coverage:
            print("📊 Coverage report generated in htmlcov/index.html")
        return 0
    else:
        print(f"\n💥 Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())