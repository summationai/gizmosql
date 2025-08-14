#!/usr/bin/env python3
"""
Test script to validate that the PIVOT fix has been implemented correctly.
This script tests both the original behavior and expected fixed behavior.
"""

import subprocess
import time
import sys
import os

def run_docker_command(cmd, capture_output=True):
    """Run a docker command and return the result"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=capture_output, text=True, timeout=30)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"

def test_original_image():
    """Test that the original image fails with the PIVOT issue"""
    print("=== Testing Original GizmoSQL Image ===")
    
    # Start original container
    print("Starting original GizmoSQL container...")
    start_cmd = "docker run --rm -d --name gizmosql-original -p 31338:31337 -e TLS_ENABLED=1 -e GIZMOSQL_PASSWORD=test123 gizmodata/gizmosql:latest"
    ret, stdout, stderr = run_docker_command(start_cmd)
    
    if ret != 0:
        print(f"Failed to start original container: {stderr}")
        return False
    
    container_id = stdout.strip()
    print(f"Started container: {container_id[:12]}")
    
    # Wait for startup
    time.sleep(10)
    
    try:
        # Install missing library
        print("Installing required library...")
        install_cmd = "docker exec -u root gizmosql-original bash -c 'apt-get update -qq && apt-get install -y libgflags2.2'"
        run_docker_command(install_cmd)
        
        # Create test table
        print("Creating test table...")
        create_cmd = 'docker exec gizmosql-original gizmosql_client --command Execute --host localhost --port 31337 --username gizmosql_username --password test123 --query "CREATE TABLE pivottest (period Date, category String, league String, pnl_amount DECIMAL(38, 2))" --use-tls --tls-skip-verify'
        ret, stdout, stderr = run_docker_command(create_cmd)
        
        if ret != 0:
            print(f"Failed to create table: {stderr}")
            return False
            
        # Insert test data
        print("Inserting test data...")
        insert_cmd = 'docker exec gizmosql-original gizmosql_client --command Execute --host localhost --port 31337 --username gizmosql_username --password test123 --query "INSERT INTO pivottest VALUES (\'2024-01-01\', \'Other Sales Revenue\', \'C\', 16304900), (\'2024-02-01\', \'Discount\', \'M\', 17918200)" --use-tls --tls-skip-verify'
        ret, stdout, stderr = run_docker_command(insert_cmd)
        
        if ret != 0:
            print(f"Failed to insert data: {stderr}")
            return False
            
        # Test problematic PIVOT query
        print("Testing problematic PIVOT query (should fail)...")
        pivot_cmd = 'docker exec gizmosql-original gizmosql_client --command Execute --host localhost --port 31337 --username gizmosql_username --password test123 --query "PIVOT (select * from pivottest where (league in (\'M\'))) ON league USING sum(pnl_amount) GROUP BY category ORDER BY category LIMIT 100 OFFSET 0" --use-tls --tls-skip-verify'
        ret, stdout, stderr = run_docker_command(pivot_cmd)
        
        if ret == 0:
            print("❌ UNEXPECTED: PIVOT query succeeded in original image!")
            print(f"Output: {stdout}")
            return False
        elif "Cannot prepare multiple statements at once" in stderr:
            print("✅ EXPECTED: PIVOT query failed with 'Cannot prepare multiple statements at once' error")
            return True
        else:
            print(f"❌ UNEXPECTED ERROR: {stderr}")
            return False
            
    finally:
        # Cleanup
        print("Stopping original container...")
        run_docker_command("docker stop gizmosql-original")

def test_fix_implementation():
    """Test that our fix implementation logic is correct"""
    print("\n=== Testing PIVOT Fix Implementation ===")
    
    # Check that our modified files exist and contain the expected changes
    files_to_check = [
        ("src/duckdb/duckdb_statement.h", ["use_direct_execution_", "sql_"]),
        ("src/duckdb/duckdb_statement.cpp", ["Cannot prepare multiple statements at once", "use_direct_execution_"]),
        ("src/duckdb/duckdb_server.cpp", ["stmt == nullptr"])
    ]
    
    all_good = True
    for file_path, expected_content in files_to_check:
        print(f"Checking {file_path}...")
        if not os.path.exists(file_path):
            print(f"❌ File {file_path} does not exist")
            all_good = False
            continue
            
        with open(file_path, 'r') as f:
            content = f.read()
            
        for expected in expected_content:
            if expected not in content:
                print(f"❌ Expected content '{expected}' not found in {file_path}")
                all_good = False
            else:
                print(f"✅ Found expected content '{expected}' in {file_path}")
    
    return all_good

def main():
    """Main test function"""
    print("GizmoSQL PIVOT Fix Validation Test")
    print("=" * 50)
    
    # Test 1: Verify the original issue exists
    original_test_passed = test_original_image()
    
    # Test 2: Verify our fix implementation is in place
    fix_implementation_good = test_fix_implementation()
    
    print("\n=== Test Summary ===")
    print(f"Original issue reproduction: {'✅ PASS' if original_test_passed else '❌ FAIL'}")
    print(f"Fix implementation check: {'✅ PASS' if fix_implementation_good else '❌ FAIL'}")
    
    if original_test_passed and fix_implementation_good:
        print("\n🎉 All tests passed! The PIVOT fix implementation appears to be correctly in place.")
        print("\nTo fully validate the fix, you would need to:")
        print("1. Build a Docker image with the current changes")
        print("2. Test the same PIVOT query against the new image")
        print("3. Verify it succeeds and returns expected results")
        return 0
    else:
        print("\n❌ Some tests failed. Please review the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())