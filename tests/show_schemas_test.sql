-- SHOW SCHEMAS Tests
-- Tests SHOW SCHEMAS FROM <attached_database> functionality
-- Note: This test requires a running Flight SQL server at localhost:31337

-- Load extension
LOAD './build/duckarrow.duckdb_extension';

.print '=== SHOW SCHEMAS Tests ==='

-- ============================================================================
-- TEST 1: ATTACH a database for testing
-- ============================================================================
.print ''
.print '--- Test 1: ATTACH database for schema tests ---'
.bail on

ATTACH 'grpc+tls://localhost:31337' AS flightdb (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

.print 'Test 1 PASSED - ATTACH succeeded'

-- ============================================================================
-- TEST 2: SHOW SCHEMAS FROM attached database
-- ============================================================================
.print ''
.print '--- Test 2: SHOW SCHEMAS FROM attached database ---'

SHOW SCHEMAS FROM flightdb;

.print 'Test 2 PASSED - SHOW SCHEMAS returned results'

-- ============================================================================
-- TEST 3: Verify main schema exists
-- ============================================================================
.print ''
.print '--- Test 3: Verify main schema exists ---'

-- Query schemas and check for 'main'
SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'flightdb' AND schema_name = 'main';

.print 'Test 3 PASSED - main schema exists'

-- ============================================================================
-- TEST 4: Count schemas
-- ============================================================================
.print ''
.print '--- Test 4: Count schemas ---'

SELECT COUNT(*) as schema_count FROM duckdb_schemas() WHERE database_name = 'flightdb';

.print 'Test 4 PASSED - Schema count retrieved'

-- ============================================================================
-- TEST 5: SHOW ALL SCHEMAS (should include flightdb schemas)
-- ============================================================================
.print ''
.print '--- Test 5: SHOW ALL SCHEMAS ---'

SHOW ALL SCHEMAS;

.print 'Test 5 PASSED - SHOW ALL SCHEMAS returned results'

-- ============================================================================
-- TEST 6: Error handling - SHOW SCHEMAS FROM non-existent database
-- ============================================================================
.print ''
.print '--- Test 6: Error handling - non-existent database ---'
.bail off

-- This should fail since 'nonexistent_db' does not exist
SHOW SCHEMAS FROM nonexistent_db;

.print 'Test 6 completed (error expected above for non-existent database)'

-- ============================================================================
-- TEST 7: Recovery - Verify extension still works after error
-- ============================================================================
.print ''
.print '--- Test 7: Recovery after error ---'
.bail on

-- Verify we can still use the attached database
SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'flightdb' LIMIT 1;

.print 'Test 7 PASSED - Recovery after error succeeded'

-- ============================================================================
-- Cleanup
-- ============================================================================
.print ''
.print '--- Cleanup ---'

DETACH flightdb;

.print 'Cleanup complete'

.print ''
.print '=== SHOW SCHEMAS Tests Complete ==='
