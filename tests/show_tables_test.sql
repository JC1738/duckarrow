-- SHOW TABLES Tests
-- Tests SHOW TABLES functionality for attached duckarrow databases
-- Note: This test requires a running Flight SQL server at localhost:31337

-- Load extension
LOAD './build/duckarrow.duckdb_extension';

.print '=== SHOW TABLES Tests ==='

-- ============================================================================
-- TEST 1: ATTACH database for testing
-- ============================================================================
.print ''
.print '--- Test 1: ATTACH database ---'
.bail on

ATTACH 'grpc+tls://localhost:31337' AS flightdb (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

.print 'Test 1 PASSED - Database attached successfully'

-- ============================================================================
-- TEST 2: SHOW TABLES FROM database
-- ============================================================================
.print ''
.print '--- Test 2: SHOW TABLES FROM flightdb ---'

SHOW TABLES FROM flightdb;

.print 'Test 2 PASSED - SHOW TABLES FROM db succeeded'

-- ============================================================================
-- TEST 3: SHOW TABLES FROM database.schema
-- ============================================================================
.print ''
.print '--- Test 3: SHOW TABLES FROM flightdb.main ---'

SHOW TABLES FROM flightdb.main;

.print 'Test 3 PASSED - SHOW TABLES FROM db.schema succeeded'

-- ============================================================================
-- TEST 4: Verify table list is returned (using duckdb_tables)
-- ============================================================================
.print ''
.print '--- Test 4: Verify tables exist using duckdb_tables ---'

-- Query the catalog to verify tables are visible
SELECT database_name, schema_name, table_name
FROM duckdb_tables()
WHERE database_name = 'flightdb'
ORDER BY table_name
LIMIT 10;

.print 'Test 4 PASSED - Tables are visible in catalog'

-- ============================================================================
-- TEST 5: SHOW ALL TABLES (includes all attached databases)
-- ============================================================================
.print ''
.print '--- Test 5: SHOW ALL TABLES ---'

SHOW ALL TABLES;

.print 'Test 5 PASSED - SHOW ALL TABLES succeeded'

-- ============================================================================
-- TEST 6: Verify specific table exists and is queryable
-- ============================================================================
.print ''
.print '--- Test 6: Verify table is queryable ---'

-- Query the Order table to verify it exists and works
SELECT COUNT(*) as order_count FROM flightdb.main."Order";

.print 'Test 6 PASSED - Table is queryable'

-- ============================================================================
-- Cleanup
-- ============================================================================
.print ''
.print '--- Cleanup ---'

DETACH flightdb;

.print 'Cleanup complete'

.print ''
.print '=== SHOW TABLES Tests Complete ==='
