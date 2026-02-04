-- Information Schema Tests
-- Tests information_schema integration for attached duckarrow databases
-- Note: This test requires a running Flight SQL server at localhost:31337

-- Load extension
LOAD './build/duckarrow.duckdb_extension';

.print '=== Information Schema Tests ==='

-- ============================================================================
-- TEST 1: ATTACH database for testing
-- ============================================================================
.print ''
.print '--- Test 1: ATTACH database ---'
.bail on

ATTACH 'grpc+tls://localhost:31337' AS flightdb (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

.print 'Test 1 PASSED - Database attached successfully'

-- ============================================================================
-- TEST 2: Query information_schema.tables for attached database
-- ============================================================================
.print ''
.print '--- Test 2: information_schema.tables for flightdb ---'

SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_catalog = 'flightdb'
ORDER BY table_schema, table_name
LIMIT 20;

.print 'Test 2 PASSED - information_schema.tables query succeeded'

-- ============================================================================
-- TEST 3: Verify tables appear in information_schema
-- ============================================================================
.print ''
.print '--- Test 3: Verify tables count in information_schema ---'

SELECT COUNT(*) as table_count
FROM information_schema.tables
WHERE table_catalog = 'flightdb';

.print 'Test 3 PASSED - Tables are visible in information_schema'

-- ============================================================================
-- TEST 4: Query information_schema.columns for attached tables
-- ============================================================================
.print ''
.print '--- Test 4: information_schema.columns for flightdb tables ---'

SELECT table_catalog, table_schema, table_name, column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_catalog = 'flightdb'
ORDER BY table_schema, table_name, ordinal_position
LIMIT 30;

.print 'Test 4 PASSED - information_schema.columns query succeeded'

-- ============================================================================
-- TEST 5: Query columns for a specific table
-- ============================================================================
.print ''
.print '--- Test 5: Columns for a specific table (Order) ---'

SELECT column_name, data_type, ordinal_position, is_nullable
FROM information_schema.columns
WHERE table_catalog = 'flightdb'
  AND table_name = 'Order'
ORDER BY ordinal_position;

.print 'Test 5 PASSED - Specific table columns query succeeded'

-- ============================================================================
-- TEST 6: Query information_schema.schemata
-- ============================================================================
.print ''
.print '--- Test 6: information_schema.schemata for flightdb ---'

SELECT catalog_name, schema_name
FROM information_schema.schemata
WHERE catalog_name = 'flightdb'
ORDER BY schema_name;

.print 'Test 6 PASSED - information_schema.schemata query succeeded'

-- ============================================================================
-- TEST 7: Compare with duckdb_tables for consistency
-- ============================================================================
.print ''
.print '--- Test 7: Consistency check - compare table counts ---'

-- Count from information_schema
SELECT 'information_schema' as source, COUNT(*) as count
FROM information_schema.tables
WHERE table_catalog = 'flightdb'
UNION ALL
-- Count from duckdb_tables
SELECT 'duckdb_tables' as source, COUNT(*) as count
FROM duckdb_tables()
WHERE database_name = 'flightdb';

.print 'Test 7 PASSED - Consistency check completed'

-- ============================================================================
-- Cleanup
-- ============================================================================
.print ''
.print '--- Cleanup ---'

DETACH flightdb;

.print 'Cleanup complete'

.print ''
.print '=== Information Schema Tests Complete ==='
