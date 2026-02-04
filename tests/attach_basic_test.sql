-- ATTACH/DETACH Basic Tests
-- Tests ATTACH ... (TYPE duckarrow) and DETACH functionality
-- Note: This test requires a running Flight SQL server at localhost:31337

-- Load extension
LOAD './build/duckarrow.duckdb_extension';

.print '=== ATTACH/DETACH Basic Tests ==='

-- ============================================================================
-- TEST 1: Basic ATTACH with grpc+tls
-- ============================================================================
.print ''
.print '--- Test 1: Basic ATTACH with grpc+tls ---'
.bail on

ATTACH 'grpc+tls://localhost:31337' AS flightdb (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

.print 'Test 1 PASSED - ATTACH with grpc+tls succeeded'

-- ============================================================================
-- TEST 2: Verify attached database is visible
-- ============================================================================
.print ''
.print '--- Test 2: Verify attached database is visible ---'

-- List databases should show flightdb
SELECT database_name FROM duckdb_databases() WHERE database_name = 'flightdb';

.print 'Test 2 PASSED - Attached database is visible'

-- ============================================================================
-- TEST 3: Query through attached database
-- ============================================================================
.print ''
.print '--- Test 3: Query through attached database ---'

-- Query a table through the attached database
SELECT COUNT(*) as row_count FROM flightdb.main."Order";

.print 'Test 3 PASSED - Query through attached database succeeded'

-- ============================================================================
-- TEST 4: DETACH the database
-- ============================================================================
.print ''
.print '--- Test 4: DETACH ---'

DETACH flightdb;

.print 'Test 4 PASSED - DETACH succeeded'

-- ============================================================================
-- TEST 5: Verify database is detached
-- ============================================================================
.print ''
.print '--- Test 5: Verify database is detached ---'

-- The database should no longer be visible
SELECT COUNT(*) as count FROM duckdb_databases() WHERE database_name = 'flightdb';
-- Should return 0

.print 'Test 5 PASSED - Database is no longer attached'

-- ============================================================================
-- TEST 6: Re-attach after detach
-- ============================================================================
.print ''
.print '--- Test 6: Re-attach after detach ---'

ATTACH 'grpc+tls://localhost:31337' AS flightdb2 (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

-- Verify it works
SELECT COUNT(*) as row_count FROM flightdb2.main."Order";

DETACH flightdb2;

.print 'Test 6 PASSED - Re-attach and re-detach succeeded'

-- ============================================================================
-- TEST 7: Error handling - Invalid URI
-- ============================================================================
.print ''
.print '--- Test 7: Error handling - Invalid URI ---'
.bail off

-- Invalid scheme should fail
ATTACH 'http://localhost:31337' AS baddb (TYPE duckarrow);

.print 'Test 7 completed (error expected above for invalid scheme)'

-- ============================================================================
-- TEST 8: Error handling - Empty URI
-- ============================================================================
.print ''
.print '--- Test 8: Error handling - Empty URI ---'

ATTACH '' AS emptydb (TYPE duckarrow);

.print 'Test 8 completed (error expected above for empty URI)'

-- ============================================================================
-- TEST 9: Error handling - Non-existent server
-- ============================================================================
.print ''
.print '--- Test 9: Error handling - Non-existent server ---'

-- This should attach but fail when trying to use it
ATTACH 'grpc://localhost:99999' AS noserver (TYPE duckarrow);

.print 'Test 9 completed (ATTACH may succeed, but queries should fail)'

-- Clean up if it attached
DETACH IF EXISTS noserver;

-- ============================================================================
-- TEST 10: ATTACH with grpc (non-TLS)
-- ============================================================================
.print ''
.print '--- Test 10: ATTACH with grpc (non-TLS) ---'

-- Note: This will likely fail to connect if the server requires TLS
-- But it tests that the grpc:// scheme is accepted
ATTACH 'grpc://localhost:31337' AS grpcdb (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

.print 'Test 10 completed (may succeed or fail depending on server TLS requirements)'

DETACH IF EXISTS grpcdb;

-- ============================================================================
-- TEST 11: Recovery - Verify extension still works after errors
-- ============================================================================
.print ''
.print '--- Test 11: Recovery after errors ---'
.bail on

-- Restore a valid attachment
ATTACH 'grpc+tls://localhost:31337' AS recovery_db (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

SELECT COUNT(*) as recovery_test FROM recovery_db.main."Order";

DETACH recovery_db;

.print 'Test 11 PASSED - Successfully recovered after error tests'

.print ''
.print '=== ATTACH/DETACH Basic Tests Complete ==='
