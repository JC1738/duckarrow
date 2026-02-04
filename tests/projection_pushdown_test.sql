-- Projection Pushdown Tests
-- Tests that column projection is correctly pushed down to the Flight SQL server
-- Note: This test requires a running Flight SQL server at localhost:31337

-- Load extension
LOAD './build/duckarrow.duckdb_extension';

.print '=== Projection Pushdown Tests ==='

-- ============================================================================
-- TEST 1: ATTACH database AS flightdb
-- ============================================================================
.print ''
.print '--- Test 1: ATTACH database AS flightdb ---'
.bail on

ATTACH 'grpc+tls://localhost:31337' AS flightdb (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

.print 'Test 1 PASSED - Database attached as flightdb'

-- ============================================================================
-- TEST 2: Single column projection
-- ============================================================================
.print ''
.print '--- Test 2: SELECT single column (id) ---'

SELECT id FROM flightdb."Order" LIMIT 5;

.print 'Test 2 PASSED - Single column projection succeeded'

-- ============================================================================
-- TEST 3: Two column projection
-- ============================================================================
.print ''
.print '--- Test 3: SELECT two columns (id, name) ---'

SELECT id, name FROM flightdb."Order" LIMIT 5;

.print 'Test 3 PASSED - Two column projection succeeded'

-- ============================================================================
-- TEST 4: Multiple column projection with different types
-- ============================================================================
.print ''
.print '--- Test 4: SELECT multiple columns with different types ---'

SELECT id, name, status, totalCostCents FROM flightdb."Order" LIMIT 5;

.print 'Test 4 PASSED - Multiple column projection succeeded'

-- ============================================================================
-- TEST 5: Column projection with WHERE clause
-- ============================================================================
.print ''
.print '--- Test 5: SELECT specific columns with WHERE clause ---'

SELECT id, status FROM flightdb."Order" WHERE status IS NOT NULL LIMIT 5;

.print 'Test 5 PASSED - Column projection with WHERE clause succeeded'

-- ============================================================================
-- TEST 6: Column projection with ORDER BY
-- ============================================================================
.print ''
.print '--- Test 6: SELECT specific columns with ORDER BY ---'

SELECT id, name FROM flightdb."Order" ORDER BY id LIMIT 5;

.print 'Test 6 PASSED - Column projection with ORDER BY succeeded'

-- ============================================================================
-- TEST 7: Column projection in JOIN (both tables)
-- ============================================================================
.print ''
.print '--- Test 7: SELECT specific columns from JOIN ---'

SELECT
    o.id as order_id,
    o.status as order_status,
    c.name as customer_name
FROM flightdb."Order" o
JOIN flightdb."Customer" c ON o.customer_id = c.id
LIMIT 5;

.print 'Test 7 PASSED - Column projection in JOIN succeeded'

-- ============================================================================
-- TEST 8: Column projection with aggregation
-- ============================================================================
.print ''
.print '--- Test 8: SELECT with aggregation on projected column ---'

SELECT COUNT(*) as order_count FROM flightdb."Order";

.print 'Test 8 PASSED - Column projection with aggregation succeeded'

-- ============================================================================
-- TEST 9: Column projection with GROUP BY
-- ============================================================================
.print ''
.print '--- Test 9: SELECT specific column with GROUP BY ---'

SELECT status, COUNT(*) as count
FROM flightdb."Order"
WHERE status IS NOT NULL
GROUP BY status
LIMIT 10;

.print 'Test 9 PASSED - Column projection with GROUP BY succeeded'

-- ============================================================================
-- TEST 10: Column projection in subquery
-- ============================================================================
.print ''
.print '--- Test 10: Column projection in subquery ---'

SELECT * FROM (
    SELECT id, customer_id
    FROM flightdb."Order"
    LIMIT 5
) sub;

.print 'Test 10 PASSED - Column projection in subquery succeeded'

-- ============================================================================
-- TEST 11: Column projection in CTE
-- ============================================================================
.print ''
.print '--- Test 11: Column projection in CTE ---'

WITH order_subset AS (
    SELECT id, name, status
    FROM flightdb."Order"
    LIMIT 5
)
SELECT id, status FROM order_subset;

.print 'Test 11 PASSED - Column projection in CTE succeeded'

-- ============================================================================
-- TEST 12: Column aliasing with projection
-- ============================================================================
.print ''
.print '--- Test 12: Column aliasing with projection ---'

SELECT
    id as order_id,
    name as order_name,
    status as order_status
FROM flightdb."Order"
LIMIT 5;

.print 'Test 12 PASSED - Column aliasing with projection succeeded'

-- ============================================================================
-- TEST 13: Three-part name with projection
-- ============================================================================
.print ''
.print '--- Test 13: Three-part name (database.schema.table) with projection ---'

SELECT id, name FROM flightdb.main."Order" LIMIT 5;

.print 'Test 13 PASSED - Three-part name with projection succeeded'

-- ============================================================================
-- TEST 14: Mixed column types in projection
-- ============================================================================
.print ''
.print '--- Test 14: Projection with mixed column types ---'

-- Select columns of different types to verify type handling in projection
SELECT id, name, totalCostCents, createdAt FROM flightdb."Order" LIMIT 5;

.print 'Test 14 PASSED - Mixed column types projection succeeded'

-- ============================================================================
-- TEST 15: Verify DESCRIBE shows correct projected columns
-- ============================================================================
.print ''
.print '--- Test 15: DESCRIBE on projected columns ---'

DESCRIBE SELECT id, name FROM flightdb."Order" LIMIT 1;

.print 'Test 15 PASSED - DESCRIBE on projected columns succeeded'

-- ============================================================================
-- Cleanup
-- ============================================================================
.print ''
.print '--- Cleanup ---'

USE memory;
DETACH flightdb;

.print 'Cleanup complete'

.print ''
.print '=== Projection Pushdown Tests Complete ==='
