-- Qualified Queries Tests
-- Tests qualified table name syntax for attached duckarrow databases
-- Note: This test requires a running Flight SQL server at localhost:31337

-- Load extension
LOAD './build/duckarrow.duckdb_extension';

.print '=== Qualified Queries Tests ==='

-- ============================================================================
-- TEST 1: ATTACH database AS flightdb
-- ============================================================================
.print ''
.print '--- Test 1: ATTACH database AS flightdb ---'
.bail on

ATTACH 'grpc+tls://localhost:31337' AS flightdb (TYPE duckarrow, username 'gizmosql_user', password 'gizmosql_password');

.print 'Test 1 PASSED - Database attached as flightdb'

-- ============================================================================
-- TEST 2: Three-part name (database.schema.table)
-- ============================================================================
.print ''
.print '--- Test 2: SELECT * FROM flightdb.main.tablename (three-part name) ---'

SELECT COUNT(*) as count FROM flightdb.main."Order";

.print 'Test 2 PASSED - Three-part name query succeeded'

-- ============================================================================
-- TEST 3: Two-part name (database.table)
-- ============================================================================
.print ''
.print '--- Test 3: SELECT * FROM flightdb.tablename (two-part name) ---'

SELECT COUNT(*) as count FROM flightdb."Order";

.print 'Test 3 PASSED - Two-part name query succeeded'

-- ============================================================================
-- TEST 4: Schema qualified with USE
-- ============================================================================
.print ''
.print '--- Test 4: USE database and query with schema.table ---'

USE flightdb;
SELECT COUNT(*) as count FROM main."Order";

.print 'Test 4 PASSED - USE database with schema.table query succeeded'

-- ============================================================================
-- TEST 5: Fully qualified in JOIN
-- ============================================================================
.print ''
.print '--- Test 5: JOIN using qualified names ---'

SELECT
    o.id as order_id,
    c.name as customer_name
FROM flightdb.main."Order" o
JOIN flightdb.main."Customer" c ON o.customer_id = c.id
LIMIT 5;

.print 'Test 5 PASSED - JOIN with qualified names succeeded'

-- ============================================================================
-- TEST 6: Mixed qualification (two-part and three-part)
-- ============================================================================
.print ''
.print '--- Test 6: Mixed qualification in single query ---'

SELECT
    o.id as order_id,
    c.name as customer_name
FROM flightdb."Order" o
JOIN flightdb.main."Customer" c ON o.customer_id = c.id
LIMIT 5;

.print 'Test 6 PASSED - Mixed qualification query succeeded'

-- ============================================================================
-- TEST 7: Subquery with qualified names
-- ============================================================================
.print ''
.print '--- Test 7: Subquery with qualified names ---'

SELECT * FROM (
    SELECT id, customer_id
    FROM flightdb.main."Order"
    LIMIT 5
) sub;

.print 'Test 7 PASSED - Subquery with qualified names succeeded'

-- ============================================================================
-- TEST 8: CTE with qualified names
-- ============================================================================
.print ''
.print '--- Test 8: CTE with qualified names ---'

WITH order_cte AS (
    SELECT id, customer_id
    FROM flightdb.main."Order"
    LIMIT 5
)
SELECT * FROM order_cte;

.print 'Test 8 PASSED - CTE with qualified names succeeded'

-- ============================================================================
-- Cleanup
-- ============================================================================
.print ''
.print '--- Cleanup ---'

USE memory;
DETACH flightdb;

.print 'Cleanup complete'

.print ''
.print '=== Qualified Queries Tests Complete ==='
