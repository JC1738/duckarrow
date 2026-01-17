-- Connection Pool Test
-- This test verifies that connection pooling is working.
-- The first query should be slower (cold connection) while
-- subsequent queries should be faster (connection reuse).
.timer on

LOAD './build/duckarrow.duckdb_extension';

-- Configure
SELECT '=== Configure ===' as test;
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'duckarrow_user', 'duckarrow_password', true);

-- First query (cold connection - expect ~150-200ms)
SELECT '=== Query 1 (cold connection) ===' as test;
SELECT COUNT(*) as count FROM duckarrow."Order";

-- Second query (should reuse connection - expect ~1-5ms)
SELECT '=== Query 2 (warm - reused connection) ===' as test;
SELECT COUNT(*) as count FROM duckarrow."Order";

-- Third query (should reuse connection)
SELECT '=== Query 3 (warm - reused connection) ===' as test;
SELECT COUNT(*) as count FROM duckarrow."Order";

-- Fourth query - different query, same connection
SELECT '=== Query 4 (warm - different query) ===' as test;
SELECT id, status FROM duckarrow."Order" LIMIT 5;

-- Fifth query - back to original table
SELECT '=== Query 5 (warm - back to Order) ===' as test;
SELECT id, name FROM duckarrow."Order" LIMIT 3;

-- Test reconfiguration creates new connection (same credentials, new pool entry)
SELECT '=== Reconfigure (same server, fresh connection) ===' as test;
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'duckarrow_user', 'duckarrow_password', true);

-- First query after reconfig (should use existing pool)
SELECT '=== Query 6 (after reconfig) ===' as test;
SELECT COUNT(*) as count FROM duckarrow."Order";

-- Second query after reconfig (warm - reused)
SELECT '=== Query 7 (warm - reused after reconfig) ===' as test;
SELECT COUNT(*) as count FROM duckarrow."Order";
