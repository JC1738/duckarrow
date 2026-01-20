-- Enable timer but keep bail off initially for validation tests
.timer on

LOAD './build/duckarrow.duckdb_extension';

-- Test 1: Configure duckarrow URI
SELECT '=== Test 1: Configure duckarrow URI ===' as test;
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'gizmosql_user', 'gizmosql_password', true);

-- Test 2: Basic replacement scan query
SELECT '=== Test 2: Basic replacement scan ===' as test;
SELECT * FROM duckarrow."Order" LIMIT 5;

-- Test 3: Column projection
SELECT '=== Test 3: Column projection ===' as test;
SELECT id, name, status FROM duckarrow."Order" LIMIT 3;

-- Test 4: WHERE clause filtering (DuckDB applies this locally)
SELECT '=== Test 4: WHERE clause ===' as test;
SELECT * FROM duckarrow."Order" WHERE status = 'COMPLETED' LIMIT 3;

-- Test 5: Aggregation with replacement scan
SELECT '=== Test 5: Aggregation ===' as test;
SELECT COUNT(*) as count FROM duckarrow."Order";

-- Test 6: Original duckarrow_query still works
SELECT '=== Test 6: Legacy table function ===' as test;
SELECT * FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT id, name FROM "Order" LIMIT 3'
);

-- Test 7: JOIN with local table
SELECT '=== Test 7: JOIN with local table ===' as test;
CREATE TEMP TABLE local_statuses AS SELECT 'CREATED' as status UNION SELECT 'COMPLETED';
SELECT g.id, g.name, g.status FROM duckarrow."Order" g
JOIN local_statuses l ON g.status = l.status
LIMIT 5;

-- Test 8: Subquery
SELECT '=== Test 8: Subquery ===' as test;
SELECT * FROM (SELECT id, name FROM duckarrow."Order" LIMIT 5) sub;

-- Test 9: LIMIT and OFFSET
SELECT '=== Test 9: LIMIT and OFFSET ===' as test;
SELECT id, name FROM duckarrow."Order" LIMIT 3 OFFSET 2;

-- Test 10: ORDER BY (applied locally by DuckDB)
SELECT '=== Test 10: ORDER BY ===' as test;
SELECT id, name FROM duckarrow."Order" ORDER BY id DESC LIMIT 5;

-- Test 11: Table name with embedded quotes (should be escaped correctly)
SELECT '=== Test 11: Table with embedded quotes ===' as test;
-- This tests that double quotes in table names are properly escaped
-- The table "Order""Test" would be escaped to "Order""""Test" in the query
-- Note: This will fail at the remote server (table doesn't exist) but tests escaping

-- Test 12: URI validation - empty URI should fail (expect error)
SELECT '=== Test 12: URI validation - empty ===' as test;
SELECT duckarrow_configure('', '', '');

-- Test 13: URI validation - invalid scheme should fail (expect error)
SELECT '=== Test 13: URI validation - invalid scheme ===' as test;
SELECT duckarrow_configure('http://localhost:1234', 'user', 'pass');

-- Test 14: URI validation - missing host should fail (expect error)
SELECT '=== Test 14: URI validation - no host ===' as test;
SELECT duckarrow_configure('grpc://', 'user', 'pass');

-- Test 15: URI validation - valid grpc:// scheme
SELECT '=== Test 15: URI validation - grpc scheme ===' as test;
SELECT duckarrow_configure('grpc://localhost:1234', 'user', 'pass');

-- Test 16: Reconfigure back to TLS for remaining tests
SELECT '=== Test 16: Reconfigure to TLS ===' as test;
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'gizmosql_user', 'gizmosql_password', true);

-- Test 17: NULL handling
SELECT '=== Test 17: NULL handling ===' as test;
SELECT duckarrow_configure(NULL, NULL, NULL);

-- Test 18: SQL injection - semicolon should be rejected (expect error)
SELECT '=== Test 18: SQL injection - semicolon ===' as test;
SELECT * FROM duckarrow."test;DROP TABLE users" LIMIT 1;

-- Test 19: SQL injection - comment sequence should be rejected (expect error)
SELECT '=== Test 19: SQL injection - comment ===' as test;
SELECT * FROM duckarrow."test--malicious" LIMIT 1;

-- Test 20: SQL injection - block comment should be rejected (expect error)
SELECT '=== Test 20: SQL injection - block comment ===' as test;
SELECT * FROM duckarrow."test/*evil*/" LIMIT 1;
