.bail on
.timer on

LOAD './build/duckarrow.duckdb_extension';

-- Configure credentials
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'gizmosql_user', 'gizmosql_password', true);

-- Test 1: Basic query with real data and types
SELECT '=== Test 1: Basic query with types ===' as test;
SELECT * FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT id, name, status, totalCostCents FROM "Order" LIMIT 5'
);

-- Test 2: Verify column types
SELECT '=== Test 2: Column types ===' as test;
DESCRIBE SELECT * FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT id, totalCostCents, createdAt FROM "Order" LIMIT 1'
);

-- Test 3: Aggregation (proves data is usable)
SELECT '=== Test 3: Aggregation ===' as test;
SELECT COUNT(*) as count, SUM(totalCostCents::BIGINT) as total
FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT totalCostCents FROM "Order"'
);

-- Test 4: Integer type preservation
SELECT '=== Test 4: Integer operations ===' as test;
SELECT * FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT 1 as int_val, 12345678901234 as bigint_val, 3.14159 as float_val'
);

-- Test 5: NULL handling
SELECT '=== Test 5: NULL handling ===' as test;
SELECT id, name FROM duckarrow_query(
    'grpc+tls://localhost:31337',
    'SELECT id, name FROM "Order" WHERE name IS NULL LIMIT 3'
);
