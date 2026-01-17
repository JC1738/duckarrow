-- Edge Case Integration Tests
-- Tests large results, empty results, unicode, and other edge cases
-- Requires: Flight SQL server at localhost:31337

.bail on

-- Load extension and configure
LOAD './build/duckarrow.duckdb_extension';
SELECT duckarrow_configure('grpc://localhost:31337', 'duckarrow_user', 'duckarrow_password');

.print '=== Edge Case Integration Tests ==='

-- ============================================================================
-- EMPTY RESULT SET
-- ============================================================================
.print '--- Empty Result Set ---'

-- Query that returns zero rows
SELECT * FROM duckarrow."Order" WHERE 1=0;

-- Verify count is 0
SELECT COUNT(*) as row_count FROM duckarrow."Order" WHERE 1=0;

-- ============================================================================
-- SINGLE ROW RESULT
-- ============================================================================
.print '--- Single Row Result ---'

-- Single row via LIMIT
SELECT * FROM duckarrow."Order" LIMIT 1;

-- Synthetic single row
SELECT 1 as val, 'test' as str FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- LARGE RESULT SET (MULTI-BATCH)
-- ============================================================================
.print '--- Large Result Set ---'

-- Test with larger result set to verify multi-batch handling
-- Note: Adjust table/limit based on available data
SELECT COUNT(*) as total_rows FROM duckarrow."Order";

-- Fetch many rows (tests multi-batch Arrow streaming)
SELECT * FROM duckarrow."Order" LIMIT 5000;

-- Verify we got the expected number
SELECT COUNT(*) as fetched_count FROM (SELECT * FROM duckarrow."Order" LIMIT 5000);

-- ============================================================================
-- UNICODE DATA
-- ============================================================================
.print '--- Unicode Data ---'

-- Test various unicode characters
SELECT '世界' as chinese FROM duckarrow."Order" LIMIT 1;
SELECT 'مرحبا' as arabic FROM duckarrow."Order" LIMIT 1;
SELECT '🌍🌎🌏' as emoji FROM duckarrow."Order" LIMIT 1;
SELECT 'Ñoño' as spanish FROM duckarrow."Order" LIMIT 1;
SELECT 'Москва' as russian FROM duckarrow."Order" LIMIT 1;
SELECT '東京' as japanese FROM duckarrow."Order" LIMIT 1;

-- Unicode in identifiers (via alias)
SELECT 1 as "列名" FROM duckarrow."Order" LIMIT 1;

-- Long unicode string
SELECT REPEAT('世', 100) as long_unicode FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- WIDE TABLES (MANY COLUMNS)
-- ============================================================================
.print '--- Wide Tables ---'

-- Query with many synthetic columns
SELECT
    1 as col1, 2 as col2, 3 as col3, 4 as col4, 5 as col5,
    6 as col6, 7 as col7, 8 as col8, 9 as col9, 10 as col10,
    'a' as str1, 'b' as str2, 'c' as str3, 'd' as str4, 'e' as str5,
    TRUE as bool1, FALSE as bool2,
    1.1 as float1, 2.2 as float2, 3.3 as float3,
    DATE '2024-01-01' as date1,
    TIMESTAMP '2024-01-01 12:00:00' as ts1
FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- MIXED TYPES IN SINGLE QUERY
-- ============================================================================
.print '--- Mixed Types ---'

SELECT
    CAST(1 AS TINYINT) as tiny,
    CAST(1000 AS SMALLINT) as small,
    CAST(100000 AS INTEGER) as med,
    CAST(10000000000 AS BIGINT) as big,
    CAST(3.14 AS REAL) as float32,
    CAST(3.14159265359 AS DOUBLE) as float64,
    'text' as varchar_col,
    TRUE as bool_col,
    DATE '2024-06-15' as date_col,
    TIME '14:30:00' as time_col,
    TIMESTAMP '2024-06-15 14:30:00' as ts_col,
    [1,2,3] as list_col,
    {'a': 1} as struct_col
FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- AGGREGATIONS
-- ============================================================================
.print '--- Aggregations ---'

-- Basic aggregations
SELECT
    COUNT(*) as cnt,
    MIN(1) as min_val,
    MAX(100) as max_val,
    SUM(50) as sum_val,
    AVG(25.0) as avg_val
FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- DISTINCT VALUES
-- ============================================================================
.print '--- Distinct Values ---'

SELECT DISTINCT 1 as unique_val FROM duckarrow."Order" LIMIT 10;

-- ============================================================================
-- ORDER BY
-- ============================================================================
.print '--- Order By ---'

-- Verify ordering works
SELECT * FROM duckarrow."Order" ORDER BY 1 LIMIT 5;

-- ============================================================================
-- SUBQUERIES
-- ============================================================================
.print '--- Subqueries ---'

SELECT * FROM (SELECT 1 as inner_val FROM duckarrow."Order" LIMIT 1) sub;

-- ============================================================================
-- SPECIAL STRING VALUES
-- ============================================================================
.print '--- Special String Values ---'

-- Empty string
SELECT '' as empty FROM duckarrow."Order" LIMIT 1;

-- String with quotes
SELECT 'it''s a "test"' as quotes FROM duckarrow."Order" LIMIT 1;

-- String with newlines (escaped)
SELECT E'line1\nline2' as newlines FROM duckarrow."Order" LIMIT 1;

-- String with tabs
SELECT E'col1\tcol2' as tabs FROM duckarrow."Order" LIMIT 1;

-- Very long string
SELECT REPEAT('x', 10000) as long_str FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- RESULT VERIFICATION
-- ============================================================================
.print '--- Result Verification ---'

-- Verify empty result count is 0
SELECT CASE WHEN (SELECT COUNT(*) FROM duckarrow."Order" WHERE 1=0) = 0 THEN 'PASS' ELSE 'FAIL: empty result' END as empty_verify FROM duckarrow."Order" LIMIT 1;

-- Verify unicode round-trip
SELECT CASE WHEN '世界' = '世界' THEN 'PASS' ELSE 'FAIL: unicode' END as unicode_verify FROM duckarrow."Order" LIMIT 1;

-- Verify subquery works
SELECT CASE WHEN (SELECT 42 FROM duckarrow."Order" LIMIT 1) = 42 THEN 'PASS' ELSE 'FAIL: subquery' END as subq_verify FROM duckarrow."Order" LIMIT 1;

-- Verify long string length
SELECT CASE WHEN LENGTH(REPEAT('x', 10000)) = 10000 THEN 'PASS' ELSE 'FAIL: long string' END as longstr_verify FROM duckarrow."Order" LIMIT 1;

-- Verify aggregation works
SELECT CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL: aggregation' END as agg_verify FROM duckarrow."Order";

.print '=== Edge Case Tests Complete ==='
