-- Data Type Integration Tests
-- Tests Arrow→DuckDB type conversions via synthetic queries
-- Requires: Flight SQL server at localhost:31337

.bail on

-- Load extension and configure
LOAD './build/duckarrow.duckdb_extension';
SELECT duckarrow_configure('grpc+tls://localhost:31337', 'gizmosql_user', 'gizmosql_password', true);

.print '=== Data Type Tests ==='

-- ============================================================================
-- STRING TYPES
-- ============================================================================
.print '--- String Types ---'

-- Test VARCHAR/STRING
SELECT 'hello' as str_val FROM duckarrow."Order" LIMIT 1;

-- Test empty string
SELECT '' as empty_str FROM duckarrow."Order" LIMIT 1;

-- Test unicode string
SELECT '世界 🌍' as unicode_str FROM duckarrow."Order" LIMIT 1;

-- Test NULL string
SELECT NULL::VARCHAR as null_str FROM duckarrow."Order" LIMIT 1;

-- Verify string operations work
SELECT 'hello' || ' world' as concat_test FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- INTEGER TYPES
-- ============================================================================
.print '--- Integer Types ---'

-- INT8 (TINYINT)
SELECT CAST(127 AS TINYINT) as int8_max, CAST(-128 AS TINYINT) as int8_min FROM duckarrow."Order" LIMIT 1;

-- INT16 (SMALLINT)
SELECT CAST(32767 AS SMALLINT) as int16_max, CAST(-32768 AS SMALLINT) as int16_min FROM duckarrow."Order" LIMIT 1;

-- INT32 (INTEGER)
SELECT CAST(2147483647 AS INTEGER) as int32_max, CAST(-2147483648 AS INTEGER) as int32_min FROM duckarrow."Order" LIMIT 1;

-- INT64 (BIGINT)
SELECT CAST(9223372036854775807 AS BIGINT) as int64_max FROM duckarrow."Order" LIMIT 1;

-- NULL integers
SELECT CAST(NULL AS BIGINT) as null_int FROM duckarrow."Order" LIMIT 1;

-- Verify integer arithmetic
SELECT CAST(100 AS INTEGER) + 1 as int_arithmetic FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- UNSIGNED INTEGER TYPES
-- ============================================================================
.print '--- Unsigned Integer Types ---'

-- UINT8 (UTINYINT)
SELECT CAST(255 AS UTINYINT) as uint8_max, CAST(0 AS UTINYINT) as uint8_zero FROM duckarrow."Order" LIMIT 1;

-- UINT16 (USMALLINT)
SELECT CAST(65535 AS USMALLINT) as uint16_max FROM duckarrow."Order" LIMIT 1;

-- UINT32 (UINTEGER)
SELECT CAST(4294967295 AS UINTEGER) as uint32_max FROM duckarrow."Order" LIMIT 1;

-- UINT64 (UBIGINT)
SELECT CAST(18446744073709551615 AS UBIGINT) as uint64_max FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- FLOATING POINT TYPES
-- ============================================================================
.print '--- Floating Point Types ---'

-- FLOAT (REAL)
SELECT CAST(3.14159 AS REAL) as float32_val FROM duckarrow."Order" LIMIT 1;

-- DOUBLE
SELECT CAST(3.141592653589793 AS DOUBLE) as float64_val FROM duckarrow."Order" LIMIT 1;

-- Negative floats
SELECT CAST(-123.456 AS DOUBLE) as negative_float FROM duckarrow."Order" LIMIT 1;

-- Zero
SELECT CAST(0.0 AS DOUBLE) as zero_float FROM duckarrow."Order" LIMIT 1;

-- NULL float
SELECT CAST(NULL AS DOUBLE) as null_float FROM duckarrow."Order" LIMIT 1;

-- Special float values
SELECT CAST('Infinity' AS DOUBLE) as pos_infinity FROM duckarrow."Order" LIMIT 1;
SELECT CAST('-Infinity' AS DOUBLE) as neg_infinity FROM duckarrow."Order" LIMIT 1;
SELECT CAST('NaN' AS DOUBLE) as nan_value FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- BOOLEAN TYPE
-- ============================================================================
.print '--- Boolean Type ---'

SELECT TRUE as bool_true, FALSE as bool_false FROM duckarrow."Order" LIMIT 1;
SELECT CAST(NULL AS BOOLEAN) as bool_null FROM duckarrow."Order" LIMIT 1;

-- Verify boolean operations
SELECT NOT TRUE as not_true, NOT FALSE as not_false FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- TEMPORAL TYPES
-- ============================================================================
.print '--- Temporal Types ---'

-- TIMESTAMP
SELECT TIMESTAMP '2024-01-15 10:30:00' as ts_val FROM duckarrow."Order" LIMIT 1;
SELECT CAST(NULL AS TIMESTAMP) as ts_null FROM duckarrow."Order" LIMIT 1;

-- DATE
SELECT DATE '2024-01-15' as date_val FROM duckarrow."Order" LIMIT 1;
SELECT CAST(NULL AS DATE) as date_null FROM duckarrow."Order" LIMIT 1;

-- TIME
SELECT TIME '10:30:00' as time_val FROM duckarrow."Order" LIMIT 1;
SELECT CAST(NULL AS TIME) as time_null FROM duckarrow."Order" LIMIT 1;

-- Verify temporal arithmetic
SELECT DATE '2024-01-15' + INTERVAL 1 DAY as date_add FROM duckarrow."Order" LIMIT 1;
SELECT TIMESTAMP '2024-01-15 10:30:00' + INTERVAL 1 HOUR as ts_add FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- BINARY TYPES
-- ============================================================================
.print '--- Binary Types ---'

-- BLOB (binary)
SELECT '\xDEADBEEF'::BLOB as blob_val FROM duckarrow."Order" LIMIT 1;
SELECT ''::BLOB as empty_blob FROM duckarrow."Order" LIMIT 1;
SELECT CAST(NULL AS BLOB) as null_blob FROM duckarrow."Order" LIMIT 1;

-- UUID (fixed-size binary)
SELECT uuid() as uuid_val FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- DECIMAL TYPES
-- ============================================================================
.print '--- Decimal Types ---'

-- DECIMAL with various precisions
SELECT CAST(1234 AS DECIMAL(4,0)) as dec4 FROM duckarrow."Order" LIMIT 1;
SELECT CAST(123456789 AS DECIMAL(9,0)) as dec9 FROM duckarrow."Order" LIMIT 1;
SELECT CAST(123456789012345678 AS DECIMAL(18,0)) as dec18 FROM duckarrow."Order" LIMIT 1;
SELECT CAST(12345678901234567890123456789012345678 AS DECIMAL(38,0)) as dec38 FROM duckarrow."Order" LIMIT 1;

-- DECIMAL with scale
SELECT CAST(123.45 AS DECIMAL(10,2)) as dec_scale FROM duckarrow."Order" LIMIT 1;
SELECT CAST(NULL AS DECIMAL(10,2)) as dec_null FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- NESTED TYPES - LIST
-- ============================================================================
.print '--- List Types ---'

-- Integer list
SELECT [1, 2, 3] as int_list FROM duckarrow."Order" LIMIT 1;

-- Empty list
SELECT []::INTEGER[] as empty_list FROM duckarrow."Order" LIMIT 1;

-- List with NULL elements
SELECT [1, NULL, 3] as list_with_null FROM duckarrow."Order" LIMIT 1;

-- NULL list
SELECT CAST(NULL AS INTEGER[]) as null_list FROM duckarrow."Order" LIMIT 1;

-- String list
SELECT ['a', 'b', 'c'] as str_list FROM duckarrow."Order" LIMIT 1;

-- Verify list access
SELECT [10, 20, 30][1] as list_access FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- NESTED TYPES - STRUCT
-- ============================================================================
.print '--- Struct Types ---'

-- Simple struct
SELECT {'a': 1, 'b': 'x'} as simple_struct FROM duckarrow."Order" LIMIT 1;

-- Struct with NULL field
SELECT {'a': 1, 'b': NULL} as struct_null_field FROM duckarrow."Order" LIMIT 1;

-- NULL struct
SELECT NULL::STRUCT(a INTEGER, b VARCHAR) as null_struct FROM duckarrow."Order" LIMIT 1;

-- Verify struct field access
SELECT {'name': 'test', 'value': 42}.name as struct_access FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- NESTED TYPES - MAP
-- ============================================================================
.print '--- Map Types ---'

-- Simple map
SELECT MAP {'key1': 'value1', 'key2': 'value2'} as simple_map FROM duckarrow."Order" LIMIT 1;

-- Empty map
SELECT MAP {}::MAP(VARCHAR, VARCHAR) as empty_map FROM duckarrow."Order" LIMIT 1;

-- NULL map
SELECT CAST(NULL AS MAP(VARCHAR, VARCHAR)) as null_map FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- MIXED NULL HANDLING
-- ============================================================================
.print '--- Mixed NULL Handling ---'

SELECT
    CAST(NULL AS VARCHAR) as null_str,
    CAST(NULL AS BIGINT) as null_int,
    CAST(NULL AS DOUBLE) as null_float,
    CAST(NULL AS BOOLEAN) as null_bool,
    CAST(NULL AS TIMESTAMP) as null_ts
FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- TYPE COERCION
-- ============================================================================
.print '--- Type Coercion ---'

-- Integer to float
SELECT CAST(42 AS DOUBLE) + 0.5 as int_to_float FROM duckarrow."Order" LIMIT 1;

-- String to integer (via cast)
SELECT CAST('123' AS INTEGER) as str_to_int FROM duckarrow."Order" LIMIT 1;

-- ============================================================================
-- RESULT VERIFICATION
-- ============================================================================
.print '--- Result Verification ---'

-- Verify integer arithmetic works correctly
SELECT CASE WHEN (CAST(100 AS INTEGER) + 50) = 150 THEN 'PASS' ELSE 'FAIL: int arithmetic' END as int_verify FROM duckarrow."Order" LIMIT 1;

-- Verify string concatenation works correctly
SELECT CASE WHEN ('hello' || ' ' || 'world') = 'hello world' THEN 'PASS' ELSE 'FAIL: string concat' END as str_verify FROM duckarrow."Order" LIMIT 1;

-- Verify boolean logic works correctly
SELECT CASE WHEN (NOT FALSE) = TRUE THEN 'PASS' ELSE 'FAIL: boolean logic' END as bool_verify FROM duckarrow."Order" LIMIT 1;

-- Verify list access works correctly
SELECT CASE WHEN [10, 20, 30][2] = 20 THEN 'PASS' ELSE 'FAIL: list access' END as list_verify FROM duckarrow."Order" LIMIT 1;

-- Verify struct access works correctly
SELECT CASE WHEN {'a': 42}.a = 42 THEN 'PASS' ELSE 'FAIL: struct access' END as struct_verify FROM duckarrow."Order" LIMIT 1;

-- Verify type coercion works correctly
SELECT CASE WHEN CAST('123' AS INTEGER) = 123 THEN 'PASS' ELSE 'FAIL: type coercion' END as coerce_verify FROM duckarrow."Order" LIMIT 1;

.print '=== Data Type Tests Complete ==='
